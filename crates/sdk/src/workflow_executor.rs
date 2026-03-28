use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

// ── Thread-local SDK wake guard ─────────────────────────────────────────────

thread_local! {
    static SDK_WAKE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// RAII guard that marks the current scope as an SDK-initiated wake source.
///
/// When the tracking waker's `wake()` is called while this guard is active
/// (depth > 0), the wake is recognized as coming from SDK internals and is not
/// flagged as nondeterministic. Nesting is safe via a depth counter, and the
/// `Drop` impl ensures cleanup even on unwind.
pub(crate) struct SdkWakeGuard {
    _priv: (), // prevent construction outside this module
}

impl SdkWakeGuard {
    pub(crate) fn new() -> Self {
        SDK_WAKE_DEPTH.with(|c| c.set(c.get() + 1));
        Self { _priv: () }
    }
}

impl Drop for SdkWakeGuard {
    fn drop(&mut self) {
        SDK_WAKE_DEPTH.with(|c| c.set(c.get() - 1));
    }
}

fn is_sdk_wake() -> bool {
    SDK_WAKE_DEPTH.with(|c| c.get() > 0)
}

// ── Wake tracker (nondeterminism detection) ─────────────────────────────────

/// Shared state for a tracking waker that detects non-SDK wake sources.
///
/// Implements `std::task::Wake` so it can be converted to a `Waker` via
/// `Waker::from(arc)`. Uses atomics and `parking_lot::Mutex` so the `Waker`
/// satisfies `Send + Sync`, while cross-thread wakes are inherently detected
/// (the thread-local guard won't be set on a foreign thread).
pub(crate) struct WakeTracker {
    /// Set when a wake arrives without the SDK guard active.
    pub(crate) non_sdk_wake_detected: AtomicBool,
    /// The real waker to forward to (from the executor's task notifier).
    parent_waker: parking_lot::Mutex<Waker>,
    detection_enabled: bool,
}

impl WakeTracker {
    pub(crate) fn new(parent_waker: Waker, detection_enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            non_sdk_wake_detected: AtomicBool::new(false),
            parent_waker: parking_lot::Mutex::new(parent_waker),
            detection_enabled,
        })
    }

    /// Update the parent waker (called at the top of each poll in case the
    /// executor provided a new waker).
    pub(crate) fn update_parent_waker(&self, waker: &Waker) {
        let mut guard = self.parent_waker.lock();
        if !guard.will_wake(waker) {
            *guard = waker.clone();
        }
    }

    /// Check and clear the detection flag.
    pub(crate) fn take_non_sdk_wake(&self) -> bool {
        self.non_sdk_wake_detected.swap(false, Ordering::AcqRel)
    }
}

impl Wake for WakeTracker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.detection_enabled && !is_sdk_wake() {
            self.non_sdk_wake_detected.store(true, Ordering::Release);
        }
        self.parent_waker.lock().wake_by_ref();
    }
}

// ── Executor shared state (Send) ───────────────────────────────────────────

struct ExecutorShared {
    ready_queue: parking_lot::Mutex<VecDeque<u64>>,
    executor_waker: parking_lot::Mutex<Option<Waker>>,
}

impl ExecutorShared {
    fn enqueue(&self, task_id: u64) {
        self.ready_queue.lock().push_back(task_id);
        if let Some(w) = self.executor_waker.lock().as_ref() {
            w.wake_by_ref();
        }
    }
}

// ── Per-task waker (Send + Sync via Arc) ────────────────────────────────────

struct TaskNotifier {
    task_id: u64,
    shared: Arc<ExecutorShared>,
}

impl Wake for TaskNotifier {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.enqueue(self.task_id);
    }
}

// ── Local task storage ──────────────────────────────────────────────────────

struct LocalTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

// ── TaskHandle (!Send join handle) ──────────────────────────────────────────

struct TaskHandleInner<T> {
    result: RefCell<Option<T>>,
    waker: RefCell<Option<Waker>>,
}

/// A `!Send` join handle returned by [`WorkflowExecutor::spawn`].
///
/// Resolves to the spawned future's output when the executor completes it.
pub(crate) struct TaskHandle<T> {
    inner: Rc<TaskHandleInner<T>>,
}

/// Error returned by [`TaskHandle`] if the task was dropped without completing.
#[derive(Debug)]
pub(crate) struct TaskDroppedError;

impl std::fmt::Display for TaskDroppedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task was dropped before completing")
    }
}

impl std::error::Error for TaskDroppedError {}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, TaskDroppedError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.inner.result.borrow_mut().take() {
            Poll::Ready(Ok(result))
        } else {
            *self.inner.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ── WorkflowExecutor ────────────────────────────────────────────────────────

/// A minimal single-threaded async executor for workflow futures.
///
/// Replaces `tokio::task::LocalSet` + `spawn_local` for workflow tasks. Runs
/// inside the existing tokio runtime (driven as an async future) but provides
/// its own task management with custom wakers, enabling nondeterminism
/// detection via [`WakeTracker`].
///
/// All spawned futures are `!Send` (they use `Rc<RefCell<...>>` internally).
/// The executor itself is `!Send` and must be driven from a `LocalSet` or
/// single-threaded context.
pub(crate) struct WorkflowExecutor {
    tasks: RefCell<HashMap<u64, LocalTask>>,
    next_id: Cell<u64>,
    shared: Arc<ExecutorShared>,
    shutdown: Cell<bool>,
}

impl WorkflowExecutor {
    pub(crate) fn new() -> Self {
        Self {
            tasks: RefCell::new(HashMap::new()),
            next_id: Cell::new(0),
            shared: Arc::new(ExecutorShared {
                ready_queue: parking_lot::Mutex::new(VecDeque::new()),
                executor_waker: parking_lot::Mutex::new(None),
            }),
            shutdown: Cell::new(false),
        }
    }

    /// Spawn a future onto this executor, returning a `!Send` join handle.
    pub(crate) fn spawn<F, T>(&self, future: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let id = self.next_id.get();
        self.next_id.set(id + 1);

        let handle_inner = Rc::new(TaskHandleInner {
            result: RefCell::new(None),
            waker: RefCell::new(None),
        });

        let inner_clone = handle_inner.clone();
        let wrapped = async move {
            let output = future.await;
            *inner_clone.result.borrow_mut() = Some(output);
            if let Some(w) = inner_clone.waker.borrow_mut().take() {
                w.wake();
            }
        };

        self.tasks.borrow_mut().insert(
            id,
            LocalTask {
                future: Box::pin(wrapped),
            },
        );

        // Queue for initial poll
        self.shared.ready_queue.lock().push_back(id);

        TaskHandle {
            inner: handle_inner,
        }
    }

    /// Signal the executor to shut down once all tasks have completed.
    pub(crate) fn shutdown(&self) {
        self.shutdown.set(true);
        // Wake the executor in case it's parked waiting for work
        if let Some(w) = self.shared.executor_waker.lock().as_ref() {
            w.wake_by_ref();
        }
    }

    /// Run the executor until shutdown is signaled and all tasks complete.
    ///
    /// This is an async method intended to be driven from a `LocalSet` or
    /// similar `!Send` context. It yields back to the outer runtime when no
    /// tasks are ready.
    pub(crate) async fn run(&self) {
        std::future::poll_fn(|cx| {
            // Store executor waker so TaskNotifiers can wake us
            *self.shared.executor_waker.lock() = Some(cx.waker().clone());

            // Drain ready queue and poll tasks
            loop {
                let task_id = self.shared.ready_queue.lock().pop_front();
                let Some(task_id) = task_id else { break };

                let mut tasks = self.tasks.borrow_mut();
                let Some(task) = tasks.get_mut(&task_id) else {
                    continue;
                };

                let waker = Waker::from(Arc::new(TaskNotifier {
                    task_id,
                    shared: self.shared.clone(),
                }));
                let mut task_cx = Context::from_waker(&waker);

                match task.future.as_mut().poll(&mut task_cx) {
                    Poll::Ready(()) => {
                        tasks.remove(&task_id);
                    }
                    Poll::Pending => {}
                }
            }

            if self.shutdown.get() && self.tasks.borrow().is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn executor_spawn_and_complete() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = WorkflowExecutor::new();
                let handle = executor.spawn(async { 42 });

                // Drive executor in background
                let exec_task = tokio::task::spawn_local({
                    async move {
                        // Need to give the handle a chance to resolve
                        // Poll executor once, then shut down
                        std::future::poll_fn(|cx| {
                            *executor.shared.executor_waker.lock() = Some(cx.waker().clone());
                            loop {
                                let task_id = executor.shared.ready_queue.lock().pop_front();
                                let Some(task_id) = task_id else { break };
                                let mut tasks = executor.tasks.borrow_mut();
                                if let Some(task) = tasks.get_mut(&task_id) {
                                    let waker = Waker::from(Arc::new(TaskNotifier {
                                        task_id,
                                        shared: executor.shared.clone(),
                                    }));
                                    let mut task_cx = Context::from_waker(&waker);
                                    if task.future.as_mut().poll(&mut task_cx).is_ready() {
                                        tasks.remove(&task_id);
                                    }
                                }
                            }
                            Poll::Ready(())
                        })
                        .await;
                    }
                });

                exec_task.await.unwrap();
                let result = handle.await.unwrap();
                assert_eq!(result, 42);
            })
            .await;
    }

    #[tokio::test]
    async fn executor_multiple_tasks() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = Rc::new(WorkflowExecutor::new());
                let h1 = executor.spawn(async { 1 });
                let h2 = executor.spawn(async { 2 });
                let h3 = executor.spawn(async { 3 });

                let executor_clone = executor.clone();
                let exec_task = tokio::task::spawn_local(async move {
                    executor_clone.shutdown();
                    executor_clone.run().await;
                });

                let (r1, r2, r3) = futures_util::join!(h1, h2, h3);
                assert_eq!(r1.unwrap(), 1);
                assert_eq!(r2.unwrap(), 2);
                assert_eq!(r3.unwrap(), 3);
                exec_task.await.unwrap();
            })
            .await;
    }

    #[tokio::test]
    async fn executor_wake_forwarding_through_oneshot() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = Rc::new(WorkflowExecutor::new());
                let (tx, rx) = oneshot::channel::<i32>();

                let handle = executor.spawn(async move { rx.await.unwrap() });

                let executor_clone = executor.clone();
                tokio::task::spawn_local(async move {
                    executor_clone.shutdown();
                    executor_clone.run().await;
                });

                // Send after a yield to ensure the future has parked
                tokio::task::yield_now().await;
                tx.send(99).unwrap();

                let result = handle.await.unwrap();
                assert_eq!(result, 99);
            })
            .await;
    }

    #[test]
    fn sdk_wake_guard_nesting() {
        assert!(!is_sdk_wake());

        let _g1 = SdkWakeGuard::new();
        assert!(is_sdk_wake());

        {
            let _g2 = SdkWakeGuard::new();
            assert!(is_sdk_wake());
        }
        // g2 dropped, but g1 still active
        assert!(is_sdk_wake());

        drop(_g1);
        assert!(!is_sdk_wake());
    }

    #[test]
    fn sdk_wake_guard_panic_safety() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = SdkWakeGuard::new();
            panic!("test panic");
        }));
        assert!(result.is_err());
        // Guard was cleaned up by Drop despite the panic
        assert!(!is_sdk_wake());
    }

    #[test]
    fn wake_tracker_detects_non_sdk_wake() {
        let noop_waker = Waker::noop().clone();
        let tracker = WakeTracker::new(noop_waker, true);
        let waker = Waker::from(tracker.clone());

        // Wake without SDK guard -- should be detected
        waker.wake_by_ref();
        assert!(tracker.take_non_sdk_wake());

        // Wake with SDK guard -- should not be detected
        let _guard = SdkWakeGuard::new();
        waker.wake_by_ref();
        assert!(!tracker.take_non_sdk_wake());
    }

    #[test]
    fn wake_tracker_detection_disabled() {
        let noop_waker = Waker::noop().clone();
        let tracker = WakeTracker::new(noop_waker, false);
        let waker = Waker::from(tracker.clone());

        // Wake without SDK guard -- should NOT be detected (disabled)
        waker.wake_by_ref();
        assert!(!tracker.take_non_sdk_wake());
    }

    #[test]
    fn wake_tracker_cross_thread_detection() {
        let noop_waker = Waker::noop().clone();
        let tracker = WakeTracker::new(noop_waker, true);
        let waker = Waker::from(tracker.clone());

        // Set SDK guard on THIS thread
        let _guard = SdkWakeGuard::new();

        // Wake from another thread -- thread-local not set there
        let handle = std::thread::spawn(move || {
            waker.wake_by_ref();
        });
        handle.join().unwrap();

        assert!(tracker.take_non_sdk_wake());
    }
}
