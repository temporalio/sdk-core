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

thread_local! {
    static SDK_WAKE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Guard that marks the current scope as an SDK-initiated wake source.
///
/// When the tracking waker's `wake()` is called while this guard is active
/// (depth > 0), the wake is recognized as coming from SDK internals and is not
/// flagged as nondeterministic. Nesting is safe via a depth counter, and the
/// `Drop` impl ensures cleanup.
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

/// Shared state for a tracking waker that detects non-SDK wake sources.
///
/// Implements [Wake] so it can be converted to a [Waker] via [Waker::from]. Satisfies `Send +
/// Sync`, but cross-thread wakes are inherently detected (the thread-local guard won't be set on
/// a foreign thread).
pub(crate) struct WakeTracker {
    /// Set when a wake arrives without the SDK guard active.
    non_sdk_wake_detected: AtomicBool,
    /// The real waker to forward to (from the executor's task notifier).
    parent_waker: parking_lot::Mutex<Waker>,
}

impl WakeTracker {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            non_sdk_wake_detected: AtomicBool::new(false),
            parent_waker: parking_lot::Mutex::new(Waker::noop().clone()),
        })
    }

    /// Update the parent waker (called at the top of each poll in case the executor provided a new
    /// waker).
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
        if !is_sdk_wake() {
            self.non_sdk_wake_detected.store(true, Ordering::Release);
        }
        self.parent_waker.lock().wake_by_ref();
    }
}

/// A future wrapper that activates [`SdkWakeGuard`] during poll. Use this around futures whose
/// internal waker machinery (e.g., `FuturesOrdered` inside `join_all`) would otherwise trigger
/// false positives in nondeterminism detection.
pub(crate) struct SdkGuardedFuture<F>(pub(crate) F);

impl<F: Future + Unpin> Future for SdkGuardedFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = SdkWakeGuard::new();
        Pin::new(&mut self.0).poll(cx)
    }
}

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

struct LocalTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
    waker: Waker,
}

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

/// A minimal single-threaded async executor for workflow futures.
///
/// Replaces [tokio::task::LocalSet] + `spawn_local` for workflow tasks. Runs inside the existing
/// tokio runtime (driven as an async future) but provides its own task management with custom
/// wakers, enabling nondeterminism detection via [WakeTracker]
///
/// All spawned futures are `!Send` (they use `Rc<RefCell<...>>` internally). The executor itself is
/// `!Send` and must be driven from a `LocalSet` or single-threaded context.
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
                waker: Waker::from(Arc::new(TaskNotifier {
                    task_id: id,
                    shared: self.shared.clone(),
                })),
            },
        );

        self.shared.enqueue(id);

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

                // Take the task out so we don't hold a borrow on `self.tasks`
                // across the poll (which would panic if the future spawns).
                let Some(mut task) = self.tasks.borrow_mut().remove(&task_id) else {
                    continue;
                };

                let mut task_cx = Context::from_waker(&task.waker);
                if task.future.as_mut().poll(&mut task_cx).is_pending() {
                    self.tasks.borrow_mut().insert(task_id, task);
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
                let executor = Rc::new(WorkflowExecutor::new());
                let handle = executor.spawn(async { 42 });

                let executor_clone = executor.clone();
                let exec_task = tokio::task::spawn_local(async move {
                    executor_clone.shutdown();
                    executor_clone.run().await;
                });

                let result = handle.await.unwrap();
                assert_eq!(result, 42);
                exec_task.await.unwrap();
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

    #[tokio::test]
    async fn spawn_while_parked_wakes_executor() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = Rc::new(WorkflowExecutor::new());
                let (tx, rx) = oneshot::channel::<()>();

                // Start the executor running with no tasks — it will park immediately.
                let executor_clone = executor.clone();
                tokio::task::spawn_local(async move {
                    executor_clone.run().await;
                });

                // Let the executor park.
                tokio::task::yield_now().await;

                // Spawn a task while the executor is parked. Without the wake in
                // enqueue, the executor never learns about this task.
                let handle = executor.spawn(async move {
                    rx.await.unwrap();
                    42
                });

                // Yield to give the executor a chance to poll the new task.
                tokio::task::yield_now().await;
                tx.send(()).unwrap();

                let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
                    .await
                    .expect("executor should have polled the spawned task")
                    .unwrap();
                assert_eq!(result, 42);

                executor.shutdown();
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
        let tracker = WakeTracker::new();
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
    fn wake_tracker_cross_thread_detection() {
        let tracker = WakeTracker::new();
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
