use crate::{
    pollers::{self, Poller},
    worker::client::WorkerClient,
};
use futures::{prelude::stream::FuturesUnordered, StreamExt};
use std::{
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
};
use tokio::{
    sync::{
        mpsc::{channel, Receiver},
        Mutex, Semaphore,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub struct LongPollBuffer<T> {
    buffered_polls: Mutex<Receiver<pollers::Result<T>>>,
    shutdown: CancellationToken,
    /// This semaphore exists to ensure that we only poll server as many times as core actually
    /// *asked* it to be polled - otherwise we might spin and buffer polls constantly. This also
    /// means unit tests can continue to function in a predictable manner when calling mocks.
    polls_requested: Arc<Semaphore>,
    join_handles: FuturesUnordered<JoinHandle<()>>,
    /// Called every time the number of pollers is changed
    num_pollers_changed: Option<Box<dyn Fn(usize) + Send + Sync>>,
    active_pollers: Arc<AtomicUsize>,
}

struct ActiveCounter<'a>(&'a AtomicUsize);
impl<'a> ActiveCounter<'a> {
    fn new(a: &'a AtomicUsize) -> Self {
        a.fetch_add(1, Ordering::Relaxed);
        Self(a)
    }
}
impl Drop for ActiveCounter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<T> LongPollBuffer<T>
where
    T: Send + Debug + 'static,
{
    pub fn new<FT>(
        poll_fn: impl Fn() -> FT + Send + Sync + 'static,
        max_pollers: usize,
        buffer_size: usize,
        shutdown: CancellationToken,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
    {
        let (tx, rx) = channel(buffer_size);
        let polls_requested = Arc::new(Semaphore::new(0));
        let active_pollers = Arc::new(AtomicUsize::new(0));
        let join_handles = FuturesUnordered::new();
        let pf = Arc::new(poll_fn);
        for _ in 0..max_pollers {
            let tx = tx.clone();
            let pf = pf.clone();
            let shutdown = shutdown.clone();
            let polls_requested = polls_requested.clone();
            let ap = active_pollers.clone();
            let jh = tokio::spawn(async move {
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let sp = tokio::select! {
                        sp = polls_requested.acquire() => sp.expect("Polls semaphore not dropped"),
                        _ = shutdown.cancelled() => continue,
                    };
                    let _active_guard = ActiveCounter::new(ap.as_ref());
                    let r = tokio::select! {
                        r = pf() => r,
                        _ = shutdown.cancelled() => continue,
                    };
                    sp.forget();
                    let _ = tx.send(r).await;
                }
            });
            join_handles.push(jh);
        }
        Self {
            buffered_polls: Mutex::new(rx),
            shutdown,
            polls_requested,
            join_handles,
            num_pollers_changed: None,
            active_pollers,
        }
    }

    /// Set a function that will be called every time the number of pollers changes.
    /// TODO: Currently a bit weird, will make more sense once we implement dynamic poller scaling.
    pub fn set_num_pollers_handler(&mut self, handler: impl Fn(usize) + Send + Sync + 'static) {
        self.num_pollers_changed = Some(Box::new(handler));
    }
}

#[async_trait::async_trait]
impl<T> Poller<T> for LongPollBuffer<T>
where
    T: Send + Sync + Debug + 'static,
{
    /// Poll the buffer. Adds one permit to the polling pool - the point of this being that the
    /// buffer may support many concurrent pollers, but there is no reason to have them poll unless
    /// enough polls have actually been requested. Calling this function adds a permit that any
    /// concurrent poller may fulfill.
    ///
    /// EX: If this function is only ever called serially and always `await`ed, there will be no
    /// concurrent polling. If it is called many times and the futures are awaited concurrently,
    /// then polling will happen concurrently.
    ///
    /// Returns `None` if the poll buffer has been shut down
    #[instrument(name = "long_poll", level = "trace", skip(self))]
    async fn poll(&self) -> Option<pollers::Result<T>> {
        self.polls_requested.add_permits(1);
        if let Some(fun) = self.num_pollers_changed.as_ref() {
            fun(self.active_pollers.load(Ordering::Relaxed));
        }

        let mut locked = self.buffered_polls.lock().await;
        let res = (*locked).recv().await;

        if let Some(fun) = self.num_pollers_changed.as_ref() {
            fun(self.active_pollers.load(Ordering::Relaxed));
        }

        res
    }

    fn notify_shutdown(&self) {
        self.shutdown.cancel();
    }

    async fn shutdown(mut self) {
        self.notify_shutdown();
        while self.join_handles.next().await.is_some() {}
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
    }
}

/// A poller capable of polling on a sticky and a nonsticky queue simultaneously for workflow tasks.
#[derive(derive_more::Constructor)]
pub struct WorkflowTaskPoller {
    normal_poller: PollWorkflowTaskBuffer,
    sticky_poller: Option<PollWorkflowTaskBuffer>,
}

#[async_trait::async_trait]
impl Poller<PollWorkflowTaskQueueResponse> for WorkflowTaskPoller {
    async fn poll(&self) -> Option<pollers::Result<PollWorkflowTaskQueueResponse>> {
        if let Some(sq) = self.sticky_poller.as_ref() {
            tokio::select! {
                r = self.normal_poller.poll() => r,
                r = sq.poll() => r,
            }
        } else {
            self.normal_poller.poll().await
        }
    }

    fn notify_shutdown(&self) {
        self.normal_poller.notify_shutdown();
        if let Some(sq) = self.sticky_poller.as_ref() {
            sq.notify_shutdown();
        }
    }

    async fn shutdown(mut self) {
        self.normal_poller.shutdown().await;
        if let Some(sq) = self.sticky_poller {
            sq.shutdown().await;
        }
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
    }
}

pub type PollWorkflowTaskBuffer = LongPollBuffer<PollWorkflowTaskQueueResponse>;
pub(crate) fn new_workflow_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    is_sticky: bool,
    concurrent_pollers: usize,
    buffer_size: usize,
    shutdown: CancellationToken,
) -> PollWorkflowTaskBuffer {
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_workflow_task(task_queue, is_sticky).await }
        },
        concurrent_pollers,
        buffer_size,
        shutdown,
    )
}

pub type PollActivityTaskBuffer = LongPollBuffer<PollActivityTaskQueueResponse>;
pub(crate) fn new_activity_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    concurrent_pollers: usize,
    buffer_size: usize,
    max_tps: Option<f64>,
    shutdown: CancellationToken,
) -> PollActivityTaskBuffer {
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_activity_task(task_queue, max_tps).await }
        },
        concurrent_pollers,
        buffer_size,
        shutdown,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::client::mocks::mock_manual_workflow_client;
    use futures::FutureExt;
    use std::time::Duration;
    use tokio::{select, sync::mpsc::channel};

    #[tokio::test]
    async fn only_polls_once_with_1_poller() {
        let mut mock_client = mock_manual_workflow_client();
        mock_client
            .expect_poll_workflow_task()
            .times(2)
            .returning(move |_, _| {
                async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(Default::default())
                }
                .boxed()
            });

        let pb = new_workflow_task_buffer(
            Arc::new(mock_client),
            "someq".to_string(),
            false,
            1,
            1,
            CancellationToken::new(),
        );

        // Poll a bunch of times, "interrupting" it each time, we should only actually have polled
        // once since the poll takes a while
        let (interrupter_tx, mut interrupter_rx) = channel(50);
        for _ in 0..10 {
            interrupter_tx.send(()).await.unwrap();
        }

        // We should never get anything out since we interrupted 100% of polls
        let mut last_val = false;
        for _ in 0..10 {
            select! {
                _ = interrupter_rx.recv() => {
                    last_val = true;
                }
                _ = pb.poll() => {
                }
            }
        }
        assert!(last_val);
        // Now we grab the buffered poll response, the poll task will go again but we don't grab it,
        // therefore we will have only polled twice.
        pb.poll().await.unwrap().unwrap();
        pb.shutdown().await;
    }
}
