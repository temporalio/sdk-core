use crate::{
    abstractions::{MeteredSemaphore, OwnedMeteredSemPermit},
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
        mpsc::{unbounded_channel, UnboundedReceiver},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub struct LongPollBuffer<T> {
    buffered_polls: Mutex<UnboundedReceiver<pollers::Result<(T, OwnedMeteredSemPermit)>>>,
    shutdown: CancellationToken,
    join_handles: FuturesUnordered<JoinHandle<()>>,
    /// Called every time the number of pollers is changed
    num_pollers_changed: Option<Box<dyn Fn(usize) + Send + Sync>>,
    active_pollers: Arc<AtomicUsize>,
}

struct ActiveCounter<'a, F: Fn(usize)>(&'a AtomicUsize, Option<F>);
impl<'a, F> ActiveCounter<'a, F>
where
    F: Fn(usize),
{
    fn new(a: &'a AtomicUsize, change_fn: Option<F>) -> Self {
        let v = a.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(cfn) = change_fn.as_ref() {
            cfn(v);
        }
        Self(a, change_fn)
    }
}
impl<F> Drop for ActiveCounter<'_, F>
where
    F: Fn(usize),
{
    fn drop(&mut self) {
        let v = self.0.fetch_sub(1, Ordering::Relaxed) - 1;
        if let Some(cfn) = self.1.as_ref() {
            cfn(v)
        }
    }
}

impl<T> LongPollBuffer<T>
where
    T: Send + Debug + 'static,
{
    pub(crate) fn new<FT>(
        poll_fn: impl Fn() -> FT + Send + Sync + 'static,
        poll_semaphore: Arc<MeteredSemaphore>,
        max_pollers: usize,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
    {
        let (tx, rx) = unbounded_channel();
        let active_pollers = Arc::new(AtomicUsize::new(0));
        let join_handles = FuturesUnordered::new();
        let pf = Arc::new(poll_fn);
        let nph = num_pollers_handler.map(Arc::new);
        for _ in 0..max_pollers {
            let tx = tx.clone();
            let pf = pf.clone();
            let shutdown = shutdown.clone();
            let ap = active_pollers.clone();
            let poll_semaphore = poll_semaphore.clone();
            let nph = nph.clone();
            let jh = tokio::spawn(async move {
                let nph = nph.as_ref().map(|a| a.as_ref());
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let permit = tokio::select! {
                        p = poll_semaphore.acquire_owned() => p,
                        _ = shutdown.cancelled() => continue,
                    };
                    let permit = if let Ok(p) = permit {
                        p
                    } else {
                        break;
                    };
                    let _active_guard = ActiveCounter::new(ap.as_ref(), nph);
                    let r = tokio::select! {
                        r = pf() => r,
                        _ = shutdown.cancelled() => continue,
                    };
                    let _ = tx.send(r.map(|r| (r, permit)));
                }
            });
            join_handles.push(jh);
        }
        Self {
            buffered_polls: Mutex::new(rx),
            shutdown,
            join_handles,
            num_pollers_changed: None,
            active_pollers,
        }
    }
}

#[async_trait::async_trait]
impl<T> Poller<(T, OwnedMeteredSemPermit)> for LongPollBuffer<T>
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
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit)>> {
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
impl Poller<(PollWorkflowTaskQueueResponse, OwnedMeteredSemPermit)> for WorkflowTaskPoller {
    async fn poll(
        &self,
    ) -> Option<pollers::Result<(PollWorkflowTaskQueueResponse, OwnedMeteredSemPermit)>> {
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
    semaphore: Arc<MeteredSemaphore>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
) -> PollWorkflowTaskBuffer {
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_workflow_task(task_queue, is_sticky).await }
        },
        semaphore,
        concurrent_pollers,
        shutdown,
        num_pollers_handler,
    )
}

pub type PollActivityTaskBuffer = LongPollBuffer<PollActivityTaskQueueResponse>;
pub(crate) fn new_activity_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    concurrent_pollers: usize,
    semaphore: Arc<MeteredSemaphore>,
    max_tps: Option<f64>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
) -> PollActivityTaskBuffer {
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_activity_task(task_queue, max_tps).await }
        },
        semaphore,
        concurrent_pollers,
        shutdown,
        num_pollers_handler,
    )
}

#[cfg(test)]
#[derive(derive_more::Constructor)]
pub(crate) struct MockPermittedPollBuffer<PT> {
    sem: Arc<MeteredSemaphore>,
    inner: PT,
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T, PT> Poller<(T, OwnedMeteredSemPermit)> for MockPermittedPollBuffer<PT>
where
    T: Send + Sync + 'static,
    PT: Poller<T> + Send + Sync + 'static,
{
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit)>> {
        let p = self
            .sem
            .acquire_owned()
            .await
            .expect("Semaphore in poller not closed!");
        self.inner.poll().await.map(|r| r.map(|r| (r, p)))
    }

    fn notify_shutdown(&self) {
        self.inner.notify_shutdown();
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }

    async fn shutdown_box(self: Box<Self>) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        telemetry::metrics::MetricsContext, worker::client::mocks::mock_manual_workflow_client,
    };
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
            Arc::new(MeteredSemaphore::new(
                10,
                MetricsContext::no_op(),
                |_, _| {},
            )),
            CancellationToken::new(),
            None::<fn(usize)>,
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
