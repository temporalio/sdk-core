use crate::{
    abstractions::{dbg_panic, MeteredSemaphore, OwnedMeteredSemPermit},
    pollers::{self, Poller},
    worker::client::WorkerClient,
};
use futures::{prelude::stream::FuturesUnordered, StreamExt};
use futures_util::{future::BoxFuture, FutureExt};
use governor::{Quota, RateLimiter};
use std::{
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_protos::temporal::api::{
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
};
use tokio::{
    sync::{
        broadcast,
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
    /// Pollers won't actually start polling until initialized & value is sent
    starter: broadcast::Sender<()>,
    did_start: AtomicBool,
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
    pub(crate) fn new<FT, DelayFut>(
        poll_fn: impl Fn() -> FT + Send + Sync + 'static,
        poll_semaphore: Arc<MeteredSemaphore>,
        max_pollers: usize,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
        pre_permit_delay: Option<impl Fn() -> DelayFut + Send + Sync + 'static>,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
        DelayFut: Future<Output = ()> + Send,
    {
        let (tx, rx) = unbounded_channel();
        let (starter, wait_for_start) = broadcast::channel(1);
        let active_pollers = Arc::new(AtomicUsize::new(0));
        let join_handles = FuturesUnordered::new();
        let pf = Arc::new(poll_fn);
        let nph = num_pollers_handler.map(Arc::new);
        let pre_permit_delay = pre_permit_delay.map(Arc::new);
        for _ in 0..max_pollers {
            let tx = tx.clone();
            let pf = pf.clone();
            let shutdown = shutdown.clone();
            let ap = active_pollers.clone();
            let poll_semaphore = poll_semaphore.clone();
            let nph = nph.clone();
            let pre_permit_delay = pre_permit_delay.clone();
            let mut wait_for_start = wait_for_start.resubscribe();
            let jh = tokio::spawn(async move {
                tokio::select! {
                    _ = wait_for_start.recv() => (),
                    _ = shutdown.cancelled() => return,
                }
                drop(wait_for_start);

                let nph = nph.as_ref().map(|a| a.as_ref());
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    if let Some(ref ppd) = pre_permit_delay {
                        tokio::select! {
                            _ = ppd() => (),
                            _ = shutdown.cancelled() => break,
                        }
                    }
                    let permit = tokio::select! {
                        p = poll_semaphore.acquire_owned() => p,
                        _ = shutdown.cancelled() => break,
                    };
                    let permit = if let Ok(p) = permit {
                        p
                    } else {
                        break;
                    };
                    let _active_guard = ActiveCounter::new(ap.as_ref(), nph);
                    let r = tokio::select! {
                        r = pf() => r,
                        _ = shutdown.cancelled() => break,
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
            starter,
            did_start: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<T> Poller<(T, OwnedMeteredSemPermit)> for LongPollBuffer<T>
where
    T: Send + Sync + Debug + 'static,
{
    /// Poll for the next item from this poller
    ///
    /// Returns `None` if the poller has been shut down
    #[instrument(name = "long_poll", level = "trace", skip(self))]
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit)>> {
        if !self.did_start.fetch_or(true, Ordering::Relaxed) {
            let _ = self.starter.send(());
        }

        let mut locked = self.buffered_polls.lock().await;
        (*locked).recv().await
    }

    fn notify_shutdown(&self) {
        self.shutdown.cancel();
    }

    async fn shutdown(mut self) {
        self.notify_shutdown();
        while let Some(jh) = self.join_handles.next().await {
            if let Err(e) = jh {
                if !e.is_cancelled() {
                    dbg_panic!("Poller task did not terminate cleanly: {:?}", e);
                }
            }
        }
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
    task_queue: TaskQueue,
    concurrent_pollers: usize,
    semaphore: Arc<MeteredSemaphore>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
) -> PollWorkflowTaskBuffer {
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_workflow_task(task_queue).await }
        },
        semaphore,
        concurrent_pollers,
        shutdown,
        num_pollers_handler,
        None::<fn() -> BoxFuture<'static, ()>>,
    )
}

pub type PollActivityTaskBuffer = LongPollBuffer<PollActivityTaskQueueResponse>;
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_activity_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    concurrent_pollers: usize,
    semaphore: Arc<MeteredSemaphore>,
    max_tps: Option<f64>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
    max_worker_acts_per_sec: Option<f64>,
) -> PollActivityTaskBuffer {
    let rate_limiter = max_worker_acts_per_sec.and_then(|ps| {
        Quota::with_period(Duration::from_secs_f64(ps.recip()))
            .map(|q| Arc::new(RateLimiter::direct(q)))
    });
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
        rate_limiter.map(|rl| {
            move || {
                let rl = rl.clone();
                async move { rl.until_ready().await }.boxed()
            }
        }),
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
    use temporal_sdk_core_protos::temporal::api::enums::v1::TaskQueueKind;
    use tokio::{select, sync::mpsc::channel};

    #[tokio::test]
    async fn only_polls_once_with_1_poller() {
        let mut mock_client = mock_manual_workflow_client();
        mock_client
            .expect_poll_workflow_task()
            .times(2)
            .returning(move |_| {
                async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(Default::default())
                }
                .boxed()
            });

        let pb = new_workflow_task_buffer(
            Arc::new(mock_client),
            TaskQueue {
                name: "sometq".to_string(),
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            },
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
