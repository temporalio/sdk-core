use crate::{
    abstractions::{MeteredPermitDealer, OwnedMeteredSemPermit, dbg_panic},
    pollers::{self, Poller},
    worker::client::WorkerClient,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use governor::{Quota, RateLimiter};
use std::{
    cmp,
    fmt::Debug,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporal_client::NoRetryOnMatching;
use temporal_sdk_core_api::worker::{
    ActivitySlotKind, NexusSlotKind, PollerBehavior, SlotKind, WorkflowSlotKind,
};
use temporal_sdk_core_protos::temporal::api::{
    sdk::v1::PollerScalingDecision,
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{
        PollActivityTaskQueueResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
    },
};
use tokio::{
    sync::{
        Mutex, broadcast,
        mpsc::{UnboundedReceiver, unbounded_channel},
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tracing::Instrument;

type PollReceiver<T, SK> =
    Mutex<UnboundedReceiver<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>>>;
pub(crate) struct LongPollBuffer<T, SK: SlotKind> {
    buffered_polls: PollReceiver<T, SK>,
    shutdown: CancellationToken,
    poller_task: JoinHandle<()>,
    /// Pollers won't actually start polling until initialized & value is sent
    starter: broadcast::Sender<()>,
    did_start: AtomicBool,
}

impl<T, SK> LongPollBuffer<T, SK>
where
    T: TaskPollerResult + Send + Debug + 'static,
    SK: SlotKind + 'static,
{
    pub(crate) fn new<FT, DelayFut>(
        poll_fn: impl Fn() -> FT + Send + Sync + 'static,
        permit_dealer: MeteredPermitDealer<SK>,
        poller_behavior: PollerBehavior,
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
        let pf = Arc::new(poll_fn);
        let pre_permit_delay = pre_permit_delay.map(Arc::new);
        let mut wait_for_start = wait_for_start.resubscribe();
        let shutdown_clone = shutdown.clone();
        let mut poll_scaler = PollScaler::new(poller_behavior, num_pollers_handler);
        let poller_task = tokio::spawn(
            async move {
                tokio::select! {
                    _ = wait_for_start.recv() => (),
                    _ = shutdown_clone.cancelled() => return,
                }
                drop(wait_for_start);

                let (spawned_tx, spawned_rx) = unbounded_channel();
                let poll_task_awaiter = tokio::spawn(async move {
                    UnboundedReceiverStream::new(spawned_rx)
                        .for_each_concurrent(None, |t| async move {
                            handle_task_panic(t).await;
                        })
                        .await;
                });
                loop {
                    if shutdown_clone.is_cancelled() {
                        break;
                    }
                    if let Some(ref ppd) = pre_permit_delay {
                        tokio::select! {
                            _ = ppd() => (),
                            _ = shutdown_clone.cancelled() => break,
                        }
                    }
                    // We wait until below max pollers before even attempting to acquire a permit.
                    // This is to avoid sticky/non-sticky starving each other for WFT pollers.
                    // TODO: See if this can be made to go away after other changes.
                    tokio::select! {
                        _ = poll_scaler.wait_until_below_max() => (),
                        _ = shutdown_clone.cancelled() => break,
                    }
                    let permit = tokio::select! {
                        p = permit_dealer.acquire_owned() => p,
                        _ = shutdown_clone.cancelled() => break,
                    };
                    let active_guard = tokio::select! {
                        ag = poll_scaler.wait_until_allowed() => ag,
                        _ = shutdown_clone.cancelled() => break,
                    };
                    // Spawn poll task
                    let shutdown = shutdown_clone.clone();
                    let pf = pf.clone();
                    let tx = tx.clone();
                    let report_handle = poll_scaler.get_report_handle();
                    let poll_task = tokio::spawn(async move {
                        let r = tokio::select! {
                            r = pf() => r,
                            _ = shutdown.cancelled() => return,
                        };
                        drop(active_guard);
                        report_handle.poll_result(&r);
                        let _ = tx.send(r.map(|r| (r, permit)));
                    });
                    let _ = spawned_tx.send(poll_task);
                }
                drop(spawned_tx);
                poll_task_awaiter.await.unwrap();
            }
            .instrument(info_span!("polling_task").or_current()),
        );
        Self {
            buffered_polls: Mutex::new(rx),
            shutdown,
            poller_task,
            starter,
            did_start: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<T, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for LongPollBuffer<T, SK>
where
    T: Send + Sync + Debug + 'static,
    SK: SlotKind + 'static,
{
    /// Poll for the next item from this poller
    ///
    /// Returns `None` if the poller has been shut down
    #[instrument(name = "long_poll", level = "trace", skip(self))]
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>> {
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
        handle_task_panic(self.poller_task).await;
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
    }
}

async fn handle_task_panic(t: JoinHandle<()>) {
    if let Err(e) = t.await {
        if e.is_panic() {
            let as_panic = e.into_panic().downcast::<String>();
            dbg_panic!(
                "Poller task died or did not terminate cleanly: {:?}",
                as_panic
            );
        }
    }
}

struct ActiveCounter<F: Fn(usize)>(watch::Sender<usize>, Option<Arc<F>>);
impl<F> ActiveCounter<F>
where
    F: Fn(usize),
{
    fn new(a: watch::Sender<usize>, change_fn: Option<Arc<F>>) -> Self {
        a.send_modify(|v| {
            *v += 1;
            if let Some(cfn) = change_fn.as_ref() {
                cfn(*v);
            }
        });
        Self(a, change_fn)
    }
}
impl<F> Drop for ActiveCounter<F>
where
    F: Fn(usize),
{
    fn drop(&mut self) {
        self.0.send_modify(|v| {
            *v -= 1;
            if let Some(cfn) = self.1.as_ref() {
                cfn(*v)
            };
        });
    }
}

struct PollScaler<F> {
    report_handle: Arc<PollScalerReportHandle>,
    active_tx: watch::Sender<usize>,
    active_rx: watch::Receiver<usize>,
    num_pollers_handler: Option<Arc<F>>,
}

impl<F> PollScaler<F>
where
    F: Fn(usize),
{
    fn new(behavior: PollerBehavior, num_pollers_handler: Option<F>) -> Self {
        let (active_tx, active_rx) = watch::channel(0);
        let num_pollers_handler = num_pollers_handler.map(Arc::new);
        let (min, max, target) = match behavior {
            PollerBehavior::SimpleMaximum(m) => (1, m, m),
            PollerBehavior::Autoscaling {
                minimum,
                maximum,
                initial,
            } => (minimum, maximum, initial),
        };
        let report_handle = Arc::new(PollScalerReportHandle {
            max,
            min,
            target: AtomicUsize::new(target),
            ever_saw_scaling_decision: AtomicBool::default(),
            behavior,
        });
        Self {
            report_handle,
            active_tx,
            active_rx,
            num_pollers_handler,
        }
    }
    async fn wait_until_below_max(&mut self) {
        self.active_rx
            .wait_for(|v| {
                *v < self.report_handle.max
                    && *v < self.report_handle.target.load(Ordering::Relaxed)
            })
            .await
            .expect("Poll allow does not panic");
    }
    async fn wait_until_allowed(&mut self) -> ActiveCounter<impl Fn(usize)> {
        self.active_rx
            .wait_for(|v| {
                *v < self.report_handle.max
                    && *v < self.report_handle.target.load(Ordering::Relaxed)
            })
            .await
            .expect("Poll allow does not panic");
        ActiveCounter::new(self.active_tx.clone(), self.num_pollers_handler.clone())
    }
    fn get_report_handle(&self) -> Arc<PollScalerReportHandle> {
        self.report_handle.clone()
    }
}

struct PollScalerReportHandle {
    max: usize,
    min: usize,
    target: AtomicUsize,
    ever_saw_scaling_decision: AtomicBool,
    behavior: PollerBehavior,
}

impl PollScalerReportHandle {
    fn poll_result(&self, res: &Result<impl TaskPollerResult, tonic::Status>) {
        match res {
            Ok(res) => {
                if let PollerBehavior::SimpleMaximum(_) = self.behavior {
                    // We don't do auto-scaling with the simple max
                    return;
                }
                if let Some(scaling_decision) = res.scaling_decision() {
                    warn!("Got sd {:?}", scaling_decision);
                    match scaling_decision.poller_delta.cmp(&0) {
                        cmp::Ordering::Less => self.change_target(
                            usize::saturating_sub,
                            scaling_decision.poller_delta.unsigned_abs() as usize,
                        ),
                        cmp::Ordering::Greater => self.change_target(
                            usize::saturating_add,
                            scaling_decision.poller_delta as usize,
                        ),
                        cmp::Ordering::Equal => {}
                    }
                    self.ever_saw_scaling_decision
                        .store(true, Ordering::Relaxed);
                }
                // We want to avoid scaling down on empty polls if the server has never made any scaling
                // decisions - otherwise we might never scale up again.
                else if self.ever_saw_scaling_decision.load(Ordering::Relaxed) && res.is_empty() {
                    self.change_target(usize::saturating_sub, 1);
                }
            }
            Err(e) => {
                // We should only see (and react to) errors in autoscaling mode
                if matches!(self.behavior, PollerBehavior::Autoscaling { .. }) {
                    // TODO: Make debug before merge
                    warn!("Got error from server: {:?}", e);
                    if e.code() == Code::ResourceExhausted {
                        // Scale down significantly for resource exhaustion
                        self.change_target(usize::saturating_div, 2);
                    } else {
                        // Other codes that would normally have made us back off briefly can
                        // reclaim this poller
                        self.change_target(usize::saturating_sub, 1);
                    }
                }
            }
        }
    }

    #[inline]
    fn change_target(&self, change: fn(usize, usize) -> usize, change_by: usize) {
        self.target
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(change(v, change_by).clamp(self.min, self.max))
            })
            .expect("Cannot fail because always returns Some");
    }
}

/// A poller capable of polling on a sticky and a nonsticky queue simultaneously for workflow tasks.
#[derive(derive_more::Constructor)]
pub(crate) struct WorkflowTaskPoller {
    normal_poller: PollWorkflowTaskBuffer,
    sticky_poller: Option<PollWorkflowTaskBuffer>,
}

#[async_trait::async_trait]
impl
    Poller<(
        PollWorkflowTaskQueueResponse,
        OwnedMeteredSemPermit<WorkflowSlotKind>,
    )> for WorkflowTaskPoller
{
    async fn poll(
        &self,
    ) -> Option<
        pollers::Result<(
            PollWorkflowTaskQueueResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        )>,
    > {
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

pub(crate) type PollWorkflowTaskBuffer =
    LongPollBuffer<PollWorkflowTaskQueueResponse, WorkflowSlotKind>;
pub(crate) fn new_workflow_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: TaskQueue,
    poller_behavior: PollerBehavior,
    permit_dealer: MeteredPermitDealer<WorkflowSlotKind>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
) -> PollWorkflowTaskBuffer {
    let no_poll_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
        Some(NoRetryOnMatching {
            predicate: poll_scaling_error_matcher,
        })
    } else {
        None
    };
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_workflow_task(task_queue, no_poll_retry).await }
        },
        permit_dealer,
        poller_behavior,
        shutdown,
        num_pollers_handler,
        None::<fn() -> BoxFuture<'static, ()>>,
    )
}

pub(crate) type PollActivityTaskBuffer =
    LongPollBuffer<PollActivityTaskQueueResponse, ActivitySlotKind>;
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_activity_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    poller_behavior: PollerBehavior,
    semaphore: MeteredPermitDealer<ActivitySlotKind>,
    max_tps: Option<f64>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
    max_worker_acts_per_sec: Option<f64>,
) -> PollActivityTaskBuffer {
    let rate_limiter = max_worker_acts_per_sec.and_then(|ps| {
        Quota::with_period(Duration::from_secs_f64(ps.recip()))
            .map(|q| Arc::new(RateLimiter::direct(q)))
    });
    let no_poll_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
        Some(NoRetryOnMatching {
            predicate: poll_scaling_error_matcher,
        })
    } else {
        None
    };
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move {
                client
                    .poll_activity_task(task_queue, max_tps, no_poll_retry)
                    .await
            }
        },
        semaphore,
        poller_behavior,
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

pub(crate) type PollNexusTaskBuffer = LongPollBuffer<PollNexusTaskQueueResponse, NexusSlotKind>;
pub(crate) fn new_nexus_task_buffer(
    client: Arc<dyn WorkerClient>,
    task_queue: String,
    poller_behavior: PollerBehavior,
    semaphore: MeteredPermitDealer<NexusSlotKind>,
    shutdown: CancellationToken,
    num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
) -> PollNexusTaskBuffer {
    let no_poll_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
        Some(NoRetryOnMatching {
            predicate: poll_scaling_error_matcher,
        })
    } else {
        None
    };
    LongPollBuffer::new(
        move || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move { client.poll_nexus_task(task_queue, no_poll_retry).await }
        },
        semaphore,
        poller_behavior,
        shutdown,
        num_pollers_handler,
        None::<fn() -> BoxFuture<'static, ()>>,
    )
}

/// Returns true for errors that the poller scaler wants to see
fn poll_scaling_error_matcher(err: &tonic::Status) -> bool {
    if matches!(
        err.code(),
        Code::ResourceExhausted | Code::Cancelled | Code::DeadlineExceeded
    ) {
        return true;
    }
    false
}

pub(crate) trait TaskPollerResult {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision>;
    /// Returns true if this is an empty poll response (IE: poll timeout)
    fn is_empty(&self) -> bool;
}
impl TaskPollerResult for PollWorkflowTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}
impl TaskPollerResult for PollActivityTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}
impl TaskPollerResult for PollNexusTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}

#[cfg(test)]
#[derive(derive_more::Constructor)]
pub(crate) struct MockPermittedPollBuffer<PT, SK: SlotKind> {
    sem: Arc<MeteredPermitDealer<SK>>,
    inner: PT,
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T, PT, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for MockPermittedPollBuffer<PT, SK>
where
    T: Send + Sync + 'static,
    PT: Poller<T> + Send + Sync + 'static,
    SK: SlotKind + 'static,
{
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>> {
        let p = self.sem.acquire_owned().await;
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
        abstractions::tests::fixed_size_permit_dealer,
        worker::client::mocks::mock_manual_workflow_client,
    };
    use futures_util::FutureExt;
    use std::time::Duration;
    use temporal_sdk_core_protos::temporal::api::enums::v1::TaskQueueKind;
    use tokio::select;

    #[tokio::test]
    async fn only_polls_once_with_1_poller() {
        let mut mock_client = mock_manual_workflow_client();
        mock_client
            .expect_poll_workflow_task()
            .times(2)
            .returning(move |_, _| {
                async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
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
            PollerBehavior::SimpleMaximum(1),
            fixed_size_permit_dealer(10),
            CancellationToken::new(),
            None::<fn(usize)>,
        );

        // Poll a bunch of times, "interrupting" it each time, we should only actually have polled
        // once since the poll takes a while
        let mut last_val = false;
        for _ in 0..10 {
            select! {
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
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
