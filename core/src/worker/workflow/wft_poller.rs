use crate::{
    MetricsContext,
    abstractions::{MeteredPermitDealer, OwnedMeteredSemPermit},
    pollers::{BoxedWFPoller, LongPollBuffer, Poller, WorkflowTaskOptions, WorkflowTaskPoller},
    protosext::ValidPollWFTQResponse,
    telemetry::metrics::{workflow_poller, workflow_sticky_poller},
    worker::{client::WorkerClient, wft_poller_behavior},
};
use futures_util::{Stream, stream};
use std::sync::{Arc, OnceLock};
use temporal_sdk_core_api::worker::{WorkerConfig, WorkflowSlotKind};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub(crate) fn make_wft_poller(
    config: &WorkerConfig,
    sticky_queue_name: &Option<String>,
    client: &Arc<dyn WorkerClient>,
    metrics: &MetricsContext,
    shutdown_token: &CancellationToken,
    wft_slots: &MeteredPermitDealer<WorkflowSlotKind>,
) -> impl Stream<
    Item = Result<
        (
            ValidPollWFTQResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        ),
        tonic::Status,
    >,
> + Sized
+ 'static {
    let wft_metrics = metrics.with_new_attrs([workflow_poller()]);
    let wft_poller_shared = Arc::new(WFTPollerShared::new());
    let wf_task_poll_buffer = LongPollBuffer::new_workflow_task(
        client.clone(),
        config.task_queue.clone(),
        None,
        wft_poller_behavior(config, false),
        wft_slots.clone(),
        shutdown_token.child_token(),
        Some(move |np| {
            wft_metrics.record_num_pollers(np);
        }),
        WorkflowTaskOptions {
            wft_poller_shared: wft_poller_shared.clone(),
        },
    );
    let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
        let sticky_metrics = metrics.with_new_attrs([workflow_sticky_poller()]);
        LongPollBuffer::new_workflow_task(
            client.clone(),
            config.task_queue.clone(),
            Some(sqn.clone()),
            wft_poller_behavior(config, true),
            wft_slots.clone().into_sticky(),
            shutdown_token.child_token(),
            Some(move |np| {
                sticky_metrics.record_num_pollers(np);
            }),
            WorkflowTaskOptions { wft_poller_shared },
        )
    });
    let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
        wf_task_poll_buffer,
        sticky_queue_poller,
    ));
    new_wft_poller(wf_task_poll_buffer, metrics.clone())
}

/// Info that needs to be shared across the sticky and non-sticky wft pollers to prioritize sticky
/// when appropriate
pub(crate) struct WFTPollerShared {
    last_seen_sticky_backlog: (watch::Receiver<usize>, watch::Sender<usize>),
    sticky_active: OnceLock<watch::Receiver<usize>>,
    non_sticky_active: OnceLock<watch::Receiver<usize>>,
}
impl WFTPollerShared {
    pub(crate) fn new() -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            last_seen_sticky_backlog: (rx, tx),
            sticky_active: OnceLock::new(),
            non_sticky_active: OnceLock::new(),
        }
    }
    pub(crate) fn set_sticky_active(&self, rx: watch::Receiver<usize>) {
        let _ = self.sticky_active.set(rx);
    }
    pub(crate) fn set_non_sticky_active(&self, rx: watch::Receiver<usize>) {
        let _ = self.non_sticky_active.set(rx);
    }
    /// Makes either the sticky or non-sticky poller wait pre-permit-acquisition so that we can
    /// balance which kind of queue we poll appropriately.
    pub(crate) async fn wait_if_needed(&self, is_sticky: bool) {
        // If there's a sticky backlog, prioritize it.
        if !is_sticky {
            let backlog = *self.last_seen_sticky_backlog.0.borrow();
            if backlog > 1 {
                let _ = self
                    .last_seen_sticky_backlog
                    .0
                    .clone()
                    .wait_for(|v| *v <= 1)
                    .await;
            }
        }

        // If there's no meaningful sticky backlog, balance poller counts
        if *self.last_seen_sticky_backlog.0.borrow() <= 1 {
            if let Some((sticky_active, non_sticky_active)) =
                self.sticky_active.get().zip(self.non_sticky_active.get())
            {
                if is_sticky {
                    let _ = sticky_active
                        .clone()
                        .wait_for(|v| *v <= *non_sticky_active.borrow())
                        .await;
                } else {
                    let _ = non_sticky_active
                        .clone()
                        .wait_for(|v| *v <= *sticky_active.borrow())
                        .await;
                }
            }
        }
    }
    pub(crate) fn record_sticky_backlog(&self, v: usize) {
        let _ = self.last_seen_sticky_backlog.1.send(v);
    }
}

fn new_wft_poller(
    poller: BoxedWFPoller,
    metrics: MetricsContext,
) -> impl Stream<
    Item = Result<
        (
            ValidPollWFTQResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        ),
        tonic::Status,
    >,
> {
    stream::unfold((poller, metrics), |(poller, metrics)| async move {
        loop {
            return match poller.poll().await {
                Some(Ok((wft, permit))) => {
                    if wft == PollWorkflowTaskQueueResponse::default() {
                        // We get the default proto in the event that the long poll times out.
                        debug!("Poll wft timeout");
                        metrics.wf_tq_poll_empty();
                        continue;
                    }
                    if let Some(dur) = wft.sched_to_start() {
                        metrics.wf_task_sched_to_start_latency(dur);
                    }
                    let work = match validate_wft(wft) {
                        Ok(w) => w,
                        Err(e) => {
                            error!(error=?e, "Server returned an unparseable workflow task");
                            continue;
                        }
                    };
                    metrics.wf_tq_poll_ok();
                    Some((Ok((work, permit)), (poller, metrics)))
                }
                Some(Err(e)) => {
                    warn!(error=?e, "Error while polling for workflow tasks");
                    Some((Err(e), (poller, metrics)))
                }
                // If poller returns None, it's dead, thus we also return None to terminate this
                // stream.
                None => {
                    // Make sure we call the actual shutdown function here to propagate any panics
                    // inside the polling tasks as errors.
                    poller.shutdown_box().await;
                    None
                }
            };
        }
    })
}

pub(crate) fn validate_wft(
    wft: PollWorkflowTaskQueueResponse,
) -> Result<ValidPollWFTQResponse, tonic::Status> {
    wft.try_into().map_err(|resp| {
        tonic::Status::new(
            tonic::Code::DataLoss,
            format!("Server returned a poll WFT response we couldn't interpret: {resp:?}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer, pollers::MockPermittedPollBuffer,
        test_help::mock_poller,
    };
    use futures_util::{StreamExt, pin_mut};
    use std::sync::Arc;
    use temporal_sdk_core_api::worker::WorkflowSlotKind;

    #[tokio::test]
    async fn poll_timeouts_do_not_produce_responses() {
        let mut mock_poller = mock_poller();
        mock_poller
            .expect_poll()
            .times(1)
            .returning(|| Some(Ok(PollWorkflowTaskQueueResponse::default())));
        mock_poller.expect_poll().times(1).returning(|| None);
        mock_poller.expect_shutdown().times(1).returning(|| ());
        let sem = Arc::new(fixed_size_permit_dealer::<WorkflowSlotKind>(10));
        let stream = new_wft_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
        );
        pin_mut!(stream);
        assert_matches!(stream.next().await, None);
    }

    #[tokio::test]
    async fn poll_errors_do_produce_responses() {
        let mut mock_poller = mock_poller();
        mock_poller
            .expect_poll()
            .times(1)
            .returning(|| Some(Err(tonic::Status::internal("ahhh"))));
        let sem = Arc::new(fixed_size_permit_dealer::<WorkflowSlotKind>(10));
        let stream = new_wft_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
        );
        pin_mut!(stream);
        assert_matches!(stream.next().await, Some(Err(_)));
    }
}
