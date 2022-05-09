use crate::{pollers::BoxedWFPoller, protosext::ValidPollWFTQResponse, MetricsContext};
use futures::{stream, Stream};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

pub(crate) fn new_wft_poller(
    poller: BoxedWFPoller,
    metrics: MetricsContext,
) -> impl Stream<Item = ValidPollWFTQResponse> {
    stream::unfold((poller, metrics), |(poller, metrics)| async move {
        loop {
            match poller.poll().await {
                Some(Ok(wft)) => {
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
                            warn!(error=?e, "Server returned an unparseable workflow task");
                            continue;
                        }
                    };
                    return Some((work, (poller, metrics)));
                }
                Some(Err(e)) => {
                    warn!(error=?e, "Error polling for workflow tasks");
                }
                None => return None,
            }
        }
    })
}

pub(crate) fn validate_wft(
    wft: PollWorkflowTaskQueueResponse,
) -> Result<ValidPollWFTQResponse, tonic::Status> {
    wft.try_into().map_err(|resp| {
        tonic::Status::new(
            tonic::Code::DataLoss,
            format!(
                "Server returned a poll WFT response we couldn't interpret: {:?}",
                resp
            ),
        )
    })
}
