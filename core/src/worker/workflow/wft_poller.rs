use crate::{pollers::BoxedWFPoller, protosext::ValidPollWFTQResponse, MetricsContext};
use futures::Stream;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::{sync::mpsc::unbounded_channel, task};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(crate) fn new_wft_poller(
    poller: BoxedWFPoller,
    metrics: MetricsContext,
) -> impl Stream<Item = ValidPollWFTQResponse> {
    let (new_wft_tx, new_wft_rx) = unbounded_channel();
    task::spawn(async move {
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
                    let work = validate_wft(wft)?;
                    new_wft_tx
                        .send(work)
                        .expect("Rcv half of new wft channel not dropped");
                }
                Some(Err(e)) => {
                    warn!(error=?e, "Error polling for workflow tasks");
                }
                None => {
                    unimplemented!("Handle shutdown");
                }
            }
            // TODO: Error/panic/shutdown handling, wait for request
        }
        Ok::<_, tonic::Status>(())
    });
    UnboundedReceiverStream::new(new_wft_rx)
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
