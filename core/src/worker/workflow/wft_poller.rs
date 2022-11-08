use crate::{pollers::BoxedWFPoller, protosext::ValidPollWFTQResponse, MetricsContext};
use futures::{stream, Stream};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

pub(crate) fn new_wft_poller(
    poller: BoxedWFPoller,
    metrics: MetricsContext,
) -> impl Stream<Item = Result<ValidPollWFTQResponse, tonic::Status>> {
    stream::unfold((poller, metrics), |(poller, metrics)| async move {
        loop {
            return match poller.poll().await {
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
                            error!(error=?e, "Server returned an unparseable workflow task");
                            continue;
                        }
                    };
                    metrics.wf_tq_poll_ok();
                    Some((Ok(work), (poller, metrics)))
                }
                Some(Err(e)) => {
                    warn!(error=?e, "Error while polling for workflow tasks");
                    Some((Err(e), (poller, metrics)))
                }
                // If poller returns None, it's dead, thus we also return None to terminate this
                // stream.
                None => None,
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
            format!(
                "Server returned a poll WFT response we couldn't interpret: {:?}",
                resp
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::mock_poller;
    use futures::{pin_mut, StreamExt};

    #[tokio::test]
    async fn poll_timeouts_do_not_produce_responses() {
        let mut mock_poller = mock_poller();
        mock_poller
            .expect_poll()
            .times(1)
            .returning(|| Some(Ok(PollWorkflowTaskQueueResponse::default())));
        mock_poller.expect_poll().times(1).returning(|| None);
        let stream = new_wft_poller(Box::new(mock_poller), MetricsContext::no_op());
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
        let stream = new_wft_poller(Box::new(mock_poller), MetricsContext::no_op());
        pin_mut!(stream);
        assert_matches!(stream.next().await, Some(Err(_)));
    }
}
