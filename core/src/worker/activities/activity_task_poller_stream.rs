use crate::{pollers::BoxedActPoller, worker::activities::PermittedTqResp, MetricsContext};
use futures_util::{stream, Stream};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;
use tokio::select;
use tokio_util::sync::CancellationToken;

struct StreamState {
    poller: BoxedActPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
    poller_was_shutdown: bool,
}

pub(crate) fn new_activity_task_poller(
    poller: BoxedActPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = Result<PermittedTqResp, tonic::Status>> {
    let state = StreamState {
        poller,
        metrics,
        shutdown_token,
        poller_was_shutdown: false,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            let poll = async {
                loop {
                    return match state.poller.poll().await {
                        Some(Ok((resp, permit))) => {
                            if resp == PollActivityTaskQueueResponse::default() {
                                // We get the default proto in the event that the long poll times
                                // out.
                                debug!("Poll activity task timeout");
                                state.metrics.act_poll_timeout();
                                continue;
                            }
                            if let Some(reason) = validate_activity_task(&resp) {
                                warn!("Received invalid activity task ({}): {:?}", reason, &resp);
                                continue;
                            }
                            Some(Ok(PermittedTqResp { permit, resp }))
                        }
                        Some(Err(e)) => {
                            warn!(error=?e, "Error while polling for activity tasks");
                            Some(Err(e))
                        }
                        // If poller returns None, it's dead, thus we also return None to terminate
                        // this stream.
                        None => None,
                    };
                }
            };
            if state.poller_was_shutdown {
                return poll.await.map(|res| (res, state));
            }
            select! {
                biased;

                _ = state.shutdown_token.cancelled() => {
                    state.poller.notify_shutdown();
                    state.poller_was_shutdown = true;
                    continue;
                }
                res = poll => {
                    return res.map(|res| (res, state));
                }
            }
        }
    })
}

fn validate_activity_task(task: &PollActivityTaskQueueResponse) -> Option<&'static str> {
    if task.task_token.is_empty() {
        return Some("missing task token");
    }
    None
}
