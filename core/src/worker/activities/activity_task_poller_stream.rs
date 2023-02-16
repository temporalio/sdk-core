use crate::abstractions::MeteredSemaphore;
use crate::worker::activities::PermittedTqResp;
use crate::{pollers::BoxedActPoller, MetricsContext};
use futures::{stream, Stream};
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;
use std::sync::Arc;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;
use tokio::select;
use tokio_util::sync::CancellationToken;

struct StreamState {
    poller: BoxedActPoller,
    semaphore: Arc<MeteredSemaphore>,
    rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
    poller_was_shutdown: bool,
}

pub(crate) fn new_activity_task_poller(
    poller: BoxedActPoller,
    semaphore: Arc<MeteredSemaphore>,
    rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = Result<PermittedTqResp, tonic::Status>> {
    let state = StreamState {
        poller,
        semaphore,
        rate_limiter,
        metrics,
        shutdown_token,
        poller_was_shutdown: false,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            let poll = async {
                let permit = state
                    .semaphore
                    .acquire_owned()
                    .await
                    .expect("outstanding activity semaphore not closed");
                if !state.poller_was_shutdown {
                    if let Some(ref rl) = state.rate_limiter {
                        rl.until_ready().await;
                    }
                }
                loop {
                    return match state.poller.poll().await {
                        Some(Ok(resp)) => {
                            if resp == PollActivityTaskQueueResponse::default() {
                                // We get the default proto in the event that the long poll times out.
                                debug!("Poll activity task timeout");
                                state.metrics.act_poll_timeout();
                                continue;
                            }
                            Some(Ok(PermittedTqResp { permit, resp }))
                        }
                        Some(Err(e)) => {
                            warn!(error=?e, "Error while polling for activity tasks");
                            Some(Err(e))
                        }
                        // If poller returns None, it's dead, thus we also return None to terminate this
                        // stream.
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
