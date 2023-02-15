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

pub(crate) fn new_activity_task_poller(
    poller: BoxedActPoller,
    semaphore: Arc<MeteredSemaphore>,
    rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = Result<PermittedTqResp, tonic::Status>> {
    stream::unfold(
        (
            poller,
            semaphore,
            rate_limiter,
            metrics,
            shutdown_token,
            false,
        ),
        |(poller, semaphore, ratelimiter, metrics, shutdown_token, mut poller_was_shutdown)| async move {
            loop {
                let poll = async {
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .expect("outstanding activity semaphore not closed");
                    if !poller_was_shutdown {
                        if let Some(ref rl) = ratelimiter {
                            rl.until_ready().await;
                        }
                    }
                    loop {
                        return match poller.poll().await {
                            Some(Ok(resp)) => {
                                if resp == PollActivityTaskQueueResponse::default() {
                                    // We get the default proto in the event that the long poll times out.
                                    debug!("Poll activity task timeout");
                                    metrics.act_poll_timeout();
                                    continue;
                                }
                                Some(Ok(PermittedTqResp { permit, resp }))
                            }
                            Some(Err(e)) => {
                                #[cfg(test)]
                                if e.code() == tonic::Code::Cancelled
                                    && e.message() == "No more work to do"
                                {
                                    // Need to work around mock poller and abort here.
                                    return None;
                                }
                                warn!(error=?e, "Error while polling for activity tasks");
                                Some(Err(e))
                            }
                            // If poller returns None, it's dead, thus we also return None to terminate this
                            // stream.
                            None => None,
                        };
                    }
                };
                if poller_was_shutdown {
                    return poll.await.map(|res| {
                        (
                            res,
                            (
                                poller,
                                semaphore,
                                ratelimiter,
                                metrics,
                                shutdown_token,
                                poller_was_shutdown,
                            ),
                        )
                    });
                }
                select! {
                    _ = shutdown_token.cancelled() => {
                        poller.notify_shutdown();
                        poller_was_shutdown = true;
                        continue;
                    }
                    res = poll => {
                        return res.map(|res| (res, (poller, semaphore, ratelimiter, metrics, shutdown_token, poller_was_shutdown)));
                    }
                }
            }
        },
    )
}
