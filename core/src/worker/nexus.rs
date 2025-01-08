use crate::{
    abstractions::UsedMeteredSemPermit,
    pollers::{new_nexus_task_poller, BoxedNexusPoller, NexusPollItem},
    telemetry::metrics::MetricsContext,
    worker::client::WorkerClient,
};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use std::{collections::HashMap, sync::Arc};
use temporal_sdk_core_api::{
    errors::{CompleteNexusError, PollActivityError},
    worker::NexusSlotKind,
};
use temporal_sdk_core_protos::{
    coresdk::{nexus::nexus_task_completion, NexusSlotInfo},
    temporal::api::{
        nexus::v1::{request::Variant, response},
        workflowservice::v1::PollNexusTaskQueueResponse,
    },
    TaskToken,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Centralizes all state related to received nexus tasks
pub(super) struct NexusManager {
    task_stream: Mutex<BoxStream<'static, Result<PollNexusTaskQueueResponse, PollActivityError>>>,
    /// Token to notify when poll returned a shutdown error
    poll_returned_shutdown_token: CancellationToken,
    /// Outstanding nexus tasks that have been issued to lang but not yet completed
    outstanding_task_map: OutstandingTaskMap,
}

impl NexusManager {
    pub(super) fn new(
        poller: BoxedNexusPoller,
        metrics: MetricsContext,
        shutdown_initiated_token: CancellationToken,
    ) -> Self {
        let source_stream = new_nexus_task_poller(poller, metrics, shutdown_initiated_token);
        let task_stream = NexusTaskStream::new(source_stream);
        let outstanding_task_map = task_stream.outstanding_task_map.clone();
        Self {
            task_stream: Mutex::new(task_stream.into_stream().boxed()),
            poll_returned_shutdown_token: CancellationToken::new(),
            outstanding_task_map,
        }
    }

    // TODO Different error or combine
    /// Block until then next nexus task is received from server
    pub(super) async fn next_nexus_task(
        &self,
    ) -> Result<PollNexusTaskQueueResponse, PollActivityError> {
        let mut sl = self.task_stream.lock().await;
        sl.next().await.unwrap_or_else(|| {
            self.poll_returned_shutdown_token.cancel();
            Err(PollActivityError::ShutDown)
        })
    }

    pub(super) async fn complete_task(
        &self,
        tt: TaskToken,
        status: nexus_task_completion::Status,
        client: &dyn WorkerClient,
    ) -> Result<(), CompleteNexusError> {
        if let Some(task_info) = self.outstanding_task_map.lock().remove(&tt) {
            let maybe_net_err = match status {
                nexus_task_completion::Status::Completed(c) => {
                    // Server doesn't provide obvious errors for this validation, so it's done
                    // here to make life easier for lang implementors.
                    match &c.variant {
                        Some(response::Variant::StartOperation(_)) => {
                            if task_info.request_kind != RequestKind::Start {
                                return Err(CompleteNexusError::MalformeNexusCompletion {
                                    reason: "Nexus request was StartOperation but response was not"
                                        .to_string(),
                                });
                            }
                        }
                        Some(response::Variant::CancelOperation(_)) => {
                            if task_info.request_kind != RequestKind::Cancel {
                                return Err(CompleteNexusError::MalformeNexusCompletion {
                                    reason:
                                        "Nexus request was CancelOperation but response was not"
                                            .to_string(),
                                });
                            }
                        }
                        None => {
                            return Err(CompleteNexusError::MalformeNexusCompletion {
                                reason: "Nexus completion must contain a status variant "
                                    .to_string(),
                            })
                        }
                    }
                    client.complete_nexus_task(tt, c).await.err()
                }
                nexus_task_completion::Status::Error(e) => {
                    client.fail_nexus_task(tt, e).await.err()
                }
            };
            if let Some(e) = maybe_net_err {
                warn!(
                    error=?e,
                    "Network error while completing Nexus task",
                );
            }
        } else {
            warn!(
                "Attempted to complete nexus task {} but we were not tracking it",
                &tt
            );
        }
        Ok(())
    }
}

struct NexusTaskStream<S> {
    source_stream: S,
    outstanding_task_map: OutstandingTaskMap,
}

impl<S> NexusTaskStream<S>
where
    S: Stream<Item = NexusPollItem>,
{
    fn new(source: S) -> Self {
        Self {
            source_stream: source,
            outstanding_task_map: Arc::new(Default::default()),
        }
    }

    fn into_stream(
        self,
    ) -> impl Stream<Item = Result<PollNexusTaskQueueResponse, PollActivityError>> {
        self.source_stream.map(move |t| match t {
            Ok(t) => {
                let (service, operation, request_kind) = t
                    .resp
                    .request
                    .as_ref()
                    .and_then(|r| r.variant.as_ref())
                    .map(|v| match v {
                        Variant::StartOperation(s) => (
                            s.service.to_owned(),
                            s.operation.to_owned(),
                            RequestKind::Start,
                        ),
                        Variant::CancelOperation(c) => (
                            c.service.to_owned(),
                            c.operation.to_owned(),
                            RequestKind::Cancel,
                        ),
                    })
                    .unwrap_or_default();
                self.outstanding_task_map.lock().insert(
                    TaskToken(t.resp.task_token.clone()),
                    NexusInFlightTask {
                        request_kind,
                        _permit: t.permit.into_used(NexusSlotInfo { service, operation }),
                    },
                );
                Ok(t.resp)
            }
            Err(e) => Err(PollActivityError::TonicError(e)),
        })
    }
}

type OutstandingTaskMap = Arc<parking_lot::Mutex<HashMap<TaskToken, NexusInFlightTask>>>;

struct NexusInFlightTask {
    request_kind: RequestKind,
    _permit: UsedMeteredSemPermit<NexusSlotKind>,
}

#[derive(Eq, PartialEq, Copy, Clone)]
enum RequestKind {
    Start,
    Cancel,
}
impl Default for RequestKind {
    fn default() -> Self {
        RequestKind::Start
    }
}
