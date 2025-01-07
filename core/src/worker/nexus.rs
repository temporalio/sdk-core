use crate::{
    abstractions::UsedMeteredSemPermit,
    pollers::{new_nexus_task_poller, BoxedNexusPoller, NexusPollItem},
    telemetry::metrics::MetricsContext,
};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use std::collections::HashMap;
use temporal_sdk_core_api::{errors::PollActivityError, worker::NexusSlotKind};
use temporal_sdk_core_protos::{
    coresdk::NexusSlotInfo,
    temporal::api::{nexus::v1::request::Variant, workflowservice::v1::PollNexusTaskQueueResponse},
    TaskToken,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Centralizes all state related to received nexus tasks
pub(super) struct NexusManager {
    task_stream: Mutex<BoxStream<'static, Result<PollNexusTaskQueueResponse, PollActivityError>>>,
    /// Token to notify when poll returned a shutdown error
    poll_returned_shutdown_token: CancellationToken,
}

impl NexusManager {
    pub(super) fn new(
        poller: BoxedNexusPoller,
        metrics: MetricsContext,
        shutdown_initiated_token: CancellationToken,
    ) -> Self {
        let source_stream = new_nexus_task_poller(poller, metrics, shutdown_initiated_token);
        let task_stream = NexusTaskStream::new(source_stream);
        Self {
            task_stream: Mutex::new(task_stream.into_stream().boxed()),
            poll_returned_shutdown_token: CancellationToken::new(),
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
}

struct NexusTaskStream<S> {
    source_stream: S,
    outstanding_task_map: HashMap<TaskToken, NexusInFlightTask>,
}

struct NexusInFlightTask {
    _permit: UsedMeteredSemPermit<NexusSlotKind>,
}

impl<S> NexusTaskStream<S>
where
    S: Stream<Item = NexusPollItem>,
{
    fn new(source: S) -> Self {
        Self {
            source_stream: source,
            outstanding_task_map: HashMap::new(),
        }
    }

    fn into_stream(
        mut self,
    ) -> impl Stream<Item = Result<PollNexusTaskQueueResponse, PollActivityError>> {
        self.source_stream.map(move |t| match t {
            Ok(t) => {
                let (service, operation) = t
                    .resp
                    .request
                    .as_ref()
                    .and_then(|r| r.variant.as_ref())
                    .map(|v| match v {
                        Variant::StartOperation(s) => {
                            (s.service.to_owned(), s.operation.to_owned())
                        }
                        Variant::CancelOperation(c) => {
                            (c.service.to_owned(), c.operation.to_owned())
                        }
                    })
                    .unwrap_or_default();
                self.outstanding_task_map.insert(
                    TaskToken(t.resp.task_token.clone()),
                    NexusInFlightTask {
                        _permit: t.permit.into_used(NexusSlotInfo { service, operation }),
                    },
                );
                Ok(t.resp)
            }
            Err(e) => Err(PollActivityError::TonicError(e)),
        })
    }
}
