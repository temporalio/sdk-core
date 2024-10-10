mod poll_buffer;

pub(crate) use poll_buffer::{
    new_activity_task_buffer, new_workflow_task_buffer, WorkflowTaskPoller,
};
pub use temporal_client::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};

use crate::abstractions::OwnedMeteredSemPermit;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
};

#[cfg(test)]
use futures_util::Future;
#[cfg(test)]
pub(crate) use poll_buffer::MockPermittedPollBuffer;
use temporal_sdk_core_api::worker::{ActivitySlotKind, WorkflowSlotKind};

pub(crate) type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// A trait for things that poll the server. Hides complexity of concurrent polling or polling
/// on sticky/nonsticky queues simultaneously.
#[cfg_attr(test, mockall::automock)]
#[cfg_attr(test, allow(unused))]
#[async_trait::async_trait]
pub(crate) trait Poller<PollResult>
where
    PollResult: Send + Sync + 'static,
{
    async fn poll(&self) -> Option<Result<PollResult>>;
    fn notify_shutdown(&self);
    async fn shutdown(self);
    /// Need a separate shutdown to be able to consume boxes :(
    async fn shutdown_box(self: Box<Self>);
}
pub(crate) type BoxedPoller<T> = Box<dyn Poller<T> + Send + Sync + 'static>;
pub(crate) type BoxedWFPoller = BoxedPoller<(
    PollWorkflowTaskQueueResponse,
    OwnedMeteredSemPermit<WorkflowSlotKind>,
)>;
pub(crate) type BoxedActPoller = BoxedPoller<(
    PollActivityTaskQueueResponse,
    OwnedMeteredSemPermit<ActivitySlotKind>,
)>;

#[async_trait::async_trait]
impl<T> Poller<T> for Box<dyn Poller<T> + Send + Sync>
where
    T: Send + Sync + 'static,
{
    async fn poll(&self) -> Option<Result<T>> {
        Poller::poll(self.as_ref()).await
    }

    fn notify_shutdown(&self) {
        Poller::notify_shutdown(self.as_ref())
    }

    async fn shutdown(self) {
        Poller::shutdown(self).await
    }

    async fn shutdown_box(self: Box<Self>) {
        Poller::shutdown_box(self).await
    }
}

#[cfg(test)]
mockall::mock! {
    pub ManualPoller<T: Send + Sync + 'static> {}
    #[allow(unused)]
    impl<T: Send + Sync + 'static> Poller<T> for ManualPoller<T> {
        fn poll<'a, 'b>(&self)
          -> impl Future<Output = Option<Result<T>>> + Send + 'b
            where 'a: 'b, Self: 'b;
        fn notify_shutdown(&self);
        fn shutdown<'a>(self)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
        fn shutdown_box<'a>(self: Box<Self>)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
    }
}
