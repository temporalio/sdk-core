mod poll_buffer;

pub(crate) use poll_buffer::{
    new_activity_task_buffer, new_workflow_task_buffer, WorkflowTaskPoller,
};
pub use temporal_client::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
};

#[cfg(test)]
use futures::Future;

pub type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// A trait for things that poll the server. Hides complexity of concurrent polling or polling
/// on sticky/nonsticky queues simultaneously.
#[cfg_attr(test, mockall::automock)]
#[cfg_attr(test, allow(unused))]
#[async_trait::async_trait]
pub trait Poller<PollResult>
where
    PollResult: Send + Sync + 'static,
{
    async fn poll(&self) -> Option<Result<PollResult>>;
    fn notify_shutdown(&self);
    async fn shutdown(self);
    /// Need a separate shutdown to be able to consume boxes :(
    async fn shutdown_box(self: Box<Self>);
}
pub type BoxedPoller<T> = Box<dyn Poller<T> + Send + Sync + 'static>;
pub type BoxedWFPoller = BoxedPoller<PollWorkflowTaskQueueResponse>;
pub type BoxedActPoller = BoxedPoller<PollActivityTaskQueueResponse>;

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
