pub mod errors;
pub mod worker;

use crate::{
    errors::{
        CompleteActivityError, CompleteWfError, PollActivityError, PollWfError,
        WorkerRegistrationError,
    },
    worker::WorkerConfig,
};
use log::Level;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporal_client::ServerGatewayApis;
use temporal_sdk_core_protos::coresdk::{
    activity_task::ActivityTask, workflow_activation::WorkflowActivation,
    workflow_completion::WorkflowActivationCompletion, ActivityHeartbeat, ActivityTaskCompletion,
};

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
#[async_trait::async_trait]
pub trait Core: Send + Sync {
    /// Register a worker with core. Workers poll on a specific task queue, and when calling core's
    /// poll functions, you must provide a task queue name. If there was already a worker registered
    /// with the same task queue name, it will be shut down and a new one will be created.
    fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError>;

    /// Ask the core for some work, returning a [WorkflowActivation]. It is then the language SDK's
    /// responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
    ///
    /// It is important to understand that all activations must be responded to. There can only
    /// be one outstanding activation for a particular run of a workflow at any time. If an
    /// activation is not responded to, it will cause that workflow to become stuck forever.
    ///
    /// Activations that contain only a `remove_from_cache` job should not cause the workflow code
    /// to be invoked and may be responded to with an empty command list. Eviction jobs may also
    /// appear with other jobs, but will always appear last in the job list. In this case it is
    /// expected that the workflow code will be invoked, and the response produced as normal, but
    /// the caller should evict the run after doing so.
    ///
    /// It is rarely a good idea to call poll concurrently. It handles polling the server
    /// concurrently internally.
    ///
    /// TODO: Examples
    async fn poll_workflow_activation(
        &self,
        task_queue: &str,
    ) -> Result<WorkflowActivation, PollWfError>;

    /// Ask the core for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
    ///
    /// It is rarely a good idea to call poll concurrently. It handles polling the server
    /// concurrently internally.
    ///
    /// TODO: Examples
    async fn poll_activity_task(&self, task_queue: &str)
        -> Result<ActivityTask, PollActivityError>;

    /// Tell the core that a workflow activation has completed. May be freely called concurrently.
    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError>;

    /// Tell the core that an activity has finished executing. May be freely called concurrently.
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError>;

    /// Notify workflow that an activity is still alive. Long running activities that take longer
    /// than `activity_heartbeat_timeout` to finish must call this function in order to report
    /// progress, otherwise the activity will timeout and a new attempt will be scheduled.
    ///
    /// The first heartbeat request will be sent immediately, subsequent rapid calls to this
    /// function will result in heartbeat requests being aggregated and the last one received during
    /// the aggregation period will be sent to the server, where that period is defined as half the
    /// heartbeat timeout.
    ///
    /// Unlike java/go SDKs we do not return cancellation status as part of heartbeat response and
    /// instead send it as a separate activity task to the lang, decoupling heartbeat and
    /// cancellation processing.
    ///
    /// For now activity still need to send heartbeats if they want to receive cancellation
    /// requests. In the future we will change this and will dispatch cancellations more
    /// proactively. Note that this function does not block on the server call and returns
    /// immediately. Underlying validation errors are swallowed and logged, this has been agreed to
    /// be optimal behavior for the user as we don't want to break activity execution due to badly
    /// configured heartbeat options.
    fn record_activity_heartbeat(&self, details: ActivityHeartbeat);

    /// Request that a workflow be evicted by its run id. This will generate a workflow activation
    /// with the eviction job inside it to be eventually returned by
    /// [Core::poll_workflow_activation]. If the workflow had any existing outstanding activations,
    /// such activations are invalidated and subsequent completions of them will do nothing and log
    /// a warning.
    fn request_workflow_eviction(&self, task_queue: &str, run_id: &str);

    /// Returns core's instance of the [ServerGatewayApis] implementor it is using.
    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis + Send + Sync>;

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down all registered workers. [Core::poll_workflow_activation] should be called until it
    /// returns [PollWfError::ShutDown] to ensure that any workflows which are still undergoing
    /// replay have an opportunity to finish. This means that the lang sdk will need to call
    /// [Core::complete_workflow_activation] for those workflows until they are done. At that point,
    /// the lang SDK can end the process, or drop the [Core] instance, which will close the
    /// connection.
    async fn shutdown(&self);

    /// Shut down a specific worker. Will cease all polling on the task queue and future attempts
    /// to poll that queue will return [PollWfError::NoWorkerForQueue].
    async fn shutdown_worker(&self, task_queue: &str);

    /// Core buffers logs that should be shuttled over to lang so that they may be rendered with
    /// the user's desired logging library. Use this function to grab the most recent buffered logs
    /// since the last time it was called. A fixed number of such logs are retained at maximum, with
    /// the oldest being dropped when full.
    ///
    /// Returns the list of logs from oldest to newest. Returns an empty vec if the feature is not
    /// configured.
    fn fetch_buffered_logs(&self) -> Vec<CoreLog>;
}

/// A log line (which ultimately came from a tracing event) exported from Core->Lang
#[derive(Debug)]
pub struct CoreLog {
    /// Log message
    pub message: String,
    /// Time log was generated (not when it was exported to lang)
    pub timestamp: SystemTime,
    /// Message level
    pub level: Level,
    // KV pairs aren't meaningfully exposed yet to the log interface by tracing
}

impl CoreLog {
    /// Return timestamp as ms since epoch
    pub fn millis_since_epoch(&self) -> u128 {
        self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    }
}
