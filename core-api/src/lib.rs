pub mod errors;
pub mod worker;

use crate::{
    errors::{CompleteActivityError, CompleteWfError, PollActivityError, PollWfError},
    worker::WorkerConfig,
};
use log::Level;
use opentelemetry::metrics::Meter;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use temporal_sdk_core_protos::coresdk::{
    activity_task::ActivityTask, workflow_activation::WorkflowActivation,
    workflow_completion::WorkflowActivationCompletion, ActivityHeartbeat, ActivityTaskCompletion,
};

/// This trait is the primary way by which language specific SDKs interact with the core SDK.
/// It represents one worker, which has a (potentially shared) client for connecting to the service
/// and is bound to a specific task queue.
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    /// Ask the worker for some work, returning a [WorkflowActivation]. It is then the language
    /// SDK's responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Worker::shutdown] is called.
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
    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError>;

    /// Ask the worker for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
    ///
    /// It is rarely a good idea to call poll concurrently. It handles polling the server
    /// concurrently internally.
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollActivityError>;

    /// Tell the worker that a workflow activation has completed. May be freely called concurrently.
    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError>;

    /// Tell the worker that an activity has finished executing. May be freely called concurrently.
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError>;

    /// Notify the Temporal service that an activity is still alive. Long running activities that
    /// take longer than `activity_heartbeat_timeout` to finish must call this function in order to
    /// report progress, otherwise the activity will timeout and a new attempt will be scheduled.
    ///
    /// The first heartbeat request will be sent immediately, subsequent rapid calls to this
    /// function will result in heartbeat requests being aggregated and the last one received during
    /// the aggregation period will be sent to the server, where that period is defined as half the
    /// heartbeat timeout.
    ///
    /// Unlike Java/Go SDKs we do not return cancellation status as part of heartbeat response and
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
    /// [Worker::poll_workflow_activation]. If the workflow had any existing outstanding activations,
    /// such activations are invalidated and subsequent completions of them will do nothing and log
    /// a warning.
    fn request_workflow_eviction(&self, run_id: &str);

    /// Return this worker's config
    fn get_config(&self) -> &WorkerConfig;

    /// Initiate shutdown. See [Worker::shutdown], this is just a sync version that starts the
    /// process. You can then wait on `shutdown` or [Worker::finalize_shutdown].
    fn initiate_shutdown(&self);

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down this worker. [Worker::poll_workflow_activation] should be called until it
    /// returns [PollWfError::ShutDown] to ensure that any workflows which are still undergoing
    /// replay have an opportunity to finish. This means that the lang sdk will need to call
    /// [Worker::complete_workflow_activation] for those workflows until they are done. At that point,
    /// the lang SDK can end the process, or drop the [Worker] instance, which will close the
    /// connection.
    async fn shutdown(&self);

    /// Completes shutdown and frees all resources. You should avoid simply dropping workers, as
    /// this does not allow async tasks to report any panics that may have occurred cleanly.
    ///
    /// This should be called only after [Worker::shutdown] has resolved.
    async fn finalize_shutdown(self);
}

/// Should be backed by a process-wide singleton who is responsible for telemetry and logging
/// management.
pub trait CoreTelemetry {
    /// Core buffers logs that should be shuttled over to lang so that they may be rendered with
    /// the user's desired logging library. Use this function to grab the most recent buffered logs
    /// since the last time it was called. A fixed number of such logs are retained at maximum, with
    /// the oldest being dropped when full.
    ///
    /// Returns the list of logs from oldest to newest. Returns an empty vec if the feature is not
    /// configured.
    fn fetch_buffered_logs(&self) -> Vec<CoreLog>;

    /// If metrics gathering is enabled, returns the OTel meter for core telemetry, which can be
    /// used to create metrics instruments, or passed to things that create/record metrics (ex:
    /// clients).
    fn get_metric_meter(&self) -> Option<&Meter>;
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
