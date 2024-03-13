use crate::errors::WorkflowErrorType;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

const MAX_OUTSTANDING_WFT_DEFAULT: usize = 100;
const MAX_CONCURRENT_WFT_POLLS_DEFAULT: usize = 5;

/// Defines per-worker configuration options
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into), build_fn(validate = "Self::validate"))]
#[non_exhaustive]
pub struct WorkerConfig {
    /// The Temporal service namespace this worker is bound to
    pub namespace: String,
    /// What task queue will this worker poll from? This task queue name will be used for both
    /// workflow and activity polling.
    pub task_queue: String,
    /// A string that should be unique to the set of code this worker uses. IE: All the workflow,
    /// activity, interceptor, and data converter code.
    pub worker_build_id: String,
    /// A human-readable string that can identify this worker. Using something like sdk version
    /// and host name is a good default. If set, overrides the identity set (if any) on the client
    /// used by this worker.
    #[builder(default)]
    pub client_identity_override: Option<String>,
    /// If set nonzero, workflows will be cached and sticky task queues will be used, meaning that
    /// history updates are applied incrementally to suspended instances of workflow execution.
    /// Workflows are evicted according to a least-recently-used policy one the cache maximum is
    /// reached. Workflows may also be explicitly evicted at any time, or as a result of errors
    /// or failures.
    #[builder(default = "0")]
    pub max_cached_workflows: usize,
    /// The maximum allowed number of workflow tasks that will ever be given to this worker at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed.
    ///
    /// Cannot be larger than `max_cached_workflows`.
    #[builder(default = "MAX_OUTSTANDING_WFT_DEFAULT")]
    pub max_outstanding_workflow_tasks: usize,
    /// The maximum number of activity tasks that will ever be given to this worker concurrently
    #[builder(default = "100")]
    pub max_outstanding_activities: usize,
    /// The maximum number of local activity tasks that will ever be given to this worker
    /// concurrently
    #[builder(default = "100")]
    pub max_outstanding_local_activities: usize,
    /// Maximum number of concurrent poll workflow task requests we will perform at a time on this
    /// worker's task queue. See also [WorkerConfig::nonsticky_to_sticky_poll_ratio]. Must be at
    /// least 1.
    #[builder(default = "MAX_CONCURRENT_WFT_POLLS_DEFAULT")]
    pub max_concurrent_wft_polls: usize,
    /// [WorkerConfig::max_concurrent_wft_polls] * this number = the number of max pollers that will
    /// be allowed for the nonsticky queue when sticky tasks are enabled. If both defaults are used,
    /// the sticky queue will allow 4 max pollers while the nonsticky queue will allow one. The
    /// minimum for either poller is 1, so if `max_concurrent_wft_polls` is 1 and sticky queues are
    /// enabled, there will be 2 concurrent polls.
    #[builder(default = "0.2")]
    pub nonsticky_to_sticky_poll_ratio: f32,
    /// Maximum number of concurrent poll activity task requests we will perform at a time on this
    /// worker's task queue
    #[builder(default = "5")]
    pub max_concurrent_at_polls: usize,
    /// If set to true this worker will only handle workflow tasks and local activities, it will not
    /// poll for activity tasks.
    #[builder(default = "false")]
    pub no_remote_activities: bool,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue where it may be picked up by any worker.
    #[builder(default = "Duration::from_secs(10)")]
    pub sticky_queue_schedule_to_start_timeout: Duration,

    /// Longest interval for throttling activity heartbeats
    #[builder(default = "Duration::from_secs(60)")]
    pub max_heartbeat_throttle_interval: Duration,

    /// Default interval for throttling activity heartbeats in case
    /// `ActivityOptions.heartbeat_timeout` is unset.
    /// When the timeout *is* set in the `ActivityOptions`, throttling is set to
    /// `heartbeat_timeout * 0.8`.
    #[builder(default = "Duration::from_secs(30)")]
    pub default_heartbeat_throttle_interval: Duration,

    /// Sets the maximum number of activities per second the task queue will dispatch, controlled
    /// server-side. Note that this only takes effect upon an activity poll request. If multiple
    /// workers on the same queue have different values set, they will thrash with the last poller
    /// winning.
    #[builder(default)]
    pub max_task_queue_activities_per_second: Option<f64>,

    /// Limits the number of activities per second that this worker will process. The worker will
    /// not poll for new activities if by doing so it might receive and execute an activity which
    /// would cause it to exceed this limit. Negative, zero, or NaN values will cause building
    /// the options to fail.
    #[builder(default)]
    pub max_worker_activities_per_second: Option<f64>,

    /// # UNDER DEVELOPMENT
    /// If set to true this worker will opt-in to the whole-worker versioning feature.
    /// `worker_build_id` will be used as the version.
    /// todo: link to feature docs
    #[builder(default = "false")]
    pub use_worker_versioning: bool,

    /// If set false (default), shutdown will not finish until all pending evictions have been
    /// issued and replied to. If set true shutdown will be considered complete when the only
    /// remaining work is pending evictions.
    ///
    /// This flag is useful during tests to avoid needing to deal with lots of uninteresting
    /// evictions during shutdown. Alternatively, if a lang implementation finds it easy to clean
    /// up during shutdown, setting this true saves some back-and-forth.
    #[builder(default = "false")]
    pub ignore_evicts_on_shutdown: bool,

    /// Maximum number of next page (or initial) history event listing requests we'll make
    /// concurrently. I don't this it's worth exposing this to users until we encounter a reason.
    #[builder(default = "5")]
    pub fetching_concurrency: usize,

    /// If set, core will issue cancels for all outstanding activities after shutdown has been
    /// initiated and this amount of time has elapsed.
    #[builder(default)]
    pub graceful_shutdown_period: Option<Duration>,

    /// The amount of time core will wait before timing out activities using its own local timers
    /// after one of them elapses. This is to avoid racing with server's own tracking of the
    /// timeout.
    #[builder(default = "Duration::from_secs(5)")]
    pub local_timeout_buffer_for_activities: Duration,

    /// Any error types listed here will cause any workflow being processed by this worker to fail,
    /// rather than simply failing the workflow task.
    #[builder(default)]
    pub workflow_failure_errors: HashSet<WorkflowErrorType>,

    /// Like [WorkerConfig::workflow_failure_errors], but specific to certain workflow types (the
    /// map key).
    #[builder(default)]
    pub workflow_types_to_failure_errors: HashMap<String, HashSet<WorkflowErrorType>>,
}

impl WorkerConfig {
    pub fn max_nonsticky_polls(&self) -> usize {
        ((self.max_concurrent_wft_polls as f32 * self.nonsticky_to_sticky_poll_ratio) as usize)
            .max(1)
    }
    pub fn max_sticky_polls(&self) -> usize {
        self.max_concurrent_wft_polls
            .saturating_sub(self.max_nonsticky_polls())
            .max(1)
    }
    /// Returns true if the configuration specifies we should fail a workflow on a certain error
    /// type rather than failing the workflow task.
    pub fn should_fail_workflow(
        &self,
        workflow_type: &str,
        error_type: &WorkflowErrorType,
    ) -> bool {
        self.workflow_failure_errors.contains(error_type)
            || self
                .workflow_types_to_failure_errors
                .get(workflow_type)
                .map(|s| s.contains(error_type))
                .unwrap_or(false)
    }
}

impl WorkerConfigBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_wft_polls == Some(0) {
            return Err("`max_concurrent_wft_polls` must be at least 1".to_owned());
        }
        if self.max_concurrent_at_polls == Some(0) {
            return Err("`max_concurrent_at_polls` must be at least 1".to_owned());
        }
        if self.max_cached_workflows > Some(0)
            && self.max_outstanding_workflow_tasks > self.max_cached_workflows
        {
            return Err(
                "Maximum concurrent workflow tasks cannot exceed the maximum number of cached \
                 workflows"
                    .to_owned(),
            );
        }
        if let Some(Some(ref x)) = self.max_worker_activities_per_second {
            if !x.is_normal() || x.is_sign_negative() {
                return Err(
                    "`max_worker_activities_per_second` must be positive and nonzero".to_owned(),
                );
            }
        }
        if matches!(self.max_concurrent_wft_polls, Some(1))
            && self.max_cached_workflows > Some(0)
            && self
                .max_outstanding_workflow_tasks
                .unwrap_or(MAX_OUTSTANDING_WFT_DEFAULT)
                <= 1
        {
            return Err(
                "`max_outstanding_workflow_tasks` must be at at least 2 when \
                 `max_cached_workflows` is nonzero"
                    .to_owned(),
            );
        }
        if self
            .max_concurrent_wft_polls
            .unwrap_or(MAX_CONCURRENT_WFT_POLLS_DEFAULT)
            > self
                .max_outstanding_workflow_tasks
                .unwrap_or(MAX_OUTSTANDING_WFT_DEFAULT)
        {
            return Err(
                "`max_concurrent_wft_polls` cannot exceed `max_outstanding_workflow_tasks`"
                    .to_owned(),
            );
        }

        if self.use_worker_versioning.unwrap_or_default()
            && self
                .worker_build_id
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or_default()
        {
            return Err(
                "`worker_build_id` must be non-empty when `use_worker_versioning` is true"
                    .to_owned(),
            );
        }
        Ok(())
    }
}
