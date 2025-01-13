use crate::{errors::WorkflowErrorType, telemetry::metrics::TemporalMeter};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use temporal_sdk_core_protos::coresdk::{
    ActivitySlotInfo, LocalActivitySlotInfo, NexusSlotInfo, WorkflowSlotInfo,
};

const MAX_CONCURRENT_WFT_POLLS_DEFAULT: usize = 5;

/// Defines per-worker configuration options
#[derive(Clone, derive_builder::Builder)]
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
    /// Set a [WorkerTuner] for this worker. Either this or at least one of the `max_outstanding_*`
    /// fields must be set.
    #[builder(setter(into = false, strip_option), default)]
    pub tuner: Option<Arc<dyn WorkerTuner + Send + Sync>>,
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
    /// Maximum number of concurrent poll nexus task requests we will perform at a time on this
    /// worker's task queue
    #[builder(default = "5")]
    pub max_concurrent_nexus_polls: usize,
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
    ///
    /// Setting this to a nonzero value will also disable eager activity execution.
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

    /// If set, core will issue cancels for all outstanding activities and nexus operations after
    /// shutdown has been initiated and this amount of time has elapsed.
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

    /// The maximum allowed number of workflow tasks that will ever be given to this worker at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed.
    ///
    /// Mutually exclusive with `tuner`
    #[builder(setter(into, strip_option), default)]
    pub max_outstanding_workflow_tasks: Option<usize>,
    /// The maximum number of activity tasks that will ever be given to this worker concurrently
    ///
    /// Mutually exclusive with `tuner`
    #[builder(setter(into, strip_option), default)]
    pub max_outstanding_activities: Option<usize>,
    /// The maximum number of local activity tasks that will ever be given to this worker
    /// concurrently
    ///
    /// Mutually exclusive with `tuner`
    #[builder(setter(into, strip_option), default)]
    pub max_outstanding_local_activities: Option<usize>,
    /// The maximum number of nexus tasks that will ever be given to this worker
    /// concurrently
    ///
    /// Mutually exclusive with `tuner`
    #[builder(setter(into, strip_option), default)]
    pub max_outstanding_nexus_tasks: Option<usize>,
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
    /// Unset all `max_outstanding_*` fields
    pub fn clear_max_outstanding_opts(&mut self) -> &mut Self {
        self.max_outstanding_workflow_tasks = None;
        self.max_outstanding_activities = None;
        self.max_outstanding_local_activities = None;
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_wft_polls == Some(0) {
            return Err("`max_concurrent_wft_polls` must be at least 1".to_owned());
        }
        if self.max_concurrent_at_polls == Some(0) {
            return Err("`max_concurrent_at_polls` must be at least 1".to_owned());
        }

        if let Some(Some(ref x)) = self.max_worker_activities_per_second {
            if !x.is_normal() || x.is_sign_negative() {
                return Err(
                    "`max_worker_activities_per_second` must be positive and nonzero".to_owned(),
                );
            }
        }

        if self.tuner.is_some()
            && (self.max_outstanding_workflow_tasks.is_some()
                || self.max_outstanding_activities.is_some()
                || self.max_outstanding_local_activities.is_some())
        {
            return Err("max_outstanding_* fields are mutually exclusive with `tuner`".to_owned());
        }

        let max_wft_polls = self
            .max_concurrent_wft_polls
            .unwrap_or(MAX_CONCURRENT_WFT_POLLS_DEFAULT);

        // It wouldn't make any sense to have more outstanding polls than workflows we can possibly
        // cache. If we allow this at low values it's possible for sticky pollers to reserve all
        // available slots, crowding out the normal queue and gumming things up.
        if let Some(max_cache) = self.max_cached_workflows {
            if max_cache > 0 && max_wft_polls > max_cache {
                return Err(
                    "`max_concurrent_wft_polls` cannot exceed `max_cached_workflows`".to_owned(),
                );
            }
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

/// This trait allows users to customize the performance characteristics of workers dynamically.
/// For more, see the docstrings of the traits in the return types of its functions.
pub trait WorkerTuner {
    /// Return a [SlotSupplier] for workflow tasks
    fn workflow_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for activity tasks
    fn activity_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for local activities
    fn local_activity_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for nexus tasks
    fn nexus_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync>;

    /// Core will call this at worker initialization time, allowing the implementation to hook up to
    /// metrics if any are configured. If not, it will not be called.
    fn attach_metrics(&self, metrics: TemporalMeter);
}

/// Implementing this trait allows users to customize how many tasks of certain kinds the worker
/// will perform concurrently.
///
/// Note that, for implementations on workflow tasks ([WorkflowSlotKind]), workers that have the
/// workflow cache enabled should be willing to hand out _at least_ two slots, to avoid the worker
/// becoming stuck only polling on the worker's sticky queue.
#[async_trait::async_trait]
pub trait SlotSupplier {
    type SlotKind: SlotKind;
    /// Block until a slot is available, then return a permit for the slot.
    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit;

    /// Try to immediately reserve a slot, returning None if one is not available. Implementations
    /// must not block, or risk blocking the async event loop.
    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit>;

    /// Marks a slot as actually now being used. This is separate from reserving one because the
    /// pollers need to reserve a slot before they have actually obtained work from server. Once
    /// that task is obtained (and validated) then the slot can actually be used to work on the
    /// task.
    ///
    /// Users' implementation of this can choose to emit metrics, or otherwise leverage the
    /// information provided by the `info` parameter to be better able to make future decisions
    /// about whether a slot should be handed out.
    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>);

    /// Frees a slot.
    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>);

    /// If this implementation knows how many slots are available at any moment, it should return
    /// that here.
    fn available_slots(&self) -> Option<usize> {
        None
    }
}

pub trait SlotReservationContext: Send + Sync {
    /// Returns the name of the task queue this worker is polling
    fn task_queue(&self) -> &str;

    /// Returns the identity of the worker
    fn worker_identity(&self) -> &str;

    /// Returns the build id of the worker
    fn worker_build_id(&self) -> &str;

    /// Returns the number of currently outstanding slot permits, whether used or un-used.
    fn num_issued_slots(&self) -> usize;

    /// Returns true iff this is a sticky poll for a workflow task
    fn is_sticky(&self) -> bool;
}

pub trait SlotMarkUsedContext: Send + Sync {
    type SlotKind: SlotKind;
    /// The slot permit that is being used
    fn permit(&self) -> &SlotSupplierPermit;
    /// Returns the info of slot that was marked as used
    fn info(&self) -> &<Self::SlotKind as SlotKind>::Info;
}

pub trait SlotReleaseContext: Send + Sync {
    type SlotKind: SlotKind;
    /// The slot permit that is being used
    fn permit(&self) -> &SlotSupplierPermit;
    /// Returns the info of slot that was released, if it was used
    fn info(&self) -> Option<&<Self::SlotKind as SlotKind>::Info>;
}

#[derive(Default, Debug)]
pub struct SlotSupplierPermit {
    user_data: Option<Box<dyn Any + Send + Sync>>,
}
impl SlotSupplierPermit {
    pub fn with_user_data<T: Any + Send + Sync>(user_data: T) -> Self {
        Self {
            user_data: Some(Box::new(user_data)),
        }
    }
    /// Attempts to downcast the inner data, if any, into the provided type and returns it.
    /// Returns none if there is no data or the data is not of the appropriate type.
    pub fn user_data<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.user_data.as_ref().and_then(|b| b.downcast_ref())
    }
    /// Attempts to downcast the inner data, if any, into the provided type and returns it mutably.
    /// Returns none if there is no data or the data is not of the appropriate type.
    pub fn user_data_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        self.user_data.as_mut().and_then(|b| b.downcast_mut())
    }
}

#[derive(Debug, Copy, Clone, derive_more::Display, Eq, PartialEq)]
pub enum SlotKindType {
    Workflow,
    Activity,
    LocalActivity,
    Nexus,
}

#[derive(Debug, Copy, Clone)]
pub struct WorkflowSlotKind {}
#[derive(Debug, Copy, Clone)]
pub struct ActivitySlotKind {}
#[derive(Debug, Copy, Clone)]
pub struct LocalActivitySlotKind {}
#[derive(Debug, Copy, Clone)]
pub struct NexusSlotKind {}

pub enum SlotInfo<'a> {
    Workflow(&'a WorkflowSlotInfo),
    Activity(&'a ActivitySlotInfo),
    LocalActivity(&'a LocalActivitySlotInfo),
    Nexus(&'a NexusSlotInfo),
}

pub trait SlotInfoTrait: prost::Message {
    fn downcast(&self) -> SlotInfo;
}
impl SlotInfoTrait for WorkflowSlotInfo {
    fn downcast(&self) -> SlotInfo {
        SlotInfo::Workflow(self)
    }
}
impl SlotInfoTrait for ActivitySlotInfo {
    fn downcast(&self) -> SlotInfo {
        SlotInfo::Activity(self)
    }
}
impl SlotInfoTrait for LocalActivitySlotInfo {
    fn downcast(&self) -> SlotInfo {
        SlotInfo::LocalActivity(self)
    }
}
impl SlotInfoTrait for NexusSlotInfo {
    fn downcast(&self) -> SlotInfo {
        SlotInfo::Nexus(self)
    }
}

pub trait SlotKind {
    type Info: SlotInfoTrait;

    fn kind() -> SlotKindType;
}
impl SlotKind for WorkflowSlotKind {
    type Info = WorkflowSlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Workflow
    }
}
impl SlotKind for ActivitySlotKind {
    type Info = ActivitySlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Activity
    }
}
impl SlotKind for LocalActivitySlotKind {
    type Info = LocalActivitySlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::LocalActivity
    }
}
impl SlotKind for NexusSlotKind {
    type Info = NexusSlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Nexus
    }
}
