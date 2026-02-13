//! Functionality related to defining and interacting with activities
//!
//!
//! An example of defining an activity:
//! ```
//! use std::sync::{
//!     Arc,
//!     atomic::{AtomicUsize, Ordering},
//! };
//! use temporalio_macros::activities;
//! use temporalio_sdk::activities::{ActivityContext, ActivityError};
//!
//! struct MyActivities {
//!     counter: AtomicUsize,
//! }
//!
//! #[activities]
//! impl MyActivities {
//!     #[activity]
//!     async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
//!         Ok(e)
//!     }
//!
//!     #[activity]
//!     async fn uses_self(self: Arc<Self>, _ctx: ActivityContext) -> Result<(), ActivityError> {
//!         self.counter.fetch_add(1, Ordering::Relaxed);
//!         Ok(())
//!     }
//! }
//!
//! // If you need to refer to an activity that is defined externally, in a different codebase or
//! // possibly a differenet language, you can simply leave the function body unimplemented like so:
//!
//! struct ExternalActivities;
//! #[activities]
//! impl ExternalActivities {
//!     #[activity(name = "foo")]
//!     async fn foo(_ctx: ActivityContext, _: String) -> Result<String, ActivityError> {
//!         unimplemented!()
//!     }
//! }
//! ```
//!
//! This will allows you to call the activity from workflow code still, but the actual function
//! will never be invoked, since you won't have registered it with the worker.

#[doc(inline)]
pub use temporalio_macros::activities;

use futures_util::{FutureExt, future::BoxFuture};
use prost_types::{Duration, Timestamp};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration as StdDuration, SystemTime},
};
use temporalio_client::Priority;
use temporalio_common::{
    ActivityDefinition,
    data_converters::{
        DataConverter, GenericPayloadConverter, SerializationContext, SerializationContextData,
    },
    protos::{
        coresdk::{ActivityHeartbeat, activity_task},
        temporal::api::common::v1::{Payload, RetryPolicy, WorkflowExecution},
        utilities::TryIntoOrNone,
    },
};
use temporalio_sdk_core::Worker as CoreWorker;
use tokio_util::sync::CancellationToken;

/// Used within activities to get info, heartbeat management etc.
#[derive(Clone)]
pub struct ActivityContext {
    worker: Arc<CoreWorker>,
    cancellation_token: CancellationToken,
    heartbeat_details: Vec<Payload>,
    header_fields: HashMap<String, Payload>,
    info: ActivityInfo,
}

impl ActivityContext {
    /// Construct new Activity Context, returning the context and all arguments to the activity.
    pub fn new(
        worker: Arc<CoreWorker>,
        cancellation_token: CancellationToken,
        task_queue: String,
        task_token: Vec<u8>,
        task: activity_task::Start,
    ) -> (Self, Vec<Payload>) {
        let activity_task::Start {
            workflow_namespace,
            workflow_type,
            workflow_execution,
            activity_id,
            activity_type,
            header_fields,
            input,
            heartbeat_details,
            scheduled_time,
            current_attempt_scheduled_time,
            started_time,
            attempt,
            schedule_to_close_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
            is_local,
            priority,
        } = task;
        let deadline = calculate_deadline(
            scheduled_time.as_ref(),
            started_time.as_ref(),
            start_to_close_timeout.as_ref(),
            schedule_to_close_timeout.as_ref(),
        );

        (
            ActivityContext {
                worker,
                cancellation_token,
                heartbeat_details,
                header_fields,
                info: ActivityInfo {
                    task_token,
                    task_queue,
                    workflow_type,
                    workflow_namespace,
                    workflow_execution,
                    activity_id,
                    activity_type,
                    heartbeat_timeout: heartbeat_timeout.try_into_or_none(),
                    scheduled_time: scheduled_time.try_into_or_none(),
                    started_time: started_time.try_into_or_none(),
                    deadline,
                    attempt,
                    current_attempt_scheduled_time: current_attempt_scheduled_time
                        .try_into_or_none(),
                    retry_policy,
                    is_local,
                    priority: priority.map(Into::into).unwrap_or_default(),
                },
            },
            input,
        )
    }

    /// Returns a future the completes if and when the activity this was called inside has been
    /// cancelled
    pub async fn cancelled(&self) {
        self.cancellation_token.clone().cancelled().await
    }

    /// Returns true if this activity has already been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Extract heartbeat details from last failed attempt. This is used in combination with retry
    /// policy.
    pub fn heartbeat_details(&self) -> &[Payload] {
        &self.heartbeat_details
    }

    /// RecordHeartbeat sends heartbeat for the currently executing activity
    pub fn record_heartbeat(&self, details: Vec<Payload>) {
        if !self.info.is_local {
            self.worker.record_activity_heartbeat(ActivityHeartbeat {
                task_token: self.info.task_token.clone(),
                details,
            })
        }
    }

    /// Returns activity info of the executing activity
    pub fn info(&self) -> &ActivityInfo {
        &self.info
    }

    /// Get headers attached to this activity
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.header_fields
    }
}

/// Various information about a specific activity attempt.
#[derive(Clone)]
#[non_exhaustive]
pub struct ActivityInfo {
    /// An opaque token representing a specific Activity task.
    pub task_token: Vec<u8>,
    /// The type of the workflow that invoked this activity.
    pub workflow_type: String,
    /// The namespace of the workflow that invoked this activity.
    pub workflow_namespace: String,
    /// The execution of the workflow that invoked this activity.
    pub workflow_execution: Option<WorkflowExecution>,
    /// The ID of this activity.
    pub activity_id: String,
    /// The type of this activity.
    pub activity_type: String,
    /// The task queue of this activity.
    pub task_queue: String,
    /// The interval within which this activity must heartbeat or be timed out.
    pub heartbeat_timeout: Option<StdDuration>,
    /// Time activity was scheduled by a workflow.
    pub scheduled_time: Option<SystemTime>,
    /// Time of activity start.
    pub started_time: Option<SystemTime>,
    /// Time of activity timeout.
    pub deadline: Option<SystemTime>,
    /// Attempt starts from 1, and increase by 1 for every retry, if retry policy is specified.
    pub attempt: u32,
    /// Time this attempt at the activity was scheduled.
    pub current_attempt_scheduled_time: Option<SystemTime>,
    /// The retry policy for this activity.
    pub retry_policy: Option<RetryPolicy>,
    /// Whether or not this is a local activity.
    pub is_local: bool,
    /// Priority of this activity. If unset uses [Priority::default].
    pub priority: Priority,
}

/// Returned as errors from activity functions.
#[derive(Debug, thiserror::Error)]
pub enum ActivityError {
    /// This error can be returned from activities to allow the explicit configuration of certain
    /// error properties. It's also the default error type that arbitrary errors will be converted
    /// into.
    #[error("Retryable activity error: {source}")]
    Retryable {
        /// The underlying error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        /// If specified, the next retry (if there is one) will occur after this delay
        explicit_delay: Option<StdDuration>,
    },
    /// Return this error to indicate your activity is cancelling
    #[error("Activity cancelled")]
    Cancelled {
        /// Some data to save as the cancellation reason
        details: Option<Payload>,
    },
    /// Return this error to indicate that the activity should not be retried.
    #[error("Non-retryable activity error: {0}")]
    NonRetryable(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Return this error to indicate that the activity will be completed outside of this activity
    /// definition, by an external client.
    #[error("Activity will complete asynchronously")]
    WillCompleteAsync,
}

impl ActivityError {
    /// Construct a cancelled error without details
    pub fn cancelled() -> Self {
        Self::Cancelled { details: None }
    }
}

impl From<anyhow::Error> for ActivityError {
    fn from(source: anyhow::Error) -> Self {
        Self::Retryable {
            source: source.into_boxed_dyn_error(),
            explicit_delay: None,
        }
    }
}

/// Deadline calculation.  This is a port of
/// https://github.com/temporalio/sdk-go/blob/8651550973088f27f678118f997839fb1bb9e62f/internal/activity.go#L225
fn calculate_deadline(
    scheduled_time: Option<&Timestamp>,
    started_time: Option<&Timestamp>,
    start_to_close_timeout: Option<&Duration>,
    schedule_to_close_timeout: Option<&Duration>,
) -> Option<SystemTime> {
    match (
        scheduled_time,
        started_time,
        start_to_close_timeout,
        schedule_to_close_timeout,
    ) {
        (
            Some(scheduled),
            Some(started),
            Some(start_to_close_timeout),
            Some(schedule_to_close_timeout),
        ) => {
            let scheduled: SystemTime = maybe_convert_timestamp(scheduled)?;
            let started: SystemTime = maybe_convert_timestamp(started)?;
            let start_to_close_timeout: StdDuration = (*start_to_close_timeout).try_into().ok()?;
            let schedule_to_close_timeout: StdDuration =
                (*schedule_to_close_timeout).try_into().ok()?;

            let start_to_close_deadline: SystemTime =
                started.checked_add(start_to_close_timeout)?;
            if schedule_to_close_timeout > StdDuration::ZERO {
                let schedule_to_close_deadline =
                    scheduled.checked_add(schedule_to_close_timeout)?;
                // Minimum of the two deadlines.
                if schedule_to_close_deadline < start_to_close_deadline {
                    Some(schedule_to_close_deadline)
                } else {
                    Some(start_to_close_deadline)
                }
            } else {
                Some(start_to_close_deadline)
            }
        }
        _ => None,
    }
}

/// Helper function lifted from prost_types::Timestamp implementation to prevent double cloning in
/// error construction
fn maybe_convert_timestamp(timestamp: &Timestamp) -> Option<SystemTime> {
    let mut timestamp = *timestamp;
    timestamp.normalize();

    let system_time = if timestamp.seconds >= 0 {
        std::time::UNIX_EPOCH.checked_add(StdDuration::from_secs(timestamp.seconds as u64))
    } else {
        std::time::UNIX_EPOCH.checked_sub(StdDuration::from_secs((-timestamp.seconds) as u64))
    };

    system_time.and_then(|system_time| {
        system_time.checked_add(StdDuration::from_nanos(timestamp.nanos as u64))
    })
}

pub(crate) type ActivityInvocation = Arc<
    dyn Fn(
            Vec<Payload>,
            DataConverter,
            ActivityContext,
        ) -> BoxFuture<'static, Result<Payload, ActivityError>>
        + Send
        + Sync,
>;

#[doc(hidden)]
pub trait ActivityImplementer {
    fn register_all(self: Arc<Self>, defs: &mut ActivityDefinitions);
}

#[doc(hidden)]
pub trait ExecutableActivity: ActivityDefinition {
    type Implementer: ActivityImplementer + Send + Sync + 'static;
    fn execute(
        receiver: Option<Arc<Self::Implementer>>,
        ctx: ActivityContext,
        input: Self::Input,
    ) -> BoxFuture<'static, Result<Self::Output, ActivityError>>;
}

#[doc(hidden)]
pub trait HasOnlyStaticMethods {}

/// Contains activity registrations in a form ready for execution by workers.
#[derive(Default, Clone)]
pub struct ActivityDefinitions {
    activities: HashMap<&'static str, ActivityInvocation>,
}

impl ActivityDefinitions {
    /// Registers all activities on an activity implementer.
    pub fn register_activities<AI: ActivityImplementer>(&mut self, instance: AI) -> &mut Self {
        let arcd = Arc::new(instance);
        AI::register_all(arcd, self);
        self
    }
    /// Registers a specific activitiy.
    pub fn register_activity<AD>(&mut self, instance: Arc<AD::Implementer>) -> &mut Self
    where
        AD: ActivityDefinition + ExecutableActivity,
        AD::Output: Send + Sync,
    {
        self.activities.insert(
            AD::name(),
            Arc::new(move |payloads, dc, c| {
                let instance = instance.clone();
                let dc = dc.clone();
                async move {
                    // Use PayloadConverter (not DataConverter) since the codec is applied
                    // at the SDK/Core boundary by the visitor, not here.
                    let pc = dc.payload_converter();
                    let ctx = SerializationContext {
                        data: &SerializationContextData::Activity,
                        converter: pc,
                    };
                    let deserialized: AD::Input = pc
                        .from_payloads(&ctx, payloads)
                        .map_err(|e| ActivityError::from(anyhow::Error::from(e)))?;
                    let result = AD::execute(Some(instance), c, deserialized).await?;
                    pc.to_payload(&ctx, &result)
                        .map_err(|e| ActivityError::from(anyhow::Error::from(e)))
                }
                .boxed()
            }),
        );
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.activities.is_empty()
    }

    pub(crate) fn get(&self, act_type: &str) -> Option<ActivityInvocation> {
        self.activities.get(act_type).cloned()
    }
}

impl Debug for ActivityDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivityDefinitions")
            .field("activities", &self.activities.keys())
            .finish()
    }
}
