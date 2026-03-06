use crate::{NamespacedClient, grpc::WorkflowService};
use std::time::{Duration, SystemTime};
use temporalio_common::protos::{
    proto_ts_to_system_time,
    temporal::api::{
        common::v1 as common_proto,
        schedule::v1 as schedule_proto,
        taskqueue::v1 as taskqueue_proto,
        workflow::v1 as workflow_proto,
        workflowservice::v1::*,
    },
};
use tonic::IntoRequest;
use uuid::Uuid;

/// Errors returned by schedule operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScheduleError {
    /// An rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Options for creating a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct CreateScheduleOptions {
    /// Whether to trigger the schedule immediately upon creation.
    #[builder(default)]
    pub trigger_immediately: bool,
    /// Whether the schedule starts in a paused state.
    #[builder(default)]
    pub paused: bool,
    /// A note to attach to the schedule state (e.g., reason for pausing).
    #[builder(default)]
    pub note: String,
}

/// The action a schedule should perform on each trigger.
#[derive(Debug, Clone)]
pub enum ScheduleAction {
    /// Start a workflow execution.
    StartWorkflow {
        /// The workflow type name.
        workflow_type: String,
        /// The task queue to run the workflow on.
        task_queue: String,
        /// The workflow ID prefix. The server may append a timestamp.
        workflow_id: String,
    },
}

impl ScheduleAction {
    /// Create a start-workflow action.
    pub fn start_workflow(
        workflow_type: impl Into<String>,
        task_queue: impl Into<String>,
        workflow_id: impl Into<String>,
    ) -> Self {
        Self::StartWorkflow {
            workflow_type: workflow_type.into(),
            task_queue: task_queue.into(),
            workflow_id: workflow_id.into(),
        }
    }

    pub(crate) fn into_proto(self) -> schedule_proto::ScheduleAction {
        match self {
            Self::StartWorkflow {
                workflow_type,
                task_queue,
                workflow_id,
            } => schedule_proto::ScheduleAction {
                action: Some(schedule_proto::schedule_action::Action::StartWorkflow(
                    workflow_proto::NewWorkflowExecutionInfo {
                        workflow_id,
                        workflow_type: Some(common_proto::WorkflowType {
                            name: workflow_type,
                        }),
                        task_queue: Some(taskqueue_proto::TaskQueue {
                            name: task_queue,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
            },
        }
    }
}

/// Defines when a schedule should trigger.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ScheduleSpec {
    /// Interval-based triggers (e.g., every 1 hour).
    #[builder(default)]
    pub intervals: Vec<ScheduleIntervalSpec>,
    /// Calendar-based triggers using range strings.
    #[builder(default)]
    pub calendars: Vec<ScheduleCalendarSpec>,
}

impl ScheduleSpec {
    pub(crate) fn into_proto(self) -> schedule_proto::ScheduleSpec {
        schedule_proto::ScheduleSpec {
            interval: self.intervals.into_iter().map(Into::into).collect(),
            calendar: self.calendars.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

/// An interval-based schedule trigger.
#[derive(Debug, Clone, bon::Builder)]
pub struct ScheduleIntervalSpec {
    /// How often the action should repeat.
    pub every: Duration,
    /// Fixed offset added to each interval.
    pub offset: Option<Duration>,
}

impl From<ScheduleIntervalSpec> for schedule_proto::IntervalSpec {
    fn from(s: ScheduleIntervalSpec) -> Self {
        Self {
            interval: Some(s.every.try_into().unwrap_or_default()),
            phase: s.offset.and_then(|d| d.try_into().ok()),
        }
    }
}

/// A calendar-based schedule trigger using range strings (e.g., `"2-7"` for hours 2 through 7).
///
/// Empty strings use server defaults (typically `"*"` for most fields, `"0"` for seconds/minutes).
#[derive(Debug, Clone, Default, bon::Builder)]
#[builder(on(String, into))]
pub struct ScheduleCalendarSpec {
    /// Second within the minute. Default: `"0"`.
    #[builder(default)]
    pub second: String,
    /// Minute within the hour. Default: `"0"`.
    #[builder(default)]
    pub minute: String,
    /// Hour of the day. Default: `"0"`.
    #[builder(default)]
    pub hour: String,
    /// Day of the month. Default: `"*"`.
    #[builder(default)]
    pub day_of_month: String,
    /// Month of the year. Default: `"*"`.
    #[builder(default)]
    pub month: String,
    /// Day of the week. Default: `"*"`.
    #[builder(default)]
    pub day_of_week: String,
    /// Year. Default: `"*"`.
    #[builder(default)]
    pub year: String,
    /// Free-form comment.
    #[builder(default)]
    pub comment: String,
}

impl From<ScheduleCalendarSpec> for schedule_proto::CalendarSpec {
    fn from(s: ScheduleCalendarSpec) -> Self {
        Self {
            second: s.second,
            minute: s.minute,
            hour: s.hour,
            day_of_month: s.day_of_month,
            month: s.month,
            day_of_week: s.day_of_week,
            year: s.year,
            comment: s.comment,
        }
    }
}

/// Options for listing schedules.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct ListSchedulesOptions {
    /// Maximum number of results per page.
    #[builder(default)]
    pub maximum_page_size: i32,
    /// Query filter string.
    #[builder(default)]
    pub query: String,
}

/// A single page of schedule list results.
#[derive(Debug)]
pub struct ListSchedulesPage {
    /// The schedule entries in this page.
    pub schedules: Vec<ScheduleSummary>,
    /// Token for the next page. Empty if no more pages.
    pub next_page_token: Vec<u8>,
}

/// A recent action taken by a schedule.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleRecentAction {
    /// When this action was scheduled to occur (including jitter).
    pub schedule_time: Option<SystemTime>,
    /// When this action actually occurred.
    pub actual_time: Option<SystemTime>,
    /// Workflow ID of the started workflow, if any.
    pub workflow_id: String,
    /// Run ID of the started workflow, if any.
    pub run_id: String,
}

/// A currently-running workflow started by a schedule.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleRunningAction {
    /// Workflow ID of the running workflow.
    pub workflow_id: String,
    /// Run ID of the running workflow.
    pub run_id: String,
}

impl From<&schedule_proto::ScheduleActionResult> for ScheduleRecentAction {
    fn from(a: &schedule_proto::ScheduleActionResult) -> Self {
        ScheduleRecentAction {
            schedule_time: a.schedule_time.as_ref().and_then(proto_ts_to_system_time),
            actual_time: a.actual_time.as_ref().and_then(proto_ts_to_system_time),
            workflow_id: a
                .start_workflow_result
                .as_ref()
                .map_or_else(String::new, |w| w.workflow_id.clone()),
            run_id: a
                .start_workflow_result
                .as_ref()
                .map_or_else(String::new, |w| w.run_id.clone()),
        }
    }
}

/// Description of a schedule returned by describe().
///
/// Provides ergonomic accessors over the raw `DescribeScheduleResponse` proto.
/// Use [`raw()`](Self::raw) or [`into_raw()`](Self::into_raw) to access the
/// full proto when needed.
#[derive(Debug, Clone)]
pub struct ScheduleDescription {
    raw: DescribeScheduleResponse,
}

impl ScheduleDescription {
    /// Token used for optimistic concurrency on updates.
    pub fn conflict_token(&self) -> &[u8] {
        &self.raw.conflict_token
    }

    /// Whether the schedule is paused.
    pub fn paused(&self) -> bool {
        self.raw
            .schedule
            .as_ref()
            .and_then(|s| s.state.as_ref())
            .is_some_and(|st| st.paused)
    }

    /// Notes on the schedule state (e.g., reason for pause).
    pub fn notes(&self) -> Option<&str> {
        self.raw
            .schedule
            .as_ref()
            .and_then(|s| s.state.as_ref())
            .map(|st| st.notes.as_str())
    }

    /// Total number of actions taken by this schedule.
    pub fn action_count(&self) -> i64 {
        self.raw.info.as_ref().map_or(0, |i| i.action_count)
    }

    /// Number of times a scheduled action was skipped due to missing the catchup window.
    pub fn missed_catchup_window(&self) -> i64 {
        self.raw
            .info
            .as_ref()
            .map_or(0, |i| i.missed_catchup_window)
    }

    /// Number of skipped actions due to overlap.
    pub fn overlap_skipped(&self) -> i64 {
        self.raw.info.as_ref().map_or(0, |i| i.overlap_skipped)
    }

    /// Most recent action results (up to 10).
    pub fn recent_actions(&self) -> Vec<ScheduleRecentAction> {
        self.raw
            .info
            .as_ref()
            .map(|i| {
                i.recent_actions
                    .iter()
                    .map(ScheduleRecentAction::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Currently-running workflows started by this schedule.
    pub fn running_actions(&self) -> Vec<ScheduleRunningAction> {
        self.raw
            .info
            .as_ref()
            .map(|i| {
                i.running_workflows
                    .iter()
                    .map(|w| ScheduleRunningAction {
                        workflow_id: w.workflow_id.clone(),
                        run_id: w.run_id.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Next scheduled action times.
    pub fn future_action_times(&self) -> Vec<SystemTime> {
        self.raw
            .info
            .as_ref()
            .map(|i| {
                i.future_action_times
                    .iter()
                    .filter_map(proto_ts_to_system_time)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// When the schedule was created.
    pub fn create_time(&self) -> Option<SystemTime> {
        self.raw
            .info
            .as_ref()
            .and_then(|i| i.create_time.as_ref())
            .and_then(proto_ts_to_system_time)
    }

    /// When the schedule was last updated.
    pub fn update_time(&self) -> Option<SystemTime> {
        self.raw
            .info
            .as_ref()
            .and_then(|i| i.update_time.as_ref())
            .and_then(proto_ts_to_system_time)
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &DescribeScheduleResponse {
        &self.raw
    }

    /// Access the raw proto mutably for modification (e.g., before calling
    /// [`into_update()`](Self::into_update)).
    pub fn raw_mut(&mut self) -> &mut DescribeScheduleResponse {
        &mut self.raw
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> DescribeScheduleResponse {
        self.raw
    }

    /// Convert this description into a [`ScheduleUpdate`] for use with
    /// [`ScheduleHandle::update()`].
    ///
    /// Extracts the schedule definition and conflict token. Modify the
    /// schedule via [`ScheduleUpdate::raw_mut()`] before passing to update.
    pub fn into_update(self) -> ScheduleUpdate {
        ScheduleUpdate {
            schedule: self.raw.schedule.unwrap_or_default(),
            conflict_token: self.raw.conflict_token,
        }
    }
}

impl From<DescribeScheduleResponse> for ScheduleDescription {
    fn from(raw: DescribeScheduleResponse) -> Self {
        Self { raw }
    }
}

/// Controls what happens when a scheduled workflow would overlap with a running one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum ScheduleOverlapPolicy {
    /// Use the server default (currently Skip).
    #[default]
    Unspecified = 0,
    /// Don't start a new workflow if one is already running.
    Skip = 1,
    /// Buffer one workflow start, to run after the current one completes.
    BufferOne = 2,
    /// Buffer all workflow starts, to run sequentially.
    BufferAll = 3,
    /// Cancel the running workflow and start a new one.
    CancelOther = 4,
    /// Terminate the running workflow and start a new one.
    TerminateOther = 5,
    /// Start any number of concurrent workflows.
    AllowAll = 6,
}

/// A backfill request for a schedule, specifying a time range of missed runs.
#[derive(Debug, Clone, bon::Builder)]
pub struct ScheduleBackfill {
    /// Start of the time range to backfill.
    pub start_time: SystemTime,
    /// End of the time range to backfill.
    pub end_time: SystemTime,
    /// How overlapping runs are handled during backfill.
    #[builder(default)]
    pub overlap_policy: ScheduleOverlapPolicy,
}

/// An update to apply to a schedule definition.
///
/// Obtain from [`ScheduleDescription::into_update()`], modify the schedule
/// using the setter methods, then pass to [`ScheduleHandle::update()`].
#[derive(Debug, Clone)]
pub struct ScheduleUpdate {
    schedule: schedule_proto::Schedule,
    conflict_token: Vec<u8>,
}

impl ScheduleUpdate {
    /// Replace the schedule spec (when to trigger).
    pub fn set_spec(&mut self, spec: ScheduleSpec) {
        self.schedule.spec = Some(spec.into_proto());
    }

    /// Replace the schedule action (what to do on trigger).
    pub fn set_action(&mut self, action: ScheduleAction) {
        self.schedule.action = Some(action.into_proto());
    }

    /// Set whether the schedule is paused.
    pub fn set_paused(&mut self, paused: bool) {
        self.state_mut().paused = paused;
    }

    /// Set the note on the schedule state.
    pub fn set_note(&mut self, note: impl Into<String>) {
        self.state_mut().notes = note.into();
    }

    /// Set the overlap policy.
    pub fn set_overlap_policy(&mut self, policy: ScheduleOverlapPolicy) {
        self.policies_mut().overlap_policy = policy as i32;
    }

    /// Set the catchup window. Actions missed by more than this duration are
    /// skipped.
    pub fn set_catchup_window(&mut self, window: Duration) {
        self.policies_mut().catchup_window = window.try_into().ok();
    }

    /// Set whether to pause the schedule when a workflow run fails or times out.
    pub fn set_pause_on_failure(&mut self, pause_on_failure: bool) {
        self.policies_mut().pause_on_failure = pause_on_failure;
    }

    /// Set whether to keep the original workflow ID without appending a
    /// timestamp.
    pub fn set_keep_original_workflow_id(&mut self, keep: bool) {
        self.policies_mut().keep_original_workflow_id = keep;
    }

    /// Access the raw schedule proto for fields not covered by setters.
    pub fn raw(&self) -> &schedule_proto::Schedule {
        &self.schedule
    }

    /// Access the raw schedule proto mutably for fields not covered by setters.
    pub fn raw_mut(&mut self) -> &mut schedule_proto::Schedule {
        &mut self.schedule
    }

    /// Consume and return the raw schedule proto.
    pub fn into_raw(self) -> schedule_proto::Schedule {
        self.schedule
    }

    /// The conflict token for optimistic concurrency.
    pub fn conflict_token(&self) -> &[u8] {
        &self.conflict_token
    }

    fn state_mut(&mut self) -> &mut schedule_proto::ScheduleState {
        self.schedule.state.get_or_insert_with(Default::default)
    }

    fn policies_mut(&mut self) -> &mut schedule_proto::SchedulePolicies {
        self.schedule.policies.get_or_insert_with(Default::default)
    }
}

/// Summary of a schedule returned in list operations.
///
/// Provides ergonomic accessors over the raw `ScheduleListEntry` proto.
/// Use [`raw()`](Self::raw) or [`into_raw()`](Self::into_raw) to access the
/// full proto when needed.
#[derive(Debug, Clone)]
pub struct ScheduleSummary {
    raw: schedule_proto::ScheduleListEntry,
}

impl ScheduleSummary {
    /// The schedule ID.
    pub fn schedule_id(&self) -> &str {
        &self.raw.schedule_id
    }

    /// The workflow type name for start-workflow actions.
    pub fn workflow_type(&self) -> Option<&str> {
        self.info()
            .and_then(|i| i.workflow_type.as_ref())
            .map(|wt| wt.name.as_str())
    }

    /// Notes on the schedule state.
    pub fn notes(&self) -> Option<&str> {
        self.info().map(|i| i.notes.as_str())
    }

    /// Whether the schedule is paused.
    pub fn paused(&self) -> bool {
        self.info().is_some_and(|i| i.paused)
    }

    /// Most recent action results (up to 10).
    pub fn recent_actions(&self) -> Vec<ScheduleRecentAction> {
        self.info()
            .map(|i| {
                i.recent_actions
                    .iter()
                    .map(ScheduleRecentAction::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Next scheduled action times.
    pub fn future_action_times(&self) -> Vec<SystemTime> {
        self.info()
            .map(|i| {
                i.future_action_times
                    .iter()
                    .filter_map(proto_ts_to_system_time)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &schedule_proto::ScheduleListEntry {
        &self.raw
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> schedule_proto::ScheduleListEntry {
        self.raw
    }

    fn info(&self) -> Option<&schedule_proto::ScheduleListInfo> {
        self.raw.info.as_ref()
    }
}

impl From<schedule_proto::ScheduleListEntry> for ScheduleSummary {
    fn from(raw: schedule_proto::ScheduleListEntry) -> Self {
        Self { raw }
    }
}

/// Handle to an existing schedule. Obtained from
/// [`Client::create_schedule`](crate::Client::create_schedule) or
/// [`Client::get_schedule_handle`](crate::Client::get_schedule_handle).
pub struct ScheduleHandle<CT> {
    client: CT,
    /// The namespace the schedule belongs to.
    pub namespace: String,
    /// The schedule ID.
    pub schedule_id: String,
}

impl<CT> ScheduleHandle<CT>
where
    CT: WorkflowService + NamespacedClient + Clone + Send + Sync,
{
    pub(crate) fn new(client: CT, namespace: String, schedule_id: String) -> Self {
        Self {
            client,
            namespace,
            schedule_id,
        }
    }

    /// Describe this schedule, returning its full definition, info, and conflict token.
    pub async fn describe(&self) -> Result<ScheduleDescription, ScheduleError> {
        let resp = WorkflowService::describe_schedule(
            &mut self.client.clone(),
            DescribeScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
            }
            .into_request(),
        )
        .await?
        .into_inner();

        Ok(ScheduleDescription::from(resp))
    }

    /// Update the schedule definition.
    ///
    /// Describes the current schedule, applies the closure to modify it, and
    /// sends the update. The conflict token is managed automatically.
    ///
    /// ```ignore
    /// handle.update(|u| {
    ///     u.set_note("updated");
    ///     u.set_paused(true);
    /// }).await?;
    /// ```
    pub async fn update(
        &self,
        updater: impl FnOnce(&mut ScheduleUpdate),
    ) -> Result<(), ScheduleError> {
        let desc = self.describe().await?;
        let mut update = desc.into_update();
        updater(&mut update);
        self.send_update(update).await
    }

    /// Send a pre-built [`ScheduleUpdate`] to the server.
    ///
    /// Prefer [`update()`](Self::update) for most use cases. Use this when you
    /// need to inspect the [`ScheduleDescription`] before deciding what to
    /// change.
    pub async fn send_update(&self, update: ScheduleUpdate) -> Result<(), ScheduleError> {
        WorkflowService::update_schedule(
            &mut self.client.clone(),
            UpdateScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                schedule: Some(update.schedule),
                conflict_token: update.conflict_token,
                identity: self.client.identity(),
                request_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }

    /// Delete this schedule.
    pub async fn delete(&self) -> Result<(), ScheduleError> {
        WorkflowService::delete_schedule(
            &mut self.client.clone(),
            DeleteScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                identity: self.client.identity(),
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }

    /// Pause the schedule with an optional note.
    pub async fn pause(&self, note: impl Into<String>) -> Result<(), ScheduleError> {
        WorkflowService::patch_schedule(
            &mut self.client.clone(),
            PatchScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                patch: Some(schedule_proto::SchedulePatch {
                    pause: note.into(),
                    ..Default::default()
                }),
                identity: self.client.identity(),
                request_id: Uuid::new_v4().to_string(),
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }

    /// Unpause the schedule with an optional note.
    pub async fn unpause(&self, note: impl Into<String>) -> Result<(), ScheduleError> {
        WorkflowService::patch_schedule(
            &mut self.client.clone(),
            PatchScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                patch: Some(schedule_proto::SchedulePatch {
                    unpause: note.into(),
                    ..Default::default()
                }),
                identity: self.client.identity(),
                request_id: Uuid::new_v4().to_string(),
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }

    /// Trigger the schedule to run immediately.
    pub async fn trigger(&self) -> Result<(), ScheduleError> {
        WorkflowService::patch_schedule(
            &mut self.client.clone(),
            PatchScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                patch: Some(schedule_proto::SchedulePatch {
                    trigger_immediately: Some(schedule_proto::TriggerImmediatelyRequest {
                        overlap_policy: 0,
                        scheduled_time: None,
                    }),
                    ..Default::default()
                }),
                identity: self.client.identity(),
                request_id: Uuid::new_v4().to_string(),
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }

    /// Request backfill of missed runs.
    pub async fn backfill(
        &self,
        backfills: Vec<ScheduleBackfill>,
    ) -> Result<(), ScheduleError> {
        let backfill_requests: Vec<schedule_proto::BackfillRequest> = backfills
            .into_iter()
            .map(|b| schedule_proto::BackfillRequest {
                start_time: Some(b.start_time.into()),
                end_time: Some(b.end_time.into()),
                overlap_policy: b.overlap_policy as i32,
            })
            .collect();
        WorkflowService::patch_schedule(
            &mut self.client.clone(),
            PatchScheduleRequest {
                namespace: self.namespace.clone(),
                schedule_id: self.schedule_id.clone(),
                patch: Some(schedule_proto::SchedulePatch {
                    backfill_request: backfill_requests,
                    ..Default::default()
                }),
                identity: self.client.identity(),
                request_id: Uuid::new_v4().to_string(),
            }
            .into_request(),
        )
        .await?;
        Ok(())
    }
}
