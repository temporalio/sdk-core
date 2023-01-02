use crate::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME},
    coresdk::{
        common::{
            build_has_change_marker_details, build_local_activity_marker_details,
            NamespacedWorkflowExecution,
        },
        external_data::LocalActivityMarkerData,
        IntoPayloadsExt,
    },
    temporal::api::{
        common::v1::{Payload, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{EventType, TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::{failure, CanceledFailureInfo, Failure},
        history::v1::{history_event::Attributes, *},
        taskqueue::v1::TaskQueue,
    },
    HistoryInfo,
};
use anyhow::bail;
use prost_types::Timestamp;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

pub static DEFAULT_WORKFLOW_TYPE: &str = "default_wf_type";

type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

#[derive(Default, Clone, Debug)]
pub struct TestHistoryBuilder {
    events: Vec<HistoryEvent>,
    /// Is incremented every time a new event is added, and that *new* value is used as that event's
    /// id
    current_event_id: i64,
    workflow_task_scheduled_event_id: i64,
    final_workflow_task_started_event_id: i64,
    previous_task_completed_id: i64,
    original_run_id: String,
}

impl TestHistoryBuilder {
    pub fn from_history(events: Vec<HistoryEvent>) -> Self {
        let find_matching_id = |etype: EventType| {
            events
                .iter()
                .rev()
                .find(|e| e.event_type() == etype)
                .map(|e| e.event_id)
                .unwrap_or_default()
        };
        Self {
            current_event_id: events.last().map(|e| e.event_id).unwrap_or_default(),
            workflow_task_scheduled_event_id: find_matching_id(EventType::WorkflowTaskScheduled),
            final_workflow_task_started_event_id: find_matching_id(EventType::WorkflowTaskStarted),
            previous_task_completed_id: find_matching_id(EventType::WorkflowTaskCompleted),
            original_run_id: extract_original_run_id_from_events(&events)
                .expect("Run id must be discoverable")
                .to_string(),
            events,
        }
    }

    /// Add an event by type with attributes. Bundles both into a [HistoryEvent] with an id that is
    /// incremented on each call to add.
    pub fn add(&mut self, event_type: EventType, attribs: Attributes) {
        self.build_and_push_event(event_type, attribs);
    }

    /// Adds an event to the history by type, with default attributes.
    pub fn add_by_type(&mut self, event_type: EventType) {
        let attribs =
            default_attribs(event_type).expect("Couldn't make default attributes in test builder");
        self.build_and_push_event(event_type, attribs);
    }

    /// Adds an event, returning the ID that was assigned to it
    pub fn add_get_event_id(&mut self, event_type: EventType, attrs: Option<Attributes>) -> i64 {
        if let Some(a) = attrs {
            self.build_and_push_event(event_type, a);
        } else {
            self.add_by_type(event_type);
        }
        self.current_event_id
    }

    /// Adds the following events:
    /// ```text
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// ```
    pub fn add_full_wf_task(&mut self) {
        self.add_workflow_task_scheduled_and_started();
        self.add_workflow_task_completed();
    }

    pub fn add_workflow_task_scheduled_and_started(&mut self) {
        self.add_workflow_task_scheduled();
        self.add_workflow_task_started();
    }

    pub fn add_workflow_task_scheduled(&mut self) {
        self.workflow_task_scheduled_event_id =
            self.add_get_event_id(EventType::WorkflowTaskScheduled, None);
    }

    pub fn add_workflow_task_started(&mut self) {
        let attrs = WorkflowTaskStartedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.final_workflow_task_started_event_id =
            self.add_get_event_id(EventType::WorkflowTaskStarted, Some(attrs.into()));
    }

    pub fn add_workflow_task_completed(&mut self) {
        let attrs = WorkflowTaskCompletedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        let id = self.add_get_event_id(EventType::WorkflowTaskCompleted, Some(attrs.into()));
        self.previous_task_completed_id = id;
    }

    pub fn add_workflow_task_timed_out(&mut self) {
        let attrs = WorkflowTaskTimedOutEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskTimedOut, attrs.into());
    }

    pub fn add_workflow_execution_completed(&mut self) {
        let attrs = WorkflowExecutionCompletedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionCompleted, attrs.into());
    }

    pub fn add_workflow_execution_terminated(&mut self) {
        let attrs = WorkflowExecutionTerminatedEventAttributes {
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionTerminated, attrs.into());
    }

    pub fn add_workflow_execution_timed_out(&mut self) {
        let attrs = WorkflowExecutionTimedOutEventAttributes {
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionTimedOut, attrs.into());
    }

    pub fn add_workflow_execution_failed(&mut self) {
        let attrs = WorkflowExecutionFailedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionFailed, attrs.into());
    }

    pub fn add_continued_as_new(&mut self) {
        let attrs = WorkflowExecutionContinuedAsNewEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionContinuedAsNew, attrs.into());
    }

    pub fn add_cancel_requested(&mut self) {
        let attrs = WorkflowExecutionCancelRequestedEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionCancelRequested, attrs.into());
    }

    pub fn add_cancelled(&mut self) {
        let attrs = WorkflowExecutionCanceledEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionCanceled, attrs.into());
    }

    pub fn add_activity_task_scheduled(&mut self, activity_id: impl Into<String>) -> i64 {
        self.add_get_event_id(
            EventType::ActivityTaskScheduled,
            Some(
                history_event::Attributes::ActivityTaskScheduledEventAttributes(
                    ActivityTaskScheduledEventAttributes {
                        activity_id: activity_id.into(),
                        ..Default::default()
                    },
                ),
            ),
        )
    }
    pub fn add_activity_task_started(&mut self, scheduled_event_id: i64) -> i64 {
        self.add_get_event_id(
            EventType::ActivityTaskStarted,
            Some(
                history_event::Attributes::ActivityTaskStartedEventAttributes(
                    ActivityTaskStartedEventAttributes {
                        scheduled_event_id,
                        ..Default::default()
                    },
                ),
            ),
        )
    }

    pub fn add_activity_task_completed(
        &mut self,
        scheduled_event_id: i64,
        started_event_id: i64,
        payload: Payload,
    ) {
        self.add(
            EventType::ActivityTaskCompleted,
            history_event::Attributes::ActivityTaskCompletedEventAttributes(
                ActivityTaskCompletedEventAttributes {
                    scheduled_event_id,
                    started_event_id,
                    result: vec![payload].into_payloads(),
                    ..Default::default()
                },
            ),
        );
    }

    pub fn add_activity_task_cancel_requested(&mut self, scheduled_event_id: i64) {
        let attrs = ActivityTaskCancelRequestedEventAttributes {
            scheduled_event_id,
            workflow_task_completed_event_id: self.previous_task_completed_id,
        };
        self.build_and_push_event(EventType::ActivityTaskCancelRequested, attrs.into());
    }

    pub fn add_workflow_task_failed_with_failure(
        &mut self,
        cause: WorkflowTaskFailedCause,
        failure: Failure,
    ) {
        let attrs = WorkflowTaskFailedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            cause: cause.into(),
            failure: Some(failure),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskFailed, attrs.into());
    }

    pub fn add_workflow_task_failed_new_id(
        &mut self,
        cause: WorkflowTaskFailedCause,
        new_run_id: &str,
    ) {
        let attrs = WorkflowTaskFailedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            cause: cause.into(),
            new_run_id: new_run_id.into(),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskFailed, attrs.into());
    }

    pub fn add_timer_fired(&mut self, timer_started_evt_id: i64, timer_id: String) {
        self.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_evt_id,
                timer_id,
            }),
        );
    }

    pub fn add_we_signaled(&mut self, signal_name: &str, payloads: Vec<Payload>) {
        let attrs = WorkflowExecutionSignaledEventAttributes {
            signal_name: signal_name.to_string(),
            input: Some(Payloads { payloads }),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionSignaled, attrs.into());
    }

    pub fn add_has_change_marker(&mut self, patch_id: &str, deprecated: bool) {
        let attrs = MarkerRecordedEventAttributes {
            marker_name: PATCH_MARKER_NAME.to_string(),
            details: build_has_change_marker_details(patch_id, deprecated),
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::MarkerRecorded, attrs.into());
    }

    pub fn add_local_activity_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Option<Payload>,
        failure: Option<Failure>,
        detail_mutator: impl FnOnce(&mut LocalActivityMarkerData),
    ) {
        let mut lamd = LocalActivityMarkerData {
            seq,
            attempt: 1,
            activity_id: activity_id.to_string(),
            activity_type: "some_act_type".to_string(),
            complete_time: None,
            backoff: None,
            original_schedule_time: None,
        };
        detail_mutator(&mut lamd);
        let attrs = MarkerRecordedEventAttributes {
            marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_string(),
            details: build_local_activity_marker_details(lamd, payload),
            workflow_task_completed_event_id: self.previous_task_completed_id,
            failure,
            ..Default::default()
        };
        self.build_and_push_event(EventType::MarkerRecorded, attrs.into());
    }

    pub fn add_local_activity_result_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Payload,
    ) {
        self.add_local_activity_marker(seq, activity_id, Some(payload), None, |_| {});
    }

    pub fn add_local_activity_result_marker_with_time(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Payload,
        complete_time: Timestamp,
    ) {
        self.add_local_activity_marker(seq, activity_id, Some(payload), None, |d| {
            d.complete_time = Some(complete_time)
        });
    }

    pub fn add_local_activity_fail_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        failure: Failure,
    ) {
        self.add_local_activity_marker(seq, activity_id, None, Some(failure), |_| {});
    }

    pub fn add_local_activity_cancel_marker(&mut self, seq: u32, activity_id: &str) {
        self.add_local_activity_marker(
            seq,
            activity_id,
            None,
            Some(Failure {
                message: "cancelled bro".to_string(),
                source: "".to_string(),
                stack_trace: "".to_string(),
                cause: None,
                failure_info: Some(failure::FailureInfo::CanceledFailureInfo(
                    CanceledFailureInfo { details: None },
                )),
                encoded_attributes: Default::default(),
            }),
            |_| {},
        );
    }

    pub fn add_signal_wf(
        &mut self,
        signal_name: impl Into<String>,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> i64 {
        let attrs = SignalExternalWorkflowExecutionInitiatedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            workflow_execution: Some(WorkflowExecution {
                workflow_id: workflow_id.into(),
                run_id: run_id.into(),
            }),
            signal_name: signal_name.into(),
            control: "".to_string(),
            ..Default::default()
        };
        self.add_get_event_id(
            EventType::SignalExternalWorkflowExecutionInitiated,
            Some(attrs.into()),
        )
    }

    pub fn add_external_signal_completed(&mut self, initiated_id: i64) {
        let attrs = ExternalWorkflowExecutionSignaledEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::ExternalWorkflowExecutionSignaled, attrs.into());
    }

    pub fn add_external_signal_failed(&mut self, initiated_id: i64) {
        let attrs = SignalExternalWorkflowExecutionFailedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::SignalExternalWorkflowExecutionFailed,
            attrs.into(),
        );
    }

    pub fn add_cancel_external_wf(&mut self, execution: NamespacedWorkflowExecution) -> i64 {
        let attrs = RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            namespace: execution.namespace,
            workflow_execution: Some(WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            }),
            ..Default::default()
        };
        self.add_get_event_id(
            EventType::RequestCancelExternalWorkflowExecutionInitiated,
            Some(attrs.into()),
        )
    }

    pub fn add_cancel_external_wf_completed(&mut self, initiated_id: i64) {
        let attrs = ExternalWorkflowExecutionCancelRequestedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::ExternalWorkflowExecutionCancelRequested,
            attrs.into(),
        );
    }

    pub fn add_cancel_external_wf_failed(&mut self, initiated_id: i64) {
        let attrs = RequestCancelExternalWorkflowExecutionFailedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::RequestCancelExternalWorkflowExecutionFailed,
            attrs.into(),
        );
    }

    pub fn add_wfe_started_with_wft_timeout(&mut self, dur: Duration) {
        let mut wesattrs = default_wes_attribs();
        wesattrs.workflow_task_timeout = Some(dur.try_into().unwrap());
        self.add(EventType::WorkflowExecutionStarted, wesattrs.into());
    }

    pub fn get_orig_run_id(&self) -> &str {
        &self.original_run_id
    }

    /// Iterates over the events in this builder to return a [HistoryInfo] including events up to
    /// the provided `to_wf_task_num`
    pub fn get_history_info(&self, to_wf_task_num: usize) -> Result<HistoryInfo, anyhow::Error> {
        HistoryInfo::new_from_history(&self.events.clone().into(), Some(to_wf_task_num))
    }

    /// Iterates over the events in this builder to return a [HistoryInfo] representing *all*
    /// events in the history
    pub fn get_full_history_info(&self) -> Result<HistoryInfo, anyhow::Error> {
        HistoryInfo::new_from_history(&self.events.clone().into(), None)
    }

    pub fn get_one_wft(&self, from_wft_number: usize) -> Result<HistoryInfo, anyhow::Error> {
        let mut histinfo =
            HistoryInfo::new_from_history(&self.events.clone().into(), Some(from_wft_number))?;
        histinfo.make_incremental();
        Ok(histinfo)
    }

    /// Return most recent wft start time or panic if unset
    pub fn wft_start_time(&self) -> Timestamp {
        self.events[(self.workflow_task_scheduled_event_id + 1) as usize]
            .event_time
            .clone()
            .unwrap()
    }

    /// Alter the workflow type of the history
    pub fn set_wf_type(&mut self, name: &str) {
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
            self.events.get_mut(0).and_then(|e| e.attributes.as_mut())
        {
            wes.workflow_type = Some(WorkflowType {
                name: name.to_string(),
            })
        }
    }

    /// Alter some specific event. You can easily craft nonsense histories this way, use carefully.
    pub fn modify_event(&mut self, event_id: i64, modifier: impl FnOnce(&mut HistoryEvent)) {
        let he = self
            .events
            .get_mut((event_id - 1) as usize)
            .expect("Event must be present");
        modifier(he);
    }

    fn build_and_push_event(&mut self, event_type: EventType, attribs: Attributes) {
        self.current_event_id += 1;
        let evt = HistoryEvent {
            event_type: event_type as i32,
            event_id: self.current_event_id,
            event_time: Some(SystemTime::now().into()),
            attributes: Some(attribs),
            ..Default::default()
        };
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(
            WorkflowExecutionStartedEventAttributes {
                original_execution_run_id,
                ..
            },
        )) = &evt.attributes
        {
            self.original_run_id = original_execution_run_id.clone();
        };
        self.events.push(evt);
    }
}

fn default_attribs(et: EventType) -> Result<Attributes> {
    Ok(match et {
        EventType::WorkflowExecutionStarted => default_wes_attribs().into(),
        EventType::WorkflowTaskScheduled => WorkflowTaskScheduledEventAttributes::default().into(),
        EventType::TimerStarted => TimerStartedEventAttributes::default().into(),
        _ => bail!("Don't know how to construct default attrs for {:?}", et),
    })
}

pub fn default_wes_attribs() -> WorkflowExecutionStartedEventAttributes {
    WorkflowExecutionStartedEventAttributes {
        original_execution_run_id: Uuid::new_v4().to_string(),
        workflow_type: Some(WorkflowType {
            name: DEFAULT_WORKFLOW_TYPE.to_owned(),
        }),
        workflow_task_timeout: Some(
            Duration::from_secs(5)
                .try_into()
                .expect("5 secs is a valid duration"),
        ),
        task_queue: Some(TaskQueue {
            name: "q".to_string(),
            kind: TaskQueueKind::Normal as i32,
        }),
        ..Default::default()
    }
}
