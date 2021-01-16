use crate::protos::temporal::api::history::v1::{
    WorkflowTaskCompletedEventAttributes, WorkflowTaskStartedEventAttributes,
};
use crate::protos::temporal::api::{
    enums::v1::EventType,
    history::{v1::history_event::Attributes, v1::HistoryEvent},
};
use std::time::SystemTime;

#[derive(Default, Debug)]
pub(crate) struct TestHistoryBuilder {
    events: Vec<HistoryEvent>,
    /// Is incremented every time a new event is added, and that *new* value is used as that event's
    /// id
    current_event_id: i64,
    workflow_task_scheduled_event_id: i64,
    previous_started_event_id: i64,
}

impl TestHistoryBuilder {
    /// Add an event by type with attributes. Bundles both into a [HistoryEvent] with an id that is
    /// incremented on each call to add.
    pub(crate) fn add(&mut self, event_type: EventType, attribs: Attributes) {
        self.build_and_push_event(event_type, Some(attribs));
    }

    /// Adds an event to the history by type, without attributes
    pub(crate) fn add_by_type(&mut self, event_type: EventType) {
        self.build_and_push_event(event_type.clone(), None);
    }

    /// Adds an event, returning the ID that was assigned to it
    pub(crate) fn add_get_event_id(
        &mut self,
        event_type: EventType,
        attrs: Option<Attributes>,
    ) -> i64 {
        self.build_and_push_event(event_type, attrs);
        self.current_event_id
    }

    /// Adds the following events:
    /// ```text
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// ```
    pub(crate) fn add_workflow_task(&mut self) {
        self.add_workflow_task_scheduled_and_started();
        self.add_workflow_task_completed();
    }

    pub(crate) fn add_workflow_task_scheduled_and_started(&mut self) {
        self.add_workflow_task_scheduled();
        self.add_workflow_task_started();
    }

    pub(crate) fn add_workflow_task_scheduled(&mut self) {
        // WFStarted always immediately follows WFScheduled
        self.previous_started_event_id = self.workflow_task_scheduled_event_id + 1;
        self.workflow_task_scheduled_event_id =
            self.add_get_event_id(EventType::WorkflowTaskScheduled, None);
    }

    pub(crate) fn add_workflow_task_started(&mut self) {
        let attrs = WorkflowTaskStartedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::WorkflowTaskStarted,
            Some(Attributes::WorkflowTaskStartedEventAttributes(attrs)),
        );
    }

    pub(crate) fn add_workflow_task_completed(&mut self) {
        let attrs = WorkflowTaskCompletedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::WorkflowTaskCompleted,
            Some(Attributes::WorkflowTaskCompletedEventAttributes(attrs)),
        );
    }

    /// Counts the number of whole workflow tasks. Looks for WFTaskStarted followed by
    /// WFTaskCompleted, adding one to the count for every match. It will additionally count
    /// a WFTaskStarted at the end of the event list.
    pub(crate) fn get_workflow_task_count(&self) -> usize {
        let mut last_wf_started_id = 0;
        let mut count = 0;
        for (i, event) in self.events.iter().enumerate() {
            let at_last_item = i == self.events.len() - 1;
            let next_item_is_wftc = self
                .events
                .get(i + 1)
                .map(|e| e.event_type == EventType::WorkflowTaskCompleted as i32)
                .unwrap_or(false);
            if event.event_type == EventType::WorkflowTaskStarted as i32
                && (at_last_item || next_item_is_wftc)
            {
                last_wf_started_id = event.event_id;
                count += 1;
            }
            if at_last_item {
                // No more events
                if last_wf_started_id != event.event_id {
                    panic!("Last item in history wasn't WorkflowTaskStarted")
                }
                return count;
            }
        }
        count
    }

    fn build_and_push_event(&mut self, event_type: EventType, attribs: Option<Attributes>) {
        self.current_event_id += 1;
        let evt = HistoryEvent {
            event_type: event_type as i32,
            event_id: self.current_event_id,
            event_time: Some(SystemTime::now().into()),
            attributes: attribs,
            ..Default::default()
        };
        self.events.push(evt);
    }
}
