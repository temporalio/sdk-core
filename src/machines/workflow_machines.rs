use crate::machines::TemporalStateMachine;
use crate::protos::temporal::api::{enums::v1::EventType, history::v1::HistoryEvent};
use std::collections::HashMap;

struct WorkflowMachines {
    /// The event id of the last event in the history which is expected to be startedEventId unless
    /// it is replay from a JSON file.
    workflow_task_started_event_id: u64,
    /// EventId of the last handled WorkflowTaskStartedEvent
    current_started_event_id: u64,
    /// The event id of the started event of the last successfully executed workflow task
    previous_started_event_id: u64,
    /// True if the workflow is replaying from history
    replaying: bool,

    machines_by_id: HashMap<u64, Box<dyn TemporalStateMachine>>,
}

impl WorkflowMachines {
    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    fn handle_event(&mut self, event: HistoryEvent, has_next_event: bool) {
        if event.is_command_event() {
            self.handle_command_event(event);
            return;
        }

        if self.replaying
            && self.current_started_event_id >= self.previous_started_event_id
            && event.event_type != EventType::WorkflowTaskCompleted as i32
        {
            // Replay is finished
            self.replaying = false;
        }

        if let Some(initial_command_event_id) = event.get_initial_command_event_id() {
            // EntityStateMachine c = stateMachines.get(initialCommandEventId);
            //     c.handle_event(event, hasNextEvent);
            //     if (c.is_final_state()) {
            //         stateMachines.remove(initialCommandEventId);
            //     }
            unimplemented!()
        } else {
            self.handle_non_stateful_event(event, has_next_event)
        }
    }

    fn handle_command_event(&self, event: HistoryEvent) {
        unimplemented!()
    }

    fn handle_non_stateful_event(&self, event: HistoryEvent, has_next_event: bool) {
        unimplemented!()
    }

    /// Given an event id (possibly zero) of the last successfully executed workflow task and an
    /// id of the last event, sets the ids internally and appropriately sets the replaying flag.
    fn set_started_ids(
        &mut self,
        previous_started_event_id: u64,
        workflow_task_started_event_id: u64,
    ) {
        self.previous_started_event_id = previous_started_event_id;
        self.workflow_task_started_event_id = workflow_task_started_event_id;
        self.replaying = previous_started_event_id > 0;
    }
}
