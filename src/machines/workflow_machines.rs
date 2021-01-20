use crate::{
    machines::TemporalStateMachine,
    protos::temporal::api::{enums::v1::EventType, history::v1::HistoryEvent},
};
use std::collections::{hash_map::Entry, HashMap};

pub(super) struct WorkflowMachines {
    /// The event id of the last event in the history which is expected to be startedEventId unless
    /// it is replay from a JSON file.
    workflow_task_started_event_id: i64,
    /// EventId of the last handled WorkflowTaskStarted event
    current_started_event_id: i64,
    /// The event id of the started event of the last successfully executed workflow task
    previous_started_event_id: i64,
    /// True if the workflow is replaying from history
    replaying: bool,

    /// A mapping for accessing all the machines, where the key is the id of the initiating event
    /// for that machine.
    machines_by_id: HashMap<i64, Box<dyn TemporalStateMachine>>,
}

impl WorkflowMachines {
    /// Returns the id of the last seen WorkflowTaskStarted event
    pub(super) fn get_last_started_event_id(&self) -> i64 {
        self.current_started_event_id
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    pub(super) fn handle_event(&mut self, event: &HistoryEvent, has_next_event: bool) {
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

        match event
            .get_initial_command_event_id()
            .map(|id| self.machines_by_id.entry(id))
        {
            Some(Entry::Occupied(sme)) => {
                let sm = sme.get();
                sm.handle_event(event, has_next_event);
                if sm.is_final_state() {
                    sme.remove();
                }
            }
            Some(Entry::Vacant(_)) => {
                error!(
                    "During event handling, this event had an initial command ID but \
                     we could not find a matching state machine! Event: {:?}",
                    event
                );
            }
            _ => self.handle_non_stateful_event(event, has_next_event),
        }
    }

    fn handle_command_event(&self, event: &HistoryEvent) {
        unimplemented!()
    }

    fn handle_non_stateful_event(&self, event: &HistoryEvent, has_next_event: bool) {
        unimplemented!()
    }

    /// Given an event id (possibly zero) of the last successfully executed workflow task and an
    /// id of the last event, sets the ids internally and appropriately sets the replaying flag.
    pub(super) fn set_started_ids(
        &mut self,
        previous_started_event_id: i64,
        workflow_task_started_event_id: i64,
    ) {
        self.previous_started_event_id = previous_started_event_id;
        self.workflow_task_started_event_id = workflow_task_started_event_id;
        self.replaying = previous_started_event_id > 0;
    }
}
