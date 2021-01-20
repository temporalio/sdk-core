use crate::{
    machines::{
        workflow_task_state_machine::WorkflowTaskMachine, CancellableCommand, MachineCommand,
        TemporalStateMachine,
    },
    protos::temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{history_event, HistoryEvent},
    },
};
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::error::Error;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

#[derive(Default)]
pub(crate) struct WorkflowMachines {
    /// The event id of the last event in the history which is expected to be startedEventId unless
    /// it is replay from a JSON file.
    workflow_task_started_event_id: i64,
    /// EventId of the last handled WorkflowTaskStarted event
    current_started_event_id: i64,
    /// The event id of the started event of the last successfully executed workflow task
    previous_started_event_id: i64,
    /// True if the workflow is replaying from history
    replaying: bool,
    /// Identifies the current run and is used as a seed for faux-randomness.
    current_run_id: String,

    /// A mapping for accessing all the machines, where the key is the id of the initiating event
    /// for that machine.
    machines_by_id: HashMap<i64, Box<dyn TemporalStateMachine>>,

    /// Queued commands which have been produced by machines and await processing
    commands: VecDeque<CancellableCommand>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WFMachinesError {
    #[error("Event {0:?} was not expected")]
    UnexpectedEvent(HistoryEvent),
    #[error("Event {0:?} was malformed: {1}")]
    MalformedEvent(HistoryEvent, String),
    #[error("Command type {0:?} was not expected")]
    UnexpectedCommand(CommandType),
    // TODO: Pretty sure can remove this if no machines need some specific error
    #[error("Underlying machine error {0:?}")]
    Underlying(#[from] Box<dyn Error + Send + Sync>),
}

impl WorkflowMachines {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the id of the last seen WorkflowTaskStarted event
    pub(super) fn get_last_started_event_id(&self) -> i64 {
        self.current_started_event_id
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    pub(crate) fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        if event.is_command_event() {
            self.handle_command_event(event);
            return Ok(());
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
            Some(Entry::Occupied(mut sme)) => {
                let sm = sme.get_mut();
                sm.handle_event(event, has_next_event)?;
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
            _ => self.handle_non_stateful_event(event, has_next_event)?,
        }

        Ok(())
    }

    fn handle_command_event(&mut self, _event: &HistoryEvent) {
        unimplemented!()
    }

    fn handle_non_stateful_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        // switch (event.getEventType()) {
        //     case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        //         this.currentRunId =
        //         event.getWorkflowExecutionStartedEventAttributes().getOriginalExecutionRunId();
        //     callbacks.start(event);
        //     break;
        //     case EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        //         WorkflowTaskStateMachine c =
        //         WorkflowTaskStateMachine.newInstance(
        //             workflowTaskStartedEventId, new WorkflowTaskCommandsListener());
        //     c.handleEvent(event, hasNextEvent);
        //     stateMachines.put(event.getEventId(), c);
        //     break;
        //     case EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
        //         callbacks.signal(event);
        //     break;
        //     case EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
        //         callbacks.cancel(event);
        //     break;
        //     case UNRECOGNIZED:
        //     break;
        //     default:
        //         throw new IllegalArgumentException("Unexpected event:" + event);
        // }
        match EventType::from_i32(event.event_type) {
            Some(EventType::WorkflowExecutionStarted) => {
                if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                    attrs,
                )) = &event.attributes
                {
                    self.current_run_id = attrs.original_execution_run_id.clone();
                // TODO: callbacks.start(event) -- but without the callbacks ;)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        event.clone(),
                        "WorkflowExecutionStarted event did not have appropriate attributes"
                            .to_string(),
                    ));
                }
            }
            Some(EventType::WorkflowTaskScheduled) => {
                let mut wf_task_sm = WorkflowTaskMachine::new(self.workflow_task_started_event_id);
                wf_task_sm.handle_event(event, has_next_event)?;
                self.machines_by_id
                    .insert(event.event_id, Box::new(wf_task_sm));
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                // TODO: Signal callbacks
            }
            Some(EventType::WorkflowExecutionCancelRequested) => {
                // TODO: Cancel callbacks
            }
            _ => return Err(WFMachinesError::UnexpectedEvent(event.clone())),
        }
        Ok(())
    }

    /// Fetches commands ready for processing from the state machines, removing them from the
    /// internal command queue.
    pub(crate) fn take_commands(&mut self) -> Vec<MachineCommand> {
        self.commands.drain(0..).flat_map(|c| c.command).collect()
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
