use crate::{
    machines::{
        test_help::TestWorkflowDriver, timer_state_machine::new_timer,
        workflow_task_state_machine::WorkflowTaskMachine, CancellableCommand, DrivenWorkflow,
        MachineCommand, TemporalStateMachine, WFCommand,
    },
    protos::temporal::api::{
        command::v1::StartTimerCommandAttributes,
        enums::v1::{CommandType, EventType},
        history::v1::{history_event, HistoryEvent},
    },
};
use futures::{channel::mpsc::Sender, channel::oneshot, Future};
use rustfsm::StateMachine;
use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
    time::SystemTime,
};
use tracing::Level;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

pub(crate) struct WorkflowMachines {
    /// The event id of the last wf task started event in the history which is expected to be
    /// [current_started_event_id] except during replay.
    workflow_task_started_event_id: i64,
    /// EventId of the last handled WorkflowTaskStarted event
    current_started_event_id: i64,
    /// The event id of the started event of the last successfully executed workflow task
    previous_started_event_id: i64,
    /// True if the workflow is replaying from history
    replaying: bool,
    /// Identifies the current run and is used as a seed for faux-randomness.
    current_run_id: Option<String>,
    /// The current workflow time if it has been established
    current_wf_time: Option<SystemTime>,

    /// A mapping for accessing all the machines, where the key is the id of the initiating event
    /// for that machine.
    // TODO: Maybe value here should just be `CancellableCommand`
    machines_by_id: HashMap<i64, Rc<RefCell<dyn TemporalStateMachine>>>,

    /// Queued commands which have been produced by machines and await processing
    commands: VecDeque<CancellableCommand>,
    /// Commands generated by the currently processed workflow task. It is a queue as commands can
    /// be added (due to marker based commands) while iterating over already added commands.
    current_wf_task_commands: VecDeque<CancellableCommand>,

    /// The workflow that is being driven by this instance of the machines
    drive_me: Box<dyn DrivenWorkflow + 'static>,

    /// Holds channels for completing timers. Key is the ID of the timer.
    pub(super) timer_futures: HashMap<String, oneshot::Sender<bool>>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum WFMachinesError {
    #[error("Event {0:?} was not expected")]
    UnexpectedEvent(HistoryEvent),
    #[error("Event {0:?} was malformed: {1}")]
    MalformedEvent(HistoryEvent, String),
    #[error("Command type {0:?} was not expected")]
    UnexpectedCommand(CommandType),
    #[error("No command was scheduled for event {0:?}")]
    NoCommandScheduledForEvent(HistoryEvent),

    // TODO: This may not be the best thing to do here, tbd.
    #[error("Underlying error {0:?}")]
    Underlying(#[from] anyhow::Error),
}

impl WorkflowMachines {
    pub(super) fn new(driven_wf: Box<dyn DrivenWorkflow>) -> Self {
        Self {
            drive_me: driven_wf,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            workflow_task_started_event_id: 0,
            current_started_event_id: 0,
            previous_started_event_id: 0,
            replaying: false,
            current_run_id: None,
            current_wf_time: None,
            machines_by_id: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
            timer_futures: Default::default(),
        }
    }

    /// Create a new timer for this workflow with the provided attributes
    ///
    /// Returns the command and a future that will resolve when the timer completes
    pub(super) fn new_timer(
        &mut self,
        attribs: StartTimerCommandAttributes,
    ) -> (CancellableCommand, impl Future) {
        let timer_id = attribs.timer_id.clone();
        let timer = new_timer(attribs);
        let (tx, rx) = oneshot::channel();
        self.timer_futures.insert(timer_id, tx);
        (timer, rx)
    }

    /// Returns the id of the last seen WorkflowTaskStarted event
    pub(super) fn get_last_started_event_id(&self) -> i64 {
        self.current_started_event_id
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    #[instrument(skip(self))]
    pub(crate) fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        if event.is_command_event() {
            self.handle_command_event(event)?;
            return Ok(());
        }
        let event_type = EventType::from_i32(event.event_type)
            .ok_or_else(|| WFMachinesError::UnexpectedEvent(event.clone()))?;

        if self.replaying
            && self.current_started_event_id >= self.previous_started_event_id
            && event_type != EventType::WorkflowTaskCompleted
        {
            // Replay is finished
            self.replaying = false;
        }

        match event.get_initial_command_event_id() {
            Some(initial_cmd_id) => {
                if let Some(sm) = self.machines_by_id.get(&initial_cmd_id) {
                    (*sm.clone())
                        .borrow_mut()
                        .handle_event(event, has_next_event, self)?;
                } else {
                    event!(
                        Level::ERROR,
                        msg = "During event handling, this event had an initial command ID but \
                     we could not find a matching state machine! Event: {:?}",
                        ?event
                    );
                }

                // Have to fetch machine again here to avoid borrowing self mutably twice
                if let Some(sm) = self.machines_by_id.get_mut(&initial_cmd_id) {
                    if sm.borrow().is_final_state() {
                        self.machines_by_id.remove(&initial_cmd_id);
                    }
                }
            }
            None => self.handle_non_stateful_event(event, has_next_event)?,
        }

        Ok(())
    }

    /// Called when we want to run the event loop because a workflow task started event has
    /// triggered
    pub(super) fn task_started(
        &mut self,
        task_started_event_id: i64,
        time: SystemTime,
    ) -> Result<()> {
        let s = span!(Level::DEBUG, "Task started trigger");
        let _enter = s.enter();

        // TODO: Seems to only matter for version machine. Figure out then.
        // // If some new commands are pending and there are no more command events.
        // for (CancellableCommand cancellableCommand : commands) {
        //     if (cancellableCommand == null) {
        //         break;
        //     }
        //     cancellableCommand.handleWorkflowTaskStarted();
        // }

        // TODO: Local activity machines
        // // Give local activities a chance to recreate their requests if they were lost due
        // // to the last workflow task failure. The loss could happen only the last workflow task
        // // was forcibly created by setting forceCreate on RespondWorkflowTaskCompletedRequest.
        // if (nonProcessedWorkflowTask) {
        //     for (LocalActivityStateMachine value : localActivityMap.values()) {
        //         value.nonReplayWorkflowTaskStarted();
        //     }
        // }

        self.current_started_event_id = task_started_event_id;
        self.set_current_time(time);
        self.event_loop()?;
        Ok(())
    }

    /// A command event is an event which is generated from a command emitted by a past decision.
    /// Each command has a correspondent event. For example ScheduleActivityTaskCommand is recorded
    /// to the history as ActivityTaskScheduledEvent.
    ///
    /// Command events always follow WorkflowTaskCompletedEvent.
    ///
    /// The handling consists of verifying that the next command in the commands queue is associated
    /// with a state machine, which is then notified about the event and the command is removed from
    /// the commands queue.
    fn handle_command_event(&mut self, event: &HistoryEvent) -> Result<()> {
        // TODO: Local activity handling stuff
        //     if (handleLocalActivityMarker(event)) {
        //       return;
        //     }

        let mut maybe_command;
        let consumed_cmd = loop {
            // handleVersionMarker can skip a marker event if the getVersion call was removed.
            // In this case we don't want to consume a command.
            // That's why front is used instead of pop_front
            maybe_command = self.commands.front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::NoCommandScheduledForEvent(event.clone()));
            };

            // Feed the machine the event
            let mut break_later = false;
            if let CancellableCommand::Active { mut machine, .. } = command.clone() {
                let out_commands = (*machine).borrow_mut().handle_event(event, true, self)?;
                // TODO: Handle invalid event errors
                //  * More special handling for version machine - see java
                //  * Command/machine supposed to have cancelled itself

                event!(
                    Level::DEBUG,
                    msg = "Machine produced commands",
                    ?out_commands
                );
                break_later = true;
            }

            // Consume command
            let cmd = self.commands.pop_front();
            if break_later {
                // Unwrap OK since we would have already exited if not present. TODO: Cleanup
                break cmd.unwrap();
            }
        };

        // TODO: validate command

        if let CancellableCommand::Active { machine, .. } = consumed_cmd {
            if !machine.borrow().is_final_state() {
                self.machines_by_id.insert(event.event_id, machine);
            }
        }

        Ok(())
    }

    fn handle_non_stateful_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        match EventType::from_i32(event.event_type) {
            Some(EventType::WorkflowExecutionStarted) => {
                if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                    attrs,
                )) = &event.attributes
                {
                    self.current_run_id = Some(attrs.original_execution_run_id.clone());
                    let results = self.drive_me.start(attrs.clone())?;
                    self.handle_driven_results(results);
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
                wf_task_sm.handle_event(event, has_next_event, self)?;
                self.machines_by_id
                    .insert(event.event_id, Rc::new(RefCell::new(wf_task_sm)));
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

    /// Fetches commands ready for processing from the state machines
    pub(crate) fn get_commands(&mut self) -> Vec<MachineCommand> {
        self.commands
            .iter()
            .filter_map(|c| {
                if let CancellableCommand::Active { command, .. } = c {
                    Some(command.clone())
                } else {
                    None
                }
            })
            .collect()
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

    fn set_current_time(&mut self, time: SystemTime) -> SystemTime {
        if self.current_wf_time.map(|t| t < time).unwrap_or(true) {
            self.current_wf_time = Some(time);
        }
        self.current_wf_time
            .expect("We have just ensured this is populated")
    }

    fn event_loop(&mut self) -> Result<()> {
        let results = self.drive_me.iterate_wf()?;
        self.handle_driven_results(results);

        self.prepare_commands();
        Ok(())
    }

    fn handle_driven_results(&mut self, results: Vec<WFCommand>) {
        for cmd in results {
            match cmd {
                WFCommand::Add(cc) => self.current_wf_task_commands.push_back(cc),
            }
        }
    }

    fn prepare_commands(&mut self) {
        while let Some(c) = self.current_wf_task_commands.pop_front() {
            // TODO - some special case stuff that can maybe be managed differently?
            //   handleCommand should be called even on canceled ones to support mutableSideEffect
            //   command.handleCommand(command.getCommandType());
            self.commands.push_back(c);
        }
    }
}
