use crate::{
    machines::{
        complete_workflow_state_machine::complete_workflow, timer_state_machine::new_timer,
        workflow_task_state_machine::WorkflowTaskMachine, ActivationListener, CancellableCommand,
        DrivenWorkflow, ProtoCommand, TemporalStateMachine, WFCommand,
    },
    protos::coresdk::WfActivationJob,
    protos::{
        coresdk::{wf_activation_job, StartWorkflowTaskAttributes, WfActivation},
        temporal::api::{
            command::v1::StartTimerCommandAttributes,
            common::v1::WorkflowExecution,
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent},
        },
    },
};
use futures::Future;
use rustfsm::StateMachine;
use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    ops::DerefMut,
    sync::{atomic::AtomicBool, Arc},
    time::SystemTime,
};
use tracing::Level;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// Handles all the logic for driving a workflow. It orchestrates many state machines that together
/// comprise the logic of an executing workflow. One instance will exist per currently executing
/// (or cached) workflow on the worker.
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
    /// Workflow identifier
    pub workflow_id: String,
    /// Identifies the current run and is used as a seed for faux-randomness.
    pub run_id: String,
    /// The current workflow time if it has been established
    current_wf_time: Option<SystemTime>,

    /// A mapping for accessing all the machines, where the key is the id of the initiating event
    /// for that machine.
    machines_by_id: HashMap<i64, Box<dyn TemporalStateMachine>>,

    /// Queued commands which have been produced by machines and await processing
    commands: VecDeque<CancellableCommand>,
    /// Commands generated by the currently processing workflow task.
    ///
    /// Old note: It is a queue as commands can be added (due to marker based commands) while
    /// iterating over already added commands.
    current_wf_task_commands: VecDeque<CancellableCommand>,
    /// Outgoing activation jobs that need to be sent to the lang sdk
    outgoing_wf_activation_jobs: VecDeque<wf_activation_job::Attributes>,

    /// The workflow that is being driven by this instance of the machines
    drive_me: Box<dyn DrivenWorkflow + 'static>,
}

/// Returned by [TemporalStateMachine]s when handling events
#[derive(Debug, derive_more::From)]
#[must_use]
pub(super) enum WorkflowTrigger {
    PushWFJob(#[from(forward)] wf_activation_job::Attributes),
    TriggerWFTaskStarted {
        task_started_event_id: i64,
        time: SystemTime,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum WFMachinesError {
    #[error("Event {0:?} was not expected: {1}")]
    UnexpectedEvent(HistoryEvent, &'static str),
    #[error("Event {0:?} was malformed: {1}")]
    MalformedEvent(HistoryEvent, String),
    // Expected to be transformed into a `MalformedEvent` with the full event by workflow machines,
    // when emitted by a sub-machine
    #[error("{0}")]
    MalformedEventDetail(String),
    #[error("Command type {0:?} was not expected")]
    UnexpectedCommand(CommandType),
    #[error("No command was scheduled for event {0:?}")]
    NoCommandScheduledForEvent(HistoryEvent),

    #[error("Underlying error {0:?}")]
    Underlying(#[from] anyhow::Error),
}

impl WorkflowMachines {
    pub(crate) fn new(
        workflow_id: String,
        run_id: String,
        driven_wf: Box<dyn DrivenWorkflow>,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            drive_me: driven_wf,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            workflow_task_started_event_id: 0,
            current_started_event_id: 0,
            previous_started_event_id: 0,
            replaying: false,
            current_wf_time: None,
            machines_by_id: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
            outgoing_wf_activation_jobs: Default::default(),
        }
    }

    /// Create a new timer for this workflow with the provided attributes and sender. The sender
    /// is sent `true` when the timer completes.
    ///
    /// Returns the command and a future that will resolve when the timer completes
    pub(super) fn new_timer(&mut self, attribs: StartTimerCommandAttributes) -> CancellableCommand {
        new_timer(attribs)
    }

    /// Returns the id of the last seen WorkflowTaskStarted event
    pub(crate) fn get_last_started_event_id(&self) -> i64 {
        self.current_started_event_id
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    #[instrument(level = "debug", skip(self))]
    pub(crate) fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        if event.is_command_event() {
            self.handle_command_event(event)?;
            return Ok(());
        }
        let event_type = EventType::from_i32(event.event_type).ok_or_else(|| {
            WFMachinesError::UnexpectedEvent(event.clone(), "The event type is unknown")
        })?;

        if self.replaying
            && self.current_started_event_id >= self.previous_started_event_id
            && event_type != EventType::WorkflowTaskCompleted
        {
            // Replay is finished
            self.replaying = false;
        }

        match event.get_initial_command_event_id() {
            Some(initial_cmd_id) => {
                // We remove the machine while we it handles events, then return it, to avoid
                // borrowing from ourself mutably.
                let mut maybe_machine = self.machines_by_id.remove(&initial_cmd_id);
                if let Some(mut sm) = maybe_machine.as_mut() {
                    self.submachine_handle_event((*sm).borrow_mut(), event, has_next_event)?;
                } else {
                    event!(
                        Level::ERROR,
                        msg = "During event handling, this event had an initial command ID but \
                     we could not find a matching state machine! Event: {:?}",
                        ?event
                    );
                }

                // Restore machine if not in it's final state
                if let Some(sm) = maybe_machine {
                    if !sm.is_final_state() {
                        self.machines_by_id.insert(initial_cmd_id, sm);
                    }
                }
            }
            None => self.handle_non_stateful_event(event, has_next_event)?,
        }

        Ok(())
    }

    /// Called when we want to run the event loop because a workflow task started event has
    /// triggered
    pub(super) fn task_started(&mut self, task_started_event_id: i64, time: SystemTime) {
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
        self.event_loop();
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

        let consumed_cmd = loop {
            // handleVersionMarker can skip a marker event if the getVersion call was removed.
            // In this case we don't want to consume a command. -- we will need to replace it back
            // to the front when implementing, or something better
            let maybe_command = self.commands.pop_front();
            let mut command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::NoCommandScheduledForEvent(event.clone()));
            };

            // Feed the machine the event
            let mut break_later = false;
            if let CancellableCommand::Active {
                ref mut machine, ..
            } = &mut command
            {
                self.submachine_handle_event((*machine).borrow_mut(), event, true)?;

                // TODO: Handle invalid event errors
                //  * More special handling for version machine - see java
                //  * Command/machine supposed to have cancelled itself

                break_later = true;
            }

            if break_later {
                break command;
            }
        };

        // TODO: validate command

        if let CancellableCommand::Active { machine, .. } = consumed_cmd {
            if !machine.is_final_state() {
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
                    self.run_id = attrs.original_execution_run_id.clone();
                    // We need to notify the lang sdk that it's time to kick off a workflow
                    self.outgoing_wf_activation_jobs.push_back(
                        StartWorkflowTaskAttributes {
                            workflow_type: attrs
                                .workflow_type
                                .as_ref()
                                .map(|wt| wt.name.clone())
                                .unwrap_or_default(),
                            workflow_id: self.workflow_id.clone(),
                            arguments: attrs.input.clone(),
                        }
                        .into(),
                    );
                    let results = self.drive_me.start(attrs.clone());
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
                self.submachine_handle_event(
                    &mut wf_task_sm as &mut dyn TemporalStateMachine,
                    event,
                    has_next_event,
                )?;
                self.machines_by_id
                    .insert(event.event_id, Box::new(wf_task_sm));
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                // TODO: Signal callbacks
            }
            Some(EventType::WorkflowExecutionCancelRequested) => {
                // TODO: Cancel callbacks
            }
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    event.clone(),
                    "The event is non a non-stateful event, but we tried to handle it as one",
                ))
            }
        }
        Ok(())
    }

    /// Fetches commands ready for processing from the state machines
    pub(crate) fn get_commands(&mut self) -> Vec<ProtoCommand> {
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

    /// Returns the next activation that needs to be performed by the lang sdk. Things like unblock
    /// timer, etc.
    pub(crate) fn get_wf_activation(&mut self) -> Option<WfActivation> {
        if self.outgoing_wf_activation_jobs.is_empty() {
            None
        } else {
            let jobs = self
                .outgoing_wf_activation_jobs
                .drain(..)
                .map(Into::into)
                .collect();
            Some(WfActivation {
                timestamp: self.current_wf_time.map(Into::into),
                run_id: self.run_id.clone(),
                jobs,
            })
        }
    }

    /// Given an event id (possibly zero) of the last successfully executed workflow task and an
    /// id of the last event, sets the ids internally and appropriately sets the replaying flag.
    pub(crate) fn set_started_ids(
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

    /// Runs the event loop, which consists of grabbing any pending outgoing commands from the
    /// workflow, handling them, and preparing them to be sent off to the server.
    pub(crate) fn event_loop(&mut self) {
        let results = self.drive_me.fetch_workflow_iteration_output();
        self.handle_driven_results(results);

        self.prepare_commands();
    }

    /// Wrapper for calling [TemporalStateMachine::handle_event] which appropriately takes action
    /// on the returned triggers
    fn submachine_handle_event<TSM: DerefMut<Target = dyn TemporalStateMachine>>(
        &mut self,
        mut sm: TSM,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        let triggers = sm.handle_event(event, has_next_event).map_err(|e| {
            if let WFMachinesError::MalformedEventDetail(s) = e {
                WFMachinesError::MalformedEvent(event.clone(), s)
            } else {
                e
            }
        })?;
        event!(Level::DEBUG, msg = "Machine produced triggers", ?triggers);
        for trigger in triggers {
            match trigger {
                WorkflowTrigger::PushWFJob(a) => {
                    self.drive_me.on_activation_job(&a);
                    self.outgoing_wf_activation_jobs.push_back(a);
                }
                WorkflowTrigger::TriggerWFTaskStarted {
                    task_started_event_id,
                    time,
                } => {
                    self.task_started(task_started_event_id, time);
                }
            }
        }
        Ok(())
    }

    fn handle_driven_results(&mut self, results: Vec<WFCommand>) {
        for cmd in results {
            // I don't love how boilerplatey this is
            match cmd {
                WFCommand::AddTimer(attrs) => {
                    let timer = self.new_timer(attrs);
                    self.current_wf_task_commands.push_back(timer);
                }
                WFCommand::CompleteWorkflow(attrs) => {
                    self.current_wf_task_commands
                        .push_back(complete_workflow(attrs));
                }
                WFCommand::NoCommandsFromLang => (),
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
