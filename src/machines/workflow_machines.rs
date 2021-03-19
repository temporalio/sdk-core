use crate::machines::activity_state_machine::new_activity;
use crate::workflow::{DrivenWorkflow, WorkflowFetcher};
use crate::{
    core_tracing::VecDisplayer,
    machines::{
        complete_workflow_state_machine::complete_workflow,
        fail_workflow_state_machine::fail_workflow, timer_state_machine::new_timer,
        workflow_task_state_machine::WorkflowTaskMachine, NewMachineWithCommand, ProtoCommand,
        TemporalStateMachine, WFCommand,
    },
    protos::{
        coresdk::{wf_activation_job, StartWorkflow, UpdateRandomSeed, WfActivation},
        temporal::api::{
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent},
        },
    },
    protosext::HistoryInfo,
};
use slotmap::SlotMap;
use std::{
    borrow::{Borrow, BorrowMut},
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    hash::{Hash, Hasher},
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
    /// Identifies the current run
    pub run_id: String,
    /// The current workflow time if it has been established
    current_wf_time: Option<SystemTime>,

    all_machines: SlotMap<MachineKey, Box<dyn TemporalStateMachine + 'static>>,

    /// A mapping for accessing all the machines, where the key is the id of the initiating event
    /// for that machine.
    machines_by_event_id: HashMap<i64, MachineKey>,

    /// Maps timer ids as created by workflow authors to their associated machines
    /// TODO: Make this apply to *all* cancellable things, once we've added more. Key can be enum.
    id_to_machine: HashMap<CommandID, MachineKey>,

    /// Queued commands which have been produced by machines and await processing / being sent to
    /// the server.
    commands: VecDeque<CommandAndMachine>,
    /// Commands generated by the currently processing workflow task, which will eventually be
    /// transferred to `commands` (and hence eventually sent to the server)
    ///
    /// Old note: It is a queue as commands can be added (due to marker based commands) while
    /// iterating over already added commands.
    current_wf_task_commands: VecDeque<CommandAndMachine>,

    /// The workflow that is being driven by this instance of the machines
    drive_me: DrivenWorkflow,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum CommandID {
    Timer(String),
    Activity(String),
}

slotmap::new_key_type! { struct MachineKey; }
#[derive(Debug, derive_more::Display)]
#[display(fmt = "Cmd&Machine({})", "command")]
struct CommandAndMachine {
    command: ProtoCommand,
    machine: MachineKey,
}

/// Returned by [TemporalStateMachine]s when handling events
#[derive(Debug, derive_more::From, derive_more::Display)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum MachineResponse {
    #[display(fmt = "PushWFJob")]
    PushWFJob(#[from(forward)] wf_activation_job::Variant),

    IssueNewCommand(ProtoCommand),
    #[display(fmt = "TriggerWFTaskStarted")]
    TriggerWFTaskStarted {
        task_started_event_id: i64,
        time: SystemTime,
    },
    #[display(fmt = "UpdateRunIdOnWorkflowReset({})", run_id)]
    UpdateRunIdOnWorkflowReset {
        run_id: String,
    },
    NoOp,
}

#[derive(thiserror::Error, Debug)]
// TODO: Some of these are redundant with MachineError -- we should try to dedupe / simplify
pub enum WFMachinesError {
    #[error("Event {0:?} was not expected: {1}")]
    UnexpectedEvent(HistoryEvent, &'static str),
    #[error("Event {0:?} was not expected: {1}")]
    InvalidTransitionDuringEvent(HistoryEvent, String),
    #[error("Event {0:?} was malformed: {1}")]
    MalformedEvent(HistoryEvent, String),
    // Expected to be transformed into a `MalformedEvent` with the full event by workflow machines,
    // when emitted by a sub-machine
    #[error("{0}")]
    MalformedEventDetail(String),
    #[error("Command type {0:?} was not expected")]
    UnexpectedCommand(CommandType),
    #[error("Command type {0} is not known")]
    UnknownCommandType(i32),
    #[error("No command was scheduled for event {0:?}")]
    NoCommandScheduledForEvent(HistoryEvent),
    #[error("Machine response {0:?} was not expected: {1}")]
    UnexpectedMachineResponse(MachineResponse, &'static str),
    #[error("Command was missing its associated machine: {0}")]
    MissingAssociatedMachine(String),

    #[error("Machine encountered an invalid transition: {0}")]
    InvalidTransition(&'static str),
}

impl WorkflowMachines {
    pub(crate) fn new(workflow_id: String, run_id: String, driven_wf: DrivenWorkflow) -> Self {
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
            all_machines: Default::default(),
            machines_by_event_id: Default::default(),
            id_to_machine: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
        }
    }

    /// Returns the id of the last seen WorkflowTaskStarted event
    pub(crate) fn get_last_started_event_id(&self) -> i64 {
        self.current_started_event_id
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    #[instrument(level = "debug", skip(self), fields(run_id = % self.run_id))]
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
                let maybe_machine = self.machines_by_event_id.remove(&initial_cmd_id);
                if let Some(sm) = maybe_machine {
                    self.submachine_handle_event(sm, event, has_next_event)?;
                } else {
                    error!(
                        event=?event,
                        "During event handling, this event had an initial command ID but we could \
                        not find a matching state machine!"
                    );
                }

                // Restore machine if not in it's final state
                if let Some(sm) = maybe_machine {
                    if !self.machine(sm).is_final_state() {
                        self.machines_by_event_id.insert(initial_cmd_id, sm);
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
        // TODO: Ideally this would actually be called every time a command is pushed from a
        //  workflow that isn't going across the public api, but as it stands that isn't really
        //  doable, so this has to exist here for test workflow driver
        self.iterate_machines()?;
        Ok(())
    }

    /// A command event is an event which is generated from a command emitted as a result of
    /// performing a workflow task. Each command has a corresponding event. For example
    /// ScheduleActivityTaskCommand is recorded to the history as ActivityTaskScheduledEvent.
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
        debug!(current_commands = ?self.commands, "handling command event");

        let consumed_cmd = loop {
            // handleVersionMarker can skip a marker event if the getVersion call was removed.
            // In this case we don't want to consume a command. -- we will need to replace it back
            // to the front when implementing, or something better
            let maybe_command = self.commands.pop_front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::NoCommandScheduledForEvent(event.clone()));
            };

            // Feed the machine the event
            let mut break_later = false;
            let canceled_before_sent = self
                .machine(command.machine)
                .was_cancelled_before_sent_to_server();

            if !canceled_before_sent {
                self.submachine_handle_event(command.machine, event, true)?;
            }

            // TODO:
            //  * More special handling for version machine - see java
            //  * Commands cancelled this iteration are allowed to not match the event?

            if !canceled_before_sent {
                break_later = true;
            }

            if break_later {
                break command;
            }
        };

        // TODO: validate command

        if !self.machine(consumed_cmd.machine).is_final_state() {
            self.machines_by_event_id
                .insert(event.event_id, consumed_cmd.machine);
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
                    self.drive_me.send_job(
                        StartWorkflow {
                            workflow_type: attrs
                                .workflow_type
                                .as_ref()
                                .map(|wt| wt.name.clone())
                                .unwrap_or_default(),
                            workflow_id: self.workflow_id.clone(),
                            arguments: attrs.input.clone(),
                            randomness_seed: str_to_randomness_seed(
                                &attrs.original_execution_run_id,
                            ),
                        }
                        .into(),
                    );
                    self.drive_me.start(attrs.clone());
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        event.clone(),
                        "WorkflowExecutionStarted event did not have appropriate attributes"
                            .to_string(),
                    ));
                }
            }
            Some(EventType::WorkflowTaskScheduled) => {
                let wf_task_sm = WorkflowTaskMachine::new(self.workflow_task_started_event_id);
                let key = self.all_machines.insert(Box::new(wf_task_sm));
                self.submachine_handle_event(key, event, has_next_event)?;
                self.machines_by_event_id.insert(event.event_id, key);
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                if let Some(history_event::Attributes::WorkflowExecutionSignaledEventAttributes(
                    attrs,
                )) = &event.attributes
                {
                    self.drive_me.signal(attrs.clone());
                } else {
                    // err
                }
            }
            Some(EventType::WorkflowExecutionCancelRequested) => {
                // TODO: Cancel callbacks
            }
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    event.clone(),
                    "The event is non a non-stateful event, but we tried to handle it as one",
                ));
            }
        }
        Ok(())
    }

    /// Fetches commands which are ready for processing from the state machines, generally to be
    /// sent off to the server. They are not removed from the internal queue, that happens when
    /// corresponding history events from the server are being handled.
    pub(crate) fn get_commands(&mut self) -> Vec<ProtoCommand> {
        self.commands
            .iter()
            .filter_map(|c| {
                if !self.machine(c.machine).is_final_state() {
                    Some(c.command.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the next activation that needs to be performed by the lang sdk. Things like unblock
    /// timer, etc. This does *not* cause any advancement of the state machines, it merely drains
    /// from the outgoing queue of activation jobs.
    pub(crate) fn get_wf_activation(&mut self) -> Option<WfActivation> {
        let jobs = self.drive_me.drain_jobs();
        if jobs.is_empty() {
            None
        } else {
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

    /// Iterate the state machines, which consists of grabbing any pending outgoing commands from
    /// the workflow, handling them, and preparing them to be sent off to the server.
    pub(crate) fn iterate_machines(&mut self) -> Result<()> {
        let results = self.drive_me.fetch_workflow_iteration_output();
        self.handle_driven_results(results)?;
        self.prepare_commands()?;
        Ok(())
    }

    /// Apply events from history to this machines instance
    pub(crate) fn apply_history_events(&mut self, history_info: &HistoryInfo) -> Result<()> {
        let (_, events) = history_info
            .events()
            .split_at(self.get_last_started_event_id() as usize);
        let mut history = events.iter().peekable();

        self.set_started_ids(
            history_info.previous_started_event_id,
            history_info.workflow_task_started_event_id,
        );

        // HistoryInfo's constructor enforces some rules about the structure of history that
        // could be enforced here, but needn't be because they have already been guaranteed by it.
        // See the errors that can be returned from [HistoryInfo::new_from_events] for detail.

        while let Some(event) = history.next() {
            let next_event = history.peek();

            if event.event_type == EventType::WorkflowTaskStarted as i32 && next_event.is_none() {
                self.handle_event(event, false)?;
                return Ok(());
            }

            self.handle_event(event, next_event.is_some())?;

            if next_event.is_none() {
                if event.is_final_wf_execution_event() {
                    return Ok(());
                }
                unreachable!()
            }
        }

        Ok(())
    }

    /// Wrapper for calling [TemporalStateMachine::handle_event] which appropriately takes action
    /// on the returned machine responses
    fn submachine_handle_event(
        &mut self,
        sm: MachineKey,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        let sm = self.all_machines.get_mut(sm).expect("Machine must exist");
        let machine_responses = sm.handle_event(event, has_next_event).map_err(|e| {
            if let WFMachinesError::MalformedEventDetail(s) = e {
                WFMachinesError::MalformedEvent(event.clone(), s)
            } else {
                e
            }
        })?;
        if !machine_responses.is_empty() {
            debug!(responses = %machine_responses.display(),
                   "Machine produced responses");
        }
        for response in machine_responses {
            match response {
                MachineResponse::PushWFJob(a) => {
                    self.drive_me.send_job(a);
                }
                MachineResponse::TriggerWFTaskStarted {
                    task_started_event_id,
                    time,
                } => {
                    self.task_started(task_started_event_id, time)?;
                }
                MachineResponse::UpdateRunIdOnWorkflowReset { run_id: new_run_id } => {
                    // TODO: Should this also update self.run_id? Should we track orig/current
                    //   separately?
                    self.drive_me
                        .send_job(wf_activation_job::Variant::UpdateRandomSeed(
                            UpdateRandomSeed {
                                randomness_seed: str_to_randomness_seed(&new_run_id),
                            },
                        ));
                }
                MachineResponse::NoOp => (),
                MachineResponse::IssueNewCommand(_) => {
                    panic!("Issue new command machine response not expected here")
                }
            }
        }
        Ok(())
    }

    fn handle_driven_results(&mut self, results: Vec<WFCommand>) -> Result<()> {
        for cmd in results {
            match cmd {
                WFCommand::AddTimer(attrs) => {
                    let tid = attrs.timer_id.clone();
                    let timer = self.add_new_machine(new_timer(attrs));
                    self.id_to_machine
                        .insert(CommandID::Timer(tid), timer.machine);
                    self.current_wf_task_commands.push_back(timer);
                }
                WFCommand::CancelTimer(attrs) => {
                    let mkey = *self
                        .id_to_machine
                        .get(&CommandID::Timer(attrs.timer_id.to_owned()))
                        .ok_or_else(|| {
                            WFMachinesError::MissingAssociatedMachine(format!(
                                "Missing associated machine for cancelling timer {}",
                                &attrs.timer_id
                            ))
                        })?;
                    let res = self.machine_mut(mkey).cancel()?;
                    match res {
                        MachineResponse::IssueNewCommand(c) => {
                            self.current_wf_task_commands.push_back(CommandAndMachine {
                                command: c,
                                machine: mkey,
                            })
                        }
                        MachineResponse::NoOp => {}
                        v => {
                            return Err(WFMachinesError::UnexpectedMachineResponse(
                                v,
                                "When cancelling timer",
                            ));
                        }
                    }
                }
                WFCommand::AddActivity(attrs) => {
                    let aid = attrs.activity_id.clone();
                    let activity = self.add_new_machine(new_activity(attrs));
                    self.id_to_machine
                        .insert(CommandID::Activity(aid), activity.machine);
                    self.current_wf_task_commands.push_back(activity);
                }
                WFCommand::RequestCancelActivity(_) => unimplemented!(),
                WFCommand::CompleteWorkflow(attrs) => {
                    let cwfm = self.add_new_machine(complete_workflow(attrs));
                    self.current_wf_task_commands.push_back(cwfm);
                }
                WFCommand::FailWorkflow(attrs) => {
                    let cwfm = self.add_new_machine(fail_workflow(attrs));
                    self.current_wf_task_commands.push_back(cwfm);
                }
                WFCommand::NoCommandsFromLang => (),
            }
        }
        Ok(())
    }

    /// Transfer commands from `current_wf_task_commands` to `commands`, so they may be sent off
    /// to the server. While doing so, [TemporalStateMachine::handle_command] is called on the
    /// machine associated with the command.
    #[instrument(level = "debug", skip(self))]
    fn prepare_commands(&mut self) -> Result<()> {
        while let Some(c) = self.current_wf_task_commands.pop_front() {
            let cmd_type = CommandType::from_i32(c.command.command_type)
                .ok_or(WFMachinesError::UnknownCommandType(c.command.command_type))?;
            if !self
                .machine(c.machine)
                .was_cancelled_before_sent_to_server()
            {
                self.machine_mut(c.machine).handle_command(cmd_type)?;
            }
            self.commands.push_back(c);
        }
        debug!(commands = %self.commands.display(), "prepared commands");
        Ok(())
    }

    fn add_new_machine<T: TemporalStateMachine + 'static>(
        &mut self,
        machine: NewMachineWithCommand<T>,
    ) -> CommandAndMachine {
        let k = self.all_machines.insert(Box::new(machine.machine));
        CommandAndMachine {
            command: machine.command,
            machine: k,
        }
    }

    fn machine(&self, m: MachineKey) -> &dyn TemporalStateMachine {
        self.all_machines
            .get(m)
            .expect("Machine must exist")
            .borrow()
    }

    fn machine_mut(&mut self, m: MachineKey) -> &mut (dyn TemporalStateMachine + 'static) {
        self.all_machines
            .get_mut(m)
            .expect("Machine must exist")
            .borrow_mut()
    }
}

fn str_to_randomness_seed(run_id: &str) -> u64 {
    let mut s = DefaultHasher::new();
    run_id.hash(&mut s);
    s.finish()
}
