use crate::{
    core_tracing::VecDisplayer,
    machines::{
        activity_state_machine::new_activity, cancel_workflow_state_machine::cancel_workflow,
        complete_workflow_state_machine::complete_workflow,
        continue_as_new_workflow_state_machine::continue_as_new,
        fail_workflow_state_machine::fail_workflow, timer_state_machine::new_timer,
        version_state_machine::has_change, workflow_task_state_machine::WorkflowTaskMachine,
        NewMachineWithCommand, ProtoCommand, TemporalStateMachine, WFCommand,
    },
    protos::{
        coresdk::{
            common::Payload,
            workflow_activation::{
                wf_activation_job::{self, Variant},
                ResolveHasChange, StartWorkflow, UpdateRandomSeed, WfActivation,
            },
            PayloadsExt, PayloadsToPayloadError,
        },
        temporal::api::{
            common::v1::Header,
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent},
        },
    },
    workflow::{CommandID, DrivenWorkflow, HistoryUpdate, WorkflowFetcher},
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
    /// The last recorded history we received from the server for this workflow run. This must be
    /// kept because the lang side polls & completes for every workflow task, but we do not need
    /// to poll the server that often during replay.
    last_history_from_server: HistoryUpdate,
    /// EventId of the last handled WorkflowTaskStarted event
    current_started_event_id: i64,
    /// The event id of the next workflow task started event that the machines need to process.
    /// Eventually, this number should reach the started id in the latest history update, but
    /// we must incrementally apply the history while communicating with lang.
    next_started_event_id: i64,
    /// True if the workflow is replaying from history
    pub replaying: bool,
    /// Workflow identifier
    pub workflow_id: String,
    /// Identifies the current run
    pub run_id: String,
    /// The current workflow time if it has been established
    current_wf_time: Option<SystemTime>,

    all_machines: SlotMap<MachineKey, Box<dyn TemporalStateMachine + 'static>>,

    /// A mapping for accessing machines associated to a particular event, where the key is the id
    /// of the initiating event for that machine.
    machines_by_event_id: HashMap<i64, MachineKey>,

    /// Maps command ids as created by workflow authors to their associated machines.
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
}

#[derive(thiserror::Error, Debug)]
// TODO: Some of these are redundant with MachineError -- we should try to dedupe / simplify
//  This also probably doesn't need to be public. The only important thing is if the error is a
//  nondeterminism error.
pub enum WFMachinesError {
    #[error("Event {0:?} was not expected: {1}")]
    UnexpectedEvent(HistoryEvent, &'static str),
    #[error("Event {0:?} was not expected: {1}")]
    InvalidTransitionDuringEvent(HistoryEvent, String),
    #[error("Event {0:?} was malformed: {1}")]
    MalformedEvent(HistoryEvent, String),
    // Expected to be transformed into a `MalformedEvent` with the full event by workflow machines,
    // when emitted by a sub-machine
    // TODO: This is really an unexpected, rather than malformed event.
    #[error("{0}")]
    MalformedEventDetail(String),
    #[error("Command type {0:?} was not expected")]
    UnexpectedCommand(CommandType),
    #[error("Command type {0} is not known")]
    UnknownCommandType(i32),
    #[error("No command was scheduled for event {0:?}")]
    NoCommandScheduledForEvent(HistoryEvent),
    #[error("Machine response {0:?} was not expected: {1}")]
    UnexpectedMachineResponse(MachineResponse, String),
    #[error("Command was missing its associated machine: {0}")]
    MissingAssociatedMachine(String),
    #[error("There was {0} when we expected exactly one payload while applying event: {1:?}")]
    NotExactlyOnePayload(PayloadsToPayloadError, HistoryEvent),
    #[error("Machine encountered an invalid transition: {0}")]
    InvalidTransition(String),
    #[error("Unrecoverable network error while fetching history: {0}")]
    HistoryFetchingError(tonic::Status),
    #[error("Unable to process partial event history because workflow is no longer cached.")]
    CacheMiss,
    #[error("Lang sent us an invalid command {0:?}: {1}")]
    BadWfCommand(WFCommand, String),
}

impl WorkflowMachines {
    pub(crate) fn new(
        workflow_id: String,
        run_id: String,
        history: HistoryUpdate,
        driven_wf: DrivenWorkflow,
    ) -> Self {
        let replaying = history.previous_started_event_id > 0;
        Self {
            last_history_from_server: history,
            workflow_id,
            run_id,
            drive_me: driven_wf,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            current_started_event_id: 0,
            next_started_event_id: 0,
            // Default to true since application of events will appropriately set false if needed
            // replaying: true,
            replaying,
            current_wf_time: None,
            all_machines: Default::default(),
            machines_by_event_id: Default::default(),
            id_to_machine: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
        }
    }

    pub(crate) async fn new_history_from_server(&mut self, update: HistoryUpdate) -> Result<()> {
        self.last_history_from_server = update;
        self.replaying = self.last_history_from_server.previous_started_event_id > 0;
        self.apply_next_wft_from_history().await?;
        Ok(())
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// TODO: Describe what actually happens in here
    #[instrument(level = "debug", skip(self, event), fields(event=%event))]
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
            && self.current_started_event_id
                >= self.last_history_from_server.previous_started_event_id
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
    fn task_started(&mut self, task_started_event_id: i64, time: SystemTime) -> Result<()> {
        let s = span!(Level::DEBUG, "Task started trigger");
        let _enter = s.enter();

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

        let consumed_cmd = loop {
            let maybe_command = self.commands.pop_front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::NoCommandScheduledForEvent(event.clone()));
            };

            // Feed the machine the event
            let canceled_before_sent = self
                .machine(command.machine)
                .was_cancelled_before_sent_to_server();

            if !canceled_before_sent {
                self.submachine_handle_event(command.machine, event, true)?;
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
        debug!(
            event = %event,
            "handling non-stateful event"
        );
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
                            arguments: Vec::from_payloads(attrs.input.clone()),
                            randomness_seed: str_to_randomness_seed(
                                &attrs.original_execution_run_id,
                            ),
                            headers: match &attrs.header {
                                None => HashMap::new(),
                                Some(Header { fields }) => fields
                                    .iter()
                                    .map(|(k, v)| (k.clone(), Payload::from(v.clone())))
                                    .collect(),
                            },
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
                let wf_task_sm = WorkflowTaskMachine::new(self.next_started_event_id);
                let key = self.all_machines.insert(Box::new(wf_task_sm));
                self.submachine_handle_event(key, event, has_next_event)?;
                self.machines_by_event_id.insert(event.event_id, key);
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                if let Some(history_event::Attributes::WorkflowExecutionSignaledEventAttributes(
                    attrs,
                )) = &event.attributes
                {
                    self.drive_me.signal(attrs.clone().into());
                } else {
                    // err
                }
            }
            Some(EventType::WorkflowExecutionCancelRequested) => {
                if let Some(
                    history_event::Attributes::WorkflowExecutionCancelRequestedEventAttributes(
                        attrs,
                    ),
                ) = &event.attributes
                {
                    self.drive_me.cancel(attrs.clone().into());
                } else {
                    // err
                }
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
    pub(crate) fn get_commands(&self) -> Vec<ProtoCommand> {
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
    ///
    /// The job list may be empty, in which case it is expected the caller handles what to do in a
    /// "no work" situation. Possibly, it may know about some work the machines don't, like queries.
    pub(crate) fn get_wf_activation(&mut self) -> WfActivation {
        let jobs = self.drive_me.drain_jobs();
        WfActivation {
            timestamp: self.current_wf_time.map(Into::into),
            is_replaying: self.replaying,
            run_id: self.run_id.clone(),
            jobs,
        }
    }

    fn set_current_time(&mut self, time: SystemTime) -> SystemTime {
        if self.current_wf_time.map(|t| t < time).unwrap_or(true) {
            self.current_wf_time = Some(time);
        }
        self.current_wf_time
            .expect("We have just ensured this is populated")
    }

    /// Iterate the state machines, which consists of grabbing any pending outgoing commands from
    /// the workflow code, handling them, and preparing them to be sent off to the server.
    ///
    /// Returns a boolean flag which indicates whether or not new activations were produced by the
    /// state machine. If true, pending activation should be created by the caller making jobs
    /// available to the lang side.
    pub(crate) async fn iterate_machines(&mut self) -> Result<bool> {
        let results = self.drive_me.fetch_workflow_iteration_output().await;
        let jobs = self.handle_driven_results(results)?;
        let has_new_lang_jobs = !jobs.is_empty();
        for job in jobs.into_iter() {
            self.drive_me.send_job(job);
        }
        self.prepare_commands()?;
        Ok(has_new_lang_jobs)
    }

    /// Apply the next entire workflow task from history to these machines.
    pub(crate) async fn apply_next_wft_from_history(&mut self) -> Result<()> {
        let last_handled_wft_started_id = self.current_started_event_id;
        let events = self
            .last_history_from_server
            .take_next_wft_sequence(last_handled_wft_started_id)
            .await
            .map_err(WFMachinesError::HistoryFetchingError)?;

        // We're caught up on reply if there are no new events to process
        // TODO: Probably this is unneeded if we evict whenever history is from non-sticky queue
        if events.is_empty() {
            self.replaying = false;
        }

        if let Some(last_event) = events.last() {
            if last_event.event_type == EventType::WorkflowTaskStarted as i32 {
                self.next_started_event_id = last_event.event_id;
            }
        }

        let first_event_id = match events.first() {
            Some(event) => event.event_id,
            None => 0,
        };
        // Workflow has been evicted, but we've received partial history from the server.
        // Need to reset sticky and trigger another poll.
        if self.current_started_event_id == 0 && first_event_id != 1 && !events.is_empty() {
            debug!("Cache miss.");
            return Err(WFMachinesError::CacheMiss);
        }

        let mut history = events.iter().peekable();

        while let Some(event) = history.next() {
            let next_event = history.peek();

            if event.event_type == EventType::WorkflowTaskStarted as i32 && next_event.is_none() {
                self.handle_event(event, false)?;
                break;
            }

            self.handle_event(event, next_event.is_some())?;
        }

        // Scan through to the next WFT, searching for any change markers, so that we can
        // pre-resolve them.
        for e in self.last_history_from_server.peek_next_wft_sequence() {
            if let Some((change_id, _deprecated)) = e.get_changed_marker_details() {
                // Found a change marker
                self.drive_me
                    .send_job(wf_activation_job::Variant::ResolveHasChange(
                        ResolveHasChange {
                            change_id,
                            is_present: true,
                        },
                    ));
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
        let machine_responses = self
            .machine_mut(sm)
            .handle_event(event, has_next_event)
            .map_err(|e| {
                if let WFMachinesError::MalformedEventDetail(s) = e {
                    WFMachinesError::MalformedEvent(event.clone(), s)
                } else {
                    e
                }
            })?;
        self.process_machine_responses(sm, machine_responses)?;
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
                let machine_responses = self.machine_mut(c.machine).handle_command(cmd_type)?;
                self.process_machine_responses(c.machine, machine_responses)?;
            }
            self.commands.push_back(c);
        }
        debug!(commands = %self.commands.display(), "prepared commands");
        Ok(())
    }

    /// After a machine handles either an event or a command, it produces [MachineResponses] which
    /// this function uses to drive sending jobs to lang, trigging new workflow tasks, etc.
    fn process_machine_responses(
        &mut self,
        sm: MachineKey,
        machine_responses: Vec<MachineResponse>,
    ) -> Result<()> {
        let sm = self.machine_mut(sm);
        if !machine_responses.is_empty() {
            debug!(responses = %machine_responses.display(), machine_name = %sm.name(),
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
                MachineResponse::IssueNewCommand(_) => {
                    panic!("Issue new command machine response not expected here")
                }
            }
        }
        Ok(())
    }

    /// Handles results of the workflow activation, delegating work to the appropriate state
    /// machine. Returns a list of workflow jobs that should be queued in the pending activation for
    /// the next poll. This list will be populated only if state machine produced lang activations
    /// as part of command processing. For example some types of activity cancellation need to
    /// immediately unblock lang side without having it to poll for an actual workflow task from the
    /// server.
    fn handle_driven_results(
        &mut self,
        results: Vec<WFCommand>,
    ) -> Result<Vec<wf_activation_job::Variant>> {
        let mut jobs = vec![];
        for cmd in results {
            match cmd {
                WFCommand::AddTimer(attrs) => {
                    let tid = attrs.timer_id.clone();
                    let timer = self.add_new_command_machine(new_timer(attrs));
                    self.id_to_machine
                        .insert(CommandID::Timer(tid), timer.machine);
                    self.current_wf_task_commands.push_back(timer);
                }
                WFCommand::CancelTimer(attrs) => self.process_cancellation(
                    &CommandID::Timer(attrs.timer_id.to_owned()),
                    &mut jobs,
                )?,
                WFCommand::AddActivity(attrs) => {
                    let aid = attrs.activity_id.clone();
                    let activity = self.add_new_command_machine(new_activity(attrs));
                    self.id_to_machine
                        .insert(CommandID::Activity(aid), activity.machine);
                    self.current_wf_task_commands.push_back(activity);
                }
                WFCommand::RequestCancelActivity(attrs) => self.process_cancellation(
                    &CommandID::Activity(attrs.activity_id.to_owned()),
                    &mut jobs,
                )?,
                WFCommand::CompleteWorkflow(attrs) => {
                    let cwfm = self.add_new_command_machine(complete_workflow(attrs));
                    self.current_wf_task_commands.push_back(cwfm);
                }
                WFCommand::FailWorkflow(attrs) => {
                    let fwfm = self.add_new_command_machine(fail_workflow(attrs));
                    self.current_wf_task_commands.push_back(fwfm);
                }
                WFCommand::ContinueAsNew(attrs) => {
                    let canm = self.add_new_command_machine(continue_as_new(attrs));
                    self.current_wf_task_commands.push_back(canm);
                }
                WFCommand::CancelWorkflow(attrs) => {
                    let cancm = self.add_new_command_machine(cancel_workflow(attrs));
                    self.current_wf_task_commands.push_back(cancm);
                }
                WFCommand::HasChange(attrs) => {
                    // TODO: probably still need some global version mapping to be able to resolve
                    //  calls to has_version w/ same change ID (IE: later calls should be resolved
                    //  with information from earlier marker)
                    let verm = self.add_new_command_machine(has_change(
                        attrs.change_id.clone(),
                        self.replaying,
                        attrs.deprecated,
                    ));
                    self.current_wf_task_commands.push_back(verm);
                }
                WFCommand::QueryResponse(_) => {
                    // Nothing to do here, queries are handled above the machine level
                    unimplemented!("Query responses should not make it down into the machines")
                }
                WFCommand::NoCommandsFromLang => (),
            }
        }
        Ok(jobs)
    }

    fn process_cancellation(&mut self, id: &CommandID, jobs: &mut Vec<Variant>) -> Result<()> {
        let m_key = self.get_machine_key(id)?;
        let res = self.machine_mut(m_key).cancel()?;
        debug!(machine_responses = ?res, cmd_id = ?id, "Cancel request responses");
        for r in res {
            match r {
                MachineResponse::IssueNewCommand(c) => {
                    self.current_wf_task_commands.push_back(CommandAndMachine {
                        command: c,
                        machine: m_key,
                    })
                }
                MachineResponse::PushWFJob(j) => {
                    jobs.push(j);
                }
                v => {
                    return Err(WFMachinesError::UnexpectedMachineResponse(
                        v,
                        format!("When cancelling {:?}", id),
                    ));
                }
            }
        }
        Ok(())
    }

    fn get_machine_key(&self, id: &CommandID) -> Result<MachineKey> {
        Ok(*self.id_to_machine.get(id).ok_or_else(|| {
            WFMachinesError::MissingAssociatedMachine(format!(
                "Missing associated machine for {:?}",
                id
            ))
        })?)
    }

    fn add_new_command_machine<T: TemporalStateMachine + 'static>(
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
