mod local_acts;

pub(crate) use temporal_sdk_core_api::errors::WFMachinesError;

use super::{
    activity_state_machine::new_activity, cancel_external_state_machine::new_external_cancel,
    cancel_workflow_state_machine::cancel_workflow,
    child_workflow_state_machine::new_child_workflow,
    complete_workflow_state_machine::complete_workflow,
    continue_as_new_workflow_state_machine::continue_as_new,
    fail_workflow_state_machine::fail_workflow, local_activity_state_machine::new_local_activity,
    patch_state_machine::has_change, signal_external_state_machine::new_external_signal,
    timer_state_machine::new_timer, upsert_search_attributes_state_machine::upsert_search_attrs,
    workflow_machines::local_acts::LocalActivityData,
    workflow_task_state_machine::WorkflowTaskMachine, Machines, NewMachineWithCommand,
    TemporalStateMachine,
};
use crate::{
    protosext::{HistoryEventExt, ValidScheduleLA},
    telemetry::{metrics::MetricsContext, VecDisplayer},
    worker::{
        workflow::{
            machines::modify_workflow_properties_state_machine::modify_workflow_properties,
            CommandID, DrivenWorkflow, HistoryUpdate, LocalResolution, OutgoingJob, WFCommand,
            WorkflowFetcher, WorkflowStartedInfo,
        },
        ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    },
};
use siphasher::sip::SipHasher13;
use slotmap::{SlotMap, SparseSecondaryMap};
use std::{
    borrow::{Borrow, BorrowMut},
    collections::{HashMap, VecDeque},
    convert::TryInto,
    hash::{Hash, Hasher},
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_protos::{
    coresdk::{
        common::NamespacedWorkflowExecution,
        workflow_activation,
        workflow_activation::{
            workflow_activation_job, NotifyHasPatch, UpdateRandomSeed, WorkflowActivation,
        },
        workflow_commands::{
            request_cancel_external_workflow_execution as cancel_we, ContinueAsNewWorkflowExecution,
        },
    },
    temporal::api::{
        command::v1::{command::Attributes as ProtoCmdAttrs, Command as ProtoCommand},
        enums::v1::EventType,
        history::v1::{history_event, HistoryEvent},
    },
};

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

slotmap::new_key_type! { struct MachineKey; }
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
    /// The event id of the most recent event processed. It's possible in some situations (ex legacy
    /// queries) to receive a history with no new workflow tasks. If the last history we processed
    /// also had no new tasks, we need a way to know not to apply the same events over again.
    pub last_processed_event: i64,
    /// True if the workflow is replaying from history
    pub replaying: bool,
    /// Namespace this workflow exists in
    pub namespace: String,
    /// Workflow identifier
    pub workflow_id: String,
    /// Workflow type identifier. (Function name, class, etc)
    pub workflow_type: String,
    /// Identifies the current run
    pub run_id: String,
    /// The time the workflow execution began, as told by the WEStarted event
    workflow_start_time: Option<SystemTime>,
    /// The time the workflow execution finished, as determined by when the machines handled
    /// a terminal workflow command. If this is `Some`, you know the workflow is ended.
    workflow_end_time: Option<SystemTime>,
    /// The WFT start time if it has been established
    wft_start_time: Option<SystemTime>,
    /// The current workflow time if it has been established. This may differ from the WFT start
    /// time since local activities may advance the clock
    current_wf_time: Option<SystemTime>,

    all_machines: SlotMap<MachineKey, Machines>,
    /// If a machine key is in this map, that machine was created internally by core, not as a
    /// command from lang.
    machine_is_core_created: SparseSecondaryMap<MachineKey, ()>,

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
    current_wf_task_commands: VecDeque<CommandAndMachine>,

    /// Information about patch markers we have already seen while replaying history
    encountered_change_markers: HashMap<String, ChangeInfo>,

    /// Contains extra local-activity related data
    local_activity_data: LocalActivityData,

    /// The workflow that is being driven by this instance of the machines
    drive_me: DrivenWorkflow,

    /// Is set to true once we've seen the final event in workflow history, to avoid accidentally
    /// re-applying the final workflow task.
    pub have_seen_terminal_event: bool,

    /// Metrics context
    pub metrics: MetricsContext,
}

#[derive(Debug, derive_more::Display)]
#[display(fmt = "Cmd&Machine({})", "command")]
struct CommandAndMachine {
    command: MachineAssociatedCommand,
    machine: MachineKey,
}

#[derive(Debug, derive_more::Display)]
enum MachineAssociatedCommand {
    Real(Box<ProtoCommand>),
    #[display(fmt = "FakeLocalActivityMarker({})", "_0")]
    FakeLocalActivityMarker(u32),
}

#[derive(Debug, Clone, Copy)]
struct ChangeInfo {
    created_command: bool,
}

/// Returned by [TemporalStateMachine]s when handling events
#[derive(Debug, derive_more::Display)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub(super) enum MachineResponse {
    #[display(fmt = "PushWFJob({})", "_0")]
    PushWFJob(OutgoingJob),

    /// Pushes a new command into the list that will be sent to server once we respond with the
    /// workflow task completion
    IssueNewCommand(ProtoCommand),
    /// The machine requests the creation of another *different* machine. This acts as if lang
    /// had replied to the activation with a command, but we use a special set of IDs to avoid
    /// collisions.
    #[display(fmt = "NewCoreOriginatedCommand({:?})", "_0")]
    NewCoreOriginatedCommand(ProtoCmdAttrs),
    #[display(fmt = "IssueFakeLocalActivityMarker({})", "_0")]
    IssueFakeLocalActivityMarker(u32),
    #[display(fmt = "TriggerWFTaskStarted")]
    TriggerWFTaskStarted {
        task_started_event_id: i64,
        time: SystemTime,
    },
    #[display(fmt = "UpdateRunIdOnWorkflowReset({})", run_id)]
    UpdateRunIdOnWorkflowReset { run_id: String },

    /// Queue a local activity to be processed by the worker
    #[display(fmt = "QueueLocalActivity")]
    QueueLocalActivity(ValidScheduleLA),
    /// Request cancellation of an executing local activity
    #[display(fmt = "RequestCancelLocalActivity({})", "_0")]
    RequestCancelLocalActivity(u32),
    /// Indicates we are abandoning the indicated LA, so we can remove it from "outstanding" LAs
    /// and we will not try to WFT heartbeat because of it.
    #[display(fmt = "AbandonLocalActivity({:?})", "_0")]
    AbandonLocalActivity(u32),

    /// Set the workflow time to the provided time
    #[display(fmt = "UpdateWFTime({:?})", "_0")]
    UpdateWFTime(Option<SystemTime>),
}

impl<T> From<T> for MachineResponse
where
    T: Into<workflow_activation_job::Variant>,
{
    fn from(v: T) -> Self {
        Self::PushWFJob(v.into().into())
    }
}

impl WorkflowMachines {
    pub(crate) fn new(
        namespace: String,
        workflow_id: String,
        workflow_type: String,
        run_id: String,
        history: HistoryUpdate,
        driven_wf: DrivenWorkflow,
        metrics: MetricsContext,
    ) -> Self {
        let replaying = history.previous_started_event_id > 0;
        Self {
            last_history_from_server: history,
            namespace,
            workflow_id,
            workflow_type,
            run_id,
            drive_me: driven_wf,
            replaying,
            metrics,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            current_started_event_id: 0,
            next_started_event_id: 0,
            last_processed_event: 0,
            workflow_start_time: None,
            workflow_end_time: None,
            wft_start_time: None,
            current_wf_time: None,
            all_machines: Default::default(),
            machine_is_core_created: Default::default(),
            machines_by_event_id: Default::default(),
            id_to_machine: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
            encountered_change_markers: Default::default(),
            local_activity_data: LocalActivityData::default(),
            have_seen_terminal_event: false,
        }
    }

    /// Returns true if workflow has seen a terminal command
    pub(crate) const fn workflow_is_finished(&self) -> bool {
        self.workflow_end_time.is_some()
    }

    /// Returns the total time it took to execute the workflow. Returns `None` if workflow is
    /// incomplete, or time went backwards.
    pub(crate) fn total_runtime(&self) -> Option<Duration> {
        self.workflow_start_time
            .zip(self.workflow_end_time)
            .and_then(|(st, et)| et.duration_since(st).ok())
    }

    pub(crate) async fn new_history_from_server(&mut self, update: HistoryUpdate) -> Result<()> {
        self.last_history_from_server = update;
        self.replaying = self.last_history_from_server.previous_started_event_id > 0;
        self.apply_next_wft_from_history().await?;
        Ok(())
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    pub(crate) fn local_resolution(&mut self, resolution: LocalResolution) -> Result<bool> {
        let mut result_important = true;
        match resolution {
            LocalResolution::LocalActivity(LocalActivityResolution {
                seq,
                result,
                runtime,
                attempt,
                backoff,
                original_schedule_time,
            }) => {
                let act_id = CommandID::LocalActivity(seq);
                let mk = self.get_machine_key(act_id)?;
                let mach = self.machine_mut(mk);
                if let Machines::LocalActivityMachine(ref mut lam) = *mach {
                    let resps =
                        lam.try_resolve(result, runtime, attempt, backoff, original_schedule_time)?;
                    if resps.is_empty() {
                        result_important = false;
                    }
                    self.process_machine_responses(mk, resps)?;
                } else {
                    return Err(WFMachinesError::Nondeterminism(format!(
                        "Command matching activity with seq num {} existed but was not a \
                        local activity!",
                        seq
                    )));
                }
                self.local_activity_data.done_executing(seq);
            }
        }
        Ok(result_important)
    }

    /// Drain all queued local activities that need executing or cancellation
    pub(crate) fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.local_activity_data
            .take_all_reqs(&self.workflow_type, &self.workflow_id, &self.run_id)
    }

    /// Returns the number of local activities we know we need to execute but have not yet finished
    pub(crate) fn outstanding_local_activity_count(&self) -> usize {
        self.local_activity_data.outstanding_la_count()
    }

    /// Returns start info for the workflow if it has started
    pub(crate) fn get_started_info(&self) -> Option<&WorkflowStartedInfo> {
        self.drive_me.get_started_info()
    }

    /// Fetches commands which are ready for processing from the state machines, generally to be
    /// sent off to the server. They are not removed from the internal queue, that happens when
    /// corresponding history events from the server are being handled.
    pub(crate) fn get_commands(&self) -> Vec<ProtoCommand> {
        self.commands
            .iter()
            .filter_map(|c| {
                if !self.machine(c.machine).is_final_state() {
                    match &c.command {
                        MachineAssociatedCommand::Real(cmd) => Some((**cmd).clone()),
                        MachineAssociatedCommand::FakeLocalActivityMarker(_) => None,
                    }
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
    pub(crate) fn get_wf_activation(&mut self) -> WorkflowActivation {
        let jobs = self.drive_me.drain_jobs();
        WorkflowActivation {
            timestamp: self.current_wf_time.map(Into::into),
            is_replaying: self.replaying,
            run_id: self.run_id.clone(),
            history_length: self.last_processed_event as u32,
            jobs,
        }
    }

    pub(crate) fn has_pending_jobs(&self) -> bool {
        !self.drive_me.peek_pending_jobs().is_empty()
    }

    pub(crate) fn has_pending_la_resolutions(&self) -> bool {
        self.drive_me
            .peek_pending_jobs()
            .iter()
            .any(|v| v.is_la_resolution)
    }

    /// Iterate the state machines, which consists of grabbing any pending outgoing commands from
    /// the workflow code, handling them, and preparing them to be sent off to the server.
    pub(crate) async fn iterate_machines(&mut self) -> Result<()> {
        let results = self.drive_me.fetch_workflow_iteration_output().await;
        self.handle_driven_results(results)?;
        self.prepare_commands()?;
        if self.workflow_is_finished() {
            if let Some(rt) = self.total_runtime() {
                self.metrics.wf_e2e_latency(rt);
            }
        }
        Ok(())
    }

    /// Apply the next (unapplied) entire workflow task from history to these machines. Will replay
    /// any events that need to be replayed until caught up to the newest WFT. May also fetch
    /// history from server if needed.
    pub(crate) async fn apply_next_wft_from_history(&mut self) -> Result<usize> {
        // If we have already seen the terminal event for the entire workflow in a previous WFT,
        // then we don't need to do anything here, and in fact we need to avoid re-applying the
        // final WFT.
        if self.have_seen_terminal_event {
            return Ok(0);
        }

        let last_handled_wft_started_id = self.current_started_event_id;
        let events = {
            let mut evts = self
                .last_history_from_server
                .take_next_wft_sequence(last_handled_wft_started_id)
                .await
                .map_err(WFMachinesError::HistoryFetchingError)?;
            // Do not re-process events we have already processed
            evts.retain(|e| e.event_id > self.last_processed_event);
            evts
        };
        let num_events_to_process = events.len();

        // We're caught up on reply if there are no new events to process
        if events.is_empty() {
            self.replaying = false;
        }
        let replay_start = Instant::now();

        if let Some(last_event) = events.last() {
            if last_event.event_type == EventType::WorkflowTaskStarted as i32 {
                self.next_started_event_id = last_event.event_id;
            }
        }

        let mut history = events.into_iter().peekable();
        while let Some(event) = history.next() {
            if event.event_id != self.last_processed_event + 1 {
                return Err(WFMachinesError::Fatal(format!(
                    "History is out of order. Last processed event: {}, event id: {}",
                    self.last_processed_event, event.event_id
                )));
            }
            let next_event = history.peek();
            let eid = event.event_id;
            let etype = event.event_type();
            self.handle_event(event, next_event.is_some())?;
            self.last_processed_event = eid;
            if etype == EventType::WorkflowTaskStarted && next_event.is_none() {
                break;
            }
        }

        // Scan through to the next WFT, searching for any patch / la markers, so that we can
        // pre-resolve them.
        for e in self.last_history_from_server.peek_next_wft_sequence() {
            if let Some((patch_id, _)) = e.get_patch_marker_details() {
                self.encountered_change_markers.insert(
                    patch_id.clone(),
                    ChangeInfo {
                        created_command: false,
                    },
                );
                // Found a patch marker
                self.drive_me.send_job(
                    workflow_activation_job::Variant::NotifyHasPatch(NotifyHasPatch { patch_id })
                        .into(),
                );
            } else if e.is_local_activity_marker() {
                self.local_activity_data.process_peekahead_marker(e)?;
            }
        }

        if !self.replaying {
            self.metrics.wf_task_replay_latency(replay_start.elapsed());
        }

        Ok(num_events_to_process)
    }

    /// Handle a single event from the workflow history. `has_next_event` should be false if `event`
    /// is the last event in the history.
    ///
    /// This function will attempt to apply the event to the workflow state machines. If there is
    /// not a matching machine for the event, a nondeterminism error is returned. Otherwise, the
    /// event is applied to the machine, which may also return a nondeterminism error if the machine
    /// does not match the expected type. A fatal error may be returned if the machine is in an
    /// invalid state.
    #[instrument(skip(self, event), fields(event=%event))]
    fn handle_event(&mut self, event: HistoryEvent, has_next_event: bool) -> Result<()> {
        if event.is_final_wf_execution_event() {
            self.have_seen_terminal_event = true;
        }
        if matches!(
            event.event_type(),
            EventType::WorkflowExecutionTerminated | EventType::WorkflowExecutionTimedOut
        ) {
            return if has_next_event {
                Err(WFMachinesError::Fatal(
                    "Machines were fed a history which has an event after workflow execution was \
                     terminated!"
                        .to_string(),
                ))
            } else {
                Ok(())
            };
        }
        if self.replaying
            && self.current_started_event_id
                >= self.last_history_from_server.previous_started_event_id
            && event.event_type() != EventType::WorkflowTaskCompleted
        {
            // Replay is finished
            self.replaying = false;
        }
        if event.event_type() == EventType::Unspecified || event.attributes.is_none() {
            return if !event.worker_may_ignore {
                Err(WFMachinesError::Fatal(format!(
                    "Event type is unspecified! This history is invalid. Event detail: {:?}",
                    event
                )))
            } else {
                debug!("Event is ignorable");
                Ok(())
            };
        }

        if event.is_command_event() {
            self.handle_command_event(event)?;
            return Ok(());
        }

        if let Some(initial_cmd_id) = event.get_initial_command_event_id() {
            // We remove the machine while we it handles events, then return it, to avoid
            // borrowing from ourself mutably.
            let maybe_machine = self.machines_by_event_id.remove(&initial_cmd_id);
            match maybe_machine {
                Some(sm) => {
                    self.submachine_handle_event(sm, event, has_next_event)?;
                    // Restore machine if not in it's final state
                    if !self.machine(sm).is_final_state() {
                        self.machines_by_event_id.insert(initial_cmd_id, sm);
                    }
                }
                None => {
                    return Err(WFMachinesError::Nondeterminism(format!(
                        "During event handling, this event had an initial command ID but we \
                            could not find a matching command for it: {:?}",
                        event
                    )));
                }
            }
        } else {
            self.handle_non_stateful_event(event, has_next_event)?;
        }

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
    fn handle_command_event(&mut self, event: HistoryEvent) -> Result<()> {
        if event.is_local_activity_marker() {
            let deets = event.extract_local_activity_marker_data().ok_or_else(|| {
                WFMachinesError::Fatal(format!("Local activity marker was unparsable: {:?}", event))
            })?;
            let cmdid = CommandID::LocalActivity(deets.seq);
            let mkey = self.get_machine_key(cmdid)?;
            if let Machines::LocalActivityMachine(lam) = self.machine(mkey) {
                if lam.marker_should_get_special_handling()? {
                    self.submachine_handle_event(mkey, event, false)?;
                    return Ok(());
                }
            } else {
                return Err(WFMachinesError::Fatal(format!(
                    "Encountered local activity marker but the associated machine was of the \
                     wrong type! {:?}",
                    event
                )));
            }
        }

        let event_id = event.event_id;

        let consumed_cmd = loop {
            if let Some(peek_machine) = self.commands.front() {
                let mach = self.machine(peek_machine.machine);
                match change_marker_handling(&event, mach)? {
                    ChangeMarkerOutcome::SkipEvent => return Ok(()),
                    ChangeMarkerOutcome::SkipCommand => {
                        self.commands.pop_front();
                        continue;
                    }
                    ChangeMarkerOutcome::Normal => {}
                }
            }

            let maybe_command = self.commands.pop_front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "No command scheduled for event {}",
                    event
                )));
            };

            let canceled_before_sent = self
                .machine(command.machine)
                .was_cancelled_before_sent_to_server();

            if !canceled_before_sent {
                // Feed the machine the event
                self.submachine_handle_event(command.machine, event, true)?;
                break command;
            }
        };

        if !self.machine(consumed_cmd.machine).is_final_state() {
            self.machines_by_event_id
                .insert(event_id, consumed_cmd.machine);
        }

        Ok(())
    }

    fn handle_non_stateful_event(
        &mut self,
        event: HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        trace!(
            event = %event,
            "handling non-stateful event"
        );
        let event_id = event.event_id;
        match EventType::from_i32(event.event_type) {
            Some(EventType::WorkflowExecutionStarted) => {
                if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                    attrs,
                )) = event.attributes
                {
                    if let Some(st) = event.event_time.clone() {
                        let as_systime: SystemTime = st.try_into()?;
                        self.workflow_start_time = Some(as_systime);
                        // Set the workflow time to be the event time of the first event, so that
                        // if there is a query issued before first WFT started event, there is some
                        // workflow time set.
                        self.set_current_time(as_systime);
                    }
                    // Notify the lang sdk that it's time to kick off a workflow
                    self.drive_me.start(
                        self.workflow_id.clone(),
                        str_to_randomness_seed(&attrs.original_execution_run_id),
                        event.event_time.unwrap_or_default(),
                        attrs,
                    );
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "WorkflowExecutionStarted event did not have appropriate attributes: {}",
                        event
                    )));
                }
            }
            Some(EventType::WorkflowTaskScheduled) => {
                let wf_task_sm = WorkflowTaskMachine::new(self.next_started_event_id);
                let key = self.all_machines.insert(wf_task_sm.into());
                self.submachine_handle_event(key, event, has_next_event)?;
                self.machines_by_event_id.insert(event_id, key);
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                if let Some(history_event::Attributes::WorkflowExecutionSignaledEventAttributes(
                    attrs,
                )) = event.attributes
                {
                    self.drive_me
                        .send_job(workflow_activation::SignalWorkflow::from(attrs).into());
                } else {
                    // err
                }
            }
            Some(EventType::WorkflowExecutionCancelRequested) => {
                if let Some(
                    history_event::Attributes::WorkflowExecutionCancelRequestedEventAttributes(
                        attrs,
                    ),
                ) = event.attributes
                {
                    self.drive_me
                        .send_job(workflow_activation::CancelWorkflow::from(attrs).into());
                } else {
                    // err
                }
            }
            _ => {
                return Err(WFMachinesError::Fatal(format!(
                    "The event is not a non-stateful event, but we tried to handle it as one: {}",
                    event
                )));
            }
        }
        Ok(())
    }

    fn set_current_time(&mut self, time: SystemTime) -> SystemTime {
        if self.current_wf_time.map_or(true, |t| t < time) {
            self.current_wf_time = Some(time);
        }
        self.current_wf_time
            .expect("We have just ensured this is populated")
    }

    /// Wrapper for calling [TemporalStateMachine::handle_event] which appropriately takes action
    /// on the returned machine responses
    fn submachine_handle_event(
        &mut self,
        sm: MachineKey,
        event: HistoryEvent,
        has_next_event: bool,
    ) -> Result<()> {
        let machine_responses = self.machine_mut(sm).handle_event(event, has_next_event)?;
        self.process_machine_responses(sm, machine_responses)?;
        Ok(())
    }

    /// Transfer commands from `current_wf_task_commands` to `commands`, so they may be sent off
    /// to the server. While doing so, [TemporalStateMachine::handle_command] is called on the
    /// machine associated with the command.
    fn prepare_commands(&mut self) -> Result<()> {
        // It's possible we might prepare commands more than once before completing a WFT. (Because
        // of local activities, of course). Some commands might have since been cancelled that we
        // already prepared. Rip them out of the outgoing command list if so.
        self.commands.retain(|c| {
            !self
                .all_machines
                .get(c.machine)
                .expect("Machine must exist")
                .was_cancelled_before_sent_to_server()
        });

        while let Some(c) = self.current_wf_task_commands.pop_front() {
            if !self
                .machine(c.machine)
                .was_cancelled_before_sent_to_server()
            {
                match &c.command {
                    MachineAssociatedCommand::Real(cmd) => {
                        let machine_responses = self
                            .machine_mut(c.machine)
                            .handle_command(cmd.command_type())?;
                        self.process_machine_responses(c.machine, machine_responses)?;
                    }
                    MachineAssociatedCommand::FakeLocalActivityMarker(_) => {}
                }
                self.commands.push_back(c);
            }
        }
        debug!(commands = %self.commands.display(), "prepared commands");
        Ok(())
    }

    /// After a machine handles either an event or a command, it produces [MachineResponses] which
    /// this function uses to drive sending jobs to lang, triggering new workflow tasks, etc.
    fn process_machine_responses(
        &mut self,
        smk: MachineKey,
        machine_responses: Vec<MachineResponse>,
    ) -> Result<()> {
        let sm = self.machine(smk);
        if !machine_responses.is_empty() {
            trace!(responses = %machine_responses.display(), machine_name = %sm.name(),
                   "Machine produced responses");
        }
        self.process_machine_resps_impl(smk, machine_responses)
    }

    fn process_machine_resps_impl(
        &mut self,
        smk: MachineKey,
        machine_responses: Vec<MachineResponse>,
    ) -> Result<()> {
        for response in machine_responses {
            match response {
                MachineResponse::PushWFJob(a) => {
                    // We don't need to notify lang about jobs created by core-internal machines
                    if !self.machine_is_core_created.contains_key(smk) {
                        self.drive_me.send_job(a);
                    }
                }
                MachineResponse::TriggerWFTaskStarted {
                    task_started_event_id,
                    time,
                } => {
                    self.task_started(task_started_event_id, time)?;
                }
                MachineResponse::UpdateRunIdOnWorkflowReset { run_id: new_run_id } => {
                    self.drive_me.send_job(
                        workflow_activation_job::Variant::UpdateRandomSeed(UpdateRandomSeed {
                            randomness_seed: str_to_randomness_seed(&new_run_id),
                        })
                        .into(),
                    );
                }
                MachineResponse::IssueNewCommand(c) => {
                    self.current_wf_task_commands.push_back(CommandAndMachine {
                        command: MachineAssociatedCommand::Real(Box::new(c)),
                        machine: smk,
                    })
                }
                MachineResponse::NewCoreOriginatedCommand(attrs) => match attrs {
                    ProtoCmdAttrs::RequestCancelExternalWorkflowExecutionCommandAttributes(
                        attrs,
                    ) => {
                        let we = NamespacedWorkflowExecution {
                            namespace: attrs.namespace,
                            workflow_id: attrs.workflow_id,
                            run_id: attrs.run_id,
                        };
                        self.add_cmd_to_wf_task(
                            new_external_cancel(0, we, attrs.child_workflow_only, attrs.reason),
                            CommandIdKind::CoreInternal,
                        );
                    }
                    c => {
                        return Err(WFMachinesError::Fatal(format!(
                        "A machine requested to create a new command of an unsupported type: {:?}",
                        c
                    )))
                    }
                },
                MachineResponse::IssueFakeLocalActivityMarker(seq) => {
                    self.current_wf_task_commands.push_back(CommandAndMachine {
                        command: MachineAssociatedCommand::FakeLocalActivityMarker(seq),
                        machine: smk,
                    });
                }
                MachineResponse::QueueLocalActivity(act) => {
                    self.local_activity_data.enqueue(act);
                }
                MachineResponse::RequestCancelLocalActivity(seq) => {
                    // We might already know about the status from a pre-resolution. Apply it if so.
                    // We need to do this because otherwise we might need to perform additional
                    // activations during replay that didn't happen during execution, just like
                    // we sometimes pre-resolve activities when first requested.
                    if let Some(preres) = self.local_activity_data.take_preresolution(seq) {
                        if let Machines::LocalActivityMachine(lam) = self.machine_mut(smk) {
                            let more_responses = lam.try_resolve_with_dat(preres)?;
                            self.process_machine_responses(smk, more_responses)?;
                        } else {
                            panic!("A non local-activity machine returned a request cancel LA response");
                        }
                    }
                    // If it's in the request queue, just rip it out.
                    else if let Some(removed_act) =
                        self.local_activity_data.remove_from_queue(seq)
                    {
                        // We removed it. Notify the machine that the activity cancelled.
                        if let Machines::LocalActivityMachine(lam) = self.machine_mut(smk) {
                            let more_responses = lam.try_resolve(
                                LocalActivityExecutionResult::empty_cancel(),
                                Duration::from_secs(0),
                                removed_act.attempt,
                                None,
                                removed_act.original_schedule_time,
                            )?;
                            self.process_machine_responses(smk, more_responses)?;
                        } else {
                            panic!("A non local-activity machine returned a request cancel LA response");
                        }
                    } else {
                        // Finally, if we know about the LA at all, it's currently running, so
                        // queue the cancel request to be given to the LA manager.
                        self.local_activity_data.enqueue_cancel(ExecutingLAId {
                            run_id: self.run_id.clone(),
                            seq_num: seq,
                        });
                    }
                }
                MachineResponse::AbandonLocalActivity(seq) => {
                    self.local_activity_data.done_executing(seq);
                }
                MachineResponse::UpdateWFTime(t) => {
                    if let Some(t) = t {
                        self.set_current_time(t);
                    }
                }
            }
        }
        Ok(())
    }

    /// Called when a workflow task started event has triggered. Ensures we are tracking the ID
    /// of the current started event as well as workflow time properly.
    fn task_started(&mut self, task_started_event_id: i64, time: SystemTime) -> Result<()> {
        self.current_started_event_id = task_started_event_id;
        self.wft_start_time = Some(time);
        self.set_current_time(time);

        // Notify local activity machines that we started a non-replay WFT, which will allow any
        // which were waiting for a marker to instead decide to execute the LA since it clearly
        // will not be resolved via marker.
        if !self.replaying {
            let mut resps = vec![];
            for (k, mach) in self.all_machines.iter_mut() {
                if let Machines::LocalActivityMachine(lam) = mach {
                    resps.push((k, lam.encountered_non_replay_wft()?));
                }
            }
            for (mkey, resp_set) in resps {
                self.process_machine_responses(mkey, resp_set)?;
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
    fn handle_driven_results(&mut self, results: Vec<WFCommand>) -> Result<()> {
        for cmd in results {
            match cmd {
                WFCommand::AddTimer(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(new_timer(attrs), CommandID::Timer(seq).into());
                }
                WFCommand::UpsertSearchAttributes(attrs) => {
                    self.add_cmd_to_wf_task(
                        upsert_search_attrs(attrs),
                        CommandIdKind::NeverResolves,
                    );
                }
                WFCommand::CancelTimer(attrs) => {
                    self.process_cancellation(CommandID::Timer(attrs.seq))?;
                }
                WFCommand::AddActivity(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(new_activity(attrs), CommandID::Activity(seq).into());
                }
                WFCommand::AddLocalActivity(attrs) => {
                    let seq = attrs.seq;
                    let attrs: ValidScheduleLA = ValidScheduleLA::from_schedule_la(
                        attrs,
                        self.get_started_info()
                            .as_ref()
                            .and_then(|x| x.workflow_execution_timeout),
                    )
                    .map_err(|e| {
                        WFMachinesError::Fatal(format!(
                            "Invalid schedule local activity request (seq {}): {}",
                            seq, e
                        ))
                    })?;
                    let (la, mach_resp) = new_local_activity(
                        attrs,
                        self.replaying,
                        self.local_activity_data.take_preresolution(seq),
                        self.current_wf_time,
                    )?;
                    let machkey = self.all_machines.insert(la.into());
                    self.id_to_machine
                        .insert(CommandID::LocalActivity(seq), machkey);
                    self.process_machine_responses(machkey, mach_resp)?;
                }
                WFCommand::RequestCancelActivity(attrs) => {
                    self.process_cancellation(CommandID::Activity(attrs.seq))?;
                }
                WFCommand::RequestCancelLocalActivity(attrs) => {
                    self.process_cancellation(CommandID::LocalActivity(attrs.seq))?;
                }
                WFCommand::CompleteWorkflow(attrs) => {
                    self.metrics.wf_completed();
                    self.add_terminal_command(complete_workflow(attrs));
                }
                WFCommand::FailWorkflow(attrs) => {
                    self.metrics.wf_failed();
                    self.add_terminal_command(fail_workflow(attrs));
                }
                WFCommand::ContinueAsNew(attrs) => {
                    self.metrics.wf_continued_as_new();
                    let attrs = self.augment_continue_as_new_with_current_values(attrs);
                    self.add_terminal_command(continue_as_new(attrs));
                }
                WFCommand::CancelWorkflow(attrs) => {
                    self.metrics.wf_canceled();
                    self.add_terminal_command(cancel_workflow(attrs));
                }
                WFCommand::SetPatchMarker(attrs) => {
                    // Do not create commands for change IDs that we have already created commands
                    // for.
                    if !matches!(self.encountered_change_markers.get(&attrs.patch_id),
                                 Some(ChangeInfo {created_command}) if *created_command)
                    {
                        self.add_cmd_to_wf_task(
                            has_change(attrs.patch_id.clone(), self.replaying, attrs.deprecated),
                            CommandIdKind::NeverResolves,
                        );

                        if let Some(ci) = self.encountered_change_markers.get_mut(&attrs.patch_id) {
                            ci.created_command = true;
                        } else {
                            self.encountered_change_markers.insert(
                                attrs.patch_id,
                                ChangeInfo {
                                    created_command: true,
                                },
                            );
                        }
                    }
                }
                WFCommand::AddChildWorkflow(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(
                        new_child_workflow(attrs),
                        CommandID::ChildWorkflowStart(seq).into(),
                    );
                }
                WFCommand::CancelChild(attrs) => self.process_cancellation(
                    CommandID::ChildWorkflowStart(attrs.child_workflow_seq),
                )?,
                WFCommand::RequestCancelExternalWorkflow(attrs) => {
                    let (we, only_child) = match attrs.target {
                        None => {
                            return Err(WFMachinesError::Fatal(
                                "Cancel external workflow command had empty target field"
                                    .to_string(),
                            ))
                        }
                        Some(cancel_we::Target::ChildWorkflowId(wfid)) => (
                            NamespacedWorkflowExecution {
                                namespace: self.namespace.clone(),
                                workflow_id: wfid,
                                run_id: "".to_string(),
                            },
                            true,
                        ),
                        Some(cancel_we::Target::WorkflowExecution(we)) => (we, false),
                    };
                    self.add_cmd_to_wf_task(
                        new_external_cancel(
                            attrs.seq,
                            we,
                            only_child,
                            format!("Cancel requested by workflow with run id {}", self.run_id),
                        ),
                        CommandID::CancelExternal(attrs.seq).into(),
                    );
                }
                WFCommand::SignalExternalWorkflow(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(
                        new_external_signal(attrs, &self.namespace)?,
                        CommandID::SignalExternal(seq).into(),
                    );
                }
                WFCommand::CancelSignalWorkflow(attrs) => {
                    self.process_cancellation(CommandID::SignalExternal(attrs.seq))?;
                }
                WFCommand::QueryResponse(_) => {
                    // Nothing to do here, queries are handled above the machine level
                    unimplemented!("Query responses should not make it down into the machines")
                }
                WFCommand::ModifyWorkflowProperties(attrs) => {
                    self.add_cmd_to_wf_task(
                        modify_workflow_properties(attrs),
                        CommandIdKind::NeverResolves,
                    );
                }
                WFCommand::NoCommandsFromLang => (),
            }
        }
        Ok(())
    }

    /// Given a command id to attempt to cancel, try to cancel it and return any jobs that should
    /// be included in the activation
    fn process_cancellation(&mut self, id: CommandID) -> Result<()> {
        let m_key = self.get_machine_key(id)?;
        let mach = self.machine_mut(m_key);
        let machine_resps = mach.cancel()?;
        debug!(machine_responses = %machine_resps.display(), cmd_id = ?id,
               "Cancel request responses");
        self.process_machine_resps_impl(m_key, machine_resps)
    }

    fn get_machine_key(&self, id: CommandID) -> Result<MachineKey> {
        Ok(*self.id_to_machine.get(&id).ok_or_else(|| {
            WFMachinesError::Fatal(format!("Missing associated machine for {:?}", id))
        })?)
    }

    fn add_terminal_command(&mut self, machine: NewMachineWithCommand) {
        let cwfm = self.add_new_command_machine(machine);
        self.workflow_end_time = Some(SystemTime::now());
        self.current_wf_task_commands.push_back(cwfm);
    }

    /// Add a new command/machines for that command to the current workflow task
    fn add_cmd_to_wf_task(&mut self, machine: NewMachineWithCommand, id: CommandIdKind) {
        let mach = self.add_new_command_machine(machine);
        if let CommandIdKind::LangIssued(id) = id {
            self.id_to_machine.insert(id, mach.machine);
        }
        if matches!(id, CommandIdKind::CoreInternal) {
            self.machine_is_core_created.insert(mach.machine, ());
        }
        self.current_wf_task_commands.push_back(mach);
    }

    fn add_new_command_machine(&mut self, machine: NewMachineWithCommand) -> CommandAndMachine {
        let k = self.all_machines.insert(machine.machine);
        CommandAndMachine {
            command: MachineAssociatedCommand::Real(Box::new(machine.command)),
            machine: k,
        }
    }

    fn machine(&self, m: MachineKey) -> &Machines {
        self.all_machines
            .get(m)
            .expect("Machine must exist")
            .borrow()
    }

    fn machine_mut(&mut self, m: MachineKey) -> &mut Machines {
        self.all_machines
            .get_mut(m)
            .expect("Machine must exist")
            .borrow_mut()
    }

    fn augment_continue_as_new_with_current_values(
        &self,
        mut attrs: ContinueAsNewWorkflowExecution,
    ) -> ContinueAsNewWorkflowExecution {
        if let Some(started_info) = self.drive_me.get_started_info() {
            if attrs.memo.is_empty() {
                attrs.memo = started_info
                    .memo
                    .clone()
                    .map(Into::into)
                    .unwrap_or_default();
            }
            if attrs.search_attributes.is_empty() {
                attrs.search_attributes = started_info
                    .search_attrs
                    .clone()
                    .map(Into::into)
                    .unwrap_or_default();
            }
            if attrs.retry_policy.is_none() {
                attrs.retry_policy = started_info.retry_policy.clone();
            }
        }
        attrs
    }
}

fn str_to_randomness_seed(run_id: &str) -> u64 {
    // This was originally `DefaultHasher` but that is potentially unstable across Rust releases.
    // This must forever be `SipHasher13` now or we risk breaking history compat.
    let mut s = SipHasher13::new();
    run_id.hash(&mut s);
    s.finish()
}

enum ChangeMarkerOutcome {
    SkipEvent,
    SkipCommand,
    Normal,
}

/// Special handling for patch markers, when handling command events as in
/// [WorkflowMachines::handle_command_event]
fn change_marker_handling(event: &HistoryEvent, mach: &Machines) -> Result<ChangeMarkerOutcome> {
    if !mach.matches_event(event) {
        // Version markers can be skipped in the event they are deprecated
        if let Some((patch_name, deprecated)) = event.get_patch_marker_details() {
            // Is deprecated. We can simply ignore this event, as deprecated change
            // markers are allowed without matching changed calls.
            if deprecated {
                debug!("Deprecated patch marker tried against wrong machine, skipping.");
                return Ok(ChangeMarkerOutcome::SkipEvent);
            }
            return Err(WFMachinesError::Nondeterminism(format!(
                "Non-deprecated patch marker encountered for change {}, \
                            but there is no corresponding change command!",
                patch_name
            )));
        }
        // Patch machines themselves may also not *have* matching markers, where non-deprecated
        // calls take the old path, and deprecated calls assume history is produced by a new-code
        // worker.
        if matches!(mach, Machines::PatchMachine(_)) {
            debug!("Skipping non-matching event against patch machine");
            return Ok(ChangeMarkerOutcome::SkipCommand);
        }
    }
    Ok(ChangeMarkerOutcome::Normal)
}

#[derive(derive_more::From)]
enum CommandIdKind {
    /// A normal command, requested by lang
    LangIssued(CommandID),
    /// A command created internally
    CoreInternal,
    /// A command which is fire-and-forget (ex: Upsert search attribs)
    NeverResolves,
}
