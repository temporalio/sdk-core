mod local_acts;

use super::{
    cancel_external_state_machine::new_external_cancel,
    cancel_workflow_state_machine::cancel_workflow,
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
    internal_flags::InternalFlags,
    protosext::{HistoryEventExt, ValidScheduleLA},
    telemetry::{metrics::MetricsContext, VecDisplayer},
    worker::{
        workflow::{
            history_update::NextWFT,
            machines::{
                activity_state_machine::ActivityMachine,
                child_workflow_state_machine::ChildWorkflowMachine,
                modify_workflow_properties_state_machine::modify_workflow_properties,
                patch_state_machine::VERSION_SEARCH_ATTR_KEY,
                upsert_search_attributes_state_machine::upsert_search_attrs_internal,
                HistEventData,
            },
            CommandID, DrivenWorkflow, HistoryUpdate, InternalFlagsRef, LocalResolution,
            OutgoingJob, RunBasics, WFCommand, WFMachinesError, WorkflowFetcher,
            WorkflowStartedInfo,
        },
        ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    },
};
use siphasher::sip::SipHasher13;
use slotmap::{SlotMap, SparseSecondaryMap};
use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::{HashMap, VecDeque},
    convert::TryInto,
    hash::{Hash, Hasher},
    rc::Rc,
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_protos::{
    coresdk::{
        common::{NamespacedWorkflowExecution, VersioningIntent},
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
        history::v1::{history_event, history_event::Attributes, HistoryEvent},
        sdk::v1::WorkflowTaskCompletedMetadata,
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
    /// The task queue this workflow is operating within
    pub task_queue: String,
    /// Is set to true once we've seen the final event in workflow history, to avoid accidentally
    /// re-applying the final workflow task.
    pub have_seen_terminal_event: bool,
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
    /// The internal flags which have been seen so far during this run's execution and thus are
    /// usable during replay.
    observed_internal_flags: InternalFlagsRef,
    /// Set on each WFT started event, the most recent size of history in bytes
    history_size_bytes: u64,
    /// Set on each WFT started event
    continue_as_new_suggested: bool,

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

    /// Metrics context
    pub metrics: MetricsContext,
}

#[derive(Debug, derive_more::Display)]
#[display(fmt = "Cmd&Machine({command})")]
struct CommandAndMachine {
    command: MachineAssociatedCommand,
    machine: MachineKey,
}

#[derive(Debug, derive_more::Display)]
enum MachineAssociatedCommand {
    Real(Box<ProtoCommand>),
    #[display(fmt = "FakeLocalActivityMarker({_0})")]
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
    #[display(fmt = "PushWFJob({_0})")]
    PushWFJob(OutgoingJob),

    /// Pushes a new command into the list that will be sent to server once we respond with the
    /// workflow task completion
    IssueNewCommand(ProtoCommand),
    /// The machine requests the creation of another *different* machine. This acts as if lang
    /// had replied to the activation with a command, but we use a special set of IDs to avoid
    /// collisions.
    #[display(fmt = "NewCoreOriginatedCommand({_0:?})")]
    NewCoreOriginatedCommand(ProtoCmdAttrs),
    #[display(fmt = "IssueFakeLocalActivityMarker({_0})")]
    IssueFakeLocalActivityMarker(u32),
    #[display(fmt = "TriggerWFTaskStarted")]
    TriggerWFTaskStarted {
        task_started_event_id: i64,
        time: SystemTime,
    },
    #[display(fmt = "UpdateRunIdOnWorkflowReset({run_id})")]
    UpdateRunIdOnWorkflowReset { run_id: String },

    /// Queue a local activity to be processed by the worker
    #[display(fmt = "QueueLocalActivity")]
    QueueLocalActivity(ValidScheduleLA),
    /// Request cancellation of an executing local activity
    #[display(fmt = "RequestCancelLocalActivity({_0})")]
    RequestCancelLocalActivity(u32),
    /// Indicates we are abandoning the indicated LA, so we can remove it from "outstanding" LAs
    /// and we will not try to WFT heartbeat because of it.
    #[display(fmt = "AbandonLocalActivity({_0:?})")]
    AbandonLocalActivity(u32),

    /// Set the workflow time to the provided time
    #[display(fmt = "UpdateWFTime({_0:?})")]
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
    pub(crate) fn new(basics: RunBasics, driven_wf: DrivenWorkflow) -> Self {
        let replaying = basics.history.previous_wft_started_id > 0;
        let mut observed_internal_flags = InternalFlags::new(basics.capabilities);
        // Peek ahead to determine used patches in the first WFT.
        if let Some(attrs) = basics.history.peek_next_wft_completed(0) {
            observed_internal_flags.add_from_complete(attrs);
        };
        Self {
            last_history_from_server: basics.history,
            namespace: basics.namespace,
            workflow_id: basics.workflow_id,
            workflow_type: basics.workflow_type,
            run_id: basics.run_id,
            task_queue: basics.task_queue,
            drive_me: driven_wf,
            replaying,
            metrics: basics.metrics,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            current_started_event_id: 0,
            next_started_event_id: 0,
            last_processed_event: 0,
            workflow_start_time: None,
            workflow_end_time: None,
            wft_start_time: None,
            current_wf_time: None,
            observed_internal_flags: Rc::new(RefCell::new(observed_internal_flags)),
            history_size_bytes: 0,
            continue_as_new_suggested: false,
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

    pub(crate) fn new_history_from_server(&mut self, update: HistoryUpdate) -> Result<()> {
        self.last_history_from_server = update;
        self.replaying = self.last_history_from_server.previous_wft_started_id > 0;
        self.apply_next_wft_from_history()?;
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
                        "Command matching activity with seq num {seq} existed but was not a \
                        local activity!"
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
        // Since we're about to write a WFT, record any internal flags we know about which aren't
        // already recorded.
        (*self.observed_internal_flags)
            .borrow_mut()
            .write_all_known();
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
            available_internal_flags: (*self.observed_internal_flags)
                .borrow()
                .all_lang()
                .collect(),
            history_size_bytes: self.history_size_bytes,
            continue_as_new_suggested: self.continue_as_new_suggested,
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

    pub(crate) fn get_metadata_for_wft_complete(&self) -> WorkflowTaskCompletedMetadata {
        (*self.observed_internal_flags)
            .borrow_mut()
            .gather_for_wft_complete()
    }

    pub(crate) fn add_lang_used_flags(&self, flags: Vec<u32>) {
        (*self.observed_internal_flags)
            .borrow_mut()
            .add_lang_used(flags);
    }

    /// Iterate the state machines, which consists of grabbing any pending outgoing commands from
    /// the workflow code, handling them, and preparing them to be sent off to the server.
    pub(crate) fn iterate_machines(&mut self) -> Result<()> {
        let results = self.drive_me.fetch_workflow_iteration_output();
        self.handle_driven_results(results)?;
        self.prepare_commands()?;
        if self.workflow_is_finished() {
            if let Some(rt) = self.total_runtime() {
                self.metrics.wf_e2e_latency(rt);
            }
        }
        Ok(())
    }

    /// Returns true if machines are ready to apply the next WFT sequence, false if events will need
    /// to be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn ready_to_apply_next_wft(&self) -> bool {
        self.last_history_from_server
            .can_take_next_wft_sequence(self.current_started_event_id)
    }

    /// Apply the next (unapplied) entire workflow task from history to these machines. Will replay
    /// any events that need to be replayed until caught up to the newest WFT.
    pub(crate) fn apply_next_wft_from_history(&mut self) -> Result<usize> {
        // If we have already seen the terminal event for the entire workflow in a previous WFT,
        // then we don't need to do anything here, and in fact we need to avoid re-applying the
        // final WFT.
        if self.have_seen_terminal_event {
            // Replay clearly counts as done now, since we return here and never do anything else.
            self.replaying = false;
            return Ok(0);
        }

        fn update_internal_flags(me: &mut WorkflowMachines) {
            // Update observed patches with any that were used in the task
            if let Some(next_complete) = me
                .last_history_from_server
                .peek_next_wft_completed(me.last_processed_event)
            {
                (*me.observed_internal_flags)
                    .borrow_mut()
                    .add_from_complete(next_complete);
            }
        }

        // We update the internal flags before applying the current task (peeking to the completion
        // of this task), and also at the end (peeking to the completion of the task that lang is
        // about to generate commands for, and for which we will want those flags active).
        update_internal_flags(self);

        let last_handled_wft_started_id = self.current_started_event_id;
        let (events, has_final_event) = match self
            .last_history_from_server
            .take_next_wft_sequence(last_handled_wft_started_id)
        {
            NextWFT::ReplayOver => (vec![], true),
            NextWFT::WFT(mut evts, has_final_event) => {
                // Do not re-process events we have already processed
                evts.retain(|e| e.event_id > self.last_processed_event);
                (evts, has_final_event)
            }
            NextWFT::NeedFetch => {
                return Err(WFMachinesError::Fatal(
                    "Need to fetch history events to continue applying workflow task, but this \
                     should be prevented ahead of time! This is a Core SDK bug."
                        .to_string(),
                ));
            }
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

        let mut saw_completed = false;
        let mut do_handle_event = true;
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

            // This definition of replaying here is that we are no longer replaying as soon as we
            // see new events that have never been seen or produced by the SDK.
            //
            // Specifically, replay ends once we have seen the last command-event which was produced
            // as a result of the last completed WFT. Thus, replay would be false for things like
            // signals which were received and after the last completion, and thus generated the
            // current WFT being handled.
            if self.replaying && has_final_event && saw_completed && !event.is_command_event() {
                // Replay is finished
                self.replaying = false;
            }
            if event.event_type() == EventType::WorkflowTaskCompleted {
                saw_completed = true;
            }

            if do_handle_event {
                let eho = self.handle_event(
                    HistEventData {
                        event,
                        replaying: self.replaying,
                        current_task_is_last_in_history: has_final_event,
                    },
                    next_event,
                )?;
                if matches!(
                    eho,
                    EventHandlingOutcome::SkipEvent {
                        skip_next_event: true
                    }
                ) {
                    do_handle_event = false;
                }
            } else {
                do_handle_event = true;
            }
            self.last_processed_event = eid;
        }

        // Scan through to the next WFT, searching for any patch / la markers, so that we can
        // pre-resolve them.
        let mut wake_las = vec![];
        for e in self
            .last_history_from_server
            .peek_next_wft_sequence(last_handled_wft_started_id)
        {
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
                if let Some(la_dat) = e.clone().into_local_activity_marker_details() {
                    if let Ok(mk) =
                        self.get_machine_key(CommandID::LocalActivity(la_dat.marker_dat.seq))
                    {
                        wake_las.push((mk, la_dat));
                    } else {
                        self.local_activity_data.insert_peeked_marker(la_dat);
                    }
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Local activity marker was unparsable: {e:?}"
                    )));
                }
            }
        }
        for (mk, la_dat) in wake_las {
            let mach = self.machine_mut(mk);
            if let Machines::LocalActivityMachine(ref mut lam) = *mach {
                if lam.will_accept_resolve_marker() {
                    let resps = lam.try_resolve_with_dat(la_dat.into())?;
                    self.process_machine_responses(mk, resps)?;
                } else {
                    self.local_activity_data.insert_peeked_marker(la_dat);
                }
            }
        }

        update_internal_flags(self);

        if !self.replaying {
            self.metrics.wf_task_replay_latency(replay_start.elapsed());
        }

        Ok(num_events_to_process)
    }

    /// Handle a single event from the workflow history.
    ///
    /// This function will attempt to apply the event to the workflow state machines. If there is
    /// not a matching machine for the event, a nondeterminism error is returned. Otherwise, the
    /// event is applied to the machine, which may also return a nondeterminism error if the machine
    /// does not match the expected type. A fatal error may be returned if the machine is in an
    /// invalid state.
    #[instrument(skip(self, event_dat), fields(event=%event_dat))]
    fn handle_event(
        &mut self,
        event_dat: HistEventData,
        next_event: Option<&HistoryEvent>,
    ) -> Result<EventHandlingOutcome> {
        let event = &event_dat.event;
        if event.is_final_wf_execution_event() {
            self.have_seen_terminal_event = true;
        }
        if matches!(
            event.event_type(),
            EventType::WorkflowExecutionTerminated | EventType::WorkflowExecutionTimedOut
        ) {
            let are_more_events =
                next_event.is_some() || !event_dat.current_task_is_last_in_history;
            return if are_more_events {
                Err(WFMachinesError::Fatal(
                    "Machines were fed a history which has an event after workflow execution was \
                     terminated!"
                        .to_string(),
                ))
            } else {
                Ok(EventHandlingOutcome::Normal)
            };
        }
        if event.event_type() == EventType::Unspecified || event.attributes.is_none() {
            return if !event.worker_may_ignore {
                Err(WFMachinesError::Fatal(format!(
                    "Event type is unspecified! This history is invalid. Event detail: {event:?}"
                )))
            } else {
                debug!("Event is ignorable");
                Ok(EventHandlingOutcome::SkipEvent {
                    skip_next_event: false,
                })
            };
        }

        if event.is_command_event() {
            return self.handle_command_event(event_dat, next_event);
        }

        if let Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(ref attrs)) =
            event.attributes
        {
            self.history_size_bytes = u64::try_from(attrs.history_size_bytes).unwrap_or_default();
            self.continue_as_new_suggested = attrs.suggest_continue_as_new;
        }

        if let Some(initial_cmd_id) = event.get_initial_command_event_id() {
            // We remove the machine while we it handles events, then return it, to avoid
            // borrowing from ourself mutably.
            let maybe_machine = self.machines_by_event_id.remove(&initial_cmd_id);
            match maybe_machine {
                Some(sm) => {
                    self.submachine_handle_event(sm, event_dat)?;
                    // Restore machine if not in it's final state
                    if !self.machine(sm).is_final_state() {
                        self.machines_by_event_id.insert(initial_cmd_id, sm);
                    }
                }
                None => {
                    return Err(WFMachinesError::Nondeterminism(format!(
                        "During event handling, this event had an initial command ID but we \
                            could not find a matching command for it: {event:?}"
                    )));
                }
            }
        } else {
            self.handle_non_stateful_event(event_dat)?;
        }

        Ok(EventHandlingOutcome::Normal)
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
    fn handle_command_event(
        &mut self,
        event_dat: HistEventData,
        next_event: Option<&HistoryEvent>,
    ) -> Result<EventHandlingOutcome> {
        let event = &event_dat.event;

        if event.is_local_activity_marker() {
            let deets = event.extract_local_activity_marker_data().ok_or_else(|| {
                WFMachinesError::Fatal(format!("Local activity marker was unparsable: {event:?}"))
            })?;
            let cmdid = CommandID::LocalActivity(deets.seq);
            let mkey = self.get_machine_key(cmdid)?;
            if let Machines::LocalActivityMachine(lam) = self.machine(mkey) {
                if lam.marker_should_get_special_handling()? {
                    self.submachine_handle_event(mkey, event_dat)?;
                    return Ok(EventHandlingOutcome::Normal);
                }
            } else {
                return Err(WFMachinesError::Fatal(format!(
                    "Encountered local activity marker but the associated machine was of the \
                     wrong type! {event:?}"
                )));
            }
        }

        let event_id = event.event_id;

        let consumed_cmd = loop {
            if let Some(peek_machine) = self.commands.front() {
                let mach = self.machine(peek_machine.machine);
                match change_marker_handling(event, mach, next_event)? {
                    EventHandlingOutcome::SkipCommand => {
                        self.commands.pop_front();
                        continue;
                    }
                    eho @ EventHandlingOutcome::SkipEvent { .. } => return Ok(eho),
                    EventHandlingOutcome::Normal => {}
                }
            }

            let maybe_command = self.commands.pop_front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "No command scheduled for event {event}"
                )));
            };

            let canceled_before_sent = self
                .machine(command.machine)
                .was_cancelled_before_sent_to_server();

            if !canceled_before_sent {
                // Feed the machine the event
                self.submachine_handle_event(command.machine, event_dat)?;
                break command;
            }
        };

        if !self.machine(consumed_cmd.machine).is_final_state() {
            self.machines_by_event_id
                .insert(event_id, consumed_cmd.machine);
        }

        Ok(EventHandlingOutcome::Normal)
    }

    fn handle_non_stateful_event(&mut self, event_dat: HistEventData) -> Result<()> {
        trace!(
            event = %event_dat.event,
            "handling non-stateful event"
        );
        let event_id = event_dat.event.event_id;
        match EventType::from_i32(event_dat.event.event_type) {
            Some(EventType::WorkflowExecutionStarted) => {
                if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                    attrs,
                )) = event_dat.event.attributes
                {
                    if let Some(st) = event_dat.event.event_time.clone() {
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
                        event_dat.event.event_time.unwrap_or_default(),
                        attrs,
                    );
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "WorkflowExecutionStarted event did not have appropriate attributes: {event_dat}"
                    )));
                }
            }
            Some(EventType::WorkflowTaskScheduled) => {
                let wf_task_sm = WorkflowTaskMachine::new(self.next_started_event_id);
                let key = self.all_machines.insert(wf_task_sm.into());
                self.submachine_handle_event(key, event_dat)?;
                self.machines_by_event_id.insert(event_id, key);
            }
            Some(EventType::WorkflowExecutionSignaled) => {
                if let Some(history_event::Attributes::WorkflowExecutionSignaledEventAttributes(
                    attrs,
                )) = event_dat.event.attributes
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
                ) = event_dat.event.attributes
                {
                    self.drive_me
                        .send_job(workflow_activation::CancelWorkflow::from(attrs).into());
                } else {
                    // err
                }
            }
            _ => {
                return Err(WFMachinesError::Fatal(format!(
                    "The event is not a non-stateful event, but we tried to handle it as one: {event_dat}"
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
    fn submachine_handle_event(&mut self, sm: MachineKey, event: HistEventData) -> Result<()> {
        let machine_responses = self.machine_mut(sm).handle_event(event)?;
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
                    ProtoCmdAttrs::UpsertWorkflowSearchAttributesCommandAttributes(attrs) => {
                        self.add_cmd_to_wf_task(
                            upsert_search_attrs_internal(attrs),
                            CommandIdKind::NeverResolves,
                        );
                    }
                    c => {
                        return Err(WFMachinesError::Fatal(format!(
                        "A machine requested to create a new command of an unsupported type: {c:?}"
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
                        upsert_search_attrs(
                            attrs,
                            self.observed_internal_flags.clone(),
                            self.replaying,
                        ),
                        CommandIdKind::NeverResolves,
                    );
                }
                WFCommand::CancelTimer(attrs) => {
                    self.process_cancellation(CommandID::Timer(attrs.seq))?;
                }
                WFCommand::AddActivity(attrs) => {
                    let seq = attrs.seq;
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_cmd_to_wf_task(
                        ActivityMachine::new_scheduled(
                            attrs,
                            self.observed_internal_flags.clone(),
                            use_compat,
                        ),
                        CommandID::Activity(seq).into(),
                    );
                }
                WFCommand::AddLocalActivity(attrs) => {
                    let seq = attrs.seq;
                    let attrs: ValidScheduleLA =
                        ValidScheduleLA::from_schedule_la(attrs).map_err(|e| {
                            WFMachinesError::Fatal(format!(
                                "Invalid schedule local activity request (seq {seq}): {e}"
                            ))
                        })?;
                    let (la, mach_resp) = new_local_activity(
                        attrs,
                        self.replaying,
                        self.local_activity_data.take_preresolution(seq),
                        self.current_wf_time,
                        self.observed_internal_flags.clone(),
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
                    if !self.replaying {
                        self.metrics.wf_completed();
                    }
                    self.add_terminal_command(complete_workflow(attrs));
                }
                WFCommand::FailWorkflow(attrs) => {
                    if !self.replaying {
                        self.metrics.wf_failed();
                    }
                    self.add_terminal_command(fail_workflow(attrs));
                }
                WFCommand::ContinueAsNew(attrs) => {
                    if !self.replaying {
                        self.metrics.wf_continued_as_new();
                    }
                    let attrs = self.augment_continue_as_new_with_current_values(attrs);
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_terminal_command(continue_as_new(attrs, use_compat));
                }
                WFCommand::CancelWorkflow(attrs) => {
                    if !self.replaying {
                        self.metrics.wf_canceled();
                    }
                    self.add_terminal_command(cancel_workflow(attrs));
                }
                WFCommand::SetPatchMarker(attrs) => {
                    // Do not create commands for change IDs that we have already created commands
                    // for.
                    let encountered_entry = self.encountered_change_markers.get(&attrs.patch_id);
                    if !matches!(encountered_entry,
                                 Some(ChangeInfo {created_command}) if *created_command)
                    {
                        let (patch_machine, other_cmds) = has_change(
                            attrs.patch_id.clone(),
                            self.replaying,
                            attrs.deprecated,
                            encountered_entry.is_some(),
                            self.encountered_change_markers.keys().map(|s| s.as_str()),
                            self.observed_internal_flags.clone(),
                        )?;
                        let mkey =
                            self.add_cmd_to_wf_task(patch_machine, CommandIdKind::NeverResolves);
                        self.process_machine_responses(mkey, other_cmds)?;

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
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_cmd_to_wf_task(
                        ChildWorkflowMachine::new_scheduled(
                            attrs,
                            self.observed_internal_flags.clone(),
                            use_compat,
                        ),
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
            WFMachinesError::Fatal(format!("Missing associated machine for {id:?}"))
        })?)
    }

    fn add_terminal_command(&mut self, machine: NewMachineWithCommand) {
        let cwfm = self.add_new_command_machine(machine);
        self.workflow_end_time = Some(SystemTime::now());
        self.current_wf_task_commands.push_back(cwfm);
    }

    /// Add a new command/machines for that command to the current workflow task
    fn add_cmd_to_wf_task(
        &mut self,
        machine: NewMachineWithCommand,
        id: CommandIdKind,
    ) -> MachineKey {
        let mach = self.add_new_command_machine(machine);
        let key = mach.machine;
        if let CommandIdKind::LangIssued(id) = id {
            self.id_to_machine.insert(id, key);
        }
        if matches!(id, CommandIdKind::CoreInternal) {
            self.machine_is_core_created.insert(key, ());
        }
        self.current_wf_task_commands.push_back(mach);
        key
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

    /// Given a user's versioning intent for a command and that command's target task queue,
    /// returns whether or not the command should set the flag for attempting to stick within the
    /// compatible version set
    fn determine_use_compatible_flag(&self, intent: VersioningIntent, target_tq: &str) -> bool {
        match intent {
            VersioningIntent::Compatible => true,
            VersioningIntent::Default => false,
            VersioningIntent::Unspecified => {
                // If the target TQ is empty, that means use same TQ.
                // When TQs match, use compat by default
                target_tq.is_empty() || target_tq == self.task_queue
            }
        }
    }
}

fn str_to_randomness_seed(run_id: &str) -> u64 {
    // This was originally `DefaultHasher` but that is potentially unstable across Rust releases.
    // This must forever be `SipHasher13` now or we risk breaking history compat.
    let mut s = SipHasher13::new();
    run_id.hash(&mut s);
    s.finish()
}

#[must_use]
enum EventHandlingOutcome {
    SkipEvent { skip_next_event: bool },
    SkipCommand,
    Normal,
}

/// Special handling for patch markers, when handling command events as in
/// [WorkflowMachines::handle_command_event]
fn change_marker_handling(
    event: &HistoryEvent,
    mach: &Machines,
    next_event: Option<&HistoryEvent>,
) -> Result<EventHandlingOutcome> {
    if !mach.matches_event(event) {
        // Version markers can be skipped in the event they are deprecated
        if let Some((patch_name, deprecated)) = event.get_patch_marker_details() {
            // Is deprecated. We can simply ignore this event, as deprecated change
            // markers are allowed without matching changed calls.
            if deprecated {
                debug!("Deprecated patch marker tried against wrong machine, skipping.");

                // Also ignore the subsequent upsert event if present
                let mut skip_next_event = false;
                if let Some(Attributes::UpsertWorkflowSearchAttributesEventAttributes(atts)) =
                    next_event.and_then(|ne| ne.attributes.as_ref())
                {
                    if let Some(ref sa) = atts.search_attributes {
                        skip_next_event = sa.indexed_fields.contains_key(VERSION_SEARCH_ATTR_KEY);
                    }
                }

                return Ok(EventHandlingOutcome::SkipEvent { skip_next_event });
            }
            return Err(WFMachinesError::Nondeterminism(format!(
                "Non-deprecated patch marker encountered for change {patch_name}, \
                            but there is no corresponding change command!"
            )));
        }
        // Patch machines themselves may also not *have* matching markers, where non-deprecated
        // calls take the old path, and deprecated calls assume history is produced by a new-code
        // worker.
        if matches!(mach, Machines::PatchMachine(_)) {
            debug!("Skipping non-matching event against patch machine");
            return Ok(EventHandlingOutcome::SkipCommand);
        }
    }
    Ok(EventHandlingOutcome::Normal)
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
