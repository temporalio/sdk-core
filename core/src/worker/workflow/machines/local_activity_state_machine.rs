use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, OnEventWrapper, WFMachinesAdapter,
    WFMachinesError,
};
use crate::{
    protosext::{CompleteLocalActivityData, HistoryEventExt, ValidScheduleLA},
    worker::{workflow::OutgoingJob, LocalActivityExecutionResult},
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::{
    convert::TryFrom,
    time::{Duration, SystemTime},
};
use temporal_sdk_core_protos::{
    constants::LOCAL_ACTIVITY_MARKER_NAME,
    coresdk::{
        activity_result::{
            ActivityResolution, Cancellation, DoBackoff, Failure as ActFail, Success,
        },
        common::build_local_activity_marker_details,
        external_data::LocalActivityMarkerData,
        workflow_activation::ResolveActivity,
        workflow_commands::ActivityCancellationType,
    },
    temporal::api::{
        command::v1::{Command, RecordMarkerCommandAttributes},
        enums::v1::{CommandType, EventType},
        failure::v1::failure::FailureInfo,
        history::v1::HistoryEvent,
    },
    utilities::TryIntoOrNone,
};

fsm! {
    pub(super) name LocalActivityMachine;
    command LocalActivityCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying (referring to whether or not the workflow
    // is replaying), and then immediately scheduled and transitions to either requesting that lang
    // execute the activity, or waiting for the marker from history.
    Executing --(Schedule, shared on_schedule) --> RequestSent;
    Replaying --(Schedule, on_schedule) --> WaitingMarkerEvent;
    ReplayingPreResolved --(Schedule, on_schedule) --> WaitingMarkerEventPreResolved;

    // Execution path =============================================================================
    RequestSent --(HandleResult(ResolveDat), on_handle_result) --> MarkerCommandCreated;
    // We loop back on RequestSent here because the LA needs to report its result
    RequestSent --(Cancel, on_cancel_requested) --> RequestSent;
    // No wait cancels skip waiting for the LA to report the result, but do generate a command
    // to record the cancel marker
    RequestSent --(NoWaitCancel(ActivityCancellationType), shared on_no_wait_cancel)
      --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded)
      --> MarkerCommandRecorded;

    // Replay path ================================================================================
    // LAs on the replay path should never have handle result explicitly called on them, but do need
    // to eventually see the marker
    WaitingMarkerEvent --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded)
      --> MarkerCommandRecorded;
    // If we are told to cancel while waiting for the marker, we still need to wait for the marker.
    WaitingMarkerEvent --(Cancel, on_cancel_requested) --> WaitingMarkerEventCancelled;
    WaitingMarkerEvent --(NoWaitCancel(ActivityCancellationType),
                          on_no_wait_cancel) --> WaitingMarkerEventCancelled;
    WaitingMarkerEventCancelled --(HandleResult(ResolveDat), on_handle_result) --> WaitingMarkerEvent;

    // It is entirely possible to have started the LA while replaying, only to find that we have
    // reached a new WFT and there still was no marker. In such cases we need to execute the LA.
    // This can easily happen if upon first execution, the worker does WFT heartbeating but then
    // dies for some reason.
    WaitingMarkerEvent --(StartedNonReplayWFT, shared on_started_non_replay_wft) --> RequestSent;

    // If the activity is pre resolved we still expect to see marker recorded event at some point,
    // even though we already resolved the activity.
    WaitingMarkerEventPreResolved --(MarkerRecorded(CompleteLocalActivityData),
                                     shared on_marker_recorded) --> MarkerCommandRecorded;
    // Ignore cancellations when waiting for the marker after being pre-resolved
    WaitingMarkerEventPreResolved --(Cancel) --> WaitingMarkerEventPreResolved;
    WaitingMarkerEventPreResolved --(NoWaitCancel(ActivityCancellationType))
                                     --> WaitingMarkerEventPreResolved;

    // Ignore cancellation in final state
    MarkerCommandRecorded --(Cancel, on_cancel_requested) --> MarkerCommandRecorded;
    MarkerCommandRecorded --(NoWaitCancel(ActivityCancellationType),
                             on_no_wait_cancel) --> MarkerCommandRecorded;

    // LAs reporting status after they've handled their result can simply be ignored. We could
    // optimize this away higher up but that feels very overkill.
    MarkerCommandCreated --(HandleResult(ResolveDat)) --> MarkerCommandCreated;
    ResultNotified --(HandleResult(ResolveDat)) --> ResultNotified;
    MarkerCommandRecorded --(HandleResult(ResolveDat)) --> MarkerCommandRecorded;
}

#[derive(Debug, Clone)]
pub(super) struct ResolveDat {
    pub(super) result: LocalActivityExecutionResult,
    pub(super) complete_time: Option<SystemTime>,
    pub(super) attempt: u32,
    pub(super) backoff: Option<prost_types::Duration>,
    pub(super) original_schedule_time: Option<SystemTime>,
}

impl From<CompleteLocalActivityData> for ResolveDat {
    fn from(d: CompleteLocalActivityData) -> Self {
        ResolveDat {
            result: match d.result {
                Ok(res) => LocalActivityExecutionResult::Completed(Success { result: Some(res) }),
                Err(fail) => {
                    if matches!(fail.failure_info, Some(FailureInfo::CanceledFailureInfo(_))) {
                        LocalActivityExecutionResult::Cancelled(Cancellation {
                            failure: Some(fail),
                        })
                    } else {
                        LocalActivityExecutionResult::Failed(ActFail {
                            failure: Some(fail),
                        })
                    }
                }
            },
            complete_time: d.marker_dat.complete_time.try_into_or_none(),
            attempt: d.marker_dat.attempt,
            backoff: d.marker_dat.backoff,
            original_schedule_time: d.marker_dat.original_schedule_time.try_into_or_none(),
        }
    }
}

/// Creates a new local activity state machine & immediately schedules the local activity for
/// execution. No command is produced immediately to be sent to the server, as the local activity
/// must resolve before we send a record marker command. A [MachineResponse] may be produced,
/// to queue the LA for execution if it needs to be.
pub(super) fn new_local_activity(
    mut attrs: ValidScheduleLA,
    replaying_when_invoked: bool,
    maybe_pre_resolved: Option<ResolveDat>,
    wf_time: Option<SystemTime>,
) -> Result<(LocalActivityMachine, Vec<MachineResponse>), WFMachinesError> {
    let initial_state = if replaying_when_invoked {
        if let Some(dat) = maybe_pre_resolved {
            ReplayingPreResolved { dat }.into()
        } else {
            Replaying {}.into()
        }
    } else {
        if maybe_pre_resolved.is_some() {
            return Err(WFMachinesError::Nondeterminism(
                "Local activity cannot be created as pre-resolved while not replaying".to_string(),
            ));
        }
        Executing {}.into()
    };

    // If the scheduled LA doesn't already have an "original" schedule time, assign one.
    attrs
        .original_schedule_time
        .get_or_insert(SystemTime::now());

    let mut machine = LocalActivityMachine {
        state: initial_state,
        shared_state: SharedState {
            attrs,
            replaying_when_invoked,
            wf_time_when_started: wf_time,
        },
    };

    let mut res = OnEventWrapper::on_event_mut(&mut machine, LocalActivityMachineEvents::Schedule)
        .expect("Scheduling local activities doesn't fail");
    let mr = if let Some(res) = res.pop() {
        machine
            .adapt_response(res, None)
            .expect("Adapting LA schedule response doesn't fail")
    } else {
        vec![]
    };
    Ok((machine, mr))
}

impl LocalActivityMachine {
    /// Is called to check if, while handling the LA marker event, we should avoid doing normal
    /// command-event processing - instead simply applying the event to this machine and then
    /// skipping over the rest. If this machine is in the `ResultNotified` state, that means
    /// command handling should proceed as normal (ie: The command needs to be matched and removed).
    /// The other valid states to make this check in are the `WaitingMarkerEvent[PreResolved]`
    /// states, which will return true.
    ///
    /// Attempting the check in any other state likely means a bug in the SDK.
    pub(super) fn marker_should_get_special_handling(&self) -> Result<bool, WFMachinesError> {
        match &self.state {
            LocalActivityMachineState::ResultNotified(_) => Ok(false),
            LocalActivityMachineState::WaitingMarkerEvent(_) => Ok(true),
            LocalActivityMachineState::WaitingMarkerEventPreResolved(_) => Ok(true),
            _ => Err(WFMachinesError::Fatal(format!(
                "Attempted to check for LA marker handling in invalid state {}",
                self.state
            ))),
        }
    }

    /// Must be called if the workflow encounters a non-replay workflow task
    pub(super) fn encountered_non_replay_wft(
        &mut self,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        // This only applies to the waiting-for-marker state. It can safely be ignored in the others
        if !matches!(
            self.state(),
            LocalActivityMachineState::WaitingMarkerEvent(_)
        ) {
            return Ok(vec![]);
        }

        let mut res =
            OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::StartedNonReplayWFT)
                .map_err(|e| match e {
                    MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                        "Invalid transition while notifying local activity (seq {})\
                         of non-replay-wft-started in {}",
                        self.shared_state.attrs.seq,
                        self.state(),
                    )),
                    MachineError::Underlying(e) => e,
                })?;
        let res = res.pop().expect("Always produces one response");
        Ok(self
            .adapt_response(res, None)
            .expect("Adapting LA wft-non-replay response doesn't fail"))
    }

    /// Attempt to resolve the local activity with a result from execution (not from history)
    pub(super) fn try_resolve(
        &mut self,
        result: LocalActivityExecutionResult,
        runtime: Duration,
        attempt: u32,
        backoff: Option<prost_types::Duration>,
        original_schedule_time: Option<SystemTime>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        self.try_resolve_with_dat(ResolveDat {
            result,
            complete_time: self.shared_state.wf_time_when_started.map(|t| t + runtime),
            attempt,
            backoff,
            original_schedule_time,
        })
    }
    /// Attempt to resolve the local activity with already known data, ex pre-resolved data
    pub(super) fn try_resolve_with_dat(
        &mut self,
        dat: ResolveDat,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let res = OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::HandleResult(dat))
            .map_err(|e| match e {
                MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                    "Invalid transition resolving local activity (seq {}) in {}",
                    self.shared_state.attrs.seq,
                    self.state(),
                )),
                MachineError::Underlying(e) => e,
            })?;

        Ok(res
            .into_iter()
            .flat_map(|res| {
                self.adapt_response(res, None)
                    .expect("Adapting LA resolve response doesn't fail")
            })
            .collect())
    }
}

#[derive(Clone)]
pub(super) struct SharedState {
    attrs: ValidScheduleLA,
    replaying_when_invoked: bool,
    wf_time_when_started: Option<SystemTime>,
}

impl SharedState {
    fn produce_no_wait_cancel_resolve_dat(&self) -> ResolveDat {
        ResolveDat {
            result: LocalActivityExecutionResult::empty_cancel(),
            // Just don't provide a complete time, which means try-cancel/abandon cancels won't
            // advance the clock. Seems like that's fine, since you can only cancel after awaiting
            // some other command, which would have appropriately advanced the clock anyway.
            complete_time: None,
            attempt: self.attrs.attempt,
            backoff: None,
            original_schedule_time: self.attrs.original_schedule_time,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, derive_more::Display)]
pub(super) enum LocalActivityCommand {
    RequestActivityExecution(ValidScheduleLA),
    #[display(fmt = "Resolved")]
    Resolved(ResolveDat),
    /// The fake marker is used to avoid special casing marker recorded event handling.
    /// If we didn't have the fake marker, there would be no "outgoing command" to match
    /// against the event. This way there is, but the command never will be issued to
    /// server because it is understood to be meaningless.
    #[display(fmt = "FakeMarker")]
    FakeMarker,
    /// Indicate we want to cancel an LA that is currently executing, or look up if we have
    /// processed a marker with resolution data since the machine was constructed.
    #[display(fmt = "Cancel")]
    RequestCancel,
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(
        self,
        dat: SharedState,
    ) -> LocalActivityMachineTransition<RequestSent> {
        TransitionResult::commands([LocalActivityCommand::RequestActivityExecution(dat.attrs)])
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResultType {
    Completed,
    Cancelled,
    Failed,
}
#[derive(Clone)]
pub(super) struct MarkerCommandCreated {
    result_type: ResultType,
}
impl From<MarkerCommandCreated> for ResultNotified {
    fn from(mc: MarkerCommandCreated) -> Self {
        Self {
            result_type: mc.result_type,
        }
    }
}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> LocalActivityMachineTransition<ResultNotified> {
        TransitionResult::from(self)
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}
impl MarkerCommandRecorded {
    fn on_cancel_requested(self) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        // We still must issue a cancel request even if this command is resolved, because if it
        // failed and we are backing off locally, we must tell the LA dispatcher to quit retrying.
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        cancel_type: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        if matches!(cancel_type, ActivityCancellationType::TryCancel) {
            // We still must issue a cancel request even if this command is resolved, because if it
            // failed and we are backing off locally, we must tell the LA dispatcher to quit
            // retrying.
            TransitionResult::ok(
                [LocalActivityCommand::RequestCancel],
                MarkerCommandRecorded::default(),
            )
        } else {
            TransitionResult::default()
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct Replaying {}
impl Replaying {
    pub(super) fn on_schedule(self) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::ok(
            [],
            WaitingMarkerEvent {
                already_resolved: false,
            },
        )
    }
}

#[derive(Clone)]
pub(super) struct ReplayingPreResolved {
    dat: ResolveDat,
}
impl ReplayingPreResolved {
    pub(super) fn on_schedule(
        self,
    ) -> LocalActivityMachineTransition<WaitingMarkerEventPreResolved> {
        TransitionResult::ok(
            [
                LocalActivityCommand::FakeMarker,
                LocalActivityCommand::Resolved(self.dat),
            ],
            WaitingMarkerEventPreResolved {},
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestSent {}

impl RequestSent {
    fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        let result_type = match &dat.result {
            LocalActivityExecutionResult::Completed(_) => ResultType::Completed,
            LocalActivityExecutionResult::Failed(_) => ResultType::Failed,
            LocalActivityExecutionResult::TimedOut(_) => ResultType::Failed,
            LocalActivityExecutionResult::Cancelled { .. } => ResultType::Cancelled,
        };
        let new_state = MarkerCommandCreated { result_type };
        TransitionResult::ok([LocalActivityCommand::Resolved(dat)], new_state)
    }

    fn on_cancel_requested(self) -> LocalActivityMachineTransition<RequestSent> {
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        shared: SharedState,
        cancel_type: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        let mut cmds = vec![];
        if matches!(cancel_type, ActivityCancellationType::TryCancel) {
            // For try-cancels also request the cancel
            cmds.push(LocalActivityCommand::RequestCancel);
        }
        // Immediately resolve
        cmds.push(LocalActivityCommand::Resolved(
            shared.produce_no_wait_cancel_resolve_dat(),
        ));
        TransitionResult::ok(
            cmds,
            MarkerCommandCreated {
                result_type: ResultType::Cancelled,
            },
        )
    }
}

macro_rules! verify_marker_dat {
    ($shared:expr, $dat:expr, $ok_expr:expr) => {
        if let Err(err) = verify_marker_data_matches($shared, $dat) {
            TransitionResult::Err(err)
        } else {
            $ok_expr
        }
    };
}

#[derive(Clone)]
pub(super) struct ResultNotified {
    result_type: ResultType,
}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        if self.result_type == ResultType::Completed && dat.result.is_err() {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Local activity (seq {}) completed successfully locally, but history said \
                 it failed!",
                shared.attrs.seq
            )));
        } else if self.result_type == ResultType::Failed && dat.result.is_ok() {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Local activity (seq {}) failed locally, but history said it completed!",
                shared.attrs.seq
            )));
        }
        verify_marker_dat!(&shared, &dat, TransitionResult::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {
    already_resolved: bool,
}

impl WaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(
            &shared,
            &dat,
            TransitionResult::commands(if self.already_resolved {
                vec![]
            } else {
                vec![LocalActivityCommand::Resolved(dat.into())]
            })
        )
    }
    pub(super) fn on_started_non_replay_wft(
        self,
        mut dat: SharedState,
    ) -> LocalActivityMachineTransition<RequestSent> {
        // We aren't really "replaying" anymore for our purposes, and want to record the marker.
        dat.replaying_when_invoked = false;
        TransitionResult::ok_shared(
            [LocalActivityCommand::RequestActivityExecution(
                dat.attrs.clone(),
            )],
            RequestSent::default(),
            dat,
        )
    }

    fn on_cancel_requested(self) -> LocalActivityMachineTransition<WaitingMarkerEventCancelled> {
        // We still "request a cancel" even though we know the local activity should not be running
        // because the data might be in the pre-resolved list.
        TransitionResult::ok(
            [LocalActivityCommand::RequestCancel],
            WaitingMarkerEventCancelled {},
        )
    }

    fn on_no_wait_cancel(
        self,
        _: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<WaitingMarkerEventCancelled> {
        // Markers are always recorded when cancelling, so this is the same as a normal cancel on
        // the replay path
        self.on_cancel_requested()
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEventCancelled {}
impl WaitingMarkerEventCancelled {
    fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::ok(
            [LocalActivityCommand::Resolved(dat)],
            WaitingMarkerEvent {
                already_resolved: true,
            },
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEventPreResolved {}
impl WaitingMarkerEventPreResolved {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(&shared, &dat, TransitionResult::default())
    }
}

impl Cancellable for LocalActivityMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let event = match self.shared_state.attrs.cancellation_type {
            ct @ ActivityCancellationType::TryCancel | ct @ ActivityCancellationType::Abandon => {
                LocalActivityMachineEvents::NoWaitCancel(ct)
            }
            _ => LocalActivityMachineEvents::Cancel,
        };
        let cmds = OnEventWrapper::on_event_mut(self, event)?;
        let mach_resps = cmds
            .into_iter()
            .map(|mc| self.adapt_response(mc, None))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(mach_resps)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        // This needs to always be false because for the situation where we cancel in the same WFT,
        // no command of any kind is created and no LA request is queued. Otherwise, the command we
        // create to record a cancel marker *needs* to be sent to the server still, which returning
        // true here would prevent.
        false
    }
}

#[derive(Default, Clone)]
pub(super) struct Abandoned {}

impl WFMachinesAdapter for LocalActivityMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        match my_command {
            LocalActivityCommand::RequestActivityExecution(act) => {
                Ok(vec![MachineResponse::QueueLocalActivity(act)])
            }
            LocalActivityCommand::Resolved(ResolveDat {
                result,
                complete_time,
                attempt,
                backoff,
                original_schedule_time,
            }) => {
                let mut maybe_ok_result = None;
                let mut maybe_failure = None;
                // Only issue record marker commands if we weren't replaying
                let record_marker = !self.shared_state.replaying_when_invoked;
                let mut will_not_run_again = false;
                match result.clone() {
                    LocalActivityExecutionResult::Completed(suc) => {
                        maybe_ok_result = suc.result;
                    }
                    LocalActivityExecutionResult::Failed(fail) => {
                        maybe_failure = fail.failure;
                    }
                    LocalActivityExecutionResult::Cancelled(Cancellation { failure })
                    | LocalActivityExecutionResult::TimedOut(ActFail { failure }) => {
                        will_not_run_again = true;
                        maybe_failure = failure;
                    }
                };
                let resolution = if let Some(b) = backoff.as_ref() {
                    ActivityResolution {
                        status: Some(
                            DoBackoff {
                                attempt: attempt + 1,
                                backoff_duration: Some(b.clone()),
                                original_schedule_time: original_schedule_time.map(Into::into),
                            }
                            .into(),
                        ),
                    }
                } else {
                    result.into()
                };
                let mut responses = vec![
                    MachineResponse::PushWFJob(OutgoingJob {
                        variant: ResolveActivity {
                            seq: self.shared_state.attrs.seq,
                            result: Some(resolution),
                        }
                        .into(),
                        is_la_resolution: true,
                    }),
                    MachineResponse::UpdateWFTime(complete_time),
                ];

                // Cancel-resolves of abandoned activities must be explicitly dropped from tracking
                // to avoid unnecessary WFT heartbeating.
                if will_not_run_again
                    && matches!(
                        self.shared_state.attrs.cancellation_type,
                        ActivityCancellationType::Abandon
                    )
                {
                    responses.push(MachineResponse::AbandonLocalActivity(
                        self.shared_state.attrs.seq,
                    ));
                }

                if record_marker {
                    let marker_data = RecordMarkerCommandAttributes {
                        marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_string(),
                        details: build_local_activity_marker_details(
                            LocalActivityMarkerData {
                                seq: self.shared_state.attrs.seq,
                                attempt,
                                activity_id: self.shared_state.attrs.activity_id.clone(),
                                activity_type: self.shared_state.attrs.activity_type.clone(),
                                complete_time: complete_time.map(Into::into),
                                backoff,
                                original_schedule_time: original_schedule_time.map(Into::into),
                            },
                            maybe_ok_result,
                        ),
                        header: None,
                        failure: maybe_failure,
                    };
                    responses.push(MachineResponse::IssueNewCommand(Command {
                        command_type: CommandType::RecordMarker as i32,
                        attributes: Some(marker_data.into()),
                    }));
                }
                Ok(responses)
            }
            LocalActivityCommand::FakeMarker => {
                // See docs for `FakeMarker` for more
                Ok(vec![MachineResponse::IssueFakeLocalActivityMarker(
                    self.shared_state.attrs.seq,
                )])
            }
            LocalActivityCommand::RequestCancel => {
                Ok(vec![MachineResponse::RequestCancelLocalActivity(
                    self.shared_state.attrs.seq,
                )])
            }
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.is_local_activity_marker()
    }
}

impl TryFrom<CommandType> for LocalActivityMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistoryEvent> for LocalActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        if e.event_type() != EventType::MarkerRecorded {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Local activity machine cannot handle this event: {}",
                e
            )));
        }

        match e.into_local_activity_marker_details() {
            Some(marker_dat) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(WFMachinesError::Nondeterminism(
                "Local activity machine encountered an unparsable marker".to_string(),
            )),
        }
    }
}

fn verify_marker_data_matches(
    shared: &SharedState,
    dat: &CompleteLocalActivityData,
) -> Result<(), WFMachinesError> {
    if shared.attrs.seq != dat.marker_dat.seq {
        return Err(WFMachinesError::Nondeterminism(format!(
            "Local activity marker data has sequence number {} but matched against LA \
            command with sequence number {}",
            dat.marker_dat.seq, shared.attrs.seq
        )));
    }

    Ok(())
}

impl From<LocalActivityExecutionResult> for ActivityResolution {
    fn from(lar: LocalActivityExecutionResult) -> Self {
        match lar {
            LocalActivityExecutionResult::Completed(c) => ActivityResolution {
                status: Some(c.into()),
            },
            LocalActivityExecutionResult::Failed(f) | LocalActivityExecutionResult::TimedOut(f) => {
                ActivityResolution {
                    status: Some(f.into()),
                }
            }
            LocalActivityExecutionResult::Cancelled(cancel) => ActivityResolution {
                status: Some(cancel.into()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder, test_help::canned_histories, worker::workflow::ManagedWFFunc,
    };
    use rstest::rstest;
    use std::time::Duration;
    use temporal_sdk::{
        CancellableFuture, LocalActivityOptions, WfContext, WorkflowFunction, WorkflowResult,
    };
    use temporal_sdk_core_protos::{
        coresdk::{
            activity_result::ActivityExecutionResult,
            workflow_activation::{workflow_activation_job, WorkflowActivationJob},
            workflow_commands::ActivityCancellationType::WaitCancellationCompleted,
        },
        temporal::api::{
            command::v1::command, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        },
    };

    async fn la_wf(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false, true)]
    #[case::replay(true, true)]
    #[case::incremental_fail(false, false)]
    #[case::replay_fail(true, false)]
    #[tokio::test]
    async fn one_la_success(#[case] replay: bool, #[case] completes_ok: bool) {
        let func = WorkflowFunction::new(la_wf);
        let activity_id = "1";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if completes_ok {
            t.add_local_activity_result_marker(1, activity_id, b"hi".into());
        } else {
            t.add_local_activity_fail_marker(
                1,
                activity_id,
                Failure::application_failure("I failed".to_string(), false),
            );
        }
        t.add_workflow_execution_completed();

        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);

        let ready_to_execute_las = wfm.drain_queued_local_activities();
        if !replay {
            assert_eq!(ready_to_execute_las.len(), 1);
        } else {
            assert_eq!(ready_to_execute_las.len(), 0);
        }

        if !replay {
            if completes_ok {
                wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"hi".into()))
                    .unwrap();
            } else {
                wfm.complete_local_activity(
                    1,
                    ActivityExecutionResult::fail(Failure {
                        message: "I failed".to_string(),
                        ..Default::default()
                    }),
                )
                .unwrap();
            }
        }

        // Now the next activation will unblock the local activity
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            if completes_ok {
                assert_matches!(
                    commands[0].attributes.as_ref().unwrap(),
                    command::Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes { failure: None, .. }
                    )
                );
            } else {
                assert_matches!(
                    commands[0].attributes.as_ref().unwrap(),
                    command::Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes {
                            failure: Some(_),
                            ..
                        }
                    )
                );
            }
            assert_eq!(
                commands[1].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        assert_eq!(wfm.drain_queued_local_activities().len(), 0);
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn two_la_wf(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn two_sequential_las(#[case] replay: bool) {
        let func = WorkflowFunction::new(two_la_wf);
        let t = canned_histories::two_local_activities_one_wft(false);
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        let act = wfm.get_next_activation().await.unwrap();
        let first_act_ts = act.timestamp.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = usize::from(!replay);
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        // Verify LAs advance time (they take 1s in this test)
        assert_eq!(act.timestamp.unwrap().seconds, first_act_ts.seconds + 1);
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 1)
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        if !replay {
            assert_eq!(ready_to_execute_las.len(), 1);
        } else {
            assert_eq!(ready_to_execute_las.len(), 0);
        }

        if !replay {
            wfm.complete_local_activity(2, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_eq!(act.timestamp.unwrap().seconds, first_act_ts.seconds + 2);
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 2)
        );
        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 3);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[2].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn two_la_wf_parallel(ctx: WfContext) -> WorkflowResult<()> {
        tokio::join!(
            ctx.local_activity(LocalActivityOptions::default()),
            ctx.local_activity(LocalActivityOptions::default())
        );
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn two_parallel_las(#[case] replay: bool) {
        let func = WorkflowFunction::new(two_la_wf_parallel);
        let t = canned_histories::two_local_activities_one_wft(true);
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity(ies) will be put into the queue
        // for execution
        let act = wfm.get_next_activation().await.unwrap();
        let first_act_ts = act.timestamp.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = if !replay { 2 } else { 0 };
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
            wfm.complete_local_activity(2, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_eq!(act.timestamp.unwrap().seconds, first_act_ts.seconds + 1);
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra2))
            }] => {assert_eq!(ra.seq, 1); assert_eq!(ra2.seq, 2)}
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 0);

        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 3);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[2].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        let act = wfm.get_next_activation().await.unwrap();
        // Still only 1s ahead b/c parallel
        assert_eq!(act.timestamp.unwrap().seconds, first_act_ts.seconds + 1);
        assert_eq!(act.jobs.len(), 0);
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn la_timer_la(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        ctx.timer(Duration::from_secs(5)).await;
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn las_separated_by_timer(#[case] replay: bool) {
        let func = WorkflowFunction::new(la_timer_la);
        let t = canned_histories::two_local_activities_separated_by_timer();
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = usize::from(!replay);
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 1)
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 0);

        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
        }

        let act = if !replay {
            wfm.new_history(t.get_history_info(2).unwrap().into())
                .await
                .unwrap()
        } else {
            wfm.get_next_activation().await.unwrap()
        };
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_))
            }]
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = usize::from(!replay);
        assert_eq!(ready_to_execute_las.len(), num_queued);
        if !replay {
            wfm.complete_local_activity(2, ActivityExecutionResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 2)
        );

        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[1].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        wfm.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn one_la_heartbeating_wft_failure_still_executes() {
        let func = WorkflowFunction::new(la_wf);
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        // Heartbeats
        t.add_full_wf_task();
        // fails a wft for some reason
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::NonDeterministicError,
            Default::default(),
        );
        t.add_workflow_task_scheduled_and_started();

        let histinfo = t.get_full_history_info().unwrap().into();
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will request to run the LA since the heartbeat & wft failure are skipped
        // over
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 1);
        // We can happily complete it now
        wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"hi".into()))
            .unwrap();

        wfm.shutdown().await.unwrap();
    }

    /// This test verifies something that technically shouldn't really be possible but is worth
    /// checking anyway. What happens if in memory we think an LA passed but then the next history
    /// chunk comes back with it failing? We should fail with a mismatch.
    #[tokio::test]
    async fn exec_passes_but_history_has_fail() {
        let func = WorkflowFunction::new(la_wf);
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_local_activity_fail_marker(
            1,
            "1",
            Failure::application_failure("I failed".to_string(), false),
        );
        t.add_workflow_execution_completed();

        let histinfo = t.get_history_info(1).unwrap().into();
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 1);
        // Completes OK
        wfm.complete_local_activity(1, ActivityExecutionResult::ok(b"hi".into()))
            .unwrap();

        // next activation unblocks LA
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(
            commands[1].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );

        let err = wfm
            .new_history(t.get_full_history_info().unwrap().into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Nondeterminism"));
        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn immediate_cancel(
        #[values(
            ActivityCancellationType::WaitCancellationCompleted,
            ActivityCancellationType::TryCancel,
            ActivityCancellationType::Abandon
        )]
        cancel_type: ActivityCancellationType,
    ) {
        let func = WorkflowFunction::new(move |ctx| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                cancel_type,
                ..Default::default()
            });
            la.cancel(&ctx);
            la.await;
            Ok(().into())
        });
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let histinfo = t.get_history_info(1).unwrap().into();
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        // We record the cancel marker
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        // Importantly, the activity shouldn't get executed since it was insta-cancelled
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 0);

        // next activation unblocks LA, which is cancelled now.
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(
            commands[1].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );

        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn cancel_after_act_starts(
        #[case] replay: bool,
        #[values(
            ActivityCancellationType::WaitCancellationCompleted,
            ActivityCancellationType::TryCancel,
            ActivityCancellationType::Abandon
        )]
        cancel_type: ActivityCancellationType,
    ) {
        let func = WorkflowFunction::new(move |ctx| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                cancel_type,
                ..Default::default()
            });
            ctx.timer(Duration::from_secs(1)).await;
            la.cancel(&ctx);
            // This extra timer is here to ensure the presence of another WF task doesn't mess up
            // resolving the LA with cancel on replay
            ctx.timer(Duration::from_secs(1)).await;
            let resolution = la.await;
            assert!(resolution.cancelled());
            Ok(().into())
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add_timer_fired(timer_started_event_id, "1".to_string());
        t.add_full_wf_task();
        if cancel_type != ActivityCancellationType::WaitCancellationCompleted {
            // With non-wait cancels, the cancel is immediate
            t.add_local_activity_cancel_marker(1, "1");
        }
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
            // With wait cancels, the cancel marker is not recorded until activity reports.
            t.add_local_activity_cancel_marker(1, "1");
        }
        t.add_timer_fired(timer_started_event_id, "2".to_string());
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = usize::from(!replay);
        assert_eq!(ready_to_execute_las.len(), num_queued);

        // Next activation timer fires and activity cancel will be requested
        if replay {
            wfm.get_next_activation().await.unwrap()
        } else {
            wfm.new_history(t.get_history_info(2).unwrap().into())
                .await
                .unwrap()
        };

        let commands = wfm.get_server_commands().commands;
        if cancel_type == ActivityCancellationType::WaitCancellationCompleted || replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        } else {
            // Try-cancel/abandon will immediately record marker (when not replaying)
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
        }

        if replay {
            wfm.get_next_activation().await.unwrap()
        } else {
            // On non replay, there's an additional activation, because completing with the cancel
            // wants to wake up the workflow to see if resolving the LA as cancelled did anything.
            // In this case, it doesn't really, because we just hit the next timer which is also
            // what would have happened if we woke up with new history -- but it does mean we
            // generate the commands at this point. This matters b/c we want to make sure the record
            // marker command is sent as soon as cancel happens.
            if cancel_type == WaitCancellationCompleted {
                wfm.complete_local_activity(1, ActivityExecutionResult::cancel_from_details(None))
                    .unwrap();
            }
            wfm.get_next_activation().await.unwrap();
            let commands = wfm.get_server_commands().commands;
            assert_eq!(commands.len(), 2);
            if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
            } else {
                assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
            }

            wfm.new_history(t.get_history_info(3).unwrap().into())
                .await
                .unwrap()
        };

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );

        wfm.shutdown().await.unwrap();
    }
}
