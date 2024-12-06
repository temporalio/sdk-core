use crate::worker::workflow::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, HistEventData,
        NewMachineWithCommand, WFMachinesAdapter,
    },
    WFMachinesError,
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::ScheduleNexusOperation,
    temporal::api::{
        command::v1::Command,
        enums::v1::{CommandType, EventType},
        history::v1::{
            history_event, NexusOperationCanceledEventAttributes,
            NexusOperationCompletedEventAttributes, NexusOperationFailedEventAttributes,
            NexusOperationStartedEventAttributes, NexusOperationTimedOutEventAttributes,
        },
    },
};

fsm! {
    pub(super) name NexusOperationMachine;
    command NexusOperationCommand;
    error WFMachinesError;
    shared_state SharedState;

    ScheduleCommandCreated --(CommandScheduleNexusOperation)--> ScheduleCommandCreated;
    ScheduleCommandCreated --(NexusOperationScheduled, on_scheduled)--> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, on_cancelled)--> Cancelled;

    ScheduledEventRecorded
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    ScheduledEventRecorded
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    ScheduledEventRecorded
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    ScheduledEventRecorded
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;
    ScheduledEventRecorded
      --(NexusOperationStarted(NexusOperationStartedEventAttributes), on_started)--> Started;

    Started
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    Started
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    Started
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    Started
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum NexusOperationCommand {
    Schedule,
    Cancel,
}

#[derive(Clone, Debug)]
pub(super) struct SharedState {
    schedule_attributes: ScheduleNexusOperation,
    cancelled_before_sent: bool,
}

impl NexusOperationMachine {
    pub(super) fn new_scheduled(attribs: ScheduleNexusOperation) -> NewMachineWithCommand {
        let s = Self::from_parts(
            ScheduleCommandCreated::default().into(),
            // TODO: Probably drop input / not clone whole thing
            SharedState {
                schedule_attributes: attribs.clone(),
                cancelled_before_sent: false,
            },
        );
        let cmd = Command {
            command_type: CommandType::ScheduleNexusOperation.into(),
            attributes: Some(attribs.into()),
            user_metadata: None,
        };
        NewMachineWithCommand {
            command: cmd,
            machine: s.into(),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated;

impl ScheduleCommandCreated {
    pub(super) fn on_scheduled(self) -> NexusOperationMachineTransition<ScheduledEventRecorded> {
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_cancelled(self) -> NexusOperationMachineTransition<Cancelled> {
        // Implementation of cancelNexusOperationCommand
        NexusOperationMachineTransition::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded;

impl ScheduledEventRecorded {
    pub(super) fn on_completed(
        self,
        _ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        // Implementation of notifyCompleted
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_failed(
        self,
        _fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        // Implementation of notifyFailed
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_canceled(
        self,
        _ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        // Implementation of notifyCanceled
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_timed_out(
        self,
        _toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        // Implementation of notifyTimedOut
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_started(
        self,
        _sa: NexusOperationStartedEventAttributes,
    ) -> NexusOperationMachineTransition<Started> {
        // Implementation of notifyStarted
        NexusOperationMachineTransition::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Started;

impl Started {
    pub(super) fn on_completed(
        self,
        _ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        // Implementation of notifyCompleted
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_failed(
        self,
        _fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        // Implementation of notifyFailed
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_canceled(
        self,
        _ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        // Implementation of notifyCanceled
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_timed_out(
        self,
        _toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        // Implementation of notifyTimedOut
        NexusOperationMachineTransition::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed;

#[derive(Default, Clone)]
pub(super) struct Failed;

#[derive(Default, Clone)]
pub(super) struct TimedOut;

#[derive(Default, Clone)]
pub(super) struct Cancelled;

impl TryFrom<HistEventData> for NexusOperationMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match EventType::try_from(e.event_type) {
            Ok(EventType::NexusOperationStarted) => {
                if let Some(history_event::Attributes::NexusOperationStartedEventAttributes(sa)) =
                    e.attributes
                {
                    Self::NexusOperationStarted(sa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationStarted attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationCompleted) => {
                if let Some(history_event::Attributes::NexusOperationCompletedEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCompleted(ca)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationCompleted attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationFailed) => {
                if let Some(history_event::Attributes::NexusOperationFailedEventAttributes(fa)) =
                    e.attributes
                {
                    Self::NexusOperationFailed(fa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationFailed attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationCanceled) => {
                if let Some(history_event::Attributes::NexusOperationCanceledEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCanceled(ca)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationCanceled attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationTimedOut) => {
                if let Some(history_event::Attributes::NexusOperationTimedOutEventAttributes(toa)) =
                    e.attributes
                {
                    Self::NexusOperationTimedOut(toa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationTimedOut attributes were unset or malformed".to_string(),
                    ));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Nexus operation machine does not handle this event: {e:?}"
                )))
            }
        })
    }
}

impl WFMachinesAdapter for NexusOperationMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        todo!()
    }
}

impl TryFrom<CommandType> for NexusOperationMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ScheduleNexusOperation => Self::CommandScheduleNexusOperation,
            // CommandType::RequestCancelNexusOperation handled by separate cancel nexus op fsm.
            _ => return Err(()),
        })
    }
}

impl Cancellable for NexusOperationMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        todo!()
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}
