#![allow(clippy::large_enum_variant)]

use crate::{
    machines::CancellableCommand,
    protos::{
        coresdk::HistoryEventId,
        temporal::api::{
            command::v1::{command::Attributes, Command, StartTimerCommandAttributes},
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent, TimerCanceledEventAttributes},
        },
    },
};
use rustfsm::{fsm, TransitionResult};

fsm! {
    name TimerMachine;
    command TimerCommand;
    error TimerMachineError;
    shared_state SharedState;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated --(CommandCancelTimer, shared on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;

    Created --(Schedule, shared on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired(HistoryEvent), on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, on_cancel) --> CancelTimerCommandCreated;
}

#[derive(Default, Clone)]
pub struct SharedState {
    timer_attributes: StartTimerCommandAttributes,
}

impl SharedState {
    fn into_timer_canceled_event_command(self) -> TimerCommand {
        let attrs = TimerCanceledEventAttributes {
            identity: "workflow".to_string(),
            timer_id: self.timer_attributes.timer_id,
            ..Default::default()
        };
        let event = HistoryEvent {
            event_type: EventType::TimerCanceled as i32,
            attributes: Some(history_event::Attributes::TimerCanceledEventAttributes(
                attrs,
            )),
            ..Default::default()
        };
        TimerCommand::ProduceHistoryEvent(event)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TimerMachineError {}

pub enum TimerCommand {
    StartTimer(CancellableCommand),
    CancelTimer(/* TODO: Command attribs */),
    ProduceHistoryEvent(HistoryEvent),
}

#[derive(Default, Clone)]
pub struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub fn on_command_cancel_timer(self, dat: SharedState) -> TimerMachineTransition {
        TimerMachineTransition::ok(
            vec![dat.into_timer_canceled_event_command()],
            Canceled::default(),
        )
    }
}

#[derive(Default, Clone)]
pub struct CancelTimerCommandSent {}

#[derive(Default, Clone)]
pub struct Canceled {}
impl From<CancelTimerCommandSent> for Canceled {
    fn from(_: CancelTimerCommandSent) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self, dat: SharedState) -> TimerMachineTransition {
        let cmd = Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(Attributes::StartTimerCommandAttributes(
                dat.timer_attributes,
            )),
        };
        TimerMachineTransition::ok(
            vec![TimerCommand::StartTimer(cmd.clone().into())],
            StartCommandCreated {
                cancellable_command: CancellableCommand::from(cmd),
            },
        )
    }
}

#[derive(Default, Clone)]
pub struct Fired {}

#[derive(Clone)]
pub struct StartCommandCreated {
    cancellable_command: CancellableCommand,
}

impl StartCommandCreated {
    pub fn on_timer_started(self, id: HistoryEventId) -> TimerMachineTransition {
        // Java recorded an initial event ID, but it seemingly was never used.
        TimerMachineTransition::default::<StartCommandRecorded>()
    }
    pub fn on_cancel(mut self, dat: SharedState) -> TimerMachineTransition {
        // Cancel the initial command - which just sets a "canceled" flag in a wrapper of a
        // proto command. TODO: Does this make any sense?
        let _canceled_cmd = self.cancellable_command.cancel();
        TimerMachineTransition::ok(
            vec![dat.into_timer_canceled_event_command()],
            Canceled::default(),
        )
    }
}

#[derive(Default, Clone)]
pub struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub fn on_timer_fired(self, event: HistoryEvent) -> TimerMachineTransition {
        TimerMachineTransition::ok(
            vec![TimerCommand::ProduceHistoryEvent(event)],
            Fired::default(),
        )
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        TimerMachineTransition::ok(
            vec![TimerCommand::CancelTimer()],
            CancelTimerCommandCreated::default(),
        )
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn wat() {}
}
