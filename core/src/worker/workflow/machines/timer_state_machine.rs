#![allow(clippy::large_enum_variant)]

use super::{
    workflow_machines::{MachineResponse, WFMachinesError},
    Cancellable, EventInfo, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::FireTimer,
        workflow_commands::{CancelTimer, StartTimer},
        HistoryEventId,
    },
    temporal::api::{
        command::v1::Command,
        enums::v1::{CommandType, EventType},
        history::v1::{history_event, HistoryEvent, TimerFiredEventAttributes},
    },
};

fsm! {
    pub(super) name TimerMachine;
    command TimerMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired(TimerFiredEventAttributes), shared on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, shared on_cancel) --> CancelTimerCommandCreated;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated
        --(CommandCancelTimer, on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;

    // Ignore any spurious cancellations after resolution
    Canceled --(Cancel) --> Canceled;
    Fired --(Cancel) --> Fired;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum TimerMachineCommand {
    Complete,
    IssueCancelCmd(Command),
    // We don't issue activations for timer cancellations. Lang SDK is expected to cancel
    // it's own timers when user calls cancel, and they cannot be cancelled by any other
    // means.
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: StartTimer,
    cancelled_before_sent: bool,
}

/// Creates a new, scheduled, timer as a [CancellableCommand]
pub(super) fn new_timer(attribs: StartTimer) -> NewMachineWithCommand {
    let (timer, add_cmd) = TimerMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: timer.into(),
    }
}

impl TimerMachine {
    /// Create a new timer and immediately schedule it
    fn new_scheduled(attribs: StartTimer) -> (Self, Command) {
        let mut s = Self::new(attribs);
        OnEventWrapper::on_event_mut(&mut s, TimerMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail");
        let cmd = Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(s.shared_state().attrs.clone().into()),
        };
        (s, cmd)
    }

    fn new(attribs: StartTimer) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                attrs: attribs,
                cancelled_before_sent: false,
            },
        }
    }
}

impl TryFrom<HistoryEvent> for TimerMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match e.event_type() {
            EventType::TimerStarted => Self::TimerStarted(e.event_id),
            EventType::TimerCanceled => Self::TimerCanceled,
            EventType::TimerFired => {
                if let Some(history_event::Attributes::TimerFiredEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::TimerFired(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Timer fired attribs were unset: {}",
                        e
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Timer machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for TimerMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartTimer => Self::CommandStartTimer,
            CommandType::CancelTimer => Self::CommandCancelTimer,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> TimerMachineTransition<StartCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}

impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self) -> TimerMachineTransition<CancelTimerCommandSent> {
        TransitionResult::ok(vec![], CancelTimerCommandSent::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandSent {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

impl From<CancelTimerCommandSent> for Canceled {
    fn from(_: CancelTimerCommandSent) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Fired {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_timer_started(
        self,
        _id: HistoryEventId,
    ) -> TimerMachineTransition<StartCommandRecorded> {
        TransitionResult::default()
    }

    pub(super) fn on_cancel(self, dat: SharedState) -> TimerMachineTransition<Canceled> {
        TransitionResult::ok_shared(
            vec![],
            Canceled::default(),
            SharedState {
                cancelled_before_sent: true,
                ..dat
            },
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub(super) fn on_timer_fired(
        self,
        dat: SharedState,
        attrs: TimerFiredEventAttributes,
    ) -> TimerMachineTransition<Fired> {
        if dat.attrs.seq.to_string() == attrs.timer_id {
            TransitionResult::ok(vec![TimerMachineCommand::Complete], Fired::default())
        } else {
            TransitionResult::Err(WFMachinesError::Fatal(format!(
                "Timer fired event did not have expected timer id {}, it was {}!",
                dat.attrs.seq, attrs.timer_id
            )))
        }
    }

    pub(super) fn on_cancel(
        self,
        dat: SharedState,
    ) -> TimerMachineTransition<CancelTimerCommandCreated> {
        let cmd = Command {
            command_type: CommandType::CancelTimer as i32,
            attributes: Some(CancelTimer { seq: dat.attrs.seq }.into()),
        };
        TransitionResult::ok(
            vec![TimerMachineCommand::IssueCancelCmd(cmd)],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl WFMachinesAdapter for TimerMachine {
    fn adapt_response(
        &self,
        my_command: TimerMachineCommand,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            // Fire the completion
            TimerMachineCommand::Complete => vec![FireTimer {
                seq: self.shared_state.attrs.seq,
            }
            .into()],
            TimerMachineCommand::IssueCancelCmd(c) => vec![MachineResponse::IssueNewCommand(c)],
        })
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::TimerStarted | EventType::TimerCanceled | EventType::TimerFired
        )
    }
}

impl Cancellable for TimerMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        Ok(
            match OnEventWrapper::on_event_mut(self, TimerMachineEvents::Cancel)?.pop() {
                Some(TimerMachineCommand::IssueCancelCmd(cmd)) => {
                    vec![MachineResponse::IssueNewCommand(cmd)]
                }
                None => vec![],
                x => panic!("Invalid cancel event response {:?}", x),
            },
        )
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder, test_help::canned_histories, worker::workflow::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};
    use std::{mem::discriminant, time::Duration};
    use temporal_sdk::{CancellableFuture, WfContext, WorkflowFunction};

    #[fixture]
    fn happy_wfm() -> ManagedWFFunc {
        /*
            We have two versions of this test, one which processes the history in two calls, and one
            which replays all of it in one go. Both versions must produce the same two activations.
            However, The former will iterate the machines three times and the latter will iterate
            them twice.

            There are two workflow tasks, so it seems we should iterate two times, but the reason
            for the extra iteration in the incremental version is that we need to "wait" for the
            timer to fire. In the all-in-one-go test, the timer is created and resolved in the same
            task, hence no extra loop.
        */
        let func = WorkflowFunction::new(|command_sink: WfContext| async move {
            command_sink.timer(Duration::from_secs(5)).await;
            Ok(().into())
        });

        let t = canned_histories::single_timer("1");
        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![])
    }

    #[rstest]
    #[tokio::test]
    async fn test_fire_happy_path_inc(mut happy_wfm: ManagedWFFunc) {
        happy_wfm.get_next_activation().await.unwrap();
        let commands = happy_wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        happy_wfm.get_next_activation().await.unwrap();
        let commands = happy_wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        happy_wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_fire_happy_path_full(mut happy_wfm: ManagedWFFunc) {
        happy_wfm.process_all_activations().await.unwrap();
        let commands = happy_wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        happy_wfm.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_timer_ids_errors() {
        let func = WorkflowFunction::new(|command_sink: WfContext| async move {
            command_sink.timer(Duration::from_secs(5)).await;
            Ok(().into())
        });

        let t = canned_histories::single_timer("badid");
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);
        let act = wfm.process_all_activations().await;
        assert!(act
            .unwrap_err()
            .to_string()
            .contains("Timer fired event did not have expected timer id 1"));
        wfm.shutdown().await.unwrap();
    }

    #[fixture]
    fn cancellation_setup() -> ManagedWFFunc {
        let func = WorkflowFunction::new(|ctx: WfContext| async move {
            let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
            ctx.timer(Duration::from_secs(5)).await;
            // Cancel the first timer after having waited on the second
            cancel_timer_fut.cancel(&ctx);
            cancel_timer_fut.await;
            Ok(().into())
        });

        let t = canned_histories::cancel_timer("2", "1");
        ManagedWFFunc::new(t, func, vec![])
    }

    #[rstest]
    #[tokio::test]
    async fn incremental_cancellation(#[from(cancellation_setup)] mut wfm: ManagedWFFunc) {
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::CancelTimer as i32);
        assert_eq!(
            commands[1].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );

        assert!(wfm.get_next_activation().await.unwrap().jobs.is_empty());
        let commands = wfm.get_server_commands().commands;
        // There should be no commands - the wf completed at the same time the timer was cancelled
        assert_eq!(commands.len(), 0);
        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn full_cancellation(#[from(cancellation_setup)] mut wfm: ManagedWFFunc) {
        wfm.process_all_activations().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        // There should be no commands - the wf completed at the same time the timer was cancelled
        assert_eq!(commands.len(), 0);
        wfm.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_before_sent_to_server() {
        let func = WorkflowFunction::new(|ctx: WfContext| async move {
            let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
            // Immediately cancel the timer
            cancel_timer_fut.cancel(&ctx);
            cancel_timer_fut.await;
            Ok(().into())
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);

        wfm.process_all_activations().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
        wfm.shutdown().await.unwrap();
    }

    #[test]
    fn cancels_ignored_terminal() {
        for state in [TimerMachineState::Canceled(Canceled {}), Fired {}.into()] {
            let mut s = TimerMachine {
                state: state.clone(),
                shared_state: Default::default(),
            };
            let cmds = s.cancel().unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(&s.state));
        }
    }
}
