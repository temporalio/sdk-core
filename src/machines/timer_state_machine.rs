#![allow(clippy::large_enum_variant)]

use crate::{
    machines::{
        workflow_machines::{MachineResponse, WFMachinesError},
        Cancellable, NewMachineWithCommand, WFMachinesAdapter,
    },
    protos::{
        coresdk::{HistoryEventId, TimerCanceledTaskAttributes, TimerFiredTaskAttributes},
        temporal::api::{
            command::v1::{CancelTimerCommandAttributes, Command, StartTimerCommandAttributes},
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent, TimerFiredEventAttributes},
        },
    },
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::TryFrom;

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
}

#[derive(Debug)]
pub(super) enum TimerMachineCommand {
    Complete,
    Canceled,
    IssueCancelCmd(Command),
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: StartTimerCommandAttributes,
    cancelled_before_sent: bool,
}

/// Creates a new, scheduled, timer as a [CancellableCommand]
pub(super) fn new_timer(
    attribs: StartTimerCommandAttributes,
) -> NewMachineWithCommand<TimerMachine> {
    let (timer, add_cmd) = TimerMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: timer,
    }
}

impl TimerMachine {
    /// Create a new timer and immediately schedule it
    pub(crate) fn new_scheduled(attribs: StartTimerCommandAttributes) -> (Self, Command) {
        let mut s = Self::new(attribs);
        s.on_event_mut(TimerMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail");
        let cmd = Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(s.shared_state().attrs.clone().into()),
        };
        (s, cmd)
    }

    fn new(attribs: StartTimerCommandAttributes) -> Self {
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
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::TimerStarted) => Self::TimerStarted(e.event_id),
            Some(EventType::TimerCanceled) => Self::TimerCanceled,
            Some(EventType::TimerFired) => {
                if let Some(history_event::Attributes::TimerFiredEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::TimerFired(attrs)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        e,
                        "Timer fired attribs were unset".to_string(),
                    ));
                }
            }
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    e,
                    "Timer machine does not handle this event",
                ))
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
    pub(super) fn on_schedule(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<StartCommandCreated>()
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}

impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self) -> TimerMachineTransition {
        TimerMachineTransition::ok(
            vec![TimerMachineCommand::Canceled],
            CancelTimerCommandSent::default(),
        )
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
    pub(super) fn on_timer_started(self, _id: HistoryEventId) -> TimerMachineTransition {
        // TODO: Java recorded an initial event ID, but it seemingly was never used.
        TimerMachineTransition::default::<StartCommandRecorded>()
    }
    pub(super) fn on_cancel(self, dat: SharedState) -> TimerMachineTransition {
        TimerMachineTransition::ok_shared(
            vec![TimerMachineCommand::Canceled],
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
    ) -> TimerMachineTransition {
        if dat.attrs.timer_id != attrs.timer_id {
            TimerMachineTransition::Err(WFMachinesError::MalformedEventDetail(format!(
                "Timer fired event did not have expected timer id {}!",
                dat.attrs.timer_id
            )))
        } else {
            TimerMachineTransition::ok(vec![TimerMachineCommand::Complete], Fired::default())
        }
    }

    pub(super) fn on_cancel(self, dat: SharedState) -> TimerMachineTransition {
        let cmd = Command {
            command_type: CommandType::CancelTimer as i32,
            attributes: Some(
                CancelTimerCommandAttributes {
                    timer_id: dat.attrs.timer_id,
                }
                .into(),
            ),
        };
        TimerMachineTransition::ok(
            vec![TimerMachineCommand::IssueCancelCmd(cmd)],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl WFMachinesAdapter for TimerMachine {
    fn adapt_response(
        &self,
        _event: &HistoryEvent,
        _has_next_event: bool,
        my_command: TimerMachineCommand,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            // Fire the completion
            TimerMachineCommand::Complete => vec![TimerFiredTaskAttributes {
                timer_id: self.shared_state.attrs.timer_id.clone(),
            }
            .into()],
            TimerMachineCommand::Canceled => vec![TimerCanceledTaskAttributes {
                timer_id: self.shared_state.attrs.timer_id.clone(),
            }
            .into()],
            TimerMachineCommand::IssueCancelCmd(c) => vec![MachineResponse::IssueNewCommand(c)],
        })
    }
}

impl Cancellable for TimerMachine {
    fn cancel(&mut self) -> Result<MachineResponse, MachineError<Self::Error>> {
        Ok(match self.on_event_mut(TimerMachineEvents::Cancel)?.pop() {
            Some(TimerMachineCommand::IssueCancelCmd(cmd)) => MachineResponse::IssueNewCommand(cmd),
            Some(TimerMachineCommand::Canceled) => MachineResponse::NoOp,
            x => panic!(format!("Invalid cancel event response {:?}", x)),
        })
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::{
            test_help::{CommandSender, TestHistoryBuilder, TestWorkflowDriver},
            workflow_machines::WorkflowMachines,
        },
        protos::temporal::api::command::v1::CompleteWorkflowExecutionCommandAttributes,
        test_help::canned_histories,
    };
    use rstest::{fixture, rstest};
    use std::time::Duration;
    use tracing::Level;

    #[fixture]
    fn fire_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        crate::core_tracing::tracing_init();
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
        let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let timer = StartTimerCommandAttributes {
                timer_id: "timer1".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(5).into()),
            };
            command_sink.timer(timer, true);

            let complete = CompleteWorkflowExecutionCommandAttributes::default();
            command_sink.send(complete.into());
        });

        let t = canned_histories::single_timer("timer1");
        let state_machines =
            WorkflowMachines::new("wfid".to_string(), "runid".to_string(), Box::new(twd));

        assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
        (t, state_machines)
    }

    #[rstest]
    fn test_fire_happy_path_inc(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "happy_inc");
        let _enter = s.enter();

        let (t, mut state_machines) = fire_happy_hist;

        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        state_machines.get_wf_activation();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(2))
            .unwrap();
        state_machines.get_wf_activation();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[rstest]
    fn test_fire_happy_path_full(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "happy_full");
        let _enter = s.enter();

        let (t, mut state_machines) = fire_happy_hist;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[test]
    fn mismatched_timer_ids_errors() {
        let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let timer = StartTimerCommandAttributes {
                timer_id: "realid".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(5).into()),
            };
            command_sink.timer(timer, true);
        });

        let t = canned_histories::single_timer("badid");
        let mut state_machines =
            WorkflowMachines::new("wfid".to_string(), "runid".to_string(), Box::new(twd));

        assert!(t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap_err()
            .to_string()
            .contains("Timer fired event did not have expected timer id realid!"))
    }

    #[fixture]
    fn cancellation_setup() -> (TestHistoryBuilder, WorkflowMachines) {
        crate::core_tracing::tracing_init();

        let twd = TestWorkflowDriver::new(|mut cmd_sink: CommandSender| async move {
            let _cancel_this = cmd_sink.timer(
                StartTimerCommandAttributes {
                    timer_id: "cancel_timer".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(500).into()),
                },
                false,
            );
            cmd_sink.timer(
                StartTimerCommandAttributes {
                    timer_id: "wait_timer".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(5).into()),
                },
                true,
            );
            // Cancel the first timer after having waited on the second
            cmd_sink.cancel_timer("cancel_timer");

            let complete = CompleteWorkflowExecutionCommandAttributes::default();
            cmd_sink.send(complete.into());
        });

        let t = canned_histories::cancel_timer("wait_timer", "cancel_timer");
        let state_machines =
            WorkflowMachines::new("wfid".to_string(), "runid".to_string(), Box::new(twd));
        (t, state_machines)
    }

    #[rstest]
    fn incremental_cancellation(cancellation_setup: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "cancel_inc");
        let _enter = s.enter();

        let (t, mut state_machines) = cancellation_setup;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(2))
            .unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::CancelTimer as i32);
        assert_eq!(
            commands[1].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        // There should be no commands - the wf completed at the same time the timer was cancelled
        assert_eq!(commands.len(), 0);
    }

    #[rstest]
    fn full_cancellation(cancellation_setup: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "cancel_full");
        let _enter = s.enter();

        let (t, mut state_machines) = cancellation_setup;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        // There should be no commands - the wf completed at the same time the timer was cancelled
        assert_eq!(commands.len(), 0);
    }

    #[test]
    fn cancel_before_sent_to_server() {
        crate::core_tracing::tracing_init();
        let twd = TestWorkflowDriver::new(|mut cmd_sink: CommandSender| async move {
            cmd_sink.timer(
                StartTimerCommandAttributes {
                    timer_id: "cancel_timer".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(500).into()),
                },
                false,
            );
            // Immediately cancel the timer
            cmd_sink.cancel_timer("cancel_timer");

            let complete = CompleteWorkflowExecutionCommandAttributes::default();
            cmd_sink.send(complete.into());
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut state_machines =
            WorkflowMachines::new("wfid".to_string(), "runid".to_string(), Box::new(twd));

        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        assert_eq!(commands.len(), 0);
    }
}
