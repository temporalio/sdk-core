#![allow(clippy::large_enum_variant)]

use crate::{
    machines::{
        workflow_machines::WFMachinesError, workflow_machines::WorkflowMachines, AddCommand,
        CancellableCommand, WFCommand, WFMachinesAdapter,
    },
    protos::{
        coresdk::HistoryEventId,
        temporal::api::{
            command::v1::{
                command::Attributes, CancelTimerCommandAttributes, Command,
                StartTimerCommandAttributes,
            },
            enums::v1::{CommandType, EventType},
            history::v1::{history_event, HistoryEvent, TimerCanceledEventAttributes},
        },
    },
};
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::{cell::RefCell, convert::TryFrom, rc::Rc, sync::atomic::Ordering};
use tracing::Level;

fsm! {
    pub(super) name TimerMachine;
    command TimerMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, shared on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired(HistoryEvent), on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, shared on_cancel) --> CancelTimerCommandCreated;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated
        --(CommandCancelTimer, shared on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;
}

#[derive(Debug)]
pub(super) enum TimerMachineCommand {
    AddCommand(AddCommand),
    Complete(HistoryEvent),
}

/// Creates a new, scheduled, timer as a [CancellableCommand]
pub(super) fn new_timer(attribs: StartTimerCommandAttributes) -> CancellableCommand {
    let (timer, add_cmd) = TimerMachine::new_scheduled(attribs);
    CancellableCommand::Active {
        command: add_cmd.command,
        machine: Rc::new(RefCell::new(timer)),
    }
}

impl TimerMachine {
    /// Create a new timer and immediately schedule it
    pub(crate) fn new_scheduled(attribs: StartTimerCommandAttributes) -> (Self, AddCommand) {
        let mut s = Self::new(attribs);
        let cmd = match s
            .on_event_mut(TimerMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail")
            .pop()
        {
            Some(TimerMachineCommand::AddCommand(c)) => c,
            _ => panic!("Timer on_schedule must produce command"),
        };
        (s, cmd)
    }

    fn new(attribs: StartTimerCommandAttributes) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                timer_attributes: attribs,
            },
        }
    }
}

impl TryFrom<HistoryEvent> for TimerMachineEvents {
    type Error = ();

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::TimerStarted) => Self::TimerStarted(e.event_id),
            Some(EventType::TimerCanceled) => Self::TimerCanceled,
            Some(EventType::TimerFired) => Self::TimerFired(e),
            _ => return Err(()),
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
pub(super) struct SharedState {
    timer_attributes: StartTimerCommandAttributes,
}

impl SharedState {
    fn into_timer_canceled_event_command(self) -> TimerMachineCommand {
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
        TimerMachineCommand::Complete(event)
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self, dat: SharedState) -> TimerMachineTransition {
        let cmd = Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(dat.timer_attributes.into()),
        };
        TimerMachineTransition::commands::<_, StartCommandCreated>(vec![
            TimerMachineCommand::AddCommand(cmd.into()),
        ])
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self, dat: SharedState) -> TimerMachineTransition {
        // TODO: Think we need to produce a completion command here
        TimerMachineTransition::ok(
            vec![dat.into_timer_canceled_event_command()],
            Canceled::default(),
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
    pub(super) fn on_cancel(mut self, dat: SharedState) -> TimerMachineTransition {
        // Cancel the initial command - which just sets a "canceled" flag in a wrapper of a
        // proto command. TODO: Does this make any sense? - no - propagate up
        TimerMachineTransition::ok(
            vec![dat.into_timer_canceled_event_command()],
            Canceled::default(),
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub(super) fn on_timer_fired(self, event: HistoryEvent) -> TimerMachineTransition {
        TimerMachineTransition::ok(vec![TimerMachineCommand::Complete(event)], Fired::default())
    }
    pub(super) fn on_cancel(self, dat: SharedState) -> TimerMachineTransition {
        let cmd = Command {
            command_type: CommandType::CancelTimer as i32,
            attributes: Some(
                CancelTimerCommandAttributes {
                    timer_id: dat.timer_attributes.timer_id,
                }
                .into(),
            ),
        };
        TimerMachineTransition::ok(
            vec![TimerMachineCommand::AddCommand(cmd.into())],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl WFMachinesAdapter for TimerMachine {
    fn adapt_response(
        &self,
        wf_machines: &mut WorkflowMachines,
        _event: &HistoryEvent,
        _has_next_event: bool,
        my_command: TimerMachineCommand,
    ) -> Result<(), WFMachinesError> {
        match my_command {
            TimerMachineCommand::AddCommand(_) => {
                unreachable!()
            }
            // Fire the completion
            TimerMachineCommand::Complete(_event) => {
                if let Some(a) = wf_machines
                    .timer_notifiers
                    .remove(&self.shared_state.timer_attributes.timer_id)
                {
                    a.store(true, Ordering::SeqCst)
                };
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::machines::test_help::CommandSender;
    use crate::{
        machines::{
            complete_workflow_state_machine::complete_workflow,
            test_help::{TestHistoryBuilder, TestWFCommand, TestWorkflowDriver},
            workflow_machines::WorkflowMachines,
            DrivenWorkflow, WFCommand,
        },
        protos::temporal::api::{
            command::v1::CompleteWorkflowExecutionCommandAttributes,
            history::v1::{
                TimerFiredEventAttributes, WorkflowExecutionCanceledEventAttributes,
                WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
            },
        },
    };
    use futures::channel::mpsc::Sender;
    use futures::{FutureExt, SinkExt};
    use rstest::{fixture, rstest};
    use std::{error::Error, time::Duration};
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[fixture]
    fn fire_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        /*
            1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
            2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            3: EVENT_TYPE_WORKFLOW_TASK_STARTED
            4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
            5: EVENT_TYPE_TIMER_STARTED
            6: EVENT_TYPE_TIMER_FIRED
            7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            8: EVENT_TYPE_WORKFLOW_TASK_STARTED

            We have two versions of this test, one which processes the history in two calls,
            and one which replays all of it in one go. The former will run the event loop three
            times total, and the latter two.

            There are two workflow tasks, so it seems we should only loop two times, but the reason
            for the extra iteration in the incremental version is that we need to "wait" for the
            timer to fire. In the all-in-one-go test, the timer is created and resolved in the same
            task, hence no extra loop.
        */
        let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let timer = StartTimerCommandAttributes {
                timer_id: "Sometimer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(5).into()),
                ..Default::default()
            };
            command_sink.timer(timer).await;

            let complete = CompleteWorkflowExecutionCommandAttributes::default();
            command_sink.send(complete.into());
        });

        let mut t = TestHistoryBuilder::default();
        let mut state_machines = WorkflowMachines::new(Box::new(twd));

        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "timer1".to_string(),
                ..Default::default()
            }),
        );
        t.add_workflow_task_scheduled_and_started();
        assert_eq!(2, t.get_workflow_task_count(None).unwrap());
        assert_eq!(3, t.get_history_info(1).unwrap().events.len());
        assert_eq!(8, t.get_history_info(2).unwrap().events.len());
        (t, state_machines)
    }

    #[rstest]
    fn test_fire_happy_path_inc(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "inc");
        let _enter = s.enter();

        let (t, mut state_machines) = fire_happy_hist;

        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(2))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[rstest]
    fn test_fire_happy_path_full(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_service_name("report_example")
            .install()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(opentelemetry)
            .try_init()
            .unwrap();

        let s = span!(Level::DEBUG, "Test start", t = "full");
        let _enter = s.enter();

        let (t, mut state_machines) = fire_happy_hist;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(2))
            .unwrap();
        dbg!(&commands);
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }
}
