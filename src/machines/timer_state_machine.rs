#![allow(clippy::large_enum_variant)]

use crate::{
    machines::{
        workflow_machines::WFMachinesError, AddCommand, CancellableCommand, TSMCommand, WFCommand,
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
use std::{cell::RefCell, convert::TryFrom, rc::Rc};
use tracing::Level;

fsm! {
    pub(super) name TimerMachine;
    command TSMCommand;
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
            Some(TSMCommand::AddCommand(c)) => c,
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
    fn into_timer_canceled_event_command(self) -> TSMCommand {
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
        TSMCommand::ProduceHistoryEvent(event)
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
        TimerMachineTransition::commands::<_, StartCommandCreated>(vec![TSMCommand::AddCommand(
            cmd.into(),
        )])
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self, dat: SharedState) -> TimerMachineTransition {
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
        TimerMachineTransition::ok(
            vec![TSMCommand::ProduceHistoryEvent(event)],
            Fired::default(),
        )
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
            vec![TSMCommand::AddCommand(cmd.into())],
            CancelTimerCommandCreated::default(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::{
            complete_workflow_state_machine::complete_workflow, test_help::TestHistoryBuilder,
            workflow_machines::WorkflowMachines, DrivenWorkflow, WFCommand,
        },
        protos::temporal::api::{
            command::v1::CompleteWorkflowExecutionCommandAttributes,
            history::v1::{
                TimerFiredEventAttributes, WorkflowExecutionCanceledEventAttributes,
                WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
            },
        },
    };
    use rstest::{fixture, rstest};
    use std::sync::mpsc::Sender;
    use std::{error::Error, sync::mpsc::channel, sync::mpsc::Receiver, time::Duration};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // TODO: This will need to be broken out into it's own place and evolved / made more generic as
    //   we learn more. It replaces "TestEnitityTestListenerBase" in java which is pretty hard to
    //   follow.
    #[derive(Debug)]
    struct TestWorkflowDriver {
        /// A queue of command lists to return upon calls to [DrivenWorkflow::iterate_wf]. This
        /// gives us more manual control than actually running the workflow for real would, for
        /// example allowing us to simulate nondeterminism.
        iteration_results: Receiver<Vec<WFCommand>>,
        iteration_sender: Sender<Vec<WFCommand>>,

        /// The entire history passed in when we were constructed
        full_history: Vec<Vec<WFCommand>>,
    }

    impl TestWorkflowDriver {
        pub fn new<I>(iteration_results: I) -> Self
        where
            I: IntoIterator<Item = Vec<WFCommand>>,
        {
            let (sender, receiver) = channel();
            Self {
                iteration_results: receiver,
                iteration_sender: sender,
                full_history: iteration_results.into_iter().collect(),
            }
        }
    }

    impl DrivenWorkflow for TestWorkflowDriver {
        #[instrument]
        fn start(
            &self,
            attribs: WorkflowExecutionStartedEventAttributes,
        ) -> Result<Vec<WFCommand>, anyhow::Error> {
            self.full_history
                .iter()
                .for_each(|cmds| self.iteration_sender.send(cmds.to_vec()).unwrap());
            Ok(vec![])
        }

        #[instrument]
        fn iterate_wf(&self) -> Result<Vec<WFCommand>, anyhow::Error> {
            // Timeout exists just to make blocking obvious. We should never block.
            let cmd = self
                .iteration_results
                .recv_timeout(Duration::from_millis(10))?;
            event!(Level::DEBUG, msg = "Test wf driver emitting", ?cmd);
            Ok(cmd)
        }

        fn signal(
            &self,
            attribs: WorkflowExecutionSignaledEventAttributes,
        ) -> Result<(), anyhow::Error> {
            Ok(())
        }

        fn cancel(
            &self,
            attribs: WorkflowExecutionCanceledEventAttributes,
        ) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

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

            Should iterate once, produce started command, iterate again, producing no commands
            (timer complete), third iteration completes workflow.
        */
        let twd = TestWorkflowDriver::new(vec![
            vec![new_timer(StartTimerCommandAttributes {
                timer_id: "Sometimer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(5).into()),
                ..Default::default()
            })
            .into()],
            // TODO: Needs to be here in incremental one, but not full :/
            // vec![], // timer complete, no new commands
            vec![complete_workflow(
                CompleteWorkflowExecutionCommandAttributes::default(),
            )],
        ]);

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
        (t, state_machines)
    }

    #[rstest]
    fn test_fire_happy_path_inc(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_service_name("report_example")
            .install()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(opentelemetry)
            .try_init()
            .unwrap();

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
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[test]
    fn test_timer_cancel() {
        // TODO: Incomplete

        let mut timer_cmd = new_timer(StartTimerCommandAttributes {
            timer_id: "Sometimer".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(5).into()),
            ..Default::default()
        });
        let timer_machine = timer_cmd.unwrap_machine();
        let twd = TestWorkflowDriver::new(vec![
            vec![timer_cmd.into()],
            vec![complete_workflow(
                CompleteWorkflowExecutionCommandAttributes::default(),
            )],
        ]);

        let mut t = TestHistoryBuilder::default();
        let mut state_machines = WorkflowMachines::new(Box::new(twd));

        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add_workflow_task_scheduled_and_started();
        assert_eq!(2, t.get_workflow_task_count(None).unwrap());
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
    }
}
