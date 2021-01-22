#![allow(clippy::large_enum_variant)]

use crate::{
    machines::{workflow_machines::WFMachinesError, AddCommand, TSMCommand},
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
use std::convert::TryFrom;

fsm! {
    pub(super) name TimerMachine;
    command TSMCommand;
    error WFMachinesError;
    shared_state SharedState;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated
        --(CommandCancelTimer, shared on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;

    Created --(Schedule, shared on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired(HistoryEvent), on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, shared on_cancel) --> CancelTimerCommandCreated;
}

impl TimerMachine {
    pub(crate) fn new(attribs: StartTimerCommandAttributes) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                timer_attributes: attribs,
            },
        }
    }

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
pub(super) struct Fired {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_timer_started(self, id: HistoryEventId) -> TimerMachineTransition {
        // Java recorded an initial event ID, but it seemingly was never used.
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
                    ..Default::default()
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
    use crate::machines::WFCommand;
    use crate::{
        machines::{
            test_help::TestHistoryBuilder, workflow_machines::WorkflowMachines, DrivenWorkflow,
        },
        protos::temporal::api::history::v1::{
            TimerFiredEventAttributes, WorkflowExecutionCanceledEventAttributes,
            WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
        },
    };
    use std::sync::mpsc::channel;
    use std::{error::Error, sync::mpsc::Receiver, time::Duration};

    // TODO: This will need to be broken out into it's own place and evolved / made more generic as
    //   we learn more. It replaces "TestEnitityTestListenerBase" in java which is pretty hard to
    //   follow.
    struct TestWorkflowDriver {
        /// A queue of things to return upon calls to [DrivenWorkflow::iterate_wf]. This gives us
        /// more manual control than actually running the workflow for real would, for example
        /// allowing us to simulate nondeterminism.
        iteration_results: Receiver<WFCommand>,
    }

    impl TestWorkflowDriver {
        pub fn new<I>(iteration_results: I) -> Self
        where
            I: IntoIterator<Item = WFCommand>,
        {
            let (sender, receiver) = channel();
            for r in iteration_results.into_iter() {
                sender.send(r);
            }
            Self {
                iteration_results: receiver,
            }
        }
    }

    impl DrivenWorkflow for TestWorkflowDriver {
        fn start(
            &self,
            attribs: WorkflowExecutionStartedEventAttributes,
        ) -> Result<Vec<WFCommand>, anyhow::Error> {
            self.iterate_wf()
        }

        fn iterate_wf(&self) -> Result<Vec<WFCommand>, anyhow::Error> {
            // Timeout exists just to make blocking obvious. We should never block.
            let cmd = self
                .iteration_results
                .recv_timeout(Duration::from_millis(10))?;
            Ok(vec![cmd])
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

    #[test]
    fn test_fire_happy_path() {
        env_logger::init();
        let twd = TestWorkflowDriver::new(vec![
            WorkflowMachines::new_timer(StartTimerCommandAttributes {
                timer_id: "Sometimer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(5).into()),
                ..Default::default()
            }),
            // Complete wf task
        ]);

        /*
            1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
            2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            3: EVENT_TYPE_WORKFLOW_TASK_STARTED
            4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
            5: EVENT_TYPE_TIMER_STARTED
            6: EVENT_TYPE_TIMER_FIRED
            7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            8: EVENT_TYPE_WORKFLOW_TASK_STARTED
        */
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
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        dbg!(&commands);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
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
