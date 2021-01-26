#![allow(clippy::enum_variant_names)]

use crate::{
    machines::{
        workflow_machines::{WFMachinesError, WorkflowMachines},
        WFMachinesAdapter,
    },
    protos::temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};
use rustfsm::{fsm, TransitionResult};
use std::{convert::TryFrom, time::SystemTime};
use tracing::Level;

fsm! {
    pub(super) name WorkflowTaskMachine;
    command WFTaskMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(WorkflowTaskScheduled) --> Scheduled;

    Scheduled --(WorkflowTaskStarted(WFTStartedDat), shared on_workflow_task_started) --> Started;
    Scheduled --(WorkflowTaskTimedOut) --> TimedOut;

    Started --(WorkflowTaskCompleted, on_workflow_task_completed) --> Completed;
    Started --(WorkflowTaskFailed, on_workflow_task_failed) --> Failed;
    Started --(WorkflowTaskTimedOut) --> TimedOut;
}

impl WorkflowTaskMachine {
    pub(super) fn new(wf_task_started_event_id: i64) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                wf_task_started_event_id,
            },
        }
    }
}

#[derive(Debug)]
pub(super) enum WFTaskMachineCommand {
    /// Issued to (possibly) trigger the event loop
    WFTaskStartedTrigger {
        task_started_event_id: i64,
        time: SystemTime,
    },
}

impl WFMachinesAdapter for WorkflowTaskMachine {
    fn adapt_response(
        &self,
        wf_machines: &mut WorkflowMachines,
        event: &HistoryEvent,
        has_next_event: bool,
        my_command: WFTaskMachineCommand,
    ) -> Result<(), WFMachinesError> {
        match my_command {
            WFTaskMachineCommand::WFTaskStartedTrigger {
                task_started_event_id,
                time,
            } => {
                let event_type = EventType::from_i32(event.event_type)
                    .ok_or_else(|| WFMachinesError::UnexpectedEvent(event.clone()))?;
                let cur_event_past_or_at_start = event.event_id >= task_started_event_id;
                if event_type == EventType::WorkflowTaskStarted
                    && (!cur_event_past_or_at_start || has_next_event)
                {
                    // Last event in history is a task started event, so we don't
                    // want to iterate.
                    return Ok(());
                }
                wf_machines.task_started(task_started_event_id, time)?;
            }
        }
        Ok(())
    }
}

impl TryFrom<HistoryEvent> for WorkflowTaskMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::WorkflowTaskScheduled) => Self::WorkflowTaskScheduled,
            Some(EventType::WorkflowTaskStarted) => Self::WorkflowTaskStarted(WFTStartedDat {
                started_event_id: e.event_id,
                current_time_millis: e.event_time.clone().map(|ts| ts.into()).ok_or_else(|| {
                    WFMachinesError::MalformedEvent(
                        e,
                        "Workflow task started event must contain timestamp".to_string(),
                    )
                })?,
            }),
            Some(EventType::WorkflowTaskTimedOut) => Self::WorkflowTaskTimedOut,
            Some(EventType::WorkflowTaskCompleted) => Self::WorkflowTaskCompleted,
            Some(EventType::WorkflowTaskFailed) => Self::WorkflowTaskFailed,
            _ => return Err(WFMachinesError::UnexpectedEvent(e)),
        })
    }
}

impl TryFrom<CommandType> for WorkflowTaskMachineEvents {
    type Error = ();

    fn try_from(_: CommandType) -> Result<Self, Self::Error> {
        Err(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct SharedState {
    wf_task_started_event_id: i64,
}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct Scheduled {}

pub(super) struct WFTStartedDat {
    current_time_millis: SystemTime,
    started_event_id: i64,
}
impl Scheduled {
    pub(super) fn on_workflow_task_started(
        self,
        shared: SharedState,
        WFTStartedDat {
            current_time_millis,
            started_event_id,
        }: WFTStartedDat,
    ) -> WorkflowTaskMachineTransition {
        WorkflowTaskMachineTransition::ok(
            vec![WFTaskMachineCommand::WFTaskStartedTrigger {
                task_started_event_id: shared.wf_task_started_event_id,
                time: current_time_millis,
            }],
            Started {
                current_time_millis,
                started_event_id,
            },
        )
    }
}

impl From<Created> for Scheduled {
    fn from(_: Created) -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub(super) struct Started {
    /// Started event's timestamp
    current_time_millis: SystemTime,
    /// Started event's id
    started_event_id: i64,
}

impl Started {
    pub(super) fn on_workflow_task_completed(self) -> WorkflowTaskMachineTransition {
        WorkflowTaskMachineTransition::commands::<_, Completed>(vec![
            WFTaskMachineCommand::WFTaskStartedTrigger {
                task_started_event_id: self.started_event_id,
                time: self.current_time_millis,
            },
        ])
    }
    pub(super) fn on_workflow_task_failed(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}
impl From<Scheduled> for TimedOut {
    fn from(_: Scheduled) -> Self {
        Self::default()
    }
}
impl From<Started> for TimedOut {
    fn from(_: Started) -> Self {
        Self::default()
    }
}
