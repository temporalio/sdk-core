use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name WorkflowTaskMachine; command WorkflowTaskCommand; error WorkflowTaskMachineError;

    Created --(WorkflowTaskScheduled, on_workflow_task_scheduled) --> Scheduled;

    Scheduled --(WorkflowTaskStarted, on_workflow_task_started) --> Started;
    Scheduled --(WorkflowTaskTimedOut, on_workflow_task_timed_out) --> TimedOut;

    Started --(WorkflowTaskCompleted, on_workflow_task_completed) --> Completed;
    Started --(WorkflowTaskFailed, on_workflow_task_failed) --> Failed;
    Started --(WorkflowTaskTimedOut, on_workflow_task_timed_out) --> TimedOut;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum WorkflowTaskMachineError {}

pub(super) enum WorkflowTaskCommand {}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_workflow_task_scheduled(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct Scheduled {}

impl Scheduled {
    pub(super) fn on_workflow_task_started(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
    pub(super) fn on_workflow_task_timed_out(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_workflow_task_completed(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
    pub(super) fn on_workflow_task_failed(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
    pub(super) fn on_workflow_task_timed_out(self) -> WorkflowTaskMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}
