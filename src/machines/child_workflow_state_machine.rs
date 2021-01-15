use rustfsm::{fsm, TransitionResult};

fsm! {
    name ChildWorkflowMachine; command ChildWorkflowCommand; error ChildWorkflowMachineError;

    Created --(Schedule, on_schedule) --> StartCommandCreated;

    Started --(ChildWorkflowExecutionCompleted, on_child_workflow_execution_completed) --> Completed;
    Started --(ChildWorkflowExecutionFailed, on_child_workflow_execution_failed) --> Failed;
    Started --(ChildWorkflowExecutionTimedOut, on_child_workflow_execution_timed_out) --> TimedOut;
    Started --(ChildWorkflowExecutionCanceled, on_child_workflow_execution_canceled) --> Canceled;
    Started --(ChildWorkflowExecutionTerminated, on_child_workflow_execution_terminated) --> Terminated;

    StartCommandCreated --(CommandStartChildWorkflowExecution) --> StartCommandCreated;
    StartCommandCreated --(StartChildWorkflowExecutionInitiated, on_start_child_workflow_execution_initiated) --> StartEventRecorded;
    StartCommandCreated --(Cancel, on_cancel) --> Canceled;

    StartEventRecorded --(ChildWorkflowExecutionStarted, on_child_workflow_execution_started) --> Started;
    StartEventRecorded --(StartChildWorkflowExecutionFailed, on_start_child_workflow_execution_failed) --> StartFailed;
}

#[derive(thiserror::Error, Debug)]
pub enum ChildWorkflowMachineError {}

pub enum ChildWorkflowCommand {}

#[derive(Default, Clone)]
pub struct Canceled {}

#[derive(Default, Clone)]
pub struct Completed {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct Failed {}

#[derive(Default, Clone)]
pub struct StartCommandCreated {}

impl StartCommandCreated {
    pub fn on_start_child_workflow_execution_initiated(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_cancel(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct StartEventRecorded {}

impl StartEventRecorded {
    pub fn on_child_workflow_execution_started(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_start_child_workflow_execution_failed(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct StartFailed {}

#[derive(Default, Clone)]
pub struct Started {}

impl Started {
    pub fn on_child_workflow_execution_completed(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_child_workflow_execution_failed(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_child_workflow_execution_timed_out(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_child_workflow_execution_canceled(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
    pub fn on_child_workflow_execution_terminated(self) -> ChildWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct Terminated {}

#[derive(Default, Clone)]
pub struct TimedOut {}
