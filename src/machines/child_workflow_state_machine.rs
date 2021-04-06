use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name ChildWorkflowMachine; command ChildWorkflowCommand; error ChildWorkflowMachineError;

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
pub(super) enum ChildWorkflowMachineError {}

pub(super) enum ChildWorkflowCommand {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> ChildWorkflowMachineTransition<StartCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_start_child_workflow_execution_initiated(
        self,
    ) -> ChildWorkflowMachineTransition<StartEventRecorded> {
        unimplemented!()
    }
    pub(super) fn on_cancel(self) -> ChildWorkflowMachineTransition<Canceled> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartEventRecorded {}

impl StartEventRecorded {
    pub(super) fn on_child_workflow_execution_started(
        self,
    ) -> ChildWorkflowMachineTransition<Started> {
        unimplemented!()
    }
    pub(super) fn on_start_child_workflow_execution_failed(
        self,
    ) -> ChildWorkflowMachineTransition<StartFailed> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartFailed {}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_child_workflow_execution_completed(
        self,
    ) -> ChildWorkflowMachineTransition<Completed> {
        unimplemented!()
    }
    pub(super) fn on_child_workflow_execution_failed(
        self,
    ) -> ChildWorkflowMachineTransition<Failed> {
        unimplemented!()
    }
    pub(super) fn on_child_workflow_execution_timed_out(
        self,
    ) -> ChildWorkflowMachineTransition<TimedOut> {
        unimplemented!()
    }
    pub(super) fn on_child_workflow_execution_canceled(
        self,
    ) -> ChildWorkflowMachineTransition<Canceled> {
        unimplemented!()
    }
    pub(super) fn on_child_workflow_execution_terminated(
        self,
    ) -> ChildWorkflowMachineTransition<Terminated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Terminated {}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}
