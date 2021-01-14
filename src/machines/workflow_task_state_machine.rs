use rustfsm::{fsm, TransitionResult};

fsm! {
	WorkflowTaskMachine, WorkflowTaskCommand, WorkflowTaskMachineError

	Created --(WorkflowTaskScheduled, on_workflow_task_scheduled) --> Scheduled;

	Scheduled --(WorkflowTaskStarted, on_workflow_task_started) --> Started;
	Scheduled --(WorkflowTaskTimedOut, on_workflow_task_timed_out) --> TimedOut;

	Started --(WorkflowTaskCompleted, on_workflow_task_completed) --> Completed;
	Started --(WorkflowTaskFailed, on_workflow_task_failed) --> Failed;
	Started --(WorkflowTaskTimedOut, on_workflow_task_timed_out) --> TimedOut;
}

#[derive(thiserror::Error, Debug)]
pub enum WorkflowTaskMachineError {}
pub enum WorkflowTaskCommand {}

#[derive(Default)]
pub struct Completed {}

#[derive(Default)]
pub struct Created {}
impl Created {
	pub fn on_workflow_task_scheduled(self) -> WorkflowTaskMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct Failed {}

#[derive(Default)]
pub struct Scheduled {}
impl Scheduled {
	pub fn on_workflow_task_started(self) -> WorkflowTaskMachineTransition { unimplemented!() }
	pub fn on_workflow_task_timed_out(self) -> WorkflowTaskMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct Started {}
impl Started {
	pub fn on_workflow_task_completed(self) -> WorkflowTaskMachineTransition { unimplemented!() }
	pub fn on_workflow_task_failed(self) -> WorkflowTaskMachineTransition { unimplemented!() }
	pub fn on_workflow_task_timed_out(self) -> WorkflowTaskMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct TimedOut {}

