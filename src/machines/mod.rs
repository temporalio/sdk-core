use crate::protos::temporal::api::command::v1::Command;

#[allow(unused)]
mod activity_state_machine;
#[allow(unused)]
mod cancel_external_state_machine;
#[allow(unused)]
mod cancel_workflow_state_machine;
#[allow(unused)]
mod child_workflow_state_machine;
#[allow(unused)]
mod complete_workflow_state_machine;
#[allow(unused)]
mod continue_as_new_workflow_state_machine;
#[allow(unused)]
mod fail_workflow_state_machine;
#[allow(unused)]
mod local_activity_state_machine;
#[allow(unused)]
mod mutable_side_effect_state_machine;
#[allow(unused)]
mod side_effect_state_machine;
#[allow(unused)]
mod signal_external_state_machine;
#[allow(unused)]
mod timer_state_machine;
#[allow(unused)]
mod upsert_search_attributes_state_machine;
#[allow(unused)]
mod version_state_machine;
#[allow(unused)]
mod workflow_task_state_machine;

#[cfg(test)]
mod test_help;

/// A command which can be cancelled
#[derive(Debug, Clone)]
pub struct CancellableCommand {
    /// The inner protobuf command, if None, command has been cancelled
    command: Option<Command>,
}

impl CancellableCommand {
    pub(crate) fn cancel(&mut self) {
        self.command = None;
    }
}

impl From<Command> for CancellableCommand {
    fn from(command: Command) -> Self {
        Self {
            command: Some(command),
        }
    }
}
