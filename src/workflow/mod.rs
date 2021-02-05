mod bridge;

pub(crate) use bridge::WorkflowBridge;

use crate::{
    machines::{ProtoCommand, WFCommand, WorkflowMachines},
    protos::temporal::api::{
        common::v1::WorkflowExecution,
        workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    CoreError, Result,
};
use std::{
    ops::DerefMut,
    sync::{mpsc::Sender, Arc, Mutex},
};

/// Implementors can provide new workflow tasks to the SDK. The connection to the server is the real
/// implementor.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait PollWorkflowTaskQueueApi {
    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
}

/// Implementors can complete tasks as would've been issued by [Core::poll]. The real implementor
/// sends the completed tasks to the server.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait RespondWorkflowTaskCompletedApi {
    /// Complete a task by sending it to the server. `task_token` is the task token that would've
    /// been received from [PollWorkflowTaskQueueApi::poll]. `commands` is a list of new commands
    /// to send to the server, such as starting a timer.
    async fn complete(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
}

/// Manages concurrent access to an instance of a [WorkflowMachines], which is not thread-safe.
pub(crate) struct WorkflowManager {
    data: Arc<Mutex<WfManagerProtected>>,
}
/// Inner data for [WorkflowManager]
pub(crate) struct WfManagerProtected {
    pub machines: WorkflowMachines,
    pub command_sink: Sender<Vec<WFCommand>>,
}

impl WorkflowManager {
    pub fn new(we: &WorkflowExecution) -> Self {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines =
            WorkflowMachines::new(we.workflow_id.clone(), we.run_id.clone(), Box::new(wfb));
        let protected = WfManagerProtected {
            machines: state_machines,
            command_sink: cmd_sink,
        };
        Self {
            data: Arc::new(Mutex::new(protected)),
        }
    }

    pub fn lock(&self) -> Result<impl DerefMut<Target = WfManagerProtected> + '_> {
        Ok(self.data.lock().map_err(|_| {
            CoreError::LockPoisoned(
                "A workflow manager lock was poisoned. This should be impossible since they \
                are run on one thread."
                    .to_string(),
            )
        })?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Enforce thread-safeness of wf manager
    fn enforcer<W: Send + Sync>(_: W) {}

    #[test]
    fn is_threadsafe() {
        enforcer(WorkflowManager::new(&Default::default()));
    }
}
