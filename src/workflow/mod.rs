mod bridge;
mod concurrency_manager;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use concurrency_manager::WorkflowConcurrencyManager;

use crate::{
    machines::{ProtoCommand, WFCommand, WorkflowMachines},
    protos::{
        coresdk::WfActivation,
        temporal::api::{
            history::v1::History,
            workflowservice::v1::{
                PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
                StartWorkflowExecutionResponse,
            },
        },
    },
    protosext::HistoryInfo,
    CoreError, Result,
};
use std::sync::mpsc::Sender;
use tracing::Level;

/// Implementors can provide new workflow tasks to the SDK. The connection to the server is the real
/// implementor.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PollWorkflowTaskQueueApi {
    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
}

/// Implementors can complete tasks as would've been issued by [Core::poll]. The real implementor
/// sends the completed tasks to the server.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait RespondWorkflowTaskCompletedApi {
    /// Complete a task by sending it to the server. `task_token` is the task token that would've
    /// been received from [PollWorkflowTaskQueueApi::poll]. `commands` is a list of new commands
    /// to send to the server, such as starting a timer.
    async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
}

/// Implementors should send StartWorkflowExecutionRequest to the server and pass the response back.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait StartWorkflowExecutionApi {
    /// Starts workflow execution.
    async fn start_workflow(
        &self,
        namespace: &str,
        task_queue: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<StartWorkflowExecutionResponse>;
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
pub(crate) struct WorkflowManager {
    pub machines: WorkflowMachines,
    pub command_sink: Sender<Vec<WFCommand>>,
    /// The last recorded history we received from the server for this workflow run. This must be
    /// kept because the lang side polls & completes for every workflow task, but we do not need
    /// to poll the server that often during replay.
    last_history_from_server: History,
    last_history_task_count: usize,
    /// The current workflow task number this run is on. Starts at one and monotonically increases.
    current_wf_task_num: usize,
}

impl WorkflowManager {
    /// Create a new workflow manager from a server workflow task queue response.
    pub fn new(poll_resp: PollWorkflowTaskQueueResponse) -> Result<Self> {
        let (history, we) = if let PollWorkflowTaskQueueResponse {
            workflow_execution: Some(we),
            history: Some(hist),
            ..
        } = poll_resp
        {
            (hist, we)
        } else {
            return Err(CoreError::BadDataFromWorkProvider(poll_resp.clone()));
        };

        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(we.workflow_id, we.run_id, Box::new(wfb));
        Ok(Self {
            machines: state_machines,
            command_sink: cmd_sink,
            last_history_task_count: history.get_workflow_task_count(None)?,
            last_history_from_server: history,
            current_wf_task_num: 1,
        })
    }
}

#[derive(Debug)]
pub(crate) struct NextWfActivation {
    pub activation: Option<WfActivation>,
    pub more_activations_needed: bool,
}

impl WorkflowManager {
    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay. It will return a workflow
    /// activation if one is needed, as well as a bool indicating if there are more workflow tasks
    /// that need to be performed to replay the remaining history.
    #[instrument(skip(self))]
    pub fn feed_history_from_server(&mut self, hist: History) -> Result<NextWfActivation> {
        let task_hist = HistoryInfo::new_from_history(&hist, Some(self.current_wf_task_num))?;
        let task_ct = hist.get_workflow_task_count(None)?;
        self.last_history_task_count = task_ct;
        self.last_history_from_server = hist;
        task_hist.apply_history_events(&mut self.machines)?;
        let activation = self.machines.get_wf_activation();
        let more_activations_needed = task_ct > self.current_wf_task_num;

        if more_activations_needed {
            event!(Level::DEBUG, msg = "More activations needed");
        }

        self.current_wf_task_num += 1;

        Ok(NextWfActivation {
            activation,
            more_activations_needed,
        })
    }

    pub fn get_next_activation(&mut self) -> Result<NextWfActivation> {
        let hist = &self.last_history_from_server;
        let task_hist = HistoryInfo::new_from_history(hist, Some(self.current_wf_task_num))?;
        task_hist.apply_history_events(&mut self.machines)?;
        let activation = self.machines.get_wf_activation();

        self.current_wf_task_num += 1;
        let more_activations_needed = self.current_wf_task_num <= self.last_history_task_count;

        Ok(NextWfActivation {
            activation,
            more_activations_needed,
        })
    }
}
