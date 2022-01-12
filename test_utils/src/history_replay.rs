mod history_builder;
mod history_info;

pub use history_builder::{default_wes_attribs, TestHistoryBuilder};
pub use history_info::HistoryInfo;

use crate::{mock_gateway, ServerGatewayApis, TEST_Q};
use std::sync::Arc;
use temporal_sdk_core::CoreSDK;
use temporal_sdk_core_api::{
    errors::{
        CompleteActivityError, CompleteWfError, PollActivityError, PollWfError,
        WorkerRegistrationError,
    },
    worker::{WorkerConfig, WorkerConfigBuilder},
    Core, CoreLog,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_task::ActivityTask, workflow_activation::WorkflowActivation,
        workflow_completion::WorkflowActivationCompletion, ActivityHeartbeat,
        ActivityTaskCompletion,
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        history::v1::History,
        workflowservice::v1::{RespondWorkflowTaskFailedResponse, StartWorkflowExecutionResponse},
    },
};

pub static DEFAULT_WORKFLOW_TYPE: &str = "not_specified";

pub trait ReplayCore {
    /// Make a fake worker for the provided task queue name which will use the provided history
    /// to simulate poll responses containing that entire history.
    fn make_replay_worker(
        &self,
        task_queue: impl Into<String>,
        history: &History,
    ) -> Result<(), WorkerRegistrationError>;
}

pub(crate) struct ReplayCoreImpl {
    pub inner: CoreSDK,
}

impl ReplayCore for ReplayCoreImpl {
    fn make_replay_worker(
        &self,
        task_queue: impl Into<String>,
        history: &History,
    ) -> Result<(), WorkerRegistrationError> {
        let mock_g = mock_gateway_from_history(history);
        let config = WorkerConfigBuilder::default()
            .task_queue(task_queue)
            .no_remote_activities(true)
            .build()
            .expect("Worker config constructs properly");
        self.inner
            .register_worker_with_gateway(config, Arc::new(mock_g))
    }
}

#[async_trait::async_trait]
impl Core for ReplayCoreImpl {
    fn register_worker(&self, _config: WorkerConfig) -> Result<(), WorkerRegistrationError> {
        panic!("Do not use `register_worker` with a replay core instance")
    }

    async fn poll_workflow_activation(
        &self,
        task_queue: &str,
    ) -> Result<WorkflowActivation, PollWfError> {
        self.inner.poll_workflow_activation(task_queue).await
    }

    async fn poll_activity_task(
        &self,
        task_queue: &str,
    ) -> Result<ActivityTask, PollActivityError> {
        self.inner.poll_activity_task(task_queue).await
    }

    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        self.inner.complete_workflow_activation(completion).await
    }

    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        self.inner.complete_activity_task(completion).await
    }

    fn record_activity_heartbeat(&self, _details: ActivityHeartbeat) {
        // do nothing
    }

    fn request_workflow_eviction(&self, task_queue: &str, run_id: &str) {
        self.inner.request_workflow_eviction(task_queue, run_id)
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis + Send + Sync> {
        self.inner.server_gateway()
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await
    }

    async fn shutdown_worker(&self, task_queue: &str) {
        self.inner.shutdown_worker(task_queue).await
    }

    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        self.inner.fetch_buffered_logs()
    }
}

pub fn mock_gateway_from_history(history: &History) -> impl ServerGatewayApis {
    let mut mg = mock_gateway();

    let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
    let wf = WorkflowExecution {
        workflow_id: "fake_wf_id".to_string(),
        run_id: hist_info.orig_run_id().to_string(),
    };

    let wf_clone = wf.clone();
    mg.expect_start_workflow().returning(move |_, _, _, _, _| {
        Ok(StartWorkflowExecutionResponse {
            run_id: wf_clone.run_id.clone(),
        })
    });

    mg.expect_poll_workflow_task().returning(move |_, _| {
        let mut resp = hist_info.as_poll_wft_response(TEST_Q);
        resp.workflow_execution = Some(wf.clone());
        Ok(resp)
    });

    mg.expect_fail_workflow_task()
        .returning(|_, _, _| Ok(RespondWorkflowTaskFailedResponse {}));

    mg
}
