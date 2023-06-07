use super::*;
use crate::{
    replay::TestHistoryBuilder,
    test_help::TEST_Q,
    worker::{
        client::mocks::DEFAULT_TEST_CAPABILITIES,
        workflow::{
            history_update::tests::TestHBExt, machines::WorkflowMachines, WFCommand,
            WorkflowFetcher,
        },
        LocalActRequest, LocalActivityResolution,
    },
};
use crossbeam::channel::bounded;
use std::{convert::TryInto, time::Duration};
use temporal_sdk::{WorkflowFunction, WorkflowResult};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{create_evict_activation, remove_from_cache::EvictionReason},
        workflow_completion::{
            workflow_activation_completion::Status, WorkflowActivationCompletion,
        },
    },
    temporal::api::common::v1::Payload,
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

pub(crate) struct WFFutureDriver {
    completions_q: crossbeam::channel::Receiver<WorkflowActivationCompletion>,
}

impl WorkflowFetcher for WFFutureDriver {
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        if let Ok(completion) = self.completions_q.try_recv() {
            debug!("Managed wf completion: {}", completion);
            completion
                .status
                .map(|s| match s {
                    Status::Successful(s) => s
                        .commands
                        .into_iter()
                        .map(|cmd| cmd.try_into().unwrap())
                        .collect(),
                    Status::Failed(_) => panic!("Ahh failed"),
                })
                .unwrap_or_default()
        } else {
            // Sender went away so nothing to do here. End of wf/test.
            vec![]
        }
    }
}

#[must_use]
pub struct ManagedWFFunc {
    mgr: WorkflowManager,
    activation_tx: UnboundedSender<WorkflowActivation>,
    completions_rx: UnboundedReceiver<WorkflowActivationCompletion>,
    completions_sync_tx: crossbeam::channel::Sender<WorkflowActivationCompletion>,
    future_handle: Option<JoinHandle<WorkflowResult<Payload>>>,
    was_shutdown: bool,
}

impl ManagedWFFunc {
    pub fn new(hist: TestHistoryBuilder, func: WorkflowFunction, args: Vec<Payload>) -> Self {
        Self::new_from_update(hist.as_history_update(), func, args)
    }

    pub fn new_from_update(
        hist: HistoryUpdate,
        func: WorkflowFunction,
        args: Vec<Payload>,
    ) -> Self {
        let (completions_tx, completions_rx) = unbounded_channel();
        let (wff, activations) = func.start_workflow(
            "testnamespace".to_string(),
            TEST_Q.to_string(),
            args,
            completions_tx,
        );
        let spawned = tokio::spawn(wff);
        let (completions_sync_tx, completions_sync_rx) = bounded(1);
        let driver = WFFutureDriver {
            completions_q: completions_sync_rx,
        };
        let state_machines = WorkflowMachines::new(
            RunBasics {
                namespace: "test_namespace".to_string(),
                workflow_id: "wfid".to_string(),
                workflow_type: "wftype".to_string(),
                run_id: "runid".to_string(),
                task_queue: TEST_Q.to_string(),
                history: hist,
                metrics: MetricsContext::no_op(),
                capabilities: DEFAULT_TEST_CAPABILITIES,
            },
            Box::new(driver).into(),
        );
        let mgr = WorkflowManager::new_from_machines(state_machines);
        Self {
            mgr,
            activation_tx: activations,
            completions_rx,
            completions_sync_tx,
            future_handle: Some(spawned),
            was_shutdown: false,
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        let res = self.mgr.get_next_activation()?;
        debug!("Managed wf next activation: {}", &res);
        self.push_activation_to_wf(&res).await?;
        Ok(res)
    }

    /// Return outgoing server commands as of the last iteration
    pub(crate) fn get_server_commands(&mut self) -> OutgoingServerCommands {
        self.mgr.get_server_commands()
    }

    pub(crate) fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.mgr.drain_queued_local_activities()
    }

    /// Feed new history, as if received a new poll result. Returns new activation
    #[instrument(skip(self, update))]
    pub(crate) async fn new_history(
        &mut self,
        update: HistoryUpdate,
    ) -> Result<WorkflowActivation> {
        let res = self.mgr.feed_history_from_server(update)?;
        self.push_activation_to_wf(&res).await?;
        Ok(res)
    }

    /// Say a local activity completed (they always take 1 second in these tests)
    pub(crate) fn complete_local_activity(
        &mut self,
        seq_num: u32,
        result: ActivityExecutionResult,
    ) -> Result<bool> {
        self.mgr
            .notify_of_local_result(LocalResolution::LocalActivity(LocalActivityResolution {
                seq: seq_num,
                // We accept normal execution results and do this conversion because there
                // are more helpers for constructing them.
                result: result
                    .status
                    .expect("LA result must have a status")
                    .try_into()
                    .expect("LA execution result must be a valid LA result"),
                runtime: Duration::from_secs(1),
                attempt: 1,
                backoff: None,
                // Tests at this level don't use the LA dispatcher, so this is irrelevant
                original_schedule_time: None,
            }))
    }

    /// During testing it can be useful to run through all activations to simulate replay
    /// easily. Returns the last produced activation with jobs in it, or an activation with no
    /// jobs if the first call had no jobs.
    pub(crate) async fn process_all_activations(&mut self) -> Result<WorkflowActivation> {
        let mut last_act = self.get_next_activation().await?;
        let mut next_act = self.get_next_activation().await?;
        while !next_act.jobs.is_empty() {
            last_act = next_act;
            next_act = self.get_next_activation().await?;
        }
        Ok(last_act)
    }

    pub async fn shutdown(&mut self) -> WorkflowResult<Payload> {
        self.was_shutdown = true;
        // Send an eviction to ensure wf exits if it has not finished (ex: feeding partial hist)
        let _ = self.activation_tx.send(create_evict_activation(
            "not actually important".to_string(),
            "force shutdown".to_string(),
            EvictionReason::Unspecified,
        ));
        self.future_handle.take().unwrap().await.unwrap()
    }

    #[instrument(skip(self, res))]
    async fn push_activation_to_wf(&mut self, res: &WorkflowActivation) -> Result<()> {
        if res.jobs.is_empty() {
            // Nothing to do here
            return Ok(());
        }
        self.activation_tx
            .send(res.clone())
            .expect("Workflow should not be dropped if we are still sending activations");
        // Move the completion response to the sync workflow bridge
        self.completions_sync_tx
            .send(self.completions_rx.recv().await.unwrap())
            .unwrap();
        self.mgr.machines.iterate_machines()?;
        Ok(())
    }
}

impl Drop for ManagedWFFunc {
    fn drop(&mut self) {
        // Double panics cause a SIGILL
        if !self.was_shutdown && !std::thread::panicking() {
            panic!("You must call `shutdown` to properly use ManagedWFFunc in tests")
        }
    }
}
