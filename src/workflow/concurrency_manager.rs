use crate::{
    protos::coresdk::workflow_activation::WfActivation,
    protos::temporal::api::common::v1::WorkflowExecution,
    workflow::HistoryUpdate,
    workflow::{Result, WorkflowError, WorkflowManager},
};
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt};
use std::fmt::Debug;
use tokio::sync::Mutex;

// TODO: Redo docstr
/// Provides a thread-safe way to access workflow machines which live exclusively on one thread
/// managed by this struct. We could make this generic for any collection of things which need
/// to live on one thread, if desired.
pub(crate) struct WorkflowConcurrencyManager {
    /// Maps run id -> task containing machines for that run
    machines: DashMap<String, Mutex<Option<WorkflowManager>>>,
}

impl WorkflowConcurrencyManager {
    pub fn new() -> Self {
        Self {
            machines: Default::default(),
        }
    }

    pub fn exists(&self, run_id: &str) -> bool {
        self.machines.contains_key(run_id)
    }

    pub async fn create_or_update(
        &self,
        run_id: &str,
        history: HistoryUpdate,
        workflow_execution: WorkflowExecution,
    ) -> Result<WfActivation> {
        let span = debug_span!("create_or_update machines", %run_id);

        if self.exists(run_id) {
            let activation = self
                .access(run_id, move |wfm: &mut WorkflowManager| {
                    async move {
                        let _enter = span.enter();
                        wfm.feed_history_from_server(history).await
                    }
                    .boxed()
                })
                .await?;
            Ok(activation)
        } else {
            // Create a new workflow machines instance for this workflow, initialize it, and
            // track it.
            let mut wfm = WorkflowManager::new(history, workflow_execution);
            match wfm.get_next_activation().await {
                Ok(activation) => {
                    if activation.jobs.is_empty() {
                        Err(WorkflowError::MachineWasCreatedWithNoJobs {
                            run_id: wfm.machines.run_id,
                        })
                    } else {
                        self.machines
                            .insert(run_id.to_string(), Mutex::new(Some(wfm)));
                        Ok(activation)
                    }
                }
                Err(e) => Err(e),
            }
        }
    }

    pub async fn access<F, Fout>(&self, run_id: &str, mutator: F) -> Result<Fout>
    where
        F: for<'a> FnOnce(&'a mut WorkflowManager) -> BoxFuture<Result<Fout>>,
        Fout: Send + Debug, //+ 'static,
    {
        let m = self
            .machines
            .get_mut(run_id)
            .ok_or_else(|| WorkflowError::MissingMachine {
                run_id: run_id.to_string(),
            })?;
        let mut wfm_mutex = m.lock().await;
        let mut wfm = wfm_mutex
            .take()
            .expect("Machine cannot possibly be accessed simultaneously");
        let res = mutator(&mut wfm).await;
        // Reinsert machine behind lock
        wfm_mutex.insert(wfm);

        res
    }

    /// Remove the workflow with the provided run id from management
    pub fn evict(&self, run_id: &str) {
        self.machines.remove(run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::history::v1::History;

    // We test mostly error paths here since the happy paths are well covered by the tests of the
    // core sdk itself, and setting up the fake data is onerous here. If we make the concurrency
    // manager generic, testing the happy path is simpler.

    #[tokio::test]
    async fn returns_errors_on_creation() {
        let mgr = WorkflowConcurrencyManager::new();
        let res = mgr
            .create_or_update(
                "some_run_id",
                HistoryUpdate::new(History::default(), 0, 0),
                Default::default(),
            )
            .await;
        // Should whine that the machines have nothing to do (history empty)
        assert_matches!(
            res.unwrap_err(),
            WorkflowError::MachineWasCreatedWithNoJobs { .. }
        )
    }
}
