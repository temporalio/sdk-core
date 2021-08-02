use crate::{
    errors::WorkflowUpdateError,
    protos::{
        coresdk::workflow_activation::WfActivation, temporal::api::common::v1::WorkflowExecution,
    },
    protosext::ValidPollWFTQResponse,
    workflow::{
        workflow_tasks::{OutstandingActivation, OutstandingTask},
        HistoryUpdate, Result, WFMachinesError, WorkflowManager,
    },
};
use futures::future::{BoxFuture, FutureExt};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
};
use tokio::sync::Mutex;

/// Provides a thread-safe way to access workflow machines for specific workflow runs
pub(crate) struct WorkflowConcurrencyManager {
    /// Maps run id -> data about and machines for that run
    runs: RwLock<HashMap<String, ManagedRun>>,
}

struct ManagedRun {
    wfm: Mutex<Option<WorkflowManager>>,
    task_queue: String,
    wft: Option<OutstandingTask>,
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and pushed into
    /// [ready_buffered_wft].
    buffered_resp: Option<ValidPollWFTQResponse>,
}

impl ManagedRun {
    fn new(wfm: WorkflowManager, task_queue: String) -> Self {
        Self {
            wfm: Mutex::new(Some(wfm)),
            task_queue,
            wft: None,
            activation: None,
            buffered_resp: None,
        }
    }
}

impl WorkflowConcurrencyManager {
    pub fn new() -> Self {
        Self {
            runs: Default::default(),
        }
    }

    /// Allows access to outstanding task for a run. Returns `None` if there is no knowledge of
    /// the run at all, or if the run exists but there is no outstanding workflow task.
    pub(crate) fn get_task(
        &self,
        run_id: &str,
    ) -> Option<impl Deref<Target = OutstandingTask> + '_> {
        let readlock = self.runs.read();
        if let Some(run) = readlock.get(run_id) {
            if run.wft.is_some() {
                Some(RwLockReadGuard::map(readlock, |hm| {
                    // Unwraps are safe because we hold the lock and just ensured run is in the map
                    hm.get(run_id).unwrap().wft.as_ref().unwrap()
                }))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Allows access to outstanding activation slot for a run. Returns `None` if there is no
    /// knowledge of the run at all, or if the run exists but there is no outstanding activation.
    pub(crate) fn get_activation(&self, run_id: &str) -> Option<OutstandingActivation> {
        let readlock = self.runs.read();
        if readlock.contains_key(run_id) {
            readlock.get(run_id).unwrap().activation
        } else {
            None
        }
    }

    /// Allows mutable access to outstanding workflow task slot for a run
    pub(crate) fn get_task_mut(
        &self,
        run_id: &str,
    ) -> Result<impl DerefMut<Target = Option<OutstandingTask>> + '_, WorkflowUpdateError> {
        // TODO: Lock could probably just be around whole managed run and then we only ever need
        //   read lock for entire machines map
        let writelock = self.runs.write();
        if writelock.contains_key(run_id) {
            Ok(RwLockWriteGuard::map(writelock, |hm| {
                // Unwrap is safe because we hold the lock and just ensured run is in the map
                &mut hm.get_mut(run_id).unwrap().wft
            }))
        } else {
            Err(WorkflowUpdateError {
                source: WFMachinesError::Fatal("Workflow machines not found".to_string()),
                run_id: run_id.to_owned(),
            })
        }
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    pub fn buffer_resp_if_outstanding_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Option<ValidPollWFTQResponse> {
        let mut writelock = self.runs.write();
        if let Some(mut run) = writelock.get_mut(&work.workflow_execution.run_id) {
            if run.wft.is_some() || run.activation.is_some() {
                debug!("Got new WFT for a run with outstanding work");
                run.buffered_resp = Some(work);
                None
            } else {
                Some(work)
            }
        } else {
            Some(work)
        }
    }

    pub fn insert_wft(
        &self,
        run_id: &str,
        task: OutstandingTask,
    ) -> Result<(), WorkflowUpdateError> {
        let mut dereffer = self.get_task_mut(&run_id)?;
        *dereffer = Some(task);
        Ok(())
    }

    pub fn delete_wft(&self, run_id: &str) -> Option<OutstandingTask> {
        if let Ok(ot) = self.get_task_mut(&run_id).as_deref_mut() {
            ot.take()
        } else {
            None
        }
    }

    pub fn insert_activation(
        &self,
        run_id: &str,
        activation: OutstandingActivation,
    ) -> Result<Option<OutstandingActivation>, WorkflowUpdateError> {
        let mut writelock = self.runs.write();
        let machine_ref = writelock.get_mut(run_id);
        if let Some(run) = machine_ref {
            Ok(run.activation.replace(activation))
        } else {
            Err(WorkflowUpdateError {
                source: WFMachinesError::Fatal("Workflow machines not found".to_string()),
                run_id: run_id.to_owned(),
            })
        }
    }

    pub fn delete_activation(&self, run_id: &str) -> Option<OutstandingActivation> {
        let mut writelock = self.runs.write();
        let machine_ref = writelock.get_mut(run_id);
        if let Some(run) = machine_ref {
            run.activation.take()
        } else {
            None
        }
    }

    pub fn task_queue_for(&self, run_id: &str) -> Option<String> {
        self.runs.read().get(run_id).map(|mr| mr.task_queue.clone())
    }

    pub async fn create_or_update(
        &self,
        run_id: &str,
        task_queue: String,
        history: HistoryUpdate,
        workflow_execution: WorkflowExecution,
    ) -> Result<WfActivation> {
        let span = debug_span!("create_or_update machines", %run_id);

        if self.runs.read().contains_key(run_id) {
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
                        Err(WFMachinesError::Fatal(
                            "Machines created with no jobs".to_string(),
                        ))
                    } else {
                        self.runs
                            .write()
                            .insert(run_id.to_string(), ManagedRun::new(wfm, task_queue));
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
        Fout: Send + Debug,
    {
        let readlock = self.runs.read();
        let m = readlock.get(run_id).ok_or(WFMachinesError::Fatal(
            "Missing workflow machines".to_string(),
        ))?;
        let mut wfm_mutex = m.wfm.lock().await;
        let mut wfm = wfm_mutex
            .take()
            .expect("Machine cannot possibly be accessed simultaneously");
        let res = mutator(&mut wfm).await;
        // Reinsert machine behind lock
        wfm_mutex.insert(wfm);

        res
    }

    /// Remove the workflow with the provided run id from management
    pub fn evict(&self, run_id: &str) -> Option<ValidPollWFTQResponse> {
        let val = self.runs.write().remove(run_id);
        val.map(|v| v.buffered_resp).flatten()
    }

    #[cfg(test)]
    pub fn outstanding_wft(&self) -> usize {
        self.runs
            .read()
            .iter()
            .filter(|(_, run)| run.wft.is_some())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // We test mostly error paths here since the happy paths are well covered by the tests of the
    // core sdk itself, and setting up the fake data is onerous here. If we make the concurrency
    // manager generic, testing the happy path is simpler.

    #[tokio::test]
    async fn returns_errors_on_creation() {
        let mgr = WorkflowConcurrencyManager::new();
        let res = mgr
            .create_or_update(
                "some_run_id",
                "some_tq".to_string(),
                HistoryUpdate::new_from_events(vec![], 0),
                Default::default(),
            )
            .await;
        // Should whine that the machines have nothing to do (history empty)
        assert_matches!(res.unwrap_err(), WFMachinesError::Fatal { .. })
    }
}
