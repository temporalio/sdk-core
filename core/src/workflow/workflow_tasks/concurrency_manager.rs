use crate::{
    protosext::ValidPollWFTQResponse,
    telemetry::metrics::{workflow_type, MetricsContext},
    workflow::{
        workflow_tasks::{OutstandingActivation, OutstandingTask, WorkflowMissingError},
        HistoryUpdate, Result, WFMachinesError, WorkflowManager,
    },
};
use futures::future::{BoxFuture, FutureExt};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
};
use temporal_sdk_core_protos::coresdk::workflow_activation::WorkflowActivation;

/// Provides a thread-safe way to access workflow machines for specific workflow runs
pub(crate) struct WorkflowConcurrencyManager {
    /// Maps run id -> data about and machines for that run
    runs: RwLock<HashMap<String, ManagedRun>>,
}

struct ManagedRun {
    wfm: Mutex<Option<WorkflowManager>>,
    wft: Option<OutstandingTask>,
    activation: Option<OutstandingActivation>,
    metrics: MetricsContext,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<ValidPollWFTQResponse>,
}

impl ManagedRun {
    fn new(wfm: WorkflowManager, metrics: MetricsContext) -> Self {
        Self {
            wfm: Mutex::new(Some(wfm)),
            wft: None,
            activation: None,
            metrics,
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
    ) -> Result<impl DerefMut<Target = Option<OutstandingTask>> + '_, WorkflowMissingError> {
        let writelock = self.runs.write();
        if writelock.contains_key(run_id) {
            Ok(RwLockWriteGuard::map(writelock, |hm| {
                // Unwrap is safe because we hold the lock and just ensured run is in the map
                &mut hm.get_mut(run_id).unwrap().wft
            }))
        } else {
            Err(WorkflowMissingError {
                run_id: run_id.to_owned(),
            })
        }
    }

    /// Fetch metrics context for a run
    pub(crate) fn run_metrics(
        &self,
        run_id: &str,
    ) -> Option<impl Deref<Target = MetricsContext> + '_> {
        let readlock = self.runs.read();
        if readlock.get(run_id).is_some() {
            Some(RwLockReadGuard::map(readlock, |hm| {
                // Unwraps are safe because we hold the lock and just ensured run is in the map
                &hm.get(run_id).unwrap().metrics
            }))
        } else {
            None
        }
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    pub fn buffer_resp_if_outstanding_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Option<ValidPollWFTQResponse> {
        let mut writelock = self.runs.write();
        let run_id = &work.workflow_execution.run_id;
        if let Some(mut run) = writelock.get_mut(run_id) {
            if run.wft.is_some() || run.activation.is_some() {
                debug!(run_id = %run_id, "Got new WFT for a run with outstanding work");
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
    ) -> Result<(), WorkflowMissingError> {
        let mut dereffer = self.get_task_mut(run_id)?;
        *dereffer = Some(task);
        Ok(())
    }

    /// Indicate it's finished and remove any outstanding workflow task associated with the run
    pub fn complete_wft(
        &self,
        run_id: &str,
        send_wft_complete_to_srv: bool,
    ) -> Option<OutstandingTask> {
        // If the WFT completion wasn't sent to the server, but we did see the final event, we still
        // want to clear the workflow task. This can really only happen in replay testing, where we
        // will generate poll responses with complete history but no attached query, and such a WFT
        // would never really exist. The server wouldn't send a workflow task with nothing to do,
        // but they are very useful for testing complete replay.
        let saw_final = self
            .access_sync(run_id, |wfm| wfm.machines.have_seen_terminal_event)
            .unwrap_or_default();
        if !saw_final && !send_wft_complete_to_srv {
            return None;
        }

        let retme = if let Ok(ot) = self.get_task_mut(run_id).as_deref_mut() {
            (*ot).take()
        } else {
            None
        };
        if let Some(ot) = &retme {
            if let Some(m) = self.run_metrics(run_id) {
                m.wf_task_latency(ot.start_time.elapsed());
            }
        }
        retme
    }

    pub fn insert_activation(
        &self,
        run_id: &str,
        activation: OutstandingActivation,
    ) -> Result<Option<OutstandingActivation>, WorkflowMissingError> {
        let mut writelock = self.runs.write();
        let machine_ref = writelock.get_mut(run_id);
        if let Some(run) = machine_ref {
            Ok(run.activation.replace(activation))
        } else {
            Err(WorkflowMissingError {
                run_id: run_id.to_owned(),
            })
        }
    }

    pub fn delete_activation(&self, run_id: &str) -> Option<OutstandingActivation> {
        let mut writelock = self.runs.write();
        let machine_ref = writelock.get_mut(run_id);
        machine_ref.and_then(|run| run.activation.take())
    }

    pub fn exists(&self, run_id: &str) -> bool {
        self.runs.read().get(run_id).is_some()
    }

    /// Create or update some workflow's machines. Borrowed arguments are cloned in the case of a
    /// new workflow instance.
    pub async fn create_or_update(
        &self,
        run_id: &str,
        history: HistoryUpdate,
        workflow_id: &str,
        namespace: &str,
        wf_type: &str,
        parent_metrics: &MetricsContext,
    ) -> Result<WorkflowActivation> {
        let span = debug_span!("create_or_update machines", %run_id);

        if self.runs.read().contains_key(run_id) {
            let activation = self
                .access(run_id, move |wfm: &mut WorkflowManager| {
                    async move {
                        let _enter = span.enter();
                        wfm.machines.metrics.sticky_cache_hit();
                        wfm.feed_history_from_server(history).await
                    }
                    .boxed()
                })
                .await?;
            Ok(activation)
        } else {
            // Create a new workflow machines instance for this workflow, initialize it, and
            // track it.
            let metrics = parent_metrics.with_new_attrs([workflow_type(wf_type.to_string())]);
            let mut wfm = WorkflowManager::new(
                history,
                namespace.to_owned(),
                workflow_id.to_owned(),
                wf_type.to_owned(),
                run_id.to_owned(),
                metrics.clone(),
            );
            match wfm.get_next_activation().await {
                Ok(activation) => {
                    if activation.jobs.is_empty() {
                        Err(WFMachinesError::Fatal(
                            "Machines created with no jobs".to_string(),
                        ))
                    } else {
                        self.runs
                            .write()
                            .insert(run_id.to_string(), ManagedRun::new(wfm, metrics));
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
        // TODO: This is less than ideal. We must avoid holding the read lock on the overall machine
        //  map while async-ly mutating the inner machine. So, we take it out of the map and put
        //  it back when we're done. We should restructure things to avoid the top-level lock
        //  on the map.

        let mut wfm = {
            let readlock = self.runs.read();
            let m = readlock
                .get(run_id)
                .ok_or_else(|| WFMachinesError::Fatal("Missing workflow machines".to_string()))?;
            let x = m
                .wfm
                .lock()
                .take()
                .expect("WorkflowManager inside a ManagedRun is never None");
            x
        };

        let res = mutator(&mut wfm).await;

        {
            let readlock = self.runs.read();
            // SAFETY: Since runs are accessed serially the machines cannot go missing while
            // we are modifying them for a given run
            let m = readlock
                .get(run_id)
                .expect("Machine cannot be missing as each run is accessed serially");
            *m.wfm.lock() = Some(wfm);
        }

        res
    }

    pub fn access_sync<F, Fout>(
        &self,
        run_id: &str,
        mutator: F,
    ) -> Result<Fout, WorkflowMissingError>
    where
        F: for<'a> FnOnce(&'a mut WorkflowManager) -> Fout,
        Fout: Send + Debug,
    {
        let readlock = self.runs.read();
        let m = readlock.get(run_id).ok_or_else(|| WorkflowMissingError {
            run_id: run_id.to_string(),
        })?;
        let mut wfm_mutex = m.wfm.lock();
        Ok(mutator(wfm_mutex.as_mut().unwrap()))
    }

    /// Remove the workflow with the provided run id from management
    pub fn evict(&self, run_id: &str) -> Option<ValidPollWFTQResponse> {
        let val = self.runs.write().remove(run_id);
        val.and_then(|v| v.buffered_resp)
    }

    /// Clear and return any buffered polling response for this run ID
    pub fn take_buffered_poll(&self, run_id: &str) -> Option<ValidPollWFTQResponse> {
        let mut writelock = self.runs.write();
        let val = writelock.get_mut(run_id);
        val.and_then(|v| v.buffered_resp.take())
    }

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
    use crate::test_help::canned_histories;
    use tokio::sync::Barrier;

    // We test mostly error paths here since the happy paths are well covered by the tests of the
    // core sdk itself, and setting up the fake data is onerous here. If we make the concurrency
    // manager generic, testing the happy path is simpler.

    #[tokio::test]
    async fn returns_errors_on_creation() {
        let mgr = WorkflowConcurrencyManager::new();
        let res = mgr
            .create_or_update(
                "some_run_id",
                HistoryUpdate::new_from_events(vec![], 0),
                "fake_wf_id",
                "fake_namespace",
                "fake_wf_type",
                &Default::default(),
            )
            .await;
        // Should whine that the machines have nothing to do (history empty)
        assert_matches!(res.unwrap_err(), WFMachinesError::Fatal { .. });
    }

    /// This test makes sure that if we're stuck on an await within the machine mutator we don't
    /// cause a deadlock if a write happens during that. This test will hang without proper
    /// implementation.
    #[tokio::test]
    async fn aba_deadlock_prevented() {
        let run_id = "some_run_id";
        let timer_hist = canned_histories::single_timer("t");
        let access_barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
        let wft = timer_hist.get_history_info(1).unwrap();

        let mgr = WorkflowConcurrencyManager::new();
        mgr.create_or_update(
            run_id,
            wft.clone().into(),
            "fake_wf_id",
            "fake_namespace",
            "fake_wf_type",
            &Default::default(),
        )
        .await
        .unwrap();
        // Perform access which blocks
        let access_fut = mgr.access(run_id, |_wfm| {
            async {
                // Wait to make sure access has started
                access_barr.wait().await;
                // Wait to make sure write has finished
                access_barr.wait().await;
                Ok(())
            }
            .boxed()
        });
        let write_fut = async {
            // Wait to make sure access has started
            access_barr.wait().await;
            // Now try writing
            mgr.create_or_update(
                "different_run_id",
                wft.clone().into(),
                "fake_wf_id",
                "fake_namespace",
                "fake_wf_type",
                &Default::default(),
            )
            .await
            .unwrap();
            // Indicate write has finished
            access_barr.wait().await;
        };
        let (r1, _) = tokio::join!(access_fut, write_fut);
        r1.unwrap();
    }
}
