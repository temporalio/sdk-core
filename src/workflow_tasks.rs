//! Management of workflow tasks

use crate::{
    errors::WorkflowUpdateError,
    machines::WFCommand,
    pending_activations::PendingActivations,
    protos::coresdk::workflow_activation::{create_evict_activation, WfActivation},
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    workflow::{
        NextWfActivation, OutgoingServerCommands, WorkflowCachingPolicy,
        WorkflowConcurrencyManager, WorkflowError, WorkflowManager,
    },
    WfActivationUpdate,
};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedSender;
use tracing::Span;

/// Centralizes concerns related to applying new workflow tasks and reporting the activations they
/// produce.
///
/// It is intentionally free of any interactions with the server client to promote testability
pub struct WorkflowTaskManager {
    /// Manages threadsafe access to workflow machine instances
    workflow_machines: WorkflowConcurrencyManager,
    /// Maps run ids to info about their associated workflow task
    workflow_info: DashMap<String, WorkflowTaskInfo>,
    /// Workflows may generate new activations immediately upon completion (ex: while replaying,
    /// or when cancelling an activity in try-cancel/abandon mode). They queue here.
    pending_activations: PendingActivations,
    /// Used to track which workflows (by run id) have outstanding workflow tasks. Additionally,
    /// if the value is set, it indicates there is a buffered poll response from the server that
    /// applies to this run. This can happen when lang takes too long to complete a task and
    /// the task times out, for example. Upon next completion, the buffered response will be
    /// removed and pushed into [ready_buffered_wft].
    ///
    /// Only the most recent such reply from the server for a given run is kept.
    outstanding_workflow_tasks: DashMap<String, Option<ValidPollWFTQResponse>>,
    /// Holds poll wft responses from the server that need to be applied
    ready_buffered_wft: SegQueue<ValidPollWFTQResponse>,
    /// Used to wake blocked workflow task polling
    workflow_activations_update: UnboundedSender<WfActivationUpdate>,
    eviction_policy: WorkflowCachingPolicy,
}

/// Contains important information about a given workflow task that we need to memorize while
/// lang handles it.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInfo {
    task_token: TaskToken,
    run_id: String,
    attempt: u32,
    task_queue: String,
}

#[derive(Debug, derive_more::From)]
pub enum NewWfTaskOutcome {
    /// A new activation for the workflow should be issued to lang
    IssueActivation(WfActivation),
    /// The poll loop should be restarted, there is nothing to do
    RestartPollLoop,
    /// The workflow task should be auto-completed with an empty command list, as it must be replied
    /// to but there is no meaningful work for lang to do.
    Autocomplete,
}

#[derive(Debug)]
pub enum FailedActivationOutcome {
    NoReport,
    Report(TaskToken),
}

impl WorkflowTaskManager {
    pub(crate) fn new(
        workflow_activations_update: UnboundedSender<WfActivationUpdate>,
        eviction_policy: WorkflowCachingPolicy,
    ) -> Self {
        Self {
            workflow_machines: WorkflowConcurrencyManager::new(),
            workflow_info: Default::default(),
            pending_activations: Default::default(),
            outstanding_workflow_tasks: Default::default(),
            ready_buffered_wft: Default::default(),
            workflow_activations_update,
            eviction_policy,
        }
    }

    pub fn shutdown(&self) {
        self.workflow_machines.shutdown();
    }

    pub fn next_pending_activation(&self) -> Option<WfActivation> {
        self.pending_activations.pop()
    }

    pub fn next_buffered_poll(&self) -> Option<ValidPollWFTQResponse> {
        self.ready_buffered_wft.pop()
    }

    #[cfg(test)]
    pub fn outstanding_wft(&self) -> usize {
        self.outstanding_workflow_tasks.len()
    }

    /// Evict a workflow from the cache by its run id. Returns that workflow's task info if
    /// it was present.
    pub fn evict_run(&self, run_id: &str) -> Option<WorkflowTaskInfo> {
        if let Some((_, wti)) = self.workflow_info.remove(run_id) {
            let run_id = &wti.run_id;
            debug!(run_id=%run_id, "Evicting run");
            let maybe_buffered = self.outstanding_workflow_tasks.remove(run_id);
            self.workflow_machines.evict(run_id);
            self.pending_activations.remove_all_with_run_id(run_id);
            // Queue up an eviction activation
            self.pending_activations.push(create_evict_activation(
                run_id.to_owned(),
                wti.task_queue.clone(),
            ));
            let _ =
                self.workflow_activations_update
                    .send(WfActivationUpdate::WorkflowTaskComplete {
                        task_queue: wti.task_queue.clone(),
                    });
            // If we just evicted something and there was a buffered poll response for the workflow,
            // it is now ready to be produced by the next poll. (Not immediate next, since, ignoring
            // other workflows, the next poll will be the eviction we just produced. Buffered polls
            // always are popped after pending activations)
            if let Some(buffered_poll) = maybe_buffered.and_then(|mb| mb.1) {
                self.ready_buffered_wft.push(buffered_poll);
            }
            Some(wti)
        } else {
            None
        }
    }

    /// Given a validated wf task from the server, prepare an activation (if there is one) to be
    /// sent to lang. If applying the task to the workflow's state does not produce a new
    /// activation, `None` is returned.
    ///
    /// The new activation is immediately considered to be an outstanding workflow task - so it is
    /// expected that new activations will be dispatched to lang right away.
    pub(crate) fn apply_new_wft(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<NewWfTaskOutcome, WorkflowUpdateError> {
        debug!(
            task_token = %&work.task_token,
            history_length = %work.history.events.len(),
            "Received new workflow task from server"
        );

        if let Some(mut outstanding_entry) = self
            .outstanding_workflow_tasks
            .get_mut(&work.workflow_execution.run_id)
        {
            debug!("Got new WFT for a run with one outstanding");
            *outstanding_entry.value_mut() = Some(work);
            return Ok(NewWfTaskOutcome::RestartPollLoop);
        }

        let task_queue = work.task_queue.clone();
        let next_activation = self.instantiate_or_update_workflow(work)?;

        if let Some(na) = next_activation {
            self.outstanding_workflow_tasks
                .insert(na.run_id().to_owned(), None);
            Ok(NewWfTaskOutcome::IssueActivation(na.finalize(task_queue)))
        } else {
            Ok(NewWfTaskOutcome::Autocomplete)
        }
    }

    /// Record a successful activation. Returns (if any) commands that should be reported to the
    /// server as part of wft completion
    pub(crate) fn successful_activation(
        &self,
        run_id: &str,
        commands: Vec<WFCommand>,
    ) -> Result<Option<OutgoingServerCommands>, WorkflowUpdateError> {
        // TODO: Error?
        let tt = self
            .workflow_info
            .get(run_id)
            .expect("Must exist")
            .task_token
            .clone();
        // Send commands from lang into the machines
        self.access_wf_machine(run_id, move |mgr| mgr.push_commands(commands))?;
        self.enqueue_next_activation_if_needed(run_id)?;
        // We want to fetch the outgoing commands only after any new activation has been queued,
        // as doing so may have altered the outgoing commands.
        let server_cmds = self.access_wf_machine(run_id, |w| Ok(w.get_server_commands(tt)))?;
        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are at the final workflow task (IE: the lang SDK has caught up on
        // replay)
        let ret = if !self.pending_activations.has_pending(run_id)
            && server_cmds.at_final_workflow_task
        {
            Some(server_cmds)
        } else {
            None
        };
        self.after_wft_report(run_id);
        Ok(ret)
    }

    /// Record that an activation failed, returns true if the failure should be reported to the
    /// server
    pub fn failed_activation(&self, run_id: &str) -> FailedActivationOutcome {
        let tt = if let Some(info) = self.workflow_info.get(run_id) {
            info.task_token.clone()
        } else {
            warn!(
                "No info for workflow with run id {} found when trying to fail activation",
                run_id
            );
            return FailedActivationOutcome::NoReport;
        };
        // Blow up any cached data associated with the workflow
        let should_report = if let Some(wti) = self.evict_run(run_id) {
            // Only report to server if the last task wasn't also a failure (avoid spam)
            wti.attempt <= 1
        } else {
            true
        };
        self.after_wft_report(run_id);
        if should_report {
            FailedActivationOutcome::Report(tt)
        } else {
            FailedActivationOutcome::NoReport
        }
    }

    /// Will create a new workflow manager if needed for the workflow activation, if not, it will
    /// feed the existing manager the updated history we received from the server.
    ///
    /// Also updates [CoreSDK::workflow_task_tokens] and validates the
    /// [PollWorkflowTaskQueueResponse]
    ///
    /// Returns the next workflow activation and the workflow's run id
    fn instantiate_or_update_workflow(
        &self,
        poll_wf_resp: ValidPollWFTQResponse,
    ) -> Result<Option<NextWfActivation>, WorkflowUpdateError> {
        let run_id = poll_wf_resp.workflow_execution.run_id.clone();

        let wft_info = WorkflowTaskInfo {
            run_id: run_id.clone(),
            attempt: poll_wf_resp.attempt,
            task_queue: poll_wf_resp.task_queue,
            task_token: poll_wf_resp.task_token,
        };
        self.workflow_info.insert(run_id.clone(), wft_info);

        match self.workflow_machines.create_or_update(
            &run_id,
            poll_wf_resp.history,
            poll_wf_resp.workflow_execution,
        ) {
            Ok(activation) => Ok(activation),
            Err(source) => Err(WorkflowUpdateError { source, run_id }),
        }
    }

    /// Check if thew workflow run needs another activation and queue it up if there is one by
    /// pushing it into the pending activations list
    fn enqueue_next_activation_if_needed(&self, run_id: &str) -> Result<bool, WorkflowUpdateError> {
        let mut new_activation = false;
        if let Some(next_activation) =
            self.access_wf_machine(run_id, move |mgr| mgr.get_next_activation())?
        {
            let info = self
                .workflow_info
                .get(run_id)
                .expect("Task token is present in map if there is a new pending activation");
            self.pending_activations
                .push(next_activation.finalize(info.task_queue.clone()));
            new_activation = true;
            let _ = self
                .workflow_activations_update
                .send(WfActivationUpdate::NewPendingActivation);
        }
        Ok(new_activation)
    }

    /// Called after every WFT completion or failure, updates outstanding task status & issues
    /// evictions if required
    fn after_wft_report(&self, run_id: &str) {
        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        if !self.pending_activations.has_pending(run_id) {
            self.outstanding_workflow_tasks.remove(run_id);

            // Blow them up if we're in non-sticky mode as well
            if self.eviction_policy == WorkflowCachingPolicy::NonSticky {
                self.evict_run(run_id);
            }

            // The evict may or may not have already done this, but even when we aren't evicting
            // we want to remove from the run id mapping if we completed the wft.
            if let Some((_, wti)) = self.workflow_info.remove(run_id) {
                let _ = self.workflow_activations_update.send(
                    WfActivationUpdate::WorkflowTaskComplete {
                        task_queue: wti.task_queue,
                    },
                );
            }
        }
    }

    /// Wraps access to `self.workflow_machines.access`, properly passing in the current tracing
    /// span to the wf machines thread.
    fn access_wf_machine<F, Fout>(
        &self,
        run_id: &str,
        mutator: F,
    ) -> Result<Fout, WorkflowUpdateError>
    where
        F: FnOnce(&mut WorkflowManager) -> Result<Fout, WorkflowError> + Send + 'static,
        Fout: Send + Debug + 'static,
    {
        let curspan = Span::current();
        let mutator = move |wfm: &mut WorkflowManager| {
            let _e = curspan.enter();
            mutator(wfm)
        };
        self.workflow_machines
            .access(run_id, mutator)
            .map_err(|source| WorkflowUpdateError {
                source,
                run_id: run_id.to_owned(),
            })
    }
}
