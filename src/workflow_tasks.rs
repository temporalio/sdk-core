//! Management of workflow tasks

use crate::pending_activations::PendingActivations;
use crate::protos::coresdk::workflow_activation::{create_evict_activation, WfActivation};
use crate::workflow::{NextWfActivation, OutgoingServerCommands};
use crate::{
    errors::WorkflowUpdateError,
    machines::WFCommand,
    pollers::{new_workflow_task_buffer, PollWorkflowTaskBuffer},
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    workflow::{WorkflowConcurrencyManager, WorkflowError, WorkflowManager},
    ServerGatewayApis,
};
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use std::{fmt::Debug, sync::Arc};
use tokio::sync::Notify;
use tracing::Span;

/// Centralizes concerns related to applying new workflow tasks and reporting the activations they
/// produce.
///
/// It is intentionally free of any interactions with the server client to promote testability
pub struct WorkflowTaskManager {
    /// Manages threadsafe access to workflow machine instances
    workflow_machines: WorkflowConcurrencyManager,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<TaskToken, String>,
    /// Workflows may generate new activations immediately upon completion (ex: while replaying,
    /// or when cancelling an activity in try-cancel/abandon mode). They queue here.
    pending_activations: PendingActivations,
    /// Workflows (by run id) for which the last task completion we sent was a failure
    workflows_last_task_failed: DashSet<String>,
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
    /// Used to wake blocked workflow task polling when tasks complete
    workflow_task_complete_notify: Arc<Notify>,
    /// If true, evict workflows after they complete each workflow task
    evict_after_wft_complete: bool,
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

impl WorkflowTaskManager {
    pub fn new(workflow_task_complete_notify: Arc<Notify>, evict_after_wft_complete: bool) -> Self {
        Self {
            workflow_machines: WorkflowConcurrencyManager::new(),
            workflow_task_tokens: Default::default(),
            pending_activations: Default::default(),
            workflows_last_task_failed: Default::default(),
            outstanding_workflow_tasks: Default::default(),
            ready_buffered_wft: Default::default(),
            workflow_task_complete_notify,
            evict_after_wft_complete,
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

    pub fn outstanding_wft(&self) -> usize {
        self.outstanding_workflow_tasks.len()
    }

    pub fn run_id_for_task(&self, task_token: &TaskToken) -> Option<String> {
        self.workflow_task_tokens
            .get(task_token)
            .map(|r| r.value().clone())
    }

    /// Evict a workflow from the cache by its task token
    pub fn evict_run(&self, task_token: &TaskToken) {
        if let Some((_, run_id)) = self.workflow_task_tokens.remove(task_token) {
            debug!(run_id=%run_id, "Evicting run");
            let maybe_buffered = self.outstanding_workflow_tasks.remove(&run_id);
            self.workflow_machines.evict(&run_id);
            self.pending_activations.remove_all_with_run_id(&run_id);
            // Queue up an eviction activation
            self.pending_activations
                .push(create_evict_activation(task_token.to_owned(), run_id));
            self.workflow_task_complete_notify.notify_waiters();
            // If we just evicted something and there was a buffered poll response for the workflow,
            // it is now ready to be produced by the next poll. (Not immediate next, since, ignoring
            // other workflows, the next poll will be the eviction we just produced. Buffered polls
            // always are popped after pending activations)
            if let Some(buffered_poll) = maybe_buffered.and_then(|mb| mb.1) {
                self.ready_buffered_wft.push(buffered_poll);
            }
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

        let task_token = work.task_token.clone();
        let next_activation = self.instantiate_or_update_workflow(work)?;

        if let Some(na) = next_activation {
            self.outstanding_workflow_tasks
                .insert(na.run_id().to_owned(), None);
            return Ok(NewWfTaskOutcome::IssueActivation(na.finalize(task_token)));
        } else {
            Ok(NewWfTaskOutcome::Autocomplete)
        }
    }

    /// Record a successful activation. Returns (if any) commands that should be reported to the
    /// server as part of wft completion
    pub(crate) fn successful_activation(
        &self,
        run_id: &str,
        task_token: TaskToken,
        commands: Vec<WFCommand>,
    ) -> Result<Option<OutgoingServerCommands>, WorkflowUpdateError> {
        self.push_lang_commands(run_id, commands)?;
        self.enqueue_next_activation_if_needed(run_id, task_token.clone())?;
        // We want to fetch the outgoing commands only after any new activation has been queued,
        // as doing so may have altered the outgoing commands.
        let server_cmds = self.access_wf_machine(run_id, |w| Ok(w.get_server_commands()))?;
        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are at the final workflow task (IE: the lang SDK has caught up on
        // replay)
        if !self.pending_activations.has_pending(run_id) && server_cmds.at_final_workflow_task {
            // Since we're telling the server about a wft success, we can remove it from the
            // last failed map (if it was present)
            self.workflows_last_task_failed.remove(run_id);
            return Ok(Some(server_cmds));
        }
        self.after_wft_report(run_id, &task_token);
        Ok(None)
    }

    /// Record that an activation failed, returns true if the failure should be reported to the
    /// server
    pub fn failed_activation(&self, run_id: &str, task_token: &TaskToken) -> bool {
        // Blow up any cached data associated with the workflow
        self.evict_run(&task_token);
        // Only report to server if the last task wasn't also a failure (avoid spam)
        let should_report = !self.workflows_last_task_failed.contains(run_id);
        self.workflows_last_task_failed.insert(run_id.to_owned());
        self.after_wft_report(run_id, &task_token);
        should_report
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
        // Correlate task token w/ run ID
        self.workflow_task_tokens
            .insert(poll_wf_resp.task_token, run_id.clone());

        match self.workflow_machines.create_or_update(
            &run_id,
            poll_wf_resp.history,
            poll_wf_resp.workflow_execution,
        ) {
            Ok(activation) => Ok(activation),
            Err(source) => Err(WorkflowUpdateError { source, run_id }),
        }
    }

    // TODO: Probably inline now?
    /// Feed commands from the lang sdk into appropriate workflow manager which will iterate
    /// the state machines and return commands ready to be sent to the server
    fn push_lang_commands(
        &self,
        run_id: &str,
        cmds: Vec<WFCommand>,
    ) -> Result<(), WorkflowUpdateError> {
        self.access_wf_machine(run_id, move |mgr| mgr.push_commands(cmds))
    }

    /// Check if thew workflow run needs another activation and queue it up if there is one by
    /// pushing it into the pending activations list
    fn enqueue_next_activation_if_needed(
        &self,
        run_id: &str,
        task_token: TaskToken,
    ) -> Result<bool, WorkflowUpdateError> {
        let mut new_activation = false;
        if let Some(next_activation) =
            self.access_wf_machine(run_id, move |mgr| mgr.get_next_activation())?
        {
            self.pending_activations
                .push(next_activation.finalize(task_token));
            new_activation = true;
        }
        self.workflow_task_complete_notify.notify_waiters();
        Ok(new_activation)
    }

    /// Called after every WFT completion or failure, updates outstanding task status & issues
    /// evictions if required
    fn after_wft_report(&self, run_id: &str, task_token: &TaskToken) {
        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        if !self.pending_activations.has_pending(run_id) {
            self.outstanding_workflow_tasks.remove(run_id);

            // Blow them up if we're in non-sticky mode as well
            if self.evict_after_wft_complete {
                self.evict_run(task_token);
            }

            // The evict may or may not have already done this, but even when we aren't evicting
            // we want to remove from the run id mapping if we completed the wft.
            self.workflow_task_tokens.remove(task_token);
        }

        self.workflow_task_complete_notify.notify_waiters();
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
