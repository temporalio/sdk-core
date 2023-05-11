#[cfg(test)]
mod managed_wf_test;

#[cfg(test)]
pub(crate) use managed_wf_test::ManagedWFFunc;

use crate::{
    abstractions::dbg_panic,
    protosext::WorkflowActivationExt,
    worker::{
        workflow::{
            history_update::HistoryPaginator, machines::WorkflowMachines, ActivationAction,
            ActivationCompleteOutcome, ActivationCompleteResult, ActivationOrAuto,
            EvictionRequestResult, FailedActivationWFTReport, HeartbeatTimeoutMsg, HistoryUpdate,
            LocalActivityRequestSink, LocalResolution, NextPageReq, OutgoingServerCommands,
            OutstandingActivation, OutstandingTask, PermittedWFT, RequestEvictMsg, RunBasics,
            ServerCommandsWithWorkflowInfo, WFCommand, WFMachinesError, WFTReportStatus,
            WorkflowBridge, WorkflowTaskInfo, WFT_HEARTBEAT_TIMEOUT_FRACTION,
        },
        LocalActRequest, LEGACY_QUERY_ID,
    },
    MetricsContext,
};
use futures_util::future::AbortHandle;
use std::{
    collections::HashSet,
    ops::Add,
    rc::Rc,
    sync::mpsc::Sender,
    time::{Duration, Instant},
};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            create_evict_activation, query_to_job, remove_from_cache::EvictionReason,
            workflow_activation_job, RemoveFromCache, WorkflowActivation,
        },
        workflow_commands::QueryResult,
        workflow_completion,
    },
    temporal::api::{enums::v1::WorkflowTaskFailedCause, failure::v1::Failure},
    TaskToken,
};
use tokio::sync::oneshot;
use tracing::Span;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;
pub(super) type RunUpdateAct = Option<ActivationOrAuto>;

/// Manages access to a specific workflow run. Everything inside is entirely synchronous and should
/// remain that way.
#[derive(derive_more::DebugCustom)]
#[debug(
    fmt = "ManagedRun {{ wft: {:?}, activation: {:?}, buffered_resp: {:?} \
           trying_to_evict: {} }}",
    wft,
    activation,
    buffered_resp,
    "trying_to_evict.is_some()"
)]
pub(super) struct ManagedRun {
    wfm: WorkflowManager,
    /// Called when the machines need to produce local activity requests. This can't be lifted up
    /// easily as return values, because sometimes local activity requests trigger immediate
    /// resolutions (ex: too many attempts). Thus lifting it up creates a lot of unneeded complexity
    /// pushing things out and then directly back in. The downside is this is the only "impure" part
    /// of the in/out nature of workflow state management. If there's ever a sensible way to lift it
    /// up, that'd be nice.
    local_activity_request_sink: Rc<dyn LocalActivityRequestSink>,
    /// Set if the run is currently waiting on the execution of some local activities.
    waiting_on_la: Option<WaitingOnLAs>,
    /// Is set to true if the machines encounter an error and the only subsequent thing we should
    /// do is be evicted.
    am_broken: bool,
    /// If set, the WFT this run is currently/will be processing.
    wft: Option<OutstandingTask>,
    /// An outstanding activation to lang
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<PermittedWFT>,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,

    /// We track if we have recorded useful debugging values onto a certain span yet, to overcome
    /// duplicating field values. Remove this once https://github.com/tokio-rs/tracing/issues/2334
    /// is fixed.
    recorded_span_ids: HashSet<tracing::Id>,
    metrics: MetricsContext,
    /// We store the paginator used for our own run's history fetching
    paginator: Option<HistoryPaginator>,
    completion_waiting_on_page_fetch: Option<RunActivationCompletion>,
}
impl ManagedRun {
    pub(super) fn new(
        basics: RunBasics,
        local_activity_request_sink: Rc<dyn LocalActivityRequestSink>,
    ) -> Self {
        let metrics = basics.metrics.clone();
        let wfm = WorkflowManager::new(basics);
        Self {
            wfm,
            local_activity_request_sink,
            waiting_on_la: None,
            am_broken: false,
            wft: None,
            activation: None,
            buffered_resp: None,
            trying_to_evict: None,
            recorded_span_ids: Default::default(),
            metrics,
            paginator: None,
            completion_waiting_on_page_fetch: None,
        }
    }

    /// Returns true if there are pending jobs that need to be sent to lang.
    pub(super) fn more_pending_work(&self) -> bool {
        // We don't want to consider there to be more local-only work to be done if there is
        // no workflow task associated with the run right now. This can happen if, ex, we
        // complete a local activity while waiting for server to send us the next WFT.
        // Activating lang would be harmful at this stage, as there might be work returned
        // in that next WFT which should be part of the next activation.
        self.wft.is_some() && self.wfm.machines.has_pending_jobs()
    }

    pub(super) fn have_seen_terminal_event(&self) -> bool {
        self.wfm.machines.have_seen_terminal_event
    }

    /// Returns a ref to info about the currently tracked workflow task, if any.
    pub(super) fn wft(&self) -> Option<&OutstandingTask> {
        self.wft.as_ref()
    }

    /// Returns a ref to info about the currently tracked workflow activation, if any.
    pub(super) fn activation(&self) -> Option<&OutstandingActivation> {
        self.activation.as_ref()
    }

    /// Returns true if this run has already been told it will be evicted.
    pub(super) fn is_trying_to_evict(&self) -> bool {
        self.trying_to_evict.is_some()
    }

    /// Called whenever a new workflow task is obtained for this run
    pub(super) fn incoming_wft(&mut self, pwft: PermittedWFT) -> RunUpdateAct {
        let res = self._incoming_wft(pwft);
        self.update_to_acts(res.map(Into::into), true)
    }

    fn _incoming_wft(
        &mut self,
        pwft: PermittedWFT,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        if self.wft.is_some() {
            dbg_panic!("Trying to send a new WFT for a run which already has one!");
        }
        let start_time = Instant::now();

        let work = pwft.work;
        let did_miss_cache = !work.is_incremental() || !work.update.is_real();
        debug!(
            run_id = %work.execution.run_id,
            task_token = %&work.task_token,
            update = ?work.update,
            has_legacy_query = %work.legacy_query.is_some(),
            attempt = %work.attempt,
            "Applying new workflow task from server"
        );
        let wft_info = WorkflowTaskInfo {
            attempt: work.attempt,
            task_token: work.task_token,
            wf_id: work.execution.workflow_id.clone(),
        };

        let legacy_query_from_poll = work
            .legacy_query
            .map(|q| query_to_job(LEGACY_QUERY_ID.to_string(), q));

        let mut pending_queries = work.query_requests;
        if !pending_queries.is_empty() && legacy_query_from_poll.is_some() {
            error!(
                "Server issued both normal and legacy queries. This should not happen. Please \
                 file a bug report."
            );
            return Err(RunUpdateErr {
                source: WFMachinesError::Fatal(
                    "Server issued both normal and legacy query".to_string(),
                ),
                complete_resp_chan: None,
            });
        }
        if let Some(lq) = legacy_query_from_poll {
            pending_queries.push(lq);
        }

        self.paginator = Some(pwft.paginator);
        self.wft = Some(OutstandingTask {
            info: wft_info,
            hit_cache: !did_miss_cache,
            pending_queries,
            start_time,
            permit: pwft.permit,
        });

        // The update field is only populated in the event we hit the cache
        let activation = if work.update.is_real() {
            self.metrics.sticky_cache_hit();
            self.wfm.feed_history_from_server(work.update)?
        } else {
            let r = self.wfm.get_next_activation()?;
            if r.jobs.is_empty() {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "Machines created for {} with no jobs",
                        self.wfm.machines.run_id
                    )),
                    complete_resp_chan: None,
                });
            }
            r
        };

        if activation.jobs.is_empty() {
            if self.wfm.machines.outstanding_local_activity_count() > 0 {
                // If the activation has no jobs but there are outstanding LAs, we need to restart
                // the WFT heartbeat.
                if let Some(ref mut lawait) = self.waiting_on_la {
                    if lawait.completion_dat.is_some() {
                        panic!("Should not have completion dat when getting new wft & empty jobs")
                    }
                    lawait.hb_timeout_handle.abort();
                    lawait.hb_timeout_handle = sink_heartbeat_timeout_start(
                        self.wfm.machines.run_id.clone(),
                        self.local_activity_request_sink.as_ref(),
                        start_time,
                        lawait.wft_timeout,
                    );
                    // No activation needs to be sent to lang. We just need to wait for another
                    // heartbeat timeout or LAs to resolve
                    return Ok(None);
                } else {
                    panic!(
                        "Got a new WFT while there are outstanding local activities, but there \
                     was no waiting on LA info."
                    )
                }
            } else {
                return Ok(Some(ActivationOrAuto::Autocomplete {
                    run_id: self.wfm.machines.run_id.clone(),
                }));
            }
        }

        Ok(Some(ActivationOrAuto::LangActivation(activation)))
    }

    /// Deletes the currently tracked WFT & records latency metrics. Should be called after it has
    /// been responded to (server has been told). Returns the WFT if there was one.
    pub(super) fn mark_wft_complete(
        &mut self,
        report_status: WFTReportStatus,
    ) -> Option<OutstandingTask> {
        debug!("Marking WFT completed");
        let retme = self.wft.take();

        // Only record latency metrics if we genuinely reported to server
        if matches!(report_status, WFTReportStatus::Reported) {
            if let Some(ot) = &retme {
                self.metrics.wf_task_latency(ot.start_time.elapsed());
            }
            // Tell the LA manager that we're done with the WFT
            self.local_activity_request_sink.sink_reqs(vec![
                LocalActRequest::IndicateWorkflowTaskCompleted(self.wfm.machines.run_id.clone()),
            ]);
        }

        retme
    }

    /// Checks if any further activations need to go out for this run and produces them if so.
    pub(super) fn check_more_activations(&mut self) -> RunUpdateAct {
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into), false)
    }

    fn _check_more_activations(&mut self) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        // No point in checking for more activations if there's already an outstanding activation.
        if self.activation.is_some() {
            return Ok(None);
        }
        // In the event it's time to evict this run, cancel any outstanding LAs
        if self.trying_to_evict.is_some() {
            self.sink_la_requests(vec![LocalActRequest::CancelAllInRun(
                self.wfm.machines.run_id.clone(),
            )])?;
        }

        if self.wft.is_none() {
            // It doesn't make sense to do workflow work unless we have a WFT
            return Ok(None);
        }

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(ActivationOrAuto::LangActivation(
                self.wfm.get_next_activation()?,
            )))
        } else {
            if !self.am_broken {
                let has_pending_queries = self
                    .wft
                    .as_ref()
                    .map(|wft| !wft.pending_queries.is_empty())
                    .unwrap_or_default();
                if has_pending_queries {
                    return Ok(Some(ActivationOrAuto::ReadyForQueries(
                        self.wfm.machines.get_wf_activation(),
                    )));
                }
            }
            if let Some(wte) = self.trying_to_evict.clone() {
                let mut act = self.wfm.machines.get_wf_activation();
                // No other jobs make any sense to send if we encountered an error.
                if self.am_broken {
                    act.jobs = vec![];
                }
                act.append_evict_job(RemoveFromCache {
                    message: wte.message,
                    reason: wte.reason as i32,
                });
                Ok(Some(ActivationOrAuto::LangActivation(act)))
            } else {
                Ok(None)
            }
        }
    }

    /// Called whenever lang successfully completes a workflow activation. Commands produced by the
    /// activation are passed in. `resp_chan` will be used to unblock the completion call when
    /// everything we need to do to fulfill it has happened.
    ///
    /// Can return an error in the event that another page of history needs to be fetched before
    /// the completion can proceed.
    pub(super) fn successful_completion(
        &mut self,
        mut commands: Vec<WFCommand>,
        used_flags: Vec<u32>,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) -> Result<RunUpdateAct, NextPageReq> {
        let activation_was_only_eviction = self.activation_has_only_eviction();
        let (task_token, has_pending_query, start_time) = if let Some(entry) = self.wft.as_ref() {
            (
                entry.info.task_token.clone(),
                !entry.pending_queries.is_empty(),
                entry.start_time,
            )
        } else {
            if !activation_was_only_eviction {
                // Not an error if this was an eviction, since it's normal to issue eviction
                // activations without an associated workflow task in that case.
                dbg_panic!(
                    "Attempted to complete activation for run {} without associated workflow task",
                    self.run_id()
                );
            }
            let outcome = if let Some((tt, reason)) = self.trying_to_evict.as_mut().and_then(|te| {
                te.auto_reply_fail_tt
                    .take()
                    .map(|tt| (tt, te.message.clone()))
            }) {
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                    tt,
                    WorkflowTaskFailedCause::Unspecified,
                    Failure::application_failure(reason, true).into(),
                ))
            } else {
                ActivationCompleteOutcome::DoNothing
            };
            self.reply_to_complete(outcome, resp_chan);
            return Ok(None);
        };

        // If the only command from the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        if matches!(&commands.as_slice(),
                    &[WFCommand::QueryResponse(qr)] if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand::QueryResponse(qr) => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            self.reply_to_complete(
                ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery {
                        result: Box::new(qr),
                    },
                }),
                resp_chan,
            );
            Ok(None)
        } else {
            // First strip out query responses from other commands that actually affect machines
            // Would be prettier with `drain_filter`
            let mut i = 0;
            let mut query_responses = vec![];
            while i < commands.len() {
                if matches!(commands[i], WFCommand::QueryResponse(_)) {
                    if let WFCommand::QueryResponse(qr) = commands.remove(i) {
                        query_responses.push(qr);
                    }
                } else {
                    i += 1;
                }
            }

            if activation_was_only_eviction && !commands.is_empty() {
                dbg_panic!("Reply to an eviction only containing an eviction included commands");
            }

            let rac = RunActivationCompletion {
                task_token,
                start_time,
                commands,
                activation_was_eviction: self.activation_has_eviction(),
                activation_was_only_eviction,
                has_pending_query,
                query_responses,
                used_flags,
                resp_chan,
            };

            // Verify we can actually apply the next workflow task, which will happen as part of
            // applying the completion to machines. If we can't, return early indicating we need
            // to fetch a page.
            if !self.wfm.ready_to_apply_next_wft() {
                return if let Some(paginator) = self.paginator.take() {
                    debug!("Need to fetch a history page before next WFT can be applied");
                    self.completion_waiting_on_page_fetch = Some(rac);
                    Err(NextPageReq {
                        paginator,
                        span: Span::current(),
                    })
                } else {
                    Ok(self.update_to_acts(
                        Err(RunUpdateErr {
                            source: WFMachinesError::Fatal(
                                "Run's paginator was absent when attempting to fetch next history \
                                page. This is a Core SDK bug."
                                    .to_string(),
                            ),
                            complete_resp_chan: rac.resp_chan,
                        }),
                        false,
                    ))
                };
            }

            Ok(self.process_completion(rac))
        }
    }

    /// Called after the higher-up machinery has fetched more pages of event history needed to apply
    /// the next workflow task. The history update and paginator used to perform the fetch are
    /// passed in, with the update being used to apply the task, and the paginator stored to be
    /// attached with another fetch request if needed.
    pub(super) fn fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> RunUpdateAct {
        let res = self._fetched_page_completion(update, paginator);
        self.update_to_acts(res.map(Into::into), false)
    }
    fn _fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        self.paginator = Some(paginator);
        if let Some(d) = self.completion_waiting_on_page_fetch.take() {
            self._process_completion(d, Some(update))
        } else {
            dbg_panic!(
                "Shouldn't be possible to be applying a next-page-fetch update when \
                        doing anything other than completing an activation."
            );
            Err(RunUpdateErr::from(WFMachinesError::Fatal(
                "Tried to apply next-page-fetch update to a run that wasn't handling a completion"
                    .to_string(),
            )))
        }
    }

    /// Called whenever either core lang cannot complete a workflow activation. EX: Nondeterminism
    /// or user code threw/panicked, respectively. The `cause` and `reason` fields are determined
    /// inside core always. The `failure` field may come from lang. `resp_chan` will be used to
    /// unblock the completion call when everything we need to do to fulfill it has happened.
    pub(super) fn failed_completion(
        &mut self,
        cause: WorkflowTaskFailedCause,
        reason: EvictionReason,
        failure: workflow_completion::Failure,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) -> RunUpdateAct {
        let tt = if let Some(tt) = self.wft.as_ref().map(|t| t.info.task_token.clone()) {
            tt
        } else {
            dbg_panic!(
                "No workflow task for run id {} found when trying to fail activation",
                self.run_id()
            );
            self.reply_to_complete(ActivationCompleteOutcome::DoNothing, resp_chan);
            return None;
        };

        self.metrics.wf_task_failed();
        let message = format!("Workflow activation completion failed: {:?}", &failure);
        // Blow up any cached data associated with the workflow
        let evict_req_outcome = self.request_eviction(RequestEvictMsg {
            run_id: self.run_id().to_string(),
            message,
            reason,
            auto_reply_fail_tt: None,
        });
        let should_report = match &evict_req_outcome {
            EvictionRequestResult::EvictionRequested(Some(attempt), _)
            | EvictionRequestResult::EvictionAlreadyRequested(Some(attempt)) => *attempt <= 1,
            _ => false,
        };
        let rur = evict_req_outcome.into_run_update_resp();
        // If the outstanding WFT is a legacy query task, report that we need to fail it
        let outcome = if self.pending_work_is_legacy_query() {
            ActivationCompleteOutcome::ReportWFTFail(
                FailedActivationWFTReport::ReportLegacyQueryFailure(tt, failure),
            )
        } else if should_report {
            ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                tt, cause, failure,
            ))
        } else {
            ActivationCompleteOutcome::WFTFailedDontReport
        };
        self.reply_to_complete(outcome, resp_chan);
        rur
    }

    /// Delete the currently tracked workflow activation and return it, if any. Should be called
    /// after the processing of the activation completion, and WFT reporting.
    pub(super) fn delete_activation(
        &mut self,
        pred: impl FnOnce(&OutstandingActivation) -> bool,
    ) -> Option<OutstandingActivation> {
        if self.activation().map(pred).unwrap_or_default() {
            self.activation.take()
        } else {
            None
        }
    }

    /// Called when local activities resolve
    pub(super) fn local_resolution(&mut self, res: LocalResolution) -> RunUpdateAct {
        let res = self._local_resolution(res);
        self.update_to_acts(res.map(Into::into), false)
    }

    fn process_completion(&mut self, completion: RunActivationCompletion) -> RunUpdateAct {
        let res = self._process_completion(completion, None);
        self.update_to_acts(res.map(Into::into), false)
    }

    fn _process_completion(
        &mut self,
        completion: RunActivationCompletion,
        new_update: Option<HistoryUpdate>,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        let data = CompletionDataForWFT {
            task_token: completion.task_token,
            query_responses: completion.query_responses,
            has_pending_query: completion.has_pending_query,
            activation_was_only_eviction: completion.activation_was_only_eviction,
        };

        self.wfm.machines.add_lang_used_flags(completion.used_flags);

        // If this is just bookkeeping after a reply to an only-eviction activation, we can bypass
        // everything, since there is no reason to continue trying to update machines.
        if completion.activation_was_only_eviction {
            return Ok(Some(self.prepare_complete_resp(
                completion.resp_chan,
                data,
                false,
            )));
        }

        let outcome = (|| {
            // Send commands from lang into the machines then check if the workflow run needs
            // another activation and mark it if so
            self.wfm.push_commands_and_iterate(completion.commands)?;
            // If there was a new update included as part of the completion, apply it.
            if let Some(update) = new_update {
                self.wfm.feed_history_from_new_page(update)?;
            }
            // Don't bother applying the next task if we're evicting at the end of this activation
            if !completion.activation_was_eviction {
                self.wfm.apply_next_task_if_ready()?;
            }
            let new_local_acts = self.wfm.drain_queued_local_activities();
            self.sink_la_requests(new_local_acts)?;

            if self.wfm.machines.outstanding_local_activity_count() == 0 {
                Ok(None)
            } else {
                let wft_timeout: Duration = self
                    .wfm
                    .machines
                    .get_started_info()
                    .and_then(|attrs| attrs.workflow_task_timeout)
                    .ok_or_else(|| {
                        WFMachinesError::Fatal(
                            "Workflow's start attribs were missing a well formed task timeout"
                                .to_string(),
                        )
                    })?;
                Ok(Some((completion.start_time, wft_timeout)))
            }
        })();

        match outcome {
            Ok(None) => Ok(Some(self.prepare_complete_resp(
                completion.resp_chan,
                data,
                false,
            ))),
            Ok(Some((start_t, wft_timeout))) => {
                if let Some(wola) = self.waiting_on_la.as_mut() {
                    wola.hb_timeout_handle.abort();
                }
                self.waiting_on_la = Some(WaitingOnLAs {
                    wft_timeout,
                    completion_dat: Some((data, completion.resp_chan)),
                    hb_timeout_handle: sink_heartbeat_timeout_start(
                        self.run_id().to_string(),
                        self.local_activity_request_sink.as_ref(),
                        start_t,
                        wft_timeout,
                    ),
                });
                Ok(None)
            }
            Err(e) => Err(RunUpdateErr {
                source: e,
                complete_resp_chan: completion.resp_chan,
            }),
        }
    }

    fn _local_resolution(
        &mut self,
        res: LocalResolution,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        debug!(resolution=?res, "Applying local resolution");
        self.wfm.notify_of_local_result(res)?;
        if self.wfm.machines.outstanding_local_activity_count() == 0 {
            if let Some(mut wait_dat) = self.waiting_on_la.take() {
                // Cancel the heartbeat timeout
                wait_dat.hb_timeout_handle.abort();
                if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                    return Ok(Some(self.prepare_complete_resp(
                        resp_chan,
                        completion_dat,
                        false,
                    )));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn heartbeat_timeout(&mut self) -> RunUpdateAct {
        let maybe_act = if self._heartbeat_timeout() {
            Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            })
        } else {
            None
        };
        self.update_to_acts(Ok(maybe_act).map(Into::into), false)
    }
    /// Returns `true` if autocompletion should be issued, which will actually cause us to end up
    /// in [completion] again, at which point we'll start a new heartbeat timeout, which will
    /// immediately trigger and thus finish the completion, forcing a new task as it should.
    fn _heartbeat_timeout(&mut self) -> bool {
        if let Some(ref mut wait_dat) = self.waiting_on_la {
            // Cancel the heartbeat timeout
            wait_dat.hb_timeout_handle.abort();
            if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                let compl = self.prepare_complete_resp(resp_chan, completion_dat, true);
                // Immediately fulfill the completion since the run update will already have
                // been replied to
                compl.fulfill();
            } else {
                // Auto-reply WFT complete
                return true;
            }
        }
        false
    }

    /// Returns true if the managed run has any form of pending work
    /// If `ignore_evicts` is true, pending evictions do not count as pending work.
    /// If `ignore_buffered` is true, buffered workflow tasks do not count as pending work.
    pub(super) fn has_any_pending_work(&self, ignore_evicts: bool, ignore_buffered: bool) -> bool {
        let evict_work = if ignore_evicts {
            false
        } else {
            self.trying_to_evict.is_some()
        };
        let act_work = if ignore_evicts {
            if let Some(ref act) = self.activation {
                !act.has_only_eviction()
            } else {
                false
            }
        } else {
            self.activation.is_some()
        };
        let buffered = if ignore_buffered {
            false
        } else {
            self.buffered_resp.is_some()
        };
        trace!(wft=self.wft.is_some(), buffered=?buffered, more_work=?self.more_pending_work(),
               act_work, evict_work, "Does run have pending work?");
        self.wft.is_some() || buffered || self.more_pending_work() || act_work || evict_work
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    pub(super) fn buffer_wft_if_outstanding_work(
        &mut self,
        work: PermittedWFT,
    ) -> Option<PermittedWFT> {
        let about_to_issue_evict = self.trying_to_evict.is_some();
        let has_wft = self.wft().is_some();
        let has_activation = self.activation().is_some();
        if has_wft || has_activation || about_to_issue_evict || self.more_pending_work() {
            debug!(run_id = %self.run_id(),
                   "Got new WFT for a run with outstanding work, buffering it");
            self.buffered_resp = Some(work);
            None
        } else {
            Some(work)
        }
    }

    /// Returns true if there is a buffered workflow task for this run.
    pub(super) fn has_buffered_wft(&self) -> bool {
        self.buffered_resp.is_some()
    }

    /// Removes and returns the buffered workflow task, if any.
    pub(super) fn take_buffered_wft(&mut self) -> Option<PermittedWFT> {
        self.buffered_resp.take()
    }

    pub(super) fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        let attempts = self.wft.as_ref().map(|wt| wt.info.attempt);

        // If we were waiting on a page fetch and we're getting evicted because fetching failed,
        // then make sure we allow the completion to proceed, otherwise we're stuck waiting forever.
        if self.completion_waiting_on_page_fetch.is_some()
            && matches!(info.reason, EvictionReason::PaginationOrHistoryFetch)
        {
            // We just checked it is some, unwrap OK.
            let c = self.completion_waiting_on_page_fetch.take().unwrap();
            let run_upd = self.failed_completion(
                WorkflowTaskFailedCause::Unspecified,
                info.reason,
                Failure::application_failure(info.message, false).into(),
                c.resp_chan,
            );
            return EvictionRequestResult::EvictionRequested(attempts, run_upd);
        }

        if !self.activation_has_eviction() && self.trying_to_evict.is_none() {
            debug!(run_id=%info.run_id, reason=%info.message, "Eviction requested");
            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionRequested(attempts, self.check_more_activations())
        } else {
            // Always store the most recent eviction reason
            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionAlreadyRequested(attempts)
        }
    }

    pub(super) fn record_span_fields(&mut self, span: &Span) {
        if let Some(spid) = span.id() {
            if self.recorded_span_ids.contains(&spid) {
                return;
            }
            self.recorded_span_ids.insert(spid);

            if let Some(wid) = self.wft().map(|wft| &wft.info.wf_id) {
                span.record("workflow_id", wid.as_str());
            }
        }
    }

    /// Take the result of some update to ourselves and turn it into a return value of zero or more
    /// actions
    fn update_to_acts(
        &mut self,
        outcome: Result<ActOrFulfill, RunUpdateErr>,
        in_response_to_wft: bool,
    ) -> RunUpdateAct {
        match outcome {
            Ok(act_or_fulfill) => {
                let (mut maybe_act, maybe_fulfill) = match act_or_fulfill {
                    ActOrFulfill::OutgoingAct(a) => (a, None),
                    ActOrFulfill::FulfillableComplete(c) => (None, c),
                };
                // If there's no activation but is pending work, check and possibly generate one
                if self.more_pending_work() && maybe_act.is_none() {
                    match self._check_more_activations() {
                        Ok(oa) => maybe_act = oa,
                        Err(e) => {
                            return self.update_to_acts(Err(e), in_response_to_wft);
                        }
                    }
                }
                let r = match maybe_act {
                    Some(ActivationOrAuto::LangActivation(mut activation)) => {
                        if in_response_to_wft {
                            let wft = self
                                .wft
                                .as_mut()
                                .expect("WFT must exist for run just updated with one");
                            // If there are in-poll queries, insert jobs for those queries into the
                            // activation, but only if we hit the cache. If we didn't, those queries
                            // will need to be dealt with once replay is over
                            if wft.hit_cache {
                                put_queries_in_act(&mut activation, wft);
                            }
                        }

                        if activation.jobs.is_empty() {
                            dbg_panic!("Should not send lang activation with no jobs");
                        }
                        Some(ActivationOrAuto::LangActivation(activation))
                    }
                    Some(ActivationOrAuto::ReadyForQueries(mut act)) => {
                        if let Some(wft) = self.wft.as_mut() {
                            put_queries_in_act(&mut act, wft);
                            Some(ActivationOrAuto::LangActivation(act))
                        } else {
                            dbg_panic!("Ready for queries but no WFT!");
                            None
                        }
                    }
                    a @ Some(
                        ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. },
                    ) => a,
                    None => {
                        if let Some(reason) = self.trying_to_evict.as_ref() {
                            // If we had nothing to do, but we're trying to evict, just do that now
                            // as long as there's no other outstanding work.
                            if self.activation.is_none() && !self.more_pending_work() {
                                let mut evict_act = create_evict_activation(
                                    self.run_id().to_string(),
                                    reason.message.clone(),
                                    reason.reason,
                                );
                                evict_act.history_length =
                                    self.most_recently_processed_event_number() as u32;
                                Some(ActivationOrAuto::LangActivation(evict_act))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };
                if let Some(f) = maybe_fulfill {
                    f.fulfill();
                }

                match r {
                    // After each run update, check if it's ready to handle any buffered poll
                    None | Some(ActivationOrAuto::Autocomplete { .. })
                        if !self.has_any_pending_work(false, true) =>
                    {
                        if let Some(bufft) = self.buffered_resp.take() {
                            self.incoming_wft(bufft)
                        } else {
                            None
                        }
                    }
                    Some(r) => {
                        self.insert_outstanding_activation(&r);
                        Some(r)
                    }
                    None => None,
                }
            }
            Err(fail) => {
                self.am_broken = true;
                let rur = if let Some(resp_chan) = fail.complete_resp_chan {
                    // Automatically fail the workflow task in the event we couldn't update machines
                    let fail_cause = if matches!(&fail.source, WFMachinesError::Nondeterminism(_)) {
                        WorkflowTaskFailedCause::NonDeterministicError
                    } else {
                        WorkflowTaskFailedCause::Unspecified
                    };
                    let wft_fail_str = format!("{:?}", fail.source);
                    self.failed_completion(
                        fail_cause,
                        fail.source.evict_reason(),
                        Failure::application_failure(wft_fail_str, false).into(),
                        Some(resp_chan),
                    )
                } else {
                    warn!(error=?fail.source, "Error while updating workflow");
                    Some(ActivationOrAuto::AutoFail {
                        run_id: self.run_id().to_owned(),
                        machines_err: fail.source,
                    })
                };
                rur
            }
        }
    }

    fn insert_outstanding_activation(&mut self, act: &ActivationOrAuto) {
        let act_type = match &act {
            ActivationOrAuto::LangActivation(act) | ActivationOrAuto::ReadyForQueries(act) => {
                if act.is_legacy_query() {
                    OutstandingActivation::LegacyQuery
                } else {
                    OutstandingActivation::Normal {
                        contains_eviction: act.eviction_index().is_some(),
                        num_jobs: act.jobs.len(),
                    }
                }
            }
            ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. } => {
                OutstandingActivation::Autocomplete
            }
        };
        if let Some(old_act) = self.activation {
            // This is a panic because we have screwed up core logic if this is violated. It must be
            // upheld.
            panic!(
                "Attempted to insert a new outstanding activation {act:?}, but there already was \
                 one outstanding: {old_act:?}"
            );
        }
        self.activation = Some(act_type);
    }

    fn prepare_complete_resp(
        &mut self,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
        data: CompletionDataForWFT,
        due_to_heartbeat_timeout: bool,
    ) -> FulfillableActivationComplete {
        let mut outgoing_cmds = self.wfm.get_server_commands();
        if data.activation_was_only_eviction && !outgoing_cmds.commands.is_empty() {
            if self.am_broken {
                // If we broke there could be commands in the pipe that we didn't get a chance to
                // handle properly during replay, just wipe them all out.
                outgoing_cmds.commands = vec![];
            } else {
                dbg_panic!(
                "There should not be any outgoing commands when preparing a completion response \
                 if the activation was only an eviction. This is an SDK bug."
                );
            }
        }

        let query_responses = data.query_responses;
        let has_query_responses = !query_responses.is_empty();
        let is_query_playback = data.has_pending_query && !has_query_responses;
        let mut force_new_wft = due_to_heartbeat_timeout;

        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are caught up on replay. We don't want to complete a wft if we already
        // saw the final event in the workflow, or if we are playing back for the express purpose of
        // fulfilling a query. If the activation we sent was *only* an eviction, don't send that
        // either.
        let should_respond = !(self.wfm.machines.has_pending_jobs()
            || outgoing_cmds.replaying
            || is_query_playback
            || data.activation_was_only_eviction
            || self.wfm.machines.have_seen_terminal_event);
        // If there are pending LA resolutions, and we're responding to a query here,
        // we want to make sure to force a new task, as otherwise once we tell lang about
        // the LA resolution there wouldn't be any task to reply to with the result of iterating
        // the workflow.
        if has_query_responses && self.wfm.machines.has_pending_la_resolutions() {
            force_new_wft = true;
        }

        let outcome = if should_respond || has_query_responses {
            ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                task_token: data.task_token,
                action: ActivationAction::WftComplete {
                    force_new_wft,
                    commands: outgoing_cmds.commands,
                    query_responses,
                    sdk_metadata: self.wfm.machines.get_metadata_for_wft_complete(),
                },
            })
        } else {
            ActivationCompleteOutcome::DoNothing
        };
        FulfillableActivationComplete {
            result: self.build_activation_complete_result(outcome),
            resp_chan,
        }
    }

    /// Pump some local activity requests into the sink, applying any immediate results to the
    /// workflow machines.
    fn sink_la_requests(
        &mut self,
        new_local_acts: Vec<LocalActRequest>,
    ) -> Result<(), WFMachinesError> {
        let immediate_resolutions = self.local_activity_request_sink.sink_reqs(new_local_acts);
        for resolution in immediate_resolutions {
            self.wfm
                .notify_of_local_result(LocalResolution::LocalActivity(resolution))?;
        }
        Ok(())
    }

    fn reply_to_complete(
        &mut self,
        outcome: ActivationCompleteOutcome,
        chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) {
        if let Some(chan) = chan {
            if chan
                .send(self.build_activation_complete_result(outcome))
                .is_err()
            {
                let warnstr = "The workflow task completer went missing! This likely indicates an \
                               SDK bug, please report."
                    .to_string();
                warn!(run_id=%self.run_id(), "{}", warnstr);
                self.request_eviction(RequestEvictMsg {
                    run_id: self.run_id().to_string(),
                    message: warnstr,
                    reason: EvictionReason::Fatal,
                    auto_reply_fail_tt: None,
                });
            }
        }
    }

    fn build_activation_complete_result(
        &self,
        outcome: ActivationCompleteOutcome,
    ) -> ActivationCompleteResult {
        ActivationCompleteResult {
            outcome,
            most_recently_processed_event: self.most_recently_processed_event_number() as usize,
            replaying: self.wfm.machines.replaying,
        }
    }

    /// Returns true if the handle is currently processing a WFT which contains a legacy query.
    fn pending_work_is_legacy_query(&self) -> bool {
        // Either we know because there is a pending legacy query, or it's already been drained and
        // sent as an activation.
        matches!(self.activation, Some(OutstandingActivation::LegacyQuery))
            || self
                .wft
                .as_ref()
                .map(|t| t.has_pending_legacy_query())
                .unwrap_or_default()
    }

    fn most_recently_processed_event_number(&self) -> i64 {
        self.wfm.machines.last_processed_event
    }

    fn activation_has_eviction(&mut self) -> bool {
        self.activation
            .map(OutstandingActivation::has_eviction)
            .unwrap_or_default()
    }

    fn activation_has_only_eviction(&mut self) -> bool {
        self.activation
            .map(OutstandingActivation::has_only_eviction)
            .unwrap_or_default()
    }

    fn run_id(&self) -> &str {
        &self.wfm.machines.run_id
    }
}

/// Drains pending queries from the workflow task and appends them to the activation's jobs
fn put_queries_in_act(act: &mut WorkflowActivation, wft: &mut OutstandingTask) {
    // Nothing to do if there are no pending queries
    if wft.pending_queries.is_empty() {
        return;
    }

    let has_legacy = wft.has_pending_legacy_query();
    // Cannot dispatch legacy query if there are any other jobs - which can happen if, ex, a local
    // activity resolves while we've gotten a legacy query after heartbeating.
    if has_legacy && !act.jobs.is_empty() {
        return;
    }

    debug!(queries=?wft.pending_queries, "Dispatching queries");
    let query_jobs = wft
        .pending_queries
        .drain(..)
        .map(|q| workflow_activation_job::Variant::QueryWorkflow(q).into());
    act.jobs.extend(query_jobs);
}
fn sink_heartbeat_timeout_start(
    run_id: String,
    sink: &dyn LocalActivityRequestSink,
    wft_start_time: Instant,
    wft_timeout: Duration,
) -> AbortHandle {
    // The heartbeat deadline is 80% of the WFT timeout
    let deadline = wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
    let (abort_handle, abort_reg) = AbortHandle::new_pair();
    sink.sink_reqs(vec![LocalActRequest::StartHeartbeatTimeout {
        send_on_elapse: HeartbeatTimeoutMsg {
            run_id,
            span: Span::current(),
        },
        deadline,
        abort_reg,
    }]);
    abort_handle
}

/// If an activation completion needed to wait on LA completions (or heartbeat timeout) we use
/// this struct to store the data we need to finish the completion once that has happened
struct WaitingOnLAs {
    wft_timeout: Duration,
    /// If set, we are waiting for LAs to complete as part of a just-finished workflow activation.
    /// If unset, we already had a heartbeat timeout and got a new WFT without any new work while
    /// there are still incomplete LAs.
    completion_dat: Option<(
        CompletionDataForWFT,
        Option<oneshot::Sender<ActivationCompleteResult>>,
    )>,
    /// Can be used to abort heartbeat timeouts
    hb_timeout_handle: AbortHandle,
}
#[derive(Debug)]
struct CompletionDataForWFT {
    task_token: TaskToken,
    query_responses: Vec<QueryResult>,
    has_pending_query: bool,
    activation_was_only_eviction: bool,
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
struct WorkflowManager {
    machines: WorkflowMachines,
    /// Is always `Some` in normal operation. Optional to allow for unit testing with the test
    /// workflow driver, which does not need to complete activations the normal way.
    command_sink: Option<Sender<Vec<WFCommand>>>,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    fn new(basics: RunBasics) -> Self {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(basics, Box::new(wfb).into());
        Self {
            machines: state_machines,
            command_sink: Some(cmd_sink),
        }
    }

    #[cfg(test)]
    const fn new_from_machines(workflow_machines: WorkflowMachines) -> Self {
        Self {
            machines: workflow_machines,
            command_sink: None,
        }
    }

    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed.
    fn feed_history_from_server(&mut self, update: HistoryUpdate) -> Result<WorkflowActivation> {
        self.machines.new_history_from_server(update)?;
        self.get_next_activation()
    }

    /// Update the machines with some events from fetching another page of history. Does *not*
    /// attempt to pull the next activation, unlike [Self::feed_history_from_server].
    fn feed_history_from_new_page(&mut self, update: HistoryUpdate) -> Result<()> {
        self.machines.new_history_from_server(update)
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<bool> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history()?;
        Ok(self.machines.get_wf_activation())
    }

    /// Returns true if machines are ready to apply the next WFT sequence, false if events will need
    /// to be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn ready_to_apply_next_wft(&self) -> bool {
        self.machines.ready_to_apply_next_wft()
    }

    /// If there are no pending jobs for the workflow, apply the next workflow task and check
    /// again if there are any jobs. Importantly, does not *drain* jobs.
    ///
    /// Returns true if there are jobs (before or after applying the next WFT).
    fn apply_next_task_if_ready(&mut self) -> Result<bool> {
        if self.machines.has_pending_jobs() {
            return Ok(true);
        }
        loop {
            let consumed_events = self.machines.apply_next_wft_from_history()?;

            if consumed_events == 0 || !self.machines.replaying || self.machines.has_pending_jobs()
            {
                // Keep applying tasks while there are events, we are still replaying, and there are
                // no jobs
                break;
            }
        }
        Ok(self.machines.has_pending_jobs())
    }

    /// Typically called after [get_next_activation], use this to retrieve commands to be sent to
    /// the server which have been generated by the machines. Does *not* drain those commands.
    /// See [WorkflowMachines::get_commands].
    fn get_server_commands(&self) -> OutgoingServerCommands {
        OutgoingServerCommands {
            commands: self.machines.get_commands(),
            replaying: self.machines.replaying,
        }
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    fn push_commands_and_iterate(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines()?;
        Ok(())
    }
}

#[derive(Debug)]
struct FulfillableActivationComplete {
    result: ActivationCompleteResult,
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}
impl FulfillableActivationComplete {
    fn fulfill(self) {
        if let Some(resp_chan) = self.resp_chan {
            let _ = resp_chan.send(self.result);
        }
    }
}

#[derive(Debug)]
struct RunActivationCompletion {
    task_token: TaskToken,
    start_time: Instant,
    commands: Vec<WFCommand>,
    activation_was_eviction: bool,
    activation_was_only_eviction: bool,
    has_pending_query: bool,
    query_responses: Vec<QueryResult>,
    used_flags: Vec<u32>,
    /// Used to notify the worker when the completion is done processing and the completion can
    /// unblock. Must always be `Some` when initialized.
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}
#[derive(Debug, derive_more::From)]
enum ActOrFulfill {
    OutgoingAct(Option<ActivationOrAuto>),
    FulfillableComplete(Option<FulfillableActivationComplete>),
}

#[derive(derive_more::DebugCustom)]
#[debug(fmt = "RunUpdateErr({source:?})")]
struct RunUpdateErr {
    source: WFMachinesError,
    complete_resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}

impl From<WFMachinesError> for RunUpdateErr {
    fn from(e: WFMachinesError) -> Self {
        RunUpdateErr {
            source: e,
            complete_resp_chan: None,
        }
    }
}
