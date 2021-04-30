type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

mod async_workflow_driver;
mod history_builder;
mod transition_coverage;

pub(super) use async_workflow_driver::{CommandSender, TestWorkflowDriver};
pub(crate) use history_builder::TestHistoryBuilder;
pub(crate) use transition_coverage::add_coverage;

use crate::{
    pollers::MockServerGatewayApis,
    protos::{
        coresdk::{
            common::UserCodeFailure,
            workflow_activation::{wf_activation_job, WfActivation, WfActivationJob},
            workflow_commands::workflow_command,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
        },
        temporal::api::common::v1::WorkflowExecution,
        temporal::api::history::v1::History,
        temporal::api::workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    task_token::TaskToken,
    Core, CoreInitOptions, CoreSDK, ServerGatewayApis, ServerGatewayOptions,
};
use bimap::BiMap;
use mockall::TimesRange;
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    ops::RangeFull,
    str::FromStr,
    sync::Arc,
};
use url::Url;

/// This wrapper around core is required when using [build_multihist_mock_sg] with [poll_and_reply].
/// It allows the latter to update the mock's understanding of workflows with outstanding tasks
/// by removing those which have been evicted.
pub(crate) struct FakeCore {
    pub(crate) inner: CoreSDK<MockServerGatewayApis>,
    pub(crate) outstanding_wf_tasks: OutstandingWfTaskMap,
}

pub type OutstandingWfTaskMap = Arc<RwLock<BiMap<String, TaskToken>>>;

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the core SDK with a mock server gateway that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned. For
/// each number in the input list, a fake response will be prepared which includes history up to the
/// workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub(crate) fn build_fake_core(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: &[usize],
) -> FakeCore {
    let mock_gateway = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wf_id.to_owned(),
            hist: t,
            response_batches: response_batches.to_vec(),
        }],
        true,
        None,
    );
    fake_core_from_mock_sg(mock_gateway)
}

/// See [build_fake_core] -- assemble a mock gateway into the final fake core
pub(crate) fn fake_core_from_mock_sg(mock_and_tasks: MockSGAndTasks) -> FakeCore {
    FakeCore {
        inner: CoreSDK::new(
            mock_and_tasks.sg,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: 5,
            },
        ),
        outstanding_wf_tasks: mock_and_tasks.task_map,
    }
}

pub(crate) fn mock_core(sg: MockServerGatewayApis) -> CoreSDK<impl ServerGatewayApis> {
    CoreSDK::new(
        sg,
        CoreInitOptions {
            gateway_opts: fake_sg_opts(),
            evict_after_pending_cleared: true,
            max_outstanding_workflow_tasks: 5,
            max_outstanding_activities: 5,
        },
    )
}

pub struct FakeWfResponses {
    pub wf_id: String,
    pub hist: TestHistoryBuilder,
    pub response_batches: Vec<usize>,
}

pub struct MockSGAndTasks {
    pub sg: MockServerGatewayApis,
    pub task_map: OutstandingWfTaskMap,
}

/// Build a mock server gateway capable of returning multiple different histories for different
/// workflows. It does so by tracking outstanding workflow tasks like is also happening in core
/// (which is unfortunately a bit redundant, we could provide hooks in core but that feels a little
/// nasty). If there is an outstanding task for a given workflow, new chunks of its history are not
/// returned. If there is not, the next batch of history is returned for any workflow without an
/// outstanding task. Outstanding tasks are cleared on completion, failure, or eviction.
///
/// `num_expected_fails` can be provided to set a specific number of expected failed workflow tasks
/// sent to the server.
pub fn build_multihist_mock_sg(
    hists: impl IntoIterator<Item = FakeWfResponses>,
    enforce_correct_number_of_polls: bool,
    num_expected_fails: Option<usize>,
) -> MockSGAndTasks {
    let mut ids_to_resps = BTreeMap::new();
    let outstanding_wf_task_tokens = Arc::new(RwLock::new(BiMap::new()));
    let mut correct_num_polls = None;

    for hist in hists {
        let full_hist_info = hist.hist.get_full_history_info().unwrap();
        // Ensure no response batch is trying to return more tasks than the history contains
        for rb_wf_num in &hist.response_batches {
            assert!(
                *rb_wf_num <= full_hist_info.wf_task_count,
                "Wf task count {} is not <= total task count {}",
                rb_wf_num,
                full_hist_info.wf_task_count
            );
        }

        if enforce_correct_number_of_polls {
            *correct_num_polls.get_or_insert(0) += hist.response_batches.len();
        }

        let responses: Vec<_> = hist
            .response_batches
            .iter()
            .map(|to_task_num| hist_to_poll_resp(&hist.hist, hist.wf_id.to_owned(), *to_task_num))
            .collect();

        let tasks = VecDeque::from(responses);
        ids_to_resps.insert(hist.wf_id, tasks);
    }

    // The gateway will return history from any workflows that do not have currently outstanding
    // tasks. Order proceeds as sorted by workflow id
    let outstanding = outstanding_wf_task_tokens.clone();

    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_workflow_task()
        .times(
            correct_num_polls
                .map::<TimesRange, _>(Into::into)
                .unwrap_or_else(|| RangeFull.into()),
        )
        .returning(move || {
            for (wfid, tasks) in ids_to_resps.iter_mut() {
                if !outstanding.read().contains_left(wfid) {
                    if let Some(t) = tasks.pop_front() {
                        outstanding
                            .write()
                            .insert(wfid.to_owned(), TaskToken(t.task_token.clone()));
                        return Ok(t);
                    }
                }
            }
            Err(tonic::Status::cancelled("No more work to do"))
        });

    let outstanding = outstanding_wf_task_tokens.clone();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(move |tt, _| {
            outstanding.write().remove_by_right(&tt);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let outstanding = outstanding_wf_task_tokens.clone();
    mock_gateway
        .expect_fail_workflow_task()
        .times(
            num_expected_fails
                .map::<TimesRange, _>(Into::into)
                .unwrap_or_else(|| RangeFull.into()),
        )
        .returning(move |tt, _, _| {
            outstanding.write().remove_by_right(&tt);
            Ok(Default::default())
        });

    MockSGAndTasks {
        sg: mock_gateway,
        task_map: outstanding_wf_task_tokens,
    }
}

pub fn hist_to_poll_resp(
    t: &TestHistoryBuilder,
    wf_id: String,
    to_task_num: usize,
) -> PollWorkflowTaskQueueResponse {
    let run_id = t.get_orig_run_id();
    let wf = WorkflowExecution {
        workflow_id: wf_id,
        run_id: run_id.to_string(),
    };
    let batch = t.get_history_info(to_task_num).unwrap().events().to_vec();
    let task_token: [u8; 16] = thread_rng().gen();
    PollWorkflowTaskQueueResponse {
        history: Some(History { events: batch }),
        workflow_execution: Some(wf),
        task_token: task_token.to_vec(),
        ..Default::default()
    }
}

pub fn fake_sg_opts() -> ServerGatewayOptions {
    ServerGatewayOptions {
        target_url: Url::from_str("https://fake").unwrap(),
        namespace: "".to_string(),
        task_queue: "task_queue".to_string(),
        identity: "".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Default::default(),
    }
}

type AsserterWithReply<'a> = (&'a dyn Fn(&WfActivation), wf_activation_completion::Status);

pub enum EvictionMode {
    #[allow(dead_code)] // Not used until we have stickyness options implemented
    /// Core is in sticky mode, and workflows are being cached
    Sticky,
    /// Core is not in sticky mode, and workflows are evicted after they finish their pending
    /// activations
    NotSticky,
    /// Not a real mode, but good for imitating crashes. Evict workflows after *every* reply,
    /// even if there are pending activations
    AfterEveryReply,
}

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed
pub(crate) async fn poll_and_reply<'a>(
    core: &'a FakeCore,
    eviction_mode: EvictionMode,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;
    let mut executed_failures = HashSet::new();

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for (i, interaction) in expect_iter.enumerate() {
            let (asserter, reply) = interaction;
            let complete_is_failure = !reply.is_success();
            // Only send activation failures once
            if executed_failures.contains(&i) {
                continue;
            }

            let mut res = core.inner.poll_workflow_task().await.unwrap();
            let contains_eviction = res.jobs.iter().position(|j| {
                matches!(
                    j,
                    WfActivationJob {
                        variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
                    }
                )
            });

            if let Some(eviction_job_ix) = contains_eviction {
                // If the job list has an eviction, make sure it was the last item in the list
                // then remove it and run the asserter against that smaller list, and then restart
                // expectations from the beginning.
                assert_eq!(
                    eviction_job_ix,
                    res.jobs.len() - 1,
                    "Eviction job was not last job in job list"
                );
                res.jobs.remove(eviction_job_ix);
                if !res.jobs.is_empty() {
                    asserter(&res);
                }
                core.outstanding_wf_tasks
                    .write()
                    .remove_by_right(&TaskToken(res.task_token));
                continue 'outer;
            }

            asserter(&res);

            let task_tok = res.task_token;

            core.inner
                .complete_workflow_task(WfActivationCompletion::from_status(
                    task_tok.clone(),
                    reply.clone(),
                ))
                .await
                .unwrap();

            if complete_is_failure {
                executed_failures.insert(i);
                // restart b/c we evicted due to failure and need to start all over again
                continue 'outer;
            }

            match eviction_mode {
                EvictionMode::Sticky => unimplemented!(),
                EvictionMode::NotSticky => (),
                EvictionMode::AfterEveryReply => {
                    if evictions < expected_evictions {
                        core.inner.evict_run(&task_tok.into());
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }
    assert!(core.inner.outstanding_workflow_tasks.is_empty());
}

pub(crate) fn gen_assert_and_reply(
    asserter: &dyn Fn(&WfActivation),
    reply_commands: Vec<workflow_command::Variant>,
) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Success::from_cmds(reply_commands).into(),
    )
}

pub(crate) fn gen_assert_and_fail(asserter: &dyn Fn(&WfActivation)) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Failure {
            failure: Some(UserCodeFailure {
                message: "Intentional test failure".to_string(),
                ..Default::default()
            }),
        }
        .into(),
    )
}

/// Generate asserts for [poll_and_reply] by passing patterns to match against the job list
#[macro_export]
macro_rules! job_assert {
    ($($pat:pat),+) => {
        |res| {
            assert_matches!(
                res.jobs.as_slice(),
                [$(WfActivationJob {
                    variant: Some($pat),
                }),+]
            );
        }
    };
}
