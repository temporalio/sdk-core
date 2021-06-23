// TODO: Move this whole thing to upper test help module
type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

pub const TEST_Q: &str = "q";

mod async_workflow_driver;
mod history_builder;
mod history_info;
mod transition_coverage;

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
        temporal::api::enums::v1::TaskQueueKind,
        temporal::api::history::v1::History,
        temporal::api::taskqueue::v1::TaskQueue,
        temporal::api::workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    task_token::TaskToken,
    workflow::WorkflowCachingPolicy,
    Core, CoreInitOptionsBuilder, CoreSDK, ServerGatewayApis, ServerGatewayOptions,
    WorkerConfigBuilder,
};
use bimap::BiMap;
use mockall::TimesRange;
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
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

/// run id -> task token
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
            task_q: TEST_Q.to_owned(),
        }],
        true,
        None,
    );
    fake_core_from_mock_sg(mock_gateway)
}

/// See [build_fake_core] -- assemble a mock gateway into the final fake core
pub(crate) fn fake_core_from_mock_sg(mock_and_tasks: MockSGAndTasks) -> FakeCore {
    let core = mock_core(mock_and_tasks.sg);
    FakeCore {
        inner: core,
        outstanding_wf_tasks: mock_and_tasks.task_map,
    }
}

pub(crate) fn mock_core<SG>(sg: SG) -> CoreSDK<SG>
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    mock_core_with_opts(sg, CoreInitOptionsBuilder::default())
}

pub(crate) fn mock_core_with_opts<SG>(sg: SG, opts: CoreInitOptionsBuilder) -> CoreSDK<SG>
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    let mut core = mock_core_with_opts_no_workers(sg, opts);
    core.reg_worker_sync(
        WorkerConfigBuilder::default()
            .task_queue(TEST_Q)
            .build()
            .unwrap(),
    );
    core
}

pub(crate) fn mock_core_with_opts_no_workers<SG>(
    sg: SG,
    mut opts: CoreInitOptionsBuilder,
) -> CoreSDK<SG>
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    CoreSDK::new(sg, opts.gateway_opts(fake_sg_opts()).build().unwrap())
}

pub struct FakeWfResponses {
    pub wf_id: String,
    pub hist: TestHistoryBuilder,
    pub response_batches: Vec<usize>,
    pub task_q: String,
}

// TODO: turn this into a builder or make a new one? to make all these different build fns simpler
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
    augment_multihist_mock_sg(
        hists,
        enforce_correct_number_of_polls,
        num_expected_fails,
        MockServerGatewayApis::new(),
    )
}

/// See [build_multihist_mock_sg] -- manual version that allows setting some expectations on the
/// mock that should be matched first, before the normal expectations that match the provided
/// history.
pub fn augment_multihist_mock_sg(
    hists: impl IntoIterator<Item = FakeWfResponses>,
    enforce_correct_number_of_polls: bool,
    num_expected_fails: Option<usize>,
    mut mock_gateway: MockServerGatewayApis,
) -> MockSGAndTasks {
    // Maps task queues to maps of wfid -> responses
    let mut task_queues_to_resps: HashMap<String, BTreeMap<String, VecDeque<_>>> = HashMap::new();
    let outstanding_wf_task_tokens = Arc::new(RwLock::new(BiMap::new()));
    let mut correct_num_polls = None;

    for hist in hists {
        let full_hist_info = hist.hist.get_full_history_info().unwrap();
        // Ensure no response batch is trying to return more tasks than the history contains
        for rb_wf_num in &hist.response_batches {
            assert!(
                *rb_wf_num <= full_hist_info.wf_task_count(),
                "Wf task count {} is not <= total task count {}",
                rb_wf_num,
                full_hist_info.wf_task_count()
            );
        }
        // Verify response batches only ever return longer histories (IE: Are sorted ascending)
        assert!(
            hist.response_batches
                .as_slice()
                .windows(2)
                .all(|w| w[0] <= w[1]),
            "response batches must have increasing wft numbers"
        );

        if enforce_correct_number_of_polls {
            *correct_num_polls.get_or_insert(0) += hist.response_batches.len();
        }

        // Convert history batches into poll responses, while also tracking how many times a given
        // history has been returned so we can increment the associated attempt number on the WFT.
        // NOTE: This is hard to use properly with the `AfterEveryReply` testing eviction mode.
        //  Such usages need a history different from other eviction modes which would include
        //  WFT timeouts or something to simulate the task getting dropped.
        let mut attempts_at_task_num = HashMap::new();
        let responses: Vec<_> = hist
            .response_batches
            .iter()
            .map(|to_task_num| {
                let cur_attempt = attempts_at_task_num.entry(to_task_num).or_insert(1);
                let mut r = hist_to_poll_resp(
                    &hist.hist,
                    hist.wf_id.to_owned(),
                    *to_task_num,
                    hist.task_q.clone(),
                );
                r.attempt = *cur_attempt;
                *cur_attempt += 1;
                r
            })
            .collect();

        let tasks = VecDeque::from(responses);
        task_queues_to_resps
            .entry(hist.task_q)
            .or_default()
            .insert(hist.wf_id, tasks);
    }

    // The gateway will return history from any workflow runs that do not have currently outstanding
    // tasks.
    let outstanding = outstanding_wf_task_tokens.clone();
    mock_gateway
        .expect_poll_workflow_task()
        .times(
            correct_num_polls
                .map::<TimesRange, _>(Into::into)
                .unwrap_or_else(|| RangeFull.into()),
        )
        .returning(move |tq| {
            if let Some(queue_tasks) = task_queues_to_resps.get_mut(&tq) {
                for (_, tasks) in queue_tasks.iter_mut() {
                    if let Some(t) = tasks.pop_front() {
                        // Must extract run id from a workflow task associated with this workflow
                        // TODO: Case where run id changes for same workflow id is not handled here
                        let rid = t.workflow_execution.as_ref().unwrap().run_id.clone();

                        if !outstanding.read().contains_left(&rid) {
                            outstanding
                                .write()
                                .insert(rid, TaskToken(t.task_token.clone()));
                            return Ok(t);
                        }
                    }
                }
                Err(tonic::Status::cancelled("No more work to do"))
            } else {
                Err(tonic::Status::not_found(format!(
                    "Task queue {} not defined in test setup",
                    tq
                )))
            }
        });

    let outstanding = outstanding_wf_task_tokens.clone();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(move |comp| {
            outstanding.write().remove_by_right(&comp.task_token);
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

/// See [build_multihist_mock_sg] -- one history convenience version
pub fn single_hist_mock_sg(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: &[usize],
    mock_gateway: MockServerGatewayApis,
    enforce_num_polls: bool,
) -> MockSGAndTasks {
    augment_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wf_id.to_owned(),
            hist: t,
            response_batches: response_batches.to_vec(),
            task_q: TEST_Q.to_owned(),
        }],
        enforce_num_polls,
        None,
        mock_gateway,
    )
}

pub fn hist_to_poll_resp(
    t: &TestHistoryBuilder,
    wf_id: String,
    to_task_num: usize,
    task_queue: String,
) -> PollWorkflowTaskQueueResponse {
    let run_id = t.get_orig_run_id();
    let wf = WorkflowExecution {
        workflow_id: wf_id,
        run_id: run_id.to_string(),
    };
    let hist_info = t.get_history_info(to_task_num).unwrap();
    let batch = hist_info.events().to_vec();
    let task_token: [u8; 16] = thread_rng().gen();
    PollWorkflowTaskQueueResponse {
        history: Some(History { events: batch }),
        workflow_execution: Some(wf),
        task_token: task_token.to_vec(),
        workflow_execution_task_queue: Some(TaskQueue {
            name: task_queue,
            kind: TaskQueueKind::Normal as i32,
        }),
        previous_started_event_id: hist_info.previous_started_event_id,
        started_event_id: hist_info.workflow_task_started_event_id,
        ..Default::default()
    }
}

pub fn fake_sg_opts() -> ServerGatewayOptions {
    ServerGatewayOptions {
        target_url: Url::from_str("https://fake").unwrap(),
        namespace: "".to_string(),
        identity: "".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Default::default(),
        tls_cfg: None,
    }
}

type AsserterWithReply<'a> = (&'a dyn Fn(&WfActivation), wf_activation_completion::Status);

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed
pub(crate) async fn poll_and_reply<'a>(
    core: &'a FakeCore,
    eviction_mode: WorkflowCachingPolicy,
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

            let mut res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
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
                    .remove_by_left(&res.run_id);
                continue 'outer;
            }

            asserter(&res);

            core.inner
                .complete_workflow_task(WfActivationCompletion::from_status(
                    res.run_id.clone(),
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
                WorkflowCachingPolicy::Sticky { .. } => unimplemented!(),
                WorkflowCachingPolicy::NonSticky => (),
                WorkflowCachingPolicy::AfterEveryReply => {
                    if evictions < expected_evictions {
                        core.inner.request_workflow_eviction(&res.run_id);
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }
    assert_eq!(core.inner.wft_manager.outstanding_wft(), 0);
}

pub(crate) fn gen_assert_and_reply(
    asserter: &dyn Fn(&WfActivation),
    reply_commands: Vec<workflow_command::Variant>,
) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Success::from_variants(reply_commands).into(),
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
