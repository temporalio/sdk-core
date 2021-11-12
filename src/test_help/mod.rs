pub mod canned_histories;

mod history_builder;
mod history_info;

pub(crate) use history_builder::{TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE};

use crate::{
    pollers::{
        BoxedActPoller, BoxedPoller, BoxedWFPoller, MockManualPoller, MockPoller,
        MockServerGatewayApis,
    },
    task_token::TaskToken,
    workflow::WorkflowCachingPolicy,
    Core, CoreInitOptionsBuilder, CoreSDK, ServerGatewayApis, ServerGatewayOptions, Url,
    WorkerConfig, WorkerConfigBuilder,
};
use bimap::BiMap;
use futures::FutureExt;
use mockall::TimesRange;
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::RangeFull,
    str::FromStr,
    sync::Arc,
};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::WfActivation,
        workflow_commands::workflow_command,
        workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
    },
    temporal::api::{
        common::v1::{WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::History,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;
pub const TEST_Q: &str = "q";
pub static NO_MORE_WORK_ERROR_MSG: &str = "No more work to do";

/// When constructing responses for mocks, indicates how a given response should be built
#[derive(derive_more::From, Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ResponseType {
    ToTaskNum(usize),
    AllHistory,
}

impl From<&usize> for ResponseType {
    fn from(u: &usize) -> Self {
        ResponseType::ToTaskNum(*u)
    }
}
// :shrug:
impl From<&ResponseType> for ResponseType {
    fn from(r: &ResponseType) -> Self {
        *r
    }
}

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the core SDK with a mock server gateway that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned. For
/// each number in the input list, a fake response will be prepared which includes history up to the
/// workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub(crate) fn build_fake_core(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
) -> CoreSDK {
    let response_batches = response_batches.into_iter().map(Into::into).collect();
    let mock_gateway = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wf_id.to_owned(),
            hist: t,
            response_batches,
            task_q: TEST_Q.to_owned(),
        }],
        true,
        None,
    );
    mock_core(mock_gateway)
}

pub(crate) fn mock_core<SG>(mocks: MocksHolder<SG>) -> CoreSDK
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    mock_core_with_opts(mocks, CoreInitOptionsBuilder::default())
}

pub(crate) fn mock_core_with_opts<SG>(
    mocks: MocksHolder<SG>,
    opts: CoreInitOptionsBuilder,
) -> CoreSDK
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    let mut core = mock_core_with_opts_no_workers(mocks.sg, opts);
    register_mock_workers(&mut core, mocks.mock_pollers.into_values());
    core
}

pub(crate) fn register_mock_workers(
    core: &mut CoreSDK,
    mocks: impl IntoIterator<Item = MockWorker>,
) {
    for worker in mocks {
        core.reg_worker_sync(worker);
    }
}

pub(crate) fn mock_core_with_opts_no_workers<SG>(
    sg: SG,
    mut opts: CoreInitOptionsBuilder,
) -> CoreSDK
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    CoreSDK::new(sg, opts.gateway_opts(fake_sg_opts()).build().unwrap())
}

pub struct FakeWfResponses {
    pub wf_id: String,
    pub hist: TestHistoryBuilder,
    pub response_batches: Vec<ResponseType>,
    pub task_q: String,
}

// TODO: turn this into a builder or make a new one? to make all these different build fns simpler
pub struct MocksHolder<SG> {
    sg: SG,
    mock_pollers: HashMap<String, MockWorker>,
    pub outstanding_task_map: Option<Arc<RwLock<BiMap<String, TaskToken>>>>,
}

impl<SG> MocksHolder<SG> {
    pub fn worker_cfg(&mut self, task_q: &str, mutator: impl FnOnce(&mut WorkerConfig)) {
        if let Some(w) = self.mock_pollers.get_mut(task_q) {
            mutator(&mut w.config);
        }
    }

    pub fn take_pollers(self) -> HashMap<String, MockWorker> {
        self.mock_pollers
    }
}

pub struct MockWorker {
    pub wf_poller: BoxedWFPoller,
    pub act_poller: Option<BoxedActPoller>,
    pub config: WorkerConfig,
}

impl Default for MockWorker {
    fn default() -> Self {
        MockWorker {
            wf_poller: Box::from(mock_poller()),
            act_poller: None,
            config: WorkerConfig::default_test_q(),
        }
    }
}

impl MockWorker {
    pub fn new(q: &str, wf_poller: BoxedWFPoller) -> Self {
        MockWorker {
            wf_poller,
            act_poller: None,
            config: WorkerConfig::default(q),
        }
    }
    pub fn for_queue(q: &str) -> Self {
        MockWorker {
            wf_poller: Box::from(mock_poller()),
            act_poller: None,
            config: WorkerConfig::default(q),
        }
    }
}

impl<SG> MocksHolder<SG>
where
    SG: ServerGatewayApis + Send + Sync + 'static,
{
    pub fn from_mock_workers(
        sg: SG,
        mock_workers: impl IntoIterator<Item = MockWorker>,
    ) -> MocksHolder<SG> {
        let mock_pollers = mock_workers
            .into_iter()
            .map(|w| (w.config.task_queue.clone(), w))
            .collect();
        MocksHolder {
            sg,
            mock_pollers,
            outstanding_task_map: None,
        }
    }

    /// Uses the provided list of tasks to create a mock poller for the `TEST_Q`
    pub fn from_gateway_with_responses<WFT, ACT>(
        sg: SG,
        wf_tasks: WFT,
        act_tasks: ACT,
    ) -> MocksHolder<SG>
    where
        WFT: IntoIterator<Item = PollWorkflowTaskQueueResponse>,
        ACT: IntoIterator<Item = PollActivityTaskQueueResponse>,
        <WFT as IntoIterator>::IntoIter: std::marker::Send + 'static,
        <ACT as IntoIterator>::IntoIter: std::marker::Send + 'static,
    {
        let mut mock_pollers = HashMap::new();
        let mock_poller = mock_poller_from_resps(wf_tasks);
        let mock_act_poller = mock_poller_from_resps(act_tasks);
        mock_pollers.insert(
            TEST_Q.to_string(),
            MockWorker {
                wf_poller: mock_poller,
                act_poller: Some(mock_act_poller),
                config: WorkerConfigBuilder::default()
                    .task_queue(TEST_Q)
                    .build()
                    .unwrap(),
            },
        );
        MocksHolder {
            sg,
            mock_pollers,
            outstanding_task_map: None,
        }
    }
}

pub fn mock_poller_from_resps<T, I>(tasks: I) -> BoxedPoller<T>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = T>,
    <I as IntoIterator>::IntoIter: std::marker::Send + 'static,
{
    let mut mock_poller = mock_poller();
    let mut tasks = tasks.into_iter();
    mock_poller.expect_poll().returning(move || {
        if let Some(t) = tasks.next() {
            Some(Ok(t))
        } else {
            Some(Err(tonic::Status::cancelled(NO_MORE_WORK_ERROR_MSG)))
        }
    });
    Box::new(mock_poller) as BoxedPoller<T>
}

pub fn mock_poller<T>() -> MockPoller<T>
where
    T: Send + Sync + 'static,
{
    let mut mock_poller = MockPoller::new();
    mock_poller.expect_shutdown_box().return_const(());
    mock_poller.expect_notify_shutdown().return_const(());
    mock_poller
}

pub fn mock_manual_poller<T>() -> MockManualPoller<T>
where
    T: Send + Sync + 'static,
{
    let mut mock_poller = MockManualPoller::new();
    mock_poller
        .expect_shutdown_box()
        .returning(|| async {}.boxed());
    mock_poller.expect_notify_shutdown().return_const(());
    mock_poller
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
) -> MocksHolder<MockServerGatewayApis> {
    let mh = MockPollCfg::new(
        hists.into_iter().collect(),
        enforce_correct_number_of_polls,
        num_expected_fails,
    );
    build_mock_pollers(mh)
}

/// See [build_multihist_mock_sg] -- one history convenience version
pub fn single_hist_mock_sg(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
    mock_gateway: MockServerGatewayApis,
    enforce_num_polls: bool,
) -> MocksHolder<MockServerGatewayApis> {
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, response_batches, mock_gateway);
    mh.enforce_correct_number_of_polls = enforce_num_polls;
    build_mock_pollers(mh)
}

pub struct MockPollCfg {
    pub hists: Vec<FakeWfResponses>,
    pub enforce_correct_number_of_polls: bool,
    pub num_expected_fails: Option<usize>,
    pub mock_gateway: MockServerGatewayApis,
    /// All calls to fail WFTs must match this predicate
    pub expect_fail_wft_matcher:
        Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>,
}

impl MockPollCfg {
    pub fn new(
        hists: Vec<FakeWfResponses>,
        enforce_correct_number_of_polls: bool,
        num_expected_fails: Option<usize>,
    ) -> Self {
        Self {
            hists,
            enforce_correct_number_of_polls,
            num_expected_fails,
            mock_gateway: MockServerGatewayApis::new(),
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
        }
    }
    pub fn from_resp_batches(
        wf_id: &str,
        t: TestHistoryBuilder,
        resps: impl IntoIterator<Item = impl Into<ResponseType>>,
        mock_gateway: MockServerGatewayApis,
    ) -> Self {
        Self {
            hists: vec![FakeWfResponses {
                wf_id: wf_id.to_owned(),
                hist: t,
                response_batches: resps.into_iter().map(Into::into).collect(),
                task_q: TEST_Q.to_owned(),
            }],
            enforce_correct_number_of_polls: true,
            num_expected_fails: None,
            mock_gateway,
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
        }
    }
}

/// Given an iterable of fake responses, return the mocks & associated data to work with them
pub fn build_mock_pollers(mut cfg: MockPollCfg) -> MocksHolder<MockServerGatewayApis> {
    // Maps task queues to maps of wfid -> responses
    let mut task_queues_to_resps: HashMap<String, BTreeMap<String, VecDeque<_>>> = HashMap::new();
    let outstanding_wf_task_tokens = Arc::new(RwLock::new(BiMap::new()));
    let mut correct_num_polls = None;

    for hist in cfg.hists {
        let full_hist_info = hist.hist.get_full_history_info().unwrap();
        // Ensure no response batch is trying to return more tasks than the history contains
        for respt in &hist.response_batches {
            if let ResponseType::ToTaskNum(rb_wf_num) = respt {
                assert!(
                    *rb_wf_num <= full_hist_info.wf_task_count(),
                    "Wf task count {} is not <= total task count {}",
                    rb_wf_num,
                    full_hist_info.wf_task_count()
                );
            }
        }

        // TODO: Fix -- or not? Sticky invalidation could make this pointless anyway
        // Verify response batches only ever return longer histories (IE: Are sorted ascending)
        // assert!(
        //     hist.response_batches
        //         .as_slice()
        //         .windows(2)
        //         .all(|w| w[0] <= w[1]),
        //     "response batches must have increasing wft numbers"
        // );

        if cfg.enforce_correct_number_of_polls {
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

    let mut mock_pollers = HashMap::new();
    for (task_q, mut queue_tasks) in task_queues_to_resps.into_iter() {
        let mut mock_poller = mock_poller();

        // The poller will return history from any workflow runs that do not have currently
        // outstanding tasks.
        let outstanding = outstanding_wf_task_tokens.clone();
        mock_poller
            .expect_poll()
            .times(
                correct_num_polls
                    .map::<TimesRange, _>(Into::into)
                    .unwrap_or_else(|| RangeFull.into()),
            )
            .returning(move || {
                for (_, tasks) in queue_tasks.iter_mut() {
                    // Must extract run id from a workflow task associated with this workflow
                    // TODO: Case where run id changes for same workflow id is not handled here
                    if let Some(t) = tasks.get(0) {
                        let rid = t.workflow_execution.as_ref().unwrap().run_id.clone();
                        if !outstanding.read().contains_left(&rid) {
                            let t = tasks.pop_front().unwrap();
                            outstanding
                                .write()
                                .insert(rid, TaskToken(t.task_token.clone()));
                            return Some(Ok(t));
                        }
                    }
                }
                Some(Err(tonic::Status::cancelled(NO_MORE_WORK_ERROR_MSG)))
            });
        let mw = MockWorker::new(&task_q, Box::from(mock_poller));
        mock_pollers.insert(task_q, mw);
    }

    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_gateway
        .expect_complete_workflow_task()
        .returning(move |comp| {
            outstanding.write().remove_by_right(&comp.task_token);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_gateway
        .expect_fail_workflow_task()
        .withf(cfg.expect_fail_wft_matcher)
        .times(
            cfg.num_expected_fails
                .map::<TimesRange, _>(Into::into)
                .unwrap_or_else(|| RangeFull.into()),
        )
        .returning(move |tt, _, _| {
            outstanding.write().remove_by_right(&tt);
            Ok(Default::default())
        });
    cfg.mock_gateway
        .expect_start_workflow()
        .returning(|_, _, _, _, _| Ok(Default::default()));

    MocksHolder {
        sg: cfg.mock_gateway,
        mock_pollers,
        outstanding_task_map: Some(outstanding_wf_task_tokens),
    }
}

pub fn hist_to_poll_resp(
    t: &TestHistoryBuilder,
    wf_id: String,
    response_type: ResponseType,
    task_queue: String,
) -> PollWorkflowTaskQueueResponse {
    let run_id = t.get_orig_run_id();
    let wf = WorkflowExecution {
        workflow_id: wf_id,
        run_id: run_id.to_string(),
    };
    let hist_info = match response_type {
        ResponseType::ToTaskNum(tn) => t.get_history_info(tn).unwrap(),
        ResponseType::AllHistory => t.get_full_history_info().unwrap(),
    };
    let batch = hist_info.events().to_vec();
    let task_token: [u8; 16] = thread_rng().gen();
    PollWorkflowTaskQueueResponse {
        history: Some(History { events: batch }),
        workflow_execution: Some(wf),
        task_token: task_token.to_vec(),
        workflow_type: Some(WorkflowType {
            name: DEFAULT_WORKFLOW_TYPE.to_owned(),
        }),
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
        client_name: "".to_string(),
        client_version: "".to_string(),
        static_headers: Default::default(),
        identity: "".to_string(),
        worker_binary_id: "".to_string(),
        tls_cfg: None,
        retry_config: Default::default(),
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
    core: &'a CoreSDK,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    poll_and_reply_clears_outstanding_evicts(core, None, eviction_mode, expect_and_reply).await
}

pub(crate) async fn poll_and_reply_clears_outstanding_evicts<'a>(
    core: &'a CoreSDK,
    outstanding_map: Option<Arc<RwLock<BiMap<String, TaskToken>>>>,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;
    let mut executed_failures = HashSet::new();
    let expected_fail_count = expect_and_reply
        .iter()
        .filter(|(_, reply)| !reply.is_success())
        .count();

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for (i, interaction) in expect_iter.enumerate() {
            let (asserter, reply) = interaction;
            let complete_is_failure = !reply.is_success();
            // Only send activation failures once
            if executed_failures.contains(&i) {
                continue;
            }

            let mut res = core.poll_workflow_activation(TEST_Q).await.unwrap();
            let contains_eviction = res.eviction_index();

            if let Some(eviction_job_ix) = contains_eviction {
                // If the job list has an eviction, make sure it was the last item in the list
                // then remove it, since in the tests we don't explicitly specify evict assertions
                assert_eq!(
                    eviction_job_ix,
                    res.jobs.len() - 1,
                    "Eviction job was not last job in job list"
                );
                res.jobs.remove(eviction_job_ix);
                if let Some(omap) = outstanding_map.as_ref() {
                    omap.write().remove_by_left(&res.run_id);
                }
            }

            // TODO: Can remove this if?
            if !res.jobs.is_empty() {
                asserter(&res);
            }

            let reply = if res.jobs.is_empty() {
                // Just an eviction
                WfActivationCompletion::empty(TEST_Q, res.run_id.clone())
            } else {
                // Eviction plus some work, we still want to issue the reply
                WfActivationCompletion {
                    task_queue: TEST_Q.to_string(),
                    run_id: res.run_id.clone(),
                    status: Some(reply.clone()),
                }
            };

            core.complete_workflow_activation(reply).await.unwrap();

            // Restart assertions from the beginning if it was an eviction
            if contains_eviction.is_some() {
                continue 'outer;
            }

            if complete_is_failure {
                executed_failures.insert(i);
            }

            match eviction_mode {
                WorkflowCachingPolicy::Sticky { .. } => unimplemented!(),
                WorkflowCachingPolicy::NonSticky => (),
                WorkflowCachingPolicy::AfterEveryReply => {
                    if evictions < expected_evictions {
                        core.request_workflow_eviction(TEST_Q, &res.run_id);
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }

    assert_eq!(expected_fail_count, executed_failures.len());
    // TODO: Really need a worker abstraction for testing
    // assert_eq!(core.wft_manager.outstanding_wft(), 0);
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
            failure: Some(Failure {
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
