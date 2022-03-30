pub(crate) use temporal_sdk_core_test_utils::canned_histories;

use crate::{
    pollers::{BoxedActPoller, BoxedPoller, BoxedWFPoller, MockManualPoller, MockPoller},
    replay::TestHistoryBuilder,
    sticky_q_name_for_worker,
    worker::client::{mocks::mock_workflow_client, MockWorkerClient, WorkerClient},
    workflow::WorkflowCachingPolicy,
    TaskToken, Worker, WorkerClientBag, WorkerConfig, WorkerConfigBuilder,
};
use bimap::BiMap;
use futures::future::BoxFuture;
use futures::FutureExt;
use mockall::TimesRange;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::RangeFull,
    sync::Arc,
};
use temporal_client::WorkflowTaskCompletion;
use temporal_sdk_core_api::Worker as WorkerTrait;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::WorkflowActivation,
        workflow_commands::workflow_command,
        workflow_completion::{self, workflow_activation_completion, WorkflowActivationCompletion},
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::WorkflowTaskFailedCause,
        failure::v1::Failure,
        workflowservice::v1::{
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};

pub const TEST_Q: &str = "q";
pub static NO_MORE_WORK_ERROR_MSG: &str = "No more work to do";

pub fn test_worker_cfg() -> WorkerConfigBuilder {
    let mut wcb = WorkerConfigBuilder::default();
    wcb.namespace("default").task_queue(TEST_Q);
    wcb
}

/// When constructing responses for mocks, indicates how a given response should be built
#[derive(derive_more::From)]
pub enum ResponseType {
    ToTaskNum(usize),
    /// Returns just the history after the WFT completed of the provided task number - 1, through to
    /// the next WFT started. Simulating the incremental history for just the provided task number
    #[from(ignore)]
    OneTask(usize),
    /// Waits until the future resolves before responding as `ToTaskNum` with the provided number
    UntilResolved(BoxFuture<'static, ()>, usize),
    AllHistory,
}
impl ResponseType {
    pub fn as_num(&self) -> usize {
        match self {
            ResponseType::ToTaskNum(n) => *n,
            ResponseType::OneTask(n) => *n,
            ResponseType::UntilResolved(_, n) => *n,
            ResponseType::AllHistory => usize::MAX,
        }
    }
}
impl From<&usize> for ResponseType {
    fn from(u: &usize) -> Self {
        Self::ToTaskNum(*u)
    }
}

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the a worker with a mock server client that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned. For
/// each number in the input list, a fake response will be prepared which includes history up to the
/// workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub(crate) fn build_fake_worker(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
) -> Worker {
    let response_batches = response_batches.into_iter().map(Into::into).collect();
    let mock_holder = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wf_id.to_owned(),
            hist: t,
            response_batches,
        }],
        true,
        None,
    );
    mock_worker(mock_holder)
}

pub(crate) fn mock_worker(mocks: MocksHolder) -> Worker {
    let sticky_q = sticky_q_name_for_worker("unit-test", &mocks.mock_worker.config);
    Worker::new_with_pollers(
        mocks.mock_worker.config,
        sticky_q,
        Arc::new(mocks.client_bag),
        mocks.mock_worker.wf_poller,
        mocks.mock_worker.act_poller,
        Default::default(),
    )
}

pub struct FakeWfResponses {
    pub wf_id: String,
    pub hist: TestHistoryBuilder,
    pub response_batches: Vec<ResponseType>,
}

// TODO: Rename to mock TQ or something?
pub struct MocksHolder {
    client_bag: WorkerClientBag,
    mock_worker: MockWorker,
    // bidirectional mapping of run id / task token
    pub outstanding_task_map: Option<Arc<RwLock<BiMap<String, TaskToken>>>>,
}

impl MocksHolder {
    pub fn worker_cfg(&mut self, mutator: impl FnOnce(&mut WorkerConfig)) {
        mutator(&mut self.mock_worker.config);
    }
    pub fn set_act_poller(&mut self, poller: BoxedActPoller) {
        self.mock_worker.act_poller = Some(poller);
    }
}

pub struct MockWorker {
    pub wf_poller: BoxedWFPoller,
    pub act_poller: Option<BoxedActPoller>,
    pub config: WorkerConfig,
}

impl Default for MockWorker {
    fn default() -> Self {
        Self::new(Box::from(mock_poller()))
    }
}

impl MockWorker {
    pub fn new(wf_poller: BoxedWFPoller) -> Self {
        Self {
            wf_poller,
            act_poller: None,
            config: test_worker_cfg().build().unwrap(),
        }
    }
}

impl MocksHolder {
    pub(crate) fn from_mock_worker(client_bag: WorkerClientBag, mock_worker: MockWorker) -> Self {
        Self {
            client_bag,
            mock_worker,
            outstanding_task_map: None,
        }
    }

    /// Uses the provided list of tasks to create a mock poller for the `TEST_Q`
    pub(crate) fn from_client_with_responses<WFT, ACT>(
        client: impl WorkerClient + 'static,
        wf_tasks: WFT,
        act_tasks: ACT,
    ) -> Self
    where
        WFT: IntoIterator<Item = QueueResponse<PollWorkflowTaskQueueResponse>>,
        ACT: IntoIterator<Item = QueueResponse<PollActivityTaskQueueResponse>>,
        <WFT as IntoIterator>::IntoIter: Send + 'static,
        <ACT as IntoIterator>::IntoIter: Send + 'static,
    {
        let mock_poller = mock_poller_from_resps(wf_tasks);
        let mock_act_poller = mock_poller_from_resps(act_tasks);
        let mock_worker = MockWorker {
            wf_poller: mock_poller,
            act_poller: Some(mock_act_poller),
            config: test_worker_cfg().build().unwrap(),
        };
        Self {
            client_bag: client.into(),
            mock_worker,
            outstanding_task_map: None,
        }
    }
}

// TODO: Un-pub ideally
pub(crate) fn mock_poller_from_resps<T, I>(tasks: I) -> BoxedPoller<T>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = QueueResponse<T>>,
    <I as IntoIterator>::IntoIter: Send + 'static,
{
    let mut mock_poller = mock_manual_poller();
    let mut tasks = tasks.into_iter();
    mock_poller.expect_poll().returning(move || {
        if let Some(t) = tasks.next() {
            async move {
                if let Some(f) = t.delay_until {
                    f.await;
                }
                Some(Ok(t.resp))
            }
            .boxed()
        } else {
            async { Some(Err(tonic::Status::cancelled(NO_MORE_WORK_ERROR_MSG))) }.boxed()
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

/// Build a mock server client capable of returning multiple different histories for different
/// workflows. It does so by tracking outstanding workflow tasks like is also happening in core
/// (which is unfortunately a bit redundant, we could provide hooks in core but that feels a little
/// nasty). If there is an outstanding task for a given workflow, new chunks of its history are not
/// returned. If there is not, the next batch of history is returned for any workflow without an
/// outstanding task. Outstanding tasks are cleared on completion, failure, or eviction.
///
/// `num_expected_fails` can be provided to set a specific number of expected failed workflow tasks
/// sent to the server.
pub(crate) fn build_multihist_mock_sg(
    hists: impl IntoIterator<Item = FakeWfResponses>,
    enforce_correct_number_of_polls: bool,
    num_expected_fails: Option<usize>,
) -> MocksHolder {
    let mh = MockPollCfg::new(
        hists.into_iter().collect(),
        enforce_correct_number_of_polls,
        num_expected_fails,
    );
    build_mock_pollers(mh)
}

/// See [build_multihist_mock_sg] -- one history convenience version
pub(crate) fn single_hist_mock_sg(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
    mock_client: MockWorkerClient,
    enforce_num_polls: bool,
) -> MocksHolder {
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, response_batches, mock_client);
    mh.enforce_correct_number_of_polls = enforce_num_polls;
    build_mock_pollers(mh)
}

pub(crate) struct MockPollCfg {
    pub hists: Vec<FakeWfResponses>,
    pub enforce_correct_number_of_polls: bool,
    pub num_expected_fails: Option<usize>,
    pub mock_client: MockWorkerClient,
    /// All calls to fail WFTs must match this predicate
    pub expect_fail_wft_matcher:
        Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>,
    pub completion_asserts: Option<Box<dyn Fn(&WorkflowTaskCompletion) + Send>>,
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
            mock_client: mock_workflow_client(),
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
            completion_asserts: None,
        }
    }
    pub fn from_resp_batches(
        wf_id: &str,
        t: TestHistoryBuilder,
        resps: impl IntoIterator<Item = impl Into<ResponseType>>,
        mock_client: MockWorkerClient,
    ) -> Self {
        Self {
            hists: vec![FakeWfResponses {
                wf_id: wf_id.to_owned(),
                hist: t,
                response_batches: resps.into_iter().map(Into::into).collect(),
            }],
            enforce_correct_number_of_polls: true,
            num_expected_fails: None,
            mock_client,
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
            completion_asserts: None,
        }
    }
}

/// Given an iterable of fake responses, return the mocks & associated data to work with them
pub(crate) fn build_mock_pollers(mut cfg: MockPollCfg) -> MocksHolder {
    let mut task_q_resps: BTreeMap<String, VecDeque<_>> = BTreeMap::new();
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
            .into_iter()
            .map(|resp| {
                let cur_attempt = attempts_at_task_num.entry(resp.as_num()).or_insert(1);
                let mut r = hist_to_poll_resp(&hist.hist, hist.wf_id.clone(), resp, TEST_Q);
                r.resp.attempt = *cur_attempt;
                *cur_attempt += 1;
                r
            })
            .collect();

        let tasks = VecDeque::from(responses);
        task_q_resps.insert(hist.wf_id, tasks);
    }

    let mut mock_poller = mock_manual_poller();
    // The poller will return history from any workflow runs that do not have currently
    // outstanding tasks.
    let outstanding = outstanding_wf_task_tokens.clone();
    mock_poller
        .expect_poll()
        .times(correct_num_polls.map_or_else(|| RangeFull.into(), Into::<TimesRange>::into))
        .returning(move || {
            for (_, tasks) in task_q_resps.iter_mut() {
                // Must extract run id from a workflow task associated with this workflow
                // TODO: Case where run id changes for same workflow id is not handled here
                if let Some(t) = tasks.get(0) {
                    let rid = t.resp.workflow_execution.as_ref().unwrap().run_id.clone();
                    if !outstanding.read().contains_left(&rid) {
                        let t = tasks.pop_front().unwrap();
                        outstanding
                            .write()
                            .insert(rid, TaskToken(t.resp.task_token.clone()));
                        return async move {
                            if let Some(f) = t.delay_until {
                                f.await;
                            }
                            Some(Ok(t.resp))
                        }
                        .boxed();
                    }
                }
            }
            async { Some(Err(tonic::Status::cancelled(NO_MORE_WORK_ERROR_MSG))) }.boxed()
        });
    let mock_worker = MockWorker::new(Box::from(mock_poller));

    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_client
        .expect_complete_workflow_task()
        .returning(move |comp| {
            if let Some(ass) = cfg.completion_asserts.as_ref() {
                // tee hee
                ass(&comp)
            }
            outstanding.write().remove_by_right(&comp.task_token);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_client
        .expect_fail_workflow_task()
        .withf(cfg.expect_fail_wft_matcher)
        .times(
            cfg.num_expected_fails
                .map_or_else(|| RangeFull.into(), Into::<TimesRange>::into),
        )
        .returning(move |tt, _, _| {
            outstanding.write().remove_by_right(&tt);
            Ok(Default::default())
        });

    MocksHolder {
        client_bag: cfg.mock_client.into(),
        mock_worker,
        outstanding_task_map: Some(outstanding_wf_task_tokens),
    }
}

pub struct QueueResponse<T> {
    pub resp: T,
    pub delay_until: Option<BoxFuture<'static, ()>>,
}
impl<T> From<T> for QueueResponse<T> {
    fn from(resp: T) -> Self {
        QueueResponse {
            resp,
            delay_until: None,
        }
    }
}

pub fn hist_to_poll_resp(
    t: &TestHistoryBuilder,
    wf_id: String,
    response_type: ResponseType,
    task_queue: impl Into<String>,
) -> QueueResponse<PollWorkflowTaskQueueResponse> {
    let run_id = t.get_orig_run_id();
    let wf = WorkflowExecution {
        workflow_id: wf_id,
        run_id: run_id.to_string(),
    };
    let mut delay_until = None;
    let hist_info = match response_type {
        ResponseType::ToTaskNum(tn) => t.get_history_info(tn).unwrap(),
        ResponseType::OneTask(tn) => t.get_one_wft(tn).unwrap(),
        ResponseType::AllHistory => t.get_full_history_info().unwrap(),
        ResponseType::UntilResolved(fut, tn) => {
            delay_until = Some(fut);
            t.get_history_info(tn).unwrap()
        }
    };
    let mut resp = hist_info.as_poll_wft_response(task_queue);
    resp.workflow_execution = Some(wf);
    QueueResponse { resp, delay_until }
}

type AsserterWithReply<'a> = (
    &'a dyn Fn(&WorkflowActivation),
    workflow_activation_completion::Status,
);

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed
pub(crate) async fn poll_and_reply<'a>(
    worker: &'a Worker,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    poll_and_reply_clears_outstanding_evicts(worker, None, eviction_mode, expect_and_reply).await;
}

pub(crate) async fn poll_and_reply_clears_outstanding_evicts<'a>(
    worker: &'a Worker,
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

            let mut res = worker.poll_workflow_activation().await.unwrap();
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
                WorkflowActivationCompletion::empty(res.run_id.clone())
            } else {
                // Eviction plus some work, we still want to issue the reply
                WorkflowActivationCompletion {
                    run_id: res.run_id.clone(),
                    status: Some(reply.clone()),
                }
            };

            worker.complete_workflow_activation(reply).await.unwrap();

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
                        worker.request_workflow_eviction(&res.run_id);
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }

    assert_eq!(expected_fail_count, executed_failures.len());
    assert_eq!(worker.outstanding_workflow_tasks(), 0);
}

pub(crate) fn gen_assert_and_reply(
    asserter: &dyn Fn(&WorkflowActivation),
    reply_commands: Vec<workflow_command::Variant>,
) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Success::from_variants(reply_commands).into(),
    )
}

pub(crate) fn gen_assert_and_fail(asserter: &dyn Fn(&WorkflowActivation)) -> AsserterWithReply<'_> {
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
                [$(WorkflowActivationJob {
                    variant: Some($pat),
                }),+]
            );
        }
    };
}
