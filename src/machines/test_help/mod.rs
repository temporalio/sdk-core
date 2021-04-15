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
            workflow_activation::WfActivation,
            workflow_commands::workflow_command,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
        },
        temporal::api::common::v1::WorkflowExecution,
        temporal::api::history::v1::History,
        temporal::api::workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    Core, CoreInitOptions, CoreSDK, ServerGatewayOptions,
};
use rand::{thread_rng, Rng};
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use url::Url;

#[derive(derive_more::Constructor)]
pub(crate) struct FakeCore {
    pub(crate) inner: CoreSDK<MockServerGatewayApis>,
    /// Number of times we expect the server to be polled for workflow tasks
    pub expected_wft_calls: usize,
}

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the core SDK with a mock server gateway that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned. For
/// each number in the input list, a fake response will be prepared which includes history up to the
/// workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub(crate) fn build_fake_core(
    wf_id: &str,
    t: &mut TestHistoryBuilder,
    response_batches: &[usize],
) -> FakeCore {
    let run_id = t.get_orig_run_id();
    let wf = Some(WorkflowExecution {
        workflow_id: wf_id.to_string(),
        run_id: run_id.to_string(),
    });

    let full_hist_info = t.get_full_history_info().unwrap();
    // Ensure no response batch is trying to return more tasks than the history contains
    for rb_wf_num in response_batches {
        assert!(
            *rb_wf_num <= full_hist_info.wf_task_count,
            "Wf task count {} is not <= total task count {}",
            rb_wf_num,
            full_hist_info.wf_task_count
        );
    }

    let responses: Vec<_> = response_batches
        .iter()
        .map(|to_task_num| {
            let batch = t.get_history_info(*to_task_num).unwrap().events().to_vec();
            let task_token: [u8; 16] = thread_rng().gen();
            PollWorkflowTaskQueueResponse {
                history: Some(History { events: batch }),
                workflow_execution: wf.clone(),
                task_token: task_token.to_vec(),
                ..Default::default()
            }
        })
        .collect();

    let mut tasks = VecDeque::from(responses);
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_workflow_task()
        .times(response_batches.len())
        .returning(move |_| Ok(tasks.pop_front().unwrap()));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    FakeCore::new(
        CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: ServerGatewayOptions {
                    target_url: Url::from_str("https://fake").unwrap(),
                    namespace: "".to_string(),
                    identity: "".to_string(),
                    worker_binary_id: "".to_string(),
                    long_poll_timeout: Default::default(),
                },
                evict_after_each_workflow_task: true,
            },
        ),
        response_batches.len(),
    )
}

type AsserterWithReply<'a> = (&'a dyn Fn(&WfActivation), wf_activation_completion::Status);

pub enum EvictionMode {
    #[allow(dead_code)] // Not used until we have stickyness options implemented
    /// Core is in sticky mode, and workflows are being cached
    Sticky,
    /// Core is not in sticky mode, and workflows are evicted after they finish their pending
    /// activations
    NotSticky,
}

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed. If you want to fail more than once, put multiple failures in the expectations.
pub(crate) async fn poll_and_reply<'a>(
    core: &'a FakeCore,
    task_queue: &'a str,
    eviction_mode: EvictionMode,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut run_id = "".to_string();
    let mut performed_wft_calls = 0;
    let mut executed_failures = HashSet::new();

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for (i, interaction) in expect_iter.enumerate() {
            let (asserter, _) = interaction;
            let res = core.inner.poll_workflow_task(task_queue).await.unwrap();
            performed_wft_calls += 1;

            asserter(&res);

            if run_id.is_empty() {
                run_id = res.run_id.clone();
            }

            let finished_all_replies = complete_and_reply_to_all_activations(
                core,
                i,
                expect_and_reply,
                res.task_token,
                &mut executed_failures,
            )
            .await;

            if finished_all_replies {
                break;
            }

            match eviction_mode {
                // Currently the "inner" loop never is triggered -- it will be in sticky mode
                EvictionMode::Sticky => unimplemented!(),
                EvictionMode::NotSticky => {
                    // If we are in non-sticky mode, we have to replay all expectations every time a
                    // wft is completed successfully (hence there are no more pending activations).
                    if performed_wft_calls < core.expected_wft_calls {
                        continue 'outer;
                    }
                }
            }
        }

        break;
    }
}

async fn complete_and_reply_to_all_activations<'a>(
    core: &'a FakeCore,
    start_at: usize,
    expect_and_reply: &'a [AsserterWithReply<'a>],
    task_tok: Vec<u8>,
    executed_failures: &mut HashSet<usize>,
) -> bool {
    let mut iter = expect_and_reply
        .iter()
        .enumerate()
        .skip(start_at)
        .peekable();
    while let Some((i, interaction)) = iter.next() {
        // Only send activation failures once
        if executed_failures.contains(&i) {
            continue;
        }
        let (_, reply) = interaction;
        let complete_is_failure = !reply.is_success();

        let maybe_pending_activation = core
            .inner
            .complete_workflow_task(WfActivationCompletion::from_status(
                task_tok.clone(),
                reply.clone(),
            ))
            .await
            .unwrap();

        if complete_is_failure {
            executed_failures.insert(i);
        }

        // Because we just sent a reply, we actually need to check against the *next* assertion
        if let Some(next_activation) = maybe_pending_activation {
            if let Some((_, (next_asserter, _))) = iter.peek() {
                next_asserter(&next_activation);
            }
        } else {
            break;
        }
    }
    return iter.peek().is_none();
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
