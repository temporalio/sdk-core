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
    Core, CoreSDK,
};
use rand::{thread_rng, Rng};
use std::collections::VecDeque;

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
        .returning(move |_| {
            warn!("Hit 'server'");
            Ok(tasks.pop_front().unwrap())
        });
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    FakeCore::new(CoreSDK::new(mock_gateway), response_batches.len())
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

pub(crate) async fn poll_and_reply<'a>(
    core: &'a FakeCore,
    task_queue: &'a str,
    eviction_mode: EvictionMode,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut run_id = "".to_string();
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;
    // Counts how many times we have reset the expect/reply iterator
    let mut performed_wft_calls = 0;

    'outer: loop {
        let expect_iter = expect_and_reply.iter();
        warn!("replaying expectations");

        for interaction in expect_iter {
            warn!("Interaction loop");
            dbg!(&performed_wft_calls);
            dbg!(&core.expected_wft_calls);
            let (asserter, reply) = interaction;
            let res = core.inner.poll_workflow_task(task_queue).await.unwrap();
            if !res.from_pending {
                performed_wft_calls += 1;
            }

            asserter(&res);

            let task_tok = res.task_token;
            if run_id.is_empty() {
                run_id = res.run_id.clone();
            }

            core.inner
                .complete_workflow_task(WfActivationCompletion::from_status(
                    task_tok,
                    reply.clone(),
                ))
                .await
                .unwrap();

            dbg!(&performed_wft_calls);
            dbg!(&core.expected_wft_calls);
            match eviction_mode {
                EvictionMode::Sticky => unimplemented!(),
                EvictionMode::NotSticky => {
                    // If we are in non-sticky mode, we have to replay all expectations every
                    // time a wft is completed (hence there are no more pending activations)
                    if performed_wft_calls < core.expected_wft_calls
                        && !core.inner.pending_activations.has_pending(&res.run_id)
                    {
                        warn!("Nosticky not done");
                        continue 'outer;
                    }
                }
                EvictionMode::AfterEveryReply => {
                    if evictions < expected_evictions {
                        core.inner.evict_run(&run_id);
                        evictions += 1;
                        continue 'outer;
                    }
                }
            }
        }

        break;
    }
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
