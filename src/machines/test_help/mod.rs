type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

mod async_workflow_driver;
mod history_builder;

pub(super) use async_workflow_driver::{CommandSender, TestWorkflowDriver};
pub(crate) use history_builder::TestHistoryBuilder;

use crate::protos::coresdk::common::UserCodeFailure;
use crate::{
    pollers::MockServerGatewayApis,
    protos::{
        coresdk::{
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

pub(crate) type FakeCore = CoreSDK<MockServerGatewayApis>;

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the core SDK with a mock server gateway that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned.
/// For each number in the input list, a fake response will be prepared which includes history
/// up to the workflow task with that number, as in [TestHistoryBuilder::get_history_info].
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
        .returning(move |_| Ok(tasks.pop_front().unwrap()));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    CoreSDK::new(mock_gateway)
}

// TODO: In reality this whole thing might be better done by just using the async wf driver --
//  then we don't need to bother with the commands being issued and assertions, but keeping this
//  probably has some value still to test without it getting in the way, too.

type AsserterWithReply<'a> = (&'a dyn Fn(&WfActivation), wf_activation_completion::Status);

pub(crate) async fn poll_and_reply<'a>(
    core: &'a FakeCore,
    task_queue: &'a str,
    evict_after_each_reply: bool,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut run_id = "".to_string();
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for interaction in expect_iter {
            match interaction {
                (asserter, reply) => {
                    let res = core.poll_workflow_task(task_queue).await.unwrap();

                    asserter(&res);

                    let task_tok = res.task_token;
                    if run_id.is_empty() {
                        run_id = res.run_id;
                    }

                    core.complete_workflow_task(WfActivationCompletion::from_status(
                        task_tok,
                        reply.clone(),
                    ))
                    .await
                    .unwrap();

                    if evict_after_each_reply && evictions < expected_evictions {
                        core.evict_run(&run_id);
                        evictions += 1;
                        continue 'outer;
                    }
                }
            }
        }

        break;
    }
}

pub(crate) fn gen_assert_and_reply<'a>(
    asserter: &'a dyn Fn(&WfActivation),
    reply_commands: Vec<workflow_command::Variant>,
) -> AsserterWithReply<'a> {
    (
        asserter,
        workflow_completion::Success::from_cmds(reply_commands).into(),
    )
}

pub(crate) fn gen_assert_and_fail<'a>(
    asserter: &'a dyn Fn(&WfActivation),
) -> AsserterWithReply<'a> {
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
