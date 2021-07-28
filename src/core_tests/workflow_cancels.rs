use crate::{
    job_assert,
    protos::coresdk::{
        workflow_activation::{wf_activation_job, WfActivationJob},
        workflow_commands::{
            CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution, StartTimer,
        },
    },
    test_help::{
        build_fake_core, canned_histories, gen_assert_and_reply, poll_and_reply, ResponseType,
    },
    workflow::WorkflowCachingPolicy::NonSticky,
};
use rstest::rstest;

enum CompletionType {
    Complete,
    Fail,
    Cancel,
}

#[rstest]
#[case::incremental_cancel(vec![1.into(), ResponseType::AllHistory], CompletionType::Cancel)]
#[case::replay_cancel(vec![ResponseType::AllHistory], CompletionType::Cancel)]
#[case::incremental_complete(vec![1.into(), ResponseType::AllHistory], CompletionType::Complete)]
#[case::replay_complete(vec![ResponseType::AllHistory], CompletionType::Complete)]
#[case::incremental_fail(vec![1.into(), ResponseType::AllHistory], CompletionType::Fail)]
#[case::replay_fail(vec![ResponseType::AllHistory], CompletionType::Fail)]
#[tokio::test]
async fn timer_then_cancel_req(
    #[case] hist_batches: Vec<ResponseType>,
    #[case] completion_type: CompletionType,
) {
    let wfid = "fake_wf_id";
    let timer_id = "timer";
    let t = match completion_type {
        CompletionType::Complete => canned_histories::timer_wf_cancel_req_completed(timer_id),
        CompletionType::Fail => canned_histories::timer_wf_cancel_req_failed(timer_id),
        CompletionType::Cancel => canned_histories::timer_wf_cancel_req_cancelled(timer_id),
    };
    let core = build_fake_core(wfid, t, hist_batches);

    let final_cmd = match completion_type {
        CompletionType::Complete => CompleteWorkflowExecution::default().into(),
        CompletionType::Fail => FailWorkflowExecution::default().into(),
        CompletionType::Cancel => CancelWorkflowExecution::default().into(),
    };

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![StartTimer {
                    timer_id: timer_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    wf_activation_job::Variant::FireTimer(_),
                    wf_activation_job::Variant::CancelWorkflow(_)
                ),
                vec![final_cmd],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn timer_then_cancel_req_then_timer_then_cancelled() {
    let wfid = "fake_wf_id";
    let t = canned_histories::timer_wf_cancel_req_do_another_timer_then_cancelled();
    let core = build_fake_core(wfid, t, [ResponseType::AllHistory]);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![StartTimer {
                    timer_id: "t1".to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    wf_activation_job::Variant::FireTimer(_),
                    wf_activation_job::Variant::CancelWorkflow(_)
                ),
                vec![StartTimer {
                    timer_id: "t2".to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![CancelWorkflowExecution::default().into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn immediate_cancel() {
    let wfid = "fake_wf_id";
    let t = canned_histories::immediate_wf_cancel();
    let core = build_fake_core(wfid, t, &[1]);

    poll_and_reply(
        &core,
        NonSticky,
        &[gen_assert_and_reply(
            &job_assert!(
                wf_activation_job::Variant::StartWorkflow(_),
                wf_activation_job::Variant::CancelWorkflow(_)
            ),
            vec![CancelWorkflowExecution {}.into()],
        )],
    )
    .await;
}
