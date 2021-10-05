use crate::{
    pollers::MockServerGatewayApis,
    prototype_rust_sdk::{TestRustWorker, WfContext, WorkflowResult},
    test_help::{
        build_mock_pollers, canned_histories, mock_core, MockPollCfg, ResponseType,
        DEFAULT_WORKFLOW_TYPE, TEST_Q,
    },
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkflowTaskFailedCause;

static DID_FAIL: AtomicBool = AtomicBool::new(false);
pub async fn timer_wf_fails_once(mut ctx: WfContext) -> WorkflowResult<()> {
    ctx.timer(Duration::from_secs(1)).await;
    if !DID_FAIL.load(Ordering::Relaxed) {
        DID_FAIL.store(true, Ordering::Relaxed);
        panic!("Ahh");
    }
    Ok(().into())
}

#[tokio::test]
async fn test_wf_task_rejected_properly() {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::workflow_fails_with_failure_after_timer("1");
    let mock = MockServerGatewayApis::new();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2], mock);
    // We should see one wft failure which has unspecified cause, since panics don't have a defined
    // type.
    mh.num_expected_fails = Some(1);
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::Unspecified));
    let mock = build_mock_pollers(mh);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(wf_type.to_owned(), timer_wf_fails_once);
    worker
        .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::with_cache(true)]
#[case::without_cache(false)]
#[tokio::test]
async fn test_wf_task_rejected_properly_due_to_nondeterminism(#[case] use_cache: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_timer_wf_completes("1");
    let mock = MockServerGatewayApis::new();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    // We should see one wft failure which has nondeterminism cause
    mh.num_expected_fails = Some(1);
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let mut mock = build_mock_pollers(mh);
    if use_cache {
        mock.worker_cfg(TEST_Q, |cfg| {
            cfg.max_cached_workflows = 2;
        });
    }
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(wf_type.to_owned(), |mut ctx: WfContext| {
        let did_nondeterminism = AtomicBool::new(false);
        async move {
            ctx.timer(Duration::from_secs(1)).await;
            if !did_nondeterminism.load(Ordering::Relaxed) {
                did_nondeterminism.store(true, Ordering::Relaxed);
                ctx.timer(Duration::from_secs(1)).await;
            }
            Ok(().into())
        }
    });

    worker
        .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
        .await
        .unwrap();
    // TODO: Shouldn't return error, should just queue eviction per
    //  https://github.com/temporalio/sdk-core/issues/171
    assert!(worker.run_until_done().await.is_err());
}
