use crate::{
    pollers::MockServerGatewayApis,
    test_help::{
        build_mock_pollers, canned_histories, mock_core, MockPollCfg, ResponseType,
        DEFAULT_WORKFLOW_TYPE, TEST_Q,
    },
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk::{TestRustWorker, WfContext, WorkflowResult};
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkflowTaskFailedCause;

static DID_FAIL: AtomicBool = AtomicBool::new(false);
pub async fn timer_wf_fails_once(ctx: WfContext) -> WorkflowResult<()> {
    ctx.timer(Duration::from_secs(1)).await;
    if DID_FAIL
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        panic!("Ahh");
    }
    Ok(().into())
}

/// Verifies that workflow panics (which in this case the Rust SDK turns into workflow activation
/// failures) are turned into unspecified WFT failures.
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

/// Verifies nondeterministic behavior in workflows results in automatic WFT failure with the
/// appropriate nondeterminism cause.
#[rstest::rstest]
#[case::with_cache(true)]
#[case::without_cache(false)]
#[tokio::test]
async fn test_wf_task_rejected_properly_due_to_nondeterminism(#[case] use_cache: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_timer_wf_completes("1");
    let mock = MockServerGatewayApis::new();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        // Two polls are needed, since the first will fail
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
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

    let started_count: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| async move {
        // The workflow is replaying all of history, so the when it schedules an extra timer it
        // should not have, it causes a nondeterminism error.
        if started_count.fetch_add(1, Ordering::Relaxed) == 0 {
            ctx.timer(Duration::from_secs(1)).await;
        }
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    worker
        .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // Started count is two since we start, restart once due to error, then we unblock the real
    // timer and proceed without restarting
    assert_eq!(2, started_count.load(Ordering::Relaxed));
}
