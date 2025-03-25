use crate::integ_tests::mk_nexus_endpoint;
use anyhow::bail;
use assert_matches::assert_matches;
use std::time::Duration;
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{CancellableFuture, NexusOperationOptions, WfContext, WfExitValue};
use temporal_sdk_core_api::errors::PollError;
use temporal_sdk_core_protos::{
    coresdk::{
        FromJsonPayloadExt,
        nexus::{
            NexusOperationResult, NexusTaskCancelReason, NexusTaskCompletion,
            nexus_operation_result, nexus_task, nexus_task_completion,
        },
    },
    temporal::api::{
        common::v1::{Callback, callback},
        enums::v1::NexusHandlerErrorRetryBehavior,
        failure::v1::failure::FailureInfo,
        nexus,
        nexus::v1::{
            CancelOperationResponse, HandlerError, StartOperationResponse, request,
            start_operation_response, workflow_event_link_from_nexus,
        },
    },
};
use temporal_sdk_core_test_utils::{CoreWfStarter, rand_6_chars};
use tokio::{join, sync::mpsc};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Outcome {
    Succeed,
    Fail,
    Cancel,
    CancelAfterRecordedBeforeStarted,
    Timeout,
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_basic(
    #[values(Outcome::Succeed, Outcome::Fail, Outcome::Timeout)] outcome: Outcome,
) {
    let wf_name = "nexus_basic";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            let started = ctx
                .start_nexus_operation(NexusOperationOptions {
                    endpoint,
                    service: "svc".to_string(),
                    operation: "op".to_string(),
                    schedule_to_close_timeout: Some(Duration::from_secs(3)),
                    ..Default::default()
                })
                .await
                .unwrap();
            let res = started.result().await;
            Ok(res.into())
        }
    });
    starter.start_with_worker(wf_name, &mut worker).await;

    let client = starter.get_client().await.get_client().clone();
    let nexus_task_handle = async {
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        match outcome {
            Outcome::Succeed => {
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: nt.task_token,
                        status: Some(nexus_task_completion::Status::Completed(
                            nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            start_operation_response::Variant::SyncSuccess(
                                                start_operation_response::Sync {
                                                    payload: Some("yay".into()),
                                                    links: vec![],
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            },
                        )),
                    })
                    .await
                    .unwrap();
            }
            Outcome::Fail | Outcome::Timeout => {
                if outcome == Outcome::Timeout {
                    // Wait for the timeout task cancel to get sent
                    let timeout_t = core_worker.poll_nexus_task().await.unwrap();
                    let cancel = assert_matches!(timeout_t.variant,
                        Some(nexus_task::Variant::CancelTask(ct)) => ct);
                    assert_eq!(cancel.reason, NexusTaskCancelReason::TimedOut as i32);
                }
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: nt.task_token,
                        status: Some(nexus_task_completion::Status::Error(HandlerError {
                            error_type: "BAD_REQUEST".to_string(), // bad req is non-retryable
                            failure: Some(nexus::v1::Failure {
                                message: "busted".to_string(),
                                ..Default::default()
                            }),
                            retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                        })),
                    })
                    .await
                    .unwrap();
            }
            Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => unreachable!(),
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(nexus_task_handle, async {
        worker.run_until_done().await.unwrap();
    });

    let res = client
        .get_untyped_workflow_handle(starter.get_task_queue().to_owned(), "")
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let res = NexusOperationResult::from_json_payload(&res.unwrap_success()[0]).unwrap();
    match outcome {
        Outcome::Succeed => {
            let p = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Completed(p)) => p
            );
            assert_eq!(p.data, b"yay");
        }
        Outcome::Fail => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Failed(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
        }
        Outcome::Timeout => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::TimedOut(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation timed out");
        }
        Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => unreachable!(),
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_async(
    #[values(
        Outcome::Succeed,
        Outcome::Fail,
        Outcome::Cancel,
        Outcome::CancelAfterRecordedBeforeStarted,
        Outcome::Timeout
    )]
    outcome: Outcome,
) {
    let wf_name = "nexus_async";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let schedule_to_close_timeout = if outcome == Outcome::CancelAfterRecordedBeforeStarted {
        // If we set this, it'll time out before we can cancel it.
        None
    } else {
        Some(Duration::from_secs(5))
    };

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            let started = ctx.start_nexus_operation(NexusOperationOptions {
                endpoint,
                service: "svc".to_string(),
                operation: "op".to_string(),
                schedule_to_close_timeout,
                ..Default::default()
            });
            if outcome == Outcome::CancelAfterRecordedBeforeStarted {
                ctx.timer(Duration::from_millis(1)).await;
                started.cancel(&ctx);
            }
            let started = started.await.unwrap();
            let result = started.result();
            if matches!(outcome, Outcome::Cancel) {
                started.cancel(&ctx);
                // Make sure double-cancel doesn't cause problems
                started.cancel(&ctx);
            }
            let res = result.await;
            // Make sure cancel after completion doesn't cause problems
            started.cancel(&ctx);
            Ok(res.into())
        }
    });
    worker.register_wf(
        "async_completer".to_owned(),
        move |ctx: WfContext| async move {
            match outcome {
                Outcome::Succeed => Ok("completed async".into()),
                Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => {
                    ctx.cancelled().await;
                    Ok(WfExitValue::Cancelled)
                }
                _ => bail!("broken"),
            }
        },
    );
    let submitter = worker.get_submitter_handle();
    starter.start_with_worker(wf_name, &mut worker).await;

    let client = starter.get_client().await.get_client().clone();
    let nexus_task_handle = async {
        let mut nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        let start_req = assert_matches!(
            nt.request.unwrap().variant.unwrap(),
            request::Variant::StartOperation(sr) => sr
        );
        let completer_id = format!("completer-{}", rand_6_chars());
        if !matches!(outcome, Outcome::Timeout) {
            if outcome == Outcome::CancelAfterRecordedBeforeStarted {
                // Server does not permit cancels to happen in this state. So, we wait for one timeout
                // to happen, then say the operation started, after which it will be cancelled.
                let ntt = core_worker.poll_nexus_task().await.unwrap();
                assert_matches!(ntt.variant, Some(nexus_task::Variant::CancelTask(_)));
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: ntt.task_token().to_vec(),
                        status: Some(nexus_task_completion::Status::AckCancel(true)),
                    })
                    .await
                    .unwrap();
                // Get the next start request
                nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
            }
            // Start the workflow which will act like the nexus handler and complete the async
            // operation
            submitter
                .submit_wf(
                    completer_id.clone(),
                    "async_completer",
                    vec![],
                    WorkflowOptions {
                        links: start_req
                            .links
                            .iter()
                            .map(workflow_event_link_from_nexus)
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap(),
                        completion_callbacks: vec![Callback {
                            variant: Some(callback::Variant::Nexus(callback::Nexus {
                                url: start_req.callback,
                                header: start_req.callback_header,
                            })),
                        }],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        core_worker
            .complete_nexus_task(NexusTaskCompletion {
                task_token: nt.task_token,
                status: Some(nexus_task_completion::Status::Completed(
                    nexus::v1::Response {
                        variant: Some(nexus::v1::response::Variant::StartOperation(
                            StartOperationResponse {
                                variant: Some(start_operation_response::Variant::AsyncSuccess(
                                    start_operation_response::Async {
                                        operation_id: "op-1".to_string(),
                                        links: vec![],
                                        operation_token: "op-1".to_string(),
                                    },
                                )),
                            },
                        )),
                    },
                )),
            })
            .await
            .unwrap();
        if matches!(
            outcome,
            Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted
        ) {
            let nt = core_worker.poll_nexus_task().await.unwrap();
            let nt = nt.unwrap_task();
            assert_matches!(
                nt.request.unwrap().variant.unwrap(),
                request::Variant::CancelOperation(_)
            );
            client
                .cancel_workflow_execution(completer_id, None, "nexus cancel".to_string(), None)
                .await
                .unwrap();
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token,
                    status: Some(nexus_task_completion::Status::Completed(
                        nexus::v1::Response {
                            variant: Some(nexus::v1::response::Variant::CancelOperation(
                                CancelOperationResponse {},
                            )),
                        },
                    )),
                })
                .await
                .unwrap();
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(nexus_task_handle, async {
        worker.run_until_done().await.unwrap();
    });

    let res = client
        .get_untyped_workflow_handle(starter.get_task_queue().to_owned(), "")
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let res = NexusOperationResult::from_json_payload(&res.unwrap_success()[0]).unwrap();
    match outcome {
        Outcome::Succeed => {
            let p = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Completed(p)) => p
            );
            assert_eq!(p.data, b"\"completed async\"");
        }
        Outcome::Fail => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Failed(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "broken");
        }
        Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation canceled");
        }
        Outcome::Timeout => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::TimedOut(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation timed out");
        }
    }
}
#[tokio::test]
async fn nexus_cancel_before_start() {
    let wf_name = "nexus_cancel_before_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            let started = ctx.start_nexus_operation(NexusOperationOptions {
                endpoint: endpoint.clone(),
                service: "svc".to_string(),
                operation: "op".to_string(),
                ..Default::default()
            });
            started.cancel(&ctx);
            let res = started.await.unwrap_err();
            assert_eq!(res.message, "Nexus Operation cancelled before scheduled");
            if let FailureInfo::NexusOperationExecutionFailureInfo(fi) = res.failure_info.unwrap() {
                assert_eq!(fi.endpoint, endpoint);
                assert_eq!(fi.service, "svc");
                assert_eq!(fi.operation, "op");
            } else {
                panic!("unexpected failure info");
            }
            Ok(().into())
        }
    });
    starter.start_with_worker(wf_name, &mut worker).await;

    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_must_complete_task_to_shutdown(#[values(true, false)] use_grace_period: bool) {
    let wf_name = "nexus_must_complete_task_to_shutdown";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    if use_grace_period {
        starter
            .worker_config
            .graceful_shutdown_period(Duration::from_millis(500));
    }
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            // We just need to create the command, not await it.
            drop(ctx.start_nexus_operation(NexusOperationOptions {
                endpoint: endpoint.clone(),
                service: "svc".to_string(),
                operation: "op".to_string(),
                ..Default::default()
            }));
            // Workflow completes right away, only having scheduled the operation. We need a timer
            // to make sure the nexus task actually gets scheduled.
            ctx.timer(Duration::from_millis(1)).await;
            // started.await.unwrap();
            Ok(().into())
        }
    });
    let handle = starter.start_with_worker(wf_name, &mut worker).await;
    let (complete_order_tx, mut complete_order_rx) = mpsc::unbounded_channel();

    let task_handle = async {
        // Should get the nexus task first
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        // The workflow will complete
        handle
            .get_workflow_result(Default::default())
            .await
            .unwrap();
        if use_grace_period {
            // Wait for cancel to be sent
            let nt = core_worker.poll_nexus_task().await.unwrap();
            assert_matches!(nt.variant, Some(nexus_task::Variant::CancelTask(_)));
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token().to_vec(),
                    status: Some(nexus_task_completion::Status::AckCancel(true)),
                })
                .await
                .unwrap();
        } else {
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token,
                    status: Some(nexus_task_completion::Status::Error(HandlerError {
                        error_type: "BAD_REQUEST".to_string(), // bad req is non-retryable
                        failure: Some(nexus::v1::Failure {
                            message: "busted".to_string(),
                            ..Default::default()
                        }),
                        retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                    })),
                })
                .await
                .unwrap();
        }
        complete_order_tx.send("t").unwrap();
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(task_handle, async {
        worker.run_until_done().await.unwrap();
        complete_order_tx.send("w").unwrap();
    });

    // The first thing to finish needs to have been the nexus task completion
    assert_eq!(complete_order_rx.recv().await.unwrap(), "t");
}
