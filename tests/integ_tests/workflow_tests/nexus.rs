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
            NexusOperationCancellationType, NexusOperationResult, NexusTaskCancelReason,
            NexusTaskCompletion, nexus_operation_result, nexus_task, nexus_task_completion,
        },
    },
    temporal::api::{
        common::v1::{Callback, callback},
        enums::v1::NexusHandlerErrorRetryBehavior,
        failure::v1::{Failure, failure::FailureInfo},
        nexus,
        nexus::v1::{
            CancelOperationResponse, HandlerError, StartOperationResponse, request,
            start_operation_response, workflow_event_link_from_nexus,
        },
    },
};
use temporal_sdk_core_test_utils::{CoreWfStarter, WorkflowHandleExt, rand_6_chars};
use tokio::{
    join,
    sync::{mpsc, watch},
};

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
            match ctx
                .start_nexus_operation(NexusOperationOptions {
                    endpoint,
                    service: "svc".to_string(),
                    operation: "op".to_string(),
                    schedule_to_close_timeout: Some(Duration::from_secs(3)),
                    ..Default::default()
                })
                .await
            {
                Ok(started) => {
                    assert_eq!(started.operation_token, None);
                    let res = started.result().await;
                    Ok(Ok(res).into())
                }
                Err(failure) => Ok(Err(failure).into()),
            }
        }
    });
    let wf_handle = starter.start_with_worker(wf_name, &mut worker).await;

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
            _ => unreachable!(),
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
    let res = Result::<NexusOperationResult, Failure>::from_json_payload(&res.unwrap_success()[0])
        .unwrap();
    match outcome {
        Outcome::Succeed => {
            let p = assert_matches!(
                res.unwrap().status,
                Some(nexus_operation_result::Status::Completed(p)) => p
            );
            assert_eq!(p.data, b"yay");
        }
        Outcome::Fail => {
            let f = res.unwrap_err();
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
        }
        Outcome::Timeout => {
            let f = res.unwrap_err();
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            let cause = f.cause.unwrap();
            assert_eq!(cause.message, "operation timed out");
            assert_matches!(
                f.failure_info,
                Some(FailureInfo::NexusOperationExecutionFailureInfo(_))
            );
            assert_matches!(cause.failure_info, Some(FailureInfo::TimeoutFailureInfo(_)));
        }
        _ => unreachable!(),
    }
    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
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
    let wf_handle = starter.start_with_worker(wf_name, &mut worker).await;

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
            let links = start_req
                .links
                .iter()
                .map(workflow_event_link_from_nexus)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            // Start the workflow which will act like the nexus handler and complete the async
            // operation
            submitter
                .submit_wf(
                    completer_id.clone(),
                    "async_completer",
                    vec![],
                    WorkflowOptions {
                        completion_callbacks: vec![Callback {
                            variant: Some(callback::Variant::Nexus(callback::Nexus {
                                url: start_req.callback,
                                header: start_req.callback_header,
                            })),
                            links: links.clone(),
                        }],
                        // Also send links in the root of the workflow options for older servers.
                        links,
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
                                    #[allow(deprecated)]
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
    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
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
    let handle = starter.start_with_worker(wf_name, &mut worker).await;

    worker.run_until_done().await.unwrap();

    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
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

    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_cancellation_types(
    #[values(
        NexusOperationCancellationType::Abandon,
        NexusOperationCancellationType::TryCancel,
        NexusOperationCancellationType::WaitCancellationRequested,
        NexusOperationCancellationType::WaitCancellationCompleted
    )]
    cancellation_type: NexusOperationCancellationType,
) {
    let wf_name = "nexus_cancellation_types";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let schedule_to_close_timeout = Some(Duration::from_secs(5));

    let (cancel_call_completion_tx, cancel_call_completion_rx) = watch::channel(false);
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        let cancel_call_completion_tx = cancel_call_completion_tx.clone();
        async move {
            let options = NexusOperationOptions {
                endpoint,
                service: "svc".to_string(),
                operation: "op".to_string(),
                schedule_to_close_timeout,
                cancellation_type: Some(cancellation_type),
                ..Default::default()
            };
            let started = ctx.start_nexus_operation(options);
            let started = started.await.unwrap();
            let result = started.result();
            started.cancel(&ctx);
            started.cancel(&ctx);

            let res = result.await;
            cancel_call_completion_tx.send(true).unwrap();

            // Make sure cancel after completion doesn't cause problems
            started.cancel(&ctx);

            // We need to wait slightly so that the workflow is not complete at the same time
            // cancellation is invoked. If it does, the caller workflow will close and the server won't attempt to send the cancellation to the handler
            ctx.timer(Duration::from_millis(1)).await;
            Ok(res.into())
        }
    });

    let (cancellation_wait_tx, cancellation_wait_rx) = watch::channel(false);
    let (cancellation_tx, mut cancellation_rx) = watch::channel(false);
    worker.register_wf("async_completer".to_owned(), move |ctx: WfContext| {
        let cancellation_tx = cancellation_tx.clone();
        let mut cancellation_wait_rx = cancellation_wait_rx.clone();
        async move {
            ctx.cancelled().await;
            cancellation_tx.send(true).unwrap();
            if cancellation_type == NexusOperationCancellationType::WaitCancellationCompleted {
                cancellation_wait_rx.changed().await.unwrap();
            }
            Ok(WfExitValue::<()>::Cancelled)
        }
    });
    let submitter = worker.get_submitter_handle();
    let wf_handle = starter.start_with_worker(wf_name, &mut worker).await;
    let client = starter.get_client().await.get_client().clone();
    let nexus_task_handle = async {
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        let start_req = assert_matches!(
            nt.request.unwrap().variant.unwrap(),
            request::Variant::StartOperation(sr) => sr
        );
        let completer_id = format!("completer-{}", rand_6_chars());
        let links = start_req
            .links
            .iter()
            .map(workflow_event_link_from_nexus)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Start the workflow which will act like the nexus handler and complete the async
        // operation
        submitter
            .submit_wf(
                completer_id.clone(),
                "async_completer",
                vec![],
                WorkflowOptions {
                    completion_callbacks: vec![Callback {
                        variant: Some(callback::Variant::Nexus(callback::Nexus {
                            url: start_req.callback,
                            header: start_req.callback_header,
                        })),
                        links: links.clone(),
                    }],
                    // Also send links in the root of the workflow options for older servers.
                    links,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        core_worker
            .complete_nexus_task(NexusTaskCompletion {
                task_token: nt.task_token,
                status: Some(nexus_task_completion::Status::Completed(
                    nexus::v1::Response {
                        variant: Some(nexus::v1::response::Variant::StartOperation(
                            StartOperationResponse {
                                variant: Some(start_operation_response::Variant::AsyncSuccess(
                                    #[allow(deprecated)]
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

        match cancellation_type {
            NexusOperationCancellationType::WaitCancellationCompleted
            | NexusOperationCancellationType::WaitCancellationRequested => {
                assert!(!*cancel_call_completion_rx.borrow());
            }
            NexusOperationCancellationType::Abandon | NexusOperationCancellationType::TryCancel => {
                wf_handle
                    .get_workflow_result(Default::default())
                    .await
                    .unwrap();
                assert!(*cancel_call_completion_rx.borrow())
            }
        }
        if cancellation_type != NexusOperationCancellationType::Abandon {
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

        // Confirm the caller WF has not completed even after the handling of the cancel request
        if cancellation_type == NexusOperationCancellationType::WaitCancellationCompleted {
            assert!(!*cancel_call_completion_rx.borrow());

            // It only completes after the handler WF terminates
            cancellation_wait_tx.send(true).unwrap();
            wf_handle
                .get_workflow_result(Default::default())
                .await
                .unwrap();
            assert!(*cancel_call_completion_rx.borrow());
        }

        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    let shutdown_handle = worker.inner_mut().shutdown_handle();
    join!(
        nexus_task_handle,
        async { worker.inner_mut().run().await.unwrap() },
        async {
            wf_handle
                .get_workflow_result(Default::default())
                .await
                .unwrap();
            if cancellation_type == NexusOperationCancellationType::TryCancel {
                cancellation_rx.changed().await.unwrap();
            }
            shutdown_handle();
        }
    );

    match cancellation_type {
        NexusOperationCancellationType::Abandon => {
            assert!(!*cancellation_rx.borrow());
        }
        NexusOperationCancellationType::TryCancel
        | NexusOperationCancellationType::WaitCancellationRequested
        | NexusOperationCancellationType::WaitCancellationCompleted => {
            assert!(*cancellation_rx.borrow())
        }
    }

    let res = client
        .get_untyped_workflow_handle(starter.get_task_queue().to_owned(), "")
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let res = NexusOperationResult::from_json_payload(&res.unwrap_success()[0]).unwrap();

    match cancellation_type {
        NexusOperationCancellationType::Abandon | NexusOperationCancellationType::TryCancel => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "Nexus operation cancelled after starting");
        }
        NexusOperationCancellationType::WaitCancellationRequested
        | NexusOperationCancellationType::WaitCancellationCompleted => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation canceled");
        }
    }

    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}
