use anyhow::bail;
use assert_matches::assert_matches;
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk::{CancellableFuture, NexusOperationOptions, WfContext};
use temporal_sdk_core_protos::{
    coresdk::{
        nexus::{nexus_operation_result, NexusOperationResult},
        FromJsonPayloadExt,
    },
    temporal::api::{
        common::v1::{callback, Callback},
        enums::v1::TaskQueueKind,
        failure::v1::failure::FailureInfo,
        nexus,
        nexus::v1::{
            endpoint_target, start_operation_response, workflow_event_link_from_nexus,
            EndpointSpec, EndpointTarget, HandlerError, StartOperationResponse,
        },
        operatorservice::v1::CreateNexusEndpointRequest,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            PollNexusTaskQueueRequest, RespondNexusTaskCompletedRequest,
            RespondNexusTaskFailedRequest,
        },
    },
};
use temporal_sdk_core_test_utils::{rand_6_chars, CoreWfStarter};
use tokio::join;

#[rstest::rstest]
#[tokio::test]
async fn nexus_basic(#[values(true, false)] succeed: bool) {
    let wf_name = "nexus_basic";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    let endpoint = mk_endpoint(&mut starter).await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            let started = ctx
                .start_nexus_operation(NexusOperationOptions {
                    endpoint,
                    service: "svc".to_string(),
                    operation: "op".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let res = started.result().await;
            Ok(res.into())
        }
    });
    starter.start_with_worker(wf_name, &mut worker).await;

    let mut client = starter.get_client().await.get_client().clone();
    let nexus_task_handle = async {
        let nt = client
            .poll_nexus_task_queue(PollNexusTaskQueueRequest {
                namespace: client.namespace().to_owned(),
                task_queue: Some(TaskQueue {
                    name: starter.get_task_queue().to_owned(),
                    kind: TaskQueueKind::Normal.into(),
                    normal_name: "".to_string(),
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        if succeed {
            client
                .respond_nexus_task_completed(RespondNexusTaskCompletedRequest {
                    namespace: client.namespace().to_owned(),
                    task_token: nt.task_token,
                    response: Some(nexus::v1::Response {
                        variant: Some(nexus::v1::response::Variant::StartOperation(
                            StartOperationResponse {
                                variant: Some(start_operation_response::Variant::SyncSuccess(
                                    start_operation_response::Sync {
                                        payload: Some("yay".into()),
                                    },
                                )),
                            },
                        )),
                    }),
                    ..Default::default()
                })
                .await
                .unwrap();
        } else {
            client
                .respond_nexus_task_failed(RespondNexusTaskFailedRequest {
                    namespace: client.namespace().to_owned(),
                    task_token: nt.task_token,
                    error: Some(HandlerError {
                        error_type: "BAD_REQUEST".to_string(), // bad req is non-retryable
                        failure: Some(nexus::v1::Failure {
                            message: "busted".to_string(),
                            ..Default::default()
                        }),
                    }),
                    identity: "whatever".to_string(),
                })
                .await
                .unwrap();
        };
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
    if succeed {
        let p = assert_matches!(
            res.status,
            Some(nexus_operation_result::Status::Completed(p)) => p
        );
        assert_eq!(p.data, b"yay");
    } else {
        let f = assert_matches!(
            res.status,
            Some(nexus_operation_result::Status::Failed(f)) => f
        );
        assert_eq!(f.message, "nexus operation completed unsuccessfully");
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_async(#[values(true, false)] succeed: bool) {
    let wf_name = "nexus_async";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    let endpoint = mk_endpoint(&mut starter).await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let endpoint = endpoint.clone();
        async move {
            let started = ctx
                .start_nexus_operation(NexusOperationOptions {
                    endpoint,
                    service: "svc".to_string(),
                    operation: "op".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let res = started.result().await;
            Ok(res.into())
        }
    });
    worker.register_wf(
        "async_completer".to_owned(),
        move |_: WfContext| async move {
            if succeed {
                Ok("completed async".into())
            } else {
                bail!("broken")
            }
        },
    );
    let submitter = worker.get_submitter_handle();
    starter.start_with_worker(wf_name, &mut worker).await;

    let mut client = starter.get_client().await.get_client().clone();
    let nexus_task_handle = async {
        let nt = client
            .poll_nexus_task_queue(PollNexusTaskQueueRequest {
                namespace: client.namespace().to_owned(),
                task_queue: Some(TaskQueue {
                    name: starter.get_task_queue().to_owned(),
                    kind: TaskQueueKind::Normal.into(),
                    normal_name: "".to_string(),
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        // Start the workflow which will act like the nexus handler and complete the async operation
        let start_req = if let nexus::v1::request::Variant::StartOperation(sr) =
            nt.request.unwrap().variant.unwrap()
        {
            sr
        } else {
            panic!("unexpected request variant");
        };
        submitter
            .submit_wf(
                format!("completer-{}", rand_6_chars()),
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
        client
            .respond_nexus_task_completed(RespondNexusTaskCompletedRequest {
                namespace: client.namespace().to_owned(),
                task_token: nt.task_token,
                response: Some(nexus::v1::Response {
                    variant: Some(nexus::v1::response::Variant::StartOperation(
                        StartOperationResponse {
                            variant: Some(start_operation_response::Variant::AsyncSuccess(
                                start_operation_response::Async {
                                    operation_id: "op-1".to_string(),
                                    links: vec![],
                                },
                            )),
                        },
                    )),
                }),
                ..Default::default()
            })
            .await
            .unwrap();
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
    if succeed {
        let p = assert_matches!(
            res.status,
            Some(nexus_operation_result::Status::Completed(p)) => p
        );
        assert_eq!(p.data, b"\"completed async\"");
    } else {
        let f = assert_matches!(
            res.status,
            Some(nexus_operation_result::Status::Failed(f)) => f
        );
        assert_eq!(f.message, "nexus operation completed unsuccessfully");
        assert_eq!(f.cause.unwrap().message, "broken");
    }
}
#[tokio::test]
async fn nexus_cancel_before_start() {
    let wf_name = "nexus_cancel_before_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    let endpoint = mk_endpoint(&mut starter).await;

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

async fn mk_endpoint(starter: &mut CoreWfStarter) -> String {
    let client = starter.get_client().await;
    let endpoint = format!("mycoolendpoint-{}", rand_6_chars());
    let mut op_client = client.get_client().inner().operator_svc().clone();
    op_client
        .create_nexus_endpoint(CreateNexusEndpointRequest {
            spec: Some(EndpointSpec {
                name: endpoint.to_owned(),
                description: None,
                target: Some(EndpointTarget {
                    variant: Some(endpoint_target::Variant::Worker(endpoint_target::Worker {
                        namespace: client.namespace().to_owned(),
                        task_queue: starter.get_task_queue().to_owned(),
                    })),
                }),
            }),
        })
        .await
        .unwrap();
    endpoint
}
