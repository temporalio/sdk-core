use assert_matches::assert_matches;
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowService};
use temporal_sdk::{NexusOperationOptions, WfContext};
use temporal_sdk_core_protos::{
    coresdk::{
        nexus::{nexus_operation_result, NexusOperationResult},
        FromJsonPayloadExt,
    },
    temporal::api::{
        enums::v1::TaskQueueKind,
        nexus,
        nexus::v1::{
            endpoint_target, start_operation_response, EndpointSpec, EndpointTarget, HandlerError,
            StartOperationResponse,
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
