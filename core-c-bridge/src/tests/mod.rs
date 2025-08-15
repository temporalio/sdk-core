use crate::ByteArrayRef;
use crate::client::{
    ClientGrpcOverrideRequest, ClientGrpcOverrideResponse, RpcService,
    temporal_core_client_grpc_override_request_headers,
    temporal_core_client_grpc_override_request_proto,
    temporal_core_client_grpc_override_request_respond,
    temporal_core_client_grpc_override_request_rpc,
    temporal_core_client_grpc_override_request_service,
};
use crate::tests::utils::{
    OwnedRpcCallOptions, RpcCallError, default_client_options, default_server_config,
};
use context::Context;
use prost::Message;
use std::sync::{Arc, LazyLock, Mutex};
use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    GetSystemInfoRequest, GetSystemInfoResponse, QueryWorkflowRequest,
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
};

mod context;
mod utils;

#[test]
fn test_get_system_info() {
    Context::with(|context| {
        context.runtime_new().unwrap();
        context
            .start_dev_server(Box::new(default_server_config()))
            .unwrap();
        context
            .client_connect(Box::new(default_client_options(
                &context.ephemeral_server_target().unwrap().unwrap(),
            )))
            .unwrap();
        let result = GetSystemInfoResponse::decode(
            &*context
                .rpc_call(Box::new(OwnedRpcCallOptions {
                    service: RpcService::Workflow,
                    rpc: "GetSystemInfo".into(),
                    req: GetSystemInfoRequest {}.encode_to_vec(),
                    retry: false,
                    metadata: None,
                    timeout_millis: 0,
                    cancellation_token: None,
                }))
                .unwrap(),
        );
        assert!(result.is_ok());
    });
}

fn rpc_call_exists(context: &Arc<Context>, service: RpcService, rpc: &str) -> bool {
    let result = context.rpc_call(Box::new(OwnedRpcCallOptions {
        service,
        rpc: rpc.into(),
        req: Vec::new(),
        retry: false,
        metadata: None,
        timeout_millis: 0,
        cancellation_token: None,
    }));

    if let Err(e) = result {
        // will panic on other types of errors
        e.downcast::<RpcCallError>().unwrap().message != format!("Unknown RPC call {rpc}")
    } else {
        true
    }
}

#[test]
fn test_missing_rpc_call_has_expected_error_message() {
    Context::with(|context| {
        context.runtime_new().unwrap();
        context
            .start_dev_server(Box::new(default_server_config()))
            .unwrap();
        context
            .client_connect(Box::new(default_client_options(
                &context.ephemeral_server_target().unwrap().unwrap(),
            )))
            .unwrap();

        assert!(!rpc_call_exists(
            context,
            RpcService::Workflow,
            "NonExistentMethod"
        ));
        assert!(!rpc_call_exists(
            context,
            RpcService::Operator,
            "NonExistentMethod"
        ));
        assert!(!rpc_call_exists(
            context,
            RpcService::Cloud,
            "NonExistentMethod"
        ));
        assert!(!rpc_call_exists(
            context,
            RpcService::Test,
            "NonExistentMethod"
        ));
        assert!(!rpc_call_exists(
            context,
            RpcService::Health,
            "NonExistentMethod"
        ));
    });
}

fn list_proto_methods(proto_def_str: &str) -> Vec<&str> {
    proto_def_str
        .lines()
        .map(|l| l.trim())
        .filter(|l| l.starts_with("rpc"))
        .map(|l| {
            let stripped = l.strip_prefix("rpc ").unwrap();
            stripped[..stripped.find('(').unwrap()].trim()
        })
        .collect()
}

fn all_rpc_calls_exist(context: &Arc<Context>, service: RpcService, proto: &str) -> bool {
    let mut ret = true;
    for rpc in list_proto_methods(proto) {
        if !rpc_call_exists(context, service, rpc) {
            eprintln!("RPC method {rpc} does not exist in service {service:?}");
            ret = false;
        }
    }
    ret
}

#[test]
fn test_all_rpc_calls_exist() {
    Context::with(|context| {
        context.runtime_new().unwrap();
        context
            .start_dev_server(Box::new(default_server_config()))
            .unwrap();
        context
            .client_connect(Box::new(default_client_options(
                &context.ephemeral_server_target().unwrap().unwrap(),
            )))
            .unwrap();

        assert!(all_rpc_calls_exist(
            context,
            RpcService::Workflow,
            include_str!(
                "../../../sdk-core-protos/protos/api_upstream/temporal/api/workflowservice/v1/service.proto"
            ),
        ));

        assert!(all_rpc_calls_exist(
            context,
            RpcService::Operator,
            include_str!(
                "../../../sdk-core-protos/protos/api_upstream/temporal/api/operatorservice/v1/service.proto"
            ),
        ));

        assert!(all_rpc_calls_exist(
            context,
            RpcService::Cloud,
            include_str!(
                "../../../sdk-core-protos/protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto"
            ),
        ));

        assert!(all_rpc_calls_exist(
            context,
            RpcService::Test,
            include_str!(
                "../../../sdk-core-protos/protos/testsrv_upstream/temporal/api/testservice/v1/service.proto"
            ),
        ));

        assert!(all_rpc_calls_exist(
            context,
            RpcService::Health,
            include_str!("../../../sdk-core-protos/protos/grpc/health/v1/health.proto"),
        ));
    });
}

static CALLBACK_OVERRIDE_CALLS: LazyLock<Mutex<Vec<String>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

struct ClientOverrideError {
    message: String,
    details: Option<Vec<u8>>,
}

impl ClientOverrideError {
    pub fn new(message: String) -> Self {
        Self {
            message,
            details: None,
        }
    }
}

unsafe extern "C" fn callback_override(
    req: *mut ClientGrpcOverrideRequest,
    user_data: *mut libc::c_void,
) {
    let mut calls = CALLBACK_OVERRIDE_CALLS.lock().unwrap();

    // Simple header check to confirm headers are working
    let headers =
        temporal_core_client_grpc_override_request_headers(req).to_string_map_on_newlines();
    assert!(headers.get("content-type").unwrap().as_str() == "application/grpc");

    // Confirm user data is as we expect
    let user_data: &String = unsafe { &*(user_data as *const String) };
    assert!(user_data.as_str() == "some-user-data");

    calls.push(format!(
        "service: {}, rpc: {}",
        temporal_core_client_grpc_override_request_service(req).to_string(),
        temporal_core_client_grpc_override_request_rpc(req).to_string()
    ));
    let resp_raw = match temporal_core_client_grpc_override_request_rpc(req).to_str() {
        "GetSystemInfo" => Ok(GetSystemInfoResponse::default().encode_to_vec()),
        "StartWorkflowExecution" => match StartWorkflowExecutionRequest::decode(
            temporal_core_client_grpc_override_request_proto(req).to_slice(),
        ) {
            Ok(req) => Ok(StartWorkflowExecutionResponse {
                run_id: format!("run-id for {}", req.workflow_id),
                ..Default::default()
            }
            .encode_to_vec()),
            Err(err) => Err(ClientOverrideError::new(format!("Bad bytes: {err}"))),
        },
        "QueryWorkflow" => match QueryWorkflowRequest::decode(
            temporal_core_client_grpc_override_request_proto(req).to_slice(),
        ) {
            // Demonstrate fail details
            Ok(_) => Err(ClientOverrideError {
                message: "query-fail".to_string(),
                details: Some(
                    Failure {
                        message: "intentional failure".to_string(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
            }),
            Err(err) => Err(ClientOverrideError::new(format!("Bad bytes: {err}"))),
        },
        v => Err(ClientOverrideError::new(format!("Unknown RPC: {v}"))),
    };
    // It is very important that we borrow not move resp_raw here. If this were a "match resp_raw"
    // without the &, ownership of bytes moves into the match arm and can drop bytes after the
    // match arm. This is why we have a "let _" below for resp_raw to ensure a developed doesn't
    // accidentally move it.
    let resp = match &resp_raw {
        Ok(bytes) => ClientGrpcOverrideResponse {
            status_code: 0,
            headers: ByteArrayRef::empty(),
            success_proto: bytes.as_slice().into(),
            fail_message: ByteArrayRef::empty(),
            fail_details: ByteArrayRef::empty(),
        },
        Err(err) => ClientGrpcOverrideResponse {
            status_code: tonic::Code::Internal.into(),
            headers: ByteArrayRef::empty(),
            success_proto: ByteArrayRef::empty(),
            fail_message: err.message.as_str().into(),
            fail_details: if let Some(details) = &err.details {
                details.as_slice().into()
            } else {
                ByteArrayRef::empty()
            },
        },
    };
    temporal_core_client_grpc_override_request_respond(req, resp);
    let _ = resp_raw;
}

#[test]
fn test_simple_callback_override() {
    Context::with(|context| {
        let mut user_data = "some-user-data".to_owned();
        context.runtime_new().unwrap();
        // Create client which will invoke GetSystemInfo
        context
            .client_connect_with_override(
                Box::new(default_client_options("127.0.0.1:4567")),
                Some(callback_override),
                &mut user_data as *mut String as *mut libc::c_void,
            )
            .unwrap();

        // Invoke start workflow so we can confirm complex proto in/out
        let start_resp_raw = context
            .rpc_call(Box::new(OwnedRpcCallOptions {
                service: RpcService::Workflow,
                rpc: "StartWorkflowExecution".into(),
                req: StartWorkflowExecutionRequest {
                    workflow_id: "my-workflow-id".into(),
                    ..Default::default()
                }
                .encode_to_vec(),
                retry: false,
                metadata: None,
                timeout_millis: 0,
                cancellation_token: None,
            }))
            .unwrap();
        let start_resp = StartWorkflowExecutionResponse::decode(&*start_resp_raw).unwrap();
        assert!(start_resp.run_id == "run-id for my-workflow-id");

        // Try a query where a query failure will actually be delivered as failure details.
        // However, we don't currently have temporal_sdk_core_protos::google::rpc::Status in
        // the crate, so we'll just use the details directly even though a proper gRPC
        // implementation will only provide a google.rpc.Status proto.
        let query_err = context
            .rpc_call(Box::new(OwnedRpcCallOptions {
                service: RpcService::Workflow,
                rpc: "QueryWorkflow".into(),
                req: QueryWorkflowRequest::default().encode_to_vec(),
                retry: false,
                metadata: None,
                timeout_millis: 0,
                cancellation_token: None,
            }))
            .unwrap_err()
            .downcast::<RpcCallError>()
            .unwrap();
        assert!(query_err.status_code == tonic::Code::Internal as u32);
        assert!(query_err.message == "query-fail");
        assert!(
            Failure::decode(query_err.details.as_ref().unwrap().as_slice())
                .unwrap()
                .message
                == "intentional failure"
        );

        // Confirm we got the expected calls
        assert!(
            *CALLBACK_OVERRIDE_CALLS.lock().unwrap()
                == vec![
                    "service: temporal.api.workflowservice.v1.WorkflowService, rpc: GetSystemInfo",
                    "service: temporal.api.workflowservice.v1.WorkflowService, rpc: StartWorkflowExecution",
                    "service: temporal.api.workflowservice.v1.WorkflowService, rpc: QueryWorkflow"
                ]
        );
    });
}
