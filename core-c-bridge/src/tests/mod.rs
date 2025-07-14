use crate::client::RpcService;
use crate::tests::utils::{
    OwnedRpcCallOptions, RpcCallError, default_client_options, default_server_config,
};
use context::Context;
use prost::Message;
use std::sync::Arc;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    GetSystemInfoRequest, GetSystemInfoResponse,
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
