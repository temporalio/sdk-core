use std::env;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use temporal_client::{
    Client, ClientOptionsBuilder, ClientTlsConfig, RetryClient, TlsConfig, WorkflowClientTrait,
};
use temporal_sdk::WfContext;
use temporal_sdk_core_protos::temporal::api::enums::v1::EventType;
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure;
use temporal_sdk_core_protos::temporal::api::history::v1::history_event::Attributes::WorkflowTaskFailedEventAttributes;
use temporal_sdk_core_test_utils::CoreWfStarter;
use url::Url;

async fn get_client(client_name: &str) -> RetryClient<Client> {
    let cloud_addr = env::var("TEMPORAL_CLOUD_ADDRESS").unwrap();
    let cloud_key = env::var("TEMPORAL_CLIENT_KEY").unwrap();

    let client_cert = env::var("TEMPORAL_CLIENT_CERT")
        .expect("TEMPORAL_CLIENT_CERT must be set")
        .replace("\\n", "\n")
        .into_bytes();
    let client_private_key = cloud_key.replace("\\n", "\n").into_bytes();
    let sgo = ClientOptionsBuilder::default()
        .target_url(Url::from_str(&cloud_addr).unwrap())
        .client_name(client_name)
        .client_version("clientver")
        .tls_cfg(TlsConfig {
            client_tls_config: Some(ClientTlsConfig {
                client_cert,
                client_private_key,
            }),
            ..Default::default()
        })
        .build()
        .unwrap();
    sgo.connect(
        env::var("TEMPORAL_NAMESPACE").expect("TEMPORAL_NAMESPACE must be set"),
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn tls_test() {
    let con = get_client("tls_tester").await;
    con.list_workflow_executions(100, vec![], "".to_string())
        .await
        .unwrap();
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    let wf_name = "oversize_grpc_message";
    let mut starter =
        CoreWfStarter::new_with_client(wf_name, get_client("grpc_message_too_large").await);
    starter.worker_config.no_remote_activities(true);
    let mut core = starter.worker().await;

    static OVERSIZE_GRPC_MESSAGE_RUN: AtomicBool = AtomicBool::new(false);
    core.register_wf(wf_name.to_owned(), |_ctx: WfContext| async move {
        if OVERSIZE_GRPC_MESSAGE_RUN.load(Relaxed) {
            Ok(vec![].into())
        } else {
            OVERSIZE_GRPC_MESSAGE_RUN.store(true, Relaxed);
            let result: Vec<u8> = vec![0; 5000000];
            Ok(result.into())
        }
    });
    starter.start_with_worker(wf_name, &mut core).await;
    core.run_until_done().await.unwrap();

    assert!(starter.get_history().await.events.iter().any(|e| {
        e.event_type == EventType::WorkflowTaskFailed as i32
            && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
                // TODO tim: Change to custom cause
                attr.cause == WorkflowWorkerUnhandledFailure as i32
                    && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
            } else {
                false
            }
    }))
}
