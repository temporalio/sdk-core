use assert_matches::assert_matches;
use futures_util::{future::BoxFuture, FutureExt};
use std::{
    collections::HashMap,
    convert::Infallible,
    env,
    task::{Context, Poll},
    time::Duration,
};
use temporal_client::{Namespace, RetryConfig, WorkflowClientTrait, WorkflowService};
use temporal_sdk_core_protos::temporal::api::{
    cloud::cloudservice::v1::GetNamespaceRequest,
    workflowservice::v1::{DescribeNamespaceRequest, GetWorkflowExecutionHistoryRequest},
};
use temporal_sdk_core_test_utils::{
    get_integ_server_options, init_integ_telem, CoreWfStarter, NAMESPACE,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc::UnboundedSender, oneshot},
};
use tonic::{
    body::BoxBody, codegen::Service, server::NamedService, transport::Server, Code, Request,
};
use tracing::info;

#[tokio::test]
async fn can_use_retry_client() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let mut core = CoreWfStarter::new("retry_client");
    let retry_client = core.get_client().await;
    for _ in 0..10 {
        retry_client.list_namespaces().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn can_use_retry_raw_client() {
    let opts = get_integ_server_options();
    let mut client = opts.connect_no_namespace(None).await.unwrap();
    client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn calls_get_system_info() {
    let opts = get_integ_server_options();
    let raw_client = opts.connect_no_namespace(None).await.unwrap();
    assert!(raw_client.get_client().capabilities().is_some());
}

#[tokio::test]
async fn per_call_timeout_respected_whole_client() {
    let opts = get_integ_server_options();
    let mut raw_client = opts.connect_no_namespace(None).await.unwrap();
    let mut hm = HashMap::new();
    hm.insert("grpc-timeout".to_string(), "0S".to_string());
    raw_client.get_client().set_headers(hm);
    let err = raw_client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_matches!(err.code(), Code::DeadlineExceeded | Code::Cancelled);
}

#[tokio::test]
async fn per_call_timeout_respected_one_call() {
    let opts = get_integ_server_options();
    let mut client = opts.connect_no_namespace(None).await.unwrap();

    let mut req = Request::new(DescribeNamespaceRequest {
        namespace: NAMESPACE.to_string(),
        ..Default::default()
    });
    req.set_timeout(Duration::from_millis(0));
    let res = client.describe_namespace(req).await;
    assert_matches!(
        res.unwrap_err().code(),
        Code::DeadlineExceeded | Code::Cancelled
    );
}

#[derive(Clone)]
struct GenericService {
    header_to_parse: &'static str,
    header_tx: UnboundedSender<String>,
}
impl Service<tonic::codegen::http::Request<BoxBody>> for GenericService {
    type Response = tonic::codegen::http::Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<BoxBody>) -> Self::Future {
        self.header_tx
            .send(
                String::from_utf8_lossy(
                    req.headers()
                        .get(self.header_to_parse)
                        .map(|hv| hv.as_bytes())
                        .unwrap_or_default(),
                )
                .to_string(),
            )
            .unwrap();
        async move { Ok(Self::Response::new(tonic::codegen::empty_body())) }.boxed()
    }
}
impl NamedService for GenericService {
    const NAME: &'static str = "temporal.api.workflowservice.v1.WorkflowService";
}

#[tokio::test]
async fn timeouts_respected_one_call_fake_server() {
    init_integ_telem();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (header_tx, mut header_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService {
                header_to_parse: "grpc-timeout",
                header_tx,
            })
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    let mut opts = get_integ_server_options();
    let uri = format!("http://localhost:{}", addr.port()).parse().unwrap();
    opts.target_url = uri;
    opts.skip_get_system_info = true;
    opts.retry_config = RetryConfig {
        max_retries: 1,
        ..Default::default()
    };

    let mut client = opts.connect_no_namespace(None).await.unwrap();

    macro_rules! call_client {
        ($client:ident, $trx:ident, $client_fn:ident, $msg:expr) => {
            let mut req = Request::new($msg);
            req.set_timeout(Duration::from_millis(100));
            // Response is always error b/c empty body
            let _ = $client.$client_fn(req).await;
            let timeout = $trx.recv().await.unwrap();
            assert_eq!("100000u", timeout);
        };
    }

    call_client!(
        client,
        header_rx,
        get_workflow_execution_history,
        Default::default()
    );
    call_client!(
        client,
        header_rx,
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest {
            // Ensure these calls when done long-poll style still respect timeout
            wait_new_event: true,
            ..Default::default()
        }
    );
    call_client!(
        client,
        header_rx,
        update_workflow_execution,
        Default::default()
    );
    call_client!(
        client,
        header_rx,
        poll_workflow_execution_update,
        Default::default()
    );

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn namespace_header_attached_to_relevant_calls() {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (header_tx, mut header_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService {
                header_to_parse: "Temporal-Namespace",
                header_tx,
            })
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    let mut opts = get_integ_server_options();
    let uri = format!("http://localhost:{}", addr.port()).parse().unwrap();
    opts.target_url = uri;
    opts.skip_get_system_info = true;
    opts.retry_config = RetryConfig {
        max_retries: 1,
        ..Default::default()
    };

    let namespace = "namespace";
    let client = opts.connect(namespace, None).await.unwrap();

    let _ = client
        .get_workflow_execution_history("hi".to_string(), None, vec![])
        .await;
    let val = header_rx.recv().await.unwrap();
    assert_eq!(namespace, val);
    let _ = client.list_namespaces().await;
    let val = header_rx.recv().await.unwrap();
    // List namespaces is not namespace-specific
    assert_eq!("", val);
    let _ = client
        .describe_namespace(Namespace::Name("Other".to_string()))
        .await;
    let val = header_rx.recv().await.unwrap();
    assert_eq!("Other", val);

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn cloud_ops_test() {
    let api_key = match env::var("TEMPORAL_CLIENT_CLOUD_API_KEY") {
        Ok(k) => k,
        Err(_) => {
            // skip test
            info!("Skipped cloud operations client test");
            return;
        }
    };
    let api_version =
        env::var("TEMPORAL_CLIENT_CLOUD_API_VERSION").expect("version env var must exist");
    let namespace =
        env::var("TEMPORAL_CLIENT_CLOUD_NAMESPACE").expect("namespace env var must exist");
    let mut opts = get_integ_server_options();
    opts.target_url = "saas-api.tmprl.cloud:443".parse().unwrap();
    opts.api_key = Some(api_key);
    opts.headers = Some({
        let mut hm = HashMap::new();
        hm.insert("temporal-cloud-api-version".to_string(), api_version);
        hm
    });
    let mut client = opts.connect_no_namespace(None).await.unwrap().into_inner();
    let cloud_client = client.cloud_svc_mut();
    let res = cloud_client
        .get_namespace(GetNamespaceRequest {
            namespace: namespace.clone(),
        })
        .await
        .unwrap();
    assert_eq!(res.into_inner().namespace.unwrap().namespace, namespace);
}
