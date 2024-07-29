use assert_matches::assert_matches;
use futures_util::{future::BoxFuture, FutureExt};
use std::{
    collections::HashMap,
    convert::Infallible,
    task::{Context, Poll},
    time::Duration,
};
use temporal_client::{RetryConfig, WorkflowClientTrait, WorkflowService};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporal_sdk_core_test_utils::{get_integ_server_options, CoreWfStarter, NAMESPACE};
use tokio::{
    net::TcpListener,
    sync::{mpsc::UnboundedSender, oneshot},
};
use tonic::{
    body::BoxBody, codegen::Service, server::NamedService, transport::Server, Code, Request,
};

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
    timeouts_tx: UnboundedSender<String>,
}
impl Service<tonic::codegen::http::Request<BoxBody>> for GenericService {
    type Response = tonic::codegen::http::Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<BoxBody>) -> Self::Future {
        self.timeouts_tx
            .send(
                String::from_utf8_lossy(req.headers().get("grpc-timeout").unwrap().as_bytes())
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
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (timeouts_tx, mut timeouts_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService { timeouts_tx })
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
        ($client:ident, $trx:ident, $client_fn:ident) => {
            let mut req = Request::new(Default::default());
            req.set_timeout(Duration::from_millis(100));
            // Response is always error b/c empty body
            let _ = $client.$client_fn(req).await;
            let timeout = $trx.recv().await.unwrap();
            assert_eq!("100000u", timeout);
        };
    }

    call_client!(client, timeouts_rx, get_workflow_execution_history);
    call_client!(client, timeouts_rx, update_workflow_execution);
    call_client!(client, timeouts_rx, poll_workflow_execution_update);

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
