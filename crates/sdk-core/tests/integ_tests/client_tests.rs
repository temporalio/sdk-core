use crate::common::{
    CoreWfStarter, NAMESPACE,
    fake_grpc_server::{GenericService, fake_server},
    get_integ_server_options,
    http_proxy::HttpProxy,
};
use assert_matches::assert_matches;
use futures_util::FutureExt;
use http_body_util::Full;
use prost::Message;
use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    HttpConnectProxyOptions, Namespace, RETRYABLE_ERROR_CODES, RetryConfig, WorkflowClientTrait,
    WorkflowService,
};
use temporalio_common::protos::temporal::api::{
    cloud::cloudservice::v1::GetNamespaceRequest,
    workflowservice::v1::{
        DescribeNamespaceRequest, GetWorkflowExecutionHistoryRequest,
        RespondActivityTaskCanceledResponse,
    },
};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::{
    Code, IntoRequest, Request, Status, body::Body, codegen::http::Response, transport::Server,
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
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
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
    raw_client.get_client().set_headers(hm).unwrap();
    let err = raw_client
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
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

#[tokio::test]
async fn timeouts_respected_one_call_fake_server() {
    let mut fs = fake_server(|_| async { Response::new(Body::empty()) }.boxed()).await;
    let header_rx = &mut fs.header_rx;

    let mut opts = get_integ_server_options();
    let uri = format!("http://localhost:{}", fs.addr.port())
        .parse()
        .unwrap();
    opts.target_url = uri;
    opts.skip_get_system_info = true;
    opts.retry_config = RetryConfig::no_retries();

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

    fs.shutdown().await;
}

#[tokio::test]
async fn non_retryable_errors() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::Unauthenticated,
        Code::Unimplemented,
    ] {
        let mut fs = fake_server(move |_| {
            let s = Status::new(code, "bla").into_http();
            async { s }.boxed()
        })
        .await;

        let mut opts = get_integ_server_options();
        let uri = format!("http://localhost:{}", fs.addr.port())
            .parse()
            .unwrap();
        opts.target_url = uri;
        opts.skip_get_system_info = true;
        let client = opts.connect("ns", None).await.unwrap();

        let result = client.cancel_activity_task(vec![1].into(), None).await;

        // Expecting an error after a single attempt, since there was a non-retryable error.
        assert!(result.is_err());
        let mut all_calls = vec![];
        fs.header_rx.recv_many(&mut all_calls, 9999).await;
        assert_eq!(all_calls.len(), 1);

        fs.shutdown().await;
    }
}

#[tokio::test]
async fn retryable_errors() {
    // Take out retry exhausted since it gets a special policy which would make this take ages
    for code in RETRYABLE_ERROR_CODES
        .iter()
        .copied()
        .filter(|p| p != &Code::ResourceExhausted)
    {
        let count = Arc::new(AtomicUsize::new(0));
        let mut fs = fake_server(move |_| {
            let prev = count.fetch_add(1, Ordering::Relaxed);
            let r = if prev < 3 {
                Status::new(code, "bla").into_http()
            } else {
                make_ok_response(RespondActivityTaskCanceledResponse::default())
            };
            async { r }.boxed()
        })
        .await;

        let mut opts = get_integ_server_options();
        let uri = format!("http://localhost:{}", fs.addr.port())
            .parse()
            .unwrap();
        opts.target_url = uri;
        opts.skip_get_system_info = true;
        let client = opts.connect("ns", None).await.unwrap();

        let result = client.cancel_activity_task(vec![1].into(), None).await;

        // Expecting successful response after retries
        assert!(result.is_ok());
        let mut all_calls = vec![];
        fs.header_rx.recv_many(&mut all_calls, 9999).await;
        // Should be 4 attempts
        assert_eq!(all_calls.len(), 4);
        fs.shutdown().await;
    }
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
                response_maker: |_| async { Response::new(Body::empty()) }.boxed(),
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
    opts.retry_config = RetryConfig::no_retries();

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
    let client = opts.connect_no_namespace(None).await.unwrap().into_inner();
    let mut cloud_client = client.cloud_svc();
    let res = cloud_client
        .get_namespace(
            GetNamespaceRequest {
                namespace: namespace.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();
    assert_eq!(res.into_inner().namespace.unwrap().namespace, namespace);
}

#[tokio::test]
async fn http_proxy() {
    // Create server
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_cloned = call_count.clone();
    let server = fake_server(move |_| {
        call_count_cloned.fetch_add(1, Ordering::SeqCst);
        async { Response::new(Body::empty()) }.boxed()
    })
    .await;

    // Create HTTP TCP proxy
    let tcp_proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_proxy_addr = tcp_proxy_listener.local_addr().unwrap();
    let tcp_proxy = HttpProxy::spawn_tcp(tcp_proxy_listener);

    // General client options
    let mut opts = get_integ_server_options();
    opts.retry_config = RetryConfig::no_retries();
    opts.skip_get_system_info = true;

    // Connect client with no proxy and make call and confirm reached
    opts.target_url = format!("http://127.0.0.1:{}", server.addr.port())
        .parse()
        .unwrap();
    let client = opts.connect("my-namespace", None).await.unwrap();
    let _ = client.list_namespaces().await;
    assert!(call_count.load(Ordering::SeqCst) == 1);
    assert!(tcp_proxy.hit_count() == 0);

    // Connect client to proxy and make call and confirm reached
    opts.http_connect_proxy = Some(HttpConnectProxyOptions {
        target_addr: tcp_proxy_addr.to_string(),
        basic_auth: None,
    });
    let proxied_client = opts.connect("my-namespace", None).await.unwrap();
    let _ = proxied_client.list_namespaces().await;
    assert!(call_count.load(Ordering::SeqCst) == 2);
    assert!(tcp_proxy.hit_count() == 1);

    // Test Unix socket too only in Unix environments
    #[cfg(unix)]
    {
        // Create temp socket path
        let mut sock_path = std::env::temp_dir();
        sock_path.push(format!("http-proxy-test-{}.sock", std::process::id()));
        // Remove if there just in case
        let _ = std::fs::remove_file(&sock_path);

        // Create unix-socket-based proxy
        let unix_proxy = HttpProxy::spawn_unix(UnixListener::bind(&sock_path).unwrap());

        // Connect client to proxy and make call and confirm reached
        opts.http_connect_proxy = Some(HttpConnectProxyOptions {
            target_addr: format!("unix:{}", sock_path.to_str().unwrap()),
            basic_auth: None,
        });
        let proxied_client = opts.connect("my-namespace", None).await.unwrap();
        let _ = proxied_client.list_namespaces().await;
        assert!(call_count.load(Ordering::SeqCst) == 3);
        assert!(unix_proxy.hit_count() == 1);

        // Shutdown unix proxy
        unix_proxy.shutdown();
    }

    // Shutdown server and proxy
    server.shutdown().await;
    tcp_proxy.shutdown();
}

fn make_ok_response<T>(message: T) -> Response<Body>
where
    T: Message,
{
    // Encode the message into a byte buffer.
    let mut buf = Vec::new();
    message
        .encode(&mut buf)
        .expect("failed to encode response message");

    // Props to o3-mini for giving me a cheap way to make a grpc response
    let mut frame = Vec::with_capacity(5 + buf.len());
    frame.push(0);
    let len = buf.len() as u32;
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&buf);
    let full_body = Full::new(frame.into());
    let body = Body::new(full_body);

    // Build the HTTP response with the required gRPC headers.
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(body)
        .expect("failed to build response")
}
