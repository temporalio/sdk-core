use futures_util::future::{BoxFuture, FutureExt};
use std::{
    convert::Infallible,
    task::{Context, Poll},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc::UnboundedSender, oneshot},
};
use tonic::{
    body::Body, codegen::Service, codegen::http::Response, server::NamedService, transport::Server,
};

#[derive(Clone)]
pub(crate) struct GenericService<F> {
    pub header_to_parse: &'static str,
    pub header_tx: UnboundedSender<String>,
    pub response_maker: F,
}
impl<F> Service<tonic::codegen::http::Request<Body>> for GenericService<F>
where
    F: FnMut(tonic::codegen::http::Request<Body>) -> BoxFuture<'static, Response<Body>>,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<Body>) -> Self::Future {
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
        let r = (self.response_maker)(req);
        async move { Ok(r.await) }.boxed()
    }
}
impl<F> NamedService for GenericService<F> {
    const NAME: &'static str = "temporal.api.workflowservice.v1.WorkflowService";
}

pub(crate) struct FakeServer {
    pub addr: std::net::SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    pub header_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    pub server_handle: tokio::task::JoinHandle<()>,
}

pub(crate) async fn fake_server<F>(response_maker: F) -> FakeServer
where
    F: FnMut(tonic::codegen::http::Request<Body>) -> BoxFuture<'static, Response<Body>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (header_tx, header_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService {
                header_to_parse: "grpc-timeout",
                header_tx,
                response_maker,
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

    FakeServer {
        addr,
        shutdown_tx,
        header_rx,
        server_handle,
    }
}

impl FakeServer {
    pub(crate) async fn shutdown(self) {
        self.shutdown_tx.send(()).unwrap();
        self.server_handle.await.unwrap();
    }
}
