use anyhow::anyhow;
use futures_util::future::BoxFuture;
use http::{HeaderMap, Request, Response};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use std::{
    convert::Infallible,
    error::Error,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::Status;
use tonic::codegen::Body;
use tower::Service;

pub struct GrpcRequest<'a> {
    pub service: &'a str,
    pub rpc: &'a str,
    pub headers: &'a HeaderMap,
    pub proto: Bytes,
}

pub struct GrpcSuccessResponse {
    pub headers: HeaderMap,
    pub proto: Vec<u8>,
}

#[derive(Clone)]
pub struct CallbackBasedGrpcService {
    pub callback: Arc<
        dyn for<'a> Fn(GrpcRequest<'a>) -> BoxFuture<'a, Result<GrpcSuccessResponse, Status>>
            + Send
            + Sync,
    >,
}

impl Service<Request<tonic::body::Body>> for CallbackBasedGrpcService {
    type Response = http::Response<tonic::body::Body>;
    type Error = anyhow::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
        let callback = self.callback.clone();

        Box::pin(async move {
            // Build req
            let (parts, body) = req.into_parts();
            let mut path_parts = parts.uri.path().trim_start_matches('/').split('/');
            let collected_body = body.collect().await.map_err(|e| anyhow!(e))?;
            let req = GrpcRequest {
                service: path_parts.next().unwrap_or_default(),
                rpc: path_parts.next().unwrap_or_default(),
                headers: &parts.headers,
                proto: collected_body.to_bytes(),
            };

            // Invoke and handle response
            match (callback)(req).await {
                Ok(success) => {
                    // Create full body from returned bytes
                    let full_body = Full::new(Bytes::from(success.proto))
                        .map_err(|_: Infallible| Status::internal("never fails"));
                    // Build response appending headers
                    let mut resp_builder = Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc");
                    for (key, value) in success.headers.iter() {
                        resp_builder = resp_builder.header(key, value);
                    }
                    Ok(resp_builder
                        .body(tonic::body::Body::new(full_body))
                        .map_err(|e| anyhow!(e))?)
                }
                Err(status) => Ok(status.into_http()),
            }
        })
    }
}
