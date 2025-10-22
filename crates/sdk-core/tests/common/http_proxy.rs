use bytes::Bytes;
use http_body_util::Empty;
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use temporalio_client::proxy::ProxyStream;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

pub(crate) struct HttpProxy {
    proxy_hits: Arc<AtomicUsize>,
    shutdown_tx: oneshot::Sender<()>,
}
impl HttpProxy {
    pub(crate) fn spawn_tcp(listener: TcpListener) -> Self {
        Self::spawn(ProxyListener::Tcp(listener))
    }

    #[cfg(unix)]
    pub(crate) fn spawn_unix(listener: UnixListener) -> Self {
        Self::spawn(ProxyListener::Unix(listener))
    }

    fn spawn(listener: ProxyListener) -> Self {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let proxy_hits = Arc::new(AtomicUsize::new(0));
        let proxy_hits_cloned = proxy_hits.clone();
        tokio::spawn(async move {
            loop {
                let proxy_hits_cloned = proxy_hits_cloned.clone();
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    stream = listener.accept() => {
                        let stream = match stream {
                            Ok(stream) => stream,
                            Err(e) => { println!("Proxy accept error: {e}"); continue; }
                        };
                        tokio::spawn(async move {
                            if let Err(e) = http1::Builder::new()
                                .serve_connection(
                                    TokioIo::new(stream),
                                    service_fn(move |req| handle_connect(req, proxy_hits_cloned.clone())),
                                )
                                .with_upgrades()
                                .await
                            {
                                println!("Proxy conn error: {e}");
                            }
                        });
                    }
                }
            }
        });
        Self {
            proxy_hits,
            shutdown_tx,
        }
    }

    pub(crate) fn hit_count(&self) -> usize {
        self.proxy_hits.load(Ordering::SeqCst)
    }

    /// Returns before shutdown occurs
    pub(crate) fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

async fn handle_connect(
    req: Request<Incoming>,
    counter: Arc<AtomicUsize>,
) -> Result<Response<Empty<Bytes>>, hyper::Error> {
    if req.method() == hyper::Method::CONNECT {
        // Increment atomic counter
        counter.fetch_add(1, Ordering::SeqCst);

        // Tell the client the tunnel is established
        tokio::spawn(async move {
            if let Some(addr) = req.uri().authority().map(|a| a.as_str()) {
                match TcpStream::connect(addr).await {
                    Ok(mut server_stream) => match hyper::upgrade::on(req).await {
                        Ok(upgraded) => {
                            let mut upgraded = TokioIo::new(upgraded);
                            let _ =
                                tokio::io::copy_bidirectional(&mut upgraded, &mut server_stream)
                                    .await;
                        }
                        Err(err) => println!("Upgrade failed: {err}"),
                    },
                    Err(e) => println!("Failed to connect to {addr}: {e}"),
                }
            }
        });

        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Empty::new())
            .unwrap())
    } else {
        Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Empty::new())
            .unwrap())
    }
}

enum ProxyListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

impl ProxyListener {
    async fn accept(&self) -> io::Result<ProxyStream> {
        match self {
            ProxyListener::Tcp(tcp) => tcp.accept().await.map(|(s, _)| ProxyStream::Tcp(s)),
            #[cfg(unix)]
            ProxyListener::Unix(unix) => unix.accept().await.map(|(s, _)| ProxyStream::Unix(s)),
        }
    }
}
