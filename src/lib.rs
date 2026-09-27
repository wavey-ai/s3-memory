//! Process-local S3-compatible HTTP storage for tests.

mod protocol;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) type Reply = Response<Full<Bytes>>;

#[derive(Clone)]
pub struct MemoryS3 {
    state: Arc<Mutex<State>>,
    max_request_bytes: usize,
}

#[derive(Default)]
pub(crate) struct State {
    pub buckets: BTreeMap<String, Bucket>,
    pub uploads: BTreeMap<String, Upload>,
}

pub(crate) struct Bucket {
    pub objects: BTreeMap<String, Object>,
    pub created: SystemTime,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            objects: BTreeMap::new(),
            created: SystemTime::now(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Object {
    pub bytes: Bytes,
    pub etag: String,
    pub modified: SystemTime,
    pub content_type: Option<String>,
    pub metadata: Vec<(String, String)>,
}

pub(crate) struct Upload {
    pub bucket: String,
    pub key: String,
    pub parts: BTreeMap<u32, Object>,
    pub content_type: Option<String>,
    pub metadata: Vec<(String, String)>,
}

pub struct RunningServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl RunningServer {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

impl Default for MemoryS3 {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryS3 {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            max_request_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn with_max_request_bytes(mut self, bytes: usize) -> Self {
        self.max_request_bytes = bytes;
        self
    }

    pub async fn start(&self) -> io::Result<RunningServer> {
        self.listen(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
    }

    pub async fn listen(&self, address: SocketAddr) -> io::Result<RunningServer> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3-memory accepts loopback addresses only",
            ));
        }
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (shutdown, mut stopped) = oneshot::channel();
        let server = self.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let server = server.clone();
                        tokio::spawn(async move {
                            let service = service_fn(move |request| {
                                let server = server.clone();
                                async move { Ok::<_, Infallible>(server.handle(request).await) }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                }
            }
        });
        Ok(RunningServer {
            address,
            shutdown: Some(shutdown),
            task,
        })
    }

    async fn handle(&self, request: Request<Incoming>) -> Reply {
        let (parts, body) = request.into_parts();
        let payload = if parts.method == Method::PUT || parts.method == Method::POST {
            match Limited::new(body, self.max_request_bytes).collect().await {
                Ok(body) => body.to_bytes(),
                Err(_) => {
                    return protocol::error(
                        hyper::StatusCode::PAYLOAD_TOO_LARGE,
                        "EntityTooLarge",
                        parts.uri.path(),
                    )
                }
            }
        } else {
            Bytes::new()
        };
        protocol::route(&mut self.state.lock().unwrap(), &parts, payload)
    }
}
