//! Process-local S3-compatible HTTP storage for tests.

mod protocol;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{
    HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_REQUEST_HEADERS,
    ORIGIN, VARY,
};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) type Reply = Response<Full<Bytes>>;

const DEFAULT_CORS_ORIGINS: [&str; 4] = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
];

#[derive(Clone)]
pub struct MemoryS3 {
    state: Arc<RwLock<State>>,
    max_request_bytes: usize,
    max_stored_bytes: usize,
    container_listener: bool,
    cors_origins: Arc<[String]>,
}

#[derive(Default)]
pub(crate) struct State {
    pub buckets: BTreeMap<String, Bucket>,
    pub uploads: BTreeMap<String, Upload>,
    pub stored_bytes: usize,
}

pub(crate) struct Bucket {
    pub objects: HashMap<String, ObjectEntry>,
    pub created: SystemTime,
}

pub(crate) struct ObjectEntry {
    pub object: Arc<Object>,
    pub slot: Arc<ArcSwapOption<Object>>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            objects: HashMap::new(),
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
    pub encryption: Option<String>,
}

pub(crate) struct Upload {
    pub bucket: String,
    pub key: String,
    pub parts: BTreeMap<u32, Object>,
    pub content_type: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub encryption: Option<String>,
}

pub struct RunningServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
pub struct ObjectHandle {
    slot: Arc<ArcSwapOption<Object>>,
}

impl ObjectHandle {
    pub fn get(&self) -> Option<Bytes> {
        self.slot.load_full().map(|object| object.bytes.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    NoSuchBucket,
    CapacityExceeded,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSuchBucket => formatter.write_str("bucket does not exist"),
            Self::CapacityExceeded => formatter.write_str("storage capacity exceeded"),
        }
    }
}

impl std::error::Error for StoreError {}

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
            state: Arc::new(RwLock::new(State::default())),
            max_request_bytes: 64 * 1024 * 1024,
            max_stored_bytes: 512 * 1024 * 1024,
            container_listener: false,
            cors_origins: DEFAULT_CORS_ORIGINS.map(String::from).into(),
        }
    }

    /// Lets `listen` bind an unspecified address, such as `0.0.0.0`, for use inside a container.
    pub fn with_container_listener(mut self) -> Self {
        self.container_listener = true;
        self
    }

    /// Sets the request origins that receive CORS headers.
    pub fn with_cors_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.cors_origins = origins.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_max_request_bytes(mut self, bytes: usize) -> Self {
        self.max_request_bytes = bytes;
        self
    }

    pub fn with_max_stored_bytes(mut self, bytes: usize) -> Self {
        self.max_stored_bytes = bytes;
        self
    }

    pub fn stored_bytes(&self) -> usize {
        self.state.read().unwrap().stored_bytes
    }

    pub fn create_bucket(&self, name: impl Into<String>) -> bool {
        let mut state = self.state.write().unwrap();
        let name = name.into();
        if state.buckets.contains_key(&name) {
            return false;
        }
        state.buckets.insert(name, Bucket::default());
        true
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), StoreError> {
        let object = protocol::make_object(bytes, &hyper::HeaderMap::new());
        let mut state = self.state.write().unwrap();
        if !state.buckets.contains_key(bucket) {
            return Err(StoreError::NoSuchBucket);
        }
        protocol::store_object_bounded(
            &mut state,
            bucket,
            key.into(),
            object,
            self.max_stored_bytes,
        )
        .map_err(|_| StoreError::CapacityExceeded)?;
        Ok(())
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Option<Bytes> {
        let state = self.state.read().unwrap();
        Some(
            state
                .buckets
                .get(bucket)?
                .objects
                .get(key)?
                .object
                .bytes
                .clone(),
        )
    }

    pub fn object_handle(&self, bucket: &str, key: &str) -> Option<ObjectHandle> {
        let state = self.state.read().unwrap();
        let slot = Arc::clone(&state.buckets.get(bucket)?.objects.get(key)?.slot);
        Some(ObjectHandle { slot })
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StoreError> {
        let mut state = self.state.write().unwrap();
        let bucket = state
            .buckets
            .get_mut(bucket)
            .ok_or(StoreError::NoSuchBucket)?;
        let deleted = protocol::remove_object(bucket, key);
        if let Some(size) = deleted {
            state.stored_bytes -= size;
        }
        Ok(deleted.is_some())
    }

    pub async fn start(&self) -> io::Result<RunningServer> {
        self.listen(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
    }

    pub async fn listen(&self, address: SocketAddr) -> io::Result<RunningServer> {
        let ip = address.ip();
        if !(ip.is_loopback() || self.container_listener && ip.is_unspecified()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "s3-memory accepts loopback addresses, or unspecified addresses with a container listener",
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
        let mut response = self.respond(&parts, body).await;
        if let Some(origin) = parts.headers.get(ORIGIN) {
            self.add_cors(&mut response, origin, &parts.headers);
        }
        response
    }

    fn add_cors(&self, response: &mut Reply, origin: &HeaderValue, request: &hyper::HeaderMap) {
        if !self
            .cors_origins
            .iter()
            .any(|allowed| allowed.as_bytes() == origin.as_bytes())
        {
            return;
        }
        let headers = response.headers_mut();
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
        headers.insert(
            ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, HEAD, PUT, POST, DELETE"),
        );
        if let Some(requested) = request.get(ACCESS_CONTROL_REQUEST_HEADERS) {
            headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, requested.clone());
        }
        headers.insert(
            ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("ETag, Content-Length, x-amz-server-side-encryption"),
        );
        headers.insert(VARY, HeaderValue::from_static("Origin"));
    }

    async fn respond(&self, parts: &hyper::http::request::Parts, body: Incoming) -> Reply {
        if parts.method == Method::OPTIONS {
            return protocol::empty(hyper::StatusCode::NO_CONTENT);
        }
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
        let payload = if parts.method == Method::PUT {
            match protocol::decode_aws_chunked(&parts.headers, payload) {
                Ok(payload) => payload,
                Err(()) => {
                    return protocol::error(
                        hyper::StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        parts.uri.path(),
                    )
                }
            }
        } else {
            payload
        };
        if let Some((bucket, key)) = protocol::basic_put_path(parts) {
            let object = protocol::make_object(payload, &parts.headers);
            return protocol::write_object(
                &mut self.state.write().unwrap(),
                &bucket,
                key,
                object,
                &parts.headers,
                self.max_stored_bytes,
            );
        }
        let hot_read = {
            let state = self.state.read().unwrap();
            protocol::hot_lookup(&state, parts)
        };
        if let Some(snapshot) = hot_read {
            return protocol::render_hot(snapshot, parts);
        }
        if parts.method == Method::GET && parts.uri.path() == "/__health" {
            return protocol::empty(hyper::StatusCode::OK);
        }
        protocol::route(
            &mut self.state.write().unwrap(),
            parts,
            payload,
            self.max_stored_bytes,
        )
    }
}
