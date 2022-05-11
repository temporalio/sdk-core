#![allow(
    // Non-camel-case types needed since this is exported as a C header and we
    // want C-like underscores in our type names
    non_camel_case_types,

    // We choose to have narrow "unsafe" blocks instead of marking entire
    // functions as unsafe. Even the example in clippy's docs at
    // https://rust-lang.github.io/rust-clippy/master/index.html#not_unsafe_ptr_arg_deref
    // cause a rustc warning for unnecessary inner-unsafe when marked on fn.
    // This check only applies to "pub" functions which are all exposed via C
    // API.
    clippy::not_unsafe_ptr_arg_deref,
)]

mod wrappers;

use bridge::{init_response, CreateWorkerRequest, InitResponse};
use prost::Message;
use std::sync::Arc;
use temporal_sdk_core::{
    fetch_global_buffered_logs, telemetry_init, Client, ClientOptions, RetryClient,
};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::coresdk::{
    bridge,
    bridge::{CreateClientRequest, InitTelemetryRequest},
};

/// A set of bytes owned by Core. No fields within nor any bytes references must
/// ever be mutated outside of Core. This must always be passed to
/// tmprl_bytes_free when no longer in use.
#[repr(C)]
pub struct tmprl_bytes_t {
    bytes: *const u8,
    len: libc::size_t,
    /// For internal use only.
    cap: libc::size_t,
    /// For internal use only.
    disable_free: bool,
}

impl tmprl_bytes_t {
    fn from_vec(vec: Vec<u8>) -> tmprl_bytes_t {
        // Mimics Vec::into_raw_parts that's only available in nightly
        let mut vec = std::mem::ManuallyDrop::new(vec);
        tmprl_bytes_t {
            bytes: vec.as_mut_ptr(),
            len: vec.len(),
            cap: vec.capacity(),
            disable_free: false,
        }
    }

    fn from_vec_disable_free(vec: Vec<u8>) -> tmprl_bytes_t {
        let mut b = tmprl_bytes_t::from_vec(vec);
        b.disable_free = true;
        b
    }

    fn into_raw(self) -> *mut tmprl_bytes_t {
        Box::into_raw(Box::new(self))
    }
}

/// Required because these instances are used by lazy_static and raw pointers
/// are not usually safe for send/sync.
unsafe impl Send for tmprl_bytes_t {}
unsafe impl Sync for tmprl_bytes_t {}

impl Drop for tmprl_bytes_t {
    fn drop(&mut self) {
        // In cases where freeing is disabled (or technically some other
        // drop-but-not-freed situation though we don't expect any), the bytes
        // remain non-null so we re-own them here
        if !self.bytes.is_null() {
            unsafe { Vec::from_raw_parts(self.bytes as *mut u8, self.len, self.cap) };
        }
    }
}

/// Free a set of bytes. The first parameter can be null in cases where a [tmprl_worker_t] instance
/// isn't available. If the second parameter is null, this is a no-op.
#[no_mangle]
pub extern "C" fn tmprl_bytes_free(worker: *mut tmprl_worker_t, bytes: *const tmprl_bytes_t) {
    // Bail if freeing is disabled
    unsafe {
        if bytes.is_null() || (*bytes).disable_free {
            return;
        }
    }
    let bytes = bytes as *mut tmprl_bytes_t;
    // Return vec back to core before dropping bytes
    let vec = unsafe { Vec::from_raw_parts((*bytes).bytes as *mut u8, (*bytes).len, (*bytes).cap) };
    // Set to null so the byte dropper doesn't try to free it
    unsafe { (*bytes).bytes = std::ptr::null_mut() };
    // Return only if worker is non-null
    if !worker.is_null() {
        let worker = unsafe { &mut *worker };
        worker.return_buf(vec);
    }
    unsafe {
        Box::from_raw(bytes);
    }
}

/// Used for maintaining pointer to user data across threads. See
/// https://doc.rust-lang.org/nomicon/send-and-sync.html.
struct UserDataHandle(*mut libc::c_void);
unsafe impl Send for UserDataHandle {}
unsafe impl Sync for UserDataHandle {}

impl From<UserDataHandle> for *mut libc::c_void {
    fn from(v: UserDataHandle) -> Self {
        v.0
    }
}

lazy_static::lazy_static! {
    static ref DEFAULT_INIT_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::InitResponse::default().encode_to_vec())
    };

    static ref DEFAULT_REGISTER_WORKER_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::RegisterWorkerResponse::default().encode_to_vec())
    };

    static ref DEFAULT_SHUTDOWN_WORKER_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::ShutdownWorkerResponse::default().encode_to_vec())
    };

    static ref DEFAULT_COMPLETE_WORKFLOW_ACTIVATION_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::CompleteWorkflowActivationResponse::default().encode_to_vec())
    };

    static ref DEFAULT_COMPLETE_ACTIVITY_TASK_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::CompleteActivityTaskResponse::default().encode_to_vec())
    };

    static ref DEFAULT_RECORD_ACTIVITY_HEARTBEAT_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::RecordActivityHeartbeatResponse::default().encode_to_vec())
    };

    static ref DEFAULT_REQUEST_WORKFLOW_EVICTION_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::RequestWorkflowEvictionResponse::default().encode_to_vec())
    };
}

/// A runtime owned by Core. This must be passed to [tmprl_runtime_free] when no longer in use. This
/// should not be freed until every call to every [tmprl_worker_t] instance created with this
/// runtime has been shutdown. In practice, since the actual runtime is behind an [Arc], it's
/// currently OK, but that's an implementation detail.
pub struct tmprl_runtime_t {
    // This is the same runtime shared with worker instances
    tokio_runtime: Arc<tokio::runtime::Runtime>,
}

/// Create a new runtime. The result is never null and must be freed via
/// tmprl_runtime_free when no longer in use.
#[no_mangle]
pub extern "C" fn tmprl_runtime_new() -> *mut tmprl_runtime_t {
    Box::into_raw(Box::new(tmprl_runtime_t {
        // TODO(cretz): Options to configure thread pool?
        tokio_runtime: Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        ),
    }))
}

/// Free a previously created runtime.
#[no_mangle]
pub extern "C" fn tmprl_runtime_free(runtime: *mut tmprl_runtime_t) {
    if !runtime.is_null() {
        unsafe {
            Box::from_raw(runtime);
        }
    }
}

/// A worker instance owned by Core. This must be passed to [tmprl_worker_shutdown]
/// when no longer in use which will free the resources.
pub struct tmprl_worker_t {
    tokio_runtime: Arc<tokio::runtime::Runtime>,
    // We are not concerned with the overhead of dynamic dispatch at this time
    worker: Arc<dyn Worker>,
}

/// Callback called by [tmprl_worker_init] on completion. The first parameter of the
/// callback is user data passed into the original function. The second
/// parameter is a worker instance if the call is successful or null if not. If
/// present, the worker instance must be freed via [tmprl_worker_shutdown] when no
/// longer in use. The third parameter of the callback is a byte array for a
/// [InitResponse] protobuf message which must be freed via [tmprl_bytes_free].
type tmprl_worker_init_callback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    worker: *mut tmprl_worker_t,
    resp: *const tmprl_bytes_t,
);

/// Callback called on function completion. The first parameter of the callback
/// is user data passed into the original function. The second parameter of the
/// callback is a never-null byte array for a response protobuf message which
/// must be freed via [tmprl_bytes_free].
type tmprl_callback =
    unsafe extern "C" fn(user_data: *mut libc::c_void, core: *const tmprl_bytes_t);

/// Create a new worker instance.
///
/// `runtime` and `client` are both required and must outlive this instance.
/// `req_proto` and `req_proto_len` represent a byte array for a [CreateWorkerRequest] protobuf
/// message.
/// The callback is invoked on completion.
#[no_mangle]
pub extern "C" fn tmprl_worker_init(
    runtime: *mut tmprl_runtime_t,
    client: *mut tmprl_client_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_worker_init_callback,
) {
    let (runtime, client) = unsafe { (&*runtime, &*client) };
    let req = match tmprl_worker_t::decode_proto::<CreateWorkerRequest>(req_proto, req_proto_len) {
        Ok(req) => req,
        Err(message) => {
            let resp = InitResponse {
                error: Some(init_response::Error { message }),
            };
            unsafe {
                callback(
                    user_data,
                    std::ptr::null_mut(),
                    tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw(),
                );
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    match tmprl_worker_t::new(
        runtime.tokio_runtime.clone(),
        client.client.clone(),
        wrappers::WorkerConfig(req),
    ) {
        Ok(worker) => unsafe {
            callback(
                user_data.into(),
                Box::into_raw(Box::new(worker)),
                &*DEFAULT_INIT_RESPONSE_BYTES,
            );
        },
        Err(message) => {
            let resp = InitResponse {
                error: Some(init_response::Error { message }),
            };
            unsafe {
                callback(
                    user_data.into(),
                    std::ptr::null_mut(),
                    tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw(),
                );
            }
        }
    };
}

/// Shutdown and free a previously created worker.
///
/// The req_proto and req_proto_len represent a byte array for a [bridge::ShutdownWorkerRequest]
/// protobuf message, which currently contains nothing and are unused, but the parameters are kept
/// for now.
///
/// The callback is invoked on completion with a ShutdownWorkerResponse protobuf message.
///
/// After the callback has been called, the worker struct will be freed and the pointer will no
/// longer be valid.
#[no_mangle]
pub extern "C" fn tmprl_worker_shutdown(
    worker: *mut tmprl_worker_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { Box::from_raw(worker) };
    let user_data = UserDataHandle(user_data);
    worker.tokio_runtime.clone().spawn(async move {
        worker.shutdown().await;
        unsafe {
            callback(user_data.into(), &*DEFAULT_SHUTDOWN_WORKER_RESPONSE_BYTES);
        }
        drop(worker);
    });
}

/// Initialize process-wide telemetry. Should only be called once, subsequent calls will be ignored
/// by core.
///
/// Unlike the other functions in this bridge, this blocks until initting is complete, as telemetry
/// should typically be initialized before doing other work.
///
/// Returns a byte array for a [InitResponse] protobuf message which must be freed via
/// tmprl_bytes_free.
#[no_mangle]
pub extern "C" fn tmprl_telemetry_init(
    req_proto: *const u8,
    req_proto_len: libc::size_t,
) -> *const tmprl_bytes_t {
    let req = match tmprl_worker_t::decode_proto::<InitTelemetryRequest>(req_proto, req_proto_len) {
        Ok(req) => req,
        Err(message) => {
            let resp = InitResponse {
                error: Some(init_response::Error { message }),
            };
            return tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw();
        }
    };

    match wrappers::InitTelemetryRequest(req)
        .try_into()
        .map(|opts| telemetry_init(&opts))
    {
        Ok(_) => &*DEFAULT_INIT_RESPONSE_BYTES,
        Err(message) => {
            let resp = InitResponse {
                error: Some(init_response::Error { message }),
            };
            tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw()
        }
    }
}

/// A client instance owned by Core. This must be passed to [tmprl_client_free]
/// when no longer in use which will free the resources.
pub struct tmprl_client_t {
    client: Arc<RetryClient<Client>>,
}

impl tmprl_client_t {
    pub fn new(client: Arc<RetryClient<Client>>) -> Self {
        Self { client }
    }
}

/// Callback called by [tmprl_client_init] on completion. The first parameter of the
/// callback is user data passed into the original function. The second
/// parameter is a client instance if the call is successful or null if not. If
/// present, the client instance must be freed via [tmprl_client_free] when no
/// longer in use. The third parameter of the callback is a byte array for a
/// [InitResponse] protobuf message which must be freed via [tmprl_bytes_free].
type tmprl_client_init_callback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    client: *mut tmprl_client_t,
    resp: *const tmprl_bytes_t,
);

/// Initialize a client connection to the Temporal service.
///
/// The runtime is required and must outlive this instance. The `req_proto` and `req_proto_len`
/// represent a byte array for a [CreateClientRequest] protobuf message. The callback is invoked on
/// completion.
#[no_mangle]
pub extern "C" fn tmprl_client_init(
    runtime: *mut tmprl_runtime_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_client_init_callback,
) {
    let runtime = unsafe { &*runtime };
    let (namespace, req) =
        match tmprl_worker_t::decode_proto::<CreateClientRequest>(req_proto, req_proto_len)
            .and_then(|cgr| {
                let ns = cgr.namespace.clone();
                wrappers::ClientOptions(cgr)
                    .try_into()
                    .map(|sgo: ClientOptions| (ns, sgo))
            }) {
            Ok(req) => req,
            Err(message) => {
                let resp = InitResponse {
                    error: Some(init_response::Error { message }),
                };
                unsafe {
                    callback(
                        user_data,
                        std::ptr::null_mut(),
                        tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw(),
                    );
                }
                return;
            }
        };

    let user_data = UserDataHandle(user_data);
    runtime.tokio_runtime.spawn(async move {
        match req.connect(namespace, None, None).await {
            Ok(client) => unsafe {
                callback(
                    user_data.into(),
                    Box::into_raw(Box::new(tmprl_client_t::new(Arc::new(client)))),
                    &*DEFAULT_INIT_RESPONSE_BYTES,
                );
            },
            Err(e) => {
                let resp = InitResponse {
                    error: Some(init_response::Error {
                        message: e.to_string(),
                    }),
                };
                unsafe {
                    callback(
                        user_data.into(),
                        std::ptr::null_mut(),
                        tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw(),
                    );
                }
            }
        }
    });
}

/// Free a previously created client
#[no_mangle]
pub extern "C" fn tmprl_client_free(client: *mut tmprl_client_t) {
    unsafe { drop(Box::from_raw(client)) };
}

/// Poll for a workflow activation.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::PollWorkflowActivationRequest] protobuf message, which currently contains nothing and
/// is unused, but the parameters are kept for now.
///
/// The callback is invoked on completion with a [bridge::PollWorkflowActivationResponse] protobuf
/// message.
#[no_mangle]
pub extern "C" fn tmprl_poll_workflow_activation(
    worker: *mut tmprl_worker_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let user_data = UserDataHandle(user_data);
    worker.tokio_runtime.clone().spawn(async move {
        let resp = bridge::PollWorkflowActivationResponse {
            response: Some(match worker.poll_workflow_activation().await {
                Ok(act) => bridge::poll_workflow_activation_response::Response::Activation(act),
                Err(err) => bridge::poll_workflow_activation_response::Response::Error(err),
            }),
        };
        unsafe { callback(user_data.into(), worker.encode_proto(&resp).into_raw()) };
    });
}

/// Poll for an activity task.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::PollActivityTaskRequest] protobuf message, which currently contains nothing and is
/// unused, but the parameters are kept for now.
///
/// The callback is invoked on completion with a [bridge::PollActivityTaskResponse] protobuf
/// message.
#[no_mangle]
pub extern "C" fn tmprl_poll_activity_task(
    worker: *mut tmprl_worker_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let user_data = UserDataHandle(user_data);
    worker.tokio_runtime.clone().spawn(async move {
        let resp = bridge::PollActivityTaskResponse {
            response: Some(match worker.poll_activity_task().await {
                Ok(task) => bridge::poll_activity_task_response::Response::Task(task),
                Err(err) => bridge::poll_activity_task_response::Response::Error(err),
            }),
        };
        unsafe { callback(user_data.into(), worker.encode_proto(&resp).into_raw()) };
    });
}

/// Complete a workflow activation.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::CompleteWorkflowActivationRequest] protobuf message. The callback is invoked on
/// completion with a [bridge::CompleteWorkflowActivationResponse] protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_complete_workflow_activation(
    worker: *mut tmprl_worker_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let req = match tmprl_worker_t::decode_proto::<bridge::CompleteWorkflowActivationRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::CompleteWorkflowActivationResponse {
                error: Some(bridge::complete_workflow_activation_response::Error { message }),
            };
            unsafe {
                callback(user_data, worker.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    worker.tokio_runtime.clone().spawn(async move {
        match worker.complete_workflow_activation(req).await {
            Ok(()) => unsafe {
                callback(
                    user_data.into(),
                    &*DEFAULT_COMPLETE_WORKFLOW_ACTIVATION_RESPONSE_BYTES,
                );
            },
            Err(err) => {
                let resp = bridge::CompleteWorkflowActivationResponse { error: Some(err) };
                unsafe { callback(user_data.into(), worker.encode_proto(&resp).into_raw()) };
            }
        }
    });
}

/// Complete an activity task.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::CompleteActivityTaskRequest] protobuf message. The callback is invoked on completion
/// with a [bridge::CompleteActivityTaskResponse] protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_complete_activity_task(
    worker: *mut tmprl_worker_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let req = match tmprl_worker_t::decode_proto::<bridge::CompleteActivityTaskRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::CompleteActivityTaskResponse {
                error: Some(bridge::complete_activity_task_response::Error { message }),
            };
            unsafe {
                callback(user_data, worker.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    worker.tokio_runtime.clone().spawn(async move {
        match worker.complete_activity_task(req).await {
            Ok(()) => unsafe {
                callback(
                    user_data.into(),
                    &*DEFAULT_COMPLETE_ACTIVITY_TASK_RESPONSE_BYTES,
                );
            },
            Err(err) => {
                let resp = bridge::CompleteActivityTaskResponse { error: Some(err) };
                unsafe { callback(user_data.into(), worker.encode_proto(&resp).into_raw()) };
            }
        }
    });
}

/// Record an activity heartbeat.
///
/// `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::RecordActivityHeartbeatRequest] protobuf message. The callback is invoked on completion
/// with a RecordActivityHeartbeatResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_record_activity_heartbeat(
    worker: *mut tmprl_worker_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let req = match tmprl_worker_t::decode_proto::<bridge::RecordActivityHeartbeatRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::RecordActivityHeartbeatResponse {
                error: Some(bridge::record_activity_heartbeat_response::Error { message }),
            };
            unsafe {
                callback(user_data, worker.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    worker.tokio_runtime.clone().spawn(async move {
        worker.record_activity_heartbeat(req);
        unsafe {
            callback(
                user_data.into(),
                &*DEFAULT_RECORD_ACTIVITY_HEARTBEAT_RESPONSE_BYTES,
            );
        }
    });
}

/// Request a workflow eviction.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::RequestWorkflowEvictionRequest] protobuf message. The callback is invoked on completion
/// with a [bridge::RequestWorkflowEvictionResponse] protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_request_workflow_eviction(
    worker: *mut tmprl_worker_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let worker = unsafe { &mut *worker };
    let req = match tmprl_worker_t::decode_proto::<bridge::RequestWorkflowEvictionRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::RequestWorkflowEvictionResponse {
                error: Some(bridge::request_workflow_eviction_response::Error { message }),
            };
            unsafe {
                callback(user_data, worker.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    worker.tokio_runtime.clone().spawn(async move {
        worker.request_workflow_eviction(req);
        unsafe {
            callback(
                user_data.into(),
                &*DEFAULT_REQUEST_WORKFLOW_EVICTION_RESPONSE_BYTES,
            );
        }
    });
}

/// Fetch buffered logs. Blocks until complete. This is still using the callback since we might
/// reasonably change log fetching to be async in the future.
///
/// The `req_proto` and `req_proto_len` represent a byte array for a
/// [bridge::FetchBufferedLogsRequest] protobuf message. The callback is invoked on completion with
/// a [bridge::FetchBufferedLogsResponse] protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_fetch_buffered_logs(
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    let resp = bridge::FetchBufferedLogsResponse {
        entries: fetch_global_buffered_logs()
            .into_iter()
            .map(|log| bridge::fetch_buffered_logs_response::LogEntry {
                message: log.message,
                timestamp: Some(log.timestamp.into()),
                level: match log.level {
                    log::Level::Error => bridge::LogLevel::Error.into(),
                    log::Level::Warn => bridge::LogLevel::Warn.into(),
                    log::Level::Info => bridge::LogLevel::Info.into(),
                    log::Level::Debug => bridge::LogLevel::Debug.into(),
                    log::Level::Trace => bridge::LogLevel::Trace.into(),
                },
            })
            .collect(),
    };

    unsafe {
        callback(
            user_data.into(),
            // TODO: Creates vec every time since no worker/core instance. Can be fixed with a
            //   pool if optimizations needed.
            tmprl_bytes_t::from_vec(resp.encode_to_vec()).into_raw(),
        )
    };
}

impl tmprl_worker_t {
    fn new(
        tokio_runtime: Arc<tokio::runtime::Runtime>,
        client: Arc<RetryClient<Client>>,
        opts: wrappers::WorkerConfig,
    ) -> Result<tmprl_worker_t, String> {
        Ok(tmprl_worker_t {
            tokio_runtime,
            worker: Arc::new(temporal_sdk_core::init_worker(opts.try_into()?, client)),
        })
    }

    async fn shutdown(&self) {
        self.worker.shutdown().await;
    }

    async fn poll_workflow_activation(
        &self,
    ) -> Result<
        temporal_sdk_core_protos::coresdk::workflow_activation::WorkflowActivation,
        bridge::poll_workflow_activation_response::Error,
    > {
        self.worker.poll_workflow_activation().await.map_err(|err| {
            bridge::poll_workflow_activation_response::Error {
                message: format!("{}", err),
                shutdown: matches!(err, temporal_sdk_core_api::errors::PollWfError::ShutDown),
            }
        })
    }

    async fn poll_activity_task(
        &self,
    ) -> Result<
        temporal_sdk_core_protos::coresdk::activity_task::ActivityTask,
        bridge::poll_activity_task_response::Error,
    > {
        self.worker.poll_activity_task().await.map_err(|err| {
            bridge::poll_activity_task_response::Error {
                message: format!("{}", err),
                shutdown: matches!(
                    err,
                    temporal_sdk_core_api::errors::PollActivityError::ShutDown
                ),
            }
        })
    }

    async fn complete_workflow_activation(
        &self,
        req: bridge::CompleteWorkflowActivationRequest,
    ) -> Result<(), bridge::complete_workflow_activation_response::Error> {
        self.worker
            .complete_workflow_activation(req.completion.unwrap_or_default())
            .await
            .map_err(|err| bridge::complete_workflow_activation_response::Error {
                message: format!("{}", err),
            })
    }

    async fn complete_activity_task(
        &self,
        req: bridge::CompleteActivityTaskRequest,
    ) -> Result<(), bridge::complete_activity_task_response::Error> {
        self.worker
            .complete_activity_task(req.completion.unwrap_or_default())
            .await
            .map_err(|err| bridge::complete_activity_task_response::Error {
                message: format!("{}", err),
            })
    }

    fn record_activity_heartbeat(&self, req: bridge::RecordActivityHeartbeatRequest) {
        self.worker
            .record_activity_heartbeat(req.heartbeat.unwrap_or_default());
    }

    fn request_workflow_eviction(&self, req: bridge::RequestWorkflowEvictionRequest) {
        self.worker.request_workflow_eviction(&req.run_id);
    }

    fn borrow_buf(&mut self) -> Vec<u8> {
        // We currently do not use a thread-safe byte pool, but if wanted, it
        // can be added here
        Vec::new()
    }

    fn return_buf(&mut self, _vec: Vec<u8>) {
        // We currently do not use a thread-safe byte pool, but if wanted, it
        // can be added here
    }

    fn encode_proto(&mut self, proto: &impl prost::Message) -> tmprl_bytes_t {
        let mut buf = self.borrow_buf();
        buf.clear();
        // Increase buf capacity if needed
        buf.reserve(proto.encoded_len());
        // Only fails if size not big enough which can't happen in our case
        proto.encode(&mut buf).unwrap();
        tmprl_bytes_t::from_vec(buf)
    }

    fn decode_proto<P>(bytes: *const u8, bytes_len: libc::size_t) -> Result<P, String>
    where
        P: prost::Message,
        P: Default,
    {
        P::decode(unsafe { std::slice::from_raw_parts(bytes, bytes_len) })
            .map_err(|err| format!("failed decoding proto: {}", err))
    }
}
