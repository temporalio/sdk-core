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

use prost::Message;
use temporal_sdk_core_protos::coresdk::bridge;

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

/// Free a set of bytes. The first parameter can be null in cases where a
/// tmprl_core_t instance isn't available. If the second parameter is null, this
/// is a no-op.
#[no_mangle]
pub extern "C" fn tmprl_bytes_free(core: *mut tmprl_core_t, bytes: *const tmprl_bytes_t) {
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
    // Return only if core is non-null
    if !core.is_null() {
        let core = unsafe { &mut *core };
        core.return_buf(vec);
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

    static ref DEFAULT_SHUTDOWN_RESPONSE_BYTES: tmprl_bytes_t = {
        tmprl_bytes_t::from_vec_disable_free(bridge::ShutdownResponse::default().encode_to_vec())
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

/// A runtime owned by Core. This must be passed to tmprl_runtime_free when no
/// longer in use. This must not be freed until every call to every tmprl_core_t
/// instance created with this runtime has been shutdown.
pub struct tmprl_runtime_t {
    // This is the same runtime shared with the core instance
    tokio_runtime: std::sync::Arc<tokio::runtime::Runtime>,
}

/// Create a new runtime. The result is never null and must be freed via
/// tmprl_runtime_free when no longer in use.
#[no_mangle]
pub extern "C" fn tmprl_runtime_new() -> *mut tmprl_runtime_t {
    Box::into_raw(Box::new(tmprl_runtime_t {
        // TODO(cretz): Options to configure thread pool?
        tokio_runtime: std::sync::Arc::new(
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

/// A core instance owned by Core. This must be passed to tmprl_core_shutdown
/// when no longer in use which will free the resources.
pub struct tmprl_core_t {
    tokio_runtime: std::sync::Arc<tokio::runtime::Runtime>,
    // We are not concerned with the overhead of dynamic dispatch at this time
    core: std::sync::Arc<dyn temporal_sdk_core_api::Core>,
}

/// Callback called by tmprl_core_init on completion. The first parameter of the
/// callback is user data passed into the original function. The second
/// parameter is a core instance if the call is successful or null if not. If
/// present, the core instance must be freed via tmprl_core_shutdown when no
/// longer in use. The third parameter of the callback is a byte array for a
/// InitResponse protobuf message which must be freed via tmprl_bytes_free.
type tmprl_core_init_callback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    core: *mut tmprl_core_t,
    resp: *const tmprl_bytes_t,
);

/// Callback called on function completion. The first parameter of the callback
/// is user data passed into the original function. The second parameter of the
/// callback is a never-null byte array for a response protobuf message which
/// must be freed via tmprl_bytes_free.
type tmprl_callback =
    unsafe extern "C" fn(user_data: *mut libc::c_void, core: *const tmprl_bytes_t);

/// Create a new core instance.
///
/// The runtime is required and must outlive this instance. The req_proto and
/// req_proto_len represent a byte array for a InitRequest protobuf message. The
/// callback is invoked on completion.
#[no_mangle]
pub extern "C" fn tmprl_core_init(
    runtime: *mut tmprl_runtime_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_core_init_callback,
) {
    let runtime = unsafe { &*runtime };
    let req = match tmprl_core_t::decode_proto::<bridge::InitRequest>(req_proto, req_proto_len) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::InitResponse {
                error: Some(bridge::init_response::Error { message }),
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
        match tmprl_core_t::new(
            runtime.tokio_runtime.clone(),
            wrappers::CoreInitOptions(req),
        )
        .await
        {
            Ok(core) => unsafe {
                callback(
                    user_data.into(),
                    Box::into_raw(Box::new(core)),
                    &*DEFAULT_INIT_RESPONSE_BYTES,
                );
            },
            Err(message) => {
                let resp = bridge::InitResponse {
                    error: Some(bridge::init_response::Error { message }),
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

/// Shutdown and free a core instance.
///
/// The req_proto and req_proto_len represent a byte array for a ShutdownRequest
/// protobuf message. The callback is invoked on completion with a
/// ShutdownResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_core_shutdown(
    core: *mut tmprl_core_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    // Re-own the object so it can be dropped
    let core = unsafe { Box::from_raw(core) };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        core.shutdown().await;
        unsafe {
            callback(user_data.into(), &*DEFAULT_SHUTDOWN_RESPONSE_BYTES);
        }
    });
}

/// Register a worker.
///
/// The req_proto and req_proto_len represent a byte array for a RegisterWorker
/// protobuf message. The callback is invoked on completion with a
/// RegisterWorkerResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_register_worker(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req =
        match tmprl_core_t::decode_proto::<bridge::RegisterWorkerRequest>(req_proto, req_proto_len)
        {
            Ok(req) => req,
            Err(message) => {
                let resp = bridge::RegisterWorkerResponse {
                    error: Some(bridge::register_worker_response::Error {
                        message,
                        worker_already_registered: false,
                    }),
                };
                unsafe {
                    callback(user_data, core.encode_proto(&resp).into_raw());
                }
                return;
            }
        };
    let user_data = UserDataHandle(user_data);
    match core.register_worker(wrappers::WorkerConfig(req)) {
        Ok(()) => unsafe {
            callback(user_data.into(), &*DEFAULT_REGISTER_WORKER_RESPONSE_BYTES);
        },
        Err(err) => {
            let resp = bridge::RegisterWorkerResponse { error: Some(err) };
            unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
        }
    }
}

/// Shutdown registered worker.
///
/// The req_proto and req_proto_len represent a byte array for a
/// ShutdownWorkerRequest protobuf message. The callback is invoked on
/// completion with a ShutdownWorkerResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_shutdown_worker(
    core: *mut tmprl_core_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req =
        match tmprl_core_t::decode_proto::<bridge::ShutdownWorkerRequest>(req_proto, req_proto_len)
        {
            Ok(req) => req,
            Err(message) => {
                let resp = bridge::ShutdownWorkerResponse {
                    error: Some(bridge::shutdown_worker_response::Error { message }),
                };
                unsafe {
                    callback(user_data, core.encode_proto(&resp).into_raw());
                }
                return;
            }
        };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        core.shutdown_worker(req).await;
        unsafe {
            callback(user_data.into(), &*DEFAULT_SHUTDOWN_WORKER_RESPONSE_BYTES);
        }
    });
}

/// Poll workflow activation.
///
/// The req_proto and req_proto_len represent a byte array for a
/// PollWorkflowActivationRequest protobuf message. The callback is invoked on
/// completion with a PollWorkflowActivationResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_poll_workflow_activation(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::PollWorkflowActivationRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::PollWorkflowActivationResponse {
                response: Some(bridge::poll_workflow_activation_response::Response::Error(
                    bridge::poll_workflow_activation_response::Error {
                        message,
                        shutdown: false,
                    },
                )),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        let resp = bridge::PollWorkflowActivationResponse {
            response: Some(match core.poll_workflow_activation(req).await {
                Ok(act) => bridge::poll_workflow_activation_response::Response::Activation(act),
                Err(err) => bridge::poll_workflow_activation_response::Response::Error(err),
            }),
        };
        unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
    });
}

/// Poll activity task.
///
/// The req_proto and req_proto_len represent a byte array for a
/// PollActivityTaskRequest protobuf message. The callback is invoked on
/// completion with a PollActivityTaskResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_poll_activity_task(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::PollActivityTaskRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::PollActivityTaskResponse {
                response: Some(bridge::poll_activity_task_response::Response::Error(
                    bridge::poll_activity_task_response::Error {
                        message,
                        shutdown: false,
                    },
                )),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        let resp = bridge::PollActivityTaskResponse {
            response: Some(match core.poll_activity_task(req).await {
                Ok(task) => bridge::poll_activity_task_response::Response::Task(task),
                Err(err) => bridge::poll_activity_task_response::Response::Error(err),
            }),
        };
        unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
    });
}

/// Complete workflow activation.
///
/// The req_proto and req_proto_len represent a byte array for a
/// CompleteWorkflowActivationRequest protobuf message. The callback is invoked
/// on completion with a CompleteWorkflowActivationResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_complete_workflow_activation(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::CompleteWorkflowActivationRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::CompleteWorkflowActivationResponse {
                error: Some(bridge::complete_workflow_activation_response::Error { message }),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        match core.complete_workflow_activation(req).await {
            Ok(()) => unsafe {
                callback(
                    user_data.into(),
                    &*DEFAULT_COMPLETE_WORKFLOW_ACTIVATION_RESPONSE_BYTES,
                );
            },
            Err(err) => {
                let resp = bridge::CompleteWorkflowActivationResponse { error: Some(err) };
                unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
            }
        }
    });
}

/// Complete activity task.
///
/// The req_proto and req_proto_len represent a byte array for a
/// CompleteActivityTaskRequest protobuf message. The callback is invoked
/// on completion with a CompleteActivityTaskResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_complete_activity_task(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::CompleteActivityTaskRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::CompleteActivityTaskResponse {
                error: Some(bridge::complete_activity_task_response::Error { message }),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    core.tokio_runtime.clone().spawn(async move {
        match core.complete_activity_task(req).await {
            Ok(()) => unsafe {
                callback(
                    user_data.into(),
                    &*DEFAULT_COMPLETE_ACTIVITY_TASK_RESPONSE_BYTES,
                );
            },
            Err(err) => {
                let resp = bridge::CompleteActivityTaskResponse { error: Some(err) };
                unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
            }
        }
    });
}

/// Record activity heartbeat.
///
/// The req_proto and req_proto_len represent a byte array for a
/// RecordActivityHeartbeatRequest protobuf message. The callback is invoked
/// on completion with a RecordActivityHeartbeatResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_record_activity_heartbeat(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::RecordActivityHeartbeatRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::RecordActivityHeartbeatResponse {
                error: Some(bridge::record_activity_heartbeat_response::Error { message }),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    core.tokio_runtime.clone().spawn(async move {
        core.record_activity_heartbeat(req);
        unsafe {
            callback(
                user_data.into(),
                &*DEFAULT_RECORD_ACTIVITY_HEARTBEAT_RESPONSE_BYTES,
            );
        }
    });
}

/// Request workflow eviction.
///
/// The req_proto and req_proto_len represent a byte array for a
/// RequestWorkflowEvictionRequest protobuf message. The callback is invoked
/// on completion with a RequestWorkflowEvictionResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_request_workflow_eviction(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let req = match tmprl_core_t::decode_proto::<bridge::RequestWorkflowEvictionRequest>(
        req_proto,
        req_proto_len,
    ) {
        Ok(req) => req,
        Err(message) => {
            let resp = bridge::RequestWorkflowEvictionResponse {
                error: Some(bridge::request_workflow_eviction_response::Error { message }),
            };
            unsafe {
                callback(user_data, core.encode_proto(&resp).into_raw());
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    core.tokio_runtime.clone().spawn(async move {
        core.request_workflow_eviction(req);
        unsafe {
            callback(
                user_data.into(),
                &*DEFAULT_REQUEST_WORKFLOW_EVICTION_RESPONSE_BYTES,
            );
        }
    });
}

/// Fetch buffered logs.
///
/// The req_proto and req_proto_len represent a byte array for a
/// FetchBufferedLogsRequest protobuf message. The callback is invoked
/// on completion with a FetchBufferedLogsResponse protobuf message.
#[no_mangle]
pub extern "C" fn tmprl_fetch_buffered_logs(
    core: *mut tmprl_core_t,
    #[allow(unused_variables)] // We intentionally ignore the request
    req_proto: *const u8,
    #[allow(unused_variables)] req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: tmprl_callback,
) {
    let core = unsafe { &mut *core };
    let user_data = UserDataHandle(user_data);
    // We intentionally spawn even though the core call is not async so the
    // callback can be made in the tokio runtime
    core.tokio_runtime.clone().spawn(async move {
        let resp = core.fetch_buffered_logs();
        unsafe { callback(user_data.into(), core.encode_proto(&resp).into_raw()) };
    });
}

impl tmprl_core_t {
    async fn new(
        tokio_runtime: std::sync::Arc<tokio::runtime::Runtime>,
        opts: wrappers::CoreInitOptions,
    ) -> Result<tmprl_core_t, String> {
        Ok(tmprl_core_t {
            tokio_runtime,
            core: std::sync::Arc::new(
                temporal_sdk_core::init(opts.try_into()?)
                    .await
                    .map_err(|err| format!("failed initializing: {}", err))?,
            ),
        })
    }

    async fn shutdown(&self) {
        self.core.shutdown().await;
    }

    fn register_worker(
        &self,
        config: wrappers::WorkerConfig,
    ) -> Result<(), bridge::register_worker_response::Error> {
        let config =
            config
                .try_into()
                .map_err(|message| bridge::register_worker_response::Error {
                    message,
                    worker_already_registered: false,
                })?;
        self.core.register_worker(config).map_err(|err| {
            bridge::register_worker_response::Error {
                message: format!("{}", err),
                worker_already_registered: matches!(
                    err,
                    temporal_sdk_core_api::errors::WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(_),
                ),
            }
        })
    }

    async fn shutdown_worker(&self, req: bridge::ShutdownWorkerRequest) {
        self.core.shutdown_worker(&req.task_queue).await;
    }

    async fn poll_workflow_activation(
        &self,
        req: bridge::PollWorkflowActivationRequest,
    ) -> Result<
        temporal_sdk_core_protos::coresdk::workflow_activation::WorkflowActivation,
        bridge::poll_workflow_activation_response::Error,
    > {
        self.core
            .poll_workflow_activation(&req.task_queue)
            .await
            .map_err(|err| bridge::poll_workflow_activation_response::Error {
                message: format!("{}", err),
                shutdown: matches!(err, temporal_sdk_core_api::errors::PollWfError::ShutDown),
            })
    }

    async fn poll_activity_task(
        &self,
        req: bridge::PollActivityTaskRequest,
    ) -> Result<
        temporal_sdk_core_protos::coresdk::activity_task::ActivityTask,
        bridge::poll_activity_task_response::Error,
    > {
        self.core
            .poll_activity_task(&req.task_queue)
            .await
            .map_err(|err| bridge::poll_activity_task_response::Error {
                message: format!("{}", err),
                shutdown: matches!(
                    err,
                    temporal_sdk_core_api::errors::PollActivityError::ShutDown
                ),
            })
    }

    async fn complete_workflow_activation(
        &self,
        req: bridge::CompleteWorkflowActivationRequest,
    ) -> Result<(), bridge::complete_workflow_activation_response::Error> {
        self.core
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
        self.core
            .complete_activity_task(req.completion.unwrap_or_default())
            .await
            .map_err(|err| bridge::complete_activity_task_response::Error {
                message: format!("{}", err),
            })
    }

    fn record_activity_heartbeat(&self, req: bridge::RecordActivityHeartbeatRequest) {
        self.core
            .record_activity_heartbeat(req.heartbeat.unwrap_or_default());
    }

    fn request_workflow_eviction(&self, req: bridge::RequestWorkflowEvictionRequest) {
        self.core
            .request_workflow_eviction(&req.task_queue, &req.run_id);
    }

    fn fetch_buffered_logs(&self) -> bridge::FetchBufferedLogsResponse {
        bridge::FetchBufferedLogsResponse {
            entries: self
                .core
                .fetch_buffered_logs()
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
        }
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
