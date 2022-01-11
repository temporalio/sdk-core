#![allow(non_camel_case_types)]

use prost::Message;
use std::str::FromStr;
use temporal_sdk_core_protos::coresdk::bridge;

extern crate libc;

/// A set of bytes owned by Core. This must always be passed to
/// tmprl_bytes_free when no longer in use.
#[repr(C)]
pub struct tmprl_bytes_t {
    bytes: *const u8,
    len: libc::size_t,
    cap: libc::size_t,
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

// We only impl this because they are required by lazy_static and raw pointers
// are not usually safe for send
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

// Used for maintaining pointer to user data across threads
struct UserDataHandle(*mut libc::c_void);
unsafe impl Send for UserDataHandle {}
unsafe impl Sync for UserDataHandle {}

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
}

/// A runtime owned by Core. This must be passed to tmprl_runtime_free when no
/// longer in use. This must not be freed until every call to every tmprl_core_t
/// instance created with this runtime has been shutdown.
pub struct tmprl_runtime_t {
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
    // TODO(cretz): Any concerns with the dynamic dispatch overhead here?
    // TODO(cretz): Should we ask Rust SDK to give us back a struct instead?
    // TODO(cretz): We could use generics, but impl traits don't cross the FFI
    // boundary properly
    core: std::sync::Arc<dyn temporal_sdk_core_api::Core>,
}

/// Create a new core instance.
/// 
/// The runtime is required and must outlive this instance. The req_proto and
/// req_proto_len represent a byte array for a InitRequest protobuf message.
/// 
/// The callback is invoked on completion. The first parameter of the callback
/// is a core instance if the call is successful or null if not. If present, the
/// core instance must be freed via tmprl_core_shutdown when no longer in use.
/// The second parameter of the callback is a byte array for a InitResponse
/// protobuf message which must be freed via tmprl_bytes_free.
#[no_mangle]
pub extern "C" fn tmprl_core_init(
    runtime: *mut tmprl_runtime_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: unsafe extern "C" fn(user_data: *mut libc::c_void, core: *mut tmprl_core_t, resp: *const tmprl_bytes_t),
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
    let user_data = Box::new(UserDataHandle(user_data));
    runtime.tokio_runtime.spawn(async move {
        match tmprl_core_t::new(runtime.tokio_runtime.clone(), req).await {
            Ok(core) => unsafe {
                callback(user_data.0, Box::into_raw(Box::new(core)), &*DEFAULT_INIT_RESPONSE_BYTES);
            },
            Err(message) => {
                let resp = bridge::InitResponse {
                    error: Some(bridge::init_response::Error { message }),
                };
                unsafe {
                    callback(
                        user_data.0,
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
/// protobuf message.
/// 
/// The callback is invoked on completion with a never-null byte array for a
/// ShutdownResponse protobuf message which must be freed via tmprl_bytes_free.
#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn tmprl_core_shutdown(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: unsafe extern "C" fn(*mut libc::c_void, *const tmprl_bytes_t),
) {
    // Re-own the object so it can be dropped
    let core = unsafe { Box::from_raw(core) };
    let user_data = Box::new(UserDataHandle(user_data));
    core.tokio_runtime.clone().spawn(async move {
        core.shutdown().await;
        unsafe {
            callback(user_data.0, &*DEFAULT_SHUTDOWN_RESPONSE_BYTES);
        }
    });
}

/// Register a worker.
/// 
/// The req_proto and req_proto_len represent a byte array for a RegisterWorker
/// protobuf message.
/// 
/// The callback is invoked on completion with a never-null byte array for a
/// RegisterWorkflowResponse protobuf message which must be freed via
/// tmprl_bytes_free.
#[no_mangle]
pub extern "C" fn tmprl_register_worker(
    core: *mut tmprl_core_t,
    req_proto: *const u8,
    req_proto_len: libc::size_t,
    user_data: *mut libc::c_void,
    callback: unsafe extern "C" fn(*mut libc::c_void, *const tmprl_bytes_t),
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
    let user_data = Box::new(UserDataHandle(user_data));
    core.tokio_runtime.clone().spawn(async move {
        match core.register_worker(req).await {
            Ok(()) => unsafe {
                callback(user_data.0, &*DEFAULT_REGISTER_WORKER_RESPONSE_BYTES);
            },
            Err(err) => {
                let resp = bridge::RegisterWorkerResponse { error: Some(err) };
                unsafe { callback(user_data.0, core.encode_proto(&resp).into_raw()) };
            }
        }
    });
}

fn register_worker_error(
    message: String,
    worker_already_registered: bool,
) -> bridge::register_worker_response::Error {
    bridge::register_worker_response::Error {
        message,
        worker_already_registered,
    }
}

impl tmprl_core_t {
    async fn new(
        tokio_runtime: std::sync::Arc<tokio::runtime::Runtime>,
        req: bridge::InitRequest,
    ) -> Result<tmprl_core_t, String> {
        let mut core_opts = temporal_sdk_core::CoreInitOptionsBuilder::default();
        if let Some(req_gateway_opts) = req.gateway_options {
            let mut gateway_opts = temporal_sdk_core::ServerGatewayOptionsBuilder::default();
            if !req_gateway_opts.target_url.is_empty() {
                gateway_opts.target_url(
                    temporal_sdk_core::Url::parse(&req_gateway_opts.target_url)
                        .map_err(|err| format!("invalid target URL: {}", err))?,
                );
            }
            if !req_gateway_opts.namespace.is_empty() {
                gateway_opts.namespace(req_gateway_opts.namespace);
            }
            if !req_gateway_opts.client_name.is_empty() {
                gateway_opts.client_name(req_gateway_opts.client_name);
            }
            if !req_gateway_opts.client_version.is_empty() {
                gateway_opts.client_version(req_gateway_opts.client_version);
            }
            if !req_gateway_opts.static_headers.is_empty() {
                gateway_opts.static_headers(req_gateway_opts.static_headers);
            }
            if !req_gateway_opts.identity.is_empty() {
                gateway_opts.identity(req_gateway_opts.identity);
            }
            if !req_gateway_opts.worker_binary_id.is_empty() {
                gateway_opts.worker_binary_id(req_gateway_opts.worker_binary_id);
            }
            if let Some(req_tls_config) = req_gateway_opts.tls_config {
                let mut tls_config = temporal_sdk_core::TlsConfig::default();
                if !req_tls_config.server_root_ca_cert.is_empty() {
                    tls_config.server_root_ca_cert = Some(req_tls_config.server_root_ca_cert);
                }
                if !req_tls_config.domain.is_empty() {
                    tls_config.domain = Some(req_tls_config.domain);
                }
                if !req_tls_config.client_cert.is_empty()
                    || !req_tls_config.client_private_key.is_empty()
                {
                    tls_config.client_tls_config = Some(temporal_sdk_core::ClientTlsConfig {
                        client_cert: req_tls_config.client_cert,
                        client_private_key: req_tls_config.client_private_key,
                    })
                }
                gateway_opts.tls_cfg(tls_config);
            }
            if let Some(req_retry_config) = req_gateway_opts.retry_config {
                let mut retry_config = temporal_sdk_core::RetryConfig::default();
                if let Some(v) = req_retry_config.initial_interval {
                    retry_config.initial_interval =
                        v.try_into().map_err(|_| "invalid initial interval")?;
                }
                if let Some(v) = req_retry_config.randomization_factor {
                    retry_config.randomization_factor = v;
                }
                if let Some(v) = req_retry_config.multiplier {
                    retry_config.multiplier = v;
                }
                if let Some(v) = req_retry_config.max_interval {
                    retry_config.max_interval = v.try_into().map_err(|_| "invalid max interval")?;
                }
                if let Some(v) = req_retry_config.max_elapsed_time {
                    retry_config.max_elapsed_time =
                        Some(v.try_into().map_err(|_| "invalid max elapsed time")?);
                }
                if let Some(v) = req_retry_config.max_retries {
                    retry_config.max_retries = v as usize;
                }
                gateway_opts.retry_config(retry_config);
            }
            core_opts.gateway_opts(
                gateway_opts
                    .build()
                    .map_err(|err| format!("invalid gateway options: {}", err))?,
            );
        }
        if let Some(req_telemetry_opts) = req.telemetry_options {
            let mut telemetry_opts = temporal_sdk_core::TelemetryOptionsBuilder::default();
            if !req_telemetry_opts.otel_collector_url.is_empty() {
                telemetry_opts.otel_collector_url(
                    temporal_sdk_core::Url::parse(&req_telemetry_opts.otel_collector_url)
                        .map_err(|err| format!("invalid OpenTelemetry collector URL: {}", err))?,
                );
            }
            if !req_telemetry_opts.tracing_filter.is_empty() {
                telemetry_opts.tracing_filter(req_telemetry_opts.tracing_filter.clone());
            }
            match req_telemetry_opts.log_forwarding_level() {
                bridge::LogLevel::Unspecified => {}
                bridge::LogLevel::Off => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Off);
                }
                bridge::LogLevel::Error => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Error);
                }
                bridge::LogLevel::Warn => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Warn);
                }
                bridge::LogLevel::Info => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Info);
                }
                bridge::LogLevel::Debug => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Debug);
                }
                bridge::LogLevel::Trace => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Trace);
                }
            }
            if !req_telemetry_opts.prometheus_export_bind_address.is_empty() {
                telemetry_opts.prometheus_export_bind_address(
                    std::net::SocketAddr::from_str(
                        &req_telemetry_opts.prometheus_export_bind_address,
                    )
                    .map_err(|err| format!("invalid Prometheus address: {}", err))?,
                );
            }
            core_opts.telemetry_opts(
                telemetry_opts
                    .build()
                    .map_err(|err| format!("invalid telemetry options: {}", err))?,
            );
        }
        let core_opts = core_opts
            .build()
            .map_err(|err| format!("invalid options: {}", err))?;
        Ok(tmprl_core_t {
            tokio_runtime,
            core: std::sync::Arc::new(
                temporal_sdk_core::init(core_opts)
                    .await
                    .map_err(|err| format!("failed initializing: {}", err))?,
            ),
        })
    }

    async fn shutdown(&self) {
        self.core.shutdown().await;
    }

    async fn register_worker(
        &self,
        req: bridge::RegisterWorkerRequest,
    ) -> Result<(), bridge::register_worker_response::Error> {
        let mut config = temporal_sdk_core_api::worker::WorkerConfigBuilder::default();
        if !req.task_queue.is_empty() {
            config.task_queue(req.task_queue);
        }
        if let Some(v) = req.max_cached_workflows {
            config.max_cached_workflows(v as usize);
        }
        if let Some(v) = req.max_outstanding_workflow_tasks {
            config.max_outstanding_workflow_tasks(v as usize);
        }
        if let Some(v) = req.max_outstanding_activities {
            config.max_outstanding_activities(v as usize);
        }
        if let Some(v) = req.max_outstanding_local_activities {
            config.max_outstanding_local_activities(v as usize);
        }
        if let Some(v) = req.max_concurrent_wft_polls {
            config.max_concurrent_wft_polls(v as usize);
        }
        if let Some(v) = req.nonsticky_to_sticky_poll_ratio {
            config.nonsticky_to_sticky_poll_ratio(v);
        }
        if let Some(v) = req.max_concurrent_at_polls {
            config.max_concurrent_at_polls(v as usize);
        }
        config.no_remote_activities(req.no_remote_activities);
        if let Some(v) = req.sticky_queue_schedule_to_start_timeout {
            let v: std::time::Duration = v.try_into().map_err(|_| {
                register_worker_error(
                    "invalid sticky queue schedule to start timeout".to_string(),
                    false,
                )
            })?;
            config.sticky_queue_schedule_to_start_timeout(v);
        }
        if let Some(v) = req.max_heartbeat_throttle_interval {
            let v: std::time::Duration = v.try_into().map_err(|_| {
                register_worker_error("invalid max heartbeat throttle interval".to_string(), false)
            })?;
            config.max_heartbeat_throttle_interval(v);
        }
        if let Some(v) = req.default_heartbeat_throttle_interval {
            let v: std::time::Duration = v.try_into().map_err(|_| {
                register_worker_error(
                    "invalid default heartbeat throttle interval".to_string(),
                    false,
                )
            })?;
            config.default_heartbeat_throttle_interval(v);
        }
        let config = config
            .build()
            .map_err(|err| register_worker_error(format!("invalid request: {}", err), false))?;
        self.core.register_worker(config).await.map_err(|err| {
            register_worker_error(format!("{}", err),
            matches!(err, temporal_sdk_core_api::errors::WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(_)))
        })
    }

    fn borrow_buf(&mut self) -> Vec<u8> {
        // TODO(cretz): Implement thread-safe byte pool?
        Vec::new()
    }

    fn return_buf(&mut self, vec: Vec<u8>) {
        // TODO(cretz): Implement thread-safe byte pool?
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
