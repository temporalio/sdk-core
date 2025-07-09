use libc::c_void;
use prost::Message;
use temporal_client::SlotManager;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{get_system_info_response, GetSystemInfoResponse};
use uuid::Uuid;
use std::sync::Arc;

use crate::{
    ByteArrayRef,
    enter_sync,
    runtime::Runtime,
    worker::WorkerOptions,
    worker::WorkerOrFail,
    worker::Worker
};

use crate::callback_based_worker_client::client_impl::{CallbackBasedWorkerClient, WorkerClientConfig};
use crate::callback_based_worker_client::interop_types::{
    CallbackBasedWorkerClientPollOptions,
    CallbackBasedWorkerClientConfig
};
use crate::callback_based_worker_client::trampoline::WorkerClientCallbackTrampoline;

/// Interop function pointer table implementing the RPC logic of the [temporal_sdk_core::WorkerClient] trait via callbacks.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CallbackBasedWorkerClientRPCs {
    pub user_data: *mut c_void,

    pub poll_workflow_task: unsafe extern "C" fn(
        user_data:    *mut libc::c_void,
        request:      ByteArrayRef,
        poll_opts:    *const CallbackBasedWorkerClientPollOptions,
        callback:     WorkerClientCallbackTrampoline,
        callback_ud:  *mut libc::c_void,
    ),

    pub poll_activity_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        poll_opts:      *const CallbackBasedWorkerClientPollOptions,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub poll_nexus_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        poll_opts:      *const CallbackBasedWorkerClientPollOptions,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub complete_workflow_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub complete_activity_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub complete_nexus_task: unsafe extern "C" fn(
        user_data:      *mut c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut c_void,
    ),

    pub record_activity_heartbeat: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub cancel_activity_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub fail_activity_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub fail_workflow_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub fail_nexus_task: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub get_workflow_execution_history: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub respond_legacy_query: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub describe_namespace: unsafe extern "C" fn(
        user_data:      *mut libc::c_void,
        request:        ByteArrayRef,
        callback:       WorkerClientCallbackTrampoline,
        callback_ud:    *mut libc::c_void,
    ),

    pub shutdown_worker: unsafe extern "C" fn(
        user_data:           *mut libc::c_void,
        request:             ByteArrayRef,
        callback:            WorkerClientCallbackTrampoline,
        callback_user_data:  *mut libc::c_void,
    ),

    pub is_mock: unsafe extern "C" fn(
        user_data: *mut libc::c_void,
    ) -> bool,

    // The [temporal_sdk_core::WorkerClient] `workers()`, `capabilities()`, and `sdk_name_and_version()` functions are purely implemented in Rust, no interop implementation is offered.
    // The [temporal_sdk_core::WorkerClient] `replace_client()` function is not supported by the [CallbackBasedWorkerClient].
}

unsafe impl Send for CallbackBasedWorkerClientRPCs {}
unsafe impl Sync for CallbackBasedWorkerClientRPCs {}

/// Create a new [Worker] with a custom callback-backed [temporal_sdk_core::WorkerClient] implementation.
///
/// # Safety
/// - All pointer args (`client_rpcs`, `options`, `client_config`, `runtime`) must be non-null and valid.
/// - `system_info_ref.data` must point to at least `system_info_ref.size` bytes if non-zero.
/// - Caller retains ownership; these must outlive the call.
///
/// # Params
/// - `client_rpcs`: pointer to [CallbackBasedWorkerClientRPCs] implementing Core SDK [temporal_sdk_core::WorkerClient] RPCs.
/// - `options`:     pointer to [WorkerOptions].
/// - `client_config`: pointer to [CallbackBasedWorkerClientConfig] (client name, version, identity).
/// - `system_info_ref`: zero-copy [GetSystemInfoResponse] bytes or empty.
/// - `runtime`:        pointer to [Runtime].
///
/// # Returns
/// A [WorkerOrFail] struct containing:
/// - `worker`: on success, a heap-allocated [Worker] pointer; null on error.
/// - `fail`: on error, a C string (UTF-8) describing the failure; null on success.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_new_callback_based_worker_client(
    client_rpcs: *const CallbackBasedWorkerClientRPCs,
    options: *const WorkerOptions,
    client_config: *const CallbackBasedWorkerClientConfig,
    system_info_ref: ByteArrayRef,
    runtime: *mut Runtime,
) -> WorkerOrFail {
    // Caller must guarantee non-null, valid pointers
    let (client_rpcs, options, client_config, runtime) = unsafe {
        (
            *client_rpcs,
            &*options,
            &*client_config,
            &mut *runtime
        )
    };

    let client_config: WorkerClientConfig = client_config.into();

    // Create unique sticky queue name for a worker, iff the config allows for 1 or more cached workflows
    let sticky_queue_name = if options.max_cached_workflows > 0 {
        Some(format!(
            "{}-{}",
            client_config.identity,
            Uuid::new_v4().simple()
        ))
    } else {
        None
    };

    // Decode optional system_info zero-copy and return capabilities
    let capabilities: Option<get_system_info_response::Capabilities> = if system_info_ref.size == 0 {
        None
    } else {
        let buf = unsafe {
            std::slice::from_raw_parts(
                system_info_ref.data,
                system_info_ref.size,
            )
        };
        let resp = match GetSystemInfoResponse::decode(buf) {
            Ok(resp)  => resp,
            Err(err)  => {
                let err_ptr = runtime
                    .alloc_utf8(&format!("GetSystemInfoResponse decode error: {}", err))
                    .into_raw()
                    .cast_const();
                return WorkerOrFail {
                    worker: std::ptr::null_mut(),
                    fail:   err_ptr,
                };
            }
        };
        resp.capabilities
    };

    enter_sync!(runtime);

    // Convert interop options to Core SDK [temporal_sdk_core::WorkerConfig]
    let worker_config: temporal_sdk_core::WorkerConfig = match options.try_into() {
        Err(err) => {
            let err_ptr = runtime
                .alloc_utf8(&format!("Invalid options: {}", err))
                .into_raw()
                .cast_const();
            return WorkerOrFail {
                worker: std::ptr::null_mut(),
                fail:   err_ptr,
            };
        }
        Ok(cfg) => cfg,
    };

    // Create the Core SDK [temporal_sdk_core::Worker]
    let core_worker = temporal_sdk_core::Worker::new(
        worker_config.clone(),
        sticky_queue_name,
        Arc::new(CallbackBasedWorkerClient {
            inner: client_rpcs,       // RPCs implemented via the [CallbackBasedWorkerClientRPCs]
            worker_config: worker_config.clone(),
            client_config: client_config,
            capabilities: capabilities,
            workers: Arc::new(SlotManager::new())
        }),
        None, // no telemetry
    );

    // Wrap it in our Bridge Worker and heap‐allocate
    let bridge_worker = Worker {
        worker:  Some(Arc::new(core_worker)),
        runtime: runtime.clone(),
    };
    WorkerOrFail {
        worker: Box::into_raw(Box::new(bridge_worker)),
        fail:   std::ptr::null_mut(),
    }
}