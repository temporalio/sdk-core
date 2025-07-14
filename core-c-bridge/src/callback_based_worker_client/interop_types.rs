use std::ffi::c_void;
use crate::ByteArrayRef;

/// Interop configuration of the [WorkerClientConfig].
#[repr(C)]
pub struct CallbackBasedWorkerClientConfig {
    pub client_name: ByteArrayRef,
    pub client_version: ByteArrayRef,
    pub identity: ByteArrayRef
}

/// Interop mirror of [temporal_sdk_core::PollOptions]
#[repr(C)]
pub struct CallbackBasedWorkerClientPollOptions {
    /// always non-null (could be empty)
    pub task_queue: ByteArrayRef,

    /// true if `no_retry_predicate` is valid
    pub has_no_retry: bool,
    /// Decide short‐circuit GRPC error
    pub no_retry_predicate: CallbackBasedWorkerClientNoRetryPredicate,
    /// Opaque context pointer passed back to your predicate thunk
    pub no_retry_user_data: *mut c_void,

    /// true if `timeout_override.is_some()`
    pub has_timeout_ms: bool,
    /// timeout in milliseconds
    pub timeout_ms: u64,
}

unsafe impl Send for CallbackBasedWorkerClientPollOptions {}
unsafe impl Sync for CallbackBasedWorkerClientPollOptions {}

/// Interop mirror of [temporal_sdk_core::PollWorkflowOptions]
#[repr(C)]
pub struct CallbackBasedWorkerClientPollWorkflowOptions {
    /// true if `sticky_queue_name.is_some()`
    pub has_sticky_queue_name: bool,
    /// the inner string (only valid if above true)
    pub sticky_queue_name: ByteArrayRef,
}

unsafe impl Send for CallbackBasedWorkerClientPollWorkflowOptions {}
unsafe impl Sync for CallbackBasedWorkerClientPollWorkflowOptions {}

/// Interop mirror of [temporal_sdk_core::PollActivityOptions]
#[repr(C)]
pub struct CallbackBasedWorkerClientPollActivityOptions {
    /// true if `max_tasks_per_sec.is_some()`
    pub has_max_tasks_per_sec: bool,
    /// the inner `f64` value
    pub max_tasks_per_sec: f64,
}

/// Interop mirror of the Rust tonic [tonic::Code] enum
#[repr(C)]
pub enum CallbackBasedWorkerClientGRPCCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

/// Interop function pointer signature of the retry predicate of [CallbackBasedWorkerClientPollOptions]
pub type CallbackBasedWorkerClientNoRetryPredicate = unsafe extern "C" fn(user_data: *mut c_void, code: CallbackBasedWorkerClientGRPCCode) -> bool;