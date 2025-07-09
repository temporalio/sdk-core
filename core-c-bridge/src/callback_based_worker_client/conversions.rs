use std::ffi::c_void;
use tonic::{Code, Status};

use temporal_sdk_core::{
    PollActivityOptions,
    PollOptions,
    PollWorkflowOptions,
};

use crate::ByteArrayRef;

use crate::callback_based_worker_client::interop_types::{
    CallbackBasedWorkerClientGRPCCode,
    CallbackBasedWorkerClientNoRetryPredicate,
    CallbackBasedWorkerClientPollActivityOptions,
    CallbackBasedWorkerClientPollOptions,
    CallbackBasedWorkerClientPollWorkflowOptions,
    CallbackBasedWorkerClientConfig
};
use crate::callback_based_worker_client::client_impl::WorkerClientConfig;

/// Convert interop [CallbackBasedWorkerClientConfig] to [WorkerClientConfig]
impl From<&CallbackBasedWorkerClientConfig> for WorkerClientConfig {
    fn from(config: &CallbackBasedWorkerClientConfig) -> Self {
        WorkerClientConfig {
            client_name:    config.client_name.to_string(),
            client_version: config.client_version.to_string(),
            identity:       config.identity.to_string(),
        }
    }
}

// Convert Rust Core SDK options → interop structs
impl From<&PollOptions> for CallbackBasedWorkerClientPollOptions {
    fn from(o: &PollOptions) -> Self {
        let task_queue = ByteArrayRef::from(o.task_queue.as_str());

        // timeout
        let (has_timeout_ms, timeout_ms) = o.timeout_override
            .map(|d| (true, d.as_millis() as u64))
            .unwrap_or((false, 0));

        // retry predicate
        let (has_no_retry, no_retry_predicate, no_retry_user_data) = if let Some(nr) = &o.no_retry {
            extern "C" fn thunk(user_data: *mut c_void, code: CallbackBasedWorkerClientGRPCCode) -> bool {
                // reconstruct the Rust predicate
                let f: fn(&Status) -> bool = unsafe { std::mem::transmute(user_data) };
                let st = Status::new(Code::from(code), "");
                f(&st)
            }
            let fptr: fn(&Status) -> bool = nr.predicate;
            (true, thunk as CallbackBasedWorkerClientNoRetryPredicate, fptr as *mut _)
        } else {
            (false, dummy_predicate as CallbackBasedWorkerClientNoRetryPredicate, std::ptr::null_mut())
        };

        CallbackBasedWorkerClientPollOptions {
            task_queue,
            has_no_retry,
            no_retry_predicate,
            no_retry_user_data,
            has_timeout_ms,
            timeout_ms,
        }
    }
}

unsafe extern "C" fn dummy_predicate(_: *mut c_void, _: CallbackBasedWorkerClientGRPCCode) -> bool {
    false
}

impl From<&PollWorkflowOptions> for CallbackBasedWorkerClientPollWorkflowOptions {
    fn from(o: &PollWorkflowOptions) -> Self {
        if let Some(name) = &o.sticky_queue_name {
            CallbackBasedWorkerClientPollWorkflowOptions {
                has_sticky_queue_name: true,
                sticky_queue_name: ByteArrayRef::from(name.as_str()),
            }
        } else {
            CallbackBasedWorkerClientPollWorkflowOptions {
                has_sticky_queue_name: false,
                sticky_queue_name: ByteArrayRef::empty(),
            }
        }
    }
}

impl From<&PollActivityOptions> for CallbackBasedWorkerClientPollActivityOptions {
    fn from(o: &PollActivityOptions) -> Self {
        if let Some(val) = o.max_tasks_per_sec {
            CallbackBasedWorkerClientPollActivityOptions {
                has_max_tasks_per_sec: true,
                max_tasks_per_sec: val,
            }
        } else {
            CallbackBasedWorkerClientPollActivityOptions {
                has_max_tasks_per_sec: false,
                max_tasks_per_sec: 0.0,
            }
        }
    }
}

// Convert Interop structs → Rust Tonic Code struct
impl From<CallbackBasedWorkerClientGRPCCode> for Code {
    fn from(c: CallbackBasedWorkerClientGRPCCode) -> Self {
        match c {
            CallbackBasedWorkerClientGRPCCode::Ok                 => Code::Ok,
            CallbackBasedWorkerClientGRPCCode::Cancelled          => Code::Cancelled,
            CallbackBasedWorkerClientGRPCCode::Unknown            => Code::Unknown,
            CallbackBasedWorkerClientGRPCCode::InvalidArgument    => Code::InvalidArgument,
            CallbackBasedWorkerClientGRPCCode::DeadlineExceeded   => Code::DeadlineExceeded,
            CallbackBasedWorkerClientGRPCCode::NotFound           => Code::NotFound,
            CallbackBasedWorkerClientGRPCCode::AlreadyExists      => Code::AlreadyExists,
            CallbackBasedWorkerClientGRPCCode::PermissionDenied   => Code::PermissionDenied,
            CallbackBasedWorkerClientGRPCCode::ResourceExhausted  => Code::ResourceExhausted,
            CallbackBasedWorkerClientGRPCCode::FailedPrecondition => Code::FailedPrecondition,
            CallbackBasedWorkerClientGRPCCode::Aborted            => Code::Aborted,
            CallbackBasedWorkerClientGRPCCode::OutOfRange         => Code::OutOfRange,
            CallbackBasedWorkerClientGRPCCode::Unimplemented      => Code::Unimplemented,
            CallbackBasedWorkerClientGRPCCode::Internal           => Code::Internal,
            CallbackBasedWorkerClientGRPCCode::Unavailable        => Code::Unavailable,
            CallbackBasedWorkerClientGRPCCode::DataLoss           => Code::DataLoss,
            CallbackBasedWorkerClientGRPCCode::Unauthenticated    => Code::Unauthenticated,
        }
    }
}