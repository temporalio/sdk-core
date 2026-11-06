//! Request extensions for tonic gRPC requests.
//!
//! These types can be inserted into tonic request extensions to modify behavior of retires or other
//! request handling logic.

use crate::RetryOptions;
use dyn_clone::DynClone;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

/// A request extension that, when set, should make the retry behavior consider this call to be a
/// worker task long poll.
#[derive(Copy, Clone, Debug)]
pub struct IsWorkerTaskLongPoll;

/// A request extension that, when set, and a call is being processed by a retrying client, allows
/// the caller to request certain matching errors to short-circuit-return immediately and not follow
/// normal retry logic.
#[derive(Copy, Clone, Debug)]
pub struct NoRetryOnMatching {
    /// Return true if the passed-in gRPC error should be immediately returned to the caller
    pub predicate: fn(&tonic::Status) -> bool,
}

/// A request extension that forces overriding the current retry policy of the [crate::Connection].
#[derive(Clone, Debug)]
pub struct RetryConfigForCall(pub RetryOptions);

/// A request extension that forces overriding the current retry policy of the [crate::Connection]
/// used in throttling (resource exhausted) scenarios.
#[derive(Clone, Debug)]
pub struct ThrottleRetryConfigForCall(pub RetryOptions);

/// A request extension that calls a callback after a request failed if it's going to be retried.
#[derive(Clone)]
pub struct RetryCallback(Box<dyn RetryCallbackTrait>);

impl RetryCallback {
    /// Make a new instance from the provided closure.
    pub fn new(callback: impl FnMut(&tonic::Status) + Clone + Send + Sync + 'static) -> Self {
        Self(Box::new(callback))
    }

    /// Call the callback.
    pub fn call(&mut self, status: &tonic::Status) {
        self.0(status);
    }
}

impl Debug for RetryCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RetryCallback")
    }
}

/// Internal. Closure type for [`RetryCallback`].
pub(crate) trait RetryCallbackTrait:
    FnMut(&tonic::Status) + DynClone + Send + Sync + 'static
{
}
impl<T> RetryCallbackTrait for T where T: FnMut(&tonic::Status) + Clone + Send + Sync + 'static {}
dyn_clone::clone_trait_object!(RetryCallbackTrait);

/// A request extension that obtains a permit from the provided semaphore before every attempt.
/// On a retryable error, the permit is released until next attempt. On the final response, whether
/// successful or an error, the permit is leaked (this can be used to implement cancellation logic
/// that only runs between attempts and not during a request and not after completion).
#[derive(Clone, Debug)]
pub struct AttemptSemaphore(pub Arc<Semaphore>);

/// Per-call payload/memo size error limits, attached to a request's extensions by a caller that
/// wants error-level enforcement on this call.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PayloadErrorLimits {
    /// Blob (payload) size error threshold, in bytes.
    pub blob: usize,
    /// Memo size error threshold, in bytes.
    pub memo: usize,
}

/// Extension trait for tonic requests to set default timeouts
pub trait RequestExt {
    /// Set a timeout for a request if one is not already specified in the metadata
    fn set_default_timeout(&mut self, duration: Duration);
}

impl<T> RequestExt for tonic::Request<T> {
    fn set_default_timeout(&mut self, duration: Duration) {
        if !self.metadata().contains_key("grpc-timeout") {
            self.set_timeout(duration)
        }
    }
}
