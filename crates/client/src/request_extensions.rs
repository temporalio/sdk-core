//! Request extensions for tonic gRPC requests.
//!
//! These types can be inserted into tonic request extensions to modify behavior of the
//! [RetryClient](crate::RetryClient) or other request handling logic.

use crate::RetryOptions;
use std::time::Duration;

/// A request extension that, when set, should make the [RetryClient](crate::RetryClient) consider
/// this call to be a [CallType::TaskLongPoll](crate::CallType::TaskLongPoll)
#[derive(Copy, Clone, Debug)]
pub struct IsWorkerTaskLongPoll;

/// A request extension that, when set, and a call is being processed by a
/// [RetryClient](crate::RetryClient), allows the caller to request certain matching errors to
/// short-circuit-return immediately and not follow normal retry logic.
#[derive(Copy, Clone, Debug)]
pub struct NoRetryOnMatching {
    /// Return true if the passed-in gRPC error should be immediately returned to the caller
    pub predicate: fn(&tonic::Status) -> bool,
}

/// A request extension that forces overriding the current retry policy of the
/// [RetryClient](crate::RetryClient).
#[derive(Clone, Debug)]
pub struct RetryConfigForCall(pub RetryOptions);

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
