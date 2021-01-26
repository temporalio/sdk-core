#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;

use protos::coresdk::{CompleteSdkTaskReq, PollSdkTaskResp, RegistrationReq};

pub type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

pub trait CoreSDKService {
    fn poll_sdk_task(&self) -> Result<PollSdkTaskResp>;
    fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<()>;
    fn register_implementations(&self, req: RegistrationReq) -> Result<()>;
}

pub struct CoreSDKInitOptions {
    _queue_name: String,
    _max_concurrent_workflow_executions: u32,
    _max_concurrent_activity_executions: u32,
}

pub fn init_sdk(_opts: CoreSDKInitOptions) -> Result<Box<dyn CoreSDKService>> {
    Err(SDKServiceError::Unknown {})
}

#[derive(thiserror::Error, Debug)]
pub enum SDKServiceError {
    #[error("Unknown service error")]
    Unknown,
    // more errors TBD
}

#[cfg(test)]
mod test {
    #[test]
    fn foo() {}
}
