#[macro_use]
extern crate log;

mod machines;
mod pollers;
pub mod protos;

use protos::coresdk::{CompleteSdkTaskReq, PollSdkTaskResp, RegistrationReq};

pub type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

// TODO: Should probably enforce Send + Sync
#[async_trait::async_trait]
pub trait CoreSDKService {
    async fn poll_sdk_task(&self) -> Result<PollSdkTaskResp>;
    async fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<()>;
    async fn register_implementations(&self, req: RegistrationReq) -> Result<()>;
}

pub struct CoreSDKInitOptions {
    queue_name: String,
    max_concurrent_workflow_executions: u32,
    max_concurrent_activity_executions: u32,
}

unsafe impl Send for CoreSDKInitOptions {}
unsafe impl Sync for CoreSDKInitOptions {}

pub fn init_sdk(opts: CoreSDKInitOptions) -> Result<Box<dyn CoreSDKService>> {
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
