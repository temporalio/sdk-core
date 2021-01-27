#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;

use protos::coresdk::{CompleteSdkTaskReq, CompleteSdkTaskResp, PollSdkTaskReq, PollSdkTaskResp};

type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

// TODO: Should probably enforce Send + Sync
#[async_trait::async_trait]
pub trait CoreSDKService {
    async fn poll_sdk_task(&self, req: PollSdkTaskReq) -> Result<PollSdkTaskResp>;
    async fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<CompleteSdkTaskResp>;
}

#[derive(thiserror::Error, Debug)]
pub enum SDKServiceError {
    // tbd
}

#[cfg(test)]
mod test {
    #[test]
    fn foo() {}
}
