pub mod protos;

use protos::coresdk::{PollSdkTaskReq, PollSdkTaskResp};

type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

// TODO: Should probably enforce Send + Sync
#[async_trait::async_trait]
pub trait CoreSDKService {
    async fn poll_sdk_task(&self, req: PollSdkTaskReq) -> Result<PollSdkTaskResp>;
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
