pub type Result<T, E = tonic::Status> = std::result::Result<T, E>;

#[async_trait::async_trait]
pub trait PollTask<T> {
    async fn poll(&mut self) -> Result<T>;
}
