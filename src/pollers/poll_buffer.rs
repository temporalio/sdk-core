use crate::{
    pollers, protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse, ServerGatewayApis,
};
use futures::{stream::repeat_with, Stream, StreamExt};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct LongPollBuffer<T> {
    buffered_polls: Mutex<Box<dyn Stream<Item = pollers::Result<T>> + Unpin + Send>>,
}

impl<T> LongPollBuffer<T>
where
    T: Send,
{
    pub fn new<FT>(poll_fn: impl Fn() -> FT + Send + 'static) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
    {
        // This is not the world's most efficient thing, but given we're wrapping IO it's unlikely
        // to be of any significance.
        let pbuff = repeat_with(poll_fn).buffered(1);
        Self {
            buffered_polls: Mutex::new(Box::new(pbuff)),
        }
    }

    pub async fn poll(&self) -> pollers::Result<T> {
        let mut locked = self.buffered_polls.lock().await;
        (*locked)
            .next()
            .await
            .expect("There is always another item in the stream")
    }
}

pub type PollWorkflowTaskBuffer = LongPollBuffer<PollWorkflowTaskQueueResponse>;
pub fn new_workflow_task_buffer(
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
) -> PollWorkflowTaskBuffer {
    LongPollBuffer::new(move || {
        let sg = sg.clone();
        async move { sg.poll_workflow_task().await }
    })
}

pub type PollActivityTaskBuffer = LongPollBuffer<PollActivityTaskQueueResponse>;
pub fn new_activity_task_buffer(
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
) -> PollActivityTaskBuffer {
    LongPollBuffer::new(move || {
        let sg = sg.clone();
        async move { sg.poll_activity_task().await }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pollers::manual_mock::MockManualGateway;
    use futures::FutureExt;
    use std::time::Duration;
    use tokio::{select, sync::mpsc::channel};

    #[tokio::test]
    async fn only_polls_once() {
        let mut mock_gateway = MockManualGateway::new();
        mock_gateway
            .expect_poll_workflow_task()
            .times(2)
            .returning(move || {
                async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(Default::default())
                }
                .boxed()
            });
        let mock_gateway = Arc::new(mock_gateway);

        let pb = new_workflow_task_buffer(mock_gateway);

        // Poll a bunch of times, "interrupting" it each time, we should only actually have polled
        // once since the poll takes a while
        let (interrupter_tx, mut interrupter_rx) = channel(50);
        for _ in 0..10 {
            interrupter_tx.send(()).await.unwrap();
        }

        let mut last_val = false;
        for _ in 0..11 {
            select! {
                _ = interrupter_rx.recv() => {
                }
                _ = pb.poll() => {
                    last_val = true;
                }
            }
        }
        assert!(last_val);
        // Now we poll for the second time here, fulfilling our 2 times expectation
        pb.poll().await.unwrap();
    }
}
