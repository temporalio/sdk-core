use crate::{
    pollers, protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse, ServerGatewayApis,
};
use std::{fmt::Debug, future::Future, sync::Arc};
use tokio::sync::{
    mpsc::{channel, Receiver},
    Mutex, Notify,
};

pub struct LongPollBuffer<T> {
    buffered_polls: Mutex<Receiver<pollers::Result<T>>>,
    shutdown: Arc<Notify>,
}

impl<T> LongPollBuffer<T>
where
    T: Send + Debug + 'static,
{
    pub fn new<FT>(
        poll_fn: impl Fn() -> FT + Send + Sync + 'static,
        concurrent_pollers: usize,
        buffer_size: usize,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
    {
        let (tx, rx) = channel(buffer_size);
        let shutdown = Arc::new(Notify::new());
        let pf = Arc::new(poll_fn);
        for _ in 0..concurrent_pollers {
            let tx = tx.clone();
            let pf = pf.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let pf = pf();
                    let r = tokio::select! {
                        r = pf => r,
                        _ = shutdown.notified() => break,
                    };
                    tx.send(r).await.expect("Long poll buffer is not dropped");
                }
            });
        }
        Self {
            buffered_polls: Mutex::new(rx),
            shutdown,
        }
    }

    pub async fn poll(&self) -> pollers::Result<T> {
        let mut locked = self.buffered_polls.lock().await;
        (*locked)
            .recv()
            .await
            .expect("There is always another item in the stream")
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

pub type PollWorkflowTaskBuffer = LongPollBuffer<PollWorkflowTaskQueueResponse>;
pub fn new_workflow_task_buffer(
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
    concurrent_pollers: usize,
    buffer_size: usize,
) -> PollWorkflowTaskBuffer {
    LongPollBuffer::new(
        move || {
            let sg = sg.clone();
            async move { sg.poll_workflow_task().await }
        },
        concurrent_pollers,
        buffer_size,
    )
}

pub type PollActivityTaskBuffer = LongPollBuffer<PollActivityTaskQueueResponse>;
pub fn new_activity_task_buffer(
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
    concurrent_pollers: usize,
    buffer_size: usize,
) -> PollActivityTaskBuffer {
    LongPollBuffer::new(
        move || {
            let sg = sg.clone();
            async move { sg.poll_activity_task().await }
        },
        concurrent_pollers,
        buffer_size,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pollers::manual_mock::MockManualGateway;
    use futures::FutureExt;
    use std::time::Duration;
    use tokio::{select, sync::mpsc::channel};

    #[tokio::test]
    async fn only_polls_once_with_1_poller() {
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

        let pb = new_workflow_task_buffer(mock_gateway, 1, 1);

        // Poll a bunch of times, "interrupting" it each time, we should only actually have polled
        // once since the poll takes a while
        let (interrupter_tx, mut interrupter_rx) = channel(50);
        for _ in 0..10 {
            interrupter_tx.send(()).await.unwrap();
        }

        // We should never get anything out since we interrupted 100% of polls
        let mut last_val = false;
        for _ in 0..10 {
            select! {
                _ = interrupter_rx.recv() => {
                    last_val = true;
                }
                _ = pb.poll() => {
                }
            }
        }
        assert!(last_val);
        // Now we grab the buffered poll response, the poll task will go again but we don't grab it,
        // therefore we will have only polled twice.
        pb.poll().await.unwrap();
        pb.shutdown();
    }
}
