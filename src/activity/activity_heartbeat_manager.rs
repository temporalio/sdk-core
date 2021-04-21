use crate::errors::ActivityHeartbeatError;
use crate::protos::coresdk::PayloadsExt;
use crate::protos::coresdk::{common, ActivityHeartbeat};
use crate::ServerGatewayApis;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Div;
use std::sync::Arc;
use std::time;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;

pub(crate) struct ActivityHeartbeatManager<SG> {
    /// Core will aggregate activity heartbeats and send them to the server periodically.
    /// This map contains sender channel for each activity that has an active heartbeat processor.
    heartbeat_processors: HashMap<Vec<u8>, ActivityHeartbeatProcessorHandle>,
    heartbeat_rx: UnboundedReceiver<LifecycleEvent>,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    server_gateway: Arc<SG>,
}

pub(crate) struct ActivityHeartbeatManagerHandle {
    events: UnboundedSender<LifecycleEvent>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

struct ActivityHeartbeatProcessorHandle {
    heartbeat_tx: Sender<Vec<common::Payload>>,
    join_handle: JoinHandle<()>,
}

struct ActivityHeartbeatProcessor<SG> {
    task_token: Vec<u8>,
    delay: time::Duration,
    heartbeat_rx: Receiver<Vec<common::Payload>>,
    shutdown_rx: Receiver<bool>,
    server_gateway: Arc<SG>,
}

#[derive(Debug)]
pub enum LifecycleEvent {
    Heartbeat(ValidActivityHeartbeat),
    Shutdown,
}

#[derive(Debug)]
pub struct ValidActivityHeartbeat {
    pub task_token: Vec<u8>,
    pub details: Vec<common::Payload>,
    pub delay: time::Duration,
}

impl ActivityHeartbeatManagerHandle {
    pub fn record(&self, details: ActivityHeartbeat) -> Result<(), ActivityHeartbeatError> {
        let heartbeat_timeout: time::Duration = details
            .heartbeat_timeout
            .ok_or(ActivityHeartbeatError::HeartbeatTimeoutNotSet)?
            .try_into()
            .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;

        self.events
            .send(LifecycleEvent::Heartbeat(ValidActivityHeartbeat {
                task_token: details.task_token,
                details: details.details,
                delay: heartbeat_timeout.div(2),
            }))
            .map_err(|_| ActivityHeartbeatError::SendError)?;

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.events
            .send(LifecycleEvent::Shutdown)
            .expect("should be able to send shutdown event");
        let mut handle = self.join_handle.lock().await;
        if let Some(h) = handle.take() {
            h.await.expect("shutdown should exit cleanly");
        }
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> ActivityHeartbeatManager<SG> {
    pub fn new(sg: Arc<SG>) -> ActivityHeartbeatManagerHandle {
        let (shutdown_tx, shutdown_rx) = channel(false);
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();
        let s = Self {
            heartbeat_processors: Default::default(),
            heartbeat_rx,
            shutdown_tx,
            shutdown_rx,
            server_gateway: sg,
        };
        let join_handle = tokio::spawn(s.lifecycle());
        ActivityHeartbeatManagerHandle {
            events: heartbeat_tx,
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    async fn lifecycle(mut self) {
        while let Some(event) = self.heartbeat_rx.recv().await {
            match event {
                LifecycleEvent::Heartbeat(heartbeat) => self.record(heartbeat),
                LifecycleEvent::Shutdown => break,
            }
        }
        self.shutdown().await.expect("shutdown should exit cleanly")
    }

    pub fn record(&mut self, heartbeat: ValidActivityHeartbeat) {
        match self.heartbeat_processors.get(&heartbeat.task_token) {
            Some(handle) => {
                handle
                    .heartbeat_tx
                    .send(heartbeat.details)
                    .expect("heartbeat channel can't be dropped if we are inside this method");
            }
            None => {
                let (heartbeat_tx, heartbeat_rx) = channel(heartbeat.details);
                let processor = ActivityHeartbeatProcessor {
                    task_token: heartbeat.task_token.clone(),
                    delay: heartbeat.delay,
                    heartbeat_rx,
                    shutdown_rx: self.shutdown_rx.clone(),
                    server_gateway: self.server_gateway.clone(),
                };
                let join_handle = tokio::spawn(processor.run());
                let handle = ActivityHeartbeatProcessorHandle {
                    heartbeat_tx,
                    join_handle,
                };
                self.heartbeat_processors
                    .insert(heartbeat.task_token, handle);
            }
        }
    }

    pub async fn shutdown(mut self) -> Result<(), JoinError> {
        self.shutdown_tx
            .send(true)
            .expect("shutdown channel can't be dropped before shutdown is complete");
        for v in self.heartbeat_processors.drain() {
            v.1.join_handle.await?;
        }
        Ok(())
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> ActivityHeartbeatProcessor<SG> {
    async fn run(mut self) {
        // Each processor is initialized with heartbeat payloads, first thing we need to do is send it out.
        let details = self.heartbeat_rx.borrow().clone();
        let _ = self
            .server_gateway
            .record_activity_heartbeat(self.task_token.clone(), details.into_payloads())
            .await;
        loop {
            sleep(self.delay).await;
            let stop = select! {
                _ = self.shutdown_rx.changed() => {
                    // Shutting down core, need to break the loop. Previous details has been sent,
                    // so there is nothing else to do.
                    true
                }
                _ = sleep(self.delay) => {
                    // Timed out while waiting for the next heartbeat.
                    // We waited 2 * delay in total, where delay is 1/2 of the activity heartbeat timeout.
                    // This means that activity has either timed out or completed by now.
                    true
                }
                _ = self.heartbeat_rx.changed() => {
                    // Received new heartbeat details.
                    let details = self.heartbeat_rx.borrow().clone();
                    let _ = self.server_gateway
                        .record_activity_heartbeat(self.task_token.clone(), details.into_payloads()).await;
                    false
                }
            };
            if stop {
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pollers::MockServerGatewayApis;
    use crate::protos::coresdk::common::Payload;
    use crate::protos::temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse;
    use std::time::Duration;

    #[tokio::test]
    async fn process_heartbeats_and_shutdown() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        for i in 0..30 {
            sleep(Duration::from_millis(10)).await;
            hm.record(ActivityHeartbeat {
                task_token: fake_task_token.clone(),
                details: vec![Payload {
                    metadata: Default::default(),
                    data: vec![i],
                }],
                heartbeat_timeout: Some(Duration::from_millis(1000).into()),
            })
            .expect("hearbeat recording should not fail");
        }
        sleep(Duration::from_millis(500)).await;
        hm.shutdown().await;
    }
}
