use crate::errors::ActivityHeartbeatError;
use crate::protos::coresdk::PayloadsExt;
use crate::protos::coresdk::{common, ActivityHeartbeat};
use crate::ServerGatewayApis;
use dashmap::DashMap;
use futures::future::join_all;
use std::convert::TryInto;
use std::ops::Div;
use std::sync::Arc;
use std::time;
use tokio::select;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub(crate) struct ActivityHeartbeatManager<SG> {
    /// Core will aggregate activity heartbeats and send them to the server periodically.
    /// This map contains sender channel for each activity that has an active heartbeat processor.
    heartbeat_processors: Arc<DashMap<Vec<u8>, ActivityHeartbeatProcessorHandle>>,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    server_gateway: Arc<SG>,
}

struct ActivityHeartbeatProcessorHandle {
    heartbeat_tx: Sender<Vec<common::Payload>>,
    join_handle: JoinHandle<()>,
}

struct ActivityHeartbeatProcessor<SG> {
    heartbeat_processors: Arc<DashMap<Vec<u8>, ActivityHeartbeatProcessorHandle>>,
    task_token: Vec<u8>,
    delay: time::Duration,
    heartbeat_rx: Receiver<Vec<common::Payload>>,
    shutdown_rx: Receiver<bool>,
    server_gateway: Arc<SG>,
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> ActivityHeartbeatManager<SG> {
    pub fn new(sg: Arc<SG>) -> Self {
        let (shutdown_tx, shutdown_rx) = channel(false);
        Self {
            heartbeat_processors: Arc::new(Default::default()),
            shutdown_tx,
            shutdown_rx,
            server_gateway: sg,
        }
    }

    pub fn record(&self, heartbeat: ActivityHeartbeat) -> Result<(), ActivityHeartbeatError> {
        match self.heartbeat_processors.get(&heartbeat.task_token) {
            Some(handle) => {
                handle
                    .heartbeat_tx
                    .send(heartbeat.details)
                    .expect("heartbeat channel can't be dropped if we are inside this method");
            }
            None => {
                let heartbeat_timeout: time::Duration = heartbeat
                    .heartbeat_timeout
                    .ok_or(ActivityHeartbeatError::HeartbeatTimeoutNotSet)?
                    .try_into()
                    .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;
                let delay = heartbeat_timeout.div(2);
                let (heartbeat_tx, heartbeat_rx) = channel(heartbeat.details);
                let processor = ActivityHeartbeatProcessor {
                    heartbeat_processors: self.heartbeat_processors.clone(),
                    task_token: heartbeat.task_token.clone(),
                    delay,
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
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown_tx
            .send(true)
            .expect("shutdown channel can't be dropped before shutdown is complete");
        let mut pending_handles = vec![];
        for v in self.heartbeat_processors.iter() {
            self.heartbeat_processors.remove(v.key()).map(|v| {
                pending_handles.push(v.1.join_handle);
            });
        }
        join_all(pending_handles)
            .await
            .into_iter()
            .for_each(|r| r.expect("Doesn't fail"));
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> ActivityHeartbeatProcessor<SG> {
    async fn run(mut self) {
        // Each processor is initialized with heartbeat payloads, first thing we need to do is send it out.
        let details = self.heartbeat_rx.borrow().clone();
        let _ = self
            .server_gateway
            .record_activity_heartbeat(self.task_token.clone(), details.into_payloads());
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
                    let details = self.heartbeat_rx.borrow();
                    // TODO see if we can get rid of cloning details.
                    let _ = self.server_gateway
                        .record_activity_heartbeat(self.task_token.clone(), details.clone().into_payloads());
                    false
                }
            };
            if stop {
                break;
            }
        }
        // Doing cleanup at the end of the processor loop ensures that we are not leaking memory.
        // There is a small race condition possible where new heartbeat may come in before this line is called,
        // in this case old processor will be used once again and new one will be created for the next heartbeat,
        // resulting in no delay between two heartbeats, and no lost data. This is fine because such race
        // condition should be extremely rare.
        self.heartbeat_processors.remove(&self.task_token);
    }
}
