use crate::errors::ActivityHeartbeatError;
use crate::protos::coresdk::{common, ActivityHeartbeat};
use crate::ServerGatewayApis;
use dashmap::mapref::one::Ref;
use dashmap::{DashMap, DashSet};
use futures::FutureExt;
use std::borrow::BorrowMut;
use std::collections::hash_map::RandomState;
use std::convert::TryInto;
use std::future::Future;
use std::ops::Div;
use std::sync::Arc;
use std::{thread, time};
use tokio::select;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Sleep};

pub(crate) struct ActivityHeartbeatManager {
    /// Core will aggregate activity heartbeats and send them to the server periodically.
    /// This map contains sender channel for each activity that has an active heartbeat processor.
    heartbeat_processors: DashMap<Vec<u8>, ActivityHeartbeatProcessorHandle>,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
}

struct ActivityHeartbeatProcessorHandle {
    heartbeat_tx: Sender<Vec<common::Payload>>,
    join_handle: JoinHandle<()>,
}

struct ActivityHeartbeatProcessor {
    delay: time::Duration,
    heartbeat_rx: Receiver<Vec<common::Payload>>,
    shutdown_rx: Receiver<bool>,
}

impl ActivityHeartbeatManager {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = channel(false);
        Self {
            heartbeat_processors: Default::default(),
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub fn record(&self, heartbeat: ActivityHeartbeat) -> Result<(), ActivityHeartbeatError> {
        /// Do not accept new heartbeat requests if core is shutting down, this is because we
        /// don't want to spawn new processors and existing processors won't be accepting new work.
        if *self.shutdown_rx.borrow() {
            return Err(ActivityHeartbeatError::ShuttingDown);
        }
        match self.heartbeat_processors.get(&heartbeat.task_token) {
            Some(handle) => {
                handle.heartbeat_tx.send(heartbeat.details);
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
                    delay,
                    heartbeat_rx,
                    shutdown_rx: self.shutdown_rx.clone(),
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

    pub fn shutdown(&self) {
        self.shutdown_tx.send(true);
        // TODO join on join handles
    }
}

impl ActivityHeartbeatProcessor {
    async fn run(mut self) {
        // TODO borrow and send details available in the heartbeat_rx once.
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
                    // TODO send details right away since enough time has passed since previous call
                    false
                }
            };
            if stop {
                break;
            }
        }
        // TODO cleanup handle from the map
    }
}
