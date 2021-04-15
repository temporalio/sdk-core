use crate::errors::ActivityHeartbeatError;
use crate::protos::coresdk::{common, ActivityHeartbeat};
use crate::ServerGatewayApis;
use crossbeam::channel::{bounded, unbounded, Receiver, Select, Sender, TryRecvError};
use dashmap::{DashMap, DashSet};
use futures::FutureExt;
use std::convert::TryInto;
use std::future::Future;
use std::ops::Div;
use std::sync::Arc;
use std::{thread, time};
use tokio::time::{sleep, Sleep};

pub(crate) struct ActivityHeartbeatManager {
    /// Core will aggregate activity heartbeats and send them to the server periodically.
    /// This map contains latest heartbeat details for each activity that reported them.
    heartbeat_details: DashMap<Vec<u8>, Vec<common::Payload>>,

    scheduled_heartbeats: DashSet<Vec<u8>>,

    heartbeat_tx: Sender<ActivityHeartbeatTask>,

    shutdown_tx: Sender<bool>,
}

struct ActivityHeartbeatTask {
    task_token: Vec<u8>,
    next_delay: time::Duration,
    trigger: Sleep,
}

impl ActivityHeartbeatManager {
    pub fn new() -> Self {
        let (heartbeat_tx, heartbeat_rx) = unbounded::<ActivityHeartbeatTask>();
        let (shutdown_tx, shutdown_rx) = bounded(1);
        thread::spawn(move || Self::processor(heartbeat_rx, shutdown_rx));
        Self {
            heartbeat_details: Default::default(),
            scheduled_heartbeats: Default::default(),
            heartbeat_tx,
            shutdown_tx,
        }
    }

    pub fn record(&self, heartbeat: ActivityHeartbeat) -> Result<(), ActivityHeartbeatError> {
        self.heartbeat_details
            .insert(heartbeat.task_token.clone(), heartbeat.details);

        if self
            .scheduled_heartbeats
            .insert(heartbeat.task_token.clone())
        {
            let initial_delay = time::Duration::from_millis(0);
            let heartbeat_timeout: time::Duration = heartbeat
                .heartbeat_timeout
                .ok_or(ActivityHeartbeatError::HeartbeatTimeoutNotSet)?
                .try_into()
                .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;
            let next_delay = heartbeat_timeout.div(2);

            self.heartbeat_tx
                .send(ActivityHeartbeatTask {
                    task_token: heartbeat.task_token,
                    next_delay,
                    trigger: sleep(initial_delay),
                })
                .expect("failed to send activity task into the channel");
        };

        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown_tx.send(true);
    }

    fn processor(heartbeat_rx: Receiver<ActivityHeartbeatTask>, shutdown_rx: Receiver<bool>) {
        // TODO this function should:
        // 1. consume heartbeat tasks
        // 2. join the futures and for any future that becomes ready:
        // - send heartbeat details to the server (if any)
        // - clear details from the local cache
        // - schedule next task
        //in case if heartbeat has been sent
        unimplemented!()
    }
}
