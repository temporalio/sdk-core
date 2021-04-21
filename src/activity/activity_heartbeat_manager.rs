use crate::errors::ActivityHeartbeatError;
use crate::protos::coresdk::PayloadsExt;
use crate::protos::coresdk::{common, ActivityHeartbeat};
use crate::ServerGatewayApis;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Div;
use std::sync::Arc;
use std::time;
use std::time::Duration;
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

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown request.
/// Join handle is used in the `shutdown` to await until all inflight requests are sent.
pub(crate) struct ActivityHeartbeatManagerHandle {
    events: UnboundedSender<LifecycleEvent>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Used to supply heartbeat details to the heartbeat processor, which periodically sends them to the server.
struct ActivityHeartbeatProcessorHandle {
    heartbeat_tx: Sender<Vec<common::Payload>>,
    join_handle: JoinHandle<()>,
}

struct ActivityHeartbeatProcessor<SG> {
    task_token: Vec<u8>,
    delay: time::Duration,
    grace_period: time::Duration,
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

/// Handle that is used by the core for all interactions with the manager, allows sending new heartbeats
/// or requesting and awaiting for the shutdown.
/// When shutdown is requested, signal gets sent to all processors, which allows them to
/// complete gracefully.
impl ActivityHeartbeatManagerHandle {
    /// Records a new heartbeat, note that first call would result in an immediate call to the server,
    /// while rapid successive calls would accumulate for up to 1/2 of the heartbeat timeout and then
    /// latest heartbeat details will be sent to the server. If there is no activity for 1/2 of the
    /// heartbeat timeout then heartbeat processor will be reset and process would start over again,
    /// meaning that next heartbeat will be sent immediately.
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

    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all heartbeat
    /// processors to terminate gracefully.
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
    #![allow(clippy::new_ret_no_self)]
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
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

    /// Main loop, that handles all heartbeat requests and dispatches them to processors.
    async fn lifecycle(mut self) {
        while let Some(event) = self.heartbeat_rx.recv().await {
            match event {
                LifecycleEvent::Heartbeat(heartbeat) => self.record(heartbeat),
                LifecycleEvent::Shutdown => break,
            }
        }
        self.shutdown().await.expect("shutdown should exit cleanly")
    }

    /// Records heartbeat, by sending it to the processor.
    /// New processor is created if one doesn't exist, otherwise new event is dispatched to the
    /// existing processor's receiver channel.
    fn record(&mut self, heartbeat: ValidActivityHeartbeat) {
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
                    grace_period: Duration::from_millis(100),
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

    /// Initiates termination of all hearbeat processors by sending a signal and awaits termination.
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
            // Either shutdown signal was received or delay has passed, this means that processor should complete.
            if stop {
                // New heartbeat requests might have been sent while processor was asleep, followed by a termination,
                // since select doesn't guarantee the order, we could have processed shutdown signal without
                // sending last heartbeat. We'll send that heartbeat so we don't lose the data.
                select! {
                    _ = sleep(self.grace_period) => {}
                    _ = self.heartbeat_rx.changed() => {
                        // Received new heartbeat details.
                        let details = self.heartbeat_rx.borrow().clone();
                        let _ = self.server_gateway
                            .record_activity_heartbeat(self.task_token.clone(), details.into_payloads()).await;
                    }
                }
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

    /// Ensure that hearbeats that are sent with a small delay are aggregated and sent roughly once
    /// every 1/2 of the heartbeat timeout.
    #[tokio::test]
    async fn process_heartbeats_and_shutdown() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        // Sending heartbeat requests for 400ms, this should send first hearbeat right away, and all other
        // requests should be aggregated and last one should be sent to the server in 500ms (1/2 of heartbeat timeout).
        for i in 0u8..40 {
            sleep(Duration::from_millis(10)).await;
            record_heartbeat(&hm, fake_task_token.clone(), i);
        }
        hm.shutdown().await;
    }

    /// Ensure that heartbeat can be called from a tight loop without any delays, resulting in two
    /// interactions with the server - one immediately and one after 500ms after the delay.
    #[tokio::test]
    async fn process_tight_loop_and_shutdown() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        // Sending heartbeat requests for 400ms, this should send first hearbeat right away, and all other
        // requests should be aggregated and last one should be sent to the server in 500ms (1/2 of heartbeat timeout).
        for i in 0u8..u8::MAX {
            record_heartbeat(&hm, fake_task_token.clone(), i);
        }
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn record_after_shutdown() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(0);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        hm.shutdown().await;
        match hm.record(ActivityHeartbeat {
            task_token: vec![1, 2, 3],
            details: vec![Payload {
                // payload doesn't matter in this case, as it shouldn't get sent anyways.
                ..Default::default()
            }],
            heartbeat_timeout: Some(Duration::from_millis(1000).into()),
        }) {
            Ok(_) => {
                unreachable!("heartbeat should not be recorded after the shutdown")
            }
            Err(e) => {
                matches!(e, ActivityHeartbeatError::ShuttingDown);
            }
        }
    }

    /// Ensure that error is returned if heartbeat timeout is not set. Heartbeat timeout is required
    /// because it's used to derive aggregation delay.
    #[tokio::test]
    async fn record_without_heartbeat_timeout() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(0);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        match hm.record(ActivityHeartbeat {
            task_token: vec![1, 2, 3],
            details: vec![Payload {
                // payload doesn't matter in this case, as it shouldn't get sent anyways.
                ..Default::default()
            }],
            heartbeat_timeout: None,
        }) {
            Ok(_) => {
                unreachable!("heartbeat should not be recorded without timeout")
            }
            Err(e) => {
                matches!(e, ActivityHeartbeatError::HeartbeatTimeoutNotSet);
            }
        }
        hm.shutdown().await;
    }

    fn record_heartbeat(hm: &ActivityHeartbeatManagerHandle, task_token: Vec<u8>, i: u8) {
        hm.record(ActivityHeartbeat {
            task_token,
            details: vec![Payload {
                metadata: Default::default(),
                data: vec![i],
            }],
            heartbeat_timeout: Some(Duration::from_millis(1000).into()),
        })
        .expect("hearbeat recording should not fail");
    }
}
