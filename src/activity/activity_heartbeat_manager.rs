use crate::task_token::TaskToken;
use crate::{
    errors::ActivityHeartbeatError,
    protos::{
        coresdk::{common, ActivityHeartbeat, PayloadsExt},
        temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    },
    ServerGatewayApis,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{self, Duration},
};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch::{channel, Receiver, Sender},
        Mutex,
    },
    task::{JoinError, JoinHandle},
    time::sleep,
};

pub(crate) struct ActivityHeartbeatManager<SG> {
    /// Core will aggregate activity heartbeats for each activity and send them to the server
    /// periodically. This map contains sender channel for each activity, identified by the task
    /// token, that has an active heartbeat processor.
    heartbeat_processors: HashMap<TaskToken, ActivityHeartbeatProcessorHandle>,
    events_tx: UnboundedSender<LifecycleEvent>,
    events_rx: UnboundedReceiver<LifecycleEvent>,
    shutdown_tx: Sender<bool>,
    shutdown_rx: Receiver<bool>,
    cancels_tx: UnboundedSender<TaskToken>,
    server_gateway: Arc<SG>,
}

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown
/// request.
pub(crate) struct ActivityHeartbeatManagerHandle {
    shutting_down: AtomicBool,
    events: UnboundedSender<LifecycleEvent>,
    /// Cancellations that have been received when heartbeating are queued here and can be consumed
    /// by [fetch_cancellations]
    incoming_cancels: Mutex<UnboundedReceiver<TaskToken>>,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Used to supply heartbeat details to the heartbeat processor, which periodically sends them to
/// the server.
struct ActivityHeartbeatProcessorHandle {
    heartbeat_tx: Sender<Vec<common::Payload>>,
    join_handle: JoinHandle<()>,
}

/// Heartbeat processor, that aggregates and periodically sends heartbeat requests for a single
/// activity to the server.
struct ActivityHeartbeatProcessor<SG> {
    task_token: TaskToken,
    delay: time::Duration,
    /// Used to receive heartbeat events.
    heartbeat_rx: Receiver<Vec<common::Payload>>,
    /// Used to receive shutdown notifications.
    shutdown_rx: Receiver<bool>,
    /// Used to send CleanupProcessor event at the end of the processor loop.
    events_tx: UnboundedSender<LifecycleEvent>,
    /// Used to send cancellation notices that we learned about when heartbeating back up to core
    cancels_tx: UnboundedSender<TaskToken>,
    server_gateway: Arc<SG>,
}

#[derive(Debug)]
pub enum LifecycleEvent {
    Heartbeat(ValidActivityHeartbeat),
    CleanupProcessor(TaskToken),
    Shutdown,
}

#[derive(Debug)]
pub struct ValidActivityHeartbeat {
    pub task_token: TaskToken,
    pub details: Vec<common::Payload>,
    pub delay: time::Duration,
}

/// Handle that is used by the core for all interactions with the manager, allows sending new
/// heartbeats or requesting and awaiting for the shutdown. When shutdown is requested, signal gets
/// sent to all processors, which allows them to complete gracefully.
impl ActivityHeartbeatManagerHandle {
    /// Records a new heartbeat, note that first call would result in an immediate call to the
    /// server, while rapid successive calls would accumulate for up to `delay`
    /// and then latest heartbeat details will be sent to the server. If there is no activity for
    /// `delay` then heartbeat processor will be reset and process would start
    /// over again, meaning that next heartbeat will be sent immediately, creating a new processor.
    pub fn record(
        &self,
        details: ActivityHeartbeat,
        delay: Duration,
    ) -> Result<(), ActivityHeartbeatError> {
        if self.shutting_down.load(Ordering::Relaxed) {
            return Err(ActivityHeartbeatError::ShuttingDown);
        }

        self.events
            .send(LifecycleEvent::Heartbeat(ValidActivityHeartbeat {
                task_token: TaskToken(details.task_token),
                details: details.details,
                delay,
            }))
            .expect("Receive half of the heartbeats event channel must not be dropped");

        Ok(())
    }

    /// Returns a future that resolves any time there is a new activity cancel that must be
    /// dispatched to lang
    pub async fn next_pending_cancel(&self) -> Option<TaskToken> {
        self.incoming_cancels.lock().await.recv().await
    }

    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all heartbeat
    /// processors to terminate gracefully.
    pub async fn shutdown(&self) {
        // If shutdown was called multiple times, shutdown signal has been sent already and consumer
        // might have been dropped already, meaning that sending to the channel may fail.
        // All we need to do is to simply await on handle for the completion.
        if !self.shutting_down.load(Ordering::Relaxed) {
            self.events
                .send(LifecycleEvent::Shutdown)
                .expect("should be able to send shutdown event");
            self.shutting_down.store(true, Ordering::Relaxed);
        }
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
        let (events_tx, events_rx) = unbounded_channel();
        let (cancels_tx, cancels_rx) = unbounded_channel();
        let s = Self {
            heartbeat_processors: Default::default(),
            events_tx: events_tx.clone(),
            events_rx,
            shutdown_tx,
            shutdown_rx,
            cancels_tx,
            server_gateway: sg,
        };
        let join_handle = tokio::spawn(s.lifecycle());
        ActivityHeartbeatManagerHandle {
            shutting_down: AtomicBool::new(false),
            events: events_tx,
            incoming_cancels: Mutex::new(cancels_rx),
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    /// Main loop, that handles all heartbeat requests and dispatches them to processors.
    async fn lifecycle(mut self) {
        while let Some(event) = self.events_rx.recv().await {
            match event {
                LifecycleEvent::Heartbeat(heartbeat) => self.record(heartbeat),
                LifecycleEvent::Shutdown => break,
                LifecycleEvent::CleanupProcessor(task_token) => {
                    self.heartbeat_processors.remove(&task_token);
                }
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
                    heartbeat_rx,
                    shutdown_rx: self.shutdown_rx.clone(),
                    events_tx: self.events_tx.clone(),
                    cancels_tx: self.cancels_tx.clone(),
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

    /// Initiates termination of all heartbeat processors by sending a signal and awaits termination
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
        // Each processor is initialized with heartbeat payloads, first thing we need to do is send
        // it out.
        self.record_heartbeat().await;
        loop {
            sleep(self.delay).await;
            select! {
                biased;

                _ = self.heartbeat_rx.changed() => {
                    self.record_heartbeat().await;
                }
                _ = self.shutdown_rx.changed() => {
                    break;
                }
                _ = sleep(self.delay) => {
                    // Timed out while waiting for the next heartbeat. We waited 2 * delay in total,
                    // where delay is 1/2 of the activity heartbeat timeout. This means that
                    // activity has either timed out or completed by now.
                    break;
                }
            };
        }
        self.events_tx
            .send(LifecycleEvent::CleanupProcessor(self.task_token))
            .expect("cleanup requests should not be dropped");
    }

    async fn record_heartbeat(&mut self) {
        let details = self.heartbeat_rx.borrow().clone();
        match self
            .server_gateway
            .record_activity_heartbeat(self.task_token.clone(), details.into_payloads())
            .await
        {
            Ok(RecordActivityTaskHeartbeatResponse { cancel_requested }) => {
                if cancel_requested {
                    self.cancels_tx
                        .send(self.task_token.clone())
                        .expect("Receive half of heartbeat cancels not blocked");
                }
            }
            // Send cancels for any activity that learns its workflow already finished (which is
            // one thing not found implies - other reasons would seem equally valid).
            Err(s) if s.code() == tonic::Code::NotFound => {
                self.cancels_tx
                    .send(self.task_token.clone())
                    .expect("Receive half of heartbeat cancels not blocked");
            }
            Err(e) => {
                warn!("Error when recording heartbeat: {:?}", e)
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
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(1000));
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
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(1000));
        }
        hm.shutdown().await;
    }

    /// This test reports one heartbeat and waits until processor times out and exits then sends another one.
    /// Expectation is that new processor should be spawned and heartbeat shouldn't get lost.
    #[tokio::test]
    async fn report_heartbeat_after_timeout() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        sleep(Duration::from_millis(500)).await;
        record_heartbeat(&hm, fake_task_token.clone(), 1, Duration::from_millis(100));
        hm.shutdown().await;
    }

    /// Recording new heartbeats after shutdown is not allowed, and will result in error.
    #[tokio::test]
    async fn record_after_shutdown() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(0);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        hm.shutdown().await;
        match hm.record(
            ActivityHeartbeat {
                task_token: vec![1, 2, 3],
                details: vec![Payload {
                    // payload doesn't matter in this case, as it shouldn't get sent anyways.
                    ..Default::default()
                }],
            },
            Duration::from_millis(1000),
        ) {
            Ok(_) => {
                unreachable!("heartbeat should not be recorded after the shutdown")
            }
            Err(e) => {
                matches!(e, ActivityHeartbeatError::ShuttingDown);
            }
        }
    }

    fn record_heartbeat(
        hm: &ActivityHeartbeatManagerHandle,
        task_token: Vec<u8>,
        i: u8,
        delay: Duration,
    ) {
        hm.record(
            ActivityHeartbeat {
                task_token,
                details: vec![Payload {
                    metadata: Default::default(),
                    data: vec![i],
                }],
            },
            delay,
        )
        .expect("hearbeat recording should not fail");
    }
}
