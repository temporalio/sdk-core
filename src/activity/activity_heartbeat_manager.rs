use crate::{
    errors::ActivityHeartbeatError,
    pollers::ServerGatewayApis,
    protos::{
        coresdk::{activity_task::ActivityCancelReason, common, ActivityHeartbeat, PayloadsExt},
        temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    },
    task_token::TaskToken,
};
use futures::StreamExt;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{self, Duration, Instant},
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
};

// TODO: Not needed
pub(crate) struct ActivityHeartbeatManager<SG> {
    _server_gateway: Arc<SG>,
    _shutting_down: Arc<AtomicBool>,
}

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown
/// request.
pub(crate) struct ActivityHeartbeatManagerHandle {
    shutting_down: Arc<AtomicBool>,
    heartbeat_tx: UnboundedSender<ValidActivityHeartbeat>,
    /// Cancellations that have been received when heartbeating are queued here and can be consumed
    /// by [fetch_cancellations]
    incoming_cancels: Mutex<UnboundedReceiver<(TaskToken, ActivityCancelReason)>>,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: Mutex<Option<JoinHandle<()>>>,
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

        self.heartbeat_tx
            .send(ValidActivityHeartbeat {
                task_token: TaskToken(details.task_token),
                details: details.details,
                delay,
            })
            .expect("Receive half of the heartbeats event channel must not be dropped");

        Ok(())
    }

    /// Returns a future that resolves any time there is a new activity cancel that must be
    /// dispatched to lang
    pub async fn next_pending_cancel(&self) -> Option<(TaskToken, ActivityCancelReason)> {
        self.incoming_cancels.lock().await.recv().await
    }

    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all heartbeat
    /// processors to terminate gracefully.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        let mut handle = self.join_handle.lock().await;
        if let Some(h) = handle.take() {
            h.await.expect("shutdown should exit cleanly");
        }
    }
}

#[derive(Debug)]
struct HbStreamState {
    last_sent: HashMap<TaskToken, ActivityHbState>,
    incoming_hbs: UnboundedReceiver<ValidActivityHeartbeat>,
}
impl HbStreamState {
    fn new(incoming_hbs: UnboundedReceiver<ValidActivityHeartbeat>) -> Self {
        Self {
            last_sent: Default::default(),
            incoming_hbs,
        }
    }
}

#[derive(Debug)]
struct ActivityHbState {
    delay: Duration,
    last_sent: Instant,
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> ActivityHeartbeatManager<SG> {
    #![allow(clippy::new_ret_no_self)]
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
    pub fn new(sg: Arc<SG>) -> ActivityHeartbeatManagerHandle {
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();
        let (cancels_tx, cancels_rx) = unbounded_channel();
        let shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown = shutting_down.clone();

        let join_handle = tokio::spawn(
            // The stream of incoming heartbeats uses scan to carry state across each item in the stream
            // The closure checks if, for any given activity, we should heartbeat or not depending on
            // its delay and when we last issued a heartbeat for it.
            futures::stream::unfold(HbStreamState::new(heartbeat_rx), move |mut hb_states| {
                let shutdown = shutdown.clone();
                async move {
                    let hb = if let Some(hb) = hb_states.incoming_hbs.recv().await {
                        hb
                    } else {
                        return None;
                    };

                    let do_record = if !hb_states.last_sent.contains_key(&hb.task_token) {
                        hb_states.last_sent.insert(
                            hb.task_token.clone(),
                            ActivityHbState {
                                delay: hb.delay,
                                last_sent: Instant::now(),
                            },
                        );
                        true
                    } else {
                        false
                    };

                    if shutdown.load(Ordering::Relaxed) {
                        return None;
                    }

                    let maybe_send_hb = if do_record { Some(hb) } else { None };
                    Some((maybe_send_hb, hb_states))
                }
            })
            .for_each_concurrent(None, move |hb| {
                let sg = sg.clone();
                let cancels_tx = cancels_tx.clone();
                async move {
                    let hb = if let Some(hb) = hb {
                        hb
                    } else {
                        return;
                    };

                    match sg
                        .record_activity_heartbeat(
                            hb.task_token.clone(),
                            hb.details.clone().into_payloads(),
                        )
                        .await
                    {
                        Ok(RecordActivityTaskHeartbeatResponse { cancel_requested }) => {
                            if cancel_requested {
                                cancels_tx
                                    .send(hb.task_token.clone())
                                    .expect("Receive half of heartbeat cancels not blocked");
                            }
                        }
                        // Send cancels for any activity that learns its workflow already finished (which is
                        // one thing not found implies - other reasons would seem equally valid).
                        Err(s) if s.code() == tonic::Code::NotFound => {
                            cancels_tx
                                .send(hb.task_token.clone())
                                .expect("Receive half of heartbeat cancels not blocked");
                        }
                        Err(e) => {
                            warn!("Error when recording heartbeat: {:?}", e)
                        }
                    }
                }
            }),
        );

        ActivityHeartbeatManagerHandle {
            shutting_down,
            heartbeat_tx,
            incoming_cancels: Mutex::new(cancels_rx),
            join_handle: Mutex::new(Some(join_handle)),
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
    use tokio::time::sleep;

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
