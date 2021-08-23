use crate::{
    errors::ActivityHeartbeatError,
    pollers::ServerGatewayApis,
    protos::{
        coresdk::{
            activity_task::ActivityCancelReason, common, ActivityHeartbeat, IntoPayloadsExt,
        },
        temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    },
    task_token::TaskToken,
    worker::activities::PendingActivityCancel,
};
use futures::StreamExt;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::{self, Duration, Instant},
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch, Mutex,
    },
    task::JoinHandle,
};

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown
/// request.
pub(crate) struct ActivityHeartbeatManager {
    heartbeat_tx: UnboundedSender<HBAction>,
    /// Cancellations that have been received when heartbeating are queued here and can be consumed
    /// by [fetch_cancellations]
    incoming_cancels: Mutex<UnboundedReceiver<PendingActivityCancel>>,
    shutting_down: watch::Sender<bool>,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
enum HBAction {
    HB(ValidActivityHeartbeat),
    Evict(TaskToken),
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
impl ActivityHeartbeatManager {
    /// Records a new heartbeat, the first call will result in an immediate call to the server,
    /// while rapid successive calls would accumulate for up to `delay` and then latest heartbeat
    /// details will be sent to the server.
    ///
    /// It is important that this function is never called with a task token equal to one given
    /// to [Self::evict] after it has been called, doing so will cause a memory leak, as there is
    /// no longer an efficient way to forget about that task token.
    pub(super) fn record(
        &self,
        details: ActivityHeartbeat,
        delay: Duration,
    ) -> Result<(), ActivityHeartbeatError> {
        if *self.shutting_down.borrow() {
            return Err(ActivityHeartbeatError::ShuttingDown);
        }

        self.heartbeat_tx
            .send(HBAction::HB(ValidActivityHeartbeat {
                task_token: TaskToken(details.task_token),
                details: details.details,
                delay,
            }))
            .expect("Receive half of the heartbeats event channel must not be dropped");

        Ok(())
    }

    /// Tell the heartbeat manager we are done forever with a certain task, so it may be forgotten.
    /// Record should *not* be called with the same TaskToken after calling this.
    pub(super) fn evict(&self, task_token: TaskToken) {
        let _ = self.heartbeat_tx.send(HBAction::Evict(task_token));
    }

    /// Returns a future that resolves any time there is a new activity cancel that must be
    /// dispatched to lang
    pub(super) async fn next_pending_cancel(&self) -> Option<PendingActivityCancel> {
        self.incoming_cancels.lock().await.recv().await
    }

    pub(super) fn notify_shutdown(&self) {
        let _ = self.shutting_down.send(true);
    }

    // TODO: Can own self now!
    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all heartbeat
    /// processors to terminate gracefully.
    pub(super) async fn shutdown(&self) {
        self.notify_shutdown();
        let mut handle = self.join_handle.lock().await;
        if let Some(h) = handle.take() {
            h.await.expect("shutdown should exit cleanly");
        }
    }
}

#[derive(Debug)]
struct HbStreamState {
    last_sent: HashMap<TaskToken, ActivityHbState>,
    incoming_hbs: UnboundedReceiver<HBAction>,
}

impl HbStreamState {
    fn new(incoming_hbs: UnboundedReceiver<HBAction>) -> Self {
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

impl ActivityHeartbeatManager {
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
    pub fn new(sg: Arc<impl ServerGatewayApis + Send + Sync + 'static + ?Sized>) -> Self {
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();
        let (cancels_tx, cancels_rx) = unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let join_handle = tokio::spawn(
            // The stream of incoming heartbeats uses unfold to carry state across each item in the
            // stream The closure checks if, for any given activity, we should heartbeat or not
            // depending on its delay and when we last issued a heartbeat for it.
            futures::stream::unfold(HbStreamState::new(heartbeat_rx), move |mut hb_states| {
                let mut shutdown = shutdown_rx.clone();
                async move {
                    let hb = loop {
                        tokio::select! {
                            biased;

                            _ = shutdown.changed() => return None,

                            hb = hb_states.incoming_hbs.recv() => {
                                if let Some(hb) = hb {
                                    match hb {
                                        HBAction::HB(hb) => break hb,
                                        HBAction::Evict(tt) => {
                                            hb_states.last_sent.remove(&tt);
                                            continue
                                        }
                                    }
                                } else {
                                    return None;
                                }
                            }
                        }
                    };

                    let do_record = match hb_states.last_sent.entry(hb.task_token.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(ActivityHbState {
                                delay: hb.delay,
                                last_sent: Instant::now(),
                            });
                            true
                        }
                        Entry::Occupied(mut o) => {
                            let o = o.get_mut();
                            let now = Instant::now();
                            let elapsed = o.last_sent.elapsed();
                            let do_rec = elapsed >= o.delay;
                            if do_rec {
                                o.last_sent = now;
                                o.delay = hb.delay;
                            }
                            do_rec
                        }
                    };

                    if *shutdown.borrow() {
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
                                    .send(PendingActivityCancel::new(
                                        hb.task_token.clone(),
                                        ActivityCancelReason::Cancelled,
                                    ))
                                    .expect("Receive half of heartbeat cancels not blocked");
                            }
                        }
                        // Send cancels for any activity that learns its workflow already finished
                        // (which is one thing not found implies - other reasons would seem equally
                        // valid).
                        Err(s) if s.code() == tonic::Code::NotFound => {
                            cancels_tx
                                .send(PendingActivityCancel::new(
                                    hb.task_token.clone(),
                                    ActivityCancelReason::NotFound,
                                ))
                                .expect("Receive half of heartbeat cancels not blocked");
                        }
                        Err(e) => {
                            warn!("Error when recording heartbeat: {:?}", e)
                        }
                    }
                }
            }),
        );

        Self {
            heartbeat_tx,
            incoming_cancels: Mutex::new(cancels_rx),
            shutting_down: shutdown_tx,
            join_handle: Mutex::new(Some(join_handle)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_help::TEST_Q;
    use crate::{
        pollers::MockServerGatewayApis,
        protos::{
            coresdk::common::Payload,
            temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
        },
    };
    use std::time::Duration;
    use tokio::time::sleep;

    /// Ensure that heartbeats that are sent with a small delay are aggregated and sent roughly once
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
        // Sending heartbeat requests for 600ms, this should send first heartbeat right away, and
        // all other requests should be aggregated and last one should be sent to the server in
        // 500ms (1/2 of heartbeat timeout).
        for i in 0u8..60 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(1000));
            sleep(Duration::from_millis(10)).await;
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
            .times(1);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        // Send a whole bunch of heartbeats very fast. We should still only send one total.
        for i in 0u8..u8::MAX {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(1000));
        }
        // Let it propagate
        sleep(Duration::from_millis(50)).await;
        hm.shutdown().await;
    }

    /// This test reports one heartbeat and waits for the delay to elapse before sending another
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
        record_heartbeat(&hm, fake_task_token, 1, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(50)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn evict_works() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        hm.evict(fake_task_token.clone().into());
        record_heartbeat(&hm, fake_task_token, 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(100)).await;
        // We know it works b/c otherwise we would have only called record 1 time w/o sleep
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
                task_queue: TEST_Q.to_string(),
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
        hm: &ActivityHeartbeatManager,
        task_token: Vec<u8>,
        payload_data: u8,
        delay: Duration,
    ) {
        hm.record(
            ActivityHeartbeat {
                task_token,
                task_queue: TEST_Q.to_string(),
                details: vec![Payload {
                    metadata: Default::default(),
                    data: vec![payload_data],
                }],
            },
            // Mimic the same delay we would apply in activity task manager
            delay / 2,
        )
        .expect("hearbeat recording should not fail");
    }
}
