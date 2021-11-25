use crate::{
    errors::ActivityHeartbeatError, pollers::ServerGatewayApis, task_token::TaskToken,
    worker::activities::PendingActivityCancel,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{self, Duration},
};
use temporal_sdk_core_protos::{
    coresdk::{activity_task::ActivityCancelReason, common, ActivityHeartbeat, IntoPayloadsExt},
    temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
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
    /// Cancellations that have been received when heartbeating are queued here and can be consumed
    /// by [fetch_cancellations]
    incoming_cancels: Mutex<UnboundedReceiver<PendingActivityCancel>>,
    shutting_down: watch::Sender<bool>,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: Mutex<Option<JoinHandle<()>>>,
    heartbeat_tx: UnboundedSender<HBAction>,
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
    pub throttle_interval: time::Duration,
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
        hb: ActivityHeartbeat,
        throttle_interval: Duration,
    ) -> Result<(), ActivityHeartbeatError> {
        if *self.shutting_down.borrow() {
            return Err(ActivityHeartbeatError::ShuttingDown);
        }
        self.heartbeat_tx
            .send(HBAction::HB(ValidActivityHeartbeat {
                task_token: TaskToken(hb.task_token),
                details: hb.details,
                throttle_interval,
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

    // TODO: Can own self now!
    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all heartbeat
    /// processors to terminate gracefully.
    pub(super) async fn shutdown(&self) {
        let _ = self.shutting_down.send(true);
        let mut handle = self.join_handle.lock().await;
        if let Some(h) = handle.take() {
            h.await.expect("shutdown should exit cleanly");
        }
    }
}

#[derive(Debug)]
struct ActivityHbState {
    last_recorded_details: Option<Vec<common::Payload>>,
    shutdown_tx: UnboundedSender<()>,
}

async fn send_heartbeat(
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static + ?Sized>,
    cancels_tx: UnboundedSender<PendingActivityCancel>,
    task_token: TaskToken,
    details: Vec<common::Payload>,
) {
    match sg
        .record_activity_heartbeat(task_token.clone(), details.into_payloads())
        .await
    {
        Ok(RecordActivityTaskHeartbeatResponse { cancel_requested }) => {
            if cancel_requested {
                cancels_tx
                    .send(PendingActivityCancel {
                        task_token,
                        reason: ActivityCancelReason::Cancelled,
                    })
                    .expect("Receive half of heartbeat cancels not blocked");
            }
        }
        // Send cancels for any activity that learns its workflow already finished
        // (which is one thing not found implies - other reasons would seem equally
        // valid).
        Err(s) if s.code() == tonic::Code::NotFound => {
            cancels_tx
                .send(PendingActivityCancel {
                    task_token,
                    reason: ActivityCancelReason::NotFound,
                })
                .expect("Receive half of heartbeat cancels not blocked");
        }
        Err(e) => {
            warn!("Error when recording heartbeat: {:?}", e);
        }
    }
}

type HBStateMap = Arc<Mutex<HashMap<TaskToken, ActivityHbState>>>;
type JoinHandleMap = Arc<Mutex<Option<HashMap<TaskToken, JoinHandle<()>>>>>;

async fn activity_heartbeat_loop(
    tt: TaskToken,
    throttle_interval: Duration,
    heartbeat_states: HBStateMap,
    join_handles: JoinHandleMap,
    mut shutdown_rx: UnboundedReceiver<()>,
    sg: Arc<impl ServerGatewayApis + Send + Sync + 'static + ?Sized>,
    cancels_tx: UnboundedSender<PendingActivityCancel>,
) {
    loop {
        {
            let mut states = heartbeat_states.lock().await;
            // state could only be removed in this case by eviction
            if let Some(state) = states.get_mut(&tt) {
                // Consume the recorded details, we rely on details being None to break out of the loop
                if let Some(details) = state.last_recorded_details.take() {
                    send_heartbeat(sg.clone(), cancels_tx.clone(), tt.clone(), details).await;
                } else {
                    // No heartbeat received in throttle_interval
                    states.remove(&tt);
                    break;
                }
            }
        }

        tokio::select! {
            _ = shutdown_rx.recv() => break,
            _ = tokio::time::sleep(throttle_interval) => continue,
        }
    }
    if let Some(handles) = join_handles.lock().await.as_mut() {
        handles.remove(&tt);
    }
}

impl ActivityHeartbeatManager {
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
    pub fn new(sg: Arc<impl ServerGatewayApis + Send + Sync + 'static + ?Sized>) -> Self {
        let (heartbeat_tx, mut heartbeat_rx) = unbounded_channel();
        let (cancels_tx, cancels_rx) = unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_states = Arc::new(Mutex::new(HashMap::<TaskToken, ActivityHbState>::new()));
        let join_handles = Arc::new(Mutex::new(
            Some(HashMap::<TaskToken, JoinHandle<()>>::new()),
        ));

        let join_handle = tokio::spawn(async move {
            let mut shutdown = shutdown_rx.clone();
            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.changed() => {
                        {
                            let mut states = heartbeat_states.lock().await;
                            for (_, state) in states.drain().into_iter() {
                                state.shutdown_tx.send(()).expect("shutdown receiver dropped");
                            }
                        }
                        let maybe_handles = {
                            // Take the handles and immediately release the lock
                            join_handles.lock().await.take()
                        };
                        if let Some(handles) = maybe_handles {
                            futures::future::join_all(handles.into_values()).await;
                        }
                        break;
                    },
                    hb = heartbeat_rx.recv() => {
                        if let Some(hb) = hb {
                            match hb {
                                HBAction::HB(hb) => {
                                    let mut states = heartbeat_states.lock().await;
                                    let tt = hb.task_token;
                                    if let Some(state) = states.get_mut(&tt) {
                                        state.last_recorded_details = Some(hb.details);
                                    } else {
                                        let (shutdown_tx, shutdown_rx) = unbounded_channel::<()>();
                                        let throttle_interval = hb.throttle_interval;
                                        states.insert(
                                            tt.clone(),
                                            ActivityHbState {
                                                last_recorded_details: Some(hb.details),
                                                shutdown_tx,
                                            },
                                        );

                                        let join_handle = tokio::spawn(
                                            activity_heartbeat_loop(
                                                tt.clone(),
                                                throttle_interval,
                                                heartbeat_states.clone(),
                                                join_handles.clone(),
                                                shutdown_rx,
                                                sg.clone(),
                                                cancels_tx.clone()
                                            )
                                        );

                                        if let Some(handles) = join_handles.lock().await.as_mut() {
                                            handles.insert(tt, join_handle);
                                        }
                                    }
                                },
                                HBAction::Evict(tt) => {
                                    if let Some(state) = heartbeat_states.lock().await.remove(&tt) {
                                        state.shutdown_tx.send(()).expect("shutdown receiver dropped");
                                    }
                                },
                            }
                        }
                    }
                }
            }
        });

        Self {
            incoming_cancels: Mutex::new(cancels_rx),
            join_handle: Mutex::new(Some(join_handle)),
            shutting_down: shutdown_tx,
            heartbeat_tx,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{pollers::MockServerGatewayApis, test_help::TEST_Q};
    use std::time::Duration;
    use temporal_sdk_core_protos::{
        coresdk::common::Payload,
        temporal::api::workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    };
    use tokio::time::sleep;

    /// Ensure that heartbeats that are sent with a small throttleInterval are aggregated and sent roughly once
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
        // Send 2 heartbeat requests for 20ms apart.
        // The first heartbeat should be sent right away, and
        // the second should be throttled until 50ms have passed.
        for i in 0_u8..2 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(50));
            sleep(Duration::from_millis(20)).await;
        }
        // sleep again to let heartbeats be flushed
        sleep(Duration::from_millis(20)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn send_heartbeats_less_frequently_throttle_interval() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(3);
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_gateway));
        let fake_task_token = vec![1, 2, 3];
        // Heartbeats always get sent if recorded less frequently than the throttle intreval
        for i in 0_u8..3 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(10));
            sleep(Duration::from_millis(20)).await;
        }
        // sleep again to let heartbeats be flushed
        hm.shutdown().await;
    }

    /// Ensure that heartbeat can be called from a tight loop without any throttle_interval, resulting in two
    /// interactions with the server - one immediately and one after 500ms after the throttle_interval.
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
        for i in 0_u8..50 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(2000));
            // Let it propagate
            sleep(Duration::from_millis(10)).await;
        }
        hm.shutdown().await;
    }

    /// This test reports one heartbeat and waits for the throttle_interval to elapse before sending another
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
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
        hm.evict(fake_task_token.clone().into());
        record_heartbeat(&hm, fake_task_token, 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
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
                unreachable!("heartbeat should not be recorded after the shutdown");
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
        throttle_interval: Duration,
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
            throttle_interval,
        )
        .expect("hearbeat recording should not fail");
    }
}
