use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        common::Payload,
        workflow_commands::{workflow_command, HasChange, ScheduleActivity, StartTimer},
    },
    prototype_rust_sdk::{CommandCreateRequest, RustWfCmd, UnblockEvent},
};
use crossbeam::channel::{Receiver, Sender};
use futures::{task::Context, FutureExt};
use parking_lot::RwLock;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::{oneshot, watch};

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    chan: Sender<RustWfCmd>,
    args: Vec<Payload>,
    am_cancelled: watch::Receiver<bool>,
    shared: Arc<RwLock<WfContextSharedData>>,
}

#[derive(Clone, Debug, Default)]
pub struct WfContextSharedData {
    /// Maps change ids -> resolved status
    pub changes: HashMap<String, bool>,
    pub is_replaying: bool,
}

impl WfContext {
    /// Create a new wf context, returning the context itself, the shared cache for blocked
    /// commands, and a receiver which outputs commands sent from the workflow.
    pub(super) fn new(
        args: Vec<Payload>,
        am_cancelled: watch::Receiver<bool>,
    ) -> (Self, Receiver<RustWfCmd>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (
            Self {
                chan,
                args,
                am_cancelled,
                shared: Arc::new(RwLock::new(Default::default())),
            },
            rx,
        )
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
    }

    pub(crate) fn get_shared_data(&self) -> Arc<RwLock<WfContextSharedData>> {
        self.shared.clone()
    }

    /// A future that resolves if/when the workflow is cancelled
    pub async fn cancelled(&mut self) {
        if *self.am_cancelled.borrow() {
            return;
        }
        self.am_cancelled
            .changed()
            .await
            .expect("Cancelled send half not dropped")
    }

    /// Request to create a timer
    pub fn timer(&mut self, a: StartTimer) -> impl Future<Output = ()> {
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: a.into(),
                unblocker,
            }
            .into(),
        );
        cmd.map(|ue| {
            if let UnblockEvent::Timer(_) = ue {
            } else {
                panic!("Wrong unblock event")
            }
        })
    }

    /// Request to run an activity
    pub fn activity(&mut self, a: ScheduleActivity) -> impl Future<Output = ActivityResult> {
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: a.into(),
                unblocker,
            }
            .into(),
        );
        cmd.map(|ue| {
            if let UnblockEvent::Activity { result, .. } = ue {
                result
            } else {
                panic!("Wrong unblock event")
            }
        })
    }

    /// Cancel a timer
    pub fn cancel_timer(&self, timer_id: &str) {
        self.send(RustWfCmd::CancelTimer(timer_id.to_string()))
    }

    /// Cancel activity
    pub fn cancel_activity(&self, activity_id: &str) {
        self.send(RustWfCmd::CancelActivity(activity_id.to_string()))
    }

    /// Check (or record) that this workflow history was created with the provided change id
    pub fn has_version(&self, change_id: &str) -> bool {
        self.has_version_impl(change_id, false)
    }

    /// Record that this workflow history was created with the provided change id, and it is being
    /// phased out.
    pub fn has_version_deprecated(&self, change_id: &str) {
        self.has_version_impl(change_id, true);
    }

    fn has_version_impl(&self, change_id: &str, deprecated: bool) -> bool {
        self.send(
            workflow_command::Variant::HasChange(HasChange {
                change_id: change_id.to_string(),
                deprecated,
            })
            .into(),
        );
        // See if we already know about the status of this change
        if let Some(present) = self.shared.read().changes.get(change_id) {
            return *present;
        }

        // If we don't already know about the change, that means there is no marker in history,
        // and we should return false if we are replaying
        let res = !self.shared.read().is_replaying;

        self.shared
            .write()
            .changes
            .insert(change_id.to_string(), res);

        res
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.send(with.into())
    }

    fn send(&self, c: RustWfCmd) {
        self.chan.send(c).unwrap();
    }
}

struct WFCommandFut {
    result_rx: oneshot::Receiver<UnblockEvent>,
}

impl WFCommandFut {
    fn new() -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (Self { result_rx: rx }, tx)
    }
}

impl Future for WFCommandFut {
    type Output = UnblockEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| x.unwrap())
    }
}
