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
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use tokio::sync::{oneshot, watch};

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    chan: Sender<RustWfCmd>,
    args: Vec<Payload>,
    am_cancelled: watch::Receiver<bool>,
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
            },
            rx,
        )
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
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
    pub fn has_version(&self, change_id: &str) -> impl Future<Output = bool> {
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: workflow_command::Variant::HasChange(HasChange {
                    change_id: change_id.to_string(),
                    deprecated: false,
                }),
                unblocker,
            }
            .into(),
        );
        cmd.map(|ue| {
            if let UnblockEvent::Change { present, .. } = ue {
                present
            } else {
                panic!("Wrong unblock event")
            }
        })
    }

    /// Force a workflow task timeout by waiting too long and gumming up the entire runtime
    pub fn force_timeout(&self, by_waiting_for: Duration) {
        self.send(RustWfCmd::ForceTimeout(by_waiting_for))
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
