use crate::{
    machines::WFCommand,
    protos::coresdk::workflow_activation::{
        wf_activation_job::{self, Variant},
        FireTimer, ResolveActivity,
    },
    test_workflow_driver::{TestWorkflowDriver, UnblockEvent},
    workflow::{ActivationListener, WorkflowFetcher},
};
use futures::executor::block_on;
use std::convert::TryInto;

// TODO: This is bad and I want to get rid of it. Specifically:
//   We should only use the normal workflow bridge -- somehow have TWD complete tasks automatically
//   (like happens from the worker). Maybe just use worker in tests that don't use it yet? Or
//   explicitly complete tasks?

impl WorkflowFetcher for TestWorkflowDriver {
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        let wf_is_done = self.wait_until_wf_iteration_done();

        let mut emit_these = vec![];
        for c in self.drain_pending_commands() {
            emit_these.push(
                c.try_into()
                    .expect("Test workflow commands are well formed"),
            );
        }

        if wf_is_done {
            // TODO: Eventually upgrade to return workflow failures on panic
            block_on(self.join()).expect("Workflow completes without panic");
        }

        debug!(emit_these = ?emit_these, "Test wf driver emitting");

        emit_these
    }
}

impl ActivationListener for TestWorkflowDriver {
    fn on_activation_job(&mut self, activation: &wf_activation_job::Variant) {
        match activation {
            Variant::FireTimer(FireTimer { timer_id }) => {
                info!("Hurr timer fired in listener");
                self.unblock(UnblockEvent::Timer(timer_id.to_owned()));
            }
            Variant::ResolveActivity(ResolveActivity {
                activity_id,
                result,
            }) => {
                self.unblock(UnblockEvent::Activity {
                    id: activity_id.clone(),
                    result: result.clone().expect("Test activity has a result"),
                });
            }
            _ => {}
        }
    }
}
