use crate::{
    machines::WFCommand,
    protos::coresdk::workflow_activation::{
        wf_activation_job::{self, Variant},
        FireTimer, ResolveActivity,
    },
    test_workflow_driver::TestWorkflowDriver,
    workflow::{ActivationListener, WorkflowFetcher},
    CommandID,
};
use futures::executor::block_on;
use std::convert::TryInto;

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
                self.unblock(CommandID::Timer(timer_id.to_owned()));
            }
            Variant::ResolveActivity(ResolveActivity {
                activity_id,
                result: _result,
            }) => {
                self.unblock(CommandID::Activity(activity_id.to_owned()));
            }
            _ => {}
        }
    }
}
