#![allow(unreachable_pub)]
use futures_util::FutureExt;
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct UpdatableTimerWorkflow {
    deadline_ms: u64,
    deadline_version: u64,
}

#[workflow_methods]
impl UpdatableTimerWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        initial_deadline_ms: u64,
    ) -> WorkflowResult<String> {
        ctx.state_mut(|s| {
            s.deadline_ms = initial_deadline_ms;
        });

        loop {
            let current_version = ctx.state(|s| s.deadline_version);
            let deadline_ms = ctx.state(|s| s.deadline_ms);

            let now_ms = ctx
                .workflow_time()
                .unwrap_or(std::time::UNIX_EPOCH)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if deadline_ms <= now_ms {
                return Ok(format!("Timer fired at deadline {deadline_ms}"));
            }

            let remaining = Duration::from_millis(deadline_ms - now_ms);

            temporalio_sdk::workflows::select! {
                _ = ctx.timer(remaining) => {
                    let final_deadline = ctx.state(|s| s.deadline_ms);
                    return Ok(format!("Timer fired at deadline {final_deadline}"));
                }
                // TODO: make `wait_condition` impl `FusedFuture`
                _ = ctx.wait_condition(|s: &Self| s.deadline_version != current_version).fuse() => {
                    // Deadline was updated, loop to recompute remaining time
                }
            }
        }
    }

    #[signal]
    pub fn update_deadline(&mut self, _ctx: &mut SyncWorkflowContext<Self>, new_deadline_ms: u64) {
        self.deadline_ms = new_deadline_ms;
        self.deadline_version += 1;
    }

    #[query]
    pub fn get_deadline(&self, _ctx: &WorkflowContextView) -> u64 {
        self.deadline_ms
    }
}
