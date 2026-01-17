use crate::common::activity_functions::StdActivities;
use std::time::Duration;
use temporalio_common::{prost_dur, protos::temporal::api::common::v1::RetryPolicy};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, LocalActivityOptions, WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
pub(crate) struct LaProblemWorkflow;

#[workflow_methods]
impl LaProblemWorkflow {
    #[run(name = "evict_while_la_running_no_interference")]
    pub(crate) async fn run(&self, ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.start_local_activity(
            StdActivities::delay,
            Duration::from_secs(15),
            LocalActivityOptions {
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_micros(15))),
                    backoff_coefficient: 1_000.,
                    maximum_interval: Some(prost_dur!(from_millis(1500))),
                    maximum_attempts: 4,
                    non_retryable_error_types: vec![],
                },
                timer_backoff_threshold: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        )?
        .await;
        ctx.start_activity(
            StdActivities::delay,
            Duration::from_secs(15),
            ActivityOptions {
                start_to_close_timeout: Some(Duration::from_secs(20)),
                ..Default::default()
            },
        )?
        .await;
        Ok(().into())
    }
}
