//! Test utilities for the core crate
//! Only available when the test-utilities feature is enabled

use assert_matches::assert_matches;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, time::Duration};
use temporal_sdk_core_api::{Worker as CoreWorker, errors::PollError};
use temporal_sdk_core_protos::coresdk::{
    workflow_activation::{WorkflowActivationJob, workflow_activation_job},
    workflow_commands::{CompleteWorkflowExecution, StartTimer},
    workflow_completion::WorkflowActivationCompletion,
};

/// Default namespace for testing
pub const NAMESPACE: &str = "default";

/// Default task queue for testing
pub const TEST_Q: &str = "q";

/// Given a desired number of concurrent executions and a provided function that produces a future,
/// run that many instances of the future concurrently.
///
/// Annoyingly, because of a sorta-bug in the way async blocks work, the async block produced by
/// the closure must be `async move` if it uses the provided iteration number. On the plus side,
/// since you're usually just accessing core in the closure, if core is a reference everything just
/// works. See <https://github.com/rust-lang/rust/issues/81653>
pub async fn fanout_tasks<FutureMaker, Fut>(num: usize, fm: FutureMaker)
where
    FutureMaker: Fn(usize) -> Fut,
    Fut: Future,
{
    let mut tasks = FuturesUnordered::new();
    for i in 0..num {
        tasks.push(fm(i));
    }

    while tasks.next().await.is_some() {}
}

/// Initiate shutdown, drain the pollers (handling evictions), and wait for shutdown to complete.
pub async fn drain_pollers_and_shutdown(worker: &dyn CoreWorker) {
    worker.initiate_shutdown();
    tokio::join!(
        async {
            assert_matches!(
                worker.poll_activity_task().await.unwrap_err(),
                PollError::ShutDown
            );
        },
        async {
            loop {
                match worker.poll_workflow_activation().await {
                    Err(PollError::ShutDown) => break,
                    Ok(a) if a.is_only_eviction() => {
                        worker
                            .complete_workflow_activation(WorkflowActivationCompletion::empty(
                                a.run_id,
                            ))
                            .await
                            .unwrap();
                    }
                    o => panic!("Unexpected activation while draining: {o:?}"),
                }
            }
        }
    );
    worker.shutdown().await;
}

#[async_trait::async_trait]
/// Test helper methods for core workers
pub trait WorkerTestHelpers {
    /// Complete a workflow execution
    async fn complete_execution(&self, run_id: &str);
    /// Complete a timer with the given sequence number and duration
    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration);
    /// Handle workflow eviction from cache
    async fn handle_eviction(&self);
}

#[async_trait::async_trait]
impl<T> WorkerTestHelpers for T
where
    T: CoreWorker + ?Sized,
{
    async fn complete_execution(&self, run_id: &str) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    }

    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![
                StartTimer {
                    seq,
                    start_to_fire_timeout: Some(duration.try_into().expect("duration fits")),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();
    }

    async fn handle_eviction(&self) {
        let task = self.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        self.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
    }
}
