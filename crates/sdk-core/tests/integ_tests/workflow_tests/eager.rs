use crate::common::{CoreWfStarter, activity_functions::StdActivities, get_integ_connection};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_client::{Client, NamespacedClient, WorkflowStartOptions, grpc::WorkflowService};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowType,
    enums::v1::TaskQueueKind,
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{StartWorkflowExecutionRequest, StartWorkflowExecutionResponse},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{
    ActivitySlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext,
    SlotSupplier, SlotSupplierPermit, TunerBuilder,
};
use tonic::IntoRequest;

const LATENCY_WARMUP: usize = 20;
const LATENCY_SAMPLES: usize = 200;

struct EagerOnlyActivitySlots;

#[async_trait::async_trait]
impl SlotSupplier for EagerOnlyActivitySlots {
    type SlotKind = ActivitySlotKind;

    async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
        // Prevent polling from hiding a failure to eagerly dispatch the activity.
        std::future::pending().await
    }

    fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        Some(SlotSupplierPermit::with_user_data(()))
    }

    fn mark_slot_used(&self, _: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, _: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {}
}

#[workflow]
#[derive(Default)]
struct EagerActivityLatencyWorkflow;

#[workflow_methods]
impl EagerActivityLatencyWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<f64>> {
        let mut samples = Vec::with_capacity(LATENCY_SAMPLES);
        for i in 0..LATENCY_WARMUP + LATENCY_SAMPLES {
            // Wall-clock time is deliberately limited to this benchmark, never workflow decisions.
            // Replaying would turn the await into a history lookup and invalidate the measurement.
            assert!(!ctx.is_replaying(), "latency measurement must not replay");
            let start = Instant::now();
            ctx.execute_activity(
                StdActivities::no_op,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
                    .do_not_eagerly_execute(false)
                    .build(),
            )
            .await?;
            let elapsed = start.elapsed();
            assert!(!ctx.is_replaying(), "latency measurement must not replay");
            if i >= LATENCY_WARMUP {
                samples.push(elapsed.as_secs_f64() * 1_000.0);
            }
        }
        Ok(samples)
    }
}

#[tokio::test]
#[ignore = "opt-in wall-clock latency measurement against a local dev server"]
async fn eager_noop_activity_latency() {
    let wf_name = EagerActivityLatencyWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<EagerActivityLatencyWorkflow>()
        .unwrap();
    starter.set_core_cfg_mutator(|cfg| {
        cfg.max_eager_activity_reservations_per_workflow_task = 1;
    });
    let mut tuner = TunerBuilder::default();
    tuner.activity_slot_supplier(Arc::new(EagerOnlyActivitySlots));
    starter.set_core_tuner(Arc::new(tuner.build()));
    let mut worker = starter.worker().await;
    let handle = worker
        .submit_workflow(
            EagerActivityLatencyWorkflow::run,
            (),
            WorkflowStartOptions::new(starter.get_task_queue(), wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let mut samples = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(samples.len(), LATENCY_SAMPLES);
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let percentile = |p: usize| samples[(p * samples.len()).div_ceil(100) - 1];
    println!(
        "Eager no-op activity await latency (ms), {} samples after {} warmups: \
         min={:.3} mean={mean:.3} p50={:.3} p95={:.3} p99={:.3} max={:.3}",
        samples.len(),
        LATENCY_WARMUP,
        samples[0],
        percentile(50),
        percentile(95),
        percentile(99),
        samples.last().unwrap(),
    );
}

#[workflow]
#[derive(Default)]
pub(crate) struct EagerWf;

#[workflow_methods]
impl EagerWf {
    #[run(name = "eager_wf_start")]
    pub(crate) async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn eager_wf_start() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if eager task dispatch failed
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    starter.sdk_config.register_workflow::<EagerWf>().unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_string();
    let res = eager_start(
        wf_name,
        task_queue,
        &starter.get_core_client().await,
        starter.workflow_options.clone(),
    )
    .await;
    res.eager_workflow_task
        .as_ref()
        .expect("no eager workflow task");
    worker.expect_workflow_completion(starter.get_task_queue(), Some(res.run_id));
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn eager_wf_start_different_clients() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if wf task needs retry
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    starter.sdk_config.register_workflow::<EagerWf>().unwrap();
    let mut worker = starter.worker().await;

    let connection = get_integ_connection(None).await;
    let client_opts =
        temporalio_client::ClientOptions::new(starter.get_core_client().await.namespace()).build();
    let client = Client::new(connection, client_opts).unwrap();
    let task_queue = starter.get_task_queue().to_string();
    let res = eager_start(
        wf_name,
        task_queue,
        &client,
        starter.workflow_options.clone(),
    )
    .await;
    // different clients means no eager_wf_start.
    assert!(res.eager_workflow_task.is_none());

    //wf task delivered through default path
    worker.expect_workflow_completion(starter.get_task_queue(), Some(res.run_id));
    worker.run_until_done().await.unwrap();
}

pub(crate) async fn eager_start(
    wf_name: impl Into<String>,
    task_queue: String,
    client: &Client,
    options: WorkflowStartOptions,
) -> StartWorkflowExecutionResponse {
    assert!(options.enable_eager_workflow_start);
    client
        .clone()
        .start_workflow_execution(
            StartWorkflowExecutionRequest {
                namespace: client.namespace(),
                input: None,
                workflow_id: task_queue.clone(),
                workflow_type: Some(WorkflowType {
                    name: wf_name.into(),
                }),
                task_queue: Some(TaskQueue {
                    name: task_queue.clone(),
                    kind: TaskQueueKind::Unspecified as i32,
                    normal_name: "".to_string(),
                }),
                request_id: uuid::Uuid::new_v4().to_string(),
                workflow_id_reuse_policy: options.id_reuse_policy as i32,
                workflow_id_conflict_policy: options.id_conflict_policy as i32,
                workflow_execution_timeout: options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_run_timeout: options.run_timeout.and_then(|d| d.try_into().ok()),
                workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                search_attributes: options.search_attributes.map(|d| d.into_proto()),
                cron_schedule: options.cron_schedule.unwrap_or_default(),
                request_eager_execution: options.enable_eager_workflow_start,
                retry_policy: options.retry_policy.map(Into::into),
                links: options.links,
                completion_callbacks: options.completion_callbacks,
                priority: Some(options.priority.into()),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner()
}
