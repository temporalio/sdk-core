use assert_matches::assert_matches;
use std::{cell::Cell, sync::Arc, time::Duration};
use temporal_client::WorkflowOptions;
use temporal_sdk::{interceptors::WorkerInterceptor, WfContext};
use temporal_sdk_core::{init_worker, CoreRuntime, ResourceBasedTuner, ResourceSlotOptions};
use temporal_sdk_core_api::{errors::WorkerValidationError, worker::WorkerConfigBuilder, Worker};
use temporal_sdk_core_protos::{
    coresdk::workflow_completion::{
        workflow_activation_completion::Status, Failure, WorkflowActivationCompletion,
    },
    temporal::api::failure::v1::Failure as InnerFailure,
};
use temporal_sdk_core_test_utils::{
    drain_pollers_and_shutdown, get_integ_server_options, get_integ_telem_options, CoreWfStarter,
};
use tokio::sync::Notify;
use uuid::Uuid;

#[tokio::test]
async fn worker_validation_fails_on_nonexistent_namespace() {
    let opts = get_integ_server_options();
    let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
    let retrying_client = opts
        .connect_no_namespace(runtime.telemetry().get_temporal_metric_meter())
        .await
        .unwrap();

    let worker = init_worker(
        &runtime,
        WorkerConfigBuilder::default()
            .namespace("i_dont_exist")
            .task_queue("Wheee!")
            .worker_build_id("blah")
            .build()
            .unwrap(),
        retrying_client,
    )
    .unwrap();

    let res = worker.validate().await;
    assert_matches!(
        res,
        Err(WorkerValidationError::NamespaceDescribeError { .. })
    );
}

#[tokio::test]
async fn worker_handles_unknown_workflow_types_gracefully() {
    let wf_type = "worker_handles_unknown_workflow_types_gracefully";
    let mut starter = CoreWfStarter::new(wf_type);
    let mut worker = starter.worker().await;

    let run_id = worker
        .submit_wf(
            format!("wce-{}", Uuid::new_v4()),
            "unregistered".to_string(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    struct GracefulAsserter {
        notify: Arc<Notify>,
        run_id: String,
        unregistered_failure_seen: Cell<bool>,
    }
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for GracefulAsserter {
        async fn on_workflow_activation_completion(
            &self,
            completion: &WorkflowActivationCompletion,
        ) {
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Failed(Failure {
                        failure: Some(InnerFailure { message, .. }),
                        ..
                    })),
                    run_id,
                } if message == "Workflow type unregistered not found" && *run_id == self.run_id
            ) {
                self.unregistered_failure_seen.set(true);
            }
            // If we've seen the failure, and the completion is a success for the same run, we're done
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Successful(..)),
                    run_id,
                } if self.unregistered_failure_seen.get() && *run_id == self.run_id
            ) {
                // Shutdown the worker
                self.notify.notify_one();
            }
        }
        fn on_shutdown(&self, _: &temporal_sdk::Worker) {}
    }

    let inner = worker.inner_mut();
    let notify = Arc::new(Notify::new());
    inner.set_worker_interceptor(GracefulAsserter {
        notify: notify.clone(),
        run_id,
        unregistered_failure_seen: Cell::new(false),
    });
    tokio::join!(async { inner.run().await.unwrap() }, async move {
        notify.notified().await;
        let worker = starter.get_worker().await.clone();
        drain_pollers_and_shutdown(&worker).await;
    });
}

#[tokio::test]
async fn resource_based_few_pollers_guarantees_non_sticky_poll() {
    let wf_name = "resource_based_few_pollers_guarantees_non_sticky_poll";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .clear_max_outstanding_opts()
        .no_remote_activities(true)
        // 3 pollers so the minimum slots of 2 can both be handed out to a sticky poller
        .max_concurrent_wft_polls(3_usize);
    // Set the limits to zero so it's essentially unwilling to hand out slots
    let mut tuner = ResourceBasedTuner::new(0.0, 0.0);
    tuner.with_workflow_slots_options(ResourceSlotOptions::new(2, 10, Duration::from_millis(0)));
    starter.worker_config.tuner(Arc::new(tuner));
    let mut worker = starter.worker().await;

    // Workflow doesn't actually need to do anything. We just need to see that we don't get stuck
    // by assigning all slots to sticky pollers.
    worker.register_wf(
        wf_name.to_owned(),
        |_: WfContext| async move { Ok(().into()) },
    );
    for i in 0..20 {
        worker
            .submit_wf(
                format!("{wf_name}_{i}"),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}
