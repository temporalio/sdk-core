use crate::common::{CoreWfStarter, NAMESPACE, activity_functions::StdActivities};
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use temporalio_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution, workflowservice::v1::ResetWorkflowExecutionRequest,
};

use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{LocalActivityOptions, WorkflowContext, WorkflowResult};
use tokio::sync::Notify;
use tonic::IntoRequest;

const POST_RESET_SIG: &str = "post-reset";

#[workflow]
struct ResetMeWf {
    notify: Arc<Notify>,
}

#[workflow_methods(factory_only)]
impl ResetMeWf {
    #[run(name = "reset_me_wf")]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        ctx.timer(Duration::from_secs(1)).await;
        self.notify.notify_one();
        let _ = ctx
            .make_signal_channel(POST_RESET_SIG)
            .next()
            .await
            .unwrap();
        Ok(().into())
    }
}

#[tokio::test]
async fn reset_workflow() {
    let wf_name = "reset_me_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();
    worker.register_workflow_with_factory(move || ResetMeWf {
        notify: notify_clone.clone(),
    });

    let handle = worker
        .submit_workflow(ResetMeWf::run, wf_name, (), WorkflowOptions::default())
        .await
        .unwrap();
    let run_id = handle.info().run_id.clone().unwrap();

    let mut client = starter.get_client().await;
    let resetter_fut = async {
        notify.notified().await;
        // Do the reset
        client
            .reset_workflow_execution(
                ResetWorkflowExecutionRequest {
                    namespace: NAMESPACE.to_owned(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: wf_name.to_owned(),
                        run_id,
                    }),
                    // End of first WFT
                    workflow_task_finish_event_id: 4,
                    request_id: "test-req-id".to_owned(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we use empty run id
        WorkflowClientTrait::signal_workflow_execution(
            &client,
            wf_name.to_owned(),
            "".to_owned(),
            POST_RESET_SIG.to_owned(),
            None,
            None,
        )
        .await
        .unwrap();

        // Wait for the now-reset workflow to finish
        client
            .get_untyped_workflow_handle(wf_name.to_owned(), "")
            .get_workflow_result(Default::default())
            .await
            .unwrap();
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(resetter_fut, run_fut);
    rr.unwrap();
}

const POST_FAIL_SIG: &str = "post-fail";

#[workflow]
struct ResetRandomseedWf {
    did_fail: Arc<AtomicBool>,
    rand_seed: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[workflow_methods(factory_only)]
impl ResetRandomseedWf {
    #[run(name = "reset_randomseed")]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let _ = self.rand_seed.compare_exchange(
            0,
            ctx.random_seed(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        ctx.timer(Duration::from_millis(100)).await;
        ctx.timer(Duration::from_millis(100)).await;
        if self
            .did_fail
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.notify.notify_one();
            panic!("Ahh");
        }
        if self.rand_seed.load(Ordering::Relaxed) == ctx.random_seed() {
            ctx.timer(Duration::from_millis(100)).await;
        } else {
            ctx.start_local_activity(
                StdActivities::echo,
                "hi!".to_string(),
                LocalActivityOptions::default(),
            )?
            .await;
        }
        let _ = ctx.make_signal_channel(POST_FAIL_SIG).next().await.unwrap();
        self.notify.notify_one();
        let _ = ctx
            .make_signal_channel(POST_RESET_SIG)
            .next()
            .await
            .unwrap();
        Ok(().into())
    }
}

#[tokio::test]
async fn reset_randomseed() {
    let wf_name = "reset_randomseed";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: true,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    let did_fail = Arc::new(AtomicBool::new(false));
    let rand_seed = Arc::new(AtomicU64::new(0));
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();
    worker.register_workflow_with_factory(move || ResetRandomseedWf {
        did_fail: did_fail.clone(),
        rand_seed: rand_seed.clone(),
        notify: notify_clone.clone(),
    });
    worker.register_activities(StdActivities);

    let handle = worker
        .submit_workflow(
            ResetRandomseedWf::run,
            wf_name,
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let run_id = handle.info().run_id.clone().unwrap();

    let mut client = starter.get_client().await;
    let client_fur = async {
        notify.notified().await;
        WorkflowClientTrait::signal_workflow_execution(
            &client,
            wf_name.to_owned(),
            run_id.clone(),
            POST_FAIL_SIG.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
        notify.notified().await;
        // Reset the workflow to be after first timer has fired
        client
            .reset_workflow_execution(
                ResetWorkflowExecutionRequest {
                    namespace: NAMESPACE.to_owned(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: wf_name.to_owned(),
                        run_id: run_id.clone(),
                    }),
                    workflow_task_finish_event_id: 14,
                    request_id: "test-req-id".to_owned(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we use empty run id
        WorkflowClientTrait::signal_workflow_execution(
            &client,
            wf_name.to_owned(),
            "".to_owned(),
            POST_RESET_SIG.to_owned(),
            None,
            None,
        )
        .await
        .unwrap();

        // Wait for the now-reset workflow to finish
        client
            .get_untyped_workflow_handle(wf_name.to_owned(), "")
            .get_workflow_result(Default::default())
            .await
            .unwrap();
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(client_fur, run_fut);
    rr.unwrap();
}
