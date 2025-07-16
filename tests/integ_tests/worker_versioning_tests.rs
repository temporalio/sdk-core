use crate::integ_tests::activity_functions::echo;
use std::time::Duration;
use temporal_client::{NamespacedClient, WorkflowOptions, WorkflowService};
use temporal_sdk::{ActivityOptions, WfContext};
use temporal_sdk_core_api::worker::{
    WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerVersioningStrategy,
};
use temporal_sdk_core_protos::{
    coresdk::{
        AsJsonPayloadExt, workflow_commands::CompleteWorkflowExecution, workflow_completion,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::VersioningBehavior,
        history::v1::history_event::Attributes,
        workflowservice::v1::{
            DescribeWorkerDeploymentRequest, SetWorkerDeploymentCurrentVersionRequest,
        },
    },
};
use temporal_sdk_core_test_utils::{CoreWfStarter, WorkerTestHelpers, eventually};
use tokio::join;

#[rstest::rstest]
#[tokio::test]
async fn sets_deployment_info_on_task_responses(#[values(true, false)] use_default: bool) {
    let wf_type = "sets_deployment_info_on_task_responses";
    let mut starter = CoreWfStarter::new(wf_type);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    let version = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "1.0".to_string(),
    };
    starter
        .worker_config
        .versioning_strategy(WorkerVersioningStrategy::WorkerDeploymentBased(
            WorkerDeploymentOptions {
                version: version.clone(),
                use_worker_versioning: true,
                default_versioning_behavior: VersioningBehavior::AutoUpgrade.into(),
            },
        ))
        .no_remote_activities(true);
    let core = starter.get_worker().await;
    let client = starter.get_client().await;

    // A bit annoying. We have to start up polling here so that the deployment will exist before
    // we can describe it and then set the current version.
    let worker_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_eq!(
            version,
            res.deployment_version_for_current_task.unwrap().into(),
        );

        let mut success_complete = workflow_completion::Success::from_variants(vec![
            CompleteWorkflowExecution { result: None }.into(),
        ]);
        if !use_default {
            success_complete.versioning_behavior = VersioningBehavior::Pinned.into();
        }
        core.complete_workflow_activation(WorkflowActivationCompletion {
            run_id: res.run_id.clone(),
            status: Some(success_complete.into()),
        })
        .await
        .unwrap();
    };

    let ops_task = async {
        let desc_resp = eventually(
            async || {
                client
                    .get_client()
                    .clone()
                    .describe_worker_deployment(DescribeWorkerDeploymentRequest {
                        namespace: client.namespace().to_string(),
                        deployment_name: deploy_name.clone(),
                    })
                    .await
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .into_inner();

        #[allow(deprecated)]
        client
            .get_client()
            .clone()
            .set_worker_deployment_current_version(SetWorkerDeploymentCurrentVersionRequest {
                namespace: client.namespace().to_owned(),
                deployment_name: deploy_name.clone(),
                version: format!("{deploy_name}.1.0"),
                conflict_token: desc_resp.conflict_token,
                ..Default::default()
            })
            .await
            .unwrap();

        starter.start_wf().await;
    };

    join!(worker_task, ops_task);
    core.handle_eviction().await;
    core.shutdown().await;

    // Fetch history & verify task complete is properly stamped
    let history = starter.get_history().await;
    let wft_complete = history
        .events
        .into_iter()
        .find_map(|e| {
            if let Attributes::WorkflowTaskCompletedEventAttributes(a) = e.attributes.unwrap() {
                Some(a)
            } else {
                None
            }
        })
        .unwrap();
    if use_default {
        assert_eq!(
            wft_complete.versioning_behavior,
            VersioningBehavior::AutoUpgrade as i32
        );
    } else {
        assert_eq!(
            wft_complete.versioning_behavior,
            VersioningBehavior::Pinned as i32
        );
    }
    assert_eq!(wft_complete.worker_deployment_name, deploy_name);
    let dv = wft_complete.deployment_version.unwrap();
    assert_eq!(dv.deployment_name, deploy_name);
    assert_eq!(dv.build_id, "1.0");
}

#[tokio::test]
async fn activity_has_deployment_stamp() {
    let wf_name = "activity_has_deployment_stamp";
    let mut starter = CoreWfStarter::new(wf_name);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    starter
        .worker_config
        .versioning_strategy(WorkerVersioningStrategy::WorkerDeploymentBased(
            WorkerDeploymentOptions {
                version: WorkerDeploymentVersion {
                    deployment_name: deploy_name.clone(),
                    build_id: "1.0".to_string(),
                },
                use_worker_versioning: true,
                default_versioning_behavior: VersioningBehavior::AutoUpgrade.into(),
            },
        ));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "echo_activity".to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            input: "hi!".as_json_payload().expect("serializes fine"),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });
    worker.register_activity("echo_activity", echo);
    let submitter = worker.get_submitter_handle();
    let shutdown_handle = worker.inner_mut().shutdown_handle();

    let client_task = async {
        let desc_resp = eventually(
            async || {
                client
                    .get_client()
                    .clone()
                    .describe_worker_deployment(DescribeWorkerDeploymentRequest {
                        namespace: client.namespace().to_string(),
                        deployment_name: deploy_name.clone(),
                    })
                    .await
            },
            Duration::from_secs(50),
        )
        .await
        .unwrap()
        .into_inner();

        #[allow(deprecated)]
        client
            .get_client()
            .clone()
            .set_worker_deployment_current_version(SetWorkerDeploymentCurrentVersionRequest {
                namespace: client.namespace().to_owned(),
                deployment_name: deploy_name.clone(),
                version: format!("{deploy_name}.1.0"),
                conflict_token: desc_resp.conflict_token,
                ..Default::default()
            })
            .await
            .unwrap();

        submitter
            .submit_wf(
                starter.get_wf_id(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        starter.wait_for_default_wf_finish().await.unwrap();
        shutdown_handle();
    };
    join!(
        async {
            worker.inner_mut().run().await.unwrap();
        },
        client_task
    );
    let hist = starter.get_history().await;
    let _activity_completed = hist
        .events
        .into_iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskCompletedEventAttributes(a) = e.attributes.unwrap() {
                Some(a)
            } else {
                None
            }
        })
        .unwrap();
    // TODO: Can't actually verify this at the moment as the deployment options are not transferred
    //   to the event.
}
