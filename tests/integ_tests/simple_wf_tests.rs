use assert_matches::assert_matches;
use crossbeam::channel::{unbounded, RecvTimeoutError};
use futures::{channel::mpsc::UnboundedReceiver, future, Future, SinkExt, StreamExt};
use rand::{self, Rng};
use std::{collections::HashMap, convert::TryFrom, env, sync::Arc, time::Duration};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::{self, activity_result as act_res, ActivityResult},
        activity_task::activity_task as act_task,
        common::{Payload, UserCodeFailure},
        workflow_activation::{
            wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, WfActivation,
            WfActivationJob,
        },
        workflow_commands::{
            CancelTimer, CompleteWorkflowExecution, FailWorkflowExecution, ScheduleActivity,
            StartTimer,
        },
        workflow_completion::WfActivationCompletion,
    },
    Core, CoreError, CoreInitOptions, ServerGatewayApis, ServerGatewayOptions, Url,
};

// TODO: These tests can get broken permanently if they break one time and the server is not
//  restarted, because pulling from the same task queue produces tasks for the previous failed
//  workflows. Fix that.

// TODO: We should also get expected histories for these tests and confirm that the history
//   at the end matches.

const NAMESPACE: &str = "default";
type GwApi = Arc<dyn ServerGatewayApis>;

async fn create_workflow(
    core: &dyn Core,
    task_q: &str,
    workflow_id: &str,
    wf_type: Option<&str>,
) -> String {
    with_gw(core, |gw: GwApi| async move {
        gw.start_workflow(
            NAMESPACE.to_owned(),
            task_q.to_owned(),
            workflow_id.to_owned(),
            wf_type.unwrap_or("test-workflow").to_owned(),
        )
        .await
        .unwrap()
        .run_id
    })
    .await
}

async fn with_gw<F: FnOnce(GwApi) -> Fout, Fout: Future>(core: &dyn Core, fun: F) -> Fout::Output {
    let gw = core.server_gateway();
    fun(gw).await
}

fn get_integ_server_options() -> ServerGatewayOptions {
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "integ_tester".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
    }
}

async fn get_integ_core() -> impl Core {
    let gateway_opts = get_integ_server_options();
    temporal_sdk_core::init(CoreInitOptions { gateway_opts })
        .await
        .unwrap()
}

#[tokio::test]
async fn timer_workflow() {
    let task_q = "timer_workflow";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_workflow() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core().await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(activity_completion_req(task_q, &activity_id, task))
        .await
        .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = dbg!(core.poll_activity_task(task_q).await.unwrap());
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    // Complete activity successfully.
    core.complete_activity_task(
        task.task_token,
        ActivityResult::ok(response_payload.clone()),
    )
    .await
    .unwrap();
    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                     ..})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn activity_non_retryable_failure() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_failed_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core().await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(activity_completion_req(task_q, &activity_id, task))
        .await
        .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = dbg!(core.poll_activity_task(task_q).await.unwrap());
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Fail activity with non-retryable error
    let failure = UserCodeFailure {
        message: "activity failed".to_string(),
        non_retryable: true,
        ..Default::default()
    };
    core.complete_activity_task(
        task.task_token,
        ActivityResult {
            status: Some(activity_result::activity_result::Status::Failed(
                activity_result::Failure {
                    failure: Some(failure.clone()),
                },
            )),
        },
    )
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Failed(activity_result::Failure{failure: Some(f)}))})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
            assert_eq!(f, &failure);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn activity_retry() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_failed_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core().await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(activity_completion_req(task_q, &activity_id, task))
        .await
        .unwrap();
    // Poll activity 1st time
    let task = core.poll_activity_task(task_q).await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Fail activity with retryable error
    let failure = UserCodeFailure {
        message: "activity failed".to_string(),
        non_retryable: false,
        ..Default::default()
    };
    core.complete_activity_task(
        task.task_token,
        ActivityResult {
            status: Some(activity_result::activity_result::Status::Failed(
                activity_result::Failure {
                    failure: Some(failure),
                },
            )),
        },
    )
    .await
    .unwrap();
    // Poll 2nd time
    let task = dbg!(core.poll_activity_task(task_q).await.unwrap());
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Complete activity successfully
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    core.complete_activity_task(
        task.task_token,
        ActivityResult {
            status: Some(activity_result::activity_result::Status::Completed(
                activity_result::Success {
                    result: Some(response_payload.clone()),
                },
            )),
        },
    )
    .await
    .unwrap();
    // Poll workflow task and verify activity has succeeded.
    let task = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)}))})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

fn activity_completion_req(
    task_q: &str,
    activity_id: &str,
    task: WfActivation,
) -> WfActivationCompletion {
    WfActivationCompletion::ok_from_cmds(
        vec![ScheduleActivity {
            activity_id: activity_id.to_string(),
            activity_type: "test_activity".to_string(),
            namespace: NAMESPACE.to_owned(),
            task_queue: task_q.to_owned(),
            schedule_to_start_timeout: Some(Duration::from_secs(30).into()),
            start_to_close_timeout: Some(Duration::from_secs(30).into()),
            schedule_to_close_timeout: Some(Duration::from_secs(60).into()),
            heartbeat_timeout: Some(Duration::from_secs(60).into()),
            ..Default::default()
        }
        .into()],
        task.task_token,
    )
}

#[tokio::test]
async fn parallel_timer_workflow() {
    let task_q = "parallel_timer_workflow";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id = "timer 1".to_string();
    let timer_2_id = "timer 2".to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: timer_2_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(100).into()),
            }
            .into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
    // Wait long enough for both timers to complete. Server seems to be a bit weird about actually
    // sending both of these in one go, so we need to wait longer than you would expect.
    std::thread::sleep(Duration::from_millis(1500));
    let task = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t1_id }
                )),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t2_id }
                )),
            }
        ] => {
            assert_eq!(t1_id, &timer_id);
            assert_eq!(t2_id, &timer_2_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let task_q = "timer_cancel_workflow";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(10).into()),
            }
            .into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = dbg!(core.poll_workflow_task(task_q).await.unwrap());
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_immediate_cancel_workflow() {
    let task_q = "timer_immediate_cancel_workflow";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let task_q = "parallel_workflows_same_queue";
    let core = get_integ_core().await;
    let num_workflows = 25;

    let run_ids: Vec<_> =
        future::join_all((0..num_workflows).map(|i| {
            let core = &core;
            async move {
                create_workflow(core, task_q, &format!("wf-id-{}", i), Some("wf-type-1")).await
            }
        }))
        .await;

    let mut send_chans = HashMap::new();

    async fn wf_task(core: Arc<dyn Core>, mut task_chan: UnboundedReceiver<WfActivation>) {
        let task = task_chan.next().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(
                    StartWorkflow {
                        workflow_type,
                        ..
                    }
                )),
            }] => assert_eq!(&workflow_type, &"wf-type-1")
        );
        core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![StartTimer {
                timer_id: "timer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            }
            .into()],
            task.task_token,
        ))
        .await
        .unwrap();
        let task = task_chan.next().await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task.task_token,
        ))
        .await
        .unwrap();
    }

    let core = Arc::new(core);
    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            send_chans.insert(run_id.clone(), tx);
            let core_c = core.clone();
            tokio::spawn(wf_task(core_c, rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 {
        let task = core.poll_workflow_task(task_q).await.unwrap();
        send_chans
            .get(&task.run_id)
            .unwrap()
            .send(task)
            .await
            .unwrap();
    }

    for handle in handles {
        handle.await.unwrap()
    }
}

// Ideally this would be a unit test, but returning a pending future with mockall bloats the mock
// code a bunch and just isn't worth it. Do it when https://github.com/asomers/mockall/issues/189 is
// fixed.
#[tokio::test]
async fn shutdown_aborts_actively_blocked_poll() {
    let task_q = "shutdown_aborts_actively_blocked_poll";
    let core = Arc::new(get_integ_core().await);
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        tcore.shutdown();
    });
    assert_matches!(
        core.poll_workflow_task(task_q).await.unwrap_err(),
        CoreError::ShuttingDown
    );
    handle.join().unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown();
    assert_matches!(
        core.poll_workflow_task(task_q).await.unwrap_err(),
        CoreError::ShuttingDown
    );
}

#[tokio::test]
async fn fail_wf_task() {
    let task_q = "fail_wf_task";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    // Start with a timer
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();

    // Allow timer to fire
    std::thread::sleep(Duration::from_millis(500));

    // Then break for whatever reason
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::fail(
        task.task_token,
        UserCodeFailure {
            message: "I did an oopsie".to_string(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn fail_workflow_execution() {
    let task_q = "fail_workflow_execution";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![FailWorkflowExecution {
            failure: Some(UserCodeFailure {
                message: "I'm ded".to_string(),
                ..Default::default()
            }),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow() {
    let task_q = "signal_workflow";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_task(task_q).await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![],
        res.task_token.clone(),
    ))
    .await
    .unwrap();

    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_2.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;

    let res = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            }
        ]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        res.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let task_q = "signal_workflow_signal_not_handled_on_workflow_completion";
    let core = get_integ_core().await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    let signal_id_1 = "signal1";
    let res = core.poll_workflow_task(task_q).await.unwrap();
    // Task is completed with a timer
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "sometimer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(10).into()),
        }
        .into()],
        res.task_token,
    ))
    .await
    .unwrap();

    // Poll before sending the signal - we should have the timer job
    let res = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );

    let task_token = res.task_token.clone();
    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;

    // Send completion - not having seen a poll response with a signal in it yet
    let unhandled = core
        .complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task_token,
        ))
        .await
        .unwrap_err();
    assert_matches!(unhandled, CoreError::UnhandledCommandWhenCompleting);

    // We should get a new task with the signal
    let res = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        res.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn long_poll_timeout_is_retried() {
    let mut gateway_opts = get_integ_server_options();
    // Server whines unless long poll > 2 seconds
    gateway_opts.long_poll_timeout = Duration::from_secs(3);
    let core = temporal_sdk_core::init(CoreInitOptions { gateway_opts })
        .await
        .unwrap();
    // Should block for more than 3 seconds, since we internally retry long poll
    let (tx, rx) = unbounded();
    tokio::spawn(async move {
        core.poll_workflow_task("some_task_q").await.unwrap();
        tx.send(())
    });
    let err = rx.recv_timeout(Duration::from_secs(4)).unwrap_err();
    assert_matches!(err, RecvTimeoutError::Timeout);
}
