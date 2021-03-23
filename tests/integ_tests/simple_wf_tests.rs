use assert_matches::assert_matches;
use futures::Future;
use rand::{self, Rng};
use std::{
    collections::HashMap,
    convert::TryFrom,
    env,
    sync::{
        mpsc::{channel, Receiver},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::{self, activity_result as act_res, ActivityResult},
        activity_task::activity_task as act_task,
        common::{Payload, UserCodeFailure},
        workflow_activation::{
            wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, WfActivationJob,
        },
        workflow_commands::{
            CancelTimer, CompleteWorkflowExecution, FailWorkflowExecution, ScheduleActivity,
            StartTimer,
        },
        Task, TaskCompletion,
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

fn create_workflow(
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
}

#[tokio::main]
async fn with_gw<F: FnOnce(GwApi) -> Fout, Fout: Future>(core: &dyn Core, fun: F) -> Fout::Output {
    let gw = core.server_gateway().unwrap();
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

fn get_integ_core() -> impl Core {
    let gateway_opts = get_integ_server_options();
    temporal_sdk_core::init(CoreInitOptions { gateway_opts }).unwrap()
}

#[test]
fn timer_workflow() {
    let task_q = "timer_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .unwrap();
    let task = dbg!(core.poll_workflow_task(task_q).unwrap());
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn activity_workflow() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
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
    ))
    .unwrap();
    let task = dbg!(core.poll_activity_task(task_q).unwrap());
    assert_matches!(
        task.get_activity_variant(),
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    let response_payloads = vec![Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    }];
    core.complete_activity_task(TaskCompletion::ok_activity(
        response_payloads.clone(),
        task.task_token,
    ))
    .unwrap();
    let task = core.poll_workflow_task(task_q).unwrap();
    assert_matches!(
        task.get_wf_jobs().as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: r}))})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payloads);
        }
    );
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        task.task_token,
    ))
    .unwrap()
}

#[test]
fn parallel_timer_workflow() {
    let task_q = "parallel_timer_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let timer_id = "timer 1".to_string();
    let timer_2_id = "timer 2".to_string();
    let task = dbg!(core.poll_workflow_task(task_q).unwrap());
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
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
    .unwrap();
    // Wait long enough for both timers to complete. Server seems to be a bit weird about actually
    // sending both of these in one go, so we need to wait longer than you would expect.
    std::thread::sleep(Duration::from_millis(1500));
    let task = core.poll_workflow_task(task_q).unwrap();
    assert_matches!(
        task.get_wf_jobs().as_slice(),
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
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn timer_cancel_workflow() {
    let task_q = "timer_cancel_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    dbg!(create_workflow(
        &core,
        task_q,
        &workflow_id.to_string(),
        None
    ));
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
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
    .unwrap();
    let task = dbg!(core.poll_workflow_task(task_q).unwrap());
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: vec![] }.into(),
        ],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn timer_immediate_cancel_workflow() {
    let task_q = "timer_immediate_cancel_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
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
            CompleteWorkflowExecution { result: vec![] }.into(),
        ],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn parallel_workflows_same_queue() {
    let task_q = "parallel_workflows_same_queue";
    let core = get_integ_core();
    let num_workflows = 25;

    let run_ids: Vec<_> = (0..num_workflows)
        .map(|i| create_workflow(&core, task_q, &format!("wf-id-{}", i), Some("wf-type-1")))
        .collect();

    let mut send_chans = HashMap::new();

    fn wf_thread(core: Arc<dyn Core>, task_chan: Receiver<Task>) {
        let task = task_chan.recv().unwrap();
        assert_matches!(
            task.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(
                    StartWorkflow {
                        workflow_type,
                        ..
                    }
                )),
            }] => assert_eq!(&workflow_type, &"wf-type-1")
        );
        core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: "timer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            }
            .into()],
            task.task_token,
        ))
        .unwrap();
        let task = task_chan.recv().unwrap();
        core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task.task_token,
        ))
        .unwrap();
    }

    let core = Arc::new(core);
    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = channel();
            send_chans.insert(run_id.clone(), tx);
            let core_c = core.clone();
            std::thread::spawn(move || wf_thread(core_c, rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 {
        let task = core.poll_workflow_task(task_q).unwrap();
        send_chans
            .get(task.get_run_id().unwrap())
            .unwrap()
            .send(task)
            .unwrap();
    }

    handles.into_iter().for_each(|h| h.join().unwrap());
}

// Ideally this would be a unit test, but returning a pending future with mockall bloats the mock
// code a bunch and just isn't worth it. Do it when https://github.com/asomers/mockall/issues/189 is
// fixed.
#[test]
fn shutdown_aborts_actively_blocked_poll() {
    let task_q = "shutdown_aborts_actively_blocked_poll";
    let core = Arc::new(get_integ_core());
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        tcore.shutdown().unwrap();
    });
    assert_matches!(
        core.poll_workflow_task(task_q).unwrap_err(),
        CoreError::ShuttingDown
    );
    handle.join().unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().unwrap();
    assert_matches!(
        core.poll_workflow_task(task_q).unwrap_err(),
        CoreError::ShuttingDown
    );
}

#[test]
fn fail_wf_task() {
    let task_q = "fail_wf_task";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);

    // Start with a timer
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .unwrap();

    // Allow timer to fire
    std::thread::sleep(Duration::from_millis(500));

    // Then break for whatever reason
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::fail(
        task.task_token,
        UserCodeFailure {
            message: "I did an oopsie".to_string(),
            ..Default::default()
        },
    ))
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .unwrap();
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn fail_workflow_execution() {
    let task_q = "fail_workflow_execution";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .unwrap();
    let task = core.poll_workflow_task(task_q).unwrap();
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![FailWorkflowExecution {
            failure: Some(UserCodeFailure {
                message: "I'm ded".to_string(),
                ..Default::default()
            }),
        }
        .into()],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn signal_workflow() {
    let task_q = "signal_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_task(task_q).unwrap();
    // Task is completed with no commands
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![],
        res.task_token.clone(),
    ))
    .unwrap();

    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.get_run_id().unwrap().to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.get_run_id().unwrap().to_string(),
            signal_id_2.to_string(),
            None,
        )
        .await
        .unwrap();
    });

    let res = core.poll_workflow_task(task_q).unwrap();
    assert_matches!(
        res.get_wf_jobs().as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            }
        ]
    );
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        res.task_token,
    ))
    .unwrap();
}

#[test]
fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let task_q = "signal_workflow_signal_not_handled_on_workflow_completion";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);

    let signal_id_1 = "signal1";
    let res = core.poll_workflow_task(task_q).unwrap();
    // Task is completed with a timer
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimer {
            timer_id: "sometimer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(10).into()),
        }
        .into()],
        res.task_token,
    ))
    .unwrap();

    // Poll before sending the signal - we should have the timer job
    let res = core.poll_workflow_task(task_q).unwrap();
    assert_matches!(
        res.get_wf_jobs().as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );

    let task_token = res.task_token.clone();
    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.get_run_id().unwrap().to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
    });

    // Send completion - not having seen a poll response with a signal in it yet
    let unhandled = core
        .complete_workflow_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task_token,
        ))
        .unwrap_err();
    assert_matches!(unhandled, CoreError::UnhandledCommandWhenCompleting);

    // We should get a new task with the signal
    let res = core.poll_workflow_task(task_q).unwrap();
    assert_matches!(
        res.get_wf_jobs().as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecution { result: vec![] }.into()],
        res.task_token,
    ))
    .unwrap();
}
