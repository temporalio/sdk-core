use assert_matches::assert_matches;
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
    protos::{
        coresdk::{
            wf_activation_job, FireTimer, StartWorkflow, Task, TaskCompletion, WfActivationJob,
        },
        temporal::api::command::v1::{
            CancelTimerCommandAttributes, CompleteWorkflowExecutionCommandAttributes,
            StartTimerCommandAttributes,
        },
    },
    Core, CoreInitOptions, ServerGatewayOptions, Url,
};

// TODO: These tests can get broken permanently if they break one time and the server is not
//  restarted, because pulling from the same task queue produces tasks for the previous failed
//  workflows. Fix that.

// TODO: We should also get expected histories for these tests and confirm that the history
//   at the end matches.

const NAMESPACE: &str = "default";

#[tokio::main]
async fn create_workflow(
    core: &dyn Core,
    task_q: &str,
    workflow_id: &str,
    wf_type: Option<&str>,
) -> String {
    core.server_gateway()
        .unwrap()
        .start_workflow(
            NAMESPACE,
            task_q,
            workflow_id,
            wf_type.unwrap_or("test-workflow"),
        )
        .await
        .unwrap()
        .run_id
}

fn get_integ_core() -> impl Core {
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let gateway_opts = ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "integ_tester".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
    };
    let core = temporal_sdk_core::init(CoreInitOptions { gateway_opts }).unwrap();
    core
}

#[test]
fn timer_workflow() {
    let task_q = "timer_workflow";
    let core = get_integ_core();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_task(task_q).unwrap();
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![StartTimerCommandAttributes {
            timer_id: timer_id.to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            ..Default::default()
        }
        .into()],
        task.task_token,
    ))
    .unwrap();
    let task = dbg!(core.poll_task(task_q).unwrap());
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
        task.task_token,
    ))
    .unwrap();
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
    let task = dbg!(core.poll_task(task_q).unwrap());
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![
            StartTimerCommandAttributes {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
                ..Default::default()
            }
            .into(),
            StartTimerCommandAttributes {
                timer_id: timer_2_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(100).into()),
                ..Default::default()
            }
            .into(),
        ],
        task.task_token,
    ))
    .unwrap();
    // Wait long enough for both timers to complete. Server seems to be a bit weird about actually
    // sending both of these in one go, so we need to wait longer than you would expect.
    std::thread::sleep(Duration::from_millis(1500));
    let task = core.poll_task(task_q).unwrap();
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
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn timer_cancel_workflow() {
    let task_q = "timer_cancel_workflow";
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let gateway_opts = ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "none".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
    };
    let core = temporal_sdk_core::init(CoreInitOptions { gateway_opts }).unwrap();
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
    let task = core.poll_task(task_q).unwrap();
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![
            StartTimerCommandAttributes {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
                ..Default::default()
            }
            .into(),
            StartTimerCommandAttributes {
                timer_id: cancel_timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(10).into()),
                ..Default::default()
            }
            .into(),
        ],
        task.task_token,
    ))
    .unwrap();
    let task = dbg!(core.poll_task(task_q).unwrap());
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![
            CancelTimerCommandAttributes {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
        ],
        task.task_token,
    ))
    .unwrap();
}

#[test]
fn timer_immediate_cancel_workflow() {
    let task_q = "timer_cancel_workflow";
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let gateway_opts = ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "none".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
    };
    let core = temporal_sdk_core::init(CoreInitOptions { gateway_opts }).unwrap();
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None);
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_task(task_q).unwrap();
    core.complete_task(TaskCompletion::ok_from_api_attrs(
        vec![
            StartTimerCommandAttributes {
                timer_id: cancel_timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            CancelTimerCommandAttributes {
                timer_id: cancel_timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
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
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimerCommandAttributes {
                timer_id: "timer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                ..Default::default()
            }
            .into()],
            task.task_token,
        ))
        .unwrap();
        let task = task_chan.recv().unwrap();
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
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
        let task = core.poll_task(task_q).unwrap();
        send_chans
            .get(task.get_run_id().unwrap())
            .unwrap()
            .send(task)
            .unwrap();
    }

    handles.into_iter().for_each(|h| h.join().unwrap());
}
