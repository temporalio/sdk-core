use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use std::collections::HashMap;
use temporalio_client::{WorkflowClientTrait, WorkflowStartOptions, WorkflowStartSignal};
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::{
        AsJsonPayloadExt, IntoPayloadsExt,
        workflow_activation::{
            ResolveSignalExternalWorkflow, WorkflowActivationJob, workflow_activation_job,
        },
    },
    temporal::api::{
        command::v1::{Command, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
    },
};

use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowOptions, Signal, SignalWorkflowOptions, SyncWorkflowContext,
    WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::test_help::MockPollCfg;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

#[workflow]
#[derive(Default)]
struct SignalSender;

#[workflow_methods]
impl SignalSender {
    #[run(name = "sender")]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (run_id, expect_failure): (String, bool),
    ) -> WorkflowResult<()> {
        let mut dat = SignalWorkflowOptions::new(
            RECEIVER_WFID,
            run_id,
            SIGNAME,
            ["hi!".to_string().as_json_payload().unwrap()],
        );
        dat.with_header("tupac", b"shakur");
        let sigres = ctx.signal_workflow(dat).await;
        if expect_failure {
            assert!(sigres.is_err());
        } else {
            sigres.unwrap();
        }
        Ok(())
    }
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    let wf_name = "sends_signal_to_missing_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalSender>();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            SignalSender::run,
            (Uuid::new_v4().to_string(), true),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalReceiver {
    received: bool,
}

#[workflow_methods]
impl SignalReceiver {
    #[run(name = "receiver")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal(name = "signame")]
    fn handle_signal(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "hi!");
        let headers = ctx.headers();
        assert_eq!(
            *headers.get("tupac").expect("tupac header exists"),
            b"shakur".into()
        );
        self.received = true;
    }
}

#[workflow]
#[derive(Default)]
struct SignalWithCreateWfReceiver {
    received: bool,
}

#[workflow_methods]
impl SignalWithCreateWfReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal(name = "signame")]
    fn handle_signal(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "tada");
        let headers = ctx.headers();
        assert_eq!(
            *headers.get("tupac").expect("tupac header exists"),
            b"shakur".into()
        );
        self.received = true;
    }
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalSender>();
    worker.register_workflow::<SignalReceiver>();

    let task_queue = starter.get_task_queue().to_owned();
    let receiver_run_id = worker
        .submit_wf(
            "receiver",
            vec![().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue.clone(), RECEIVER_WFID).build(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "sender",
            vec![(receiver_run_id, false).as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, "sends-signal-sender").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn sends_signal_with_create_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_with_create_wf");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalWithCreateWfReceiver>();

    let client = starter.get_client().await;
    let mut header: HashMap<String, Payload> = HashMap::new();
    header.insert("tupac".into(), "shakur".into());
    let task_queue = worker.inner_mut().task_queue().to_string();
    let start_signal = WorkflowStartSignal::new(SIGNAME)
        .maybe_input(vec!["tada".to_string().as_json_payload().unwrap()].into_payloads())
        .maybe_header(Some(header.into()))
        .build();
    let options = WorkflowStartOptions::new(task_queue, "sends_signal_with_create_wf")
        .start_signal(start_signal)
        .build();
    let handle = client
        .start_workflow(SignalWithCreateWfReceiver::run, (), options)
        .await
        .expect("request succeeds.qed");

    worker.expect_workflow_completion(
        "sends_signal_with_create_wf",
        Some(handle.run_id().unwrap().to_string()),
    );
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalsChild;

#[workflow_methods]
impl SignalsChild {
    #[run(name = "child_signaler")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started_child = ctx
            .child_workflow(ChildWorkflowOptions {
                workflow_id: "my_precious_child".to_string(),
                workflow_type: "receiver".to_string(),
                input: vec![().as_json_payload().unwrap()],
                ..Default::default()
            })
            .start()
            .await
            .into_started()
            .expect("Must start ok");
        let mut sig = Signal::new(SIGNAME, ["hi!".to_string().as_json_payload().unwrap()]);
        sig.data.with_header("tupac", b"shakur");
        started_child.signal(sig).await.unwrap();
        started_child.result().await.status.unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn sends_signal_to_child() {
    let mut starter = CoreWfStarter::new("sends_signal_to_child");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalsChild>();
    worker.register_workflow::<SignalReceiver>();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            "child_signaler",
            vec![().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, "sends-signal-to-child").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalSenderCanned;

#[workflow_methods]
impl SignalSenderCanned {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let mut dat = SignalWorkflowOptions::new(
            "fake_wid",
            "fake_rid",
            SIGNAME,
            ["hi!".to_string().as_json_payload().unwrap()],
        );
        dat.with_header("tupac", b"shakur");
        let res = ctx.signal_workflow(dat).await;
        if res.is_err() {
            Err(anyhow::anyhow!("Signal fail!").into())
        } else {
            Ok(())
        }
    }
}

#[rstest::rstest]
#[case::succeeds(false)]
#[case::fails(true)]
#[tokio::test]
async fn sends_signal(#[case] fails: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let id = t.add_signal_wf(SIGNAME, "fake_wid", "fake_rid");
    if fails {
        t.add_external_signal_failed(id);
    } else {
        t.add_external_signal_completed(id);
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                assert_matches!(wft.commands.as_slice(),
                    [Command { attributes: Some(
                        command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs)),..}] => {
                        assert_eq!(attrs.signal_name, SIGNAME);
                        assert_eq!(attrs.input.as_ref().unwrap().payloads[0], "hi!".to_string().as_json_payload().unwrap());
                        assert_eq!(*attrs.header.as_ref().unwrap().fields.get("tupac").unwrap(),
                                   b"shakur".into());
                    }
                );
            }).then(move |wft| {
                let cmds = &wft.commands;
                assert_eq!(cmds.len(), 1);
                if fails {
                    assert_eq!(cmds[0].command_type(), CommandType::FailWorkflowExecution);
                } else {
                    assert_eq!(
                        cmds[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
        });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<SignalSenderCanned>();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelsBeforeSending;

#[workflow_methods]
impl CancelsBeforeSending {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let sig = ctx.signal_workflow(SignalWorkflowOptions::new(
            "fake_wid",
            "fake_rid",
            SIGNAME,
            ["hi!".to_string().as_json_payload().unwrap()],
        ));
        sig.cancel();
        let _res = sig.await;
        Ok(())
    }
}

#[tokio::test]
async fn cancels_before_sending() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one().then(move |act| {
        assert_matches!(
            &act.jobs[0],
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveSignalExternalWorkflow(
                    ResolveSignalExternalWorkflow {
                        failure: Some(c),
                        ..
                    }
                ))
            } => c.message == "Signal was cancelled before being sent"
        );
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 1);
            assert_eq!(
                wft.commands[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    worker.register_workflow::<CancelsBeforeSending>();
    worker.run().await.unwrap();
}
