use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use futures_util::StreamExt;
use std::collections::HashMap;
use temporalio_client::{SignalWithStartOptions, WorkflowClientTrait, WorkflowOptions};
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::{
        IntoPayloadsExt,
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
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowOptions, Signal, SignalWorkflowOptions, WfContext,
    WorkflowResult,
};
use temporalio_sdk_core::test_help::MockPollCfg;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

async fn signal_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)
        .unwrap()
        .to_owned();
    let mut dat = SignalWorkflowOptions::new(RECEIVER_WFID, run_id, SIGNAME, [b"hi!"]);
    dat.with_header("tupac", b"shakur");
    let sigres = ctx.signal_workflow(dat).await;
    if ctx.get_args().get(1).is_some() {
        // We expect failure
        assert!(sigres.is_err());
    } else {
        sigres.unwrap();
    }
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    let wf_name = "sends_signal_to_missing_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), signal_sender);

    worker
        .submit_wf(
            wf_name,
            wf_name,
            vec![Uuid::new_v4().to_string().into(), [1].into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

async fn signal_receiver(ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    assert_eq!(&res.input, &[b"hi!".into()]);
    assert_eq!(
        *res.headers.get("tupac").expect("tupac header exists"),
        b"shakur".into()
    );
    Ok(().into())
}

async fn signal_with_create_wf_receiver(ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    assert_eq!(&res.input, &[b"tada".into()]);
    assert_eq!(
        *res.headers.get("tupac").expect("tupac header exists"),
        b"shakur".into()
    );
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_wf("sender", signal_sender);
    worker.register_wf("receiver", signal_receiver);

    let receiver_run_id = worker
        .submit_wf(
            RECEIVER_WFID,
            "receiver",
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "sends-signal-sender",
            "sender",
            vec![receiver_run_id.into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn sends_signal_with_create_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_with_create_wf");
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_wf("receiver_signal", signal_with_create_wf_receiver);

    let client = starter.get_client().await;
    let mut header: HashMap<String, Payload> = HashMap::new();
    header.insert("tupac".into(), "shakur".into());
    let options = SignalWithStartOptions::builder()
        .task_queue(worker.inner_mut().task_queue())
        .workflow_id("sends_signal_with_create_wf")
        .workflow_type("receiver_signal")
        .signal_name(SIGNAME)
        .maybe_signal_input(vec![b"tada".into()].into_payloads())
        .maybe_signal_header(Some(header.into()))
        .build();
    let res = client
        .signal_with_start_workflow_execution(options, WorkflowOptions::default())
        .await
        .expect("request succeeds.qed");

    worker.expect_workflow_completion("sends_signal_with_create_wf", Some(res.run_id));
    worker.run_until_done().await.unwrap();
}

async fn signals_child(ctx: WfContext) -> WorkflowResult<()> {
    let started_child = ctx
        .child_workflow(ChildWorkflowOptions {
            workflow_id: "my_precious_child".to_string(),
            workflow_type: "child_receiver".to_string(),
            ..Default::default()
        })
        .start(&ctx)
        .await
        .into_started()
        .expect("Must start ok");
    let mut sig = Signal::new(SIGNAME, [b"hi!"]);
    sig.data.with_header("tupac", b"shakur");
    started_child.signal(&ctx, sig).await.unwrap();
    started_child.result().await.status.unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_child() {
    let mut starter = CoreWfStarter::new("sends_signal_to_child");
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_wf("child_signaler", signals_child);
    worker.register_wf("child_receiver", signal_receiver);

    worker
        .submit_wf(
            "sends-signal-to-child",
            "child_signaler",
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

async fn signal_sender_canned(ctx: WfContext) -> WorkflowResult<()> {
    let mut dat = SignalWorkflowOptions::new("fake_wid", "fake_rid", SIGNAME, [b"hi!"]);
    dat.with_header("tupac", b"shakur");
    let res = ctx.signal_workflow(dat).await;
    if res.is_err() {
        Err(anyhow::anyhow!("Signal fail!"))
    } else {
        Ok(().into())
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
                        assert_eq!(attrs.input.as_ref().unwrap().payloads[0], b"hi!".into());
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
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, signal_sender_canned);
    worker.run().await.unwrap();
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
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        let sig = ctx.signal_workflow(SignalWorkflowOptions::new(
            "fake_wid",
            "fake_rid",
            SIGNAME,
            [b"hi!"],
        ));
        sig.cancel(&ctx);
        let _res = sig.await;
        Ok(().into())
    });
    worker.run().await.unwrap();
}
