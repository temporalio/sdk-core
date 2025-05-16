use futures_util::{FutureExt, StreamExt, sink, stream::FuturesUnordered};
use rand::{Rng, SeedableRng, prelude::Distribution, rngs::SmallRng};
use std::{future, time::Duration};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{
    ActContext, ActivityError, ActivityOptions, LocalActivityOptions, WfContext, WorkflowResult,
};
use temporal_sdk_core_protos::coresdk::{AsJsonPayloadExt, FromJsonPayloadExt, IntoPayloadsExt};
use temporal_sdk_core_test_utils::CoreWfStarter;
use tokio_util::sync::CancellationToken;

const FUZZY_SIG: &str = "fuzzy_sig";

#[derive(serde::Serialize, serde::Deserialize, Copy, Clone)]
enum FuzzyWfAction {
    Shutdown,
    DoAct,
    DoLocalAct,
}

struct FuzzyWfActionSampler;
impl Distribution<FuzzyWfAction> for FuzzyWfActionSampler {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> FuzzyWfAction {
        let v: u8 = rng.random_range(1..=2);
        match v {
            1 => FuzzyWfAction::DoAct,
            2 => FuzzyWfAction::DoLocalAct,
            _ => unreachable!(),
        }
    }
}

async fn echo(_ctx: ActContext, echo_me: String) -> Result<String, ActivityError> {
    Ok(echo_me)
}

async fn fuzzy_wf_def(ctx: WfContext) -> WorkflowResult<()> {
    let sigchan = ctx
        .make_signal_channel(FUZZY_SIG)
        .map(|sd| FuzzyWfAction::from_json_payload(&sd.input[0]).expect("Can deserialize signal"));
    let done = CancellationToken::new();
    let done_setter = done.clone();

    sigchan
        .take_until(done.cancelled())
        .for_each_concurrent(None, |action| match action {
            FuzzyWfAction::DoAct => ctx
                .activity(ActivityOptions {
                    activity_type: "echo_activity".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .map(|_| ())
                .boxed(),
            FuzzyWfAction::DoLocalAct => ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo_activity".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .map(|_| ())
                .boxed(),
            FuzzyWfAction::Shutdown => {
                done_setter.cancel();
                future::ready(()).boxed()
            }
        })
        .await;

    Ok(().into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzzy_workflow() {
    let num_workflows = 200;
    let wf_name = "fuzzy_wf";
    let mut starter = CoreWfStarter::new("fuzzy_workflow");
    starter
        .worker_config
        .max_outstanding_workflow_tasks(25_usize)
        .max_cached_workflows(25_usize)
        .max_outstanding_activities(25_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), fuzzy_wf_def);
    worker.register_activity("echo_activity", echo);
    let client = starter.get_client().await;

    let mut workflow_handles = vec![];
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}");
        let rid = worker
            .submit_wf(
                wfid.clone(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
    }

    let rng = SmallRng::seed_from_u64(523189);
    let mut actions: Vec<FuzzyWfAction> = rng.sample_iter(FuzzyWfActionSampler).take(15).collect();
    actions.push(FuzzyWfAction::Shutdown);

    let sig_sender = async {
        for action in actions {
            let sends: FuturesUnordered<_> = (0..num_workflows)
                .map(|i| {
                    client.signal_workflow_execution(
                        format!("{wf_name}_{i}"),
                        "".to_string(),
                        FUZZY_SIG.to_string(),
                        [action.as_json_payload().expect("Serializes ok")].into_payloads(),
                        None,
                    )
                })
                .collect();
            sends
                .map(|_| Ok(()))
                .forward(sink::drain())
                .await
                .expect("Sending signals works");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let (r1, _) = tokio::join!(worker.run_until_done(), sig_sender);
    r1.unwrap();
}
