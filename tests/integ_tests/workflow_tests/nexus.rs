use temporal_sdk::WfContext;
use temporal_sdk_core_test_utils::CoreWfStarter;

#[tokio::test]
async fn nexus_basic() {
    let wf_name = "nexus_basic";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        Ok(().into())
    });
    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}
