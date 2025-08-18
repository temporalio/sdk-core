//! Shared tests that are meant to be run against both local dev server and cloud

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use temporal_sdk::WfContext;
use temporal_sdk_core_protos::temporal::api::{
    enums::v1::{EventType, WorkflowTaskFailedCause::GrpcMessageTooLarge},
    history::v1::history_event::Attributes::WorkflowTaskFailedEventAttributes,
};
use temporal_sdk_core_test_utils::CoreWfStarter;

pub(crate) mod priority;

pub(crate) async fn grpc_message_too_large() {
    let wf_name = "oversize_grpc_message";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();
    starter.worker_config.no_remote_activities(true);
    let mut core = starter.worker().await;

    static OVERSIZE_GRPC_MESSAGE_RUN: AtomicBool = AtomicBool::new(false);
    core.register_wf(wf_name.to_owned(), |_ctx: WfContext| async move {
        if OVERSIZE_GRPC_MESSAGE_RUN.load(Relaxed) {
            Ok(vec![].into())
        } else {
            OVERSIZE_GRPC_MESSAGE_RUN.store(true, Relaxed);
            let result: Vec<u8> = vec![0; 5000000];
            Ok(result.into())
        }
    });
    starter.start_with_worker(wf_name, &mut core).await;
    core.run_until_done().await.unwrap();

    assert!(starter.get_history().await.events.iter().any(|e| {
        e.event_type == EventType::WorkflowTaskFailed as i32
            && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
                attr.cause == GrpcMessageTooLarge as i32
                    && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
            } else {
                false
            }
    }))
}
