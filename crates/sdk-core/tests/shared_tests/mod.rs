//! Shared tests that are meant to be run against both local dev server and cloud

use crate::common::CoreWfStarter;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use temporalio_common::{
    protos::temporal::api::{
        enums::v1::{EventType, WorkflowTaskFailedCause::GrpcMessageTooLarge},
        history::v1::history_event::Attributes::{
            WorkflowExecutionTerminatedEventAttributes, WorkflowTaskFailedEventAttributes,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::WfContext;

pub(crate) mod priority;

pub(crate) async fn grpc_message_too_large() {
    let wf_name = "oversize_grpc_message";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
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

    let events = starter.get_history().await.events;
    // Depending on the version of server, it may terminate the workflow, or simply be a task
    // failure
    assert!(
        events.iter().any(|e| {
            // Task failure
            e.event_type == EventType::WorkflowTaskFailed as i32
                && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
                    attr.cause == GrpcMessageTooLarge as i32
                        && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
                } else {
                    false
                }
            // Workflow terminated
            ||
            e.event_type == EventType::WorkflowExecutionTerminated as i32
                && if let WorkflowExecutionTerminatedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
                    attr.reason == "GrpcMessageTooLarge"
                } else {
                    false
                }
        }),
        "Expected workflow task failure or termination b/c grpc message too large: {events:?}",
    );
}
