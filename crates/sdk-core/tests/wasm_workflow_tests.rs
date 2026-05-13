//! Tests that exercise the WASM workflow execution path. These are kept in a separate test binary
//! because they require `cargo component` and the `wasm32-unknown-unknown` target to build the
//! sample components, which not every CI environment has installed.

#[allow(dead_code)]
mod common;

use crate::common::CoreWfStarter;
use std::{path::PathBuf, time::Duration};
use temporalio_client::{UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    worker::WorkerTaskTypes,
};
use temporalio_sdk::WasmWorkflowComponent;
use tokio::process::Command;
use wasmparser::{Operator, Parser, Payload};

const WASM_COMPONENT_ID: &str = "hello-workflow-component";
const WASM_WORKFLOW_TYPE: &str = "HelloWorkflow";
const WASM_TIMER_WORKFLOW_TYPE: &str = "TimerWorkflow";
const WASM_FUNCREF_WORKFLOW_TYPE: &str = "FuncrefWorkflow";

#[tokio::test]
async fn wasm_workflow_component_executes() {
    let component_path = build_wasm_hello_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    run_hello_workflow("wasm_workflow_component_executes", component).await;
}

// Mirrors `wasm_workflow_component_executes` but loads the component bytes into memory and
// registers via `from_bytes`, exercising the dynamic-blob loading path that callers will use
// for runtime-supplied components (e.g. fetched over the network rather than read from disk).
#[tokio::test]
async fn wasm_workflow_component_executes_from_bytes() {
    let component_path = build_wasm_hello_component().await;
    let bytes = tokio::fs::read(&component_path)
        .await
        .expect("WASM component file should be readable");
    let component = WasmWorkflowComponent::from_bytes(WASM_COMPONENT_ID, bytes)
        .expect("WASM component bytes should be loadable");
    run_hello_workflow("wasm_workflow_component_executes_from_bytes", component).await;
}

#[tokio::test]
async fn wasm_workflow_snapshot_rehydrates_after_timer() {
    let component_path = build_wasm_hello_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    run_timer_based_workflow(
        "wasm_workflow_snapshot_rehydrates_after_timer",
        component,
        WASM_TIMER_WORKFLOW_TYPE,
        "Timer fired for workflow!",
        true,
    )
    .await;
}

#[tokio::test]
async fn wasm_workflow_snapshot_rehydrates_funcref_across_timer() {
    let component_path = build_wasm_hello_component().await;
    assert_component_contains_funcref_table_mutation(&component_path);
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    run_timer_based_workflow(
        "wasm_workflow_snapshot_rehydrates_funcref_across_timer",
        component,
        WASM_FUNCREF_WORKFLOW_TYPE,
        "Funcref restored for workflow! table=22->22",
        true,
    )
    .await;
}

#[tokio::test]
async fn wasm_workflow_timer_executes_without_snapshots() {
    let component_path = build_wasm_hello_component().await;
    let component = WasmWorkflowComponent::from_file(WASM_COMPONENT_ID, component_path)
        .expect("sample WASM component should be loadable");
    run_timer_based_workflow(
        "wasm_workflow_timer_executes_without_snapshots",
        component,
        WASM_TIMER_WORKFLOW_TYPE,
        "Timer fired for workflow!",
        false,
    )
    .await;
}

async fn run_timer_based_workflow(
    test_name: &'static str,
    component: WasmWorkflowComponent,
    workflow_type: &'static str,
    expected_result: &'static str,
    enable_snapshots: bool,
) {
    let mut starter = CoreWfStarter::new(test_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.max_cached_workflows = 1;
    starter.sdk_config.register_wasm_workflow(component);
    if enable_snapshots {
        starter.sdk_config.enable_wasm_workflow_snapshots();
    }

    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let submitter = worker.get_submitter_handle();
    let shutdown_handle = worker.inner_mut().shutdown_handle();
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&"workflow", &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();

    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id.clone()).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    let client_task = async {
        submitter
            .submit_wf(workflow_type, input.payloads, start_options)
            .await
            .expect("WASM workflow should start");
        let result = client
            .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
            .get_result(Default::default())
            .await;
        shutdown_handle();
        let result = result.expect("WASM workflow result should be available");
        let greeting: String = result.to_value(&payload_converter);
        assert_eq!(greeting, expected_result);
    };
    tokio::join!(
        async {
            worker
                .inner_mut()
                .run()
                .await
                .expect("WASM worker should run");
        },
        client_task
    );
}

async fn run_hello_workflow(test_name: &'static str, component: WasmWorkflowComponent) {
    let mut starter = CoreWfStarter::new(test_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.register_wasm_workflow(component);

    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let payload_converter = PayloadConverter::default();
    let input = RawValue::from_value(&"workflow", &payload_converter);
    let workflow_id = starter.get_wf_id().to_owned();

    let mut start_options =
        WorkflowStartOptions::new(starter.get_task_queue().to_owned(), workflow_id.clone()).build();
    start_options.execution_timeout = Some(Duration::from_secs(60));
    worker
        .submit_wf(WASM_WORKFLOW_TYPE, input.payloads, start_options)
        .await
        .expect("WASM workflow should start");
    worker
        .run_until_done()
        .await
        .expect("WASM workflow should complete");

    let result = client
        .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
        .get_result(Default::default())
        .await
        .expect("WASM workflow result should be available");
    let greeting: String = result.to_value(&payload_converter);
    assert_eq!(greeting, "Hello, workflow!");
}

fn assert_component_contains_funcref_table_mutation(component_path: &PathBuf) {
    let bytes = std::fs::read(component_path).expect("WASM component should be readable");
    let mut table_get = 0;
    let mut table_set = 0;
    let mut call_indirect = 0;
    for payload in Parser::new(0).parse_all(&bytes) {
        if let Payload::CodeSectionEntry(body) =
            payload.expect("WASM component should parse for instruction verification")
        {
            let mut operators = body
                .get_operators_reader()
                .expect("WASM function body should parse");
            while !operators.eof() {
                match operators
                    .read()
                    .expect("WASM operator should parse for instruction verification")
                {
                    Operator::TableGet { .. } => table_get += 1,
                    Operator::TableSet { .. } => table_set += 1,
                    Operator::CallIndirect { .. } => call_indirect += 1,
                    _ => {}
                }
            }
        }
    }

    assert!(
        table_get > 0 && table_set > 0 && call_indirect > 0,
        "WASM component should contain dynamic funcref table mutation instructions; found table.get={table_get}, table.set={table_set}, call_indirect={call_indirect}"
    );
}

async fn build_wasm_hello_component() -> PathBuf {
    let sample_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("sdk-core crate should live under crates/")
        .join("samples/wasm-workflows/hello");
    let output = Command::new(env!("CARGO"))
        .args([
            "component",
            "build",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .current_dir(&sample_dir)
        .output()
        .await
        .expect("cargo component should be runnable");

    assert!(
        output.status.success(),
        "cargo component build --release --target wasm32-unknown-unknown failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let component_path =
        sample_dir.join("target/wasm32-unknown-unknown/release/temporal_wasm_hello_workflow.wasm");
    assert!(
        component_path.exists(),
        "cargo component did not create {}",
        component_path.display()
    );
    component_path
}
