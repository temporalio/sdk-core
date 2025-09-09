//! Integration tests

#[macro_use]
extern crate rstest;
#[macro_use]
extern crate assert_matches;

mod common;

#[cfg(test)]
mod shared_tests;

#[cfg(test)]
mod integ_tests {
    mod activity_functions;
    mod client_tests;
    mod ephemeral_server_tests;
    mod heartbeat_tests;
    mod metrics_tests;
    mod pagination_tests;
    mod polling_tests;
    mod queries_tests;
    mod update_tests;
    mod visibility_tests;
    mod worker_tests;
    mod worker_versioning_tests;
    mod workflow_tests;

    use crate::common::{
        CoreWfStarter, get_integ_server_options, get_integ_telem_options, rand_6_chars,
    };
    use std::time::Duration;
    use temporal_client::{ClientOptionsBuilder, NamespacedClient, WorkflowService};
    use temporal_sdk_core::{CoreRuntime, init_worker};
    use temporal_sdk_core_api::{telemetry::TelemetryOptions, worker::WorkerConfigBuilder};
    use temporal_sdk_core_protos::temporal::api::{
        nexus::v1::{EndpointSpec, EndpointTarget, endpoint_target},
        operatorservice::v1::CreateNexusEndpointRequest,
        workflowservice::v1::ListNamespacesRequest,
    };
    use tonic::IntoRequest;

    // Create a worker like a bridge would (unwraps aside)
    #[tokio::test]
    #[ignore] // Really a compile time check more than anything
    async fn lang_bridge_example() {
        let opts = get_integ_server_options();
        let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
        let mut retrying_client = opts
            .connect_no_namespace(runtime.telemetry().get_temporal_metric_meter())
            .await
            .unwrap();

        let _worker = init_worker(
            &runtime,
            WorkerConfigBuilder::default()
                .namespace("default")
                .task_queue("Wheee!")
                .build()
                .unwrap(),
            // clone the client if you intend to use it later. Strip off the retry wrapper since
            // worker will assert its own
            retrying_client.clone(),
        );

        // Do things with worker or client
        let _ = retrying_client
            .list_namespaces(ListNamespacesRequest::default().into_request())
            .await;
    }

    pub(crate) async fn mk_nexus_endpoint(starter: &mut CoreWfStarter) -> String {
        let client = starter.get_client().await;
        let endpoint = format!("mycoolendpoint-{}", rand_6_chars());
        let mut op_client = client.get_client().inner().operator_svc();
        op_client
            .create_nexus_endpoint(
                CreateNexusEndpointRequest {
                    spec: Some(EndpointSpec {
                        name: endpoint.to_owned(),
                        description: None,
                        target: Some(EndpointTarget {
                            variant: Some(endpoint_target::Variant::Worker(
                                endpoint_target::Worker {
                                    namespace: client.namespace(),
                                    task_queue: starter.get_task_queue().to_owned(),
                                },
                            )),
                        }),
                    }),
                }
                .into_request(),
            )
            .await
            .unwrap();
        // Endpoint creation can (as of server 1.25.2 at least) return before they are actually usable.
        tokio::time::sleep(Duration::from_millis(800)).await;
        endpoint
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CoreRuntime::new_assume_tokio(TelemetryOptions::default())?;

        let client_options = ClientOptionsBuilder::default()
            .target_url("localhost:7233")
            .identity("my-client")
            .build()?;
        let client = client_options
            .connect("my-namespace", runtime.telemetry().get_metric_meter())
            .await?;

        let mut worker_config = WorkerConfigBuilder::default()
            .task_queue("my-task-q")
            .activities([MyActivity])
            .workflows([MyWorkflow])
            .build()?;
        let worker = Worker::new("my-task-queue");

        // Spawned off so we can continue to do stuff. This glosses over joining this eventually
        // and some other error handling the user would probably want to do.
        tokio::spawn(worker.run());

        let handle = client
            .start_workflow::<MyWorkflow>("hi", WorkflowOptions::new("my-wf-id", "my-task-q"))
            .await?;
        handle.signal(MyWorkflow::signal).await?;
        handle.get_result().await?;
        Ok(())
    }
}
