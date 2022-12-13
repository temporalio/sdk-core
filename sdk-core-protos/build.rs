fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../protos");
    tonic_build::configure()
        // We don't actually want to build the grpc definitions - we don't need them (for now).
        // Just build the message structs.
        .build_server(false)
        .build_client(true)
        // Make conversions easier for some types
        .type_attribute(
            "temporal.api.history.v1.HistoryEvent.attributes",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.history.v1.History",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.command.v1.Command.attributes",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.WorkflowType",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Header",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Memo",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.SignalExternalWorkflowExecutionFailedCause",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.CancelExternalWorkflowExecutionFailedCause",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.WorkflowCommand.variant",
            "#[derive(::derive_more::From, ::derive_more::Display)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.QueryResult.variant",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.workflow_activation_job",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.WorkflowActivationJob.variant",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_completion.WorkflowActivationCompletion.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityExecutionResult.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityResolution.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_task.ActivityCancelReason",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute("coresdk.Task.variant", "#[derive(::derive_more::From)]")
        // All external data is useful to be able to JSON serialize, so it can render in web UI
        .type_attribute(
            ".coresdk.external_data",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.complete_time",
            "#[serde(with = \"opt_timestamp\")]",
        )
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.original_schedule_time",
            "#[serde(with = \"opt_timestamp\")]",
        )
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.backoff",
            "#[serde(with = \"opt_duration\")]",
        )
        .compile(
            &[
                "../protos/local/temporal/sdk/core/core_interface.proto",
                "../protos/api_upstream/temporal/api/workflowservice/v1/service.proto",
                "../protos/api_upstream/temporal/api/operatorservice/v1/service.proto",
                "../protos/testsrv_upstream/temporal/api/testservice/v1/service.proto",
                "../protos/grpc/health/v1/health.proto",
            ],
            &[
                "../protos/api_upstream",
                "../protos/local",
                "../protos/testsrv_upstream",
                "../protos/grpc",
            ],
        )?;
    Ok(())
}
