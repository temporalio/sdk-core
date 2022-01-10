use prost_wkt_build::{FileDescriptorSet, Message};
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptors.bin");

    tonic_build::configure()
        // We don't actually want to build the grpc definitions - we don't need them (for now).
        // Just build the message structs.
        .build_server(false)
        .build_client(true)
        // Serde serialization for all types. Although it's not supposed to happen, later calls
        // are overriding this so it needs to be specified additionally for later calls.
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
        // Make conversions easier for some types
        .type_attribute(
            "temporal.api.history.v1.HistoryEvent.attributes",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.history.v1.History",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.command.v1.Command.attributes",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.WorkflowType",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Header",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Memo",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.SignalExternalWorkflowExecutionFailedCause",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::Display)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.CancelExternalWorkflowExecutionFailedCause",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::Display)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.WorkflowCommand.variant",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From, ::derive_more::Display)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.QueryResult.variant",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.workflow_activation_job",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.WorkflowActivationJob.variant",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_completion.WorkflowActivationCompletion.status",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityExecutionResult.status",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityResolution.status",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_task.ActivityCancelReason",
            "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::Display)]",
        )
        .type_attribute("coresdk.Task.variant",
                        "#[derive(::serde::Serialize, ::serde::Deserialize, ::derive_more::From)]")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .file_descriptor_set_path(&descriptor_file)
        .compile(
            &[
                "../protos/local/temporal/sdk/core/core_interface.proto",
                "../protos/api_upstream/temporal/api/workflowservice/v1/service.proto",
            ],
            &["../protos/api_upstream", "../protos/local"],
        )?;

    let descriptor_bytes = std::fs::read(descriptor_file)?;
    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..])?;
    prost_wkt_build::add_serde(out, descriptor);

    Ok(())
}
