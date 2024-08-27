use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

static ALWAYS_SERDE: &str = "#[cfg_attr(not(feature = \"serde_serialize\"), \
                               derive(::serde::Serialize, ::serde::Deserialize))]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=./protos");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptors.bin");
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
        .type_attribute(".coresdk.external_data", ALWAYS_SERDE)
        .type_attribute(
            ".",
            "#[cfg_attr(feature = \"serde_serialize\", derive(::serde::Serialize, ::serde::Deserialize))]",
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
        .extern_path(
            ".google.protobuf.Any",
            "::prost_wkt_types::Any"
        )
        .extern_path(
            ".google.protobuf.Timestamp",
            "::prost_wkt_types::Timestamp"
        )
        .extern_path(
            ".google.protobuf.Duration",
            "::prost_wkt_types::Duration"
        )
        .extern_path(
            ".google.protobuf.Value",
            "::prost_wkt_types::Value"
        )
        .file_descriptor_set_path(descriptor_file)
        .compile(
            &[
                "./protos/local/temporal/sdk/core/core_interface.proto",
                "./protos/api_upstream/temporal/api/workflowservice/v1/service.proto",
                "./protos/api_upstream/temporal/api/operatorservice/v1/service.proto",
                "./protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto",
                "./protos/testsrv_upstream/temporal/api/testservice/v1/service.proto",
                "./protos/grpc/health/v1/health.proto",
            ],
            &[
                "./protos/api_upstream",
                "./protos/api_cloud_upstream",
                "./protos/local",
                "./protos/testsrv_upstream",
                "./protos/grpc",
            ],
        )?;

    let file_path = out.join("temporal.api.common.v1.rs");
    post_process_payload_file(file_path)?;

    Ok(())
}

fn post_process_payload_file(file_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let file_contents: BufReader<File> = BufReader::new(File::open(file_path.clone())?);
    let mut new_file_contents = String::new();

    let mut found_payload = false;
    let mut written = false;
    for line in file_contents.lines() {
        let line = line?;
        if line.contains("pub struct Payload {") {
            new_file_contents.push_str("#[prost(skip_debug)]\n");
            found_payload = true;
        }

        // Write the current line to the new content
        new_file_contents.push_str(&line);
        new_file_contents.push('\n');

        if found_payload && line == "}" && !written {
            let debug_impl = r#"
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.data.len() > 64 {
            let mut windows = self.data.as_slice().windows(32);
            write!(
                f,
                "[{}..{}]",
                BASE64_STANDARD.encode(windows.next().unwrap_or_default()),
                BASE64_STANDARD.encode(windows.next_back().unwrap_or_default())
            )
        } else {
            write!(f, "[{}]", BASE64_STANDARD.encode(&self.data))
        }
    }
}
"#;
            new_file_contents.push_str(debug_impl);
            new_file_contents.push('\n');

            written = true;
        }
    }

    assert!(found_payload);

    // Replace the original file with the modified content
    std::fs::write(&file_path, new_file_contents)?;

    Ok(())
}
