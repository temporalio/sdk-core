fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // We don't actually want to build the grpc definitions - we don't need them (for now).
        // Just build the message structs.
        .build_server(false)
        .build_client(false)
        .out_dir("src/protos")
        // Make conversions easier for some types
        .type_attribute(
            "temporal.api.history.v1.HistoryEvent.attributes",
            "#[derive(::derive_more::From)]",
        )
        .compile(
            &["protos/local/core_interface.proto"],
            &["protos/api_upstream", "protos/local"],
        )?;
    Ok(())
}
