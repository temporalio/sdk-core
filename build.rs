fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // We don't actually want to build the grpc definitions - we don't need them (for now).
        // Just build the message structs.
        .build_server(false)
        .build_client(false)
        .compile(
            &["protos/core_interface.proto"],
            &["protos/api_upstream", "protos"],
        )?;
    Ok(())
}