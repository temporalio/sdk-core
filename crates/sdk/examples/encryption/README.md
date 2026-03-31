# Encryption

This sample demonstrates a custom `PayloadCodec` that encrypts workflow payloads in transit.

A simple XOR-based codec is used to keep dependencies minimal. In production, use a real encryption library (e.g., AES-GCM via `ring` or `aes-gcm`). The codec is wired into a custom `DataConverter` that both the worker and starter use.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

       cargo run --features examples --example encryption-worker

3. In another terminal, run the workflow:

       cargo run --features examples --example encryption-starter

The starter should print:

    Workflow result: Hello, Temporal!

Payloads in the Temporal UI will appear encrypted rather than as plain JSON.
