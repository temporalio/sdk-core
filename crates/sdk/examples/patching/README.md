# Patching

This sample demonstrates workflow versioning with `ctx.patched()`. When deploying a change to workflow logic, `patched()` lets new executions take the updated code path while existing executions replay the original path, preserving compatibility.

### Running this sample

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the worker:

       cargo run --features examples --example patching-worker

3. In another terminal, run the workflow:

       cargo run --features examples --example patching-starter

The starter should print:

    Workflow result: Hello, Temporal! Welcome to Temporal.
