# Temporal Rust SDK Examples

These examples demonstrate common Temporal patterns using the Rust SDK. Each example is a self-contained directory with:

- `workflows.rs` — Workflow and activity definitions
- `worker.rs` — Registers workflows/activities and runs a worker
- `starter.rs` — Starts a workflow and prints the result

## Prerequisites

A running Temporal server. The quickest way to get one locally:

```bash
temporal server start-dev
```

By default, examples connect to `http://localhost:7233` in the `default` namespace.
Override with `TEMPORAL_SERVICE_ADDRESS` and `TEMPORAL_NAMESPACE` environment variables.

## Running an Example

From the `crates/sdk/` directory, each example has a worker and a starter. Run the worker first, then the starter in a separate terminal:

```bash
# Terminal 1: start the worker
cargo run --features examples --example hello-world-worker

# Terminal 2: start the workflow
cargo run --features examples --example hello-world-starter
```

## Examples

| Example | Description |
|---|---|
| `hello_world` | Basic workflow that calls a single activity |
| `activity_heartbeating` | Long-running activity with heartbeating and resume-on-retry |
| `timer_examples` | Workflow timers, racing timers against activities, and timer cancellation |
| `message_passing` | Signals, queries, and updates on a workflow |
| `child_workflows` | Starting and collecting results from child workflows |
| `continue_as_new` | Long-running workflows via continue-as-new |
| `saga` | Saga/compensation pattern for distributed transactions |
| `patching` | Workflow versioning with `ctx.patched()` |
| `local_activities` | Local vs remote activity execution |
| `search_attributes` | Reading and upserting workflow search attributes |
| `updatable_timer` | Timer that can be rescheduled via signals |
| `polling` | Polling an external condition with activities and timers |
| `cancellation` | Workflow and activity cancellation with cleanup |
| `encryption` | Custom `PayloadCodec` for payload encryption |
| `schedules` | Creating and managing scheduled workflows |
