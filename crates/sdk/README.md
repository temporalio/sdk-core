# Temporal Rust SDK

This crate contains a prerelease Rust SDK. The SDK is built on top of
Core and provides a native Rust experience for writing Temporal workflows and activities.

⚠️ **The SDK is under active development and should be considered prerelease.** The API can and
will continue to evolve.

## Quick Start

### Activities

Activities are defined using the `#[activities]` and `#[activity]` macros:

```rust
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

struct MyActivities {
    counter: AtomicUsize,
}

#[activities]
impl MyActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {}!", name))
    }

    // Activities can also use shared state via Arc<Self>
    #[activity]
    pub async fn increment(self: Arc<Self>, _ctx: ActivityContext) -> Result<u32, ActivityError> {
        Ok(self.counter.fetch_add(1, Ordering::Relaxed) as u32)
    }
}
```

### Workflows

Workflows are defined using the `#[workflow]` and `#[workflow_methods]` macros:

```rust
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};
use std::time::Duration;

#[workflow]
pub struct GreetingWorkflow {
    name: String,
}

#[workflow_methods]
impl GreetingWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, name: String) -> Self {
        Self { name }
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let name = ctx.state(|s| s.name.clone());

        // Execute an activity
        let greeting = ctx.start_activity(
            MyActivities::greet,
            name,
            ActivityOptions {
                start_to_close_timeout: Some(Duration::from_secs(10)),
                ..Default::default()
            }
        )?.await?;

        Ok(greeting)
    }
}
```

### Running a Worker

```rust
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = CoreRuntime::new_assume_tokio(RuntimeOptions::builder().build()?)?;

    let connection = Connection::connect(
        ConnectionOptions::new(Url::from_str("http://localhost:7233")?).build()
    ).await?;
    let client = Client::new(connection, ClientOptions::new("default").build());

    let worker_options = WorkerOptions::new("my-task-queue")
        .register_activities(MyActivities { counter: Default::default() })
        .register_workflow::<GreetingWorkflow>()
        .build();

    Worker::new(&runtime, client, worker_options)?.run().await?;
    Ok(())
}
```

## Workflows in detail

Workflows are the core abstraction in Temporal. They are defined as structs with associated methods:

- **`#[init]`** (optional) - Constructor that receives initial input
- **`#[run]`** (required) - Main workflow logic, must be async
- **`#[signal]`** - Handlers for external signals (sync or async)
- **`#[query]`** - Read-only handlers for querying workflow state (must be sync)
- **`#[update]`** - Handlers that can mutate state and return a result (sync or async)

`#[run]`, `#[signal]`, `#[query]`, and `#[update]` all accept an optional `name` parameter to specify the name of the method. If not specified, the name of the method will be used.

Sync signals and updates are able to mutate state directly. Async methods must mutate state through
the context with `state()` or `state_mut()`.

```rust
#[workflow]
pub struct MyWorkflow {
    values: Vec<u32>,
}

#[workflow_methods]
impl MyWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<u32>> {
        // Wait until we have at least 3 values
        ctx.wait_condition(|s| s.values.len() >= 3).await;
        Ok(ctx.state(|s| s.values.clone()))
    }

    #[signal(name = "add_value")]
    fn push_value(&mut self, _ctx: &mut WorkflowContext<Self>, value: u32) {
        self.values.push(value);
    }

    #[query]
    fn get_values(&self, _ctx: &WorkflowContextView) -> Vec<u32> {
        self.values.clone()
    }

    #[update]
    async fn add_wait_return(ctx: &mut WorkflowContext<Self>, value: u32) -> Vec<u32> {
        ctx.state_mut(|s| s.values.push(value));
        ctx.timer(Duration::from_secs(1)).await;
        ctx.state(|s| s.values.clone())
    }
}
```

### Workflow Logic Constraints

Workflow code must be deterministic. This means:

- No direct I/O operations (use activities instead)
- No threading or random number generation
- No access to system time (use `ctx.workflow_time()` instead)
- No global mutable state
- Future select! and join! should always used `biased` - we will provide first-class APIs in the
  future for these purposes. All interactions with futures should be done deterministically.

### Timers

```rust
// Wait for a duration
ctx.timer(Duration::from_secs(60)).await;
```

### Child Workflows

```rust
let child_opts = ChildWorkflowOptions {
    workflow_id: "child-1".to_string(),
    workflow_type: "ChildWorkflow".to_string(),
    ..Default::default()
};

let started = ctx.child_workflow(child_opts).start().await.into_started().unwrap();
let result = started.join().await?;
```

### Continue-As-New

```rust
// To continue as new, return an error with WorkflowTermination::ContinueAsNew
Err(WorkflowTermination::continue_as_new(ContinueAsNewWorkflowExecution {
    workflow_type: "MyWorkflow".to_string(),
    arguments: vec![new_input.into()],
    ..Default::default()
}))
```

### Patching (Versioning)

Use patching to safely evolve workflow logic:

```rust
if ctx.patched("my-patch-id") {
    // New code path
} else {
    // Old code path (for existing workflows)
}
```

### Nexus Operations

The SDK supports starting Nexus operations from a workflow:

```rust
let started = ctx.start_nexus_operation(NexusOperationOptions {
    endpoint: "my-endpoint".to_string(),
    service: "my-service".to_string(),
    operation: "my-operation".to_string(),
    input: Some(payload),
    ..Default::default()
}).await?;
```

Defining Nexus handlers will be added later.


## Activities in detail

Use Activities to perform side effects like I/O operations, API calls, or any non-deterministic
work.

### Error Handling

Activities return `Result<T, ActivityError>` with the following error types:

- **`ActivityError::Retryable`** - Transient failure, will be retried
- **`ActivityError::NonRetryable`** - Permanent failure, will not be retried
- **`ActivityError::Cancelled`** - Activity was cancelled
- **`ActivityError::WillCompleteAsync`** - Activity will complete asynchronously

### Local Activities

For short-lived activities that you want to run on the same worker as the workflow:

```rust
ctx.start_local_activity(
    MyActivities::quick_operation,
    input,
    LocalActivityOptions {
        schedule_to_close_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    }
)?.await?;
```

## Cancellation

Workflows and activities support cancellation. Note that in an activity, you must regularly
heartbeat with `ctx.record_heartbeat(...)` to receive cancellations.

```rust
// In a workflow: wait for cancellation
let reason = ctx.cancelled().await;

// Race a timer against cancellation
tokio::select! {
    biased;
    _ = ctx.timer(Duration::from_secs(60)) => { /* timer fired */ }
    reason = ctx.cancelled() => { /* workflow cancelled */ }
}
```

## Worker Configuration

Workers can be configured with various options:

```rust
let worker_options = WorkerOptions::new("task-queue")
    .max_cached_workflows(1000)           // Workflow cache size
    .workflow_task_poller_behavior(...)   // Polling configuration
    .activity_task_poller_behavior(...)
    .graceful_shutdown_period(Duration::from_secs(30))
    .register_activities(my_activities)
    .register_workflow::<MyWorkflow>()
    .build();
```

## Using the Client

The `temporalio_client` crate provides a client for interacting with the Temporal service. You can
use it to start workflows and communicate with running workflows via signals, queries, and updates,
among other operations.

### Connecting and Starting Workflows

```rust
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions,
    WorkflowClientTrait, WorkflowOptions, GetWorkflowResultOptions,
};
use temporalio_sdk_core::{Url, CoreRuntime, RuntimeOptions};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::connect(
        ConnectionOptions::new(Url::from_str("http://localhost:7233")?)
            .identity("my-client".to_string())
            .build()
    ).await?;

    // "default" is your namespace
    let client = Client::new(connection, ClientOptions::new("default").build());

    // Start a workflow
    let handle = client.start_workflow(
        GreetingWorkflow::run,
        "World".to_string(),
        WorkflowOptions::new("my-task-queue", "greeting-workflow-1").build()
    ).await?;

    // Wait for the result
    let result = handle.get_result(GetWorkflowResultOptions::default()).await?;

    Ok(())
}
```

### Signals, Queries, and Updates

Once you have a workflow handle, you can interact with the running workflow:

```rust
use temporalio_client::{
    SignalOptions, QueryOptions, UpdateOptions,
    StartUpdateOptions, WorkflowUpdateWaitStage,
    UntypedSignal,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};

// Get a handle to an existing workflow (or use one from start_workflow)
let handle = client.get_workflow_handle::<MyWorkflow>("workflow-id");

// --- Signals (fire-and-forget messages) ---
handle.signal(MyWorkflow::push_value, 42, SignalOptions::default()).await?;

// --- Queries (read workflow state) ---
let values = handle
    .query(MyWorkflow::get_values, (), QueryOptions::default())
    .await?;

// --- Updates (modify state and get a result) ---
let values = handle
    .execute_update(MyWorkflow::add_wait_return, 100, UpdateOptions::default())
    .await?;

// Start an update and wait for acceptance only
let update_handle = handle
    .start_update(
        MyWorkflow::add_wait_return,
        50,
        StartUpdateOptions::builder()
            .wait_for_stage(WorkflowUpdateWaitStage::Accepted)
            .build()
    )
    .await?;
update_handle.get_result().await?;

// --- Untyped interactions (when types aren't known at compile time) ---
let pc = PayloadConverter::serde_json();
handle
    .signal(
        UntypedSignal::new("increment"),
        RawValue::from_value(&25i32, &pc),
        SignalOptions::default(),
    )
    .await?;
// UntypedQuery and UntypedUpdate work similarly
```

### Cancelling and Terminating Workflows

```rust
use temporalio_client::{CancelWorkflowOptions, TerminateWorkflowOptions};

// Request cancellation (workflow can handle this gracefully)
handle.cancel(CancelWorkflowOptions::builder().reason("No longer needed").build()).await?;

// Terminate immediately (workflow cannot intercept this)
handle.terminate(TerminateWorkflowOptions::builder().reason("Emergency shutdown").build()).await?;
```

### Listing Workflows

```rust
use temporalio_client::ListWorkflowsOptions;
use futures_util::StreamExt;

let mut stream = client.list_workflows(
    "WorkflowType = 'GreetingWorkflow'",
    ListWorkflowsOptions::builder().limit(10).build()
);

while let Some(result) = stream.next().await {
    let execution = result?;
    println!("Workflow: {} ({})", execution.id(), execution.workflow_type());
}
```
