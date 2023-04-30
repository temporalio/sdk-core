//! Abstractions based on SDK [crate] for defining workflow and activity definitions in idiomatic Async Rust
//!
//! This module has a large API surface mainly due to Rust's lack of [reflection](https://en.wikipedia.org/wiki/Reflective_programming)
//! capability and variadic arguments/generics on which other language SDKs depend on. Once the API is good enough, the variations can be hidden behind macros.
//!
//! Even though we can just use 'tuples' as arguments for all the functions for easy implementation,
//! the goal is to support general enough Workflow and Activity functions to the most extent possible but only with some limitations on arguments and results
//! as required by [temporal_sdk_core] and [Workflow definitions](https://docs.temporal.io/workflows#workflow-definition) and in general by Temporal platform.
//!
//! There are two main types of high-level functions. Other convenience facilities and possibly macros will be included in the future.
//! 1) Register functions (Example: [into_activity_1_args_exit_value], [into_workflow_0_args]) to register Activity/Workflow functions.
//! 2) Command functions (Example: [execute_activity_1_args], [execute_child_workflow_2_args_exit_value]) for workflow [commands](https://docs.temporal.io/workflows#command).
//!
//! These functions vary in which type of user-defined Activity/Workflow functions they accept.
//! Generic traits representing the user-defined functions are defined in the module. For example [AsyncFn2] is a function which takes two arguments.
//!
//! Currently, Activity/Workflow Functions with below arguments and result variations are supported.
//!
//! Argument variations
//! 1) Activity Functions which take [ActContext] and zero or more arguments
//! 2) Workflow Functions which take [WfContext] and zero or more arguments
//! 3) Activity Functions which take zero or more arguments. These are for simple Activity implementations which do not rely on Activity Context (Cancellation, Heartbeats etc.)
//!
//! Result variations
//! 1) Returning [ActExitValue]/[WfExitValue] and [anyhow::Error].
//! 2) Returning [ActExitValue]/[WfExitValue] and user-defined error types.<br/>
//!    Define Activity/Workflow Functions as these if you want to return typed errors(Ex: errors defined using [thiserror](https://docs.rs/thiserror/latest/thiserror/)).
//!    These require an implementation of [FromFailureExt] to map [Failure](temporal_sdk_core_protos::temporal::api::failure::v1::Failure) to the user-defined error types in addition to Activity/Workflow definitions.
//! 3) Returning just the user-defined result and [anyhow::Error]
//! 4) Returning just the user-defined result and error types.(Same requirements as type 2 above)
//!
//! Note: Any type `T` which implements [Debug] has generic implementation of [`Into<ActExitValue<T>>`] and [`Into<WfExitValue<T>>`]
//!
//! Defining workflows should feel like just a normal Rust(Async) program. Use [Prelude](crate::prelude) for easy import of types needed.
//!
//!
//! An example workflow definition is below. For more, refer to tests and examples.
//! ```no_run
//! use temporal_sdk::prelude::registry::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut worker = worker::worker().await.unwrap();
//!
//!     worker.register_activity(
//!         "sdk_example_activity",
//!         into_activity_1_args_with_errors(activity::sdk_example_activity),
//!     );
//!
//!     worker.register_wf(
//!         "sdk_example_workflow",
//!         into_workflow_1_args(workflow::sdk_example_workflow),
//!     );
//!
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//!
//! mod worker {
//!     use std::{str::FromStr, sync::Arc};
//!     use temporal_sdk::{sdk_client_options, Worker};
//!     use temporal_sdk_core::{init_worker, CoreRuntime, Url};
//!     use temporal_sdk_core_api::{telemetry::TelemetryOptionsBuilder, worker::WorkerConfigBuilder};
//!     pub(crate) async fn worker() -> Result<Worker, Box<dyn std::error::Error>> {
//!         let server_options = sdk_client_options(Url::from_str("http://!localhost:7233")?).build()?;
//!
//!         let client = server_options.connect("default", None, None).await?;
//!
//!         let telemetry_options = TelemetryOptionsBuilder::default().build()?;
//!         let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;
//!
//!         let worker_config = WorkerConfigBuilder::default()
//!             .namespace("default")
//!             .task_queue("task_queue")
//!             .build()?;
//!
//!         let core_worker = init_worker(&runtime, worker_config, client)?;
//!
//!         Ok(Worker::new_from_core(Arc::new(core_worker), "task_queue"))
//!     }
//! }
//!
//! mod activity {
//!     use temporal_sdk::prelude::activity::*;
//!
//!     #[derive(Debug, thiserror::Error)]
//!     #[non_exhaustive]
//!     pub enum Error {
//!         #[error(transparent)]
//!         Io(#[from] std::io::Error),
//!         #[error(transparent)]
//!         Any(anyhow::Error),
//!     }
//!
//!     impl FromFailureExt for Error {
//!         fn from_failure(failure: Failure) -> Error {
//!             Error::Any(anyhow::anyhow!("{:?}", failure.message))
//!         }
//!     }
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct ActivityInput {
//!         pub activity: String,
//!         pub workflow: String,
//!     }
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct ActivityOutput {
//!         pub language: String,
//!         pub platform: String,
//!     }
//!
//!     pub async fn sdk_example_activity(
//!         _ctx: ActContext,
//!         input: ActivityInput,
//!     ) -> Result<(String, ActivityOutput), Error> {
//!         Ok((
//!             format!("{}-{}", input.activity, input.workflow),
//!             ActivityOutput {
//!                 language: "rust".to_string(),
//!                 platform: "temporal".to_string(),
//!             },
//!         ))
//!     }
//! }
//!
//! mod workflow {
//!     use super::activity::*;
//!     use temporal_sdk::prelude::workflow::*;
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct WorkflowInput {
//!         pub activity: String,
//!         pub workflow: String,
//!     }
//!
//!     pub async fn sdk_example_workflow(
//!         ctx: WfContext,
//!         input: WorkflowInput,
//!     ) -> Result<WfExitValue<ActivityOutput>, anyhow::Error> {
//!         let activity_timeout = Duration::from_secs(5);
//!         let output = execute_activity_1_args_with_errors(
//!             &ctx,
//!             ActivityOptions {
//!                 activity_id: Some("sdk_example_activity".to_string()),
//!                 activity_type: "sdk_example_activity".to_string(),
//!                 schedule_to_close_timeout: Some(activity_timeout),
//!                 ..Default::default()
//!             },
//!             sdk_example_activity,
//!             ActivityInput {
//!                 activity: input.activity,
//!                 workflow: input.workflow,
//!             },
//!         )
//!         .await;
//!         match output {
//!             Ok(output) => Ok(WfExitValue::Normal(output.1)),
//!             Err(e) => Err(anyhow::Error::from(e)),
//!         }
//!     }
//! }
//! ```

// TODO: Remove this after complete implementation
#![allow(unused_imports)]

// TODO: Create ActivityOptions, ChildOptions type and builders for workflow abstractions

use crate::{
    ActContext, ActExitValue, ActivityOptions, ChildWorkflowOptions, WfContext, WfExitValue,
};
use anyhow::anyhow;
use futures::FutureExt;
use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, future::Future};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{self, activity_resolution},
        child_workflow::{self, child_workflow_result},
        workflow_commands::ActivityCancellationType,
        AsJsonPayloadExt, FromFailureExt, FromJsonPayloadExt,
    },
    temporal::api::common::v1::Payload,
};

/// Trait to represent an async function with 0 arguments
pub trait AsyncFn0: Fn() -> Self::OutputFuture {
    /// Output type of the async function which implements serde traits
    type Output;
    /// Future of the output
    type OutputFuture: Future<Output = <Self as AsyncFn0>::Output> + Send + 'static;
}

impl<F: ?Sized, Fut> AsyncFn0 for F
where
    F: Fn() -> Fut,
    Fut: Future + Send + 'static,
{
    type Output = Fut::Output;
    type OutputFuture = Fut;
}

/// Trait to represent an async function with 1 argument
pub trait AsyncFn1<Arg0>: Fn(Arg0) -> Self::OutputFuture {
    /// Output type of the async function which implements serde traits
    type Output;
    /// Future of the output
    type OutputFuture: Future<Output = <Self as AsyncFn1<Arg0>>::Output> + Send + 'static;
}

impl<F: ?Sized, Fut, Arg0> AsyncFn1<Arg0> for F
where
    F: Fn(Arg0) -> Fut,
    Fut: Future + Send + 'static,
{
    type Output = Fut::Output;
    type OutputFuture = Fut;
}

/// Trait to represent an async function with 2 arguments
pub trait AsyncFn2<Arg0, Arg1>: Fn(Arg0, Arg1) -> Self::OutputFuture {
    /// Output type of the async function which implements serde traits
    type Output;
    /// Future of the output
    type OutputFuture: Future<Output = <Self as AsyncFn2<Arg0, Arg1>>::Output> + Send + 'static;
}

impl<F: ?Sized, Fut, Arg0, Arg1> AsyncFn2<Arg0, Arg1> for F
where
    F: Fn(Arg0, Arg1) -> Fut,
    Fut: Future + Send + 'static,
{
    type Output = Fut::Output;
    type OutputFuture = Fut;
}

/// Trait to represent an async function with 3 arguments
pub trait AsyncFn3<Arg0, Arg1, Arg2>: Fn(Arg0, Arg1, Arg2) -> Self::OutputFuture {
    /// Output type of the async function which implements serde traits
    type Output;
    /// Future of the output
    type OutputFuture: Future<Output = <Self as AsyncFn3<Arg0, Arg1, Arg2>>::Output>
        + Send
        + 'static;
}

impl<F: ?Sized, Fut, Arg0, Arg1, Arg2> AsyncFn3<Arg0, Arg1, Arg2> for F
where
    F: Fn(Arg0, Arg1, Arg2) -> Fut,
    Fut: Future + Send + 'static,
{
    type Output = Fut::Output;
    type OutputFuture = Fut;
}

/// Register activity which takes [ActContext] with 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_activity_0_args] to execute the activity in the workflow definition.
pub fn into_activity_0_args<F, R, O>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    F: AsyncFn1<ActContext, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: ActContext| {
        (f)(ctx)
            .map(|r| {
                r.and_then(|r| {
                    let r = r.into();
                    Ok(match r {
                        ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                        ActExitValue::Normal(o) => ActExitValue::Normal(o.as_json_payload()?),
                    })
                })
            })
            .boxed()
    }
}

/// Execute activity which takes [ActContext] with 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload]. <br/>
/// Use [into_activity_0_args] to register the activity with the worker.
pub async fn execute_activity_0_args<'a, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn1<ActContext, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Execute activity which takes [ActContext] with 0 arguments and returns a [ActExitValue] wrapping 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_activity_0_args] to register the activity with the worker.
pub async fn execute_activity_0_args_exit_value<'a, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn1<ActContext, Output = Result<ActExitValue<R>, anyhow::Error>>
        + Send
        + Sync
        + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Register activity which takes 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_activity_0_args_without_ctx] to execute the activity in the workflow definition.
pub fn into_activity_0_args_without_ctx<F, R, O>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    F: AsyncFn0<Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |_ctx: ActContext| {
        (f)()
            .map(|r| {
                r.and_then(|r| {
                    let r: ActExitValue<O> = r.into();
                    Ok(match r {
                        ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                        ActExitValue::Normal(o) => ActExitValue::Normal(o.as_json_payload()?),
                    })
                })
            })
            .boxed()
    }
}

/// Execute activity which takes 0 arguments.
/// Use this for an activity which does not need to handle
/// [Cancellation](https://docs.temporal.io/activities#cancellation),
/// [Heartbeat](https://docs.temporal.io/activities#activity-heartbeat)
/// or any context dependent actions.
/// Use [into_activity_0_args_without_ctx] to register the activity with the worker.
pub async fn execute_activity_0_args_without_ctx<'a, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn0<Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Execute activity which takes 0 arguments.
/// Use this for an activity which does not need to handle
/// [Cancellation](https://docs.temporal.io/activities#cancellation),
/// [Heartbeat](https://docs.temporal.io/activities#activity-heartbeat)
/// or any context dependent actions.
/// Use [into_activity_0_args_without_ctx] to register the activity with the worker.
pub async fn execute_activity_0_args_without_ctx_exit_value<'a, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn0<Output = Result<ActExitValue<R>, anyhow::Error>> + Send + Sync + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Register activity which takes [ActContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_activity_1_args] to execute the activity in the workflow definition.
pub fn into_activity_1_args<A, F, R, O>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<ActContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: ActContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| {
                r.and_then(|r| {
                    let r = r.into();
                    Ok(match r {
                        ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                        ActExitValue::Normal(o) => ActExitValue::Normal(o.as_json_payload()?),
                    })
                })
            })
            .boxed(),
        Err(e) => async move { Err(anyhow::Error::new(e)) }.boxed(),
    }
}

/// Execute activity which takes [ActContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_activity_1_args] to register the activity with the worker.
pub async fn execute_activity_1_args<'a, A, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn2<ActContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![a],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Register activity which takes [ActContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_activity_1_args] to execute the activity in the workflow definition.
pub fn into_activity_1_args_exit_value<A, F, R, O>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<ActContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: ActContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| {
                r.and_then(|r| {
                    let r = r.into();
                    Ok(match r {
                        ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                        ActExitValue::Normal(o) => ActExitValue::Normal(o.as_json_payload()?),
                    })
                })
            })
            .boxed(),
        Err(e) => async move { Err(anyhow::Error::new(e)) }.boxed(),
    }
}

/// Execute activity which takes [ActContext] with 1 arguments and returns a [ActExitValue] wrapping 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_activity_1_args] to register the activity with the worker.
pub async fn execute_activity_1_args_exit_value<'a, A, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn2<ActContext, A, Output = Result<ActExitValue<R>, anyhow::Error>>
        + Send
        + Sync
        + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![a],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Register activity which takes [ActContext] with 1 arguments and returns a result [Result<R,E>] where 'R'
/// implements [serde] traits for serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// and 'E' is custom error type
/// Use [execute_activity_1_args_with_errors] to execute the activity in the workflow definition.
pub fn into_activity_1_args_with_errors<A, F, R, O, E>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<ActContext, A, Output = Result<R, E>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
    E: std::error::Error + Send + Sync + 'static,
{
    move |ctx: ActContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| match r {
                Ok(r) => {
                    let exit_val: ActExitValue<O> = r.into();
                    Ok(match exit_val {
                        ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                        ActExitValue::Normal(x) => ActExitValue::Normal(x.as_json_payload()?),
                    })
                }
                Err(e) => Err(anyhow::Error::from(e)),
            })
            .boxed(),
        Err(e) => async move { Err(anyhow::Error::from(e)) }.boxed(),
    }
}

/// Execute activity which takes [ActContext] with 1 arguments and returns a result [Result<R,E>] where 'R'
/// implements [serde] traits for serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// and 'E' is custom error type
/// Use [into_activity_1_args_with_errors] to register the activity with the worker.
pub async fn execute_activity_1_args_with_errors<'a, A, F, R, E>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
    a: A,
) -> Result<R, E>
where
    F: AsyncFn2<ActContext, A, Output = Result<R, E>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
    E: FromFailureExt,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![a],
        ..options
    };
    let activity_resolution = ctx.activity(options).await;
    match activity_resolution.status {
        Some(activity_resolution::Status::Completed(activity_result::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(activity_resolution::Status::Failed(activity_result::Failure { failure })) => {
            Err(E::from_failure(failure.unwrap()))
        }
        _ => panic!("Unexpected activity resolution status"),
    }
}

/// Register activity which takes [ActContext] with 2 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_activity_2_args] to execute the activity in the workflow definition.
pub fn into_activity_2_args<A, B, F, R, O>(
    f: F,
) -> impl Fn(ActContext) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    B: FromJsonPayloadExt + Send,
    F: AsyncFn3<ActContext, A, B, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: ActContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => match B::from_json_payload(&ctx.get_args()[1]) {
            Ok(b) => (f)(ctx, a, b)
                .map(|r| {
                    r.and_then(|r| {
                        let r = r.into();
                        Ok(match r {
                            ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                            ActExitValue::Normal(o) => ActExitValue::Normal(o.as_json_payload()?),
                        })
                    })
                })
                .boxed(),
            Err(e) => async move { Err(e.into()) }.boxed(),
        },
        Err(e) => async move { Err(e.into()) }.boxed(),
    }
}

/// Execute activity which takes [ActContext] with 2 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_activity_2_args] to register the activity with the worker.
pub async fn execute_activity_2_args<'a, 'b, A, B, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
    a: A,
    b: B,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn3<ActContext, A, B, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    B: Serialize + Deserialize<'b> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let b = B::as_json_payload(&b).expect("serializes fine");
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input: vec![a, b],
        //input,
        ..options
    };
    let output = ctx.activity(options).await.unwrap_ok_payload();
    Ok(R::from_json_payload(&output).unwrap())
}

/// Register child workflow which takes [WfContext] with 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_child_workflow_0_args] to execute the workflow in the workflow definition.
pub fn into_workflow_0_args<F, R, O>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    F: AsyncFn1<WfContext, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: WfContext| {
        (f)(ctx)
            .map(|r| {
                r.and_then(|r| {
                    let r = r.into();
                    Ok(match r {
                        WfExitValue::ContinueAsNew(b) => WfExitValue::ContinueAsNew(b),
                        WfExitValue::Cancelled => WfExitValue::Cancelled,
                        WfExitValue::Evicted => WfExitValue::Evicted,
                        WfExitValue::Normal(a) => WfExitValue::Normal(a.as_json_payload()?),
                    })
                })
            })
            .boxed()
    }
}

/// Execute child workflow which takes [WfContext] with 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_0_args] to register the workflow with the worker.
pub async fn execute_child_workflow_0_args<'a, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn1<WfContext, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Execute child workflow which takes [WfContext] with 0 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_0_args] to register the workflow with the worker.
pub async fn execute_child_workflow_0_args_exit_value<'a, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn1<WfContext, Output = Result<WfExitValue<R>, anyhow::Error>> + Send + Sync + 'static,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Register child workflow which takes [WfContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_child_workflow_1_args] to execute the workflow in the workflow definition.
pub fn into_workflow_1_args<A, F, R, O>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<WfContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: WfContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| {
                r.and_then(|r| {
                    let r: WfExitValue<O> = r.into();
                    Ok(match r {
                        WfExitValue::ContinueAsNew(b) => WfExitValue::ContinueAsNew(b),
                        WfExitValue::Cancelled => WfExitValue::Cancelled,
                        WfExitValue::Evicted => WfExitValue::Evicted,
                        WfExitValue::Normal(a) => WfExitValue::Normal(a.as_json_payload()?),
                    })
                })
            })
            .boxed(),
        Err(e) => async move { Err(e.into()) }.boxed(),
    }
}

/// Execute child workflow which takes [WfContext] with 2 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_1_args] to register the workflow with the worker
pub async fn execute_child_workflow_1_args<'a, A, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn2<WfContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![a],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Register child workflow which takes [WfContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_child_workflow_1_args_exit_value] to execute the workflow in the workflow definition.
pub fn into_workflow_1_args_exit_value<A, F, R, O>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<WfContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: WfContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| {
                r.and_then(|r| {
                    let r = r.into();
                    Ok(match r {
                        WfExitValue::ContinueAsNew(b) => WfExitValue::ContinueAsNew(b),
                        WfExitValue::Cancelled => WfExitValue::Cancelled,
                        WfExitValue::Evicted => WfExitValue::Evicted,
                        WfExitValue::Normal(a) => WfExitValue::Normal(a.as_json_payload()?),
                    })
                })
            })
            .boxed(),
        Err(e) => async move { Err(anyhow::Error::new(e)) }.boxed(),
    }
}

/// Execute child workflow which takes [WfContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_1_args_exit_value] to register the workflow with the worker.
pub async fn execute_child_workflow_1_args_exit_value<'a, A, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn2<WfContext, A, Output = Result<WfExitValue<R>, anyhow::Error>>
        + Send
        + Sync
        + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![a],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(child_workflow_result::Status::Failed(child_workflow::Failure { failure })) => {
            Err(anyhow::anyhow!("{:?}", failure))
        }
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Execute activity which takes [ActContext] with 1 arguments and returns a result [Result<R,E>] where 'R'
/// implements [serde] traits for serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// and 'E' is custom error type
/// Use [execute_child_workflow_1_args_with_errors] to register the activity with the worker.
pub fn into_workflow_1_args_with_errors<A, F, R, O, E>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn2<WfContext, A, Output = Result<R, E>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
    E: std::error::Error + Send + Sync + 'static,
{
    move |ctx: WfContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a)
            .map(|r| match r {
                Ok(r) => {
                    let r: WfExitValue<O> = r.into();
                    Ok(match r {
                        WfExitValue::ContinueAsNew(b) => WfExitValue::ContinueAsNew(b),
                        WfExitValue::Cancelled => WfExitValue::Cancelled,
                        WfExitValue::Evicted => WfExitValue::Evicted,
                        WfExitValue::Normal(a) => WfExitValue::Normal(a.as_json_payload()?),
                    })
                }
                Err(e) => Err(anyhow::Error::from(e)),
            })
            .boxed(),
        Err(e) => async move { Err(anyhow::Error::from(e)) }.boxed(),
    }
}

/// Execute child workflow which takes [WfContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_0_args] to register the workflow with the worker.
pub async fn execute_child_workflow_1_args_with_errors<'a, A, F, R, E>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
) -> Result<R, E>
where
    F: AsyncFn2<WfContext, A, Output = Result<R, E>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
    E: FromFailureExt,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![a],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        Some(child_workflow_result::Status::Failed(child_workflow::Failure { failure })) => {
            Err(E::from_failure(failure.unwrap()))
        }
        _ => panic!("Unexpected child WF status"),
    }
}

/// Register child workflow which takes [WfContext] with 1 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_child_workflow_2_args] to execute the workflow in the workflow definition.
pub fn into_workflow_2_args<A, B, F, R, O>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<Payload>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    B: FromJsonPayloadExt + Send,
    F: AsyncFn3<WfContext, A, B, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: WfContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => match B::from_json_payload(&ctx.get_args()[1]) {
            Ok(b) => (f)(ctx, a, b)
                .map(|r| {
                    r.and_then(|r| {
                        let exit_val: WfExitValue<O> = r.into();
                        Ok(match exit_val {
                            WfExitValue::ContinueAsNew(b) => WfExitValue::ContinueAsNew(b),
                            WfExitValue::Cancelled => WfExitValue::Cancelled,
                            WfExitValue::Evicted => WfExitValue::Evicted,
                            WfExitValue::Normal(a) => WfExitValue::Normal(a.as_json_payload()?),
                        })
                    })
                })
                .boxed(),
            Err(e) => async move { Err(e.into()) }.boxed(),
        },
        Err(e) => async move { Err(e.into()) }.boxed(),
    }
}

/// Execute child workflow which takes [WfContext] with 2 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_2_args] to register the workflow with the worker
pub async fn execute_child_workflow_2_args<'a, A, B, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
    b: B,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn3<WfContext, A, B, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    B: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let b = B::as_json_payload(&b).expect("serializes fine");
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![a, b],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => {
            Ok(R::from_json_payload(&result.unwrap()).unwrap())
        }
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Execute child workflow which takes [WfContext] with 2 arguments and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow_2_args] to register the workflow with the worker
pub async fn execute_child_workflow_2_args_exit_value<'a, A, B, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
    b: B,
) -> Result<WfExitValue<R>, anyhow::Error>
where
    F: AsyncFn3<WfContext, A, B, Output = Result<WfExitValue<R>, anyhow::Error>>
        + Send
        + Sync
        + 'static,
    A: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    B: Serialize + Deserialize<'a> + AsJsonPayloadExt + Debug,
    R: Serialize + Deserialize<'a> + FromJsonPayloadExt + Debug,
{
    let a = A::as_json_payload(&a).expect("serializes fine");
    let b = B::as_json_payload(&b).expect("serializes fine");
    let workflow_type = std::any::type_name::<F>();

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type: workflow_type.to_string(),
        input: vec![a, b],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(child_workflow::Success { result })) => Ok(
            WfExitValue::Normal(R::from_json_payload(&result.unwrap()).unwrap()),
        ),
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_trait_bounds() {
        assert_eq!(6, 6);
    }
}
