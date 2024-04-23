//! This needs to be its own crate so that it doesn't pull in anything that would make compiling
//! to WASM not work. I've already figured out how to do all that once before with my WASM workflows
//! hackathon

mod activity_definitions;

use activity_definitions::MyActFnWfCtxExt;
use futures::future::BoxFuture;
use std::time::Duration;
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::ContinueAsNewWorkflowExecution, temporal::api::common::v1::Payload,
};

// anyhow errors are used for the errors returned by user-defined functions. This makes `?` work
// well everywhere by default which is a very nice property, as well as preserving backtraces. We
// may need to define our own error type instead to allow attaching things like the non-retryable
// flag... but I suspect we can just make downcasting work for that.

/// Workflow authors must implement this trait to create Temporal Rust workflows
pub trait Workflow: Sized {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The workflow's name
    const NAME: &'static str;

    /// Called when an instance of a Workflow is first initialized.
    ///
    /// `input` contains the input argument to the workflow as defined by the client who requested
    /// the Workflow Execution.
    fn new(input: Self::Input, ctx: SafeWfContext) -> Self;

    /// Defines the actual workflow logic. The function must return a future, and this future is
    /// cached and polled as updates to the workflow history are received.
    ///
    /// `ctx` should be used to perform various Temporal commands like starting timers and
    /// activities.
    fn run(
        &mut self,
        ctx: WfContext,
    ) -> BoxFuture<Result<WfExitValue<Self::Output>, anyhow::Error>>;

    /// All signals this workflow can handle. Typically you won't implement this directly, it will
    /// automatically contain all signals defined with the `#[signal]` attribute.
    fn signals() -> &'static [&'static SignalDefinition<Self>] {
        // TODO
        &[]
    }
    /// All queries this workflow can handle. Typically you won't implement this directly, it will
    /// automatically contain all queries defined with the `#[query]` attribute.
    fn queries() -> &'static [&'static QueryDefinition<Self>] {
        // TODO
        &[]
    }
}

/// TODO: Exists in SDK in slightly different form, and would move into this crate
#[derive(Debug)]
pub enum WfExitValue<T: TemporalSerializable> {
    /// Continue the workflow as a new execution
    ContinueAsNew(Box<ContinueAsNewWorkflowExecution>), // Wouldn't be raw proto in reality
    /// Confirm the workflow was cancelled
    Cancelled,
    /// Finish with a result
    Normal(T),
}
impl<T: TemporalSerializable> From<T> for WfExitValue<T> {
    fn from(v: T) -> Self {
        Self::Normal(v)
    }
}
// ... also convenience functions for constructing C-A-N, etc.

/// A workflow context which contains only information, but does not allow any commands to
/// be created.
pub struct SafeWfContext {
    // TODO
}

/// TODO: Placeholder, exists in SDK and would move into this crate & (likely) become a trait
pub struct WfContext {}
impl WfContext {
    pub async fn timer(&self, _: Duration) {
        todo!()
    }
}

pub struct SignalDefinition<WF: Workflow> {
    // TODO: Could be a matching predicate, to allow for dynamic registration
    name: String,
    // The handler input type must be erased here, since otherwise we couldn't store/return the
    // heterogeneous collection of definition types in the workflow itself. The signal macro
    // will wrap the user's function with code that performs deserialization.
    handler: Box<dyn FnMut(&mut WF, Payload) -> Result<(), anyhow::Error>>,
}
pub struct QueryDefinition<WF: Workflow> {
    // TODO: Could be a matching predicate, to allow for dynamic registration
    name: String,
    // The query macro will wrap the user's function with code that performs deserialization of
    // input and serialization of output, as well as error boxing.
    handler: Box<dyn FnMut(&WF, Payload) -> Result<Payload, anyhow::Error>>,
}

/// TODO: Placeholders, likely belong inside protos crate. These will be auto-implemented for
///   anything using serde already (which I expect is how virtually everyone will do this).
pub trait TemporalSerializable {}
impl<T> TemporalSerializable for T {}
pub trait TemporalDeserializable {}
impl<T> TemporalDeserializable for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::{collections::HashMap, marker::PhantomData};

    // Workflow implementation example
    struct MyWorkflow {
        foo: u64,
        bar: HashMap<String, u64>,
    }

    impl Workflow for MyWorkflow {
        type Input = String;
        type Output = u64;
        const NAME: &'static str = "MyWorkflowType";

        fn new(input: Self::Input, _ctx: SafeWfContext) -> Self {
            let mut bar = HashMap::new();
            bar.insert(input, 10);
            Self { foo: 0, bar }
        }

        fn run(
            &mut self,
            ctx: WfContext,
        ) -> BoxFuture<Result<WfExitValue<Self::Output>, anyhow::Error>> {
            async move {
                ctx.timer(Duration::from_secs(1)).await;
                self.foo = 1;
                // See activity definitions file
                ctx.my_act_fn("Hi!").await.unwrap();
                // The into() is unfortunately unavoidable without making C-A-N and confirm cancel
                // be errors instead. Personally, I don't love that and I think it's not idiomatic
                // Rust, whereas needing to `into()` something is. Other way would be macros, but
                // it's slightly too much magic I think.
                Ok(self.foo.into())
            }
            .boxed()
            // TODO: The need to box here is slightly unfortunate, but it's either that or require
            //  users to depend on `async_trait` (which just hides the same thing). IMO this is the
            //  best option until more language features stabilize and this can go away.
        }
    }

    // #[workflow] miiiight be necessary here, but, ideally is not.
    impl MyWorkflow {
        // Attrib commented out since it's nonexistent for now, but that's what it'd look like.
        // #[signal]
        pub fn my_signal(&mut self, arg: String) {
            self.bar.insert(arg, 1);
        }
        // #[query]
        pub fn my_query(&self, arg: String) -> Option<u64> {
            self.bar.get(&arg).cloned()
        }
    }

    // This would need to be moved into this crate and depended on by client
    struct WorkflowHandle<WF: Workflow> {
        _d: PhantomData<WF>,
    }
    struct SignalError; // just a placeholder
    struct QueryError; // just a placeholder

    // The signal/query macros would generate this trait and impl:
    trait MyWorkflowClientExtension {
        fn my_signal(&self, arg: String) -> BoxFuture<Result<(), SignalError>>;
        fn my_query(&self, arg: String) -> BoxFuture<Result<Option<u64>, QueryError>>;
    }
    impl MyWorkflowClientExtension for WorkflowHandle<MyWorkflow> {
        fn my_signal(&self, arg: String) -> BoxFuture<Result<(), SignalError>> {
            // Becomes something like:
            // self.signal("my_signal", arg.serialize())
            todo!()
        }

        fn my_query(&self, arg: String) -> BoxFuture<Result<Option<u64>, QueryError>> {
            todo!()
        }
    }

    async fn client_example() {
        // Now you can use the client like:
        // (actually comes from client.start() or client.get_handle() etc)
        let wfh = WorkflowHandle {
            _d: PhantomData::<MyWorkflow>,
        };
        let _ = wfh.my_signal("hi!".to_string()).await;
    }

    #[test]
    fn compile() {}
}
