use futures::future::BoxFuture;
use std::{error::Error, time::Duration};
use temporal_sdk_core_protos::temporal::api::common::v1::Payload;

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
    fn run(&mut self, ctx: WfContext) -> BoxFuture<Self::Output>;

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

/// A workflow context which contains only information, but does not allow any commands to
/// be created.
pub struct SafeWfContext {
    // TODO
}

/// TODO: Placeholder, move from SDK
pub struct WfContext {}
impl WfContext {
    pub async fn timer(&self, _: Duration) {
        todo!()
    }
}

pub struct SignalDefinition<WF: Workflow> {
    // TODO: Could be a matching predicate
    name: String,
    // The handler input type must be erased here, since otherwise we couldn't store/return the
    // heterogeneous collection of definition types in the workflow itself. The signal macro
    // will wrap the user's function with code that performs deserialization, as well as error
    // boxing.
    handler: Box<dyn FnMut(&mut WF, Payload) -> Result<(), Box<dyn Error>>>,
}
pub struct QueryDefinition<WF: Workflow> {
    // TODO: Could be a matching predicate
    name: String,
    // The query macro will wrap the user's function with code that performs deserialization of
    // input and serialization of output, as well as error boxing.
    handler: Box<dyn FnMut(&WF, Payload) -> Result<Payload, Box<dyn Error>>>,
}

/// TODO: Placeholder, move from (and improve in) SDK
pub trait TemporalSerializable {}
impl<T> TemporalSerializable for T {}
/// TODO: Placeholder, move from (and improve in) SDK
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

        fn run(&mut self, ctx: WfContext) -> BoxFuture<Self::Output> {
            async move {
                ctx.timer(Duration::from_secs(1)).await;
                self.foo = 1;
                self.foo
            }
            .boxed()
            // TODO: The need to box here is slightly unfortunate, but it's either that or require
            //  users to depend on `async_trait` (which just hides the same thing). IMO this is the
            //  best option until more language features stabilize and this can go away.
        }
    }

    // #[workflow] miiiight be necessary here, but, ideally is not.
    impl MyWorkflow {
        // Attrib commented out since nonexistent for now, but that's what it'd look like.
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
            // Is actually something like:
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
}
