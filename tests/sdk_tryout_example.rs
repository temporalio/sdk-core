use std::collections::HashMap;

use futures_util::{FutureExt, future::BoxFuture};

// Public types we will expose =====================================================================

// Placeholders, likely belong inside protos crate. These will be auto-implemented for
//   anything using serde already (which I expect is how virtually everyone will do this).
pub trait TemporalSerializable {}
impl<T> TemporalSerializable for T {}
pub trait TemporalDeserializable {}
impl<T> TemporalDeserializable for T {}
pub struct Payload {}

// Context placeholders
pub struct SafeWfContext {}
pub struct WfContext {}

#[derive(Debug)]
pub enum WfExitValue<T: TemporalSerializable> {
    /// Continue the workflow as a new execution
    ContinueAsNew,
    /// Confirm the workflow was cancelled
    Cancelled,
    /// Finish with a result
    Normal(T),
}

// User doesn't really need to understand this trait, as it's impl is generated for them
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The type that implements this definition
    type Implementation: WorkflowImplementation + 'static;
}

// User doesn't really need to understand this trait, as it's impl is generated for them
// Actual implementation's input/output types are type-erased, so that they can be stored in a
// collection together (obviously when registering them they need to be)
pub trait WorkflowImplementation {
    fn init(input: Payload, ctx: SafeWfContext) -> Self
    where
        Self: Sized;
    fn run(&mut self, ctx: WfContext)
    -> BoxFuture<'_, Result<WfExitValue<Payload>, anyhow::Error>>;
    // This need to appear here too because the this is the only type we can accept in a collection
    // when registering, so we need to use the names to match definitions to implementations.
    fn name() -> &'static str
    where
        Self: Sized;
}

struct Client {}
impl Client {
    fn new() -> Self {
        Client {}
    }

    async fn start_workflow<WD>(
        &self,
        _input: WD::Input,
    ) -> Result<WD::Output, Box<dyn std::error::Error>>
    where
        WD: WorkflowDefinition,
    {
        todo!()
    }
}

// Just playing around with how I can actually store registered workflows
type WorkflowInitializer = fn(Payload, SafeWfContext) -> Box<dyn WorkflowImplementation>;
pub struct WorkerOptions {
    workflows: HashMap<&'static str, WorkflowInitializer>,
}
impl WorkerOptions {
    pub fn new() -> Self {
        WorkerOptions {
            workflows: HashMap::new(),
        }
    }
    pub fn register_workflow<WD: WorkflowDefinition>(&mut self) -> &mut Self {
        self.workflows.insert(WD::Implementation::name(), |p, c| {
            Box::new(WD::Implementation::init(p, c))
        });
        self
    }
}

// =================================================================================================
// User's code =====================================================================================

pub struct MyWorkflow {
    // Some internal state
}

// #[workflow]
impl MyWorkflow {
    // #[init]
    pub fn new(_input: String, _ctx: SafeWfContext) -> Self {
        todo!()
    }

    // #[run]
    pub async fn run(&mut self, _ctx: WfContext) -> Result<WfExitValue<String>, anyhow::Error> {
        todo!()
    }
}

// =================================================================================================
// Generated code from above =======================================================================
impl WorkflowDefinition for MyWorkflow {
    type Input = String;
    type Output = String;
    type Implementation = MyWorkflow;
}
impl WorkflowImplementation for MyWorkflow {
    fn init(_input: Payload, ctx: SafeWfContext) -> Self {
        let deserialzied: <MyWorkflow as WorkflowDefinition>::Input =
            todo!("deserialize from input");
        MyWorkflow::new(deserialzied, ctx)
    }

    fn run(
        &mut self,
        ctx: WfContext,
    ) -> BoxFuture<'_, Result<WfExitValue<Payload>, anyhow::Error>> {
        self.run(ctx).map(|_| todo!("Serialize output")).boxed()
    }

    fn name() -> &'static str {
        return "MyWorkflow";
    }
}

// =================================================================================================
// More user code using the definitions from above ===================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker_opts = WorkerOptions::new();
    worker_opts
        .register_workflow::<MyWorkflow>()
        // Obviously IRL they'd be different, but, this is how you register multiple.
        // Passing in a collection wouldn't make sense.
        .register_workflow::<MyWorkflow>()
        .register_workflow::<MyWorkflow>();

    let client = Client::new();
    client
        .start_workflow::<MyWorkflow>("hi".to_string())
        .await?;
    Ok(())
}
