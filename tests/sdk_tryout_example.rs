use futures_util::{FutureExt, future::BoxFuture};

/// TODO: Placeholders, likely belong inside protos crate. These will be auto-implemented for
///   anything using serde already (which I expect is how virtually everyone will do this).
pub trait TemporalSerializable {}
impl<T> TemporalSerializable for T {}
pub trait TemporalDeserializable {}
impl<T> TemporalDeserializable for T {}

// TODO: How does a heterogenous collection of workflow definitions work?
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The workflow's name
    const NAME: &'static str;
}

// TODO: Could be "WorkflowImplementation"?
pub trait Workflow: WorkflowDefinition + Sized {
    fn new(input: Self::Input, ctx: SafeWfContext) -> Self;

    fn run(
        &mut self,
        ctx: WfContext,
    ) -> BoxFuture<'_, Result<WfExitValue<Self::Output>, anyhow::Error>>;
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

pub struct MyWorkflow {
    // Some internal state
}

// #[workflow]
impl MyWorkflow {
    // #[run]
    pub async fn run(
        &mut self,
        _ctx: &WfContext,
        _my_input: &String,
    ) -> Result<WfExitValue<String>, anyhow::Error> {
        todo!()
    }
}

// Generated code =========================================================
impl WorkflowDefinition for MyWorkflow {
    type Input = String;
    type Output = String;
    const NAME: &'static str = "MyWorkflow";
}
impl Workflow for MyWorkflow {
    fn new(_input: Self::Input, _ctx: SafeWfContext) -> Self {
        todo!()
    }

    fn run(
        &mut self,
        ctx: WfContext,
    ) -> BoxFuture<'_, Result<WfExitValue<Self::Output>, anyhow::Error>> {
        self.run(&ctx, todo!("Would be decoded from ctx")).boxed()
    }
}

// ========================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    client
        .start_workflow::<MyWorkflow>("hi".to_string())
        .await?;
    Ok(())
}

pub struct SafeWfContext {
    // TODO
}

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
