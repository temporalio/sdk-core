use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

// Public types we will expose =====================================================================

// Placeholders, likely belong inside protos crate. These will be auto-implemented for
//   anything using serde already (which I expect is how virtually everyone will do this).
pub trait TemporalSerializable {}
impl<T> TemporalSerializable for T {}
pub trait TemporalDeserializable {}
impl<T> TemporalDeserializable for T {}
pub struct Payload {}
impl Payload {
    fn deserialize<T>(self) -> T {
        todo!("just here to make unreachable warnings go away");
    }
}

pub struct SafeWorkflowContext {}
pub struct WorkflowContext {}
pub enum WorkflowError {
    // Variants for panic, bubbled up failure, etc
}

#[derive(Debug)]
pub enum WfExitValue<T: TemporalSerializable> {
    /// Continue the workflow as a new execution
    ContinueAsNew,
    /// Confirm the workflow was cancelled
    Cancelled,
    /// Finish with a result
    Normal(T),
}

// User doesn't really need to understand this trait, as its impl is generated for them. All such
// traits can be hidden in docs by default.
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The type that implements this definition
    type Implementation: WorkflowImplementation + 'static;
}

// User doesn't really need to understand this trait, as its impl is generated for them
// Actual implementation's input/output types are type-erased, so that they can be stored in a
// collection together (obviously when registering them they need to be)
pub trait WorkflowImplementation {
    fn init(input: Payload, ctx: SafeWorkflowContext) -> Self
    where
        Self: Sized;
    fn run(
        &mut self,
        ctx: WorkflowContext,
    ) -> BoxFuture<'_, Result<WfExitValue<Payload>, WorkflowError>>;
    fn name() -> &'static str
    where
        Self: Sized;
}

pub trait UpdateDefinition {
    type Input: TemporalDeserializable;
    type Output: TemporalSerializable + 'static;
}

pub struct ActivityContext {}
pub type ActivityError = Box<dyn std::error::Error + Send + Sync>;

// User doesn't really need to understand this trait, as its impl is generated for them
pub trait ActivityDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable + 'static;
    type Implementer: ActivityImplementer + 'static;

    fn name() -> &'static str
    where
        Self: Sized;

    fn execute(
        receiver: Option<Arc<Self::Implementer>>,
        ctx: ActivityContext,
        input: Self::Input,
    ) -> BoxFuture<'static, Result<Self::Output, ActivityError>>;
}

// User doesn't really need to understand this trait, as its impl is generated for them
pub trait ActivityImplementer {
    fn register_all_static(worker_options: &mut WorkerOptions);
    fn register_all_instance(self: Arc<Self>, worker_options: &mut WorkerOptions);
}

/// Marker trait for activity structs that only have static methods
pub trait HasOnlyStaticMethods: ActivityImplementer {}

struct Client {}
// TODO: It probably makes sense to have more specific error per-call, but not going to enumerate
//   all of them here.
type ClientError = Box<dyn std::error::Error>;
impl Client {
    fn new() -> Self {
        Client {}
    }

    // Would return a more specific error type
    async fn start_workflow<WD>(
        &self,
        _input: WD::Input,
    ) -> Result<WorkflowHandle<WD::Output>, ClientError>
    where
        WD: WorkflowDefinition,
    {
        todo!()
    }
}

#[derive(Debug)]
enum WorkflowResultError {
    // Needs to be different from WorkflowError, since, for example a continue-as-new variant needs
    // to exist there, but should not here. Unless we don't make that an error.
}

struct WorkflowHandle<T> {
    _rt: PhantomData<T>,
}
impl<T> WorkflowHandle<T> {
    pub async fn result(self) -> Result<T, WorkflowResultError> {
        todo!()
    }

    pub async fn update<SD>() {}
}

// Just playing around with how I can actually store registered workflows
type WorkflowInitializer = fn(Payload, SafeWorkflowContext) -> Box<dyn WorkflowImplementation>;
type ActivityInvocation =
    Box<dyn Fn(Payload, ActivityContext) -> BoxFuture<'static, Result<Payload, anyhow::Error>>>;
pub struct WorkerOptions {
    workflows: HashMap<&'static str, WorkflowInitializer>,
    activities: HashMap<&'static str, ActivityInvocation>,
}
impl WorkerOptions {
    pub fn new() -> Self {
        WorkerOptions {
            workflows: HashMap::new(),
            activities: HashMap::new(),
        }
    }
    pub fn register_workflow<WD: WorkflowDefinition>(&mut self) -> &mut Self {
        self.workflows.insert(WD::Implementation::name(), |p, c| {
            Box::new(WD::Implementation::init(p, c))
        });
        self
    }
    pub fn register_activities<AI>(&mut self) -> &mut Self
    where
        AI: ActivityImplementer + HasOnlyStaticMethods,
    {
        AI::register_all_static(self);
        self
    }
    pub fn register_activities_with_instance<AI: ActivityImplementer>(
        &mut self,
        instance: AI,
    ) -> &mut Self {
        AI::register_all_static(self);
        let arcd = Arc::new(instance);
        AI::register_all_instance(arcd, self);
        self
    }
    pub fn register_activity<AD: ActivityDefinition>(&mut self) -> &mut Self {
        self.activities.insert(
            AD::name(),
            Box::new(|p, c| {
                let deserialized = p.deserialize();
                AD::execute(None, c, deserialized)
                    .map(|_| todo!("serialize"))
                    .boxed()
            }),
        );
        self
    }
    pub fn register_activity_with_instance<AD: ActivityDefinition>(
        &mut self,
        instance: Arc<AD::Implementer>,
    ) -> &mut Self {
        self.activities.insert(
            AD::name(),
            Box::new(move |p, c| {
                let deserialized = p.deserialize();
                AD::execute(Some(instance.clone()), c, deserialized)
                    .map(|_| todo!("serialize"))
                    .boxed()
            }),
        );
        self
    }
}

// =================================================================================================
// User's code =====================================================================================

pub struct MyWorkflow {
    // Some internal state
}

// #[workflow] -- Can override name
impl MyWorkflow {
    // #[init]
    pub fn new(_input: String, _ctx: SafeWorkflowContext) -> Self {
        todo!()
    }

    // #[run]
    pub async fn run(
        &mut self,
        _ctx: WorkflowContext,
    ) -> Result<WfExitValue<String>, anyhow::Error> {
        todo!()
    }

    // #[signal] -- May be sync or async
    pub fn signal(&mut self, _input: bool) {
        todo!()
    }

    // #[query] -- Glory, finally, immutable-guaranteed queries. Can't be async.
    pub fn query(&self, _input: String) -> String {
        todo!()
    }

    // #[update] -- May also be sync or async
    pub fn update(&mut self, _input: String) -> String {
        todo!()
    }
}

pub struct MyActivities {
    // Some internal state
}
// This will likely be necessary because procmacros can't access things outside their scope
// #[activities]
impl MyActivities {
    // #[activity] -- Can override name
    pub async fn static_activity(
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<&'static str, ActivityError> {
        Ok("Can be static")
    }
    // #[activity]
    pub async fn activity(
        // Activities that want to manipulate shared state must do it through an Arc. We can't allow
        // mut access because we can't guarantee concurrent tasks won't need to be executed.
        // We could maybe allow `&self` access, but, I think that just obscures the fact that
        // instances will need to be stored in Arcs. Macro can reject any other receiver type.
        self: Arc<Self>,
        _ctx: ActivityContext,
        _in: bool,
    ) -> Result<&'static str, ActivityError> {
        Ok("Can also take &self or &mut self")
    }
    // #[activity]
    pub fn sync_activity(_ctx: ActivityContext, _in: bool) -> Result<&'static str, ActivityError> {
        Ok("Sync activities are supported too")
    }
}

pub struct MyActivitiesStatic;
impl MyActivitiesStatic {
    // #[activity]
    pub async fn static_activity(
        _ctx: ActivityContext,
        _in: String,
    ) -> Result<&'static str, ActivityError> {
        Ok("Can be static")
    }
}

// TODO: It'd definitely be _possible_ to support top-level activities, but they would either need
// more dedicated APIs, or we'd need to generate marker structs that "hold" them so they can be
// registered via the static path. That's simple enough that I skip demonstrating it here.

// =================================================================================================
// Generated code from above =======================================================================

// Generated module for MyWorkflow signals/queries/updates
pub mod my_workflow {
    use super::*;

    pub struct signal;
    pub struct query;
    pub struct update;
}

impl WorkflowDefinition for MyWorkflow {
    type Input = String;
    type Output = String;
    type Implementation = MyWorkflow;
}
impl WorkflowImplementation for MyWorkflow {
    fn init(input: Payload, ctx: SafeWorkflowContext) -> Self {
        let deserialzied: <MyWorkflow as WorkflowDefinition>::Input = input.deserialize();
        MyWorkflow::new(deserialzied, ctx)
    }

    fn run(
        &mut self,
        ctx: WorkflowContext,
    ) -> BoxFuture<'_, Result<WfExitValue<Payload>, WorkflowError>> {
        self.run(ctx).map(|_| todo!("Serialize output")).boxed()
    }

    fn name() -> &'static str {
        return "MyWorkflow";
    }
}

// Generated module for MyActivities activity definitions
pub mod my_activities {
    use super::*;

    pub struct StaticActivity;
    pub struct Activity;
    pub struct SyncActivity;

    impl ActivityDefinition for StaticActivity {
        type Input = String;
        type Output = &'static str;
        type Implementer = MyActivities;
        fn name() -> &'static str
        where
            Self: Sized,
        {
            "my_activities_static::StaticActivity"
        }
        fn execute(
            _receiver: Option<Arc<Self::Implementer>>,
            ctx: ActivityContext,
            input: Self::Input,
        ) -> BoxFuture<'static, Result<Self::Output, ActivityError>> {
            MyActivities::static_activity(ctx, input).boxed()
        }
    }

    impl ActivityDefinition for Activity {
        type Input = bool;
        type Output = &'static str;
        type Implementer = MyActivities;
        fn name() -> &'static str
        where
            Self: Sized,
        {
            "my_activities::Activity"
        }
        fn execute(
            receiver: Option<Arc<Self::Implementer>>,
            ctx: ActivityContext,
            input: Self::Input,
        ) -> BoxFuture<'static, Result<Self::Output, ActivityError>> {
            MyActivities::activity(receiver.unwrap(), ctx, input).boxed()
        }
    }

    impl ActivityDefinition for SyncActivity {
        type Input = bool;
        type Output = &'static str;
        type Implementer = MyActivities;
        fn name() -> &'static str
        where
            Self: Sized,
        {
            "my_activities_static::StaticActivity"
        }
        fn execute(
            _receiver: Option<Arc<Self::Implementer>>,
            ctx: ActivityContext,
            input: Self::Input,
        ) -> BoxFuture<'static, Result<Self::Output, ActivityError>> {
            // Here we spawn any sync activities as blocking via tokio. All the normal caveats with that
            // apply, and we'll say so to users.
            tokio::task::spawn_blocking(move || MyActivities::sync_activity(ctx, input))
                .map(|jh| match jh {
                    Err(err) => Err(ActivityError::from(err)),
                    Ok(v) => v,
                })
                .boxed()
        }
    }

    impl ActivityImplementer for MyActivities {
        fn register_all_static(worker_options: &mut WorkerOptions) {
            worker_options.register_activity::<StaticActivity>();
            worker_options.register_activity::<SyncActivity>();
        }
        fn register_all_instance(self: Arc<Self>, worker_options: &mut WorkerOptions) {
            worker_options.register_activity_with_instance::<Activity>(self.clone());
        }
    }
}

// Generated module for MyActivitiesStatic activity definitions
pub mod my_activities_static {
    use super::*;

    pub struct StaticActivity;

    impl ActivityDefinition for StaticActivity {
        type Input = String;
        type Output = &'static str;
        type Implementer = MyActivities;
        fn name() -> &'static str
        where
            Self: Sized,
        {
            "my_activities_static::StaticActivity"
        }
        fn execute(
            _receiver: Option<Arc<Self::Implementer>>,
            ctx: ActivityContext,
            input: Self::Input,
        ) -> BoxFuture<'static, Result<Self::Output, ActivityError>> {
            MyActivities::static_activity(ctx, input).boxed()
        }
    }

    impl ActivityImplementer for MyActivitiesStatic {
        fn register_all_static(worker_options: &mut WorkerOptions) {
            worker_options.register_activity::<StaticActivity>();
        }
        fn register_all_instance(self: Arc<Self>, _: &mut WorkerOptions) {
            unreachable!()
        }
    }
    impl HasOnlyStaticMethods for MyActivitiesStatic {}
}

// =================================================================================================
// More user code using the definitions from above ===================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let activity_instance = MyActivities {};
    let mut worker_opts = WorkerOptions::new();
    worker_opts
        .register_workflow::<MyWorkflow>()
        // Obviously IRL they'd be different, but, this is how you register multiple definitions.
        // Passing in a collection wouldn't make sense.
        .register_workflow::<MyWorkflow>()
        .register_workflow::<MyWorkflow>()
        // This also registers the static activity
        .register_activities_with_instance(activity_instance)
        // ----
        // This is a compile error, since MyActivities is known to have non static-methods
        // .register_activities::<MyActivities>();
        // ----
        // But this works
        .register_activities::<MyActivitiesStatic>();

    let client = Client::new();
    let handle = client
        .start_workflow::<MyWorkflow>("hi".to_string())
        .await?;
    // Activity invocation will also look very similar like
    // ctx.execute_activity::<my_activities::Activity>(input).await?;
    let _result = handle.result().await?;
    Ok(())
}

// Stuff to just make compile work, not worth reviewing ============================================
impl std::error::Error for WorkflowResultError {}
impl std::fmt::Display for WorkflowResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowResultError")
    }
}
