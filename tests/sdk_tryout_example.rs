#![allow(dead_code)]

use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use temporal_client::WorkflowService;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::Payload,
    failure::v1::Failure,
    workflowservice::v1::{
        PollWorkflowExecutionUpdateRequest, PollWorkflowExecutionUpdateResponse,
        StartWorkflowExecutionRequest,
    },
};
use tonic::IntoRequest;

// Public types we will expose =====================================================================

// All data conversion functionality likely belongs inside protos or api crate.
pub struct DataConverter {
    payload_converter: PayloadConverter,
    failure_converter: Box<dyn FailureConverter>,
    codec: Box<dyn PayloadCodec>,
}
impl DataConverter {
    pub fn new(
        payload_converter: PayloadConverter,
        failure_converter: impl FailureConverter + 'static,
        codec: impl PayloadCodec + 'static,
    ) -> Self {
        Self {
            payload_converter,
            failure_converter: Box::new(failure_converter),
            codec: Box::new(codec),
        }
    }
}

type PayloadConversionError = Box<dyn std::error::Error + Send + Sync + 'static>;
// Payload conversion can't really be defined at the whole-converter level in a type-safe way
// in Rust. Instead we provide an easy way to provide serde-backed converters, or you can use
// wrapper types to opt into a different form of conversion for the specific place that type is
// used.
#[derive(Clone)]
pub enum PayloadConverter {
    Serde(Arc<dyn ErasedSerdePayloadConverter>),
    // This variant signals the user wants to delegate to wrapper types
    UseWrappers,
    Composite(Arc<CompositePayloadConverter>),
}
impl PayloadConverter {
    pub fn serde_json() -> Self {
        Self::Serde(Arc::new(SerdeJsonPayloadConverter))
    }
    // ... and more
}

pub trait FailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error>,
    ) -> Result<Failure, PayloadConversionError>;
    fn from_failure(&self, failure: Failure) -> Result<Failure, PayloadConversionError>;
}
pub struct DefaultFailureConverter;
pub trait PayloadCodec {
    fn encode(&self, payloads: Vec<Payload>) -> Vec<Payload>;
    fn decode(&self, payloads: Vec<Payload>) -> Vec<Payload>;
}
pub struct DefaultPayloadCodec;

// Users don't need to implement these unless they are using a non-serde-compatible custom
// converter, in which case they will implement the to/from_payload functions on some wrapper type.
// (See example below)
//
// These should remain as separate traits because the serializable half is dyn-safe and can be
// passed like `&dyn TemporalSerializable` but the deserialize half is not.
pub trait TemporalSerializable {
    fn as_serde(&self) -> Option<&dyn erased_serde::Serialize> {
        None
    }
    fn to_payload(&self) -> Option<Payload> {
        None
    }
}
pub trait TemporalDeserializable {
    fn from_serde(_: &dyn ErasedSerdePayloadConverter, _: Payload) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
    fn from_payload(_: Payload) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

pub struct SafeWorkflowContext {}
pub struct WorkflowContext {}
pub enum WorkflowError {
    // Variants for panic, bubbled up failure, etc
}

#[derive(Debug)]
pub enum WorkflowExitValue<T> {
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
    type Input: TemporalDeserializable + TemporalSerializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The type that implements this definition
    type Implementation: WorkflowImplementation + 'static;
}

// User doesn't really need to understand this trait, as its impl is generated for them
// Actual implementation's input/output types are type-erased, so that they can be stored in a
// collection together (obviously when registering them they need to be)
pub trait WorkflowImplementation {
    fn init(
        input: Payload,
        converter: PayloadConverter,
        ctx: SafeWorkflowContext,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized;
    fn run(
        &mut self,
        ctx: WorkflowContext,
    ) -> BoxFuture<'_, Result<WorkflowExitValue<Payload>, WorkflowError>>;
    fn name() -> &'static str
    where
        Self: Sized;
}

pub trait UpdateDefinition {
    type Input: TemporalDeserializable;
    type Output: TemporalSerializable + 'static;

    fn name() -> &'static str
    where
        Self: Sized;
}

pub struct ActivityContext {}
pub type ActivityError = Box<dyn std::error::Error + Send + Sync + 'static>;

// User doesn't really need to understand this trait, as its impl is generated for them
#[doc(hidden)]
pub trait ActivityDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + 'static;
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
#[doc(hidden)]
pub trait ActivityImplementer {
    fn register_all_static<S: worker_options_builder::State>(
        worker_options: &mut WorkerOptionsBuilder<S>,
    );
    fn register_all_instance<S: worker_options_builder::State>(
        self: Arc<Self>,
        worker_options: &mut WorkerOptionsBuilder<S>,
    );
}

/// Marker trait for activity structs that only have static methods
#[doc(hidden)]
pub trait HasOnlyStaticMethods: ActivityImplementer {}

// Much nicer builder pattern library enables more ergonomic options building
#[derive(bon::Builder)]
#[builder(start_fn = new)]
pub struct ConnectionOptions {
    #[builder(start_fn, into)]
    target: String,
    #[builder(into)]
    api_key: Option<String>,
    // retry options, etc.
}
#[derive(Clone)] // Connections are clonable
pub struct Connection {}
// This is what users could replace with a mock to control the underlying client calls,
// and today is `RawClientProducer` which is private. The `WorkflowService` trait already is exactly
// what you'd expect.
pub trait ConnectionTrait {
    fn workflow_service(&self) -> Box<dyn WorkflowService>;
    // ... etc
}

#[derive(bon::Builder)]
#[builder(start_fn = new)]
pub struct ClientOptions {
    #[builder(start_fn, into)]
    namespace: String,
    #[builder(default)]
    data_converter: DataConverter,
    #[builder(default)]
    interceptors: Vec<Box<dyn OutboundInterceptor>>,
}

// The client is just the client. There's no series of wrapper generics like exist today in the
// client crate. None of that is exposed to the user. They just set things like RetryPolicy and
// it applies. All that other stuff is internalized (and some of it can go away entirely after
// the recent refactoring to make traits dyn-safe)
//
// Clients are cheaply clonable to avoid requiring either Arc or generics in all APIs accepting
// a client,
#[derive(Clone)]
pub struct Client {
    conn: Arc<dyn ConnectionTrait>,
    // Just showing it's object safe/storable - is
    interceptor: Arc<dyn OutboundInterceptor>,
}
// This substitutes for what is today `WorkflowClientTrait`.
//
// It does _not_ need to be dyn-safe:
// because we will not be accepting it as a parameter anywhere (that's only the Connection trait,
// workerclient trait, interceptor traits, and base gRPC traits). Users can create their own
// mocks/impls of this, and store them as concrete types. If they want to store something that swaps
// between our real version and theirs, they can store an enum of `Client | TheirFakeClient` and
// then re-impl the trait/deref.
pub trait ClientTrait {
    // Would return a more specific error type
    fn start_workflow<WD>(
        &self,
        _input: WD::Input,
    ) -> impl Future<Output = Result<WorkflowHandle<WD::Output>, ClientError>>
    where
        WD: WorkflowDefinition;
    // ...etc
}
pub struct StartWorkflowInput {
    workflow: String, // yada yada
    args: Vec<Box<dyn TemporalSerializable>>,
}
// All interceptor traits _do_ need to be dyn safe, and hence lack generic params. Since Rust
// doesn't have reflection anyway, this doesn't really change much, because the trait bounds could
// only be for the serde traits, and so having boxes of them is the same semantics as generics.
// Anything not-yet-serialized will be `TemporalSerializable` and anything deserialized already will
// be `Any`.
pub trait OutboundInterceptor {
    fn start_workflow(
        &self,
        _input: StartWorkflowInput,
    ) -> BoxFuture<'_, Result<WorkflowHandle<Box<dyn std::any::Any>>, ClientError>>;
}
// TODO: It probably makes sense to have more specific error enums per-call, but not going to
// enumerate all of them here, because there will be a lot. EX: Things like
// `WorkflowAlreadyStarted`. These will probably all live in an `errors` submodule.
type ClientError = Box<dyn std::error::Error>;
impl Client {
    fn new(_connection: impl ConnectionTrait, _options: ClientOptions) -> Self {
        todo!()
    }
}

#[derive(Debug)]
pub enum WorkflowResultError {
    // Needs to be different from WorkflowError, since, for example a continue-as-new variant needs
    // to exist there, but should not here. Unless we don't make that an error.
}

pub struct WorkflowHandle<T> {
    _rt: PhantomData<T>,
}
impl<T> WorkflowHandle<T> {
    pub async fn result(self) -> Result<T, WorkflowResultError> {
        todo!()
    }

    pub async fn execute_update<UD>(&self, _input: UD::Input) -> Result<UD::Output, ClientError>
    where
        UD: UpdateDefinition,
    {
        todo!()
    }
}

// A lot of this here is implementation detail, but, was necessary to make sure I could actually
// store the things I will need to store and prove this interface works.
type WorkflowInitializer = fn(
    Payload,
    PayloadConverter,
    SafeWorkflowContext,
) -> Result<Box<dyn WorkflowImplementation>, PayloadConversionError>;
type ActivityInvocation = Box<
    dyn Fn(
        Payload,
        PayloadConverter,
        ActivityContext,
    )
        -> Result<BoxFuture<'static, Result<Payload, ActivityError>>, PayloadConversionError>,
>;

#[derive(bon::Builder)]
#[builder(start_fn = new)]
pub struct WorkerOptions {
    #[builder(start_fn, into)]
    task_queue: String,
    #[builder(field)]
    workflows: HashMap<&'static str, WorkflowInitializer>,
    #[builder(field)]
    activities: HashMap<&'static str, ActivityInvocation>,
}
impl<S: worker_options_builder::State> WorkerOptionsBuilder<S> {
    pub fn register_workflow<WD: WorkflowDefinition>(&mut self) -> &mut Self {
        self.workflows
            .insert(WD::Implementation::name(), move |p, pc, c| {
                Ok(Box::new(WD::Implementation::init(p, pc, c)?))
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
            Box::new(move |p, pc, c| {
                let deserialized = pc.from_payload(p)?;
                let pc2 = pc.clone();
                Ok(AD::execute(None, c, deserialized)
                    .map(move |v| match v {
                        Ok(okv) => pc2.to_payload(&okv),
                        Err(e) => Err(e),
                    })
                    .boxed())
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
            Box::new(move |p, pc, c| {
                let deserialized = pc.from_payload(p)?;
                let pc2 = pc.clone();
                Ok(AD::execute(Some(instance.clone()), c, deserialized)
                    .map(move |v| match v {
                        Ok(okv) => pc2.to_payload(&okv),
                        Err(e) => Err(e),
                    })
                    .boxed())
            }),
        );
        self
    }
}

pub type WorkerRunError = Box<dyn std::error::Error + Send + Sync>;
pub struct Worker {}
impl Worker {
    pub fn new(_client: Client, _options: WorkerOptions) -> Self {
        todo!()
    }

    pub fn shutdown_handle(&self) -> WorkerShutdownHandle {
        todo!()
    }

    pub async fn run(self) -> Result<(), WorkerRunError> {
        todo!()
    }
}
#[derive(Clone)]
pub struct WorkerShutdownHandle {}
impl WorkerShutdownHandle {
    pub fn shutdown(&self) {
        todo!()
    }
}

pub struct WorkflowReplayerOptions {}
pub struct WorkflowReplayer {}
pub struct WorkflowHistory {}
impl WorkflowReplayer {
    pub fn new(_options: WorkflowReplayerOptions) -> Self {
        todo!()
    }
    // We know people have asked to be able to see the workflow result after replay in existing SDKs
    // so it's included here. I don't provide a multiple-at-once version because Rust users are very
    // used to map/collect stuff where they can choose how they want to view results, and it's easy
    // for them to early-return with try_map etc.
    //
    // There's some oddness with the generic here - if this is called in a `map` obviously T
    // can't be heterogenous. The user could provide `Payload` as T and we could use that to
    // mean "don't deserialize", and then they can do it later if they want. Open to suggestions.
    pub fn replay_workflow<T: TemporalDeserializable>(
        _history: &WorkflowHistory,
    ) -> Result<WorkflowExitValue<T>, WorkflowError> {
        todo!()
    }
    // No explicit shutdown, can auto-shutdown on drop
}

// TODO: Tracing config
// TODO: Testing

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
    ) -> Result<WorkflowExitValue<String>, WorkflowError> {
        todo!()
    }

    // #[signal] -- May be sync or async
    pub fn signal(&mut self, _ctx: WorkflowContext, _input: bool) {
        todo!()
    }

    // #[query] -- Glory, finally, immutable-guaranteed queries. Can't be async.
    pub fn query(&self, _ctx: SafeWorkflowContext, _input: String) -> String {
        todo!()
    }

    // #[update] -- May also be sync or async
    pub fn update(&mut self, _ctx: WorkflowContext, _input: String) -> String {
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
// More user code using the definitions from above ===================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let activity_instance = MyActivities {};

    let conn_options = ConnectionOptions::new("localhost:7233")
        .api_key("fake")
        .build();
    let connection = Connection::new(conn_options);

    let payload_converter = PayloadConverter::serde_json();
    let data_converter = DataConverter::new(
        payload_converter,
        DefaultFailureConverter,
        DefaultPayloadCodec,
    );
    let client = Client::new(
        connection,
        ClientOptions::new("my-ns")
            .data_converter(data_converter)
            .build(),
    );

    let mut worker_opts = WorkerOptions::new("my_task_queue");
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

    let worker = Worker::new(client.clone(), worker_opts.build());
    let worker_shutdown = worker.shutdown_handle();
    // Spawning off worker to live in another task - note that running consumes the worker, which
    // is consistent with other SDKs runtime erroring on double-start.
    let worker_task = tokio::spawn(worker.run());

    let handle = client
        .start_workflow::<MyWorkflow>("hi".to_string())
        .await?;
    let _update_res = handle
        .execute_update::<my_workflow::update>("hello".to_string())
        .await?;
    // Activity invocation will also look very similar like
    // ctx.execute_activity::<my_activities::Activity>(input).await?;
    let _result = handle.result().await?;

    worker_shutdown.shutdown();
    worker_task.await.unwrap().unwrap();
    Ok(())
}
// =================================================================================================
// Generated code from procmacros ==================================================================

pub mod my_workflow {
    use super::*;

    #[allow(non_camel_case_types)]
    pub struct signal;
    #[allow(non_camel_case_types)]
    pub struct query;
    #[allow(non_camel_case_types)]
    pub struct update;

    impl UpdateDefinition for update {
        type Input = String;
        type Output = String;

        fn name() -> &'static str
        where
            Self: Sized,
        {
            "update"
        }

        // Some way to invoke -- left for pt2 with more detail on workflows.
    }

    impl WorkflowDefinition for MyWorkflow {
        type Input = String;
        type Output = String;
        type Implementation = MyWorkflow;
    }
    impl WorkflowImplementation for MyWorkflow {
        fn init(
            input: Payload,
            converter: PayloadConverter,
            ctx: SafeWorkflowContext,
        ) -> Result<Self, PayloadConversionError> {
            let deserialzied: <MyWorkflow as WorkflowDefinition>::Input =
                converter.from_payload(input)?;
            Ok(MyWorkflow::new(deserialzied, ctx))
        }

        fn run(
            &mut self,
            ctx: WorkflowContext,
        ) -> BoxFuture<'_, Result<WorkflowExitValue<Payload>, WorkflowError>> {
            self.run(ctx).map(|_| todo!("Serialize output")).boxed()
        }

        fn name() -> &'static str {
            return "MyWorkflow";
        }
    }
}

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
        fn register_all_static<S: worker_options_builder::State>(
            worker_options: &mut WorkerOptionsBuilder<S>,
        ) {
            worker_options.register_activity::<StaticActivity>();
            worker_options.register_activity::<SyncActivity>();
        }
        fn register_all_instance<S: worker_options_builder::State>(
            self: Arc<Self>,
            worker_options: &mut WorkerOptionsBuilder<S>,
        ) {
            worker_options.register_activity_with_instance::<Activity>(self.clone());
        }
    }
}

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
        fn register_all_static<S: worker_options_builder::State>(
            worker_options: &mut WorkerOptionsBuilder<S>,
        ) {
            worker_options.register_activity::<StaticActivity>();
        }
        fn register_all_instance<S: worker_options_builder::State>(
            self: Arc<Self>,
            _: &mut WorkerOptionsBuilder<S>,
        ) {
            unreachable!()
        }
    }
    impl HasOnlyStaticMethods for MyActivitiesStatic {}
}
// =================================================================================================
// Stuff to just make compile work, not worth reviewing ============================================
impl std::error::Error for WorkflowResultError {}
impl std::fmt::Display for WorkflowResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowResultError")
    }
}
impl Connection {
    pub fn new(_: ConnectionOptions) -> Self {
        Self {}
    }
}
impl ConnectionTrait for Connection {
    fn workflow_service(&self) -> Box<dyn WorkflowService> {
        todo!()
    }
}

trait GenericPayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        val: &T,
    ) -> Result<Payload, PayloadConversionError>;
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
    ) -> Result<T, PayloadConversionError>;
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(pc.to_payload(val.as_serde().ok_or_else(|| todo!())?)?)
            }
            PayloadConverter::UseWrappers => Ok(T::to_payload(&val).ok_or_else(|| todo!())?),
            // Fairly clear how this would work
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(T::from_serde(pc.as_ref(), payload).ok_or_else(|| todo!())?)
            }
            PayloadConverter::UseWrappers => Ok(T::from_payload(payload).ok_or_else(|| todo!())?),
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }
}

impl<T> TemporalSerializable for T
where
    T: serde::Serialize,
{
    fn as_serde(&self) -> Option<&dyn erased_serde::Serialize> {
        Some(self)
    }
}
impl<T> TemporalDeserializable for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_serde(pc: &dyn ErasedSerdePayloadConverter, payload: Payload) -> Option<Self>
    where
        Self: Sized,
    {
        erased_serde::deserialize(&mut pc.from_payload(payload).ok()?).ok()
    }
}

// Users can implement for any serde backend they want, we'll provide the standard defaults.
struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value)?;
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: as_json,
        })
    }

    fn from_payload(
        &self,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        // TODO: Would check metadata
        let json_v: serde_json::Value = serde_json::from_slice(&payload.data)?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
pub trait ErasedSerdePayloadConverter: Send + Sync {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError>;

    fn from_payload(
        &self,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// If users want to use a non-serde conversion methodology, they have to wrap the types that want
// to use it specifically. We can potentially add a type-erased way later but for something like
// proto binary I found this to be extremely tricky, since on the deserialization side it
// requires `Default`, and `Default` things can't show up in dyn-safe interfaces.
struct ProstSerializable<T: prost::Message>(T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self) -> Option<Payload> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Some(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"proto/binary".to_vec());
                hm
            },
            data: as_proto,
        })
    }
}
impl<T> TemporalDeserializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn from_payload(p: Payload) -> Option<Self>
    where
        Self: Sized,
    {
        // TODO: Check metadata
        Some(ProstSerializable(T::decode(p.data.as_slice()).ok()?))
    }
}

struct TestProto;
impl UpdateDefinition for TestProto {
    type Input = ProstSerializable<PollWorkflowExecutionUpdateRequest>;
    type Output = ProstSerializable<PollWorkflowExecutionUpdateResponse>;

    fn name() -> &'static str
    where
        Self: Sized,
    {
        "hi"
    }
}

#[derive(Clone)]
pub struct CompositePayloadConverter {
    converters: Vec<PayloadConverter>,
}

impl Default for DataConverter {
    fn default() -> Self {
        todo!()
    }
}
impl FailureConverter for DefaultFailureConverter {
    fn to_failure(&self, _: Box<dyn std::error::Error>) -> Result<Failure, PayloadConversionError> {
        todo!()
    }

    fn from_failure(&self, _: Failure) -> Result<Failure, PayloadConversionError> {
        todo!()
    }
}
impl PayloadCodec for DefaultPayloadCodec {
    fn encode(&self, payloads: Vec<Payload>) -> Vec<Payload> {
        payloads
    }

    fn decode(&self, payloads: Vec<Payload>) -> Vec<Payload> {
        payloads
    }
}

impl ClientTrait for Client {
    async fn start_workflow<WD>(
        &self,
        input: WD::Input,
    ) -> Result<WorkflowHandle<WD::Output>, ClientError>
    where
        WD: WorkflowDefinition,
    {
        // serialize input into raw request
        let inp = input.to_payload().unwrap();
        let _res = self
            .conn
            .workflow_service()
            .start_workflow_execution(
                StartWorkflowExecutionRequest {
                    input: Some(inp.into()),
                    ..Default::default()
                }
                .into_request(),
            )
            .await?;
        // todo: pass in execution info, etc
        Ok(WorkflowHandle { _rt: PhantomData })
    }
}
