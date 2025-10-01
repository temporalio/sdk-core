#![allow(dead_code)]

use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};
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

// Example of user code ============================================================================

pub struct MyWorkflow {
    // Some internal state
}

// #[workflow] -- Can override name
impl MyWorkflow {
    // Optional (but if not specified, the workflow struct must impl Default). Input must
    // be accepted either here, or in run, but not both (allowing input to be consumed and dropped).
    // #[init]
    pub fn new(_input: String, _ctx: SafeWorkflowContext) -> Self {
        todo!()
    }

    // #[run]
    pub async fn run(&mut self, _ctx: WorkflowContext) -> Result<String, WorkflowError> {
        Ok("I ran!".to_string())
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
        Ok("I'm done!")
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

// How does a user define activites/workflows from another language? Easy, just use unimplemented!
// They can also manually implement definition traits if they want.
pub struct MyExternallyDefinedWorkflow;
impl MyExternallyDefinedWorkflow {
    // Here I also show a workflow accepting "multiple arguments". Tuples will be used as the
    // convention for multiple args, as they integrate well with the "definition" types.
    // Since the definition has one associated `Input` type, we need some single type that can
    // represent the multiple args, and tuple is the only thing that fits the bill. We could
    // be clever in the macro, and allow mulitple actual args here, and turn that into a tuple
    // behind the scenes... but that makes invocation confusing and feels too much magic.

    // #[run]
    pub async fn run(
        &mut self,
        _input: (String, bool),
        _ctx: WorkflowContext,
    ) -> Result<String, WorkflowError> {
        // This isn't like todo elsewhere in this file. The user literally just provides
        // `unimplemented!` here for external workflows.
        unimplemented!("Implemented externally")
    }
}

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
// Public types we will expose =====================================================================
// Note that a lot of these are in different crates, which goes where should be fairly clear

// All data conversion functionality belongs inside the new common crate
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

pub enum SerializationContext {
    // Details inside variants elided
    Workflow,
    Activity,
}
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

pub enum PayloadConversionError {
    // implementations return this when they don't match. We don't have an
    // `EncodingPayloadConverter` concept like other SDKs since there is no generic converter for
    // the `UseWrappers` case, but only to/from payload on the serializable itself.
    WrongEncoding,
    EncodingError(Box<dyn std::error::Error>),
}

pub trait FailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error>,
        payload_converter: &PayloadConverter,
        context: &SerializationContext,
    ) -> Result<Failure, PayloadConversionError>;

    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContext,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError>;
}
pub struct DefaultFailureConverter;
pub trait PayloadCodec {
    fn encode(
        &self,
        payloads: Vec<Payload>,
        context: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>>;
    fn decode(
        &self,
        payloads: Vec<Payload>,
        context: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>>;
}
pub struct DefaultPayloadCodec;

// Users don't need to implement these unless they are using a non-serde-compatible custom
// converter, in which case they will implement the to/from_payload functions on some wrapper type
// and can ignore the serde methods. (See example at bottom)
//
// These should remain as separate traits because the serializable half is dyn-safe and can be
// passed like `&dyn TemporalSerializable` but the deserialize half is not due to requiring Sized.
// Also this follows the serde mold which many people are used to, and implementing them separately
// is two lines extra. If we want a trait that requires both, that can easily be added.
pub trait TemporalSerializable {
    fn as_serde(&self) -> Option<&dyn erased_serde::Serialize> {
        None
    }
    fn to_payload(&self, _: &SerializationContext) -> Option<Payload> {
        None
    }
}
pub trait TemporalDeserializable: Sized {
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _: Payload,
        _: &SerializationContext,
    ) -> Option<Self> {
        None
    }
    fn from_payload(_: Payload, _: &SerializationContext) -> Option<Self> {
        None
    }
}
// Isn't serde serializable, therefore not caught by the blanket impl. Will implement
// serialize/deserialize directly to always bypass the converter.
#[derive(Clone)]
pub struct RawValue {
    pub payload: Payload,
}

// Being a trait allows users to test their activity code without needing a whole activity
// environment. Must be dyn-safe.
pub trait ActivityContextTrait: Send + Sync {
    fn heartbeat(&self, details: &dyn TemporalSerializable);
    // ...Cancellation tokens, get_info, etc
}
// This allows us to avoid having `Arc` in the public api, and retain flexibility to change layout
#[derive(Clone)]
#[repr(transparent)]
pub struct ActivityContext {
    // This can actually be an enum of Custom(arc) | InternalActivityContext to avoid dynamic
    // dispatch in the 99% case.
    inner: Arc<dyn ActivityContextTrait>,
}
impl ActivityContext {
    // User can build their own this way
    pub fn from_trait(inner: impl ActivityContextTrait + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

// In many places in this example, I'm aliasing errors to std errors for convenience. In reality,
// most/all of these errors will have more specific variants.
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

pub struct SafeWorkflowContext {
    // Various read-only context functions
}
pub struct WorkflowContext {
    // Everything in safe context + command-issuing functions
}
// I decided that CAN and Cancel should indeed probably be errors, mostly for one reason - which is
// that waiting on `ctx.execute_activity().await?` should automatically bubble out cancel in a way
// that ends the workflow as cancelled unless a user specifically decides not to do that. But - that
// can't happen (at least not without custom `Try` implementations which is still nightly only) if
// the confirm-cancel is on the `Ok` side. That would leave just CAN in there, which seems not worth
// it.
pub enum WorkflowError {
    ContinueAsNew,
    Canceled,
    // Variants for panic, bubbled up failure, etc
}

// User doesn't really need to understand this trait, as its impl is generated for them. All such
// traits can be hidden in docs by default. Traits like this that the client needs access to as
// well as the SDK will live in the common crate.
#[doc(hidden)]
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + TemporalSerializable;
    /// Type of the output of the workflow
    type Output: TemporalSerializable;
    /// The workflow type name
    fn name() -> &'static str
    where
        Self: Sized;
}

// User doesn't really need to understand this trait, as its impl is generated for them
// Actual implementation's input/output types are type-erased, so that they can be stored in a
// collection together (obviously when registering them they need to be)
#[doc(hidden)]
pub trait WorkflowImplementation {
    fn init(
        input: Payload,
        converter: PayloadConverter,
        ctx: SafeWorkflowContext,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized;
    fn run(&mut self, ctx: WorkflowContext) -> BoxFuture<'_, Result<Payload, WorkflowError>>;
}

#[doc(hidden)]
pub trait UpdateDefinition {
    type Input: TemporalDeserializable;
    type Output: TemporalSerializable + 'static;

    fn name() -> &'static str
    where
        Self: Sized;
}

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
    // Just showing it's object safe/storable
    interceptor: Arc<dyn OutboundInterceptor>,
}
// This substitutes for what is today `WorkflowClientTrait`.
//
// It does _not_ need to be dyn-safe (it may not even need to be a trait):
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
// All interceptor traits _do_ need to be dyn safe, and hence lack generic params. Since Rust
// doesn't have reflection anyway, this doesn't really change much, because the trait bounds could
// only be for the serde traits, and so having boxes of them is the same semantics as generics.
// Anything not-yet-serialized will be `TemporalSerializable` and anything deserialized already will
// be `Any`.
pub struct StartWorkflowInput {
    workflow: String, // yada yada
    args: Vec<Box<dyn TemporalSerializable>>,
}
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
    pub fn register_workflow<WD: WorkflowDefinition + WorkflowImplementation + 'static>(
        &mut self,
    ) -> &mut Self {
        self.workflows.insert(WD::name(), move |p, pc, c| {
            Ok(Box::new(WD::init(p, pc, c)?))
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
                let deserialized = pc.from_payload(p, &SerializationContext::Activity)?;
                let pc2 = pc.clone();
                Ok(AD::execute(None, c, deserialized)
                    .map(move |v| match v {
                        Ok(okv) => pc2
                            .to_payload(&okv, &SerializationContext::Activity)
                            .map_err(|_| todo!()),
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
                let deserialized = pc.from_payload(p, &SerializationContext::Activity)?;
                let pc2 = pc.clone();
                Ok(AD::execute(Some(instance.clone()), c, deserialized)
                    .map(move |v| match v {
                        Ok(okv) => pc2
                            .to_payload(&okv, &SerializationContext::Activity)
                            .map_err(|_| todo!()),
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
    ) -> Result<T, WorkflowError> {
        todo!()
    }
    // No explicit shutdown, can auto-shutdown on drop
}

pub struct WorkflowEnvironment {
    // Stores an inner of TimeSkipping | DevServer | Local
}
pub struct WorkflowEnvironmentStartLocalOptions {}
pub struct WorkflowEnvironmentTimeSkippingOptions {}
impl WorkflowEnvironment {
    pub fn start_local(_options: WorkflowEnvironmentStartLocalOptions) -> Self {
        Self {}
    }
    pub async fn start_time_skipping(_options: WorkflowEnvironmentTimeSkippingOptions) -> Self {
        Self {}
    }
    pub async fn from_client(_client: &Client) -> Self {
        Self {}
    }
    pub async fn shutdown(&self) {}
    pub async fn sleep(&self, _dur: Duration) {}
    // ..etc
}

// =================================================================================================
// Generated code from procmacros in the user code at the start ====================================

pub mod my_workflow {
    use super::*;

    #[allow(non_camel_case_types)]
    pub struct signal;
    #[allow(non_camel_case_types)]
    pub struct query;
    #[allow(non_camel_case_types)]
    pub struct update;

    // Signal and query use the same idea
    impl UpdateDefinition for update {
        type Input = String;
        type Output = String;

        fn name() -> &'static str
        where
            Self: Sized,
        {
            "update"
        }

        // ...some way to invoke -- left for pt2 with more detail on workflows.
    }

    impl WorkflowDefinition for MyWorkflow {
        type Input = String;
        type Output = String;
        fn name() -> &'static str {
            "MyWorkflow"
        }
    }
    impl WorkflowImplementation for MyWorkflow {
        fn init(
            input: Payload,
            converter: PayloadConverter,
            ctx: SafeWorkflowContext,
        ) -> Result<Self, PayloadConversionError> {
            let deserialzied: <MyWorkflow as WorkflowDefinition>::Input =
                converter.from_payload(input, &SerializationContext::Workflow)?;
            Ok(MyWorkflow::new(deserialzied, ctx))
        }

        fn run(&mut self, ctx: WorkflowContext) -> BoxFuture<'_, Result<Payload, WorkflowError>> {
            self.run(ctx).map(|_| todo!("Serialize output")).boxed()
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

pub mod my_externally_defined_workflow {
    use super::*;

    impl WorkflowDefinition for MyExternallyDefinedWorkflow {
        type Input = (String, bool);
        type Output = String;
        fn name() -> &'static str {
            "MyExternallyDefinedWorkflow"
        }
    }
    // Workflow implementation literally just not implemented here - so it's actually not
    // registerable as a workflow with a worker.
}
// =================================================================================================
// Everything below mostly to make compile work, not needed for review but feel free to look =======
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
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<T, PayloadConversionError>;
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        val: &T,
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(pc.to_payload(val.as_serde().ok_or_else(|| todo!())?, context)?)
            }
            PayloadConverter::UseWrappers => {
                Ok(T::to_payload(val, context).ok_or_else(|| todo!())?)
            }
            // Fairly clear how this would work
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<T, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(T::from_serde(pc.as_ref(), payload, context).ok_or_else(|| todo!())?)
            }
            PayloadConverter::UseWrappers => {
                Ok(T::from_payload(payload, context).ok_or_else(|| todo!())?)
            }
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }
}

// These blanket traits we can allow users to opt-out of with a compile time flag, which will allow
// them to implement their own blanket traits... but I don't think that's actually useful in any
// way. They can never add their own blanket impls since they don't own the traits.
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
    fn from_serde(
        pc: &dyn ErasedSerdePayloadConverter,
        payload: Payload,
        context: &SerializationContext,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        erased_serde::deserialize(&mut pc.from_payload(payload, context).ok()?).ok()
    }
}

// Users can implement for any serde backend they want, we'll provide the standard defaults.
// If they need to use custom serialization contexts, they'll need to provide their own impl, as
// there's no sensible place to insert a generic hook (and these impls are very easy to make).
struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
        _context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value).map_err(|_| todo!())?;
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
        _context: &SerializationContext,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        // TODO: Would check metadata
        let json_v: serde_json::Value =
            serde_json::from_slice(&payload.data).map_err(|_| todo!())?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
pub trait ErasedSerdePayloadConverter: Send + Sync {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// The below we'll include in the SDK, but also serves as an example of something a user might
// write. If users want to use a non-serde conversion methodology, they have to wrap the types that
// want to use it specifically. We can potentially add a type-erased way later but for something
// like proto binary I found this to be extremely tricky, since on the deserialization side it
// requires `Default`, and `Default` things can't show up in dyn-safe interfaces.
struct ProstSerializable<T: prost::Message>(T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext) -> Option<Payload> {
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
    fn from_payload(p: Payload, _: &SerializationContext) -> Option<Self>
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
    fn to_failure(
        &self,
        _: Box<dyn std::error::Error>,
        _: &PayloadConverter,
        _: &SerializationContext,
    ) -> Result<Failure, PayloadConversionError> {
        todo!()
    }
    fn to_error(
        &self,
        _: Failure,
        _: &PayloadConverter,
        _: &SerializationContext,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError> {
        todo!()
    }
}
impl PayloadCodec for DefaultPayloadCodec {
    fn encode(
        &self,
        payloads: Vec<Payload>,
        _: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
    fn decode(
        &self,
        payloads: Vec<Payload>,
        _: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
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
        let inp = input.to_payload(&SerializationContext::Workflow).unwrap();
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
