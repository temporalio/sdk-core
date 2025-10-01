use crate::{
    ByteArray, ByteArrayRef, ByteArrayRefArray, UserDataHandle, client::Client, runtime::Runtime,
};
use anyhow::{Context, bail};
use crossbeam_utils::atomic::AtomicCell;
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    num::NonZero,
    sync::Arc,
    time::Duration,
};
use temporal_sdk_core::{
    WorkerConfigBuilder,
    replay::{HistoryForReplay, ReplayWorkerInput},
};
use temporal_sdk_core_api::{
    Worker as CoreWorker,
    errors::{PollError, WorkflowErrorType},
    worker::{
        SlotInfoTrait, SlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext,
        SlotSupplierPermit,
    },
};
use temporal_sdk_core_protos::{
    coresdk::{
        ActivityHeartbeat, ActivityTaskCompletion, nexus::NexusTaskCompletion,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::history::v1::History,
};
use tokio::sync::{
    Notify,
    mpsc::{Sender, channel},
};
use tokio_stream::wrappers::ReceiverStream;

#[repr(C)]
pub struct WorkerOptions {
    pub namespace: ByteArrayRef,
    pub task_queue: ByteArrayRef,
    pub versioning_strategy: WorkerVersioningStrategy,
    pub identity_override: ByteArrayRef,
    pub max_cached_workflows: u32,
    pub tuner: TunerHolder,
    pub no_remote_activities: bool,
    pub sticky_queue_schedule_to_start_timeout_millis: u64,
    pub max_heartbeat_throttle_interval_millis: u64,
    pub default_heartbeat_throttle_interval_millis: u64,
    pub max_activities_per_second: f64,
    pub max_task_queue_activities_per_second: f64,
    pub graceful_shutdown_period_millis: u64,
    pub workflow_task_poller_behavior: PollerBehavior,
    pub nonsticky_to_sticky_poll_ratio: f32,
    pub activity_task_poller_behavior: PollerBehavior,
    pub nexus_task_poller_behavior: PollerBehavior,
    pub nondeterminism_as_workflow_fail: bool,
    pub nondeterminism_as_workflow_fail_for_types: ByteArrayRefArray,
}

#[repr(C)]
pub struct PollerBehaviorSimpleMaximum {
    pub simple_maximum: usize,
}

#[repr(C)]
pub struct PollerBehaviorAutoscaling {
    pub minimum: usize,
    pub maximum: usize,
    pub initial: usize,
}

// Only one of simple_maximum and autoscaling can be present.
#[repr(C)]
pub struct PollerBehavior {
    pub simple_maximum: *const PollerBehaviorSimpleMaximum,
    pub autoscaling: *const PollerBehaviorAutoscaling,
}

impl TryFrom<&PollerBehavior> for temporal_sdk_core_api::worker::PollerBehavior {
    type Error = anyhow::Error;
    fn try_from(value: &PollerBehavior) -> Result<Self, Self::Error> {
        if !value.simple_maximum.is_null() && !value.autoscaling.is_null() {
            bail!("simple_maximum and autoscaling cannot both be non-null values");
        }
        if let Some(value) = unsafe { value.simple_maximum.as_ref() } {
            return Ok(
                temporal_sdk_core_api::worker::PollerBehavior::SimpleMaximum(value.simple_maximum),
            );
        } else if let Some(value) = unsafe { value.autoscaling.as_ref() } {
            return Ok(temporal_sdk_core_api::worker::PollerBehavior::Autoscaling {
                minimum: value.minimum,
                maximum: value.maximum,
                initial: value.initial,
            });
        }
        bail!("simple_maximum and autoscaling cannot both be null values");
    }
}

#[repr(C)]
pub enum WorkerVersioningStrategy {
    None(WorkerVersioningNone),
    DeploymentBased(WorkerDeploymentOptions),
    LegacyBuildIdBased(LegacyBuildIdBasedStrategy),
}

#[repr(C)]
pub struct WorkerVersioningNone {
    pub build_id: ByteArrayRef,
}

#[repr(C)]
pub struct WorkerDeploymentOptions {
    pub version: WorkerDeploymentVersion,
    pub use_worker_versioning: bool,
    pub default_versioning_behavior: i32,
}

#[repr(C)]
pub struct LegacyBuildIdBasedStrategy {
    pub build_id: ByteArrayRef,
}

#[repr(C)]
pub struct WorkerDeploymentVersion {
    pub deployment_name: ByteArrayRef,
    pub build_id: ByteArrayRef,
}

#[repr(C)]
pub struct TunerHolder {
    pub workflow_slot_supplier: SlotSupplier,
    pub activity_slot_supplier: SlotSupplier,
    pub local_activity_slot_supplier: SlotSupplier,
    pub nexus_task_slot_supplier: SlotSupplier,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub enum SlotSupplier {
    FixedSize(FixedSizeSlotSupplier),
    ResourceBased(ResourceBasedSlotSupplier),
    Custom(CustomSlotSupplierCallbacksImpl),
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FixedSizeSlotSupplier {
    pub num_slots: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ResourceBasedSlotSupplier {
    pub minimum_slots: usize,
    pub maximum_slots: usize,
    pub ramp_throttle_ms: u64,
    pub tuner_options: ResourceBasedTunerOptions,
}

struct CustomSlotSupplier<SK> {
    inner: CustomSlotSupplierCallbacksImpl,
    _pd: std::marker::PhantomData<SK>,
}

unsafe impl<SK> Send for CustomSlotSupplier<SK> {}
unsafe impl<SK> Sync for CustomSlotSupplier<SK> {}

pub type CustomSlotSupplierReserveCallback = unsafe extern "C" fn(
    ctx: *const SlotReserveCtx,
    completion_ctx: *const SlotReserveCompletionCtx,
    user_data: *mut libc::c_void,
);
pub type CustomSlotSupplierCancelReserveCallback = unsafe extern "C" fn(
    completion_ctx: *const SlotReserveCompletionCtx,
    user_data: *mut libc::c_void,
);
pub type CustomSlotSupplierTryReserveCallback =
    unsafe extern "C" fn(ctx: *const SlotReserveCtx, user_data: *mut libc::c_void) -> usize;
pub type CustomSlotSupplierMarkUsedCallback =
    unsafe extern "C" fn(ctx: *const SlotMarkUsedCtx, user_data: *mut libc::c_void);
pub type CustomSlotSupplierReleaseCallback =
    unsafe extern "C" fn(ctx: *const SlotReleaseCtx, user_data: *mut libc::c_void);
pub type CustomSlotSupplierAvailableSlotsCallback =
    Option<unsafe extern "C" fn(available_slots: *mut usize, user_data: *mut libc::c_void) -> bool>;
pub type CustomSlotSupplierFreeCallback =
    unsafe extern "C" fn(userimpl: *const CustomSlotSupplierCallbacks);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CustomSlotSupplierCallbacksImpl(pub *const CustomSlotSupplierCallbacks);

#[repr(C)]
pub struct CustomSlotSupplierCallbacks {
    /// Called to initiate asynchronous slot reservation. `ctx` contains information about
    /// reservation request. The pointer is only valid for the duration of the function call; the
    /// implementation should copy the data out of it for later use, and return as soon as possible.
    ///
    /// When slot is reserved, the implementation should call [`temporal_core_complete_async_reserve`]
    /// with the same `completion_ctx` as passed to this function. Reservation cannot be cancelled
    /// by Lang, but it can be cancelled by Core through [`cancel_reserve`](Self::cancel_reserve)
    /// callback. If reservation was cancelled, [`temporal_core_complete_async_cancel_reserve`]
    /// should be called instead.
    ///
    /// Slot reservation cannot error. The implementation should recover from errors and keep trying
    /// to reserve a slot until it eventually succeeds, or until reservation is cancelled by Core.
    pub reserve: CustomSlotSupplierReserveCallback,
    /// Called to cancel slot reservation. `completion_ctx` specifies which reservation is being
    /// cancelled; the matching [`reserve`] call was made with the same `completion_ctx`. After
    /// cancellation, the implementation should call [`temporal_core_complete_async_cancel_reserve`]
    /// with the same `completion_ctx`. Calling [`temporal_core_complete_async_reserve`] is not
    /// needed after cancellation.
    pub cancel_reserve: CustomSlotSupplierCancelReserveCallback,
    /// Called to try an immediate slot reservation. The callback should return 0 if immediate
    /// reservation is not currently possible, or permit ID if reservation was successful. Permit ID
    /// is arbitrary, but must be unique among live reservations as it's later used for `mark_used`
    /// and `release` callbacks.
    pub try_reserve: CustomSlotSupplierTryReserveCallback,
    /// Called after successful reservation to mark slot as used. See [`SlotSupplier`](temporal_sdk_core_api::worker::SlotSupplier)
    /// trait for details.
    pub mark_used: CustomSlotSupplierMarkUsedCallback,
    /// Called to free a previously reserved slot.
    pub release: CustomSlotSupplierReleaseCallback,
    /// Called to retrieve the number of available slots if known. If the implementation knows how
    /// many slots are available at the moment, it should set the value behind the `available_slots`
    /// pointer and return true. If that number is unknown, it should return false.
    ///
    /// This function pointer can be set to null. It will be treated as if the number of available
    /// slots is never known.
    pub available_slots: CustomSlotSupplierAvailableSlotsCallback,
    /// Called when the slot supplier is being dropped. All resources should be freed.
    pub free: CustomSlotSupplierFreeCallback,
    /// Passed as an extra argument to the callbacks.
    pub user_data: *mut libc::c_void,
}

impl CustomSlotSupplierCallbacksImpl {
    fn into_ss<SK: SlotKind + Send + Sync + 'static>(
        self,
    ) -> Arc<dyn temporal_sdk_core_api::worker::SlotSupplier<SlotKind = SK> + Send + Sync + 'static>
    {
        Arc::new(CustomSlotSupplier {
            inner: self,
            _pd: Default::default(),
        })
    }
}
impl Drop for CustomSlotSupplierCallbacks {
    fn drop(&mut self) {
        unsafe {
            (self.free)(&*self);
        }
    }
}

#[repr(C)]
pub enum SlotKindType {
    WorkflowSlotKindType,
    ActivitySlotKindType,
    LocalActivitySlotKindType,
    NexusSlotKindType,
}

#[repr(C)]
pub struct SlotReserveCtx {
    pub slot_type: SlotKindType,
    pub task_queue: ByteArrayRef,
    pub worker_identity: ByteArrayRef,
    pub worker_build_id: ByteArrayRef,
    pub is_sticky: bool,
}
unsafe impl Send for SlotReserveCtx {}

#[repr(C)]
pub enum SlotInfo {
    WorkflowSlotInfo {
        workflow_type: ByteArrayRef,
        is_sticky: bool,
    },
    ActivitySlotInfo {
        activity_type: ByteArrayRef,
    },
    LocalActivitySlotInfo {
        activity_type: ByteArrayRef,
    },
    NexusSlotInfo {
        operation: ByteArrayRef,
        service: ByteArrayRef,
    },
}

#[repr(C)]
pub struct SlotMarkUsedCtx {
    pub slot_info: SlotInfo,
    /// Lang-issued permit ID.
    pub slot_permit: usize,
}

#[repr(C)]
pub struct SlotReleaseCtx {
    pub slot_info: *const SlotInfo,
    /// Lang-issued permit ID.
    pub slot_permit: usize,
}

pub struct SlotReserveCompletionCtx {
    state: AtomicCell<SlotReserveOperationState>,
    notify: Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotReserveOperationState {
    Pending,
    Cancelled,
    Completed(NonZero<usize>),
}

struct CancelReserveGuard<'a, SK: SlotKind + Send + Sync> {
    slot_supplier: &'a CustomSlotSupplier<SK>,
    completion_ctx: Arc<SlotReserveCompletionCtx>,
    completed: bool,
}

impl<'a, SK: SlotKind + Send + Sync> Drop for CancelReserveGuard<'a, SK> {
    fn drop(&mut self) {
        // do not cancel if already completed
        if !self.completed {
            let state = self
                .completion_ctx
                .state
                .swap(SlotReserveOperationState::Cancelled);
            unsafe {
                let inner = &*self.slot_supplier.inner.0;
                match state {
                    SlotReserveOperationState::Cancelled => (), // should not be possible but it's safe
                    SlotReserveOperationState::Pending => {
                        (inner.cancel_reserve)(Arc::as_ptr(&self.completion_ctx), inner.user_data)
                    }
                    SlotReserveOperationState::Completed(slot_permit) => (inner.release)(
                        &SlotReleaseCtx {
                            slot_info: std::ptr::null(),
                            slot_permit: slot_permit.into(),
                        },
                        inner.user_data,
                    ),
                }
            }
        }
    }
}

//unsafe impl<'a, SK: SlotKind + Send + Sync> Send for CancelReserveGuard<'a, SK> {}

#[async_trait::async_trait]
impl<SK: SlotKind + Send + Sync> temporal_sdk_core_api::worker::SlotSupplier
    for CustomSlotSupplier<SK>
{
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        let ctx = Self::convert_reserve_ctx(ctx);
        let completion_ctx = Arc::new(SlotReserveCompletionCtx {
            state: AtomicCell::new(SlotReserveOperationState::Pending),
            notify: Notify::new(),
        });
        unsafe {
            let inner = &*self.inner.0;
            (inner.reserve)(&ctx, Arc::into_raw(completion_ctx.clone()), inner.user_data);
        }
        let mut guard = CancelReserveGuard {
            slot_supplier: self,
            completion_ctx,
            completed: false,
        };
        // if the future is dropped before this await resolves, the guard is dropped which triggers cancellation
        guard.completion_ctx.notify.notified().await;
        guard.completed = true;
        match guard.completion_ctx.state.load() {
            SlotReserveOperationState::Completed(permit_id) => {
                SlotSupplierPermit::with_user_data(permit_id)
            }
            other => panic!("Unexpected slot reservation state: expected Completed, got {other:?}"),
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        let ctx = Self::convert_reserve_ctx(ctx);
        let permit_id = unsafe { ((*self.inner.0).try_reserve)(&ctx, (*self.inner.0).user_data) };
        if permit_id == 0 {
            None
        } else {
            Some(SlotSupplierPermit::with_user_data(permit_id))
        }
    }

    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {
        let ctx = SlotMarkUsedCtx {
            slot_info: Self::convert_slot_info(ctx.info().downcast()),
            slot_permit: ctx.permit().user_data::<usize>().copied().unwrap_or(0),
        };
        unsafe {
            let inner = &*self.inner.0;
            (inner.mark_used)(&ctx, inner.user_data);
        }
    }

    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        let mut info_ptr = std::ptr::null();
        let converted_slot_info = ctx.info().map(|i| Self::convert_slot_info(i.downcast()));
        if let Some(ref converted) = converted_slot_info {
            info_ptr = converted;
        }
        let ctx = SlotReleaseCtx {
            slot_info: info_ptr,
            slot_permit: ctx.permit().user_data::<usize>().copied().unwrap_or(0),
        };
        unsafe {
            let inner = &*self.inner.0;
            (inner.release)(&ctx, inner.user_data);
        }
    }

    fn available_slots(&self) -> Option<usize> {
        unsafe {
            let inner = &*self.inner.0;
            inner.available_slots.and_then(|f| {
                let mut available_slots = 0;
                f(&mut available_slots, inner.user_data).then_some(available_slots)
            })
        }
    }
}

impl<SK: SlotKind + Send + Sync> CustomSlotSupplier<SK> {
    fn convert_reserve_ctx(ctx: &dyn SlotReservationContext) -> SlotReserveCtx {
        SlotReserveCtx {
            slot_type: match SK::kind() {
                temporal_sdk_core_api::worker::SlotKindType::Workflow => {
                    SlotKindType::WorkflowSlotKindType
                }
                temporal_sdk_core_api::worker::SlotKindType::Activity => {
                    SlotKindType::ActivitySlotKindType
                }
                temporal_sdk_core_api::worker::SlotKindType::LocalActivity => {
                    SlotKindType::LocalActivitySlotKindType
                }
                temporal_sdk_core_api::worker::SlotKindType::Nexus => {
                    SlotKindType::NexusSlotKindType
                }
            },
            task_queue: ctx.task_queue().into(),
            worker_identity: ctx.worker_identity().into(),
            worker_build_id: if let Some(vers) = ctx.worker_deployment_version() {
                vers.build_id.as_str().into()
            } else {
                ByteArrayRef::empty()
            },
            is_sticky: ctx.is_sticky(),
        }
    }

    fn convert_slot_info(info: temporal_sdk_core_api::worker::SlotInfo) -> SlotInfo {
        match info {
            temporal_sdk_core_api::worker::SlotInfo::Workflow(w) => SlotInfo::WorkflowSlotInfo {
                workflow_type: w.workflow_type.as_str().into(),
                is_sticky: w.is_sticky,
            },
            temporal_sdk_core_api::worker::SlotInfo::Activity(a) => SlotInfo::ActivitySlotInfo {
                activity_type: a.activity_type.as_str().into(),
            },
            temporal_sdk_core_api::worker::SlotInfo::LocalActivity(a) => {
                SlotInfo::LocalActivitySlotInfo {
                    activity_type: a.activity_type.as_str().into(),
                }
            }
            temporal_sdk_core_api::worker::SlotInfo::Nexus(n) => SlotInfo::NexusSlotInfo {
                operation: n.operation.as_str().into(),
                service: n.service.as_str().into(),
            },
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct ResourceBasedTunerOptions {
    pub target_memory_usage: f64,
    pub target_cpu_usage: f64,
}

#[derive(Clone)]
pub struct Worker {
    worker: Option<Arc<temporal_sdk_core::Worker>>,
    runtime: Runtime,
}

/// Only runtime or fail will be non-null. Whichever is must be freed when done.
#[repr(C)]
pub struct WorkerOrFail {
    pub worker: *mut Worker,
    pub fail: *const ByteArray,
}

pub struct WorkerReplayPusher {
    tx: Sender<HistoryForReplay>,
}

#[repr(C)]
pub struct WorkerReplayerOrFail {
    pub worker: *mut Worker,
    pub worker_replay_pusher: *mut WorkerReplayPusher,
    pub fail: *const ByteArray,
}

#[repr(C)]
pub struct WorkerReplayPushResult {
    pub fail: *const ByteArray,
}

/// Should be called at the top of any C bridge call that will need to use the tokio runtime from
/// the Core runtime provided as an argument. Also sets up tracing for the duration of the scope in
/// which the call was made.
macro_rules! enter_sync {
    ($runtime:expr) => {
        let _trace_guard = $runtime
            .core
            .telemetry()
            .trace_subscriber()
            .map(|s| tracing::subscriber::set_default(s));
        let _guard = $runtime.core.tokio_handle().enter();
    };
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_new(
    client: *mut Client,
    options: *const WorkerOptions,
) -> WorkerOrFail {
    let client = unsafe { &mut *client };
    enter_sync!(client.runtime);
    let options = unsafe { &*options };

    let (worker, fail) = match options.try_into() {
        Err(err) => (
            std::ptr::null_mut(),
            client
                .runtime
                .alloc_utf8(&format!("Invalid options: {err}"))
                .into_raw()
                .cast_const(),
        ),
        Ok(config) => match temporal_sdk_core::init_worker(
            &client.runtime.core,
            config,
            client.core.clone().into_inner(),
        ) {
            Err(err) => (
                std::ptr::null_mut(),
                client
                    .runtime
                    .alloc_utf8(&format!("Worker start failed: {err}"))
                    .into_raw()
                    .cast_const(),
            ),
            Ok(worker) => (
                Box::into_raw(Box::new(Worker {
                    worker: Some(Arc::new(worker)),
                    runtime: client.runtime.clone(),
                })),
                std::ptr::null(),
            ),
        },
    };
    WorkerOrFail { worker, fail }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_free(worker: *mut Worker) {
    if worker.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(worker);
    }
}

/// If fail is present, it must be freed.
pub type WorkerCallback =
    unsafe extern "C" fn(user_data: *mut libc::c_void, fail: *const ByteArray);

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_validate(
    worker: *mut Worker,
    user_data: *mut libc::c_void,
    callback: WorkerCallback,
) {
    let worker = unsafe { &*worker };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let fail = match core_worker.validate().await {
            Ok(_) => std::ptr::null(),
            Err(err) => worker
                .runtime
                .clone()
                .alloc_utf8(&format!("Worker validation failed: {err}"))
                .into_raw()
                .cast_const(),
        };
        unsafe {
            callback(user_data.into(), fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_replace_client(
    worker: *mut Worker,
    new_client: *mut Client,
) {
    let worker = unsafe { &*worker };
    let core_worker = worker.worker.as_ref().expect("missing worker").clone();
    let client = unsafe { &*new_client };
    core_worker.replace_client(client.core.get_client().clone());
}

/// If success or fail are present, they must be freed. They will both be null
/// if this is a result of a poll shutdown.
pub type WorkerPollCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *const ByteArray,
    fail: *const ByteArray,
);

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_poll_workflow_activation(
    worker: *mut Worker,
    user_data: *mut libc::c_void,
    callback: WorkerPollCallback,
) {
    let worker = unsafe { &*worker };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let (success, fail) = match core_worker.poll_workflow_activation().await {
            Ok(act) => (
                ByteArray::from_vec(act.encode_to_vec())
                    .into_raw()
                    .cast_const(),
                std::ptr::null(),
            ),
            Err(PollError::ShutDown) => (std::ptr::null(), std::ptr::null()),
            Err(err) => (
                std::ptr::null(),
                worker
                    .runtime
                    .clone()
                    .alloc_utf8(&format!("Workflow polling failure: {err}"))
                    .into_raw()
                    .cast_const(),
            ),
        };
        unsafe {
            callback(user_data.into(), success, fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_poll_activity_task(
    worker: *mut Worker,
    user_data: *mut libc::c_void,
    callback: WorkerPollCallback,
) {
    let worker = unsafe { &*worker };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let (success, fail) = match core_worker.poll_activity_task().await {
            Ok(act) => (
                ByteArray::from_vec(act.encode_to_vec())
                    .into_raw()
                    .cast_const(),
                std::ptr::null(),
            ),
            Err(PollError::ShutDown) => (std::ptr::null(), std::ptr::null()),
            Err(err) => (
                std::ptr::null(),
                worker
                    .runtime
                    .clone()
                    .alloc_utf8(&format!("Activity polling failure: {err}"))
                    .into_raw()
                    .cast_const(),
            ),
        };
        unsafe {
            callback(user_data.into(), success, fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_poll_nexus_task(
    worker: *mut Worker,
    user_data: *mut libc::c_void,
    callback: WorkerPollCallback,
) {
    let worker = unsafe { &*worker };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let (success, fail) = match core_worker.poll_nexus_task().await {
            Ok(task) => (
                ByteArray::from_vec(task.encode_to_vec())
                    .into_raw()
                    .cast_const(),
                std::ptr::null(),
            ),
            Err(PollError::ShutDown) => (std::ptr::null(), std::ptr::null()),
            Err(err) => (
                std::ptr::null(),
                worker
                    .runtime
                    .clone()
                    .alloc_utf8(&format!("Nexus polling failure: {err}"))
                    .into_raw()
                    .cast_const(),
            ),
        };
        unsafe {
            callback(user_data.into(), success, fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_complete_workflow_activation(
    worker: *mut Worker,
    completion: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: WorkerCallback,
) {
    let worker = unsafe { &*worker };
    let completion = match WorkflowActivationCompletion::decode(completion.to_slice()) {
        Ok(completion) => completion,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    worker
                        .runtime
                        .clone()
                        .alloc_utf8(&format!("Workflow task decode failure: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let fail = match core_worker.complete_workflow_activation(completion).await {
            Ok(_) => std::ptr::null(),
            Err(err) => worker
                .runtime
                .clone()
                .alloc_utf8(&format!("Workflow completion failure: {err}"))
                .into_raw()
                .cast_const(),
        };
        unsafe {
            callback(user_data.into(), fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_complete_activity_task(
    worker: *mut Worker,
    completion: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: WorkerCallback,
) {
    let worker = unsafe { &*worker };
    let completion = match ActivityTaskCompletion::decode(completion.to_slice()) {
        Ok(completion) => completion,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    worker
                        .runtime
                        .clone()
                        .alloc_utf8(&format!("Activity task decode failure: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let fail = match core_worker.complete_activity_task(completion).await {
            Ok(_) => std::ptr::null(),
            Err(err) => worker
                .runtime
                .clone()
                .alloc_utf8(&format!("Activity completion failure: {err}"))
                .into_raw()
                .cast_const(),
        };
        unsafe {
            callback(user_data.into(), fail);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_complete_nexus_task(
    worker: *mut Worker,
    completion: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: WorkerCallback,
) {
    let worker = unsafe { &*worker };
    let completion = match NexusTaskCompletion::decode(completion.to_slice()) {
        Ok(completion) => completion,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    worker
                        .runtime
                        .clone()
                        .alloc_utf8(&format!("Nexus task decode failure: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    let user_data = UserDataHandle(user_data);
    let core_worker = worker.worker.as_ref().unwrap().clone();
    worker.runtime.core.tokio_handle().spawn(async move {
        let fail = match core_worker.complete_nexus_task(completion).await {
            Ok(_) => std::ptr::null(),
            Err(err) => worker
                .runtime
                .clone()
                .alloc_utf8(&format!("Nexus completion failure: {err}"))
                .into_raw()
                .cast_const(),
        };
        unsafe {
            callback(user_data.into(), fail);
        }
    });
}

/// Returns error if any. Must be freed if returned.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_record_activity_heartbeat(
    worker: *mut Worker,
    heartbeat: ByteArrayRef,
) -> *const ByteArray {
    let worker = unsafe { &*worker };
    enter_sync!(worker.runtime);
    match ActivityHeartbeat::decode(heartbeat.to_slice()) {
        Ok(heartbeat) => {
            worker
                .worker
                .as_ref()
                .unwrap()
                .record_activity_heartbeat(heartbeat);
            std::ptr::null()
        }
        Err(err) => worker
            .runtime
            .clone()
            .alloc_utf8(&format!("Activity heartbeat decode failure: {err}"))
            .into_raw(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_request_workflow_eviction(
    worker: *mut Worker,
    run_id: ByteArrayRef,
) {
    let worker = unsafe { &*worker };
    enter_sync!(worker.runtime);
    worker
        .worker
        .as_ref()
        .unwrap()
        .request_workflow_eviction(run_id.to_str());
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_initiate_shutdown(worker: *mut Worker) {
    let worker = unsafe { &*worker };
    worker.worker.as_ref().unwrap().initiate_shutdown();
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_finalize_shutdown(
    worker: *mut Worker,
    user_data: *mut libc::c_void,
    callback: WorkerCallback,
) {
    let worker = unsafe { &mut *worker };
    let user_data = UserDataHandle(user_data);
    worker.runtime.core.tokio_handle().spawn(async move {
        // Take the worker out of the option and leave None. This should be the
        // only reference remaining to the worker so try_unwrap will work.
        let core_worker = match Arc::try_unwrap(worker.worker.take().unwrap()) {
            Ok(core_worker) => core_worker,
            Err(arc) => {
                unsafe {
                    callback(
                        user_data.into(),
                        worker
                            .runtime
                            .clone()
                            .alloc_utf8(&format!(
                                "Cannot finalize, expected 1 reference, got {}",
                                Arc::strong_count(&arc)
                            ))
                            .into_raw(),
                    );
                }
                return;
            }
        };
        core_worker.finalize_shutdown().await;
        unsafe {
            callback(user_data.into(), std::ptr::null());
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_replayer_new(
    runtime: *mut Runtime,
    options: *const WorkerOptions,
) -> WorkerReplayerOrFail {
    let runtime = unsafe { &mut *runtime };
    enter_sync!(runtime);
    let options = unsafe { &*options };

    let (worker, worker_replay_pusher, fail) = match options.try_into() {
        Err(err) => (
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            runtime
                .alloc_utf8(&format!("Invalid options: {err}"))
                .into_raw()
                .cast_const(),
        ),
        Ok(config) => {
            let (tx, rx) = channel(1);
            match temporal_sdk_core::init_replay_worker(ReplayWorkerInput::new(
                config,
                ReceiverStream::new(rx),
            )) {
                Err(err) => (
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    runtime
                        .alloc_utf8(&format!("Worker replay init failed: {err}"))
                        .into_raw()
                        .cast_const(),
                ),
                Ok(worker) => (
                    Box::into_raw(Box::new(Worker {
                        worker: Some(Arc::new(worker)),
                        runtime: runtime.clone(),
                    })),
                    Box::into_raw(Box::new(WorkerReplayPusher { tx })),
                    std::ptr::null(),
                ),
            }
        }
    };
    WorkerReplayerOrFail {
        worker,
        worker_replay_pusher,
        fail,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_replay_pusher_free(
    worker_replay_pusher: *mut WorkerReplayPusher,
) {
    unsafe {
        let _ = Box::from_raw(worker_replay_pusher);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_worker_replay_push(
    worker: *mut Worker,
    worker_replay_pusher: *mut WorkerReplayPusher,
    workflow_id: ByteArrayRef,
    history: ByteArrayRef,
) -> WorkerReplayPushResult {
    let worker = unsafe { &mut *worker };
    let worker_replay_pusher = unsafe { &*worker_replay_pusher };
    let workflow_id = workflow_id.to_string();
    match History::decode(history.to_slice()) {
        Err(err) => {
            return WorkerReplayPushResult {
                fail: worker
                    .runtime
                    .alloc_utf8(&format!("Worker replay init failed: {err}"))
                    .into_raw()
                    .cast_const(),
            };
        }
        Ok(history) => worker.runtime.core.tokio_handle().spawn(async move {
            // Intentionally ignoring error here
            let _ = worker_replay_pusher
                .tx
                .send(HistoryForReplay::new(history, workflow_id))
                .await;
        }),
    };
    WorkerReplayPushResult {
        fail: std::ptr::null(),
    }
}

/// Completes asynchronous slot reservation started by a call to [`CustomSlotSupplierCallbacks::reserve`].
///
/// `completion_ctx` must be the same as the one passed to the matching `reserve` call.
/// `permit_id` is arbitrary, but must be unique among live reservations as it's later used for
/// `mark_used` and `release` callbacks.
///
/// This function returns true if the reservation was completed successfully, or false if the
/// reservation was cancelled before completion. If this function returns false, the implementation
/// should call [`temporal_core_complete_async_cancel_reserve`] with the same `completion_ctx`.
///
/// **Caution:** if this function returns true, `completion_ctx` gets freed. Afterwards, calling
/// either [`temporal_core_complete_async_reserve`] or [`temporal_core_complete_async_cancel_reserve`]
/// with the same `completion_ctx` will cause **memory corruption!**
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_complete_async_reserve(
    completion_ctx: *const SlotReserveCompletionCtx,
    permit_id: usize,
) -> bool {
    if completion_ctx.is_null() {
        panic!("completion_ctx is null");
    }
    let permit_id =
        NonZero::new(permit_id).expect("permit_id cannot be 0 on successful reservation");
    let prev_state = unsafe {
        // Not turning completion_ctx into Arc yet as we only want to deallocate it on success
        (*completion_ctx).state.compare_exchange(
            SlotReserveOperationState::Pending,
            SlotReserveOperationState::Completed(permit_id),
        )
    };
    match prev_state {
        Ok(_) => {
            let completion_ctx = unsafe { Arc::from_raw(completion_ctx) };
            completion_ctx.notify.notify_one();
            true
        }
        Err(SlotReserveOperationState::Cancelled) => false,
        Err(SlotReserveOperationState::Completed(prev_permit_id)) => {
            panic!(
                "temporal_core_complete_async_reserve called twice for the same reservation - first permit ID {prev_permit_id}, second permit ID {permit_id}"
            )
        }
        Err(SlotReserveOperationState::Pending) => unreachable!(),
    }
}

/// Completes cancellation of asynchronous slot reservation. Cancellation can only be initiated by
/// Core. It's done by calling [`CustomSlotSupplierCallbacks::cancel_reserve`] after an earlier call
/// to [`CustomSlotSupplierCallbacks::reserve`].
///
/// `completion_ctx` must be the same as the one passed to the matching `cancel_reserve` call.
///
/// This function returns true on successful cancellation, or false if cancellation was not
/// requested for the given `completion_ctx`. It is a bug to call this function when cancellation
/// was not requested.
///
/// **Caution:** if this function returns true, `completion_ctx` gets freed. Afterwards, calling
/// either [`temporal_core_complete_async_reserve`] or [`temporal_core_complete_async_cancel_reserve`]
/// with the same `completion_ctx` will cause **memory corruption!**
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_complete_async_cancel_reserve(
    completion_ctx: *const SlotReserveCompletionCtx,
) -> bool {
    if completion_ctx.is_null() {
        panic!("completion_ctx is null");
    }
    let state = unsafe { (*completion_ctx).state.load() };
    match state {
        SlotReserveOperationState::Cancelled => {
            drop(unsafe { Arc::from_raw(completion_ctx) });
            true
        }
        SlotReserveOperationState::Pending => false,
        SlotReserveOperationState::Completed(permit_id) => {
            panic!(
                "temporal_core_complete_async_cancel_reserve called on completed reservation - permit ID {permit_id}"
            )
        }
    }
}

impl TryFrom<&WorkerOptions> for temporal_sdk_core::WorkerConfig {
    type Error = anyhow::Error;

    fn try_from(opt: &WorkerOptions) -> anyhow::Result<Self> {
        let converted_tuner: temporal_sdk_core::TunerHolder = (&opt.tuner).try_into()?;
        WorkerConfigBuilder::default()
            .namespace(opt.namespace.to_str())
            .task_queue(opt.task_queue.to_str())
            .versioning_strategy({
                match &opt.versioning_strategy {
                    WorkerVersioningStrategy::None(n) => {
                        temporal_sdk_core_api::worker::WorkerVersioningStrategy::None {
                            build_id: n.build_id.to_string(),
                        }
                    }
                    WorkerVersioningStrategy::DeploymentBased(dopts) => {
                        let dvb = if let Ok(v) = dopts.default_versioning_behavior.try_into() {
                            Some(v)
                        } else {
                            bail!("Invalid default versioning behavior {}", dopts.default_versioning_behavior)
                        };
                        temporal_sdk_core_api::worker::WorkerVersioningStrategy::WorkerDeploymentBased(
                            temporal_sdk_core_api::worker::WorkerDeploymentOptions {
                                version: temporal_sdk_core_api::worker::WorkerDeploymentVersion {
                                    deployment_name: dopts.version.deployment_name.to_string(),
                                    build_id: dopts.version.build_id.to_string(),
                                },
                                use_worker_versioning: dopts.use_worker_versioning,
                                default_versioning_behavior: dvb,
                            }
                        )
                    }
                    WorkerVersioningStrategy::LegacyBuildIdBased(l) => {
                        temporal_sdk_core_api::worker::WorkerVersioningStrategy::LegacyBuildIdBased {
                            build_id: l.build_id.to_string(),
                        }
                    }
                }
            })
            .client_identity_override(opt.identity_override.to_option_string())
            .max_cached_workflows(opt.max_cached_workflows as usize)
            .tuner(Arc::new(converted_tuner))
            .no_remote_activities(opt.no_remote_activities)
            .sticky_queue_schedule_to_start_timeout(Duration::from_millis(
                opt.sticky_queue_schedule_to_start_timeout_millis,
            ))
            .max_heartbeat_throttle_interval(Duration::from_millis(
                opt.max_heartbeat_throttle_interval_millis,
            ))
            .default_heartbeat_throttle_interval(Duration::from_millis(
                opt.default_heartbeat_throttle_interval_millis,
            ))
            .max_worker_activities_per_second(if opt.max_activities_per_second == 0.0 {
                None
            } else {
                Some(opt.max_activities_per_second)
            })
            .max_task_queue_activities_per_second(
                if opt.max_task_queue_activities_per_second == 0.0 {
                    None
                } else {
                    Some(opt.max_task_queue_activities_per_second)
                },
            )
            // Even though grace period is optional, if it is not set then the
            // auto-cancel-activity behavior or shutdown will not occur, so we
            // always set it even if 0.
            .graceful_shutdown_period(Duration::from_millis(opt.graceful_shutdown_period_millis))
            .workflow_task_poller_behavior(temporal_sdk_core_api::worker::PollerBehavior::try_from(&opt.workflow_task_poller_behavior)?)
            .nonsticky_to_sticky_poll_ratio(opt.nonsticky_to_sticky_poll_ratio)
            .activity_task_poller_behavior(temporal_sdk_core_api::worker::PollerBehavior::try_from(&opt.activity_task_poller_behavior)?)
            .nexus_task_poller_behavior(temporal_sdk_core_api::worker::PollerBehavior::try_from(&opt.nexus_task_poller_behavior)?)
            .workflow_failure_errors(if opt.nondeterminism_as_workflow_fail {
                HashSet::from([WorkflowErrorType::Nondeterminism])
            } else {
                HashSet::new()
            })
            .workflow_types_to_failure_errors(
                opt.nondeterminism_as_workflow_fail_for_types
                    .to_str_vec()
                    .into_iter()
                    .map(|s| {
                        (
                            s.to_owned(),
                            HashSet::from([WorkflowErrorType::Nondeterminism]),
                        )
                    })
                    .collect::<HashMap<String, HashSet<WorkflowErrorType>>>(),
            )
            .build()
            .map_err(|err| anyhow::anyhow!(err))
    }
}

impl TryFrom<&TunerHolder> for temporal_sdk_core::TunerHolder {
    type Error = anyhow::Error;

    fn try_from(holder: &TunerHolder) -> anyhow::Result<Self> {
        // Verify all resource-based options are the same if any are set
        let maybe_wf_resource_opts =
            if let SlotSupplier::ResourceBased(ref ss) = holder.workflow_slot_supplier {
                Some(&ss.tuner_options)
            } else {
                None
            };
        let maybe_act_resource_opts =
            if let SlotSupplier::ResourceBased(ref ss) = holder.activity_slot_supplier {
                Some(&ss.tuner_options)
            } else {
                None
            };
        let maybe_local_act_resource_opts =
            if let SlotSupplier::ResourceBased(ref ss) = holder.local_activity_slot_supplier {
                Some(&ss.tuner_options)
            } else {
                None
            };
        let maybe_nexus_resource_opts =
            if let SlotSupplier::ResourceBased(ref ss) = holder.nexus_task_slot_supplier {
                Some(&ss.tuner_options)
            } else {
                None
            };
        let all_resource_opts = [
            maybe_wf_resource_opts,
            maybe_act_resource_opts,
            maybe_local_act_resource_opts,
            maybe_nexus_resource_opts,
        ];
        let mut set_resource_opts = all_resource_opts.iter().flatten();
        let first = set_resource_opts.next();
        let all_are_same = if let Some(first) = first {
            set_resource_opts.all(|elem| elem == first)
        } else {
            true
        };
        if !all_are_same {
            bail!("All resource-based slot suppliers must have the same ResourceBasedTunerOptions",);
        }

        let mut options = temporal_sdk_core::TunerHolderOptionsBuilder::default();
        if let Some(first) = first {
            options.resource_based_options(
                temporal_sdk_core::ResourceBasedSlotsOptionsBuilder::default()
                    .target_mem_usage(first.target_memory_usage)
                    .target_cpu_usage(first.target_cpu_usage)
                    .build()
                    .expect("Building ResourceBasedSlotsOptions is infallible"),
            );
        };
        options
            .workflow_slot_options(holder.workflow_slot_supplier.try_into()?)
            .activity_slot_options(holder.activity_slot_supplier.try_into()?)
            .local_activity_slot_options(holder.local_activity_slot_supplier.try_into()?)
            .nexus_slot_options(holder.nexus_task_slot_supplier.try_into()?)
            .build()
            .context("Invalid tuner holder options")?
            .build_tuner_holder()
            .context("Failed building tuner holder")
    }
}

impl<SK: SlotKind + Send + Sync + 'static> TryFrom<SlotSupplier>
    for temporal_sdk_core::SlotSupplierOptions<SK>
{
    type Error = anyhow::Error;

    fn try_from(
        supplier: SlotSupplier,
    ) -> anyhow::Result<temporal_sdk_core::SlotSupplierOptions<SK>> {
        Ok(match supplier {
            SlotSupplier::FixedSize(fs) => temporal_sdk_core::SlotSupplierOptions::FixedSize {
                slots: fs.num_slots,
            },
            SlotSupplier::ResourceBased(ss) => {
                temporal_sdk_core::SlotSupplierOptions::ResourceBased(
                    temporal_sdk_core::ResourceSlotOptions::new(
                        ss.minimum_slots,
                        ss.maximum_slots,
                        Duration::from_millis(ss.ramp_throttle_ms),
                    ),
                )
            }
            SlotSupplier::Custom(cs) => {
                temporal_sdk_core::SlotSupplierOptions::Custom(cs.into_ss())
            }
        })
    }
}
