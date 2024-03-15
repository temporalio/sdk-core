use std::{marker::PhantomData, sync::Arc};
use temporal_sdk_core_api::worker::{
    SlotKind, SlotReleaseReason, SlotSupplier, SlotSupplierPermit, WorkerConfigBuilder,
};
use tokio::sync::Semaphore;

mod resource_based;

pub(crate) struct FixedSizeSlotSupplier<SK> {
    sem: Arc<Semaphore>,
    _pd: PhantomData<SK>,
}

impl<SK> FixedSizeSlotSupplier<SK> {
    pub fn new(size: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(size)),
            _pd: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<SK> SlotSupplier for FixedSizeSlotSupplier<SK>
where
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self) -> SlotSupplierPermit {
        let perm = self.sem.clone().acquire_owned().await.expect("todo");
        SlotSupplierPermit::Data(Box::new(perm))
    }

    fn try_reserve_slot(&self) -> Option<SlotSupplierPermit> {
        let perm = self.sem.clone().try_acquire_owned();
        perm.ok().map(|p| SlotSupplierPermit::Data(Box::new(p)))
    }

    fn mark_slot_used(&self, _info: Option<&SK::Info>, _error: Option<&()>) {}

    fn release_slot(&self, _: SlotReleaseReason) {}

    fn available_slots(&self) -> Option<usize> {
        Some(self.sem.available_permits())
    }
}

pub trait WorkerConfigSlotSupplierExt {
    /// Creates a [FixedSizeSlotSupplier] using the provided max and assigns it as the workflow
    /// task slot supplier
    fn max_outstanding_workflow_tasks(&mut self, max: usize) -> &mut Self;
    /// Creates a [FixedSizeSlotSupplier] using the provided max and assigns it as the activity task
    /// slot supplier
    fn max_outstanding_activities(&mut self, max: usize) -> &mut Self;
    /// Creates a [FixedSizeSlotSupplier] using the provided max and assigns it as the local
    /// activity task slot supplier
    fn max_outstanding_local_activities(&mut self, max: usize) -> &mut Self;
}

impl WorkerConfigSlotSupplierExt for WorkerConfigBuilder {
    fn max_outstanding_workflow_tasks(&mut self, max: usize) -> &mut Self {
        let fsss = FixedSizeSlotSupplier::new(max);
        self.workflow_task_slot_supplier(Arc::new(fsss));
        self
    }

    fn max_outstanding_activities(&mut self, max: usize) -> &mut Self {
        let fsss = FixedSizeSlotSupplier::new(max);
        self.activity_task_slot_supplier(Arc::new(fsss));
        self
    }

    fn max_outstanding_local_activities(&mut self, max: usize) -> &mut Self {
        let fsss = FixedSizeSlotSupplier::new(max);
        self.local_activity_task_slot_supplier(Arc::new(fsss));
        self
    }
}
