use temporal_sdk_core_api::worker::{
    SlotKind, SlotReleaseReason, SlotSupplier, SlotSupplierPermit,
};
use tokio::sync::Semaphore;

mod resource_based;

pub(crate) struct FixedSizeSlotSupplier {
    sem: Semaphore,
}

impl FixedSizeSlotSupplier {
    pub fn new(size: usize) -> Self {
        Self {
            sem: Semaphore::new(size),
        }
    }
}

impl<SK> SlotSupplier for FixedSizeSlotSupplier
where
    SK: SlotKind,
{
    type SlotKind = SK;

    async fn reserve_slot(&self) -> SlotSupplierPermit {
        let perm = self.sem.acquire().await;
        // TODO: Needs to hold permit
        SlotSupplierPermit::OtherImpl
    }

    fn try_reserve_slot(&self) -> Option<SlotSupplierPermit> {
        todo!()
    }

    fn mark_slot_used(&self, info: Option<&SK::Info>, error: Option<&()>) {
        todo!()
    }

    fn release_slot(&self, info: SlotReleaseReason) {
        todo!()
    }

    fn available_slots(&self) -> Option<usize> {
        todo!()
    }
}
