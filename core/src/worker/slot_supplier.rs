mod resource_based;

trait SlotSupplier {
    type SlotKind: SlotKind;
    /// Blocks until a slot is available. In languages with explicit cancel mechanisms, this should
    /// be cancellable and return a boolean indicating whether a slot was actually obtained or not.
    /// In Rust, the future can simply be dropped if the reservation is no longer desired.
    async fn reserve_slot(&self);

    /// Tries to immediately reserve a slot, returning true if a slot is available.
    fn try_reserve_slot(&self) -> bool;

    /// Marks a slot as actually now being used. This is separate from reserving one because the
    /// pollers need to reserve a slot before they have actually obtained work from server. Once
    /// that task is obtained (and validated) then the slot can actually be used to work on the
    /// task.
    ///
    /// Users' implementation of this can choose to emit metrics, or otherwise leverage the
    /// information provided by the `info` parameter to be better able to make future decisions
    /// about whether a slot should be handed out.
    ///
    /// `info` may not be provided if the slot was never used
    /// `error` may be provided if an error was encountered at any point during processing
    ///     TODO: Error type should maybe also be generic and bound to slot type
    fn mark_slot_used(
        &self,
        info: Option<&<Self::SlotKind as SlotKind>::Info>,
        error: Option<&anyhow::Error>,
    );

    /// Frees a slot.
    fn release_slot(&self, info: &SlotReleaseReason<<Self::SlotKind as SlotKind>::Info>);

    /// If this implementation knows how many slots are available at any moment, it should return
    /// that here.
    fn available_slots(&self) -> Option<usize>;
}

enum SlotReleaseReason<Info> {
    TaskComplete(Info),
    Error, // TODO: Details
}

struct WorkflowSlotInfo {
    workflow_type: String,
    // etc...
}

struct ActivitySlotInfo {
    activity_type: String,
    // etc...
}
struct LocalActivitySlotInfo {
    activity_type: String,
    // etc...
}

struct WorkflowSlotKind {}
struct ActivitySlotKind {}
struct LocalActivitySlotKind {}
trait SlotKind {
    type Info;
}
impl SlotKind for WorkflowSlotKind {
    type Info = WorkflowSlotInfo;
}
impl SlotKind for ActivitySlotKind {
    type Info = ActivitySlotInfo;
}
impl SlotKind for LocalActivitySlotKind {
    type Info = LocalActivitySlotInfo;
}
trait WorkflowTaskSlotSupplier: SlotSupplier<SlotKind = WorkflowSlotKind> {}
trait ActivityTaskSlotSupplier: SlotSupplier<SlotKind = ActivitySlotKind> {}
trait LocalActivityTaskSlotSupplier: SlotSupplier<SlotKind = LocalActivitySlotKind> {}

struct WorkflowSlotsInfo {
    used_slots: Vec<WorkflowSlotInfo>,
    /// Current size of the workflow cache.
    num_cached_workflows: usize,
    /// The limit on the size of the cache, if any. This is important for users to know as discussed below in the section
    /// on workflow cache management.
    max_cache_size: Option<usize>,
    // ... Possibly also metric information
}

trait WorkflowCacheSizer {
    /// Return true if it is acceptable to cache a new workflow. Information about already-in-use slots, and just-received
    /// task is provided. Will not be called for an already-cached workflow who is receiving a new task.
    ///
    /// Because the number of available slots must be <= the number of workflows cached, if this returns false
    /// when there are no idle workflows in the cache (IE: All other outstanding slots are in use), we will buffer the
    /// task and wait for another to complete so we can evict it and make room for the new one.
    fn can_allow_workflow(
        &self,
        slots_info: &WorkflowSlotsInfo,
        new_task: &WorkflowSlotInfo,
    ) -> bool;
}

// struct PermitSlotSupplier<SS> {
//     slot_supplier: SS,
// }
// impl<SS> PermitSlotSupplier<SS>
// where
//     SS: SlotSupplier,
// {
//     pub async fn acquire(&self) -> Result<OwnedMeteredSemPermit, ()> {
//         self.slot_supplier.reserve_slot().await;
//     }
// }

// impl<K> SlotSupplier for MeteredSemaphore
// where
//     K: SlotKind,
// {
//     type SlotKind = K;
//
//     async fn reserve_slot(&self) {
//         self.acquire_owned()
//     }
//
//     fn try_reserve_slot(&self) -> bool {
//         todo!()
//     }
//
//     fn mark_slot_used(&self, info: Option<&K::Info>, error: Option<&Error>) {
//         todo!()
//     }
//
//     fn release_slot(&self, info: &SlotReleaseReason<K::Info>) {
//         todo!()
//     }
//
//     fn available_slots(&self) -> Option<usize> {
//         todo!()
//     }
// }
