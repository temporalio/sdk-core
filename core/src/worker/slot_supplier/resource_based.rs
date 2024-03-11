use crate::worker::slot_supplier::{
    SlotKind, SlotReleaseReason, SlotSupplier, SlotSupplierPermit, WorkflowCacheSizer,
    WorkflowSlotInfo, WorkflowSlotKind, WorkflowSlotsInfo,
};
use anyhow::Error;
use std::time::Duration;

struct ResourceBasedWorkflowSlots<MI> {
    target_mem_usage: f64,
    assumed_maximum_marginal_contribution: f64,
    mem_info_supplier: MI,
}

trait MemoryInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by this process in bytes
    fn process_used_mem(&self) -> u64;

    fn process_used_percent(&self) -> f64 {
        self.process_used_mem() as f64 / self.total_mem() as f64
    }
}

#[async_trait::async_trait]
impl<MI> SlotSupplier for ResourceBasedWorkflowSlots<MI>
where
    MI: MemoryInfo + Sync,
{
    type SlotKind = WorkflowSlotKind;

    async fn reserve_slot(&self) -> SlotSupplierPermit {
        loop {
            if let Some(p) = self.try_reserve_slot() {
                return p;
            }
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    }

    fn try_reserve_slot(&self) -> Option<SlotSupplierPermit> {
        if self.can_reserve() {
            Some(SlotSupplierPermit::OtherImpl)
        } else {
            None
        }
    }

    fn mark_slot_used(
        &self,
        _info: Option<&<Self::SlotKind as SlotKind>::Info>,
        _error: Option<&Error>,
    ) {
    }

    fn release_slot(&self, _info: SlotReleaseReason) {}

    fn available_slots(&self) -> Option<usize> {
        None
    }
}

impl<MI> WorkflowCacheSizer for ResourceBasedWorkflowSlots<MI>
where
    MI: MemoryInfo + Sync,
{
    fn can_allow_workflow(
        &self,
        _slots_info: &WorkflowSlotsInfo,
        _new_task: &WorkflowSlotInfo,
    ) -> bool {
        self.can_reserve()
    }
}

impl<MI: MemoryInfo + Sync> ResourceBasedWorkflowSlots<MI> {
    fn can_reserve(&self) -> bool {
        self.mem_info_supplier.process_used_percent() + self.assumed_maximum_marginal_contribution
            <= self.target_mem_usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    struct FakeMIS {
        used: Arc<AtomicU64>,
    }
    impl FakeMIS {
        fn new() -> (Self, Arc<AtomicU64>) {
            let used = Arc::new(AtomicU64::new(0));
            (Self { used: used.clone() }, used)
        }
    }
    impl MemoryInfo for FakeMIS {
        fn total_mem(&self) -> u64 {
            100_000
        }

        fn process_used_mem(&self) -> u64 {
            self.used.load(Ordering::Acquire)
        }
    }

    #[test]
    fn mem_workflow_sync() {
        let (fmis, used) = FakeMIS::new();
        let rbs = ResourceBasedWorkflowSlots {
            target_mem_usage: 0.8,
            assumed_maximum_marginal_contribution: 0.1,
            mem_info_supplier: fmis,
        };
        assert!(rbs.try_reserve_slot());
        used.store(90_000, Ordering::Release);
        assert!(!rbs.try_reserve_slot());
    }

    #[tokio::test]
    async fn mem_workflow_async() {
        let (fmis, used) = FakeMIS::new();
        used.store(90_000, Ordering::Release);
        let rbs = ResourceBasedWorkflowSlots {
            target_mem_usage: 0.8,
            assumed_maximum_marginal_contribution: 0.1,
            mem_info_supplier: fmis,
        };
        let order = crossbeam_queue::ArrayQueue::new(2);
        let waits_free = async {
            rbs.reserve_slot().await;
            order.push(2).unwrap();
        };
        let frees = async {
            used.store(70_000, Ordering::Release);
            order.push(1).unwrap();
        };
        tokio::join!(waits_free, frees);
        assert_eq!(order.pop(), Some(1));
        assert_eq!(order.pop(), Some(2));
    }
}
