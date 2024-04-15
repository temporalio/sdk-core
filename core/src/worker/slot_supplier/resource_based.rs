use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    ops::SubAssign,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};
use temporal_sdk_core_api::worker::{
    SlotKind, SlotReleaseReason, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
    WorkflowCacheSizer, WorkflowSlotInfo, WorkflowSlotsInfo,
};
use tokio::sync::watch;

pub struct ResourceBasedSlots<MI> {
    target_mem_usage: f64,
    assumed_maximum_marginal_contribution: f64,
    mem_info_supplier: MI,
    max: usize,
}
pub struct ResourceBasedSlotsForType<MI, SK> {
    inner: Arc<ResourceBasedSlots<MI>>,
    minimum: usize,
    /// Minimum time we will wait (after passing the minimum slots number) between handing out new
    /// slots
    ramp_throttle: Duration,

    last_slot_issued_tx: watch::Sender<Instant>,
    last_slot_issued_rx: watch::Receiver<Instant>,
    _slot_kind: PhantomData<SK>,
}

impl ResourceBasedSlots<SysinfoMem> {
    pub fn new(target_mem_usage: f64, marginal_contribution: f64, max: usize) -> Self {
        Self {
            target_mem_usage,
            assumed_maximum_marginal_contribution: marginal_contribution,
            mem_info_supplier: SysinfoMem::new(),
            max,
        }
    }
}

trait MemoryInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by this process in bytes
    // TODO: probably needs to be just overall used... won't work w/ subprocesses for example
    fn process_used_mem(&self) -> u64;

    fn process_used_percent(&self) -> f64 {
        self.process_used_mem() as f64 / self.total_mem() as f64
    }
}

#[async_trait::async_trait]
impl<MI, SK> SlotSupplier for ResourceBasedSlotsForType<MI, SK>
where
    MI: MemoryInfo + Send + Sync,
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        loop {
            if ctx.num_issued_slots() >= self.minimum {
                let must_wait_for = self
                    .ramp_throttle
                    .saturating_sub(self.time_since_last_issued());
                if must_wait_for > Duration::from_millis(0) {
                    tokio::time::sleep(must_wait_for).await;
                }
            }
            if let Some(p) = self.try_reserve_slot(ctx) {
                return p;
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        if self.time_since_last_issued() > self.ramp_throttle
            && self.inner.can_reserve(ctx.num_issued_slots())
        {
            let _ = self.last_slot_issued_tx.send(Instant::now());
            Some(SlotSupplierPermit::NoData)
        } else {
            None
        }
    }

    fn mark_slot_used(&self, _info: SK::Info<'_>) {}

    fn release_slot(&self, _info: SlotReleaseReason) {}

    fn available_slots(&self) -> Option<usize> {
        None
    }
}

impl<MI, SK> ResourceBasedSlotsForType<MI, SK>
where
    MI: MemoryInfo + Send + Sync,
    SK: SlotKind + Send + Sync,
{
    fn time_since_last_issued(&self) -> Duration {
        Instant::now()
            .checked_duration_since(*self.last_slot_issued_rx.borrow())
            .unwrap_or_default()
    }
}

impl<MI> WorkflowCacheSizer for ResourceBasedSlots<MI>
where
    MI: MemoryInfo + Sync,
{
    fn can_allow_workflow(
        &self,
        slots_info: &WorkflowSlotsInfo,
        _new_task: &WorkflowSlotInfo,
    ) -> bool {
        self.can_reserve(slots_info.used_slots.len())
    }
}

impl<MI: MemoryInfo + Sync> ResourceBasedSlots<MI> {
    // TODO: Can just be an into impl probably?
    pub fn as_kind<SK: SlotKind + Send + Sync>(
        self: &Arc<Self>,
        minimum: usize,
        ramp_throttle: Duration,
    ) -> Arc<ResourceBasedSlotsForType<MI, SK>> {
        let (tx, rx) = watch::channel(Instant::now());
        Arc::new(ResourceBasedSlotsForType {
            inner: self.clone(),
            minimum,
            ramp_throttle,
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            _slot_kind: PhantomData,
        })
    }

    pub fn into_kind<SK: SlotKind + Send + Sync>(self) -> ResourceBasedSlotsForType<MI, SK> {
        let (tx, rx) = watch::channel(Instant::now());
        ResourceBasedSlotsForType {
            inner: Arc::new(self),
            // TODO: Configure
            minimum: 1,
            ramp_throttle: Duration::from_millis(0),
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            _slot_kind: PhantomData,
        }
    }

    fn can_reserve(&self, num_used: usize) -> bool {
        if num_used > self.max {
            return false;
        }
        self.mem_info_supplier.process_used_percent() + self.assumed_maximum_marginal_contribution
            <= self.target_mem_usage
    }
}

#[derive(Debug)]
pub struct SysinfoMem {
    sys: Mutex<sysinfo::System>,
    pid: sysinfo::Pid,
    cur_mem_usage: AtomicU64,
    last_refresh: AtomicCell<Instant>,
}
impl SysinfoMem {
    fn new() -> Self {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().expect("get pid works");
        sys.refresh_processes();
        sys.refresh_memory();
        Self {
            sys: Default::default(),
            last_refresh: AtomicCell::new(Instant::now()),
            pid,
            cur_mem_usage: AtomicU64::new(0),
        }
    }
    fn refresh_if_needed(&self) {
        // This is all quite expensive and meaningfully slows everything down if it's allowed to
        // happen more often. A better approach than a lock would be needed to go faster.
        if (Instant::now() - self.last_refresh.load()) > Duration::from_millis(100) {
            let mut lock = self.sys.lock();
            lock.refresh_memory();
            lock.refresh_processes();
            let proc = lock.process(self.pid).expect("exists");
            self.cur_mem_usage
                .store(dbg!(proc.memory()), std::sync::atomic::Ordering::Release);
            self.last_refresh.store(Instant::now())
        }
    }
}
impl MemoryInfo for SysinfoMem {
    fn total_mem(&self) -> u64 {
        self.refresh_if_needed();
        self.sys.lock().total_memory()
    }

    fn process_used_mem(&self) -> u64 {
        self.refresh_if_needed();
        self.cur_mem_usage
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    use temporal_sdk_core_api::worker::WorkflowSlotKind;

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
    struct FakeResCtx {}
    impl SlotReservationContext for FakeResCtx {
        fn num_issued_slots(&self) -> usize {
            0
        }
    }

    #[test]
    fn mem_workflow_sync() {
        let (fmis, used) = FakeMIS::new();
        let rbs = ResourceBasedSlots {
            target_mem_usage: 0.8,
            assumed_maximum_marginal_contribution: 0.1,
            mem_info_supplier: fmis,
            max: 1000,
        }
        .into_kind::<WorkflowSlotKind>();
        assert!(rbs.try_reserve_slot(&FakeResCtx {}).is_some());
        used.store(90_000, Ordering::Release);
        assert!(rbs.try_reserve_slot(&FakeResCtx {}).is_none());
    }

    #[tokio::test]
    async fn mem_workflow_async() {
        let (fmis, used) = FakeMIS::new();
        used.store(90_000, Ordering::Release);
        let rbs = ResourceBasedSlots {
            target_mem_usage: 0.8,
            assumed_maximum_marginal_contribution: 0.1,
            mem_info_supplier: fmis,
            max: 1000,
        }
        .into_kind::<WorkflowSlotKind>();
        let order = crossbeam_queue::ArrayQueue::new(2);
        let waits_free = async {
            rbs.reserve_slot(&FakeResCtx {}).await;
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
