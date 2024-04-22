use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use temporal_sdk_core_api::worker::{
    SlotKind, SlotReleaseReason, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
    WorkflowCacheSizer, WorkflowSlotInfo, WorkflowSlotsInfo,
};
use tokio::sync::watch;

pub struct ResourceBasedSlots<MI> {
    target_mem_usage: f64,
    target_cpu_usage: f32,
    sys_info_supplier: MI,
}
pub struct ResourceBasedSlotsForType<MI, SK> {
    inner: Arc<ResourceBasedSlots<MI>>,
    minimum: usize,
    /// Maximum amount of slots of this type permitted
    max: usize,
    /// Minimum time we will wait (after passing the minimum slots number) between handing out new
    /// slots
    ramp_throttle: Duration,

    pids: Arc<Mutex<PidControllers>>,
    last_slot_issued_tx: watch::Sender<Instant>,
    last_slot_issued_rx: watch::Receiver<Instant>,
    _slot_kind: PhantomData<SK>,
}
struct PidControllers {
    mem: pid::Pid<f64>,
    cpu: pid::Pid<f32>,
}

impl ResourceBasedSlots<RealSysInfo> {
    pub fn new(target_mem_usage: f64, target_cpu_usage: f32) -> Self {
        Self {
            target_mem_usage,
            target_cpu_usage,
            sys_info_supplier: RealSysInfo::new(),
        }
    }
}

impl PidControllers {
    fn new(mem_target: f64, cpu_target: f32) -> Self {
        let mut mem = pid::Pid::new(mem_target, 100.0);
        mem.p(5.0, 100).i(0.0, 100).d(1.0, 100);
        let mut cpu = pid::Pid::new(cpu_target, 100.0);
        cpu.p(5.0, 100.).i(0.0, 100.).d(1.0, 100.);
        Self { mem, cpu }
    }
}

trait MemoryInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by this process in bytes
    // TODO: probably needs to be just overall used... won't work w/ subprocesses for example
    fn process_used_mem(&self) -> u64;

    fn used_cpu_percent(&self) -> f32;

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
            } else {
                let _ = self.last_slot_issued_tx.send(Instant::now());
                return SlotSupplierPermit::NoData;
            }
            if let Some(p) = self.try_reserve_slot(ctx) {
                return p;
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        if self.time_since_last_issued() > self.ramp_throttle
            && ctx.num_issued_slots() < self.max
            && self.pid_decision()
            && self.inner.can_reserve()
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
    fn new(
        inner: Arc<ResourceBasedSlots<MI>>,
        minimum: usize,
        max: usize,
        ramp_throttle: Duration,
    ) -> Self {
        let (tx, rx) = watch::channel(Instant::now());
        Self {
            minimum,
            max,
            ramp_throttle,
            pids: Arc::new(Mutex::new(PidControllers::new(
                inner.target_mem_usage,
                inner.target_cpu_usage,
            ))),
            inner,
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            _slot_kind: PhantomData,
        }
    }

    fn time_since_last_issued(&self) -> Duration {
        Instant::now()
            .checked_duration_since(*self.last_slot_issued_rx.borrow())
            .unwrap_or_default()
    }

    /// Returns true if the pid controllers think a new slot should be given out
    fn pid_decision(&self) -> bool {
        let mut pids = self.pids.lock();
        let mem_output = pids
            .mem
            .next_control_output(self.inner.sys_info_supplier.process_used_percent())
            .output;
        let cpu_output = pids
            .cpu
            .next_control_output(self.inner.sys_info_supplier.used_cpu_percent())
            .output;
        mem_output > 0.25 && cpu_output > 0.25
    }
}

impl<MI> WorkflowCacheSizer for ResourceBasedSlots<MI>
where
    MI: MemoryInfo + Sync + Send,
{
    fn can_allow_workflow(&self, _: &WorkflowSlotsInfo, _: &WorkflowSlotInfo) -> bool {
        self.can_reserve()
    }
}

impl<MI: MemoryInfo + Sync + Send> ResourceBasedSlots<MI> {
    // TODO: Can just be an into impl probably?
    pub fn as_kind<SK: SlotKind + Send + Sync>(
        self: &Arc<Self>,
        minimum: usize,
        max: usize,
        ramp_throttle: Duration,
    ) -> Arc<ResourceBasedSlotsForType<MI, SK>> {
        Arc::new(ResourceBasedSlotsForType::new(
            self.clone(),
            minimum,
            max,
            ramp_throttle,
        ))
    }

    pub fn into_kind<SK: SlotKind + Send + Sync>(self) -> ResourceBasedSlotsForType<MI, SK> {
        // TODO: remove or parameterize
        ResourceBasedSlotsForType::new(Arc::new(self), 1, 1000, Duration::from_millis(0))
    }

    fn can_reserve(&self) -> bool {
        self.sys_info_supplier.process_used_percent() <= self.target_mem_usage
    }
}

#[derive(Debug)]
pub struct RealSysInfo {
    sys: Mutex<sysinfo::System>,
    pid: sysinfo::Pid,
    cur_mem_usage: AtomicU64,
    cur_cpu_usage: AtomicU32,
    last_refresh: AtomicCell<Instant>,
}
impl RealSysInfo {
    fn new() -> Self {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().expect("get pid works");
        sys.refresh_processes();
        sys.refresh_memory();
        sys.refresh_cpu();
        Self {
            sys: Default::default(),
            last_refresh: AtomicCell::new(Instant::now()),
            pid,
            cur_mem_usage: AtomicU64::new(0),
            cur_cpu_usage: AtomicU32::new(0),
        }
    }
    fn refresh_if_needed(&self) {
        // This is all quite expensive and meaningfully slows everything down if it's allowed to
        // happen more often. A better approach than a lock would be needed to go faster.
        if (Instant::now() - self.last_refresh.load()) > Duration::from_millis(100) {
            let mut lock = self.sys.lock();
            lock.refresh_memory();
            lock.refresh_processes();
            lock.refresh_cpu_usage();
            let proc = lock.process(self.pid).expect("exists");
            self.cur_mem_usage.store(proc.memory(), Ordering::Release);
            self.cur_cpu_usage.store(
                lock.global_cpu_info().cpu_usage().to_bits(),
                Ordering::Release,
            );
            self.last_refresh.store(Instant::now())
        }
    }
}
impl MemoryInfo for RealSysInfo {
    fn total_mem(&self) -> u64 {
        self.refresh_if_needed();
        self.sys.lock().total_memory()
    }

    fn process_used_mem(&self) -> u64 {
        self.refresh_if_needed();
        self.cur_mem_usage.load(Ordering::Acquire)
    }

    fn used_cpu_percent(&self) -> f32 {
        self.refresh_if_needed();
        f32::from_bits(self.cur_cpu_usage.load(Ordering::Acquire))
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

        fn used_cpu_percent(&self) -> f32 {
            todo!()
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
            target_cpu_usage: 1.0,
            sys_info_supplier: fmis,
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
            target_cpu_usage: 1.0,
            sys_info_supplier: fmis,
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
