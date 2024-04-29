use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    ops::Sub,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};
use temporal_sdk_core_api::{
    telemetry::metrics::{CoreMeter, GaugeF64, MetricAttributes, TemporalMeter},
    worker::{
        SlotKind, SlotReleaseReason, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
        WorkflowCacheSizer, WorkflowSlotInfo, WorkflowSlotsInfo,
    },
};
use tokio::sync::watch;

pub struct ResourceBasedSlots<MI> {
    target_mem_usage: f64,
    target_cpu_usage: f64,
    sys_info_supplier: MI,
    metrics: OnceLock<MetricInstruments>,
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
    last_metric_emission: AtomicCell<Instant>,
    _slot_kind: PhantomData<SK>,
}
struct PidControllers {
    mem: pid::Pid<f64>,
    cpu: pid::Pid<f64>,
}
struct MetricInstruments {
    attribs: MetricAttributes,
    mem_usage: Arc<dyn GaugeF64>,
    cpu_usage: Arc<dyn GaugeF64>,
    mem_pid_output: Arc<dyn GaugeF64>,
    cpu_pid_output: Arc<dyn GaugeF64>,
}

impl ResourceBasedSlots<RealSysInfo> {
    pub fn new(target_mem_usage: f64, target_cpu_usage: f64) -> Self {
        Self {
            metrics: OnceLock::new(),
            target_mem_usage,
            target_cpu_usage,
            sys_info_supplier: RealSysInfo::new(),
        }
    }
}

impl PidControllers {
    fn new(mem_target: f64, cpu_target: f64) -> Self {
        let mut mem = pid::Pid::new(mem_target, 100.0);
        mem.p(5.0, 100).i(0.0, 100).d(1.0, 100);
        let mut cpu = pid::Pid::new(cpu_target, 100.0);
        cpu.p(5.0, 100.).i(0.0, 100.).d(1.0, 100.);
        Self { mem, cpu }
    }
}

impl MetricInstruments {
    fn new(meter: TemporalMeter) -> Self {
        let mem_usage = meter.inner.gauge_f64("resource_slots_mem_usage".into());
        let cpu_usage = meter.inner.gauge_f64("resource_slots_cpu_usage".into());
        let mem_pid_output = meter
            .inner
            .gauge_f64("resource_slots_mem_pid_output".into());
        let cpu_pid_output = meter
            .inner
            .gauge_f64("resource_slots_cpu_pid_output".into());
        let attribs = meter.inner.new_attributes(meter.default_attribs);
        Self {
            attribs,
            mem_usage,
            cpu_usage,
            mem_pid_output,
            cpu_pid_output,
        }
    }
}

trait MemoryInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by the system in bytes
    fn used_mem(&self) -> u64;
    /// Return system used CPU as a float in the range [0.0, 1.0] where 1.0 is defined as all
    /// cores pegged
    fn used_cpu_percent(&self) -> f64;
    /// Return system used memory as a float in the range [0.0, 1.0]
    fn process_used_percent(&self) -> f64 {
        self.used_mem() as f64 / self.total_mem() as f64
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
            // TODO: Just using this for now to make sure metrics get emitted regularly
            self.pid_decision();
            if ctx.num_issued_slots() < self.minimum {
                let _ = self.last_slot_issued_tx.send(Instant::now());
                return SlotSupplierPermit::NoData;
            } else {
                let must_wait_for = self
                    .ramp_throttle
                    .saturating_sub(self.time_since_last_issued());
                if must_wait_for > Duration::from_millis(0) {
                    tokio::time::sleep(must_wait_for).await;
                }
                if let Some(p) = self.try_reserve_slot(ctx) {
                    return p;
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
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
    pub fn attach_metrics(&self, metrics: TemporalMeter) {
        let _ = self.inner.metrics.set(MetricInstruments::new(metrics));
    }

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
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            last_metric_emission: AtomicCell::new(Instant::now().sub(Duration::from_secs(1))),
            inner,
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
        let mem_used_percent = self.inner.sys_info_supplier.process_used_percent();
        let cpu_used_percent = self.inner.sys_info_supplier.used_cpu_percent();
        let mem_output = pids.mem.next_control_output(mem_used_percent).output;
        let cpu_output = pids.cpu.next_control_output(cpu_used_percent).output;
        if Instant::now() - self.last_metric_emission.load() > Duration::from_millis(100) {
            if let Some(m) = self.inner.metrics.get() {
                m.mem_pid_output.record(mem_output, &m.attribs);
                m.cpu_pid_output.record(cpu_output, &m.attribs);
                m.mem_usage.record(mem_used_percent * 100., &m.attribs);
                m.cpu_usage.record(cpu_used_percent * 100., &m.attribs);
            }
            self.last_metric_emission.store(Instant::now());
        }
        mem_output > 0.25 && cpu_output > 0.05
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

    pub fn attach_metrics(&self, metrics: TemporalMeter) {
        let _ = self.metrics.set(MetricInstruments::new(metrics));
    }

    fn can_reserve(&self) -> bool {
        self.sys_info_supplier.process_used_percent() <= self.target_mem_usage
    }
}

#[derive(Debug)]
pub struct RealSysInfo {
    sys: Mutex<sysinfo::System>,
    total_mem: u64,
    cur_mem_usage: AtomicU64,
    cur_cpu_usage: AtomicU64,
    last_refresh: AtomicCell<Instant>,
}
impl RealSysInfo {
    fn new() -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let total_mem = sys.total_memory();
        let s = Self {
            sys: Mutex::new(sys),
            last_refresh: AtomicCell::new(Instant::now()),
            cur_mem_usage: AtomicU64::new(0),
            cur_cpu_usage: AtomicU64::new(0),
            total_mem,
        };
        s.refresh();
        s
    }

    fn refresh_if_needed(&self) {
        // This is all quite expensive and meaningfully slows everything down if it's allowed to
        // happen more often. A better approach than a lock would be needed to go faster.
        if (Instant::now() - self.last_refresh.load()) > Duration::from_millis(100) {
            self.refresh();
        }
    }

    fn refresh(&self) {
        let mut lock = self.sys.lock();
        lock.refresh_memory();
        lock.refresh_cpu_usage();
        let mem = lock.used_memory();
        let cpu = lock.global_cpu_info().cpu_usage() as f64 / 100.;
        self.cur_mem_usage.store(mem, Ordering::Release);
        self.cur_cpu_usage.store(cpu.to_bits(), Ordering::Release);
        self.last_refresh.store(Instant::now());
    }
}
impl MemoryInfo for RealSysInfo {
    fn total_mem(&self) -> u64 {
        self.total_mem
    }

    fn used_mem(&self) -> u64 {
        self.refresh_if_needed();
        self.cur_mem_usage.load(Ordering::Acquire)
    }

    fn used_cpu_percent(&self) -> f64 {
        self.refresh_if_needed();
        f64::from_bits(self.cur_cpu_usage.load(Ordering::Acquire))
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

        fn used_mem(&self) -> u64 {
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
            metrics: Default::default(),
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
            metrics: Default::default(),
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
