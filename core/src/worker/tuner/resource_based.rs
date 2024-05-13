use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};
use temporal_sdk_core_api::{
    telemetry::metrics::{CoreMeter, GaugeF64, MetricAttributes, TemporalMeter},
    worker::{
        ActivitySlotKind, LocalActivitySlotKind, SlotKind, SlotReservationContext, SlotSupplier,
        SlotSupplierPermit, WorkerTuner, WorkflowSlotKind,
    },
};
use tokio::{sync::watch, task::JoinHandle};

/// Implements [WorkerTuner] and attempts to maintain certain levels of resource usage when
/// under load.
///
/// It does so by using two PID controllers, one for memory and one for CPU, which are fed the
/// current usage levels of their respective resource as measurements. The user specifies a target
/// threshold for each, and slots are handed out if the output of both PID controllers is above some
/// defined threshold. See [ResourceBasedSlotsOptions] for the default PID controller settings.
pub struct ResourceBasedTuner<MI> {
    slots: Arc<ResourceBasedSlots<MI>>,
    wf_opts: Option<ResourceSlotOptions>,
    act_opts: Option<ResourceSlotOptions>,
    la_opts: Option<ResourceSlotOptions>,
}

impl<MI> ResourceBasedTuner<MI> {
    /// Build a new tuner from a [ResourceBasedSlots] instance
    pub fn new(resourcer: ResourceBasedSlots<MI>) -> Self {
        Self {
            slots: Arc::new(resourcer),
            wf_opts: None,
            act_opts: None,
            la_opts: None,
        }
    }

    /// Set workflow slot options
    pub fn with_workflow_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.wf_opts = Some(opts);
        self
    }

    /// Set activity slot options
    pub fn with_activity_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.act_opts = Some(opts);
        self
    }

    /// Set local activity slot options
    pub fn with_local_activity_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.la_opts = Some(opts);
        self
    }
}

const DEFAULT_WF_SLOT_OPTS: ResourceSlotOptions = ResourceSlotOptions {
    min_slots: 2,
    max_slots: 10_000,
    ramp_throttle: Duration::from_millis(0),
};
const DEFAULT_ACT_SLOT_OPTS: ResourceSlotOptions = ResourceSlotOptions {
    min_slots: 1,
    max_slots: 10_000,
    ramp_throttle: Duration::from_millis(50),
};

/// Options for a specific slot type
#[derive(Debug, Clone, Copy, derive_more::Constructor)]
pub struct ResourceSlotOptions {
    /// Amount of slots of this type that will be issued regardless of any other checks
    min_slots: usize,
    /// Maximum amount of slots of this type permitted
    max_slots: usize,
    /// Minimum time we will wait (after passing the minimum slots number) between handing out new
    /// slots
    ramp_throttle: Duration,
}

/// Implements [SlotSupplier] and attempts to maintain certain levels of resource usage when
/// under load.
///
/// It does so by using two PID controllers, one for memory and one for CPU, which are fed the
/// current usage levels of their respective resource as measurements. The user specifies a target
/// threshold for each, and slots are handed out if the output of both PID controllers is above some
/// defined threshold. See [ResourceBasedSlotsOptions] for the default PID controller settings.
pub struct ResourceBasedSlots<MI> {
    options: ResourceBasedSlotsOptions,
    sys_info_supplier: MI,
    metrics: OnceLock<JoinHandle<()>>,
    pids: Mutex<PidControllers>,
    last_metric_vals: Arc<AtomicCell<LastMetricVals>>,
}
/// Wraps [ResourceBasedSlots] for a specific slot type
pub struct ResourceBasedSlotsForType<MI, SK> {
    inner: Arc<ResourceBasedSlots<MI>>,

    opts: ResourceSlotOptions,

    last_slot_issued_tx: watch::Sender<Instant>,
    last_slot_issued_rx: watch::Receiver<Instant>,
    _slot_kind: PhantomData<SK>,
}
#[derive(Clone, Debug, derive_builder::Builder)]
#[non_exhaustive]
pub struct ResourceBasedSlotsOptions {
    /// A value in the range [0.0, 1.0] representing the target memory usage.
    target_mem_usage: f64,
    /// A value in the range [0.0, 1.0] representing the target CPU usage.
    target_cpu_usage: f64,

    #[builder(default = "5.0")]
    pub mem_p_gain: f64,
    #[builder(default = "0.0")]
    pub mem_i_gain: f64,
    #[builder(default = "1.0")]
    pub mem_d_gain: f64,
    /// If the mem PID controller outputs a value higher than this, we say the mem half of things
    /// will allow a slot
    #[builder(default = "0.25")]
    pub mem_output_threshold: f64,
    #[builder(default = "5.0")]
    pub cpu_p_gain: f64,
    #[builder(default = "0.0")]
    pub cpu_i_gain: f64,
    #[builder(default = "1.0")]
    pub cpu_d_gain: f64,
    /// If the CPU PID controller outputs a value higher than this, we say the CPU half of things
    /// will allow a slot
    #[builder(default = "0.05")]
    pub cpu_output_threshold: f64,
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
#[derive(Clone, Copy, Default)]
struct LastMetricVals {
    mem_output: f64,
    cpu_output: f64,
    mem_used_percent: f64,
    cpu_used_percent: f64,
}

impl ResourceBasedSlots<RealSysInfo> {
    /// Create an instance attempting to target the provided memory and cpu thresholds as values
    /// between 0 and 1.
    pub fn new(target_mem_usage: f64, target_cpu_usage: f64) -> Self {
        let opts = ResourceBasedSlotsOptionsBuilder::default()
            .target_mem_usage(target_mem_usage)
            .target_cpu_usage(target_cpu_usage)
            .build()
            .expect("default resource based slot options can't fail to build");
        Self::new_with_sysinfo(opts, RealSysInfo::new())
    }

    /// Create an instance using the fully configurable set of PID controller options
    pub fn new_from_options(options: ResourceBasedSlotsOptions) -> Self {
        Self::new_with_sysinfo(options, RealSysInfo::new())
    }
}

impl PidControllers {
    fn new(options: &ResourceBasedSlotsOptions) -> Self {
        let mut mem = pid::Pid::new(options.target_mem_usage, 100.0);
        mem.p(options.mem_p_gain, 100)
            .i(options.mem_i_gain, 100)
            .d(options.mem_d_gain, 100);
        let mut cpu = pid::Pid::new(options.target_cpu_usage, 100.0);
        cpu.p(options.cpu_p_gain, 100)
            .i(options.cpu_i_gain, 100)
            .d(options.cpu_d_gain, 100);
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

/// Implementors provide information about system resource usage
pub trait SystemResourceInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by the system in bytes
    fn used_mem(&self) -> u64;
    /// Return system used CPU as a float in the range [0.0, 1.0] where 1.0 is defined as all
    /// cores pegged
    fn used_cpu_percent(&self) -> f64;
    /// Return system used memory as a float in the range [0.0, 1.0]
    fn used_mem_percent(&self) -> f64 {
        self.used_mem() as f64 / self.total_mem() as f64
    }
}

#[async_trait::async_trait]
impl<MI, SK> SlotSupplier for ResourceBasedSlotsForType<MI, SK>
where
    MI: SystemResourceInfo + Send + Sync + 'static,
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        loop {
            if ctx.num_issued_slots() < self.opts.min_slots {
                return self.issue_slot();
            } else {
                let must_wait_for = self
                    .opts
                    .ramp_throttle
                    .saturating_sub(self.time_since_last_issued());
                if must_wait_for > Duration::from_millis(0) {
                    tokio::time::sleep(must_wait_for).await;
                }
                if let Some(p) = self.try_reserve_slot(ctx) {
                    return p;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        let num_issued = ctx.num_issued_slots();
        if num_issued < self.opts.min_slots
            || (self.time_since_last_issued() > self.opts.ramp_throttle
                && num_issued < self.opts.max_slots
                && self.inner.pid_decision()
                && self.inner.can_reserve())
        {
            Some(self.issue_slot())
        } else {
            None
        }
    }

    fn mark_slot_used(&self, _info: SK::Info<'_>) {}

    fn release_slot(&self) {}
}

impl<MI, SK> ResourceBasedSlotsForType<MI, SK>
where
    MI: Send + Sync + SystemResourceInfo,
    SK: Send + SlotKind + Sync,
{
}

impl<MI, SK> ResourceBasedSlotsForType<MI, SK>
where
    MI: SystemResourceInfo + Send + Sync,
    SK: SlotKind + Send + Sync,
{
    fn new(inner: Arc<ResourceBasedSlots<MI>>, opts: ResourceSlotOptions) -> Self {
        let (tx, rx) = watch::channel(Instant::now());
        Self {
            opts,
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            inner,
            _slot_kind: PhantomData,
        }
    }

    fn issue_slot(&self) -> SlotSupplierPermit {
        let _ = self.last_slot_issued_tx.send(Instant::now());
        SlotSupplierPermit::default()
    }

    fn time_since_last_issued(&self) -> Duration {
        Instant::now()
            .checked_duration_since(*self.last_slot_issued_rx.borrow())
            .unwrap_or_default()
    }
}

impl<MI: SystemResourceInfo + Sync + Send + 'static> WorkerTuner for ResourceBasedTuner<MI> {
    fn workflow_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync> {
        let o = self.wf_opts.unwrap_or(DEFAULT_WF_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn activity_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync> {
        let o = self.act_opts.unwrap_or(DEFAULT_ACT_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn local_activity_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync> {
        let o = self.la_opts.unwrap_or(DEFAULT_ACT_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn attach_metrics(&self, metrics: TemporalMeter) {
        self.slots.attach_metrics(metrics);
    }
}

impl<MI: SystemResourceInfo + Sync + Send> ResourceBasedSlots<MI> {
    /// Create a [ResourceBasedSlotsForType] for this instance which is willing to hand out
    /// `minimum` slots with no checks at all and `max` slots ever. Otherwise the underlying
    /// mem/cpu targets will attempt to be matched while under load.
    ///
    /// `ramp_throttle` determines how long this will pause for between making determinations about
    /// whether it is OK to hand out new slot(s). This is important to set to nonzero in situations
    /// where activities might use a lot of resources, because otherwise the implementation may
    /// hand out many slots quickly before resource usage has a chance to be reflected, possibly
    /// resulting in OOM (for example).
    pub fn as_kind<SK: SlotKind + Send + Sync>(
        self: &Arc<Self>,
        opts: ResourceSlotOptions,
    ) -> Arc<ResourceBasedSlotsForType<MI, SK>> {
        Arc::new(ResourceBasedSlotsForType::new(self.clone(), opts))
    }

    fn new_with_sysinfo(options: ResourceBasedSlotsOptions, sys_info: MI) -> Self {
        Self {
            pids: Mutex::new(PidControllers::new(&options)),
            options,
            metrics: OnceLock::new(),
            sys_info_supplier: sys_info,
            last_metric_vals: Arc::new(AtomicCell::new(Default::default())),
        }
    }

    fn can_reserve(&self) -> bool {
        self.sys_info_supplier.used_mem_percent() <= self.options.target_mem_usage
    }

    /// Returns true if the pid controllers think a new slot should be given out
    fn pid_decision(&self) -> bool {
        let mut pids = self.pids.lock();
        let mem_used_percent = self.sys_info_supplier.used_mem_percent();
        let cpu_used_percent = self.sys_info_supplier.used_cpu_percent();
        let mem_output = pids.mem.next_control_output(mem_used_percent).output;
        let cpu_output = pids.cpu.next_control_output(cpu_used_percent).output;
        self.last_metric_vals.store(LastMetricVals {
            mem_output,
            cpu_output,
            mem_used_percent,
            cpu_used_percent,
        });
        mem_output > self.options.mem_output_threshold
            && cpu_output > self.options.cpu_output_threshold
    }

    fn attach_metrics(&self, metrics: TemporalMeter) {
        // Launch a task to periodically emit metrics
        self.metrics.get_or_init(move || {
            let m = MetricInstruments::new(metrics);
            let last_vals = self.last_metric_vals.clone();
            tokio::task::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    let lv = last_vals.load();
                    m.mem_pid_output.record(lv.mem_output, &m.attribs);
                    m.cpu_pid_output.record(lv.cpu_output, &m.attribs);
                    m.mem_usage.record(lv.mem_used_percent * 100., &m.attribs);
                    m.cpu_usage.record(lv.cpu_used_percent * 100., &m.attribs);
                    interval.tick().await;
                }
            })
        });
    }
}

/// Implements [SystemResourceInfo] using the [sysinfo] crate
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
impl SystemResourceInfo for RealSysInfo {
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
    use crate::{abstractions::MeteredPermitDealer, telemetry::metrics::MetricsContext};
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
    impl SystemResourceInfo for FakeMIS {
        fn total_mem(&self) -> u64 {
            100_000
        }

        fn used_mem(&self) -> u64 {
            self.used.load(Ordering::Acquire)
        }

        fn used_cpu_percent(&self) -> f64 {
            0.0
        }
    }

    fn test_options() -> ResourceBasedSlotsOptions {
        ResourceBasedSlotsOptionsBuilder::default()
            .target_mem_usage(0.8)
            .target_cpu_usage(1.0)
            .build()
            .expect("default resource based slot options can't fail to build")
    }

    #[test]
    fn mem_workflow_sync() {
        let (fmis, used) = FakeMIS::new();
        let rbs = Arc::new(ResourceBasedSlots::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(rbs.clone(), MetricsContext::no_op(), None);
        assert!(rbs.try_reserve_slot(&pd).is_some());
        used.store(90_000, Ordering::Release);
        assert!(rbs.try_reserve_slot(&pd).is_none());
    }

    #[tokio::test]
    async fn mem_workflow_async() {
        let (fmis, used) = FakeMIS::new();
        used.store(90_000, Ordering::Release);
        let rbs = Arc::new(ResourceBasedSlots::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(rbs.clone(), MetricsContext::no_op(), None);
        let order = crossbeam_queue::ArrayQueue::new(2);
        let waits_free = async {
            rbs.reserve_slot(&pd).await;
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

    #[test]
    fn minimum_respected() {
        let (fmis, used) = FakeMIS::new();
        let rbs = Arc::new(ResourceBasedSlots::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 2,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(rbs.clone(), MetricsContext::no_op(), None);
        used.store(90_000, Ordering::Release);
        let _p1 = pd.try_acquire_owned().unwrap();
        let _p2 = pd.try_acquire_owned().unwrap();
        assert!(pd.try_acquire_owned().is_err());
    }
}
