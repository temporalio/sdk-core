mod fixed_size;
mod resource_based;

pub use fixed_size::FixedSizeSlotSupplier;
pub use resource_based::{
    RealSysInfo, ResourceBasedSlots, ResourceBasedTuner, ResourceSlotOptions,
};

use std::sync::{Arc, OnceLock};
use temporal_sdk_core_api::{
    telemetry::metrics::TemporalMeter,
    worker::{
        ActivitySlotKind, LocalActivitySlotKind, SlotSupplier, WorkerConfig, WorkerTuner,
        WorkflowSlotKind,
    },
};

/// Allows for the composition of different slot suppliers into a [WorkerTuner]
pub struct TunerHolder {
    wft_supplier: Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>,
    act_supplier: Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>,
    la_supplier: Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>,
    metrics: OnceLock<TemporalMeter>,
}

/// Can be used to construct a `TunerHolder` from individual slot suppliers. Any supplier which is
/// not provided will default to a [FixedSizeSlotSupplier] with a capacity of 100.
#[derive(Default, Clone)]
pub struct TunerBuilder {
    workflow_slot_supplier:
        Option<Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>>,
    activity_slot_supplier:
        Option<Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>>,
    local_activity_slot_supplier:
        Option<Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>>,
}

impl TunerBuilder {
    pub(crate) fn from_config(cfg: &WorkerConfig) -> Self {
        let mut builder = Self::default();
        if let Some(m) = cfg.max_outstanding_workflow_tasks {
            builder.workflow_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(m)));
        }
        if let Some(m) = cfg.max_outstanding_activities {
            builder.activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(m)));
        }
        if let Some(m) = cfg.max_outstanding_local_activities {
            builder.local_activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(m)));
        }
        builder
    }

    /// Set a workflow slot supplier
    pub fn workflow_slot_supplier(
        &mut self,
        supplier: Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>,
    ) -> &mut Self {
        self.workflow_slot_supplier = Some(supplier);
        self
    }

    /// Set an activity slot supplier
    pub fn activity_slot_supplier(
        &mut self,
        supplier: Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>,
    ) -> &mut Self {
        self.activity_slot_supplier = Some(supplier);
        self
    }

    /// Set a local activity slot supplier
    pub fn local_activity_slot_supplier(
        &mut self,
        supplier: Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>,
    ) -> &mut Self {
        self.local_activity_slot_supplier = Some(supplier);
        self
    }

    /// Build a [WorkerTuner] from the configured slot suppliers
    pub fn build(&mut self) -> Arc<dyn WorkerTuner + Send + Sync> {
        Arc::new(TunerHolder {
            wft_supplier: self
                .workflow_slot_supplier
                .clone()
                .unwrap_or_else(|| Arc::new(FixedSizeSlotSupplier::new(100))),
            act_supplier: self
                .activity_slot_supplier
                .clone()
                .unwrap_or_else(|| Arc::new(FixedSizeSlotSupplier::new(100))),
            la_supplier: self
                .local_activity_slot_supplier
                .clone()
                .unwrap_or_else(|| Arc::new(FixedSizeSlotSupplier::new(100))),
            metrics: OnceLock::new(),
        })
    }
}

impl WorkerTuner for TunerHolder {
    fn workflow_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync> {
        self.wft_supplier.clone()
    }

    fn activity_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync> {
        self.act_supplier.clone()
    }

    fn local_activity_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync> {
        self.la_supplier.clone()
    }

    fn attach_metrics(&self, m: TemporalMeter) {
        let _ = self.metrics.set(m);
    }
}
