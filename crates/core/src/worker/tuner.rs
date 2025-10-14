mod fixed_size;
mod resource_based;

pub use fixed_size::FixedSizeSlotSupplier;
pub use resource_based::{
    ResourceBasedSlotsOptions, ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner,
    ResourceSlotOptions,
};

pub(crate) use resource_based::{RealSysInfo, SystemResourceInfo};

use std::sync::Arc;
use temporalio_common::worker::{
    ActivitySlotKind, LocalActivitySlotKind, NexusSlotKind, SlotKind, SlotSupplier, WorkerConfig,
    WorkerTuner, WorkflowSlotKind,
};

/// Allows for the composition of different slot suppliers into a [WorkerTuner]
pub struct TunerHolder {
    wft_supplier: Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>,
    act_supplier: Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>,
    la_supplier: Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>,
    nexus_supplier: Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync>,
}

/// Can be used to construct a [TunerHolder] without needing to manually construct each
/// [SlotSupplier]. Useful for lang bridges to allow more easily passing through user options.
#[derive(Clone, Debug, derive_builder::Builder)]
#[builder(build_fn(validate = "Self::validate"))]
#[non_exhaustive]
pub struct TunerHolderOptions {
    /// Options for workflow slots
    #[builder(default, setter(strip_option))]
    pub workflow_slot_options: Option<SlotSupplierOptions<WorkflowSlotKind>>,
    /// Options for activity slots
    #[builder(default, setter(strip_option))]
    pub activity_slot_options: Option<SlotSupplierOptions<ActivitySlotKind>>,
    /// Options for local activity slots
    #[builder(default, setter(strip_option))]
    pub local_activity_slot_options: Option<SlotSupplierOptions<LocalActivitySlotKind>>,
    /// Options for nexus slots
    #[builder(default, setter(strip_option))]
    pub nexus_slot_options: Option<SlotSupplierOptions<NexusSlotKind>>,
    /// Options that will apply to all resource based slot suppliers. Must be set if any slot
    /// options are [SlotSupplierOptions::ResourceBased]
    #[builder(default, setter(strip_option))]
    pub resource_based_options: Option<ResourceBasedSlotsOptions>,
}

impl TunerHolderOptions {
    /// Create a [TunerHolder] from these options
    pub fn build_tuner_holder(self) -> Result<TunerHolder, anyhow::Error> {
        let mut builder = TunerBuilder::default();
        // safety note: unwraps here are OK since the builder validator guarantees options for
        // a resource based tuner are present if any supplier is resource based
        let mut rb_tuner = self
            .resource_based_options
            .map(ResourceBasedTuner::new_from_options);
        match self.workflow_slot_options {
            Some(SlotSupplierOptions::FixedSize { slots }) => {
                builder.workflow_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(slots)));
            }
            Some(SlotSupplierOptions::ResourceBased(rso)) => {
                builder.workflow_slot_supplier(
                    rb_tuner
                        .as_mut()
                        .unwrap()
                        .with_workflow_slots_options(rso)
                        .workflow_task_slot_supplier(),
                );
            }
            Some(SlotSupplierOptions::Custom(ss)) => {
                builder.workflow_slot_supplier(ss);
            }
            None => {}
        }
        match self.activity_slot_options {
            Some(SlotSupplierOptions::FixedSize { slots }) => {
                builder.activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(slots)));
            }
            Some(SlotSupplierOptions::ResourceBased(rso)) => {
                builder.activity_slot_supplier(
                    rb_tuner
                        .as_mut()
                        .unwrap()
                        .with_activity_slots_options(rso)
                        .activity_task_slot_supplier(),
                );
            }
            Some(SlotSupplierOptions::Custom(ss)) => {
                builder.activity_slot_supplier(ss);
            }
            None => {}
        }
        match self.local_activity_slot_options {
            Some(SlotSupplierOptions::FixedSize { slots }) => {
                builder.local_activity_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(slots)));
            }
            Some(SlotSupplierOptions::ResourceBased(rso)) => {
                builder.local_activity_slot_supplier(
                    rb_tuner
                        .as_mut()
                        .unwrap()
                        .with_local_activity_slots_options(rso)
                        .local_activity_slot_supplier(),
                );
            }
            Some(SlotSupplierOptions::Custom(ss)) => {
                builder.local_activity_slot_supplier(ss);
            }
            None => {}
        }
        match self.nexus_slot_options {
            Some(SlotSupplierOptions::FixedSize { slots }) => {
                builder.nexus_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(slots)));
            }
            Some(SlotSupplierOptions::ResourceBased(rso)) => {
                builder.nexus_slot_supplier(
                    rb_tuner
                        .as_mut()
                        .unwrap()
                        .with_nexus_slots_options(rso)
                        .nexus_task_slot_supplier(),
                );
            }
            Some(SlotSupplierOptions::Custom(ss)) => {
                builder.nexus_slot_supplier(ss);
            }
            None => {}
        }
        if let Some(tuner) = rb_tuner {
            builder.sys_info(tuner.sys_info());
        }
        Ok(builder.build())
    }
}

/// Options for known kinds of slot suppliers
#[derive(Clone, derive_more::Debug)]
pub enum SlotSupplierOptions<SK: SlotKind> {
    /// Options for a [FixedSizeSlotSupplier]
    FixedSize {
        /// The number of slots the fixed supplier will have
        slots: usize,
    },
    /// Options for a [ResourceBasedSlots]
    ResourceBased(ResourceSlotOptions),
    /// A user-implemented slot supplier
    #[debug("Custom")]
    Custom(Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>),
}

impl TunerHolderOptionsBuilder {
    /// Create a [TunerHolder] from this builder
    pub fn build_tuner_holder(self) -> Result<TunerHolder, anyhow::Error> {
        let s = self.build()?;
        s.build_tuner_holder()
    }

    fn validate(&self) -> Result<(), String> {
        let any_is_resource_based = matches!(
            self.workflow_slot_options,
            Some(Some(SlotSupplierOptions::ResourceBased(_)))
        ) || matches!(
            self.activity_slot_options,
            Some(Some(SlotSupplierOptions::ResourceBased(_)))
        ) || matches!(
            self.local_activity_slot_options,
            Some(Some(SlotSupplierOptions::ResourceBased(_)))
        ) || matches!(
            self.nexus_slot_options,
            Some(Some(SlotSupplierOptions::ResourceBased(_)))
        );
        if any_is_resource_based && matches!(self.resource_based_options, None | Some(None)) {
            return Err(
                "`resource_based_options` must be set if any slot options are ResourceBased"
                    .to_string(),
            );
        }
        Ok(())
    }
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
    nexus_slot_supplier: Option<Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync>>,
    sys_info: Option<Arc<dyn SystemResourceInfo + Send + Sync>>,
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
        if let Some(m) = cfg.max_outstanding_nexus_tasks {
            builder.nexus_slot_supplier(Arc::new(FixedSizeSlotSupplier::new(m)));
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

    /// Set a nexus slot supplier
    pub fn nexus_slot_supplier(
        &mut self,
        supplier: Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync>,
    ) -> &mut Self {
        self.nexus_slot_supplier = Some(supplier);
        self
    }

    /// Sets a field that implements [SystemResourceInfo]
    pub fn sys_info(&mut self, sys_info: Arc<dyn SystemResourceInfo + Send + Sync>) -> &mut Self {
        self.sys_info = Some(sys_info);
        self
    }

    /// Gets the field that implements [SystemResourceInfo]
    pub fn get_sys_info(&self) -> Option<Arc<dyn SystemResourceInfo + Send + Sync>> {
        self.sys_info.clone()
    }

    /// Build a [WorkerTuner] from the configured slot suppliers
    pub fn build(&mut self) -> TunerHolder {
        TunerHolder {
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
            nexus_supplier: self
                .nexus_slot_supplier
                .clone()
                .unwrap_or_else(|| Arc::new(FixedSizeSlotSupplier::new(100))),
        }
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

    fn nexus_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync> {
        self.nexus_supplier.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use temporalio_common::worker::{
        SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplierPermit,
    };

    struct TestSlotSupplier;
    #[async_trait::async_trait]
    impl SlotSupplier for TestSlotSupplier {
        type SlotKind = NexusSlotKind;
        async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
            SlotSupplierPermit::default()
        }
        fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
            Some(SlotSupplierPermit::default())
        }
        fn mark_slot_used(&self, _: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}
        fn release_slot(&self, _: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {}
    }

    #[test]
    fn tuner_holder_options_nexus_fixed_size() {
        let options = TunerHolderOptions {
            workflow_slot_options: None,
            activity_slot_options: None,
            local_activity_slot_options: None,
            nexus_slot_options: Some(SlotSupplierOptions::FixedSize { slots: 50 }),
            resource_based_options: None,
        };

        let tuner = options.build_tuner_holder().unwrap();
        // The tuner is built successfully with fixed size nexus slots
        let _ = tuner.nexus_task_slot_supplier();
    }

    #[test]
    fn tuner_holder_options_nexus_resource_based() {
        let resource_opts = ResourceBasedSlotsOptionsBuilder::default()
            .target_mem_usage(0.8)
            .target_cpu_usage(0.9)
            .build()
            .unwrap();

        let options = TunerHolderOptions {
            workflow_slot_options: None,
            activity_slot_options: None,
            local_activity_slot_options: None,
            nexus_slot_options: Some(SlotSupplierOptions::ResourceBased(
                ResourceSlotOptions::new(5, 100, Duration::from_millis(100)),
            )),
            resource_based_options: Some(resource_opts),
        };

        let tuner = options.build_tuner_holder().unwrap();
        // The tuner is built successfully with resource-based nexus slots
        let _ = tuner.nexus_task_slot_supplier();
    }

    #[test]
    fn tuner_holder_options_nexus_custom() {
        let custom_supplier: Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync> =
            Arc::new(TestSlotSupplier);

        let options = TunerHolderOptions {
            workflow_slot_options: None,
            activity_slot_options: None,
            local_activity_slot_options: None,
            nexus_slot_options: Some(SlotSupplierOptions::Custom(custom_supplier.clone())),
            resource_based_options: None,
        };

        let tuner = options.build_tuner_holder().unwrap();
        // The tuner is built successfully with custom nexus slots
        let _ = tuner.nexus_task_slot_supplier();
    }

    #[test]
    fn tuner_builder_with_nexus_slot_supplier() {
        let mut builder = TunerBuilder::default();
        let custom_supplier: Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync> =
            Arc::new(FixedSizeSlotSupplier::new(25));

        builder.nexus_slot_supplier(custom_supplier.clone());
        let tuner = builder.build();

        // The tuner is built successfully with the custom nexus slot supplier
        let _ = tuner.nexus_task_slot_supplier();
    }

    #[test]
    fn tuner_holder_options_builder_validates_resource_based_requirements() {
        // Should fail when nexus uses ResourceBased but resource_based_options is not set
        let result = TunerHolderOptionsBuilder::default()
            .nexus_slot_options(SlotSupplierOptions::ResourceBased(
                ResourceSlotOptions::new(5, 100, Duration::from_millis(100)),
            ))
            .build();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("resource_based_options")
        );
    }

    #[test]
    fn tuner_holder_options_all_slot_types() {
        let resource_opts = ResourceBasedSlotsOptionsBuilder::default()
            .target_mem_usage(0.8)
            .target_cpu_usage(0.9)
            .build()
            .unwrap();

        let options = TunerHolderOptions {
            workflow_slot_options: Some(SlotSupplierOptions::FixedSize { slots: 10 }),
            activity_slot_options: Some(SlotSupplierOptions::FixedSize { slots: 20 }),
            local_activity_slot_options: Some(SlotSupplierOptions::ResourceBased(
                ResourceSlotOptions::new(2, 50, Duration::from_millis(100)),
            )),
            nexus_slot_options: Some(SlotSupplierOptions::ResourceBased(
                ResourceSlotOptions::new(5, 100, Duration::from_millis(100)),
            )),
            resource_based_options: Some(resource_opts),
        };

        let tuner = options.build_tuner_holder().unwrap();
        // All suppliers should be successfully configured
        let _ = tuner.workflow_task_slot_supplier();
        let _ = tuner.activity_task_slot_supplier();
        let _ = tuner.local_activity_slot_supplier();
        let _ = tuner.nexus_task_slot_supplier();
    }
}
