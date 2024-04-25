// use crate::telemetry::metrics::MetricsContext;
// use std::sync::{
//     atomic::{AtomicU64, Ordering},
//     Arc,
// };
// use temporal_sdk_core_api::worker::{
//     SlotKind, SlotReleaseReason, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
// };
//
// pub(crate) struct TrackingSlotSupplier<SK> {
//     inner: Arc<TrackingSSImpl<SK>>,
// }
//
// struct TrackingSSImpl<SK> {
//     inner: Box<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
//     outstanding_slots: AtomicU64,
//     // TODO: Used slots distinction
//     metrics: MetricsContext,
// }
//
// impl<SK> TrackingSSImpl<SK> {
//     pub(crate) fn record_used(&self, val: u64) {
//         self.metrics.task_slots_used(val);
//     }
// }
//
// struct MetricPermit
//
// #[async_trait::async_trait]
// impl<SK: SlotKind + Send + Sync> SlotSupplier for TrackingSlotSupplier<SK> {
//     type SlotKind = SK;
//
//     async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
//         let p = self.inner.inner.reserve_slot(ctx).await;
//         let v = self.inner.outstanding_slots.fetch_add(1, Ordering::Relaxed);
//         self.inner.record_used(v + 1);
//         p
//     }
//
//     fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
//         let p = self.inner.try_reserve_slot(ctx);
//         if p.is_some() {
//             let v = self.outstanding_slots.fetch_add(1, Ordering::Relaxed);
//             self.record_used(v + 1);
//         }
//         p
//     }
//
//     fn mark_slot_used(&self, info: <Self::SlotKind as SlotKind>::Info<'_>) {
//         self.inner.mark_slot_used(info)
//     }
//
//     fn release_slot(&self, info: SlotReleaseReason) {
//         self.inner.release_slot(info);
//         let v = self.outstanding_slots.fetch_sub(1, Ordering::Relaxed);
//         self.record_used(v - 1);
//     }
//
//     fn available_slots(&self) -> Option<usize> {
//         self.inner.available_slots()
//     }
// }
