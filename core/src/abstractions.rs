//! This module contains very generic helpers that can be used codebase-wide

pub(crate) mod take_cell;

use crate::MetricsContext;
use derive_more::DebugCustom;
use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use temporal_sdk_core_api::worker::{
    SlotKind, SlotReleaseReason, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
};
use tokio_util::sync::CancellationToken;

/// Wraps a [SlotSupplier] and turns successful slot reservations into permit structs, as well
/// as handling associated metrics tracking.
#[derive(Clone)]
pub(crate) struct MeteredPermitDealer<SK: SlotKind> {
    supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
    /// The number of permit owners who have acquired a permit, but are not yet meaningfully using
    /// that permit. This is useful for giving a more semantically accurate count of used task
    /// slots, since we typically wait for a permit first before polling, but that slot isn't used
    /// in the sense the user expects until we actually also get the corresponding task.
    unused_claimants: Arc<AtomicUsize>,
    /// The number of permits that have been handed out
    extant_permits: Arc<AtomicUsize>,
    metrics_ctx: MetricsContext,
}

impl<SK> MeteredPermitDealer<SK>
where
    SK: SlotKind + 'static,
{
    pub(crate) fn new(
        supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
        metrics_ctx: MetricsContext,
    ) -> Self {
        Self {
            supplier,
            unused_claimants: Arc::new(AtomicUsize::new(0)),
            extant_permits: Arc::new(AtomicUsize::new(0)),
            metrics_ctx,
        }
    }

    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.supplier.available_slots()
    }

    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.available_permits()
            .map(|ap| ap + self.unused_claimants.load(Ordering::Acquire))
    }

    pub(crate) async fn acquire_owned(&self) -> Result<OwnedMeteredSemPermit<SK>, ()> {
        let res = self.supplier.reserve_slot(self).await;
        Ok(self.build_owned(res))
    }

    pub(crate) fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit<SK>, ()> {
        if let Some(res) = self.supplier.try_reserve_slot(self) {
            Ok(self.build_owned(res))
        } else {
            Err(())
        }
    }

    fn build_owned(&self, res: SlotSupplierPermit) -> OwnedMeteredSemPermit<SK> {
        self.unused_claimants.fetch_add(1, Ordering::Release);
        self.extant_permits.fetch_add(1, Ordering::Release);
        // Eww
        let uc_c = self.unused_claimants.clone();
        let ep_c = self.extant_permits.clone();
        let ep_c_c = self.extant_permits.clone();
        let supp = self.supplier.clone();
        let supp_c = self.supplier.clone();
        let supp_c_c = self.supplier.clone();
        let mets = self.metrics_ctx.clone();
        let metric_rec =
            // When being called from the drop impl, the semaphore permit isn't actually dropped yet,
            // so account for that. TODO: that should move somehow into fixed impl only
            move |add_one: bool| {
                let extra = usize::from(add_one);
                if let Some(avail) = supp.available_slots() {
                    mets.available_task_slots(avail + uc_c.load(Ordering::Acquire) + extra);
                }
                mets.task_slots_used((ep_c.load(Ordering::Acquire) + extra) as u64);
            };
        let mrc = metric_rec.clone();
        mrc(false);

        OwnedMeteredSemPermit {
            _inner: res,
            unused_claimants: Some(self.unused_claimants.clone()),
            use_fn: Box::new(move |info| {
                supp_c.mark_slot_used(info);
                metric_rec(false)
            }),
            release_fn: Box::new(move || {
                // TODO: Real release reason
                supp_c_c.release_slot(SlotReleaseReason::TaskComplete);
                ep_c_c.fetch_sub(1, Ordering::Release);
                mrc(true)
            }),
        }
    }
}

impl<SK: SlotKind> SlotReservationContext for MeteredPermitDealer<SK> {
    fn num_issued_slots(&self) -> usize {
        self.extant_permits.load(Ordering::Acquire)
    }
}

/// A version of [MeteredPermitDealer] that can be closed and supports waiting for close to complete.
/// Once closed, no permits will be handed out.
/// Close completes when all permits have been returned.
pub(crate) struct ClosableMeteredPermitDealer<SK: SlotKind> {
    inner: Arc<MeteredPermitDealer<SK>>,
    outstanding_permits: AtomicUsize,
    close_requested: AtomicBool,
    close_complete_token: CancellationToken,
}

impl<SK> ClosableMeteredPermitDealer<SK>
where
    SK: SlotKind,
{
    pub(crate) fn new_arc(sem: Arc<MeteredPermitDealer<SK>>) -> Arc<Self> {
        Arc::new(Self {
            inner: sem,
            outstanding_permits: Default::default(),
            close_requested: AtomicBool::new(false),
            close_complete_token: CancellationToken::new(),
        })
    }
}

impl<SK> ClosableMeteredPermitDealer<SK>
where
    SK: SlotKind + 'static,
{
    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.inner.unused_permits()
    }

    /// Request to close the semaphore and prevent new permits from being acquired.
    pub(crate) fn close(&self) {
        self.close_requested.store(true, Ordering::Release);
        if self.outstanding_permits.load(Ordering::Acquire) == 0 {
            self.close_complete_token.cancel();
        }
    }

    /// Returns after close has been requested and all outstanding permits have been returned.
    pub(crate) async fn close_complete(&self) {
        self.close_complete_token.cancelled().await;
    }

    /// Acquire a permit if one is available and close was not requested.
    pub(crate) fn try_acquire_owned(
        self: &Arc<Self>,
    ) -> Result<TrackedOwnedMeteredSemPermit<SK>, ()> {
        if self.close_requested.load(Ordering::Acquire) {
            return Err(());
        }
        self.outstanding_permits.fetch_add(1, Ordering::Release);
        let res = self.inner.try_acquire_owned();
        if res.is_err() {
            self.outstanding_permits.fetch_sub(1, Ordering::Release);
        }
        res.map(|permit| TrackedOwnedMeteredSemPermit {
            inner: Some(permit),
            on_drop: self.on_permit_dropped(),
        })
    }

    fn on_permit_dropped(self: &Arc<Self>) -> Box<dyn Fn() + Send + Sync> {
        let sem = self.clone();
        Box::new(move || {
            sem.outstanding_permits.fetch_sub(1, Ordering::Release);
            if sem.close_requested.load(Ordering::Acquire)
                && sem.outstanding_permits.load(Ordering::Acquire) == 0
            {
                sem.close_complete_token.cancel();
            }
        })
    }
}

/// Tracks an OwnedMeteredSemPermit and calls on_drop when dropped.
#[derive(DebugCustom)]
#[clippy::has_significant_drop]
#[debug(fmt = "Tracked({inner:?})")]
pub(crate) struct TrackedOwnedMeteredSemPermit<SK: SlotKind> {
    inner: Option<OwnedMeteredSemPermit<SK>>,
    on_drop: Box<dyn Fn() + Send + Sync>,
}
impl<SK: SlotKind> From<TrackedOwnedMeteredSemPermit<SK>> for OwnedMeteredSemPermit<SK> {
    fn from(mut value: TrackedOwnedMeteredSemPermit<SK>) -> Self {
        value
            .inner
            .take()
            .expect("Inner permit should be available")
    }
}
impl<SK: SlotKind> Drop for TrackedOwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// Wraps an [OwnedSemaphorePermit] to update metrics when it's dropped
#[clippy::has_significant_drop]
pub(crate) struct OwnedMeteredSemPermit<SK: SlotKind> {
    _inner: SlotSupplierPermit,
    /// See [MeteredPermitDealer::unused_claimants]. If present when dropping, used to decrement the
    /// count.
    unused_claimants: Option<Arc<AtomicUsize>>,
    use_fn: Box<dyn Fn(SK::Info<'_>) + Send + Sync>,
    release_fn: Box<dyn Fn() + Send + Sync>,
}
impl<SK: SlotKind> Drop for OwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        (self.release_fn)()
    }
}
impl<SK: SlotKind> Debug for OwnedMeteredSemPermit<SK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnedMeteredSemPermit()")
    }
}
impl<SK: SlotKind> OwnedMeteredSemPermit<SK> {
    /// Should be called once this permit is actually being "used" for the work it was meant to
    /// permit.
    pub(crate) fn into_used(mut self, info: SK::Info<'_>) -> UsedMeteredSemPermit<SK> {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        (self.use_fn)(info);
        UsedMeteredSemPermit(self)
    }
}

#[derive(Debug)]
pub(crate) struct UsedMeteredSemPermit<SK: SlotKind>(#[allow(dead_code)] OwnedMeteredSemPermit<SK>);

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::worker::slot_supplier::FixedSizeSlotSupplier;
    use temporal_sdk_core_api::worker::WorkflowSlotKind;

    pub fn fixed_size_permit_dealer<SK: SlotKind + Send + Sync + 'static>(
        size: usize,
    ) -> MeteredPermitDealer<SK> {
        MeteredPermitDealer::new(
            Arc::new(FixedSizeSlotSupplier::new(size)),
            MetricsContext::no_op(),
            |_, _| {},
        )
    }

    #[tokio::test]
    async fn closable_semaphore_permit_drop_returns_permit() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        let permits = sem.outstanding_permits.load(Ordering::Acquire);
        assert_eq!(permits, 1);
        drop(perm);
        let permits = sem.outstanding_permits.load(Ordering::Acquire);
        assert_eq!(permits, 0);
    }

    #[tokio::test]
    async fn closable_semaphore_permit_drop_after_close_resolves_close_complete() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        sem.close();
        drop(perm);
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_close_complete_ready_if_unused() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_does_not_hand_out_permits_after_closed() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        let perm = sem.try_acquire_owned().unwrap_err();
        assert_matches!(perm, ());
    }
}
