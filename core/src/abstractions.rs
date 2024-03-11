//! This module contains very generic helpers that can be used codebase-wide

pub(crate) mod take_cell;

use crate::{
    worker::slot_supplier::{SlotKind, SlotSupplier, SlotSupplierPermit},
    MetricsContext,
};
use derive_more::DebugCustom;
use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
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
    metrics_ctx: MetricsContext,
    record_fn: fn(&MetricsContext, usize),
}

impl<SK> MeteredPermitDealer<SK>
where
    SK: SlotKind,
{
    pub(crate) fn new(
        supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
        metrics_ctx: MetricsContext,
        record_fn: fn(&MetricsContext, usize),
    ) -> Self {
        Self {
            supplier,
            unused_claimants: Arc::new(AtomicUsize::new(0)),
            metrics_ctx,
            record_fn,
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

    pub(crate) async fn acquire_owned(&self) -> Result<OwnedMeteredSemPermit, ()> {
        let res = self.supplier.reserve_slot().await;
        Ok(self.build_owned(res))
    }

    pub(crate) fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit, ()> {
        if let Some(res) = self.supplier.try_reserve_slot() {
            Ok(self.build_owned(res))
        } else {
            Err(())
        }
    }

    fn build_owned(&self, res: SlotSupplierPermit) -> OwnedMeteredSemPermit {
        self.unused_claimants.fetch_add(1, Ordering::Release);
        // self.record();
        OwnedMeteredSemPermit {
            inner: res,
            unused_claimants: Some(self.unused_claimants.clone()),
            // TODO: Fix
            record_fn: Box::new(|_| {}),
        }
    }

    // TODO: Move inside semaphore slot supplier impl
    // fn record(&self) {
    //     (self.record_fn)(
    //         &self.metrics_ctx,
    //         self.sem.available_permits() + self.unused_claimants.load(Ordering::Acquire),
    //     );
    // }
    //
    // fn record_owned(&self) -> Box<dyn Fn(bool) + Send + Sync> {
    //     let rcf = self.record_fn;
    //     let mets = self.metrics_ctx.clone();
    //     let sem = self.sem.clone();
    //     let uc = self.unused_claimants.clone();
    //     // When being called from the drop impl, the semaphore permit isn't actually dropped yet,
    //     // so account for that.
    //     Box::new(move |add_one: bool| {
    //         let extra = usize::from(add_one);
    //         rcf(
    //             &mets,
    //             sem.available_permits() + uc.load(Ordering::Acquire) + extra,
    //         )
    //     })
    // }
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
    SK: SlotKind,
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
    pub(crate) fn try_acquire_owned(self: &Arc<Self>) -> Result<TrackedOwnedMeteredSemPermit, ()> {
        if self.close_requested.load(Ordering::Acquire) {
            return Err(());
        }
        self.outstanding_permits.fetch_add(1, Ordering::Release);
        let res = self.inner.try_acquire_owned();
        if res.is_err() {
            self.outstanding_permits.fetch_sub(1, Ordering::Release);
        }
        res.map(|permit| TrackedOwnedMeteredSemPermit {
            inner: permit,
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
#[debug(fmt = "Tracked({inner:?})")]
pub(crate) struct TrackedOwnedMeteredSemPermit {
    inner: OwnedMeteredSemPermit,
    on_drop: Box<dyn Fn() + Send + Sync>,
}
impl From<TrackedOwnedMeteredSemPermit> for OwnedMeteredSemPermit {
    fn from(mut value: TrackedOwnedMeteredSemPermit) -> Self {
        value.inner
    }
}
impl Drop for TrackedOwnedMeteredSemPermit {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// Wraps an [OwnedSemaphorePermit] to update metrics when it's dropped
pub(crate) struct OwnedMeteredSemPermit {
    inner: SlotSupplierPermit,
    /// See [MeteredPermitDealer::unused_claimants]. If present when dropping, used to decrement the
    /// count.
    unused_claimants: Option<Arc<AtomicUsize>>,
    // TODO: Should probably be ref to supplier
    record_fn: Box<dyn Fn(bool) + Send + Sync>,
}
impl Drop for OwnedMeteredSemPermit {
    fn drop(&mut self) {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        (self.record_fn)(true)
    }
}
impl Debug for OwnedMeteredSemPermit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnedMeteredSemPermit()")
    }
}
impl OwnedMeteredSemPermit {
    /// Should be called once this permit is actually being "used" for the work it was meant to
    /// permit.
    pub(crate) fn into_used(mut self) -> UsedMeteredSemPermit {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
            (self.record_fn)(false)
        }
        UsedMeteredSemPermit(self)
    }
}

#[derive(Debug)]
pub(crate) struct UsedMeteredSemPermit(
    // Field isn't read, but we need to hold on to it.
    #[allow(dead_code)] OwnedMeteredSemPermit,
);

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closable_semaphore_permit_drop_returns_permit() {
        let inner = MeteredPermitDealer::new(2, MetricsContext::no_op(), |_, _| {});
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
        let inner = MeteredPermitDealer::new(2, MetricsContext::no_op(), |_, _| {});
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        sem.close();
        drop(perm);
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_close_complete_ready_if_unused() {
        let inner = MeteredPermitDealer::new(2, MetricsContext::no_op(), |_, _| {});
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_does_not_hand_out_permits_after_closed() {
        let inner = MeteredPermitDealer::new(2, MetricsContext::no_op(), |_, _| {});
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        let perm = sem.try_acquire_owned().unwrap_err();
        assert_matches!(perm, TryAcquireError::Closed);
    }
}
