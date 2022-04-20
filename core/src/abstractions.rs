//! This module contains very generic helpers that can be used codebase-wide

use crate::MetricsContext;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

/// Wraps a [Semaphore] with a function call that is fed the available permits any time a permit is
/// acquired or restored through the provided methods
pub(crate) struct MeteredSemaphore {
    pub sem: Semaphore,
    max_permits: usize,
    metrics_ctx: MetricsContext,
    record_fn: fn(&MetricsContext, usize),
}

impl MeteredSemaphore {
    pub fn new(
        inital_permits: usize,
        metrics_ctx: MetricsContext,
        record_fn: fn(&MetricsContext, usize),
    ) -> Self {
        Self {
            sem: Semaphore::new(inital_permits),
            max_permits: inital_permits,
            metrics_ctx,
            record_fn,
        }
    }

    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        let res = self.sem.acquire().await;
        (self.record_fn)(&self.metrics_ctx, self.sem.available_permits());
        res
    }

    /// Adds just one permit. Will not add if already at the initial/max capacity.
    pub fn add_permit(&self) {
        if self.sem.available_permits() < self.max_permits {
            self.sem.add_permits(1);
            (self.record_fn)(&self.metrics_ctx, self.sem.available_permits());
        } else if cfg!(debug_assertions) {
            // Panic only during debug mode if this happens
            panic!("Tried to add permit to a semaphore that already was at capacity!");
        }
    }
}
