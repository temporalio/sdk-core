//! This module contains very generic helpers that can be used codebase-wide

use crate::MetricsContext;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit, TryAcquireError};

/// Wraps a [Semaphore] with a function call that is fed the available permits any time a permit is
/// acquired or restored through the provided methods
pub(crate) struct MeteredSemaphore {
    sem: Semaphore,
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
            metrics_ctx,
            record_fn,
        }
    }

    #[cfg(test)]
    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        let res = self.sem.acquire().await;
        self.record();
        res
    }

    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        let res = self.sem.try_acquire();
        self.record();
        res
    }

    /// Adds just one permit
    pub fn add_permit(&self) {
        self.add_permits(1);
    }

    pub fn add_permits(&self, amount: usize) {
        self.sem.add_permits(amount);
        self.record();
    }

    fn record(&self) {
        (self.record_fn)(&self.metrics_ctx, self.sem.available_permits());
    }
}
