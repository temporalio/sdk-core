//! This module contains very generic helpers that can be used codebase-wide

use crate::MetricsContext;
use futures::{stream, Stream, StreamExt};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tokio::sync::{
    AcquireError, Notify, OwnedSemaphorePermit, Semaphore, SemaphorePermit, TryAcquireError,
};

/// Wraps a [Semaphore] with a function call that is fed the available permits any time a permit is
/// acquired or restored through the provided methods
pub(crate) struct MeteredSemaphore {
    sem: Arc<Semaphore>,
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
            sem: Arc::new(Semaphore::new(inital_permits)),
            max_permits: inital_permits,
            metrics_ctx,
            record_fn,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        let res = self.sem.acquire().await;
        self.record();
        res
    }

    pub async fn acquire_owned(&self) -> Result<OwnedMeteredSemPermit, AcquireError> {
        let res = self.sem.clone().acquire_owned().await?;
        self.record();
        Ok(OwnedMeteredSemPermit {
            inner: res,
            record_fn: self.record_drop_owned(),
        })
    }

    pub fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit, TryAcquireError> {
        let res = self.sem.clone().try_acquire_owned()?;
        self.record();
        Ok(OwnedMeteredSemPermit {
            inner: res,
            record_fn: self.record_drop_owned(),
        })
    }

    /// Adds just one permit. Will not add if already at the initial/max capacity.
    pub fn add_permit(&self) {
        self.add_permits(1);
    }

    /// Adds a number of permits. Will not add if already at the initial/max capacity.
    pub fn add_permits(&self, amount: usize) {
        if self.sem.available_permits() + amount <= self.max_permits {
            self.sem.add_permits(amount);
            self.record();
        } else if cfg!(debug_assertions) {
            // Panic only during debug mode if this happens
            panic!("Tried to add permit to a semaphore that already was at capacity!");
        }
    }

    fn record(&self) {
        (self.record_fn)(&self.metrics_ctx, self.sem.available_permits());
    }

    fn record_drop_owned(&self) -> Box<dyn Fn() + Send + Sync> {
        let rcf = self.record_fn;
        let mets = self.metrics_ctx.clone();
        let sem = self.sem.clone();
        Box::new(move || rcf(&mets, sem.available_permits() + 1))
    }
}

/// Wraps an [OwnedSemaphorePermit] to update metrics when it's dropped
pub(crate) struct OwnedMeteredSemPermit {
    inner: OwnedSemaphorePermit,
    record_fn: Box<dyn Fn() + Send + Sync>,
}
impl Drop for OwnedMeteredSemPermit {
    fn drop(&mut self) {
        (self.record_fn)()
    }
}
impl Debug for OwnedMeteredSemPermit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(Clone)]
pub(crate) struct StreamAllowHandle {
    acceptable_notify: Arc<Notify>,
}
impl StreamAllowHandle {
    pub fn allow_one(&self) {
        self.acceptable_notify.notify_one();
    }
}

/// From the input stream, create a new stream which only pulls from the input stream when allowed.
/// The stream will start allowed, and sets itself to unallowed each time it delivers the next item.
/// The returned [StreamAllowHandle::allow_one] function can be used to indicate it is OK to deliver
/// the next item.
pub(crate) fn stream_when_allowed<S>(input: S) -> (StreamAllowHandle, impl Stream<Item = S::Item>)
where
    S: Stream + Send + 'static,
{
    let acceptable_notify = Arc::new(Notify::new());
    acceptable_notify.notify_one();
    let handle = StreamAllowHandle { acceptable_notify };
    let stream = stream::unfold(
        (handle.clone(), input.boxed()),
        |(handle, mut input)| async {
            handle.acceptable_notify.notified().await;
            input.next().await.map(|i| (i, (handle, input)))
        },
    );
    (handle, stream)
}

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      error!($($arg)*);
      debug_assert!(true, $($arg)*);
  };
}
pub(crate) use dbg_panic;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::pin_mut;
    use std::task::Poll;

    #[test]
    fn stream_when_allowed_works() {
        let inputs = stream::iter([1, 2, 3]);
        let (handle, when_allowed) = stream_when_allowed(inputs);

        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);
        pin_mut!(when_allowed);

        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Ready(Some(1)));
        // Now, it won't be ready
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        // Still not ready...
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        handle.allow_one();
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Ready(Some(2)));
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        handle.allow_one();
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Ready(Some(3)));
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        handle.allow_one();
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Ready(None));
    }
}
