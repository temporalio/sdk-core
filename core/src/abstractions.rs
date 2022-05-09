//! This module contains very generic helpers that can be used codebase-wide

use crate::MetricsContext;
use futures::{stream, Stream, StreamExt};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{
    AcquireError, Notify, OwnedSemaphorePermit, Semaphore, SemaphorePermit, TryAcquireError,
};

/// Wraps a [Semaphore] with a function call that is fed the available permits any time a permit is
/// acquired or restored through the provided methods
pub(crate) struct MeteredSemaphore {
    pub sem: Arc<Semaphore>,
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

    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        let res = self.sem.acquire().await;
        (self.record_fn)(&self.metrics_ctx, self.sem.available_permits());
        res
    }

    pub fn try_acquire_owned(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let res = self.sem.clone().try_acquire_owned();
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

#[derive(Clone)]
pub(crate) struct StreamAllowHandle {
    acceptable_to_get_next: Arc<AtomicBool>,
    acceptable_notify: Arc<Notify>,
}
impl StreamAllowHandle {
    pub fn allow_one(&self) {
        self.acceptable_to_get_next.store(true, Ordering::Release);
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
    let acceptable_to_get_next = Arc::new(AtomicBool::new(true));
    let acceptable_notify = Arc::new(Notify::new());
    let handle = StreamAllowHandle {
        acceptable_to_get_next,
        acceptable_notify,
    };
    let stream = stream::unfold(
        (handle.clone(), input.boxed()),
        |(handle, mut input)| async {
            if !handle.acceptable_to_get_next.load(Ordering::Acquire) {
                handle.acceptable_notify.notified().await;
            }
            handle
                .acceptable_to_get_next
                .store(false, Ordering::Release);
            input.next().await.map(|i| (i, (handle, input)))
        },
    );
    (handle, stream)
}

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
