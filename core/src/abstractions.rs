//! This module contains very generic helpers that can be used codebase-wide

use crate::MetricsContext;
use futures::{stream, Stream, StreamExt};
use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Wraps a [Semaphore] with a function call that is fed the available permits any time a permit is
/// acquired or restored through the provided methods
#[derive(Clone)]
pub(crate) struct MeteredSemaphore {
    sem: Arc<Semaphore>,
    /// The number of permit owners who have acquired a permit from the semaphore, but are not yet
    /// meaningfully using that permit. This is useful for giving a more semantically accurate count
    /// of used task slots, since we typically wait for a permit first before polling, but that slot
    /// isn't used in the sense the user expects until we actually also get the corresponding task.
    unused_claimants: Arc<AtomicUsize>,
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
            unused_claimants: Arc::new(AtomicUsize::new(0)),
            metrics_ctx,
            record_fn,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire_owned(&self) -> Result<OwnedMeteredSemPermit, AcquireError> {
        let res = self.sem.clone().acquire_owned().await?;
        Ok(self.build_owned(res))
    }

    pub fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit, TryAcquireError> {
        let res = self.sem.clone().try_acquire_owned()?;
        Ok(self.build_owned(res))
    }

    fn build_owned(&self, res: OwnedSemaphorePermit) -> OwnedMeteredSemPermit {
        self.unused_claimants.fetch_add(1, Ordering::Release);
        self.record();
        OwnedMeteredSemPermit {
            inner: res,
            unused_claimants: Some(self.unused_claimants.clone()),
            record_fn: self.record_owned(),
        }
    }

    fn record(&self) {
        (self.record_fn)(
            &self.metrics_ctx,
            self.sem.available_permits() + self.unused_claimants.load(Ordering::Acquire),
        );
    }

    fn record_owned(&self) -> Box<dyn Fn(bool) + Send + Sync> {
        let rcf = self.record_fn;
        let mets = self.metrics_ctx.clone();
        let sem = self.sem.clone();
        let uc = self.unused_claimants.clone();
        // When being called from the drop impl, the semaphore permit isn't actually dropped yet,
        // so account for that.
        Box::new(move |add_one: bool| {
            let extra = usize::from(add_one);
            rcf(
                &mets,
                sem.available_permits() + uc.load(Ordering::Acquire) + extra,
            )
        })
    }
}

/// Wraps an [OwnedSemaphorePermit] to update metrics when it's dropped
pub(crate) struct OwnedMeteredSemPermit {
    inner: OwnedSemaphorePermit,
    /// See [MeteredSemaphore::unused_claimants]. If present when dropping, used to decrement the
    /// count.
    unused_claimants: Option<Arc<AtomicUsize>>,
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
        self.inner.fmt(f)
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
pub(crate) struct UsedMeteredSemPermit(OwnedMeteredSemPermit);
impl UsedMeteredSemPermit {
    #[cfg(feature = "save_wf_inputs")]
    pub(crate) fn fake_deserialized() -> Self {
        let sem = Arc::new(Semaphore::new(1));
        let inner = sem.try_acquire_owned().unwrap();
        Self(OwnedMeteredSemPermit {
            inner,
            unused_claimants: None,
            record_fn: Box::new(|_| {}),
        })
    }
}

/// From the input stream, create a new stream which only pulls from the input stream when allowed.
/// When allowed is determined by the passed in `proceeder` emitting an item. The input stream is
/// only pulled from when that future resolves.
///
/// This is *almost* identical to `zip`, but does not terminate early if the input stream closes.
/// The proceeder must allow the poll before the returned stream closes. If the proceeder terminates
/// the overall stream will terminate.
pub(crate) fn stream_when_allowed<S, AS>(
    input: S,
    proceeder: AS,
) -> impl Stream<Item = (S::Item, AS::Item)>
where
    S: Stream + Send + 'static,
    AS: Stream + Send + 'static,
{
    let stream = stream::unfold(
        (proceeder.boxed(), input.boxed()),
        |(mut proceeder, mut input)| async {
            let v = proceeder.next().await;
            if let Some(v) = v {
                input.next().await.map(|i| ((i, v), (proceeder, input)))
            } else {
                None
            }
        },
    );
    stream
}

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
    use futures::pin_mut;
    use std::task::Poll;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    #[test]
    fn stream_when_allowed_works() {
        let inputs = stream::iter([1, 2, 3]);
        let (allow_tx, allow_rx) = unbounded_channel();
        let when_allowed = stream_when_allowed(inputs, UnboundedReceiverStream::new(allow_rx));

        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);
        pin_mut!(when_allowed);

        allow_tx.send(()).unwrap();
        assert_eq!(
            when_allowed.poll_next_unpin(&mut cx),
            Poll::Ready(Some((1, ())))
        );
        // Now, it won't be ready
        for _ in 1..10 {
            assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        }
        allow_tx.send(()).unwrap();
        assert_eq!(
            when_allowed.poll_next_unpin(&mut cx),
            Poll::Ready(Some((2, ())))
        );
        for _ in 1..10 {
            assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        }
        allow_tx.send(()).unwrap();
        assert_eq!(
            when_allowed.poll_next_unpin(&mut cx),
            Poll::Ready(Some((3, ())))
        );
        for _ in 1..10 {
            assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Pending);
        }
        allow_tx.send(()).unwrap();
        assert_eq!(when_allowed.poll_next_unpin(&mut cx), Poll::Ready(None));
    }
}
