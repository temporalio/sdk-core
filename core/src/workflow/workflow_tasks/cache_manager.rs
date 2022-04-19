use crate::{telemetry::metrics::MetricsContext, workflow::WorkflowCachingPolicy};
use futures::future;
use lru::LruCache;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

/// Helps to maintain an LRU ordering in which workflow runs have been accessed so that old runs may
/// be evicted once we reach the cap.
#[derive(Debug)]
pub(crate) struct WorkflowCacheManager {
    cache: LruCache<String, ()>,
    metrics: MetricsContext,
    watch_tx: watch::Sender<usize>,
    watch_rx: watch::Receiver<usize>,
    cap_mutex: Arc<tokio::sync::Mutex<()>>,
    outstanding_eviction: Option<String>,
}

impl WorkflowCacheManager {
    pub fn new(policy: WorkflowCachingPolicy, metrics: MetricsContext) -> Self {
        let cap = match policy {
            WorkflowCachingPolicy::Sticky {
                max_cached_workflows,
            } => max_cached_workflows,
            _ => 0,
        };
        let (watch_tx, watch_rx) = watch::channel(0);
        Self {
            cache: LruCache::new(cap),
            metrics,
            watch_tx,
            watch_rx,
            cap_mutex: Arc::new(Mutex::new(())),
            outstanding_eviction: None,
        }
    }

    #[cfg(test)]
    fn new_test(policy: WorkflowCachingPolicy) -> Self {
        Self::new(policy, Default::default())
    }

    /// Returns total cache capacity
    pub fn capacity(&self) -> usize {
        self.cache.cap()
    }

    /// Resolves once there is an open slot in the cache
    pub fn wait_for_capacity(
        &self,
    ) -> future::Either<impl Future<Output = ()>, impl Future<Output = ()>> {
        if self.cache.cap() == 0 {
            return future::Either::Left(future::ready(()));
        }

        let mut rx = self.watch_rx.clone();
        let cap = self.cache.cap();
        let mx = self.cap_mutex.clone();
        future::Either::Right(async move {
            let _l = mx.lock();
            // Must use strict `>` because we need to be able to exceed the cache size by one
            // temporarily, to allow for the fact that if we poll while at the limit, we might
            // get a task for a new run we haven't seen before - which will cause us to issue an
            // evict, thus temporarily exceeding the limit. Until the evict is completed, this
            // will block and hence we won't exceed further.
            while *rx.borrow_and_update() > cap {
                let _ = rx.changed().await;
            }
        })
    }

    /// Inserts a record associated with the run id into the lru cache.
    /// Once cache reaches capacity, overflow records will be returned back to the caller.
    pub fn insert(&mut self, run_id: &str) -> Option<String> {
        if matches!(self.outstanding_eviction.as_ref(), Some(r) if r == run_id) {
            return None;
        }

        let res = if self.cache.len() < self.cache.cap() {
            // Blindly add a record into the cache, since it still has capacity.
            self.cache.put(run_id.to_owned(), ());
            None
        } else if self.cache.cap() == 0 {
            // Run id should be evicted right away as cache size is 0.
            Some(run_id.to_owned())
        } else {
            let maybe_got_evicted = self.cache.peek_lru().map(|r| r.0.clone());
            let not_cached = self.cache.put(run_id.to_owned(), ()).is_none();
            not_cached.then(|| maybe_got_evicted).flatten()
        };

        self.size_changed();

        if let Some(rid) = res.as_ref() {
            self.outstanding_eviction = Some(rid.clone())
        }

        res
    }

    /// If run id exists in the cache it will be moved to the top of the LRU cache.
    pub fn touch(&mut self, run_id: &str) {
        self.cache.get(run_id);
    }

    pub fn remove(&mut self, run_id: &str) {
        let outstanding = self.outstanding_eviction.take();
        if matches!(outstanding.as_ref(), Some(r) if r != run_id) {
            // TODO: This isn't an error since eviction could be completed out-of-order?
            error!("Evicted not-outstanding run");
        }
        self.cache.pop(run_id);
        self.size_changed();
    }

    fn size_changed(&self) {
        let size = self.cache.len();
        self.metrics.cache_size(size as u64);
        let _ = self.watch_tx.send(size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insert_with_overflow() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        assert_matches!(wcm.insert("1"), None);
        assert_matches!(wcm.insert("2"), None);
        assert_matches!(wcm.insert("3"), Some(run_id) => {
            assert_eq!(run_id, "1");
        });
    }

    #[test]
    fn insert_remove_insert() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 1,
        });
        assert_matches!(wcm.insert("1"), None);
        wcm.remove("1");
        assert_matches!(wcm.insert("2"), None);
        assert_matches!(wcm.insert("3"), Some(run_id) => {
            assert_eq!(run_id, "2");
        });
    }

    #[test]
    fn insert_same_id_twice_doesnt_evict_self() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 1,
        });
        assert_matches!(wcm.insert("1"), None);
        assert_matches!(wcm.insert("1"), None);
    }

    #[test]
    fn insert_and_touch() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        assert_matches!(wcm.insert("1"), None);
        assert_matches!(wcm.insert("2"), None);
        wcm.touch("1");
        assert_matches!(wcm.insert("3"), Some(run_id) => {
            assert_eq!(run_id, "2");
        });
    }

    #[test]
    fn touch_early() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        wcm.touch("1");
        assert_matches!(wcm.insert("1"), None);
        assert_matches!(wcm.insert("2"), None);
        assert_matches!(wcm.insert("3"), Some(run_id) => {
            assert_eq!(run_id, "1");
        });
    }

    #[test]
    fn zero_cache_size() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 0,
        });
        assert_matches!(wcm.insert("1"), Some(run_id) => {
            assert_eq!(run_id, "1");
        });
        assert_matches!(wcm.insert("2"), Some(run_id) => {
            assert_eq!(run_id, "2");
        });
    }

    #[test]
    fn non_sticky_always_pending_eviction() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::NonSticky);
        assert_matches!(wcm.insert("1"), Some(run_id) => {
            assert_eq!(run_id, "1");
        });
        assert_matches!(wcm.insert("2"), Some(run_id) => {
            assert_eq!(run_id, "2");
        });
    }
}
