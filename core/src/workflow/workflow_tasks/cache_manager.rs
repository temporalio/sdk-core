use crate::{telemetry::metrics::MetricsContext, workflow::WorkflowCachingPolicy};
use lru::LruCache;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;

/// Helps to maintain an LRU ordering in which workflow runs have been accessed so that old runs may
/// be evicted once we reach the cap.
#[derive(Debug)]
pub(crate) struct WorkflowCacheManager {
    cache: LruCache<String, ()>,
    metrics: MetricsContext,
    cap_notify: Arc<Notify>,
    cache_size: Arc<AtomicUsize>,
    cap_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl WorkflowCacheManager {
    pub fn new(policy: WorkflowCachingPolicy, metrics: MetricsContext) -> Self {
        let cap = match policy {
            WorkflowCachingPolicy::Sticky {
                max_cached_workflows,
            } => max_cached_workflows,
            _ => 0,
        };
        Self {
            cache: LruCache::new(cap),
            metrics,
            cap_notify: Arc::new(Notify::new()),
            cache_size: Arc::new(AtomicUsize::new(0)),
            cap_mutex: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[cfg(test)]
    fn new_test(policy: WorkflowCachingPolicy) -> Self {
        Self::new(policy, Default::default())
    }

    /// Resolves once there is an open slot in the cache. The passed in closure can be used to
    /// exit the wait loop if it returns true even if the cache is not below the limit. It will be
    /// re-evaluated every time there is an insert or remove to the cache, even if it did not change
    /// the size.
    pub fn wait_for_capacity<Fun>(&self, early_exit: Fun) -> Option<impl Future<Output = ()>>
    where
        Fun: Fn() -> bool,
    {
        if self.cache.cap() == 0 {
            return None;
        }

        let size = self.cache_size.clone();
        let notify = self.cap_notify.clone();
        let cap = self.cache.cap();
        let mx = self.cap_mutex.clone();
        Some(async move {
            let _l = mx.lock().await;
            while !early_exit() && size.load(Ordering::Acquire) >= cap {
                notify.notified().await;
            }
        })
    }

    /// Inserts a record associated with the run id into the lru cache.
    /// Once cache reaches capacity, overflow records will be returned back to the caller.
    pub fn insert(&mut self, run_id: &str) -> Option<String> {
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

        res
    }

    /// If run id exists in the cache it will be moved to the top of the LRU cache.
    pub fn touch(&mut self, run_id: &str) {
        self.cache.get(run_id);
    }

    pub fn remove(&mut self, run_id: &str) {
        self.cache.pop(run_id);
        self.size_changed();
    }

    fn size_changed(&self) {
        let size = self.cache.len();
        self.metrics.cache_size(size as u64);
        self.cache_size.store(size, Ordering::Release);
        self.cap_notify.notify_one();
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
