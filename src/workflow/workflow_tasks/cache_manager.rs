use crate::{telemetry::metrics::MetricsContext, workflow::WorkflowCachingPolicy};
use lru::LruCache;

/// Helps to maintain an LRU ordering in which workflow runs have been accessed so that old runs may
/// be evicted once we reach the cap.
#[derive(Debug)]
pub(crate) struct WorkflowCacheManager {
    cache: LruCache<String, ()>,
    metrics: MetricsContext,
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
        }
    }

    #[cfg(test)]
    fn new_test(policy: WorkflowCachingPolicy) -> Self {
        Self::new(policy, Default::default())
    }

    /// Inserts a record associated with the run id into the lru cache.
    /// Once cache reaches capacity, overflow records will be returned back to the caller.
    pub fn insert(&mut self, run_id: &str) -> Option<String> {
        let res = if self.cache.len() < self.cache.cap() {
            // Blindly add a record into the cache, since it still has capacity.
            self.cache.put(run_id.to_owned(), ());
            None
        } else if self.cache.cap() != 0 {
            let maybe_got_evicted = self.cache.peek_lru().map(|r| r.0.to_owned());
            let already_existed = self.cache.put(run_id.to_owned(), ()).is_some();
            if !already_existed {
                maybe_got_evicted
            } else {
                None
            }
        } else {
            // Run id should be evicted right away as cache size is 0.
            Some(run_id.to_owned())
        };

        self.metrics.cache_size(self.cache.len() as u64);

        res
    }

    /// If run id exists in the cache it will be moved to the top of the LRU cache.
    pub fn touch(&mut self, run_id: &str) {
        self.cache.get(run_id);
    }

    pub fn remove(&mut self, run_id: &str) {
        self.cache.pop(run_id);
        self.metrics.cache_size(self.cache.len() as u64);
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
            assert_eq!(run_id, "1")
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
            assert_eq!(run_id, "2")
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
            assert_eq!(run_id, "2")
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
            assert_eq!(run_id, "1")
        });
    }

    #[test]
    fn zero_cache_size() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 0,
        });
        assert_matches!(wcm.insert("1"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
        assert_matches!(wcm.insert("2"), Some(run_id) => {
            assert_eq!(run_id, "2")
        });
    }

    #[test]
    fn non_sticky_always_pending_eviction() {
        let mut wcm = WorkflowCacheManager::new_test(WorkflowCachingPolicy::NonSticky);
        assert_matches!(wcm.insert("1"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
        assert_matches!(wcm.insert("2"), Some(run_id) => {
            assert_eq!(run_id, "2")
        });
    }
}
