use crate::workflow::WorkflowCachingPolicy;
use lru::LruCache;

pub(crate) struct WorkflowCacheManager {
    /// LRU cache of run IDs.
    cache: LruCache<String, ()>,
}

impl WorkflowCacheManager {
    pub fn new(policy: WorkflowCachingPolicy) -> Self {
        let cap = match policy {
            WorkflowCachingPolicy::Sticky {
                max_cached_workflows,
            } => max_cached_workflows,
            _ => 0,
        };
        Self {
            cache: LruCache::new(cap),
        }
    }

    /// Touches record associated with run id in the cache.
    /// Once cache reaches capacity, overflow records will be returned back to the caller.
    /// Caller is responsible for doing all cleanup for the run id.
    pub fn touch(&mut self, run_id: &str) -> Option<String> {
        if self.cache.len() < self.cache.cap() {
            // Blindly add a record into the cache, since it still has capacity.
            self.cache.put(run_id.to_owned(), ());
            None
        } else if self.cache.cap() != 0 {
            let result = self.cache.peek_lru().map(|r| r.0.to_owned());
            self.cache.put(run_id.to_owned(), ());
            result
        } else {
            // Run id should be evicted right away as cache size is 0.
            Some(run_id.to_owned())
        }
    }

    /// Removes run id from the cache.
    pub fn evict(&mut self, run_id: &str) {
        self.cache.pop(&run_id.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn touch_with_overflow() {
        let mut wcm = WorkflowCacheManager::new(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        assert_matches!(wcm.touch("1"), None);
        assert_matches!(wcm.touch("2"), None);
        assert_matches!(wcm.touch("3"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
    }

    #[test]
    fn touch_and_evict() {
        let mut wcm = WorkflowCacheManager::new(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        assert_matches!(wcm.touch("1"), None);
        assert_matches!(wcm.touch("2"), None);
        wcm.evict("1");
        assert_matches!(wcm.touch("3"), None);
        assert_matches!(wcm.touch("4"), Some(run_id) => {
            assert_eq!(run_id, "2")
        });
    }

    #[test]
    fn evict_early() {
        let mut wcm = WorkflowCacheManager::new(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 2,
        });
        wcm.evict("1");
        assert_matches!(wcm.touch("1"), None);
        assert_matches!(wcm.touch("2"), None);
        assert_matches!(wcm.touch("3"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
    }

    #[test]
    fn zero_cache_size() {
        let mut wcm = WorkflowCacheManager::new(WorkflowCachingPolicy::Sticky {
            max_cached_workflows: 0,
        });
        assert_matches!(wcm.touch("1"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
        assert_matches!(wcm.touch("2"), Some(run_id) => {
            assert_eq!(run_id, "2")
        });
    }

    #[test]
    fn non_sticky_always_pending_eviction() {
        let mut wcm = WorkflowCacheManager::new(WorkflowCachingPolicy::NonSticky);
        assert_matches!(wcm.touch("1"), Some(run_id) => {
            assert_eq!(run_id, "1")
        });
        assert_matches!(wcm.touch("2"), Some(run_id) => {
            assert_eq!(run_id, "2")
        });
    }
}
