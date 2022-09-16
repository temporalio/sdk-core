use crate::{
    telemetry::metrics::workflow_type,
    worker::workflow::{
        managed_run::WorkflowManager, HistoryUpdate, LocalActivityRequestSink, ManagedRunHandle,
        NewIncomingWFT, RunUpdateResponse,
    },
    MetricsContext,
};
use lru::LruCache;
use std::{num::NonZeroUsize, time::Instant};
use tokio::sync::mpsc::UnboundedSender;

pub(super) struct RunCache {
    max: usize,
    namespace: String,
    run_update_tx: UnboundedSender<RunUpdateResponse>,
    /// Run id -> Data
    runs: LruCache<String, ManagedRunHandle>,
    local_activity_request_sink: LocalActivityRequestSink,

    metrics: MetricsContext,
}

impl RunCache {
    pub fn new(
        max_cache_size: usize,
        namespace: String,
        run_update_tx: UnboundedSender<RunUpdateResponse>,
        local_activity_request_sink: LocalActivityRequestSink,
        metrics: MetricsContext,
    ) -> Self {
        // The cache needs room for at least one run, otherwise we couldn't do anything. In
        // "0" size mode, the run is evicted once the workflow task is complete.
        let lru_size = if max_cache_size > 0 {
            max_cache_size
        } else {
            1
        };
        Self {
            max: max_cache_size,
            namespace,
            run_update_tx,
            runs: LruCache::new(
                NonZeroUsize::new(lru_size).expect("LRU size is guaranteed positive"),
            ),
            local_activity_request_sink,
            metrics,
        }
    }

    pub fn instantiate_or_update(
        &mut self,
        run_id: &str,
        workflow_id: &str,
        wf_type: &str,
        history_update: HistoryUpdate,
        start_time: Instant,
    ) -> &mut ManagedRunHandle {
        let cur_num_cached_runs = self.runs.len();

        if self.runs.contains(run_id) {
            // For some weird reason, maybe a NLL bug, there are double-mutable-borrow errors if I
            // use get_mut above instead of in here (even though we always return from this branch).
            // So, forced to do this.
            let run_handle = self.runs.get_mut(run_id).unwrap();

            run_handle.metrics.sticky_cache_hit();
            run_handle.incoming_wft(NewIncomingWFT {
                history_update: Some(history_update),
                start_time,
            });
            self.metrics.cache_size(cur_num_cached_runs as u64);
            return run_handle;
        }

        // Create a new workflow machines instance for this workflow, initialize it, and
        // track it.
        let metrics = self
            .metrics
            .with_new_attrs([workflow_type(wf_type.to_string())]);
        let wfm = WorkflowManager::new(
            history_update,
            self.namespace.clone(),
            workflow_id.to_string(),
            wf_type.to_string(),
            run_id.to_string(),
            metrics.clone(),
        );
        let mut mrh = ManagedRunHandle::new(
            wfm,
            self.run_update_tx.clone(),
            self.local_activity_request_sink.clone(),
            metrics,
        );
        mrh.incoming_wft(NewIncomingWFT {
            history_update: None,
            start_time,
        });
        if self.runs.push(run_id.to_string(), mrh).is_some() {
            panic!("Overflowed run cache! Cache owner is expected to avoid this!");
        }
        self.metrics.cache_size(cur_num_cached_runs as u64 + 1);
        // This is safe, we just inserted.
        self.runs.get_mut(run_id).unwrap()
    }
    pub fn remove(&mut self, k: &str) -> Option<ManagedRunHandle> {
        let r = self.runs.pop(k);
        self.metrics.cache_size(self.len() as u64);
        r
    }

    pub fn get_mut(&mut self, k: &str) -> Option<&mut ManagedRunHandle> {
        self.runs.get_mut(k)
    }
    pub fn get(&mut self, k: &str) -> Option<&ManagedRunHandle> {
        self.runs.get(k)
    }

    /// Returns the current least-recently-used run. Returns `None` when cache empty.
    pub fn current_lru_run(&self) -> Option<&str> {
        self.runs.peek_lru().map(|(run_id, _)| run_id.as_str())
    }
    /// Returns an iterator yielding cached runs in LRU order
    pub fn runs_lru_order(&self) -> impl Iterator<Item = (&str, &ManagedRunHandle)> {
        self.runs.iter().rev().map(|(k, v)| (k.as_str(), v))
    }
    pub fn peek(&self, k: &str) -> Option<&ManagedRunHandle> {
        self.runs.peek(k)
    }
    pub fn has_run(&self, k: &str) -> bool {
        self.runs.contains(k)
    }
    pub fn handles(&self) -> impl Iterator<Item = &ManagedRunHandle> {
        self.runs.iter().map(|(_, v)| v)
    }
    pub fn is_full(&self) -> bool {
        self.runs.cap().get() == self.runs.len()
    }
    pub fn len(&self) -> usize {
        self.runs.len()
    }
    pub fn cache_capacity(&self) -> usize {
        self.max
    }
}
