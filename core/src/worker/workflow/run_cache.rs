use crate::{
    telemetry::metrics::workflow_type,
    worker::workflow::{
        managed_run::{ManagedRun, RunUpdateActs},
        HeartbeatTimeoutMsg, HistoryUpdate, LocalActivityRequestSink, PermittedWFT,
    },
    MetricsContext,
};
use lru::LruCache;
use std::{mem, num::NonZeroUsize};
use tokio::sync::mpsc::UnboundedSender;

pub(super) struct RunCache {
    max: usize,
    namespace: String,
    /// Run id -> Data
    runs: LruCache<String, ManagedRun>,
    local_activity_request_sink: LocalActivityRequestSink,
    // Used to feed heartbeat timeout events back into the workflow inputs.
    // TODO: Ideally, we wouldn't spawn tasks inside here now that this is tokio-free-ish.
    //   Instead, `Workflows` can spawn them itself if there are any LAs.
    heartbeat_tx: UnboundedSender<HeartbeatTimeoutMsg>,

    metrics: MetricsContext,
}

impl RunCache {
    pub fn new(
        max_cache_size: usize,
        namespace: String,
        local_activity_request_sink: LocalActivityRequestSink,
        heartbeat_tx: UnboundedSender<HeartbeatTimeoutMsg>,
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
            runs: LruCache::new(
                NonZeroUsize::new(lru_size).expect("LRU size is guaranteed positive"),
            ),
            local_activity_request_sink,
            heartbeat_tx,
            metrics,
        }
    }

    pub fn instantiate_or_update(&mut self, mut pwft: PermittedWFT) -> RunUpdateActs {
        let cur_num_cached_runs = self.runs.len();
        let run_id = &pwft.work.execution.run_id;

        if let Some(run_handle) = self.runs.get_mut(run_id) {
            let rur = run_handle.incoming_wft(pwft);
            self.metrics.cache_size(cur_num_cached_runs as u64);
            return rur;
        }

        // Create a new workflow machines instance for this workflow, initialize it, and
        // track it.
        let metrics = self
            .metrics
            .with_new_attrs([workflow_type(pwft.work.workflow_type.clone())]);
        // Replace the update in the wft with a dummy one, since we must instantiate the machines
        // with the update.
        let history_update = mem::replace(&mut pwft.work.update, HistoryUpdate::dummy());
        let mut mrh = ManagedRun::new(
            history_update,
            self.namespace.clone(),
            pwft.work.execution.workflow_id.clone(),
            pwft.work.workflow_type.clone(),
            pwft.work.execution.run_id.clone(),
            self.local_activity_request_sink.clone(),
            self.heartbeat_tx.clone(),
            metrics,
        );
        let run_id = run_id.to_string();
        let rur = mrh.incoming_wft(pwft);
        if self.runs.push(run_id, mrh).is_some() {
            panic!("Overflowed run cache! Cache owner is expected to avoid this!");
        }
        self.metrics.cache_size(cur_num_cached_runs as u64 + 1);
        rur
    }
    pub fn remove(&mut self, k: &str) -> Option<ManagedRun> {
        let r = self.runs.pop(k);
        self.metrics.cache_size(self.len() as u64);
        r
    }

    pub fn get_mut(&mut self, k: &str) -> Option<&mut ManagedRun> {
        self.runs.get_mut(k)
    }
    pub fn get(&mut self, k: &str) -> Option<&ManagedRun> {
        self.runs.get(k)
    }

    /// Returns the current least-recently-used run. Returns `None` when cache empty.
    pub fn current_lru_run(&self) -> Option<&str> {
        self.runs.peek_lru().map(|(run_id, _)| run_id.as_str())
    }
    /// Returns an iterator yielding cached runs in LRU order
    pub fn runs_lru_order(&self) -> impl Iterator<Item = (&str, &ManagedRun)> {
        self.runs.iter().rev().map(|(k, v)| (k.as_str(), v))
    }
    pub fn peek(&self, k: &str) -> Option<&ManagedRun> {
        self.runs.peek(k)
    }
    pub fn has_run(&self, k: &str) -> bool {
        self.runs.contains(k)
    }
    pub fn handles(&self) -> impl Iterator<Item = &ManagedRun> {
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
