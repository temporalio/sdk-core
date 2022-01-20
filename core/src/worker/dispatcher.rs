use crate::{
    telemetry::metrics::{task_queue, MetricsContext},
    worker::Worker,
    WorkerConfig, WorkerLookupErr, WorkerRegistrationError,
};
use arc_swap::ArcSwap;
use futures::future::join_all;
use std::{collections::HashMap, ops::Deref, option::Option, sync::Arc};
use temporal_client::ServerGatewayApis;
use tokio::sync::Notify;

/// Allows access to workers by task queue name
#[derive(Default)]
pub(crate) struct WorkerDispatcher {
    /// Maps task queue names to workers. If the value is `None` then the worker existed at one
    /// point and was shut down.
    workers: ArcSwap<HashMap<String, Option<WorkerRefCt>>>,
}

impl WorkerDispatcher {
    pub fn new_worker(
        &self,
        config: WorkerConfig,
        sticky_queue: Option<String>,
        gateway: Arc<dyn ServerGatewayApis + Send + Sync>,
        parent_metrics: MetricsContext,
    ) -> Result<(), WorkerRegistrationError> {
        let tq = config.task_queue.clone();
        let metrics = parent_metrics.with_new_attrs([task_queue(config.task_queue.clone())]);
        let worker = Worker::new(config, sticky_queue, gateway, metrics);
        self.set_worker_for_task_queue(tq, worker)
    }

    pub fn set_worker_for_task_queue(
        &self,
        tq: String,
        worker: Worker,
    ) -> Result<(), WorkerRegistrationError> {
        if self
            .workers
            .load()
            .get(&tq)
            .map(Option::is_some)
            .unwrap_or_default()
        {
            return Err(WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(tq));
        }
        let tq = &tq;
        let worker = WorkerRefCt::new(worker);
        self.workers.rcu(|map| {
            let mut map = HashMap::clone(map);
            map.insert(tq.clone(), Some(worker.clone()));
            map
        });
        Ok(())
    }

    pub fn get(&self, task_queue: &str) -> Result<impl Deref<Target = Worker>, WorkerLookupErr> {
        if let Some(w) = self.workers.load().get(task_queue) {
            if let Some(w) = w {
                Ok(w.clone())
            } else {
                Err(WorkerLookupErr::Shutdown(task_queue.to_string()))
            }
        } else {
            Err(WorkerLookupErr::NoWorker(task_queue.to_string()))
        }
    }

    pub async fn shutdown_one(&self, task_queue: &str) {
        info!("Shutting down worker on queue {}", task_queue);
        let mut maybe_worker = None;
        if let Some(stored_worker) = self.workers.load().get(task_queue) {
            if let Some(w) = stored_worker {
                w.shutdown().await;
            }
            self.workers.rcu(|map| {
                let mut map = HashMap::clone(map);
                if maybe_worker.is_none() {
                    maybe_worker = map.get_mut(task_queue).and_then(Option::take);
                }
                map
            });
        }
        if let Some(w) = maybe_worker {
            w.destroy().await;
        }
    }

    pub async fn shutdown_all(&self) {
        // First notify all workers and allow tasks to drain
        join_all(self.workers.load().values().map(|w| async move {
            if let Some(w) = w {
                w.shutdown().await;
            }
        }))
        .await;

        let mut all_workers = vec![];
        self.workers.rcu(|map| {
            let mut map = HashMap::clone(map);
            for worker in map.values_mut() {
                all_workers.push(worker.take());
            }
            map
        });
        join_all(all_workers.into_iter().map(|w| async move {
            if let Some(w) = w {
                w.destroy().await;
            }
        }))
        .await;
    }
}

/// Fun little struct that allows us to efficiently `await` for outstanding references to workers
/// to reach 0 before we consume it forever.
#[derive(Clone)]
struct WorkerRefCt {
    inner: Option<Arc<Worker>>,
    notify: Arc<Notify>,
}

impl WorkerRefCt {
    fn new(worker: Worker) -> Self {
        Self {
            inner: Some(Arc::new(worker)),
            notify: Arc::new(Notify::new()),
        }
    }

    async fn destroy(mut self) {
        let mut arc = self.inner.take().unwrap();
        loop {
            self.notify.notified().await;
            match Arc::try_unwrap(arc) {
                Ok(w) => {
                    w.finalize_shutdown().await;
                    return;
                }
                Err(a) => {
                    arc = a;
                    continue;
                }
            }
        }
    }
}

impl Deref for WorkerRefCt {
    type Target = Worker;

    fn deref(&self) -> &Self::Target {
        self.inner.as_deref().expect("Must exist")
    }
}

impl Drop for WorkerRefCt {
    fn drop(&mut self) {
        match &self.inner {
            // Notify once destroy has been requested
            None => self.notify.notify_one(),
            Some(arc) => {
                // We wait until 2 rather than 1 because we ourselves still have an Arc
                if Arc::strong_count(arc) == 2 {
                    self.notify.notify_one();
                }
            }
        };
    }
}
