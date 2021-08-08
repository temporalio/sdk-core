use crate::{worker::Worker, ServerGatewayApis, WorkerConfig, WorkerRegistrationError};
use arc_swap::ArcSwap;
use std::{collections::HashMap, sync::Arc};

/// Allows access to workers by task queue name
#[derive(Default)]
pub struct WorkerDispatcher {
    /// Maps task queue names to workers
    workers: ArcSwap<HashMap<String, Arc<Worker>>>,
}

impl WorkerDispatcher {
    pub async fn store_worker(
        &self,
        config: WorkerConfig,
        sticky_queue: Option<String>,
        gateway: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
    ) -> Result<(), WorkerRegistrationError> {
        if let Some(_) = self.workers.load().get(&config.task_queue) {
            return Err(WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(
                config.task_queue,
            ));
        }
        let tq = &config.task_queue.clone();
        let worker = Arc::new(Worker::new(config, sticky_queue, gateway));
        self.workers.rcu(move |map| {
            let mut map = HashMap::clone(map);
            map.insert(tq.clone(), worker.clone());
            map
        });
        Ok(())
    }

    #[cfg(test)]
    pub fn store_worker_mut(&mut self, tq: String, worker: Worker) {
        // TODO: This is silly now
        let tq = &tq.clone();
        let worker = Arc::new(worker);
        self.workers.rcu(|map| {
            let mut map = HashMap::clone(map);
            map.insert(tq.clone(), worker.clone());
            map
        });
    }

    pub fn get(&self, task_queue: &str) -> Option<Arc<Worker>> {
        self.workers.load().get(task_queue).cloned()
    }

    pub async fn shutdown_one(&self, task_queue: &str) {
        info!("Shutting down worker on queue {}", task_queue);
        if let Some(w) = self.workers.load().get(task_queue) {
            w.notify_shutdown();
        }
        // TODO: Figure out how to wait on no outstanding arcs and consume, also allows us to
        //   make sure all outstanding tasks are completed. Probably spawn off a task.
    }

    pub async fn shutdown_all(&self) {
        // todo: Do in one pass
        for (tq, _) in self.workers.load().iter() {
            self.shutdown_one(tq).await;
        }
    }
}
