use crate::{worker::Worker, ServerGatewayApis, WorkerConfig, WorkerRegistrationError};
use std::{collections::HashMap, ops::Deref, sync::Arc};
use tokio::sync::{Notify, RwLock, RwLockReadGuard};

pub enum WorkerStatus {
    Live(Worker),
    Shutdown,
}

impl WorkerStatus {
    #[cfg(test)]
    pub fn unwrap(&self) -> &Worker {
        match self {
            WorkerStatus::Live(w) => w,
            WorkerStatus::Shutdown => panic!("Worker not present"),
        }
    }
}

/// Allows access to workers by task queue name
#[derive(Default)]
pub struct WorkerDispatcher {
    /// Maps task queue names to workers
    workers: RwLock<HashMap<String, WorkerStatus>>,
}

impl WorkerDispatcher {
    pub async fn store_worker(
        &self,
        config: WorkerConfig,
        sticky_queue: Option<String>,
        gateway: Arc<impl ServerGatewayApis + Send + Sync + 'static>,
    ) -> Result<(), WorkerRegistrationError> {
        if let Some(WorkerStatus::Live(_)) = self.workers.read().await.get(&config.task_queue) {
            return Err(WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(
                config.task_queue,
            ));
        }
        let tq = config.task_queue.clone();
        let worker = Worker::new(config, sticky_queue, gateway);
        self.workers
            .write()
            .await
            .insert(tq, WorkerStatus::Live(worker));
        Ok(())
    }

    #[cfg(test)]
    pub fn store_worker_mut(&mut self, tq: String, worker: Worker) {
        self.workers
            .get_mut()
            .insert(tq, WorkerStatus::Live(worker));
    }

    pub async fn get(&self, task_queue: &str) -> Option<impl Deref<Target = WorkerStatus> + '_> {
        let guard = self.workers.read().await;
        RwLockReadGuard::try_map(guard, move |map| map.get(task_queue)).ok()
    }

    pub async fn shutdown_one(&self, task_queue: &str, notify_lock_unavailable: &Notify) {
        info!("Shutting down worker on queue {}", task_queue);
        if let Some(WorkerStatus::Live(w)) = self.workers.read().await.get(task_queue) {
            w.notify_shutdown();
        }
        let mut workers = match self.workers.try_write() {
            Ok(wg) => wg,
            Err(_) => {
                notify_lock_unavailable.notify_waiters();
                self.workers.write().await
            }
        };
        if let Some(WorkerStatus::Live(w)) = workers.remove(task_queue) {
            workers.insert(task_queue.to_owned(), WorkerStatus::Shutdown);
            drop(workers);
            w.shutdown_complete().await;
        }
    }

    pub async fn shutdown_all(&self) {
        for (_, w) in self.workers.write().await.drain() {
            if let WorkerStatus::Live(w) = w {
                w.shutdown_complete().await;
            }
        }
    }
}
