//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use parking_lot::RwLock;
use slotmap::SlotMap;
use std::collections::hash_map::Entry::Occupied;
use std::collections::{HashMap, hash_map::Entry::Vacant};
use std::sync::Arc;
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerHeartbeat;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use uuid::Uuid;

slotmap::new_key_type! {
    /// Registration key for a worker
    pub struct WorkerKey;
}

/// This trait represents a slot reserved for processing a WFT by a worker.
#[cfg_attr(test, mockall::automock)]
pub trait Slot {
    /// Consumes this slot by dispatching a WFT to its worker. This can only be called once.
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error>;
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
struct SlotKey {
    namespace: String,
    task_queue: String,
}

impl SlotKey {
    fn new(namespace: String, task_queue: String) -> SlotKey {
        SlotKey {
            namespace,
            task_queue,
        }
    }
}

/// This is an inner class for [ClientWorkerSet] needed to hide the mutex.
#[derive(Default, Debug)]
struct ClientWorkerSetImpl {
    /// Maps keys, i.e., namespace#task_queue, to provider.
    slot_providers: HashMap<SlotKey, Arc<dyn ClientWorker + Send + Sync>>,
    /// Maps ids to keys in `providers`.
    slot_index: SlotMap<WorkerKey, SlotKey>,
    /// Maps keys, i.e. namespace#worker_instance_key, to heartbeat callback
    heartbeat_map: HashMap<String, Box<dyn SharedNamespaceWorkerTrait + Send + Sync>>,
}

impl ClientWorkerSetImpl {
    /// Factory method.
    fn new() -> Self {
        Self {
            slot_index: Default::default(),
            slot_providers: Default::default(),
            heartbeat_map: Default::default(),
        }
    }

    fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        let key = SlotKey::new(namespace, task_queue);
        if let Some(p) = self.slot_providers.get(&key)
            && let Some(slot) = p.try_reserve_wft_slot()
        {
            return Some(slot);
        }
        None
    }

    fn register(&mut self, worker: Arc<dyn ClientWorker + Send + Sync>) -> Option<WorkerKey> {
        let slot_key = SlotKey::new(
            worker.namespace().to_string(),
            worker.task_queue().to_string(),
        );

        let worker_key = if let Vacant(p) = self.slot_providers.entry(slot_key.clone()) {
            p.insert(worker.clone());
            self.slot_index.insert(slot_key)
        } else {
            warn!("Ignoring slot registration for worker: {slot_key:?}.");
            return None;
        };

        if worker.heartbeat_enabled()
            && let Some(heartbeat_callback) = worker.heartbeat_callback()
            && let Some(worker_instance_key) = worker.worker_instance_key()
        {
            // TODO: add logs to verify if this works? or a test
            match self.heartbeat_map.entry(worker.namespace().to_string()) {
                Vacant(p) => {
                    if let Some(shared_worker) = worker.new_shared_namespace_worker() {
                        shared_worker.register_callback(worker_instance_key, heartbeat_callback.0);
                        p.insert(shared_worker);
                    }
                }
                Occupied(p) => {
                    p.get()
                        .register_callback(worker_instance_key, heartbeat_callback.0);
                }
            }
        }
        Some(worker_key)
    }

    fn unregister(&mut self, id: WorkerKey) -> Option<Arc<dyn ClientWorker + Send + Sync>> {
        let worker = if let Some(key) = self.slot_index.remove(id) {
            self.slot_providers.remove(&key)
        } else {
            None
        };

        if let Some(w) = worker.as_ref()
            && let Some(worker_instance_key) = w.worker_instance_key()
        {
            if let Some(shared_worker) = self.heartbeat_map.get(w.namespace()) {
                let (callback, is_empty) = shared_worker.unregister_callback(worker_instance_key);
                if is_empty {
                    self.heartbeat_map.remove(w.namespace());
                }
                if let Some(cb) = callback {
                    w.register_callback(HeartbeatCallback(cb))
                }
            } else {
                warn!(
                    "Namespace {} isn't registered to client worker heartbeat, ignoring unregister.",
                    w.namespace()
                );
            }
        }
        worker
    }

    #[cfg(test)]
    fn num_providers(&self) -> (usize, usize) {
        (self.slot_index.len(), self.slot_providers.len())
    }
}

/// This trait represents a shared namespace worker that sends worker heartbeats and worker commands.
pub trait SharedNamespaceWorkerTrait: std::fmt::Debug {
    /// Namespace that the shared namespace worker is connected to.
    fn namespace(&self) -> String;

    /// Unregisters a heartbeat callback. Returns the callback removed, as well as a bool that
    /// indicates if there are no remaining callbacks in the SharedNamespaceWorker, indicating
    /// the shared worker itself can be shut down.
    fn unregister_callback(
        &self,
        worker_instance_key: String,
    ) -> (Option<Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>>, bool);

    /// Registers a heartbeat callback.
    fn register_callback(
        &self,
        worker_instance_key: String,
        heartbeat_callback: Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>,
    );
}

/// Enables local workers to make themselves visible to a shared client instance.
///
/// For slot managing, there can only be one worker registered per
/// namespace+queue_name+client, others will get ignored.
/// It also provides a convenient method to find compatible slots within the collection.
#[derive(Default, Debug)]
pub struct ClientWorkerSet {
    worker_set_key: Uuid,
    worker_manager: RwLock<ClientWorkerSetImpl>,
}

impl ClientWorkerSet {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            worker_set_key: Uuid::new_v4(),
            worker_manager: RwLock::new(ClientWorkerSetImpl::new()),
        }
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    pub(crate) fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        self.worker_manager
            .read()
            .try_reserve_wft_slot(namespace, task_queue)
    }

    /// Unregisters a local worker, typically when its worker starts shutdown.
    pub fn unregister_worker(&self, id: WorkerKey) -> Option<Arc<dyn ClientWorker + Send + Sync>> {
        self.worker_manager.write().unregister(id)
    }

    /// Register a local worker that can provide WFT processing slots and potentially worker heartbeating.
    pub fn register_worker(
        &self,
        worker: Arc<dyn ClientWorker + Send + Sync>,
    ) -> Option<WorkerKey> {
        self.worker_manager.write().register(worker)
    }

    /// Returns the worker set key, which is unique for each client. Used for worker heartbeating
    pub fn worker_set_key(&self) -> Uuid {
        self.worker_set_key
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    /// There is only one provider per bucket so `num_providers` should be equal to `num_buckets`.
    pub fn num_providers(&self) -> (usize, usize) {
        self.worker_manager.read().num_providers()
    }
}

/// Contains a worker heartbeat callback, wrapped for mocking
pub struct HeartbeatCallback(pub Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>);

/// Represents a complete worker that can handle both slot management
/// and worker heartbeat functionality.
#[cfg_attr(test, mockall::automock)]
pub trait ClientWorker: Send + Sync + std::fmt::Debug {
    /// The namespace this worker operates in
    fn namespace(&self) -> &str;

    /// The task queue this worker listens to
    fn task_queue(&self) -> &str;

    /// Try to reserve a slot for workflow task processing.
    ///
    /// This method should return `Some(slot)` if a workflow task slot is available,
    /// or `None` if all slots are currently in use. The returned slot will be used
    /// to process exactly one workflow task.
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn Slot + Send>>;

    /// Unique identifier for this worker instance.
    /// This should be stable across the worker's lifetime but unique per instance.
    fn worker_instance_key(&self) -> Option<String>;

    /// Indicates if worker heartbeating is enabled for this client worker.
    fn heartbeat_enabled(&self) -> bool;

    /// Returns the heartbeat callback that can be used to get WorkerHeartbeat data.
    fn heartbeat_callback(&self) -> Option<HeartbeatCallback>;

    /// Creates a new worker that implements the [SharedNamespaceWorkerTrait]
    fn new_shared_namespace_worker(
        &self,
    ) -> Option<Box<dyn SharedNamespaceWorkerTrait + Send + Sync>>;

    /// Registers a worker heartbeat callback, typically when a worker is unregistered from a client
    fn register_callback(&self, callback: HeartbeatCallback);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_mock_slot(with_error: bool) -> Box<MockSlot> {
        let mut mock_slot = MockSlot::new();
        if with_error {
            mock_slot
                .expect_schedule_wft()
                .returning(|_| Err(anyhow::anyhow!("Changed my mind")));
        } else {
            mock_slot.expect_schedule_wft().returning(|_| Ok(()));
        }
        Box::new(mock_slot)
    }

    fn new_mock_provider(
        namespace: String,
        task_queue: String,
        with_error: bool,
        no_slots: bool,
        heartbeat_enabled: bool,
    ) -> MockClientWorker {
        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_try_reserve_wft_slot()
            .returning(move || {
                if no_slots {
                    None
                } else {
                    Some(new_mock_slot(with_error))
                }
            });
        mock_provider.expect_namespace().return_const(namespace);
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider
            .expect_heartbeat_enabled()
            .return_const(heartbeat_enabled);
        mock_provider
            .expect_worker_instance_key()
            .return_const(Some(Uuid::new_v4().to_string()));
        mock_provider
    }

    #[test]
    fn registry_respects_registration_order() {
        let mock_provider1 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, false, false);
        let mock_provider2 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, true, false);

        let manager = ClientWorkerSet::new();
        let some_slots = manager.register_worker(Arc::new(mock_provider1));
        let no_slots = manager.register_worker(Arc::new(mock_provider2));
        assert!(no_slots.is_none());

        let mut found = 0;
        for _ in 0..10 {
            if manager
                .try_reserve_wft_slot("foo".to_string(), "bar_q".to_string())
                .is_some()
            {
                found += 1;
            }
        }
        assert_eq!(found, 10);
        assert_eq!((1, 1), manager.num_providers());

        manager.unregister_worker(some_slots.unwrap());
        assert_eq!((0, 0), manager.num_providers());

        let mock_provider1 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, false, false);
        let mock_provider2 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, true, false);

        let no_slots = manager.register_worker(Arc::new(mock_provider2));
        let some_slots = manager.register_worker(Arc::new(mock_provider1));
        assert!(some_slots.is_none());

        let mut not_found = 0;
        for _ in 0..10 {
            if manager
                .try_reserve_wft_slot("foo".to_string(), "bar_q".to_string())
                .is_none()
            {
                not_found += 1;
            }
        }
        assert_eq!(not_found, 10);
        assert_eq!((1, 1), manager.num_providers());
        manager.unregister_worker(no_slots.unwrap());
        assert_eq!((0, 0), manager.num_providers());
    }

    #[test]
    fn registry_keeps_one_provider_per_namespace() {
        let manager = ClientWorkerSet::new();
        let mut worker_keys = vec![];
        for i in 0..10 {
            let namespace = format!("myId{}", i % 3);
            let mock_provider =
                new_mock_provider(namespace, "bar_q".to_string(), false, false, false);
            worker_keys.push(manager.register_worker(Arc::new(mock_provider)));
        }
        assert_eq!((3, 3), manager.num_providers());

        let count = worker_keys
            .iter()
            .filter(|key| key.is_some())
            .fold(0, |count, key| {
                manager.unregister_worker(key.unwrap());
                // Should be idempotent
                manager.unregister_worker(key.unwrap());
                count + 1
            });
        assert_eq!(3, count);
        assert_eq!((0, 0), manager.num_providers());
    }
}
