//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use anyhow::bail;
use parking_lot::RwLock;
use std::collections::{
    HashMap,
    hash_map::Entry::{Occupied, Vacant},
};
use std::sync::Arc;
use temporalio_common::protos::temporal::api::worker::v1::WorkerHeartbeat;
use temporalio_common::protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use uuid::Uuid;

/// This trait represents a slot reserved for processing a WFT by a worker.
#[cfg_attr(test, mockall::automock)]
pub trait Slot {
    /// Consumes this slot by dispatching a WFT to its worker. This can only be called once.
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error>;
}

/// Result of reserving a workflow task slot, including deployment options if applicable.
pub(crate) struct SlotReservation {
    /// The reserved slot for processing the workflow task
    pub slot: Box<dyn Slot + Send>,
    /// Worker deployment options, if the worker is using deployment-based versioning
    pub deployment_options: Option<temporal_sdk_core_api::worker::WorkerDeploymentOptions>,
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
struct ClientWorkerSetImpl {
    /// Maps slot keys to slot provider worker.
    slot_providers: HashMap<SlotKey, Uuid>,
    /// Maps worker_instance_key to registered workers
    all_workers: HashMap<Uuid, Arc<dyn ClientWorker + Send + Sync>>,
    /// Maps namespace to shared worker for worker heartbeating
    shared_worker: HashMap<String, Box<dyn SharedNamespaceWorkerTrait + Send + Sync>>,
}

impl ClientWorkerSetImpl {
    /// Factory method.
    fn new() -> Self {
        Self {
            slot_providers: Default::default(),
            all_workers: Default::default(),
            shared_worker: Default::default(),
        }
    }

    fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<SlotReservation> {
        let key = SlotKey::new(namespace, task_queue);
        if let Some(p) = self.slot_providers.get(&key)
            && let Some(worker) = self.all_workers.get(p)
            && let Some(slot) = worker.try_reserve_wft_slot()
        {
            let deployment_options = worker.deployment_options();
            return Some(SlotReservation {
                slot,
                deployment_options,
            });
        }
        None
    }

    fn register(
        &mut self,
        worker: Arc<dyn ClientWorker + Send + Sync>,
        skip_client_worker_set_check: bool,
    ) -> Result<(), anyhow::Error> {
        let slot_key = SlotKey::new(
            worker.namespace().to_string(),
            worker.task_queue().to_string(),
        );
        if self.slot_providers.contains_key(&slot_key) && !skip_client_worker_set_check {
            bail!(
                "Registration of multiple workers on the same namespace and task queue for the same client not allowed: {slot_key:?}, worker_instance_key: {:?}.",
                worker.worker_instance_key()
            );
        }

        if worker.heartbeat_enabled()
            && let Some(heartbeat_callback) = worker.heartbeat_callback()
        {
            let worker_instance_key = worker.worker_instance_key();
            let namespace = worker.namespace().to_string();

            let shared_worker = match self.shared_worker.entry(namespace.clone()) {
                Occupied(o) => o.into_mut(),
                Vacant(v) => {
                    let shared_worker = worker.new_shared_namespace_worker()?;
                    v.insert(shared_worker)
                }
            };
            shared_worker.register_callback(worker_instance_key, heartbeat_callback);
        }

        self.slot_providers
            .insert(slot_key.clone(), worker.worker_instance_key());

        self.all_workers
            .insert(worker.worker_instance_key(), worker);

        Ok(())
    }

    fn unregister(
        &mut self,
        worker_instance_key: Uuid,
    ) -> Result<Arc<dyn ClientWorker + Send + Sync>, anyhow::Error> {
        let worker = self
            .all_workers
            .remove(&worker_instance_key)
            .ok_or_else(|| {
                anyhow::anyhow!("Worker with worker_instance_key {worker_instance_key} not found")
            })?;

        let slot_key = SlotKey::new(
            worker.namespace().to_string(),
            worker.task_queue().to_string(),
        );

        self.slot_providers.remove(&slot_key);

        if let Some(w) = self.shared_worker.get_mut(worker.namespace()) {
            let (callback, is_empty) = w.unregister_callback(worker.worker_instance_key());
            if callback.is_some() && is_empty {
                self.shared_worker.remove(worker.namespace());
            }
        }

        Ok(worker)
    }

    #[cfg(test)]
    fn num_providers(&self) -> usize {
        self.slot_providers.len()
    }

    #[cfg(test)]
    fn num_heartbeat_workers(&self) -> usize {
        self.shared_worker.values().map(|v| v.num_workers()).sum()
    }
}

/// This trait represents a shared namespace worker that sends worker heartbeats and
/// receives worker commands.
pub trait SharedNamespaceWorkerTrait {
    /// Namespace that the shared namespace worker is connected to.
    fn namespace(&self) -> String;

    /// Registers a heartbeat callback.
    fn register_callback(&self, worker_instance_key: Uuid, heartbeat_callback: HeartbeatCallback);

    /// Unregisters a heartbeat callback. Returns the callback removed, as well as a bool that
    /// indicates if there are no remaining callbacks in the SharedNamespaceWorker, indicating
    /// the shared worker itself can be shut down.
    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<HeartbeatCallback>, bool);

    /// Returns the number of workers registered to this shared worker.
    fn num_workers(&self) -> usize;
}

/// Enables local workers to make themselves visible to a shared client instance.
///
/// For slot managing, there can only be one worker registered per
/// namespace+queue_name+client, others will return an error.
/// It also provides a convenient method to find compatible slots within the collection.
pub struct ClientWorkerSet {
    worker_grouping_key: Uuid,
    worker_manager: RwLock<ClientWorkerSetImpl>,
}

impl Default for ClientWorkerSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientWorkerSet {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            worker_grouping_key: Uuid::new_v4(),
            worker_manager: RwLock::new(ClientWorkerSetImpl::new()),
        }
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    /// Returns the slot and the worker's deployment options (if using deployment-based versioning).
    pub(crate) fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<SlotReservation> {
        self.worker_manager
            .read()
            .try_reserve_wft_slot(namespace, task_queue)
    }

    /// Register a local worker that can provide WFT processing slots and potentially worker heartbeating.
    pub fn register_worker(
        &self,
        worker: Arc<dyn ClientWorker + Send + Sync>,
        skip_client_worker_set_check: bool,
    ) -> Result<(), anyhow::Error> {
        self.worker_manager
            .write()
            .register(worker, skip_client_worker_set_check)
    }

    /// Unregisters a local worker, typically when that worker starts shutdown.
    pub fn unregister_worker(
        &self,
        worker_instance_key: Uuid,
    ) -> Result<Arc<dyn ClientWorker + Send + Sync>, anyhow::Error> {
        self.worker_manager.write().unregister(worker_instance_key)
    }

    /// Returns the worker grouping key, which is unique for each worker.
    pub fn worker_grouping_key(&self) -> Uuid {
        self.worker_grouping_key
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    /// There is only one provider per bucket so `num_providers` should be equal to `num_buckets`.
    pub fn num_providers(&self) -> usize {
        self.worker_manager.read().num_providers()
    }

    #[cfg(test)]
    /// Returns the total number of heartbeat workers registered across all namespaces.
    pub fn num_heartbeat_workers(&self) -> usize {
        self.worker_manager.read().num_heartbeat_workers()
    }
}

impl std::fmt::Debug for ClientWorkerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientWorkerSet")
            .field("worker_grouping_key", &self.worker_grouping_key)
            .finish()
    }
}

/// Contains a worker heartbeat callback, wrapped for mocking
pub type HeartbeatCallback = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;

/// Represents a complete worker that can handle both slot management
/// and worker heartbeat functionality.
#[cfg_attr(test, mockall::automock)]
pub trait ClientWorker: Send + Sync {
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

    /// Get the worker deployment options for this worker, if using deployment-based versioning.
    fn deployment_options(&self) -> Option<temporal_sdk_core_api::worker::WorkerDeploymentOptions>;

    /// Unique identifier for this worker instance.
    /// This must be stable across the worker's lifetime and unique per instance.
    fn worker_instance_key(&self) -> Uuid;

    /// Indicates if worker heartbeating is enabled for this client worker.
    fn heartbeat_enabled(&self) -> bool;

    /// Returns the heartbeat callback that can be used to get WorkerHeartbeat data.
    fn heartbeat_callback(&self) -> Option<HeartbeatCallback>;

    /// Creates a new worker that implements the [SharedNamespaceWorkerTrait]
    fn new_shared_namespace_worker(
        &self,
    ) -> Result<Box<dyn SharedNamespaceWorkerTrait + Send + Sync>, anyhow::Error>;
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
        mock_provider.expect_deployment_options().return_const(None);
        mock_provider
            .expect_heartbeat_enabled()
            .return_const(heartbeat_enabled);
        mock_provider
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        mock_provider
    }

    #[test]
    fn registry_keeps_one_provider_per_namespace() {
        let manager = ClientWorkerSet::new();
        let mut worker_keys = vec![];
        let mut successful_registrations = 0;

        for i in 0..10 {
            let namespace = format!("myId{}", i % 3);
            let mock_provider =
                new_mock_provider(namespace, "bar_q".to_string(), false, false, false);
            let worker_instance_key = mock_provider.worker_instance_key();

            let result = manager.register_worker(Arc::new(mock_provider), false);
            if result.is_ok() {
                successful_registrations += 1;
                worker_keys.push(worker_instance_key);
            } else {
                // Should get error for duplicate namespace+task_queue combinations
                assert!(result.unwrap_err().to_string().contains(
                    "Registration of multiple workers on the same namespace and task queue"
                ));
            }
        }

        assert_eq!(successful_registrations, 3);
        assert_eq!(3, manager.num_providers());

        let count = worker_keys.iter().fold(0, |count, key| {
            manager.unregister_worker(*key).unwrap();
            // expect error since worker is already unregistered
            let result = manager.unregister_worker(*key);
            assert!(result.is_err());
            count + 1
        });
        assert_eq!(3, count);
        assert_eq!(0, manager.num_providers());
    }

    struct MockSharedNamespaceWorker {
        namespace: String,
        callbacks: Arc<RwLock<HashMap<Uuid, HeartbeatCallback>>>,
    }

    impl std::fmt::Debug for MockSharedNamespaceWorker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockSharedNamespaceWorker")
                .field("namespace", &self.namespace)
                .field("callbacks_count", &self.callbacks.read().len())
                .finish()
        }
    }

    impl MockSharedNamespaceWorker {
        fn new(namespace: String) -> Self {
            Self {
                namespace,
                callbacks: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    impl SharedNamespaceWorkerTrait for MockSharedNamespaceWorker {
        fn namespace(&self) -> String {
            self.namespace.clone()
        }

        fn register_callback(
            &self,
            worker_instance_key: Uuid,
            heartbeat_callback: HeartbeatCallback,
        ) {
            self.callbacks
                .write()
                .insert(worker_instance_key, heartbeat_callback);
        }

        fn unregister_callback(
            &self,
            worker_instance_key: Uuid,
        ) -> (Option<HeartbeatCallback>, bool) {
            let mut callbacks = self.callbacks.write();
            let callback = callbacks.remove(&worker_instance_key);
            let is_empty = callbacks.is_empty();
            (callback, is_empty)
        }

        fn num_workers(&self) -> usize {
            self.callbacks.read().len()
        }
    }

    fn new_mock_provider_with_heartbeat(
        namespace: String,
        task_queue: String,
        heartbeat_enabled: bool,
        worker_instance_key: Uuid,
    ) -> MockClientWorker {
        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_try_reserve_wft_slot()
            .returning(|| Some(new_mock_slot(false)));
        mock_provider
            .expect_namespace()
            .return_const(namespace.clone());
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider
            .expect_heartbeat_enabled()
            .return_const(heartbeat_enabled);
        mock_provider
            .expect_worker_instance_key()
            .return_const(worker_instance_key);
        mock_provider.expect_deployment_options().return_const(None);

        if heartbeat_enabled {
            mock_provider
                .expect_heartbeat_callback()
                .returning(|| Some(Arc::new(WorkerHeartbeat::default)));

            let namespace_clone = namespace.clone();
            mock_provider
                .expect_new_shared_namespace_worker()
                .returning(move || {
                    Ok(Box::new(MockSharedNamespaceWorker::new(
                        namespace_clone.clone(),
                    )))
                });
        }

        mock_provider
    }

    #[test]
    fn duplicate_namespace_task_queue_registration_fails() {
        let manager = ClientWorkerSet::new();

        let worker1 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "test_queue".to_string(),
            true,
            Uuid::new_v4(),
        );

        // Same namespace+task_queue but different worker instance
        let worker2 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "test_queue".to_string(),
            true,
            Uuid::new_v4(),
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();

        // second worker register should fail due to duplicate namespace+task_queue
        let result = manager.register_worker(Arc::new(worker2), false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Registration of multiple workers on the same namespace and task queue")
        );

        assert_eq!(1, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 1);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
    }

    #[test]
    fn multiple_workers_same_namespace_share_heartbeat_manager() {
        let manager = ClientWorkerSet::new();

        let worker1 = new_mock_provider_with_heartbeat(
            "shared_namespace".to_string(),
            "queue1".to_string(),
            true,
            Uuid::new_v4(),
        );

        // Same namespace but different task queue
        let worker2 = new_mock_provider_with_heartbeat(
            "shared_namespace".to_string(),
            "queue2".to_string(),
            true,
            Uuid::new_v4(),
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("shared_namespace"));

        let shared_worker = impl_ref.shared_worker.get("shared_namespace").unwrap();
        assert_eq!(shared_worker.namespace(), "shared_namespace");
    }

    #[test]
    fn different_namespaces_get_separate_heartbeat_managers() {
        let manager = ClientWorkerSet::new();
        let worker1 = new_mock_provider_with_heartbeat(
            "namespace1".to_string(),
            "queue1".to_string(),
            true,
            Uuid::new_v4(),
        );
        let worker2 = new_mock_provider_with_heartbeat(
            "namespace2".to_string(),
            "queue1".to_string(),
            true,
            Uuid::new_v4(),
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.num_heartbeat_workers(), 2);
        assert!(impl_ref.shared_worker.contains_key("namespace1"));
        assert!(impl_ref.shared_worker.contains_key("namespace2"));
    }

    #[test]
    fn unregister_heartbeat_workers_cleans_up_shared_worker_when_last_removed() {
        let manager = ClientWorkerSet::new();

        // Create two workers with same namespace but different task queues
        let worker1 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "queue1".to_string(),
            true,
            Uuid::new_v4(),
        );
        let worker2 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "queue2".to_string(),
            true,
            Uuid::new_v4(),
        );
        let worker_instance_key1 = worker1.worker_instance_key();
        let worker_instance_key2 = worker2.worker_instance_key();

        assert_ne!(worker_instance_key1, worker_instance_key2);

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        // Verify initial state: 2 slot providers, 2 heartbeat workers, 1 shared worker
        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
        assert_eq!(
            impl_ref
                .shared_worker
                .get("test_namespace")
                .unwrap()
                .num_workers(),
            2
        );
        drop(impl_ref);

        // Unregister first worker
        manager.unregister_worker(worker_instance_key1).unwrap();

        // After unregistering first worker: 1 slot provider, 1 heartbeat worker, shared worker still exists
        assert_eq!(1, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 1);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.num_heartbeat_workers(), 1); // SharedNamespaceWorker still exists
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
        assert_eq!(
            impl_ref
                .shared_worker
                .get("test_namespace")
                .unwrap()
                .num_workers(),
            1
        );
        drop(impl_ref);

        // Unregister second worker
        manager.unregister_worker(worker_instance_key2).unwrap();

        // After unregistering last worker: 0 slot providers, 0 heartbeat workers, shared worker is removed
        assert_eq!(0, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 0);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 0); // SharedNamespaceWorker is cleaned up
        assert!(!impl_ref.shared_worker.contains_key("test_namespace"));
    }
}
