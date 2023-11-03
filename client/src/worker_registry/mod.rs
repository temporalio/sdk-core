//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use parking_lot::RwLock;
use std::collections::{hash_map::Entry::Vacant, HashMap};

use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

/// This trait is implemented by an object associated with a worker, which provides WFT processing slots.
#[cfg_attr(test, mockall::automock)]
pub trait SlotProvider: std::fmt::Debug {
    /// A unique identifier for the worker.
    fn id(&self) -> &str;
    /// The namespace for the WFTs that it can process.
    fn namespace(&self) -> &str;
    /// The task queue this provider listens to.
    fn task_queue(&self) -> &str;
    /// Try to reserve a slot on this worker.
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn Slot + Send>>;
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

/// This trait enables local workers to made themselves visible to a shared client instance.
/// There can only be one worker registered per namespace+queue_name+client, others will get ignored.
#[cfg_attr(test, mockall::automock)]
pub trait WorkerRegistry {
    /// Register a local worker that can provide WFT processing slots.
    fn register(&self, provider: Box<dyn SlotProvider + Send + Sync>);
    /// Unregister a provider, typically when its worker starts shutdown.
    fn unregister(&self, id: &str);
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

/// Implements a [WorkerRegistry] and provides a convenient method
/// to find compatible slots within the collection.
/// This is an inner class to hide the mutex.
#[derive(Default, Debug)]
struct SlotManagerImpl {
    /// Maps keys, i.e., namespace#task_queue, to provider.
    providers: HashMap<SlotKey, Box<dyn SlotProvider + Send + Sync>>,
    /// Maps ids to keys in `providers`.
    index: HashMap<String, SlotKey>,
}

impl SlotManagerImpl {
    /// Factory method.
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            providers: HashMap::new(),
        }
    }

    fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        let key = SlotKey::new(namespace, task_queue);
        if let Some(p) = self.providers.get(&key) {
            if let Some(slot) = p.try_reserve_wft_slot() {
                return Some(slot);
            }
        }
        None
    }

    fn register(&mut self, provider: Box<dyn SlotProvider + Send + Sync>) {
        let id = provider.id();
        if let Vacant(e) = self.index.entry(id.to_string()) {
            let key = SlotKey::new(
                provider.namespace().to_string(),
                provider.task_queue().to_string(),
            );
            if let Vacant(p) = self.providers.entry(key.clone()) {
                p.insert(provider);
                e.insert(key);
            } else {
                warn!("Ignoring registration for worker {id} in bucket {key:?}.");
            }
        }
    }

    fn unregister(&mut self, id: &str) {
        if let Some(key) = self.index.remove(id) {
            self.providers.remove(&key);
        }
    }

    #[cfg(test)]
    fn num_providers(&self) -> (usize, usize) {
        (self.index.len(), self.providers.len())
    }
}

/// Implements a [WorkerRegistry] and provides a convenient method
/// to find compatible slots within the collection.
#[derive(Default, Debug)]
pub struct SlotManager {
    manager: RwLock<SlotManagerImpl>,
}

impl SlotManager {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            manager: RwLock::new(SlotManagerImpl::new()),
        }
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    pub fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        self.manager
            .read()
            .try_reserve_wft_slot(namespace, task_queue)
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    /// There is only one provider per bucket so `num_providers` should be equal to `num_buckets`.
    pub fn num_providers(&self) -> (usize, usize) {
        self.manager.read().num_providers()
    }
}

impl WorkerRegistry for SlotManager {
    fn register(&self, provider: Box<dyn SlotProvider + Send + Sync>) {
        self.manager.write().register(provider)
    }

    fn unregister(&self, id: &str) {
        self.manager.write().unregister(id)
    }
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
        id: String,
        namespace: String,
        task_queue: String,
        with_error: bool,
        no_slots: bool,
    ) -> MockSlotProvider {
        let mut mock_provider = MockSlotProvider::new();
        mock_provider
            .expect_try_reserve_wft_slot()
            .returning(move || {
                if no_slots {
                    None
                } else {
                    Some(new_mock_slot(with_error))
                }
            });
        mock_provider.expect_id().return_const(id);
        mock_provider.expect_namespace().return_const(namespace);
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider
    }

    #[test]
    fn registry_respects_registration_order() {
        let mock_provider1 = new_mock_provider(
            "some_slots_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            false,
            false,
        );
        let mock_provider2 = new_mock_provider(
            "no_slots_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            false,
            true,
        );

        let manager = SlotManager::new();
        manager.register(Box::new(mock_provider1));
        manager.register(Box::new(mock_provider2));

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

        manager.unregister("some_slots_id");
        assert_eq!((0, 0), manager.num_providers());

        let mock_provider1 = new_mock_provider(
            "some_slots_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            false,
            false,
        );
        let mock_provider2 = new_mock_provider(
            "no_slots_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            false,
            true,
        );

        manager.register(Box::new(mock_provider2));
        manager.register(Box::new(mock_provider1));

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
    }

    #[test]
    fn registry_keeps_one_provider_per_namespace() {
        let manager = SlotManager::new();
        for i in 0..10 {
            let id = format!("myId{}", i);
            let namespace = format!("myId{}", i % 3);
            let mock_provider = new_mock_provider(id, namespace, "bar_q".to_string(), false, false);
            manager.register(Box::new(mock_provider));
        }
        assert_eq!((3, 3), manager.num_providers());

        for i in 0..10 {
            let id = format!("myId{}", i);
            manager.unregister(&id);
        }
        assert_eq!((0, 0), manager.num_providers());
    }

    #[test]
    fn registry_is_idempotent() {
        let manager = SlotManager::new();
        for _ in 0..10 {
            let mock_provider = new_mock_provider(
                "same_id".to_string(),
                "ns".to_string(),
                "bar_q".to_string(),
                false,
                false,
            );
            manager.register(Box::new(mock_provider));
        }
        assert_eq!((1, 1), manager.num_providers());

        for _ in 0..10 {
            manager.unregister("same_id");
        }
        assert_eq!((0, 0), manager.num_providers());
    }
}
