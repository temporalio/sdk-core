//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use rand::{seq::SliceRandom, thread_rng};
use std::{collections::HashMap, fmt::Debug};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

/// This trait wraps a worker, which provides WFT processing slots.
#[cfg_attr(test, mockall::automock)]
pub trait SlotProvider: Send + Sync + Debug {
    /// A unique identifier for the worker.
    fn uuid(&self) -> String;
    /// The namespace for the WFTs that it can process.
    fn namespace(&self) -> String;
    /// The task queue this provider listens to.
    fn task_queue(&self) -> String;
    /// Try to reserve a slot with matching namespace and task queue.
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn Slot>>;
}

/// This trait represents a slot reserved for processing a WFT by a worker.
#[cfg_attr(test, mockall::automock)]
pub trait Slot: Send + Sync {
    /// Consumes this slot by dispatching a WFT to its worker. This can only be called once.
    fn schedule_wft(&mut self, task: PollWorkflowTaskQueueResponse) -> Result<(), anyhow::Error>;
}

/// This trait enables local workers to made themselves visible to a shared client instance.
pub trait WorkerRegistry: Send + Sync {
    /// Register a local worker that can provide WFT processing slots.
    fn register(&mut self, provider: Box<dyn SlotProvider>);
    /// Unregister a provider, typically when its worker starts shutdown.
    fn unregister(&mut self, uuid: String);
}

/// Implements a [WorkerRegistry] and provides a convenient method
/// to find compatible slots within the collection.
#[derive(Default, Debug)]
pub struct SlotManager {
    /// Maps keys, i.e., namespace#task_queue, to providers.
    providers: HashMap<String, Vec<Box<dyn SlotProvider>>>,
    /// Maps uuids to keys in `providers`.
    index: HashMap<String, String>,
}

fn to_key(namespace: &str, task_queue: &str) -> String {
    format!("{}#{}", namespace, task_queue)
}

fn random_permutation(num_entries: usize) -> Vec<usize> {
    let mut vec: Vec<usize> = (0..num_entries).collect();
    vec.shuffle(&mut thread_rng());
    vec
}

impl SlotManager {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            providers: HashMap::new(),
        }
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    pub fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot>> {
        let key = to_key(&namespace, &task_queue);
        if let Some(all) = self.providers.get(&key) {
            let index = random_permutation(all.len());
            for i in &index {
                let slot = all[*i].try_reserve_wft_slot();
                if let Some(s) = slot {
                    return Some(s);
                }
            }
        }
        None
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    pub fn num_providers(&self) -> (usize, usize) {
        (self.index.len(), self.providers.len())
    }
}

impl WorkerRegistry for SlotManager {
    fn register(&mut self, provider: Box<dyn SlotProvider>) {
        let uuid = provider.uuid();
        if self.index.get(&uuid).is_none() {
            let key = to_key(&provider.namespace(), &provider.task_queue());
            self.index.insert(uuid, key.clone());
            let all = self.providers.entry(key).or_default();
            all.push(provider);
        }
    }

    fn unregister(&mut self, uuid: String) {
        if self.index.contains_key(&uuid) {
            if let Some(key) = self.index.remove(&uuid) {
                if let Some(all) = self.providers.get_mut(&key) {
                    all.retain(|x| *x.uuid() != uuid);
                    if all.is_empty() {
                        self.providers.remove(&key);
                    }
                }
            }
        }
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
        uuid: String,
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
        mock_provider.expect_uuid().return_const(uuid);
        mock_provider.expect_namespace().return_const(namespace);
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider
    }

    fn check_mock_providers(manager: &SlotManager) -> (bool, bool) {
        let mut saw_ok = false;
        let mut saw_err = false;
        for _ in 0..100 {
            if let Some(mut slot) =
                manager.try_reserve_wft_slot("foo".to_string(), "bar_q".to_string())
            {
                match slot.schedule_wft(PollWorkflowTaskQueueResponse::default()) {
                    Ok(_) => saw_ok = true,
                    Err(_) => saw_err = true,
                }
            }
        }
        (saw_ok, saw_err)
    }

    #[test]
    fn registry_randomizes_providers_order() {
        let mock_provider1 = new_mock_provider(
            "no_error_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            false,
            false,
        );
        let mock_provider2 = new_mock_provider(
            "with_error_id".to_string(),
            "foo".to_string(),
            "bar_q".to_string(),
            true,
            false,
        );

        let mut manager = SlotManager::new();
        manager.register(Box::new(mock_provider1));
        manager.register(Box::new(mock_provider2));
        let (saw_ok, saw_err) = check_mock_providers(&manager);
        assert!(saw_ok && saw_err);

        manager.unregister("with_error_id".to_string());
        let (saw_ok, saw_err) = check_mock_providers(&manager);
        assert!(saw_ok && !saw_err);

        manager.unregister("no_error_id".to_string());
        let (saw_ok, saw_err) = check_mock_providers(&manager);
        assert!(!saw_ok && !saw_err);
    }

    #[test]
    fn registry_eventually_finds_provider() {
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

        let mut manager = SlotManager::new();
        manager.register(Box::new(mock_provider1));
        manager.register(Box::new(mock_provider2));

        let mut found = 0;
        for _ in 0..100 {
            if let Some(_) = manager.try_reserve_wft_slot("foo".to_string(), "bar_q".to_string()) {
                found = found + 1;
            }
        }
        assert_eq!(found, 100);

        manager.unregister("some_slots_id".to_string());
        let mut not_found = 0;
        for _ in 0..100 {
            if let None = manager.try_reserve_wft_slot("foo".to_string(), "bar_q".to_string()) {
                not_found = not_found + 1;
            }
        }
        assert_eq!(not_found, 100);
    }

    #[test]
    fn registry_drops_providers() {
        let mut manager = SlotManager::new();
        for i in 0..100 {
            let uuid = format!("myId{}", i);
            let namespace = format!("myId{}", i % 3);
            let mock_provider =
                new_mock_provider(uuid, namespace, "bar_q".to_string(), false, false);
            manager.register(Box::new(mock_provider));
        }
        assert_eq!((100, 3), manager.num_providers());

        for i in 0..100 {
            let uuid = format!("myId{}", i);
            manager.unregister(uuid);
        }
        assert_eq!((0, 0), manager.num_providers());
    }

    #[test]
    fn registry_is_idempotent() {
        let mut manager = SlotManager::new();
        for _ in 0..100 {
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

        for _ in 0..100 {
            manager.unregister("same_id".to_string());
        }
        assert_eq!((0, 0), manager.num_providers());
    }
}
