//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use rand::{seq::SliceRandom, thread_rng};
use std::{collections::HashMap, fmt::Debug};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

/// This trait wraps a worker, which provides WFT processing slots.
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
}

impl WorkerRegistry for SlotManager {
    fn register(&mut self, provider: Box<dyn SlotProvider>) {
        let uuid = provider.uuid();
        debug!("register {}", uuid);
        if self.index.get(&uuid).is_none() {
            let key = to_key(&provider.namespace(), &provider.task_queue());
            self.index.insert(uuid, key.clone());
            let all = self.providers.entry(key).or_default();
            all.push(provider);
        }
        debug!("#entries {}", self.index.len());
        for (key, value) in &self.providers {
            debug!("{}: {}", key, value.len());
        }
    }

    fn unregister(&mut self, uuid: String) {
        debug!("unregister {}", uuid);
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
        debug!("#entries {}", self.index.len());
        for (key, value) in &self.providers {
            debug!("{}: {}", key, value.len());
        }
    }
}
