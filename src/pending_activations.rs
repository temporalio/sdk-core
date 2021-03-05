use crate::protosext::fmt_task_token;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::fmt::{Display, Formatter};

/// Tracks pending activations using an internal queue, while also allowing fast lookup of any
/// pending activations by run ID
#[derive(Default)]
pub struct PendingActivations {
    queue: SegQueue<PendingActivation>,
    count_by_id: DashMap<String, usize>,
}

impl PendingActivations {
    pub fn push(&self, v: PendingActivation) {
        *self
            .count_by_id
            .entry(v.run_id.clone())
            .or_insert_with(|| 0)
            .value_mut() += 1;
        self.queue.push(v);
    }

    pub fn pop(&self) -> Option<PendingActivation> {
        let rme = self.queue.pop();
        if let Some(pa) = &rme {
            if let Some(mut c) = self.count_by_id.get_mut(&pa.run_id) {
                *c.value_mut() -= 1
            }
            self.count_by_id.remove_if(&pa.run_id, |_, v| v <= &0);
        }
        rme
    }

    pub fn has_pending(&self, run_id: &str) -> bool {
        self.count_by_id.contains_key(run_id)
    }
}

#[derive(Debug)]
pub struct PendingActivation {
    pub run_id: String,
    pub task_token: Vec<u8>,
}

impl Display for PendingActivation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PendingActivation(run_id: {}, task_token: {})",
            &self.run_id,
            fmt_task_token(&self.task_token)
        )
    }
}
