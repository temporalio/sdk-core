use crate::{
    machines::ProtoCommand,
    workflow::{NextWfActivation, WfManagerProtected, WorkflowManager},
    CoreError, Result,
};
use crossbeam::channel::Sender;
use dashmap::DashMap;

type MachineSender = Sender<Box<dyn FnOnce(&mut WfManagerProtected) -> Result<()> + Send>>;

// TODO: If can't be totally generic, could do enum of Fn's with defined responses
/// Responses that workflow machines can respond with from their thread
enum WfMgrResponse {
    Nothing,
    Activation(NextWfActivation),
    Commands(Vec<ProtoCommand>),
}

#[derive(Default)]
pub(crate) struct WorkflowConcurrencyManager {
    machines: DashMap<String, MachineSender>,
}

impl WorkflowConcurrencyManager {
    pub fn exists(&self, run_id: &str) -> bool {
        self.machines.contains_key(run_id)
    }

    pub fn create_or_update(&self, run_id: &str) -> Result<NextWfActivation> {
        unimplemented!()
    }

    /// Access a workflow manager to do something with it. Ideally, the return type could be generic
    /// but in practice I couldn't find a way to do it. If we need more and more response types,
    /// it's worth trying again.
    pub fn access<F, Fout>(&self, run_id: &str, mutator: F) -> Result<Fout>
    where
        F: FnOnce(&mut WfManagerProtected) -> Result<Fout>,
        F: Send,
    {
        if let Some(m) = self.machines.get(run_id) {
            // m.value().send(Box::new(mutator)).unwrap();
            unimplemented!()
        } else {
            Err(CoreError::MissingMachines(run_id.to_string()))
        }
    }
}

trait BeSendSync: Send + Sync {}
impl BeSendSync for WorkflowConcurrencyManager {}
