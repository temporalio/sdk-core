use crate::{
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    workflow::{NextWfActivation, WorkflowManager},
    CoreError, Result,
};
use crossbeam::channel::{bounded, unbounded, Select, Sender, TryRecvError};
use dashmap::DashMap;
use std::{
    fmt::Debug,
    thread::{self, JoinHandle},
    time::Duration,
};

type MachineSender = Sender<Box<dyn FnOnce(&mut WorkflowManager) + Send>>;
type MachineCreatorMsg = (
    PollWorkflowTaskQueueResponse,
    Sender<MachineCreatorResponseMsg>,
);
type MachineCreatorResponseMsg = Result<(NextWfActivation, MachineSender)>;

pub(crate) struct WorkflowConcurrencyManager {
    machines: DashMap<String, MachineSender>,
    wf_thread: JoinHandle<()>,
    machine_creator: Sender<MachineCreatorMsg>,
}

impl WorkflowConcurrencyManager {
    pub fn new() -> Self {
        let (machine_creator, create_rcv) = unbounded::<MachineCreatorMsg>();

        let wf_thread = thread::spawn(move || {
            let mut machine_rcvs = vec![];
            loop {
                // If there's a message ready on the creation channel, make a new machine
                // and put it's receiver into the list, replying with the machine's activation and
                // a channel to send requests to it, or an error otherwise.
                // TODO: handle disconnected / other channel errors
                match create_rcv.try_recv() {
                    Ok((pwtqr, resp_chan)) => match WorkflowManager::new(pwtqr)
                        .and_then(|mut wfm| Ok((wfm.get_next_activation()?, wfm)))
                    {
                        Ok((activation, wfm)) => {
                            let (machine_sender, machine_rcv) = unbounded();
                            machine_rcvs.push((machine_rcv, wfm));
                            resp_chan.send(Ok((activation, machine_sender))).unwrap();
                        }
                        Err(e) => {
                            resp_chan.send(Err(e)).unwrap();
                        }
                    },
                    Err(TryRecvError::Disconnected) => {
                        dbg!("Channel disconnected!");
                        break;
                    }
                    Err(TryRecvError::Empty) => {}
                }

                // Having created any new machines, we now check if there are any pending requests
                // to interact with the machines. If multiple requests are pending they are dealt
                // with in random order.
                let mut sel = Select::new();
                for (rcv, _) in machine_rcvs.iter() {
                    sel.recv(rcv);
                }
                match sel.try_ready() {
                    Ok(index) => match machine_rcvs[index].0.try_recv() {
                        Ok(func) => {
                            dbg!("Blorgp");
                            // Recall that calling this function also sends the response
                            func(&mut machine_rcvs[index].1);
                        }
                        Err(_) => {}
                    },
                    Err(_) => {}
                }
                // TODO: remove probably
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        Self {
            machines: Default::default(),
            wf_thread,
            machine_creator,
        }
    }

    pub fn exists(&self, run_id: &str) -> bool {
        self.machines.contains_key(run_id)
    }

    pub fn create_or_update(
        &self,
        run_id: &str,
        poll_wf_resp: PollWorkflowTaskQueueResponse,
    ) -> Result<NextWfActivation> {
        if self.exists(run_id) {
            if let Some(history) = poll_wf_resp.history {
                // TODO: Sort of dumb to use entry here now
                let activation = self.access(run_id, |wfm: &mut WorkflowManager| {
                    wfm.feed_history_from_server(history)
                })?;
                Ok(activation)
            } else {
                Err(CoreError::BadDataFromWorkProvider(poll_wf_resp))
            }
        } else {
            // Creates a channel for the response to attempting to create the machine, sends
            // the task q response, and waits for the result of machine creation along with
            // the activation
            let (resp_send, resp_rcv) = bounded(1);
            self.machine_creator
                .send((poll_wf_resp, resp_send))
                .unwrap();
            let (activation, machine_sender) = resp_rcv.recv().unwrap()?;
            self.machines.insert(run_id.to_string(), machine_sender);
            Ok(activation)
        }
    }

    pub fn access<F, Fout>(&self, run_id: &str, mutator: F) -> Result<Fout>
    where
        F: FnOnce(&mut WorkflowManager) -> Result<Fout> + Send + 'static,
        Fout: Send + Debug + 'static,
    {
        let m = self
            .machines
            .get(run_id)
            .ok_or_else(|| CoreError::MissingMachines(run_id.to_string()))?;

        // This code fetches the channel for a workflow manager and sends it a modified version of
        // of closure the caller provided which includes a channel for the response, and sends
        // the result of the user-provided closure down that response channel.
        let (sender, receiver) = bounded(1);
        let f = move |x: &mut WorkflowManager| {
            let _ = sender.send(dbg!(mutator(x)));
        };
        // TODO: Clean up unwraps
        m.send(Box::new(f)).unwrap();
        receiver.recv().unwrap()
    }

    /// Attempt to join the thread where the workflow machines live.
    ///
    /// # Panics
    /// If the workflow machine thread panicked
    pub fn shutdown(self) {
        self.wf_thread
            .join()
            .expect("Workflow manager thread should shut down cleanly");
    }
}

trait BeSendSync: Send + Sync {}
impl BeSendSync for WorkflowConcurrencyManager {}

#[cfg(test)]
mod tests {
    use super::*;
}
