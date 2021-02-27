//! Ultimately it would be nice to make this generic and push it out into its own crate but
//! doing so is nontrivial

use crate::{
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    workflow::{NextWfActivation, WorkflowManager},
    CoreError, Result,
};
use crossbeam::channel::{bounded, unbounded, Receiver, Select, Sender, TryRecvError};
use dashmap::DashMap;
use std::{
    fmt::Debug,
    sync::Mutex,
    thread::{self, JoinHandle},
};
use tracing::Level;

/// Provides a thread-safe way to access workflow machines which live exclusively on one thread
/// managed by this struct. We could make this generic for any collection of things which need
/// to live on one thread, if desired.
pub(crate) struct WorkflowConcurrencyManager {
    // TODO: We need to remove things from here at some point, but that wasn't implemented
    //  in core SDK yet either - once we're ready to remove things, they can be removed from this
    //  map and the wfm thread will drop the machines.
    machines: DashMap<String, MachineMutationSender>,
    wf_thread: Mutex<Option<JoinHandle<()>>>,
    machine_creator: Sender<MachineCreatorMsg>,
    shutdown_chan: Sender<bool>,
}

/// The tx side of a channel which accepts closures to mutably operate on a workflow manager
type MachineMutationSender = Sender<Box<dyn FnOnce(&mut WorkflowManager) + Send>>;
type MachineMutationReceiver = Receiver<Box<dyn FnOnce(&mut WorkflowManager) + Send>>;
/// This is the message sent from the concurrency manager to the dedicated thread in order to
/// instantiate a new workflow manager
type MachineCreatorMsg = (
    PollWorkflowTaskQueueResponse,
    Sender<MachineCreatorResponseMsg>,
);
/// The response to [MachineCreatorMsg], which includes the wf activation and the channel to
/// send requests to the newly instantiated workflow manager.
type MachineCreatorResponseMsg = Result<(NextWfActivation, MachineMutationSender)>;

impl WorkflowConcurrencyManager {
    pub fn new() -> Self {
        let (machine_creator, create_rcv) = unbounded::<MachineCreatorMsg>();
        let (shutdown_chan, shutdown_rx) = bounded(1);

        let wf_thread = thread::spawn(move || {
            WorkflowConcurrencyManager::workflow_thread(create_rcv, shutdown_rx)
        });

        Self {
            machines: Default::default(),
            wf_thread: Mutex::new(Some(wf_thread)),
            machine_creator,
            shutdown_chan,
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
                .expect("wfm creation channel can't be dropped if we are inside this method");
            let (activation, machine_sender) = resp_rcv
                .recv()
                .expect("wfm create resp channel can't be dropped, it is in this stackframe")?;
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
        let (resp_tx, resp_rx) = bounded(1);
        let f = move |x: &mut WorkflowManager| {
            let _ = resp_tx.send(mutator(x));
        };
        m.send(Box::new(f))
            .expect("wfm mutation send can't fail, if it does a wfm is missing from their thread");
        resp_rx
            .recv()
            .expect("wfm access resp channel can't be dropped, it is in this stackframe")
    }

    /// Attempt to join the thread where the workflow machines live.
    ///
    /// # Panics
    /// If the workflow machine thread panicked
    pub fn shutdown(&self) {
        let _ = self.shutdown_chan.send(true);
        self.wf_thread
            .lock()
            .expect("Workflow manager thread mutex must be lockable")
            .take()
            .unwrap()
            .join()
            .expect("Workflow manager thread should shut down cleanly");
    }

    /// The implementation of the dedicated thread workflow managers live on
    fn workflow_thread(create_rcv: Receiver<MachineCreatorMsg>, shutdown_rx: Receiver<bool>) {
        let mut machine_rcvs: Vec<(MachineMutationReceiver, WorkflowManager)> = vec![];
        loop {
            // To avoid needing to busy loop, we want to block until either a creation message
            // arrives, or any machine access request arrives, so we cram all of them into a big
            // select. If multiple messages are ready at once they're handled in random order. This
            // is OK because they all go to independent workflows.

            // **IMPORTANT** the first operation in the select is always reading from the shutdown
            //   channel, and the second is always reading from the creation channel.
            let mut sel = Select::new();
            sel.recv(&shutdown_rx);
            sel.recv(&create_rcv);
            for (rcv, _) in machine_rcvs.iter() {
                sel.recv(rcv);
            }

            let index = sel.ready();
            if index == 0 {
                // Shutdown seen
                break;
            } else if index == 1 {
                // If there's a message ready on the creation channel, make a new machine
                // and put its receiver into the list, replying with the machine's activation and
                // a channel to send requests to it, or an error otherwise.
                let maybe_create_chan_msg = create_rcv.try_recv();
                let should_break = WorkflowConcurrencyManager::handle_creation_message(
                    &mut machine_rcvs,
                    maybe_create_chan_msg,
                );
                if should_break {
                    break;
                }
            } else {
                // If we're here, a request to access a workflow manager is ready.

                // We must subtract two to account for the shutdown and creation channels reads
                // being the first two operations in the select
                WorkflowConcurrencyManager::handle_access_msg(index - 2, &mut machine_rcvs)
            }
        }
    }

    /// Handle requests to create new workflow managers. Returns true if the creation channel
    /// was dropped and dedicated thread loop should be exited.
    fn handle_creation_message(
        machine_rcvs: &mut Vec<(MachineMutationReceiver, WorkflowManager)>,
        maybe_create_chan_msg: Result<MachineCreatorMsg, TryRecvError>,
    ) -> bool {
        match maybe_create_chan_msg {
            Ok((pwtqr, resp_chan)) => match WorkflowManager::new(pwtqr)
                .and_then(|mut wfm| Ok((wfm.get_next_activation()?, wfm)))
            {
                Ok((activation, wfm)) => {
                    let (machine_sender, machine_rcv) = unbounded();
                    machine_rcvs.push((machine_rcv, wfm));
                    resp_chan
                        .send(Ok((activation, machine_sender)))
                        .expect("wfm create resp rx side can't be dropped");
                }
                Err(e) => {
                    resp_chan
                        .send(Err(e))
                        .expect("wfm create resp rx side can't be dropped");
                }
            },
            Err(TryRecvError::Disconnected) => {
                event!(
                    Level::WARN,
                    "Sending side of workflow machine creator was dropped. Likely the \
                            WorkflowConcurrencyManager was dropped. This indicates a failure to \
                            call shutdown."
                );
                return true;
            }
            Err(TryRecvError::Empty) => {}
        }
        false
    }

    /// Handles requests to access/mutate a workflow manager. The passed in index indicates which
    /// machine in the `machine_rcvs` vec is ready to be read from.
    fn handle_access_msg(
        index: usize,
        machine_rcvs: &mut Vec<(MachineMutationReceiver, WorkflowManager)>,
    ) {
        match machine_rcvs[index].0.try_recv() {
            Ok(func) => {
                // Recall that calling this function also sends the response
                func(&mut machine_rcvs[index].1);
            }
            Err(TryRecvError::Disconnected) => {
                // This is expected when core is done with a workflow manager. IE: is
                // ready to remove it from the cache. It dropping the send side from the
                // concurrency manager is the signal to this thread that the workflow
                // manager can be dropped.
                let wfid = &machine_rcvs[index].1.machines.workflow_id;
                event!(
                    Level::DEBUG,
                    "Workflow manager thread done with workflow id {}",
                    wfid
                );
                machine_rcvs.remove(index);
            }
            Err(TryRecvError::Empty) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machines::test_help::TestHistoryBuilder;
    use crate::protos::temporal::api::common::v1::WorkflowExecution;
    use crate::protos::temporal::api::enums::v1::EventType;
    use crate::protos::temporal::api::history::v1::History;

    // We test mostly error paths here since the happy paths are well covered by the tests of the
    // core sdk itself, and setting up the fake data is onerous here. If we make the concurrency
    // manager generic, testing the happy path is simpler.

    #[test]
    fn can_shutdown_after_creating_machine() {
        let mgr = WorkflowConcurrencyManager::new();
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        let activation = mgr
            .create_or_update(
                "some_run_id",
                PollWorkflowTaskQueueResponse {
                    history: Some(History {
                        events: t.get_history_info(1).unwrap().events,
                    }),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: "wid".to_string(),
                        run_id: "rid".to_string(),
                    }),
                    task_token: vec![1],
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(activation.activation.is_some());

        mgr.shutdown();
    }

    #[test]
    fn returns_errors_on_creation() {
        let mgr = WorkflowConcurrencyManager::new();
        let res = mgr.create_or_update("some_run_id", PollWorkflowTaskQueueResponse::default());
        // Should whine that we didn't provide history
        assert_matches!(res.unwrap_err(), CoreError::BadDataFromWorkProvider(_))
    }
}
