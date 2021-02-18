//! Ultimately it would be nice to make this generic and push it out into it's own crate but
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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
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
    wf_thread: JoinHandle<()>,
    machine_creator: Sender<MachineCreatorMsg>,
    shutdown_flag: Arc<AtomicBool>,
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
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_for_thread = shutdown_flag.clone();

        let wf_thread = thread::spawn(move || {
            let mut machine_rcvs: Vec<(MachineMutationReceiver, WorkflowManager)> = vec![];
            loop {
                if shutdown_flag_for_thread.load(Ordering::Relaxed) {
                    break;
                }
                // If there's a message ready on the creation channel, make a new machine
                // and put it's receiver into the list, replying with the machine's activation and
                // a channel to send requests to it, or an error otherwise.
                match create_rcv.try_recv() {
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
                if let Ok(index) = sel.try_ready() {
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
                // TODO: remove probably
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        Self {
            machines: Default::default(),
            wf_thread,
            machine_creator,
            shutdown_flag,
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
    #[allow(unused)] // TODO: Will be used when other shutdown PR is merged
    pub fn shutdown(self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
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
        t.add_workflow_task();

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
