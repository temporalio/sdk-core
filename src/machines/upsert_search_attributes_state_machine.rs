use rustfsm::{fsm, TransitionResult};
use crate::machines::{
    workflow_machines::{MachineResponse, WFMachinesError},
    Cancellable, EventInfo, MachineKind, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
};
use temporal_sdk_core_protos::{
    coresdk::{
        // workflow_activation::FireTimer,
        // workflow_commands::{UpsertCommandCreated, UpsertCommandRecorded, UpsertSearchAttributesCommand},
        workflow_commands::{UpsertWorkflowSearchAttributes}, // no `UpsertWorkflowSearchAttributes` in `coresdk::workflow_commands`
        HistoryEventId,
    },
    temporal::api::{
        command::v1::Command,
        enums::v1::{CommandType, EventType},
        history::v1::{history_event, HistoryEvent, TimerFiredEventAttributes},
    },
};

fsm! {
    pub(super) name UpsertSearchAttributesMachine;
    command UpsertSearchAttributesCommand;
    error UpsertSearchAttributesMachineError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> UpsertCommandCreated;

    UpsertCommandCreated --(CommandUpsertWorkflowSearchAttributes) --> UpsertCommandCreated;
    UpsertCommandCreated --(UpsertWorkflowSearchAttributes, on_upsert_workflow_search_attributes) --> UpsertCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum UpsertSearchAttributesMachineError {}

pub(super) enum UpsertSearchAttributesCommand {}



/// Creates a upsert workflow attribute command as a [CancellableCommand]
pub(super) fn upsert_search_attrs(attribs: UpsertWorkflowSearchAttributes) -> NewMachineWithCommand {
    let (state_machine, add_cmd) = UpsertSearchAttributesMachine::new_upsert(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: state_machine.into(),
    }
}

impl UpsertSearchAttributesMachine {
    /// Create a new UpsertSearchAttributesCommand
    fn new_upsert(attribs: UpsertWorkflowSearchAttributes) -> (Self, Command) {
        let mut s = Self::new(attribs);
        let cmd = Command {
            command_type: CommandType::UpsertSearchAttributesCommand as i32,
            attributes: Some(s.shared_state().attrs.clone().into()),
        };
        (s, cmd)
    }

    fn new(attribs: UpsertWorkflowSearchAttributes) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                attrs: attribs,
                cancelled_before_sent: false,
            },
        }
    }
}


#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: UpsertWorkflowSearchAttributes,
    cancelled_before_sent: bool,
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> UpsertSearchAttributesMachineTransition<UpsertCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct UpsertCommandCreated {}

impl UpsertCommandCreated {
    pub(super) fn on_upsert_workflow_search_attributes(
        self,
    ) -> UpsertSearchAttributesMachineTransition<UpsertCommandRecorded> {
        TransitionResult::ok(vec![], UpsertCommandRecorded::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct UpsertCommandRecorded {}


#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        prototype_rust_sdk::{CancellableFuture, WfContext, WorkflowFunction},
        test_help::{canned_histories, TestHistoryBuilder},
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};
    use std::{mem::discriminant, time::Duration};


    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_search_attributes() {
        let func = WorkflowFunction::new(|ctx: WfContext| async move {
            let upsert_fut = ctx.upsert_search_attributes(
                vec![Payload {
                    CustomStringField: b"hello ".to_vec(),
                }]
            );
            upsert_fut.await;
            Ok(().into())
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);

        wfm.process_all_activations().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::UpsertWorkflowSearchAttributes as i32);
        wfm.shutdown().await.unwrap();
    }
}