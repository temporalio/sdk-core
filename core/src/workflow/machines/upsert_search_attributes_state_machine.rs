use rustfsm::{fsm, StateMachine, TransitionResult};
use super::{
    workflow_machines::{MachineResponse, WFMachinesError},
    NewMachineWithCommand
};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_commands::{UpsertWorkflowSearchAttributes},
    },
    temporal::api::{
        command::v1::Command,
        enums::v1::{CommandType, EventType},
        history::v1::{history_event, HistoryEvent},
    },
};

fsm! {
    pub(super) name UpsertSearchAttributesMachine;
    command UpsertSearchAttributesCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> UpsertCommandCreated;

    UpsertCommandCreated --(CommandUpsertWorkflowSearchAttributes) --> UpsertCommandCreated;
    UpsertCommandCreated --(UpsertWorkflowSearchAttributes, on_upsert_workflow_search_attributes) --> UpsertCommandRecorded;
}

pub(super) enum UpsertSearchAttributesCommand {}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: UpsertWorkflowSearchAttributes
}

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
            command_type: CommandType::UpsertWorkflowSearchAttributes as i32,
            attributes: Some(s.shared_state().attrs.clone().into()),
        };
        (s, cmd)
    }

    fn new(attribs: UpsertWorkflowSearchAttributes) -> Self {
        Self {
            state: Created {}.into(),
            shared_state: SharedState {
                attrs: attribs
            },
        }
    }
}



impl TryFrom<HistoryEvent> for UpsertSearchAttributesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match e.event_type() {
            EventType::UpsertWorkflowSearchAttributes => {
                Self::UpsertWorkflowSearchAttributes
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "UpsertWorkflowSearchAttributes machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for UpsertSearchAttributesMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::UpsertWorkflowSearchAttributes => {
                Self::UpsertWorkflowSearchAttributes
            }
            _ => return Err(()),
        })
    }
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
