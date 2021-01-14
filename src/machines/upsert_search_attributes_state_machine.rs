use rustfsm::{fsm, TransitionResult};

fsm! {
    UpsertSearchAttributesMachine, UpsertSearchAttributesCommand, UpsertSearchAttributesMachineError

    Created --(Schedule, on_schedule) --> UpsertCommandCreated;

    UpsertCommandCreated --(CommandUpsertWorkflowSearchAttributes) --> UpsertCommandCreated;
    UpsertCommandCreated --(UpsertWorkflowSearchAttributes, on_upsert_workflow_search_attributes) --> UpsertCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub enum UpsertSearchAttributesMachineError {}

pub enum UpsertSearchAttributesCommand {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> UpsertSearchAttributesMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct UpsertCommandCreated {}

impl UpsertCommandCreated {
    pub fn on_upsert_workflow_search_attributes(self) -> UpsertSearchAttributesMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct UpsertCommandRecorded {}
