use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name UpsertSearchAttributesMachine; command UpsertSearchAttributesCommand; error UpsertSearchAttributesMachineError;

    Created --(Schedule, on_schedule) --> UpsertCommandCreated;

    UpsertCommandCreated --(CommandUpsertWorkflowSearchAttributes) --> UpsertCommandCreated;
    UpsertCommandCreated --(UpsertWorkflowSearchAttributes, on_upsert_workflow_search_attributes) --> UpsertCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum UpsertSearchAttributesMachineError {}

pub(super) enum UpsertSearchAttributesCommand {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> UpsertSearchAttributesMachineTransition<UpsertCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct UpsertCommandCreated {}

impl UpsertCommandCreated {
    pub(super) fn on_upsert_workflow_search_attributes(
        self,
    ) -> UpsertSearchAttributesMachineTransition<UpsertCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct UpsertCommandRecorded {}
