use std::convert::Infallible;
use temporalio_common::fsm_trait::TransitionResult;
use temporalio_macros::fsm;

fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A)--> Two
}

#[derive(Default, Clone)]
pub struct One {}

#[derive(Default, Clone)]
pub struct Two {}
impl From<One> for Two {
    fn from(_: One) -> Self {
        Two {}
    }
}

pub enum SimpleMachineCommand {}

fn main() {
    // state enum exists with both states
    let _ = SimpleMachineState::One(One {});
    let _ = SimpleMachineState::Two(Two {});
    // Event enum exists
    let _ = SimpleMachineEvents::A;
}
