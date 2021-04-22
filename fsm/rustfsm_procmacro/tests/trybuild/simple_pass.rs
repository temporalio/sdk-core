extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;
use rustfsm_trait::TransitionResult;
use std::convert::Infallible;

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
