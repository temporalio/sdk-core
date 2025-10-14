extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;
use rustfsm_trait::TransitionResult;
use std::convert::Infallible;

fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A)--> Two;
    Two --(B)--> One;
}

#[derive(Default, Clone)]
pub struct One {}

#[derive(Default, Clone)]
pub struct Two {}
// We implement one of them because trait bound satisfaction error output is not deterministically
// ordered
impl From<One> for Two {
    fn from(_: One) -> Self {
        Two {}
    }
}

enum SimpleMachineCommand {}

fn main() {}
