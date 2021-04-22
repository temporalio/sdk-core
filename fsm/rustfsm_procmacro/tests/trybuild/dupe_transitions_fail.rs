extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;

fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A)--> Two;
    One --(A)--> Two;
}

#[derive(Default, Clone)]
pub struct One {}

#[derive(Default, Clone)]
pub struct Two {}

fn main() {}
