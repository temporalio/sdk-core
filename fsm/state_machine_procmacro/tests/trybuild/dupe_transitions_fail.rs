extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;

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
