extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;

fsm! {
    Simple, SimpleCommand, Infallible

    One --(A{foo: String}, on_a)--> Two
}

pub struct One {}
pub struct Two {}

pub enum SimpleCommand {}

fn main() {}
