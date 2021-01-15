extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;

fsm! {
    name Simple; command SimpleCommand; error Infallible;

    One --(A{foo: String}, on_a)--> Two
}

#[derive(Default, Clone)]
pub struct One {}
#[derive(Default, Clone)]
pub struct Two {}

pub enum SimpleCommand {}

fn main() {}
