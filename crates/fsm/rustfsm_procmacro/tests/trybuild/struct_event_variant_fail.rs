extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;

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
