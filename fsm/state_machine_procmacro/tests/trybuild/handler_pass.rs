extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;
use state_machine_trait::TransitionResult;
use std::convert::Infallible;

fsm! {
    Simple, SimpleCommand, Infallible

    One --(A, on_a)--> Two
}

pub struct One {}
impl One {
    fn on_a(self) -> SimpleTransition {
        SimpleTransition::ok(vec![], Two {})
    }
}
pub struct Two {}

pub enum SimpleCommand {}

fn main() {
    // main enum exists with both states
    let _ = Simple::One(One {});
    let _ = Simple::Two(Two {});
}
