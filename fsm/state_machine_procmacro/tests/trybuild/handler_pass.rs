extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;
use state_machine_trait::TransitionResult;
use std::convert::Infallible;

fsm! {
    name Simple; command SimpleCommand; error Infallible;

    One --(A, on_a)--> Two
}

#[derive(Default, Clone)]
pub struct One {}
impl One {
    fn on_a(self) -> SimpleTransition {
        SimpleTransition::ok(vec![], Two {})
    }
}

#[derive(Default, Clone)]
pub struct Two {}

pub enum SimpleCommand {}

fn main() {
    // state enum exists with both states
    let _ = SimpleState::One(One {});
    let _ = SimpleState::Two(Two {});
}
