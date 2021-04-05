#![allow(dead_code)]

extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;
use state_machine_trait::TransitionResult;
use std::convert::Infallible;

fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A(String), foo)--> Two;
    One --(B)--> Two;
    Two --(B)--> One;
    Two --(C, baz)--> One
}

#[derive(Default, Clone)]
pub struct One {}
impl One {
    fn foo(self, _: String) -> SimpleMachineTransition<Two> {
        TransitionResult::default()
    }
}
impl From<Two> for One {
    fn from(_: Two) -> Self {
        One {}
    }
}

#[derive(Default, Clone)]
pub struct Two {}
impl Two {
    fn baz(self) -> SimpleMachineTransition<One> {
        TransitionResult::default()
    }
}
impl From<One> for Two {
    fn from(_: One) -> Self {
        Two {}
    }
}

pub enum SimpleMachineCommand {}

fn main() {}
