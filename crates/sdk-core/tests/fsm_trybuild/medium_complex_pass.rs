#![allow(dead_code)]

use std::convert::Infallible;
use temporalio_common::fsm_trait::TransitionResult;
use temporalio_macros::fsm;

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
