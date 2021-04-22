#![allow(dead_code)]

extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;
use rustfsm_trait::TransitionResult;
use std::convert::Infallible;

fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A(String), foo)--> Two;
    One --(A(String), foo)--> Three;

    Two --(B(String), bar)--> One;
    Two --(B(String), bar)--> Two;
    Two --(B(String), bar)--> Three;
}

#[derive(Default, Clone)]
pub struct One {}
impl One {
    fn foo(self, _: String) -> SimpleMachineTransition<TwoOrThree> {
        TransitionResult::ok(vec![], Two {}.into())
    }
}

#[derive(Default, Clone)]
pub struct Two {}
impl Two {
    fn bar(self, _: String) -> SimpleMachineTransition<OneOrTwoOrThree> {
        TransitionResult::ok(vec![], Three {}.into())
    }
}

#[derive(Default, Clone)]
pub struct Three {}

pub enum SimpleMachineCommand {}

fn main() {}
