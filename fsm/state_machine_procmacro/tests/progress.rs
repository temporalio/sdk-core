extern crate state_machine_trait as rustfsm;

use state_machine_trait::TransitionResult;
use std::convert::Infallible;

#[test]
fn tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/trybuild/*_pass.rs");
    t.compile_fail("tests/trybuild/*_fail.rs");
}

// Kept here to inspect manual expansion
state_machine_procmacro::fsm! {
    name SimpleMachine; command SimpleMachineCommand; error Infallible;

    One --(A(String), foo)--> Two;
    One --(B)--> Two;
    Two --(B)--> One;
    Two --(C, baz)--> One
}

#[derive(Default, Clone)]
pub struct One {}
impl One {
    fn foo(self, _: String) -> SimpleMachineTransition {
        TransitionResult::default::<Two>()
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
    fn baz(self) -> SimpleMachineTransition {
        TransitionResult::default::<One>()
    }
}
impl From<One> for Two {
    fn from(_: One) -> Self {
        Two {}
    }
}

pub enum SimpleMachineCommand {}
