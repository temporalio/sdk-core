use std::convert::Infallible;
use temporalio_common::fsm_trait::TransitionResult;
use temporalio_macros::fsm;

fsm! {
    name Simple; command SimpleCommand; error Infallible;

    One --(A(String), on_a)--> Two
}

#[derive(Default, Clone)]
pub struct One {}
impl One {
    fn on_a(self, _: String) -> SimpleTransition<Two> {
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
    // Avoid dead code warning
    let _ = SimpleEvents::A("yo".to_owned());
}
