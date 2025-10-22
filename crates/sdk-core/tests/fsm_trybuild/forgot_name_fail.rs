use temporalio_macros::fsm;

fsm! {
    One --(A)--> Two
}

#[derive(Default, Clone)]
pub struct One {}
#[derive(Default, Clone)]
pub struct Two {}

fn main() {}
