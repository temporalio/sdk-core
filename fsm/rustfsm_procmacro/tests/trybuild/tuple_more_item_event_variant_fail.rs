extern crate rustfsm_trait as rustfsm;

use rustfsm_procmacro::fsm;

fsm! {
    name Simple; command SimpleCmd; error Infallible;

    One --(A(Foo, Bar), on_a)--> Two
}

fn main() {}
