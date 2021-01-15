extern crate state_machine_trait as rustfsm;

use state_machine_procmacro::fsm;

fsm! {
    name Simple; command SimpleCmd; error Infallible;

    One --(A(Foo, Bar), on_a)--> Two
}

fn main() {}
