use temporalio_macros::fsm;

fsm! {
    name Simple; command SimpleCmd; error Infallible;

    One --(A(Foo, Bar), on_a)--> Two
}

fn main() {}
