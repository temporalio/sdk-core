extern crate rustfsm_trait as rustfsm;

#[test]
fn tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/trybuild/*_pass.rs");
    t.compile_fail("tests/trybuild/*_fail.rs");
}
