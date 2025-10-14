#[test]
fn fsm_procmacro_build_tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/fsm_trybuild/*_pass.rs");
    t.compile_fail("tests/fsm_trybuild/*_fail.rs");
}
