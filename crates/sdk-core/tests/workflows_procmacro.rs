#[test]
fn workflows_procmacro_build_tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/workflows_trybuild/*_pass.rs");
    t.compile_fail("tests/workflows_trybuild/*_fail.rs");
}
