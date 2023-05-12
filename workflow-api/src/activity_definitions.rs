use crate::WfContext;
use futures::future::BoxFuture;

// #[activity_definition]
type MyActFn = fn(String) -> String;

// Macro enforces types are serializable.

// The biggest problem with activity definitions is they need to be defined in a crate which doesn't
// depend on the entire SDK, because then the workflow code which uses them wouldn't be able to
// be compiled down to WASM. Of course, the issue is activities _aren't_ compiled to WASM, and need
// access to full native functionality. Thus users need to structure their app a bit oddly. They
// can either define all their workflow code & activity _definitions_ in one crate, and then
// depend on that crate from another crate containing their activity implementations / worker, or
// they could make a crate with *just* activity definitions, which is depended on by the workflow
// implementation crate and the worker crate independently. It all makes perfect sense, but is
// maybe a bit annoying in terms of setup - though not really any worse than TS.

// Macro generates this extension & implementation:
//
// The generated code taking `impl Into<Argtype>` is quite nice for ergonomics inside the workflow,
// but might be impossible in some cases, so probably macro would need a flag to turn it off.
pub trait MyActFnWfCtxExt {
    // In reality this returns the `CancellableFuture` type from SDK, would also need to move into
    // this crate.
    fn my_act_fn(
        &self,
        input: impl Into<String>,
    ) -> BoxFuture<'static, Result<String, ActivityFail>>;
}
impl MyActFnWfCtxExt for WfContext {
    fn my_act_fn(&self, _: impl Into<String>) -> BoxFuture<'static, Result<String, ActivityFail>> {
        // Type name is injected in this implementation, taken from macro
        todo!()
    }
}

// To implement the activity in their implementation crate, the user would do something like:
// worker.register_activity(MyActFn, |input: String| async move { .... });

// Placeholder. Activity failures as can be seen by the WF code.
#[derive(Debug)]
pub struct ActivityFail {}
