use std::time::Duration;

use temporalio_workflow::{
    WorkflowContext, WorkflowResult, export_workflow_module, workflow, workflow_methods,
};

unsafe extern "C" {
    fn temporal_funcref_table_set_to_after();
    fn temporal_funcref_table_call() -> i32;
}

#[workflow]
#[derive(Default)]
pub struct HelloWorkflow;

#[workflow_methods]
impl HelloWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
#[derive(Default)]
pub struct TimerWorkflow;

#[workflow_methods]
impl TimerWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(1)).await;
        Ok(format!("Timer fired for {name}!"))
    }
}

type FuncrefFormatter = fn(&str) -> String;

struct FuncrefState {
    formatter: FuncrefFormatter,
}

#[inline(never)]
fn restored_funcref_message(name: &str) -> String {
    format!("Funcref restored for {name}!")
}

#[inline(never)]
fn alternate_funcref_message(name: &str) -> String {
    format!("Alternate funcref restored for {name}!")
}

#[inline(never)]
fn choose_funcref_formatter(name: &str) -> FuncrefFormatter {
    if name.len() % 2 == 0 {
        restored_funcref_message
    } else {
        alternate_funcref_message
    }
}

#[workflow]
#[derive(Default)]
pub struct FuncrefWorkflow;

#[workflow_methods]
impl FuncrefWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let state = FuncrefState {
            formatter: choose_funcref_formatter(&name),
        };
        unsafe {
            temporal_funcref_table_set_to_after();
        }
        let table_value_before_snapshot = unsafe { temporal_funcref_table_call() };
        ctx.timer(Duration::from_millis(1)).await;
        let table_value_after_snapshot = unsafe { temporal_funcref_table_call() };
        Ok(format!(
            "{} table={table_value_before_snapshot}->{table_value_after_snapshot}",
            (state.formatter)(&name)
        ))
    }
}

export_workflow_module!([HelloWorkflow, TimerWorkflow, FuncrefWorkflow]);
