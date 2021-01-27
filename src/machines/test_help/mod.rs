type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

mod history_builder;
mod workflow_driver;

pub(crate) use history_builder::TestHistoryBuilder;
pub(super) use workflow_driver::{CommandSender, TestWFCommand, TestWorkflowDriver};
