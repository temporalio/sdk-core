mod history_builder;
mod history_info;

pub static DEFAULT_WORKFLOW_TYPE: &str = "not_specified";

pub use history_builder::{default_wes_attribs, TestHistoryBuilder};
pub use history_info::HistoryInfo;
