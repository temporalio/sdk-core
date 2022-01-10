mod history_builder;
mod history_info;

pub static DEFAULT_WORKFLOW_TYPE: &str = "not_specified";

pub use history_builder::{default_wes_attribs, TestHistoryBuilder};
pub use history_info::HistoryInfo;
use std::{fs, io, path::Path};
use temporal_sdk_core_protos::temporal::api::history::v1::History;

pub fn history_from_file(path: &Path) -> io::Result<History> {
    let str = fs::read_to_string(path)?;
    let as_history: History = serde_json::from_str(&str)?;
    Ok(as_history)
}
