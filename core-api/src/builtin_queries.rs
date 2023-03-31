use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use temporal_sdk_core_protos::coresdk::workflow_commands::{
    query_result, QueryResult, QuerySuccess,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SDKInfo {
    pub name: String,
    pub version: String,
}

/// Represents a slice of a file starting at line_offset
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FileSlice {
    /// slice of a file with `\n` (newline) line terminator.
    pub content: String,
    /// Only used possible to trim the file without breaking syntax highlighting.
    pub line_offset: u64,
}

/// A pointer to a location in a file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FileLocation {
    /// Path to source file (absolute or relative).
    /// When using a relative path, make sure all paths are relative to the same root.
    pub file_path: Option<String>,
    /// If possible, SDK should send this, required for displaying the code location.
    pub line: Option<u64>,
    /// If possible, SDK should send this.
    pub column: Option<u64>,
    /// Function name this line belongs to (if applicable).
    /// Used for falling back to stack trace view.
    pub function_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InternalCommandType {
    ScheduleActivity,
    StartTimer,
}

/// An internal (Lang<->Core) command identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalCommand {
    pub r#type: InternalCommandType,
    pub seq: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InternalStackTrace {
    pub locations: Vec<FileLocation>,
    pub commands: Vec<InternalCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InternalEnhancedStackTrace {
    pub sdk: SDKInfo,
    /// Mapping of file path to file contents. SDK may choose to send no, some or all sources.
    /// Sources might be trimmed, and some time only the file(s) of the top element of the trace
    /// will be sent.
    pub sources: HashMap<String, Vec<FileSlice>>,
    pub stacks: Vec<InternalStackTrace>,
}

impl TryFrom<&QueryResult> for InternalEnhancedStackTrace {
    type Error = ();

    fn try_from(qr: &QueryResult) -> Result<Self, Self::Error> {
        if let Some(query_result::Variant::Succeeded(QuerySuccess { ref response })) = qr.variant {
            if let Some(payload) = response {
                if payload.is_json_payload() {
                    if let Ok(internal_trace) = serde_json::from_slice::<InternalEnhancedStackTrace>(
                        payload.data.as_slice(),
                    ) {
                        return Ok(internal_trace);
                    }
                }
            }
        }
        Err(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StackTrace {
    pub locations: Vec<FileLocation>,
    pub correlating_event_ids: Vec<i64>,
}

// Used as the result for the enhanced stack trace query
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EnhancedStackTrace {
    pub sdk: SDKInfo,
    /// Mapping of file path to file contents. SDK may choose to send no, some or all sources.
    /// Sources might be trimmed, and some time only the file(s) of the top element of the trace
    /// will be sent.
    pub sources: HashMap<String, Vec<FileSlice>>,
    pub stacks: Vec<StackTrace>,
}

// Used as the result for the time travel stack trace query
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimeTravelStackTrace {
    pub sdk: SDKInfo,
    /// Mapping of file path to file contents. SDK may choose to send no, some or all sources.
    /// Sources might be trimmed, and some time only the file(s) of the top element of the trace
    /// will be sent.
    pub sources: HashMap<String, Vec<FileSlice>>,
    /// Maps WFT started event ids to active stack traces upon completion of that WFT
    pub stacks: HashMap<u32, Vec<StackTrace>>,
}
