use log::{Level, LevelFilter, Log, Metadata, Record};
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A log line (which ultimately came from a tracing event) exported from Core->Lang
#[derive(Debug)]
pub struct CoreLog {
    /// Log message
    pub message: String,
    /// Time log was generated (not when it was exported to lang)
    pub timestamp: SystemTime,
    /// Message level
    pub level: Level,
    // KV pairs aren't meaningfully exposed yet to the log interface by tracing
}

impl CoreLog {
    /// Return timestamp as ms since epoch
    pub fn millis_since_epoch(&self) -> u128 {
        self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    }
}

pub(crate) struct CoreExportLogger {
    records: Mutex<VecDeque<CoreLog>>,
    level_filter: LevelFilter,
}

impl CoreExportLogger {
    pub(crate) fn new(level: LevelFilter) -> Self {
        Self {
            records: Mutex::new(Default::default()),
            level_filter: level,
        }
    }

    pub(crate) fn drain(&self) -> Vec<CoreLog> {
        self.records
            .lock()
            .expect("Logging mutex must be acquired")
            .drain(..)
            .collect()
    }
}

impl Log for CoreExportLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // Never forward logging from other crates
        if !metadata.target().contains("temporal_sdk_core") {
            return false;
        }
        metadata.level() <= self.level_filter
    }

    fn log(&self, record: &Record) {
        let clog = CoreLog {
            message: format!("[{}] {}", record.target(), record.args()),
            timestamp: SystemTime::now(),
            level: record.level(),
        };
        self.records
            .lock()
            .expect("Logging mutex must be acquired")
            .push_back(clog)
    }

    fn flush(&self) {}
}
