use log::{Level, Log, Metadata, Record};
use std::{collections::VecDeque, sync::Mutex, time::Instant};

/// A log line (which ultimately came from a tracing event) exported from Core->Lang
#[derive(Debug)]
pub struct CoreLog {
    message: String,
    timestamp: Instant,
    level: Level,
    // KV pairs aren't meaningfully exposed yet to the log interface by tracing
}

#[derive(Default)]
pub(crate) struct CoreExportLogger {
    records: Mutex<VecDeque<CoreLog>>,
}

impl Log for CoreExportLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        // TODO: Disable in test only mode where no lang half
        true
    }

    fn log(&self, record: &Record) {
        let clog = CoreLog {
            message: format!("{}", record.args()),
            timestamp: Instant::now(),
            level: record.level(),
        };
        dbg!(&clog);
        self.records
            .lock()
            .expect("Logging mutex must be acquired")
            .push_back(clog)
    }

    fn flush(&self) {
        // Does nothing
    }
}
