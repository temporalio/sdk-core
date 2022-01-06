use log::{LevelFilter, Log, Metadata, Record};
use ringbuf::{Consumer, Producer, RingBuffer};
use std::{sync::Mutex, time::SystemTime};
use temporal_sdk_core_api::CoreLog;

pub(crate) struct CoreExportLogger {
    logs_in: Mutex<Producer<CoreLog>>,
    logs_out: Mutex<Consumer<CoreLog>>,
    level_filter: LevelFilter,
}

impl CoreExportLogger {
    pub(crate) fn new(level: LevelFilter) -> Self {
        let (lin, lout) = RingBuffer::new(2048).split();
        Self {
            logs_in: Mutex::new(lin),
            logs_out: Mutex::new(lout),
            level_filter: level,
        }
    }

    pub(crate) fn drain(&self) -> Vec<CoreLog> {
        let mut lout = self
            .logs_out
            .lock()
            .expect("Logging output mutex must be acquired");
        let mut retme = Vec::with_capacity(lout.len());
        lout.pop_each(
            |el| {
                retme.push(el);
                true
            },
            None,
        );
        retme
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
        let _ = self
            .logs_in
            .lock()
            .expect("Logging mutex must be acquired")
            .push(clog);
    }

    fn flush(&self) {}
}
