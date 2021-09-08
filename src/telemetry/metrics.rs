use super::TELEM_SERVICE_NAME;
use opentelemetry::{global, metrics::Counter};

/// Define a temporal metric
macro_rules! tm {
    (ctr, $ident:ident, $name:literal) => {
        lazy_static::lazy_static! {
            pub(crate) static ref $ident: Counter<u64> = {
                let meter = global::meter(TELEM_SERVICE_NAME);
                meter.u64_counter($name).init()
            };
        }
    };
}

tm!(
    ctr,
    WORKFLOW_TASK_QUEUE_POLL_EMPTY_COUNTER,
    "workflow_task_queue_poll_empty"
);
tm!(
    ctr,
    WORKFLOW_TASK_QUEUE_POLL_SUCCEED_COUNTER,
    "workflow_task_queue_poll_succeed"
);

pub(crate) const KEY_NAMESPACE: &str = "namespace";
