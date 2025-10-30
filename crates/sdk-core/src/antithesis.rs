#[cfg(feature = "antithesis_assertions")]
use std::sync::OnceLock;

#[cfg(feature = "antithesis_assertions")]
use serde_json::Value;

/// Ensure Antithesis is initialized exactly once.
#[cfg(feature = "antithesis_assertions")]
pub(crate) fn ensure_init() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        ::antithesis_sdk::antithesis_init();
    });
}

#[cfg(not(feature = "antithesis_assertions"))]
#[inline]
pub(crate) fn ensure_init() {}

/// Record an always-true assertion failure to Antithesis when an invariant is
/// violated.
#[cfg(feature = "antithesis_assertions")]
pub(crate) fn assert_always_failure(name: &str, metadata: Value) {
    ::antithesis_sdk::assert_always!(false, name, &metadata);
}
