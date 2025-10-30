#[cfg(feature = "antithesis_assertions")]
use std::sync::OnceLock;

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

#[cfg(feature = "antithesis_assertions")]
macro_rules! assert_always {
    ($message:literal, $condition:expr, $details:expr) => {{
        $crate::antithesis::ensure_init();
        let details: ::serde_json::Value = $details;
        ::antithesis_sdk::assert_always!($condition, $message, &details);
    }};
    ($message:literal, $condition:expr) => {{
        $crate::antithesis::ensure_init();
        ::antithesis_sdk::assert_always!($condition, $message);
    }};
}

#[cfg(not(feature = "antithesis_assertions"))]
macro_rules! assert_always {
    ($message:literal, $condition:expr, $details:expr) => {{
        let _ = ($message, $condition);
        let _ = $details;
    }};
    ($message:literal, $condition:expr) => {{
        let _ = ($message, $condition);
    }};
}

#[cfg(feature = "antithesis_assertions")]
macro_rules! assert_always_failure {
    ($message:literal, $details:expr) => {{
        $crate::antithesis::assert_always!($message, false, $details);
    }};
}

#[cfg(not(feature = "antithesis_assertions"))]
macro_rules! assert_always_failure {
    ($message:literal, $details:expr) => {{
        let _ = $message;
        let _ = $details;
    }};
}

pub(crate) use assert_always;
pub(crate) use assert_always_failure;
