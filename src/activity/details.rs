use prost_types::Duration;

/// Contains minimal set of details that core needs to store, while activity is running.
#[derive(derive_more::Constructor, Debug)]
pub(crate) struct InflightActivityDetails {
    pub activity_id: String,
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<Duration>,
    /// Set to true if we have already issued a cancellation activation to lang for this activity
    pub issued_cancel_to_lang: bool,
    /// Set to true if we have already learned from the server this activity doesn't exist. EX:
    /// we have learned from heartbeating and issued a cancel task, in which case we may simply
    /// discard the reply.
    pub known_not_found: bool,
}
