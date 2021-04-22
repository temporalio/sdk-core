use prost_types::Duration;

/// Contains minimal set of details that core needs to store, while activity is running.
pub(crate) struct InflightActivityDetails {
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<Duration>,
}

impl InflightActivityDetails {
    pub fn new(heartbeat_timeout: Option<Duration>) -> Self {
        Self { heartbeat_timeout }
    }
}
