use prost_types::Duration;

/// Contains minimal set of details that core needs to store, while activity is running.
#[derive(derive_more::Constructor, Debug)]
pub(crate) struct InflightActivityDetails {
    pub activity_id: String,
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<Duration>,
}
