/// Number of max retries that will be used in the retry gateway.
pub const MAX_RETRIES: usize = 10;
/// The default initial interval value in milliseconds (0.1 seconds).
pub const INITIAL_INTERVAL_MILLIS: u64 = 100;
/// The default randomization factor (0.2 which results in a random period ranging between 20%
/// below and 20% above the retry interval).
pub const RANDOMIZATION_FACTOR: f64 = 0.2;
/// The default multiplier value (1.5 which is 50% increase per back off).
pub const MULTIPLIER: f64 = 1.5;
/// The default maximum back off time in milliseconds (1 minute).
pub const MAX_INTERVAL_MILLIS: u64 = 2_000;
/// The default maximum elapsed time in milliseconds (30 seconds).
pub const MAX_ELAPSED_TIME_MILLIS: u64 = 30_000;
