//! Integration tests

#[macro_use]
extern crate rstest;
#[macro_use]
extern crate temporal_sdk_core_test_utils;

#[cfg(test)]
mod integ_tests {
    mod activity_functions;
    mod client_tests;
    mod ephemeral_server_tests;
    mod heartbeat_tests;
    mod metrics_tests;
    mod polling_tests;
    mod queries_tests;
    mod update_tests;
    mod visibility_tests;
    mod workflow_tests;
}
