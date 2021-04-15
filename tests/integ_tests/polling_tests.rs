use crate::integ_tests::get_integ_server_options;
use assert_matches::assert_matches;
use crossbeam::channel::{unbounded, RecvTimeoutError};
use std::time::Duration;
use temporal_sdk_core::{Core, CoreInitOptions};

#[tokio::test]
async fn long_poll_timeout_is_retried() {
    let mut gateway_opts = get_integ_server_options();
    // Server whines unless long poll > 2 seconds
    gateway_opts.long_poll_timeout = Duration::from_secs(3);
    let core = temporal_sdk_core::init(CoreInitOptions {
        gateway_opts,
        evict_after_each_workflow_task: false,
    })
    .await
    .unwrap();
    // Should block for more than 3 seconds, since we internally retry long poll
    let (tx, rx) = unbounded();
    tokio::spawn(async move {
        core.poll_workflow_task("some_task_q").await.unwrap();
        tx.send(())
    });
    let err = rx.recv_timeout(Duration::from_secs(4)).unwrap_err();
    assert_matches!(err, RecvTimeoutError::Timeout);
}
