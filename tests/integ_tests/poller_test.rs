use std::convert::TryFrom;
use temporal_sdk_core::{Core, CoreInitOptions, Url};

#[test]
fn empty_poll() {
    let core = temporal_sdk_core::init(CoreInitOptions {
        target_url: Url::try_from("http://localhost:7233").unwrap(),
        namespace: "default".to_string(),
        identity: "none".to_string(),
        binary_checksum: "".to_string(),
    })
    .unwrap();

    dbg!(core.poll_task("test-tq").unwrap());
}
