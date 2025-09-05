//! This crate contains testing functionality that can be useful when building SDKs against Core,
//! or even when testing workflows written in SDKs that use Core.

use std::env;
use temporal_client::TlsConfig;
use temporal_sdk_core::{ClientOptions, ClientOptionsBuilder};
use url::Url;

// Re-export from new locations for backwards compatibility
pub use temporal_sdk_core::{replay::HistoryForReplay, test_utils::NAMESPACE};

/// The env var used to specify where the integ tests should point
pub const INTEG_SERVER_TARGET_ENV_VAR: &str = "TEMPORAL_SERVICE_ADDRESS";
pub const INTEG_USE_TLS_ENV_VAR: &str = "TEMPORAL_USE_TLS";
pub const INTEG_API_KEY: &str = "TEMPORAL_API_KEY_PATH";

/// Returns the client options used to connect to the server used for integration tests.
pub fn get_integ_server_options() -> ClientOptions {
    let temporal_server_address = env::var(INTEG_SERVER_TARGET_ENV_VAR)
        .unwrap_or_else(|_| "http://localhost:7233".to_owned());
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let mut cb = ClientOptionsBuilder::default();
    cb.identity("integ_tester".to_string())
        .target_url(url)
        .client_name("temporal-core".to_string())
        .client_version("0.1.0".to_string());
    if let Ok(key_file) = env::var(INTEG_API_KEY) {
        let content = std::fs::read_to_string(key_file).unwrap();
        cb.api_key(Some(content));
    }
    if let Some(tls) = get_integ_tls_config() {
        cb.tls_cfg(tls);
    };
    cb.build().unwrap()
}

pub fn get_integ_tls_config() -> Option<TlsConfig> {
    if env::var(INTEG_USE_TLS_ENV_VAR).is_ok() {
        use temporal_client::ClientTlsConfig;
        let root = std::fs::read("../.cloud_certs/ca.pem").unwrap();
        let client_cert = std::fs::read("../.cloud_certs/client.pem").unwrap();
        let client_private_key = std::fs::read("../.cloud_certs/client.key").unwrap();
        Some(TlsConfig {
            server_root_ca_cert: Some(root),
            domain: None,
            client_tls_config: Some(ClientTlsConfig {
                client_cert,
                client_private_key,
            }),
        })
    } else {
        None
    }
}
