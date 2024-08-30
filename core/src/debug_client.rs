//! This module contains code for an http client that is used for the VSCode debug plugin.

use prost::Message;
use reqwest;
use std::time::Duration;
use temporal_sdk_core_protos::temporal::api::history::v1::History;

const CLIENT_NAME: &str = "temporal-core";
const CLIENT_VERSION: &str = "0.1.0";

/// A client for interacting with the VSCode debug plugin
#[derive(Clone)]
pub struct DebugClient {
    /// URL for the local instance of the debugger server
    debugger_url: String,

    /// reqwest::Client to avoid re-creating a client for every request.
    client: reqwest::Client,
}

impl DebugClient {
    /// Create a new instance of a DebugClient with the specified url.
    pub fn new(url: String) -> DebugClient {
        DebugClient {
            debugger_url: url,
            client: reqwest::Client::new(),
        }
    }

    /// Get the history from the instance of the debug plugin server
    pub async fn get_history(&self) -> Result<History, anyhow::Error> {
        let url = self.debugger_url.clone() + "/history";
        let resp = self
            .client
            .get(url)
            .header("Temporal-Client-Name", CLIENT_NAME)
            .header("Temporal-Client-Version", CLIENT_VERSION)
            .send()
            .await?;

        let bytes = resp.bytes().await?;
        Ok(History::decode(bytes)?)
    }

    /// Post to current-wft-started to communicate with debug plugin server
    pub async fn post_wft_started(
        &self,
        event_id: &i64,
    ) -> Result<reqwest::Response, anyhow::Error> {
        let url = self.debugger_url.clone() + "/current-wft-started";
        Ok(self
            .client
            .get(url)
            .header("Temporal-Client-Name", CLIENT_NAME)
            .header("Temporal-Client-Version", CLIENT_VERSION)
            .timeout(Duration::from_secs(5))
            .json(event_id)
            .send()
            .await?)
    }
}

#[cfg(test)]
mod tests {
    // this test wont work without a local instance of the debugger running.
    // #[tokio::test]
    // async fn test_debug_client_delete() {
    //     let dc = DebugClient::new("http://127.0.0.1:51563".into());
    //     dc.get_history().await.unwrap();
    // }
}
