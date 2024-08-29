use prost::Message;
use reqwest;
use std::time::Duration;
use temporal_sdk_core_protos::temporal::api::history::v1::History;

const CLIENT_NAME: &str = "temporal-core";
const CLIENT_VERSION: &str = "0.1.0";

struct DebugClient {
    debugger_url: String,
    client: reqwest::Client,
}

impl DebugClient {
    fn new(url: String) -> DebugClient {
        DebugClient {
            debugger_url: url,
            client: reqwest::Client::new(),
        }
    }

    async fn get_history(&self) -> Result<History, anyhow::Error> {
        let url = self.debugger_url.clone() + "/history";
        let resp = self
            .client
            .get(url)
            .header("Temporal-Client-Name", CLIENT_NAME)
            .header("Temporal-Client-Version", CLIENT_VERSION)
            .send()
            .await?;

        let bytes = resp.bytes().await?;
        Ok(History::decode(bytes)?) // decode_length_delimited() does not work
    }

    async fn post_wft_started(&self, event_id: &i64) -> Result<Response, anyhow::Error> {
        let url = self.debugger_url.clone() + "/current-wft-started";
        self.client
            .get(url)
            .header("Temporal-Client-Name", CLIENT_NAME)
            .header("Temporal-Client-Version", CLIENT_VERSION)
            .timeout(Duration::from_secs(5))
            .json(event_id)
            .send()
            .await?
    }

    // debating whether this is necessary at all
    fn content_length(response: &reqwest::Response) -> Result<u64, String> {
        match response.content_length() {
            Some(length) => Ok(length),
            None => Err("Content length unavailable. The header is unavailable or the response was gzipped.".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_debug_client_delete() {
        let dc = DebugClient::new("http://127.0.0.1:51563".into());
        dc.get_history().await.unwrap();
    }
}
