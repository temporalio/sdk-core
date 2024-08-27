use reqwest;
use prost::Message;
use temporal_sdk_core_protos::temporal::api::history::v1::History;
use std::time::Duration;

struct DebugClient {
    debugger_url: String,
    _client: reqwest::Client
}

impl DebugClient {

    fn new(url: String) -> DebugClient {
        DebugClient {
            debugger_url: url,
            _client: reqwest::Client::new(),
        }
    }

    async fn get_history(&self) -> Result<History, anyhow::Error> {
        let url = self.debugger_url.clone() + "/history";
        let resp = self._client.get(url)
            .header("Temporal-Client-Name", "temporal-core")
            .header("Temporal-Client-Version", "0.1.0")
            .send()
            .await?;

        let bytes = resp.bytes().await?;
        Ok(History::decode(bytes)?) // decode_length_delimited() does not work

    }

    async fn post_wft_started(&self, event_id: &i64) -> bool {
        let url = self.debugger_url.clone() + "/current-wft-started";
        let resp = self._client.get(url)
            .header("Temporal-Client-Name", "temporal-debug-core")
            .header("Temporal-Client-Version", "0.1.0")
            .timeout(Duration::from_secs(5))
            .json(event_id)
            .send()
            .await;

        match resp {
            Ok(r) => { r.status() == reqwest::StatusCode::OK },
            Err(_) => false
        }
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