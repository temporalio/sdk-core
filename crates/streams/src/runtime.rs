//! The Stream runtime, a plain object that lang's bridge builds per client and calls by name.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CloseWriterRequest, CloseWriterResponse, CloseWritersRequest, CloseWritersResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        FreeWriterRequest, FreeWriterResponse, MemoryStreamConfig, OpenWriterRequest,
        OpenWriterResponse, ReadRecordsRequest, ReadRecordsResponse, WriteRecordsRequest,
        WriteRecordsResponse,
    },
    store::Store,
};
use prost::Message;
use std::sync::Arc;
use temporalio_common::telemetry::metrics::TemporalMeter;

/// Connects the Store of a runtime.
///
/// The runtime connects on its first call that needs the Store. So a bridge can build the runtime
/// with no async runtime, and a Workflow that never publishes never connects. A failed connect is
/// tried again on the next call.
#[async_trait::async_trait]
pub trait StoreFactory: Send + Sync + 'static {
    /// Connects a Store.
    async fn connect(&self) -> StreamResult<Arc<dyn Store>>;
}

/// A Store that is already connected.
struct Connected(Arc<dyn Store>);

#[async_trait::async_trait]
impl StoreFactory for Connected {
    async fn connect(&self) -> StreamResult<Arc<dyn Store>> {
        Ok(self.0.clone())
    }
}

/// The state that the runtime's calls share.
pub(crate) struct Shared {
    name: &'static str,
    backend: String,
}

/// The Stream runtime on one Store.
///
/// A bridge builds one for each client with a Stream backend, and that client's Workers share it.
/// A Store crate, such as `temporalio-streams-redis`, builds it with its own Store.
pub struct StreamRuntime {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("name", &self.shared.name)
            .field("backend", &self.shared.backend)
            .finish_non_exhaustive()
    }
}

impl StreamRuntime {
    /// Builds a runtime. `name` identifies the runtime in logs. `backend` is the name that lang
    /// registered the runtime under. `factory` connects the Store, and `meter` is lang's runtime
    /// meter, if set. No call of this runtime uses the Store or records a metric, so the runtime
    /// keeps neither.
    pub fn new(
        name: &'static str,
        backend: impl Into<String>,
        factory: Arc<dyn StoreFactory>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        let _ = (factory, meter);
        let shared = Arc::new(Shared {
            name,
            backend: backend.into(),
        });
        Arc::new(Self { shared })
    }

    /// Builds a runtime on a Store that is already connected.
    pub fn with_store(
        name: &'static str,
        backend: impl Into<String>,
        store: Arc<dyn Store>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        Self::new(name, backend, Arc::new(Connected(store)), meter)
    }

    /// A name that identifies the runtime in logs.
    pub fn name(&self) -> &str {
        self.shared.name
    }

    /// Releases one reference to a writer with no wait, as the `FreeWriter` method does. A bridge
    /// calls it from a finalizer, which has no async runtime to await on. The runtime opens no
    /// writer, so the call fails with [Reason::Unsupported].
    pub fn free_writer(&self, writer_id: u64) -> StreamResult<()> {
        let _ = writer_id;
        Err(not_implemented())
    }

    /// Runs one method on encoded input and returns the encoded output. The bridge raises an error
    /// with [StreamError::code] and [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        // `FreeWriter` needs no Store, so it works when the Store is down too.
        if method == "FreeWriter" {
            let request = decode::<FreeWriterRequest>(input)?;
            self.free_writer(request.writer_id)?;
            return Ok(FreeWriterResponse {}.encode_to_vec());
        }
        let output = match method {
            "CreateStream" => refuse::<CreateStreamRequest, CreateStreamResponse>(input)?,
            "AppendRecords" => refuse::<AppendRecordsRequest, AppendRecordsResponse>(input)?,
            "CloseStream" => refuse::<CloseStreamRequest, CloseStreamResponse>(input)?,
            "ReadRecords" => refuse::<ReadRecordsRequest, ReadRecordsResponse>(input)?,
            "DescribeStream" => refuse::<DescribeStreamRequest, DescribeStreamResponse>(input)?,
            "OpenWriter" => refuse::<OpenWriterRequest, OpenWriterResponse>(input)?,
            "WriteRecords" => refuse::<WriteRecordsRequest, WriteRecordsResponse>(input)?,
            "CloseWriters" => refuse::<CloseWritersRequest, CloseWritersResponse>(input)?,
            "CloseWriter" => refuse::<CloseWriterRequest, CloseWriterResponse>(input)?,
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }
}

/// The name that [StreamRuntime::name] gives for a memory runtime.
const MEMORY_RUNTIME_NAME: &str = "temporalio-streams-memory";

/// The Stream runtime for tests.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Takes an encoded [MemoryStreamConfig]. Its field numbers are those of the same fields in
    /// the config of another Store, such as Redis, so a backend can pass that config too. A bad
    /// config fails with [Reason::Invalid]. The crate has no memory Store, so a good config fails
    /// with [Reason::Unsupported].
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> StreamResult<Arc<StreamRuntime>> {
        MemoryStreamConfig::decode(config)
            .map_err(|error| StreamError::invalid(format!("The config is bad: {error}")))?;
        let _ = meter;
        Err(not_implemented())
    }

    /// Builds a runtime on any Store, for tests of a Store that lacks some capabilities.
    #[doc(hidden)]
    pub fn with_store(store: Arc<dyn Store>, meter: Option<TemporalMeter>) -> Arc<StreamRuntime> {
        StreamRuntime::with_store(MEMORY_RUNTIME_NAME, String::new(), store, meter)
    }
}

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

fn not_implemented() -> StreamError {
    StreamError::new(Reason::Unsupported, "Not implemented")
}

/// Decodes the request of a method that the runtime doesn't serve, and refuses the call. `Resp` is
/// the response message of the method.
fn refuse<Req: Message + Default, Resp: Message>(input: &[u8]) -> StreamResult<Vec<u8>> {
    decode::<Req>(input)?;
    let response: StreamResult<Resp> = Err(not_implemented());
    response.map(|response| response.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A factory whose connect fails, so a call that needs the Store would fail too.
    struct NoStore;

    #[async_trait::async_trait]
    impl StoreFactory for NoStore {
        async fn connect(&self) -> StreamResult<Arc<dyn Store>> {
            Err(StreamError::new(Reason::StorageUnavailable, "No Store"))
        }
    }

    const METHODS: [&str; 10] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
        "OpenWriter",
        "WriteRecords",
        "CloseWriters",
        "CloseWriter",
        "FreeWriter",
    ];

    fn runtime() -> Arc<StreamRuntime> {
        StreamRuntime::new("test", "", Arc::new(NoStore), None)
    }

    #[tokio::test]
    async fn each_method_decodes_its_request_and_is_unsupported() {
        let runtime = runtime();
        for method in METHODS {
            // Empty bytes are the default request of every method.
            let error = runtime.call(method, &[]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Unsupported, "{method}");
            assert_eq!(error.message, "Not implemented", "{method}");
        }
        let error = runtime.call("Nope", &[]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert_eq!(error.message, "Unknown method Nope");
    }

    #[tokio::test]
    async fn a_bad_request_is_invalid() {
        let runtime = runtime();
        for method in METHODS {
            let error = runtime.call(method, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{method}");
        }
    }

    #[test]
    fn free_writer_and_the_memory_runtime_are_unsupported() {
        let error = runtime().free_writer(1).unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        let config = MemoryStreamConfig::default().encode_to_vec();
        let error = MemoryStreamRuntime::from_config_bytes(&config, None).unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        let error = MemoryStreamRuntime::from_config_bytes(&[0xff], None).unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
    }
}
