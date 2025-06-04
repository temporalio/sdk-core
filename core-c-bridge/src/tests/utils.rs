use crate::client::RpcService;
use crate::runtime::{Runtime, temporal_core_byte_array_free};
use crate::{ByteArray, CancellationToken};
use std::collections::HashMap;
use std::ops::Deref;
use temporal_client::{ClientOptions, ClientOptionsBuilder};
use temporal_sdk_core::ephemeral_server::{
    TemporalDevServerConfig, TemporalDevServerConfigBuilder,
};
use temporal_sdk_core_test_utils::default_cached_download;
use url::Url;

pub fn byte_array_to_vec(runtime: *mut Runtime, byte_array: *const ByteArray) -> Option<Vec<u8>> {
    (!byte_array.is_null()).then(|| {
        let ret = unsafe { &*byte_array }.as_ref().to_vec();
        temporal_core_byte_array_free(runtime, byte_array);
        ret
    })
}

pub fn byte_array_to_string(runtime: *mut Runtime, byte_array: *const ByteArray) -> Option<String> {
    (!byte_array.is_null()).then(|| {
        let ret = unsafe { &*byte_array }.as_ref().to_string();
        temporal_core_byte_array_free(runtime, byte_array);
        ret
    })
}

pub fn pointer_or_null<T>(x: Option<impl Deref<Target = T>>) -> *const T {
    x.map(|x| &*x as *const T).unwrap_or(std::ptr::null())
}

pub fn default_server_config() -> TemporalDevServerConfig {
    TemporalDevServerConfigBuilder::default()
        .exe(default_cached_download())
        .build()
        .unwrap()
}

pub fn default_client_options(target: &str) -> ClientOptions {
    ClientOptionsBuilder::default()
        .target_url(Url::parse(&format!("http://{target}")).unwrap())
        .client_name("core-c-bridge-tests".to_owned())
        .client_version("0.1.0".to_owned())
        .build()
        .unwrap()
}

pub struct OwnedRpcCallOptions {
    pub service: RpcService,
    pub rpc: String,
    pub req: Vec<u8>,
    pub retry: bool,
    pub metadata: Option<MetadataMap>,
    pub timeout_millis: u32,
    pub cancellation_token: Option<*mut CancellationToken>,
}

unsafe impl Send for OwnedRpcCallOptions {}
unsafe impl Sync for OwnedRpcCallOptions {}

#[derive(Clone, Debug)]
pub enum MetadataMap {
    Deserialized(HashMap<String, String>),
    Serialized(String),
}

impl From<HashMap<String, String>> for MetadataMap {
    fn from(value: HashMap<String, String>) -> Self {
        Self::Deserialized(value)
    }
}

#[allow(dead_code)]
impl MetadataMap {
    pub fn serialize_from_map(map: &HashMap<String, String>) -> String {
        map.iter().map(|(k, v)| format!("{k}\n{v}\n")).collect()
    }

    pub fn serialize(&self) -> String {
        match self {
            Self::Deserialized(map) => Self::serialize_from_map(map),
            Self::Serialized(s) => s.clone(),
        }
    }

    pub fn serialize_in_place(&mut self) {
        if let Self::Deserialized(map) = self {
            *self = Self::Serialized(Self::serialize_from_map(map));
        }
    }

    pub fn as_str(&mut self) -> &str {
        self.serialize_in_place();
        let Self::Serialized(s) = self else {
            unreachable!();
        };
        s
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{message} (status code {status_code})")]
pub struct RpcCallError {
    pub status_code: u32,
    pub message: String,
    pub details: Option<Vec<u8>>,
}
