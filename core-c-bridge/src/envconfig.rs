use crate::{ByteArray, ByteArrayRef};
use serde::Serialize;
use std::collections::HashMap;
use std::ffi::CStr;
use temporal_sdk_core_api::envconfig::{
    self, ClientConfig as CoreClientConfig, ClientConfigCodec as CoreClientConfigCodec,
    ClientConfigProfile as CoreClientConfigProfile, ClientConfigTLS as CoreClientConfigTLS,
    DataSource as CoreDataSource, LoadClientConfigOptions, LoadClientConfigProfileOptions,
};

/// Callback for client config load operations.
/// If success or fail are not null, they must be manually freed when done.
pub type ClientConfigCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *const ByteArray,
    fail: *const ByteArray,
);

// Wrapper types for JSON serialization
#[derive(Serialize)]
struct ClientConfig {
    profiles: HashMap<String, ClientConfigProfile>,
}

impl From<CoreClientConfig> for ClientConfig {
    fn from(c: CoreClientConfig) -> Self {
        Self {
            profiles: c.profiles.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

#[derive(Serialize)]
struct ClientConfigProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<ClientConfigTLS>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codec: Option<ClientConfigCodec>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    grpc_meta: HashMap<String, String>,
}

impl From<CoreClientConfigProfile> for ClientConfigProfile {
    fn from(c: CoreClientConfigProfile) -> Self {
        Self {
            address: c.address,
            namespace: c.namespace,
            api_key: c.api_key,
            tls: c.tls.map(Into::into),
            codec: c.codec.map(Into::into),
            grpc_meta: c.grpc_meta,
        }
    }
}

#[derive(Serialize)]
struct ClientConfigTLS {
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_ca_cert: Option<DataSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_cert: Option<DataSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_key: Option<DataSource>,
}

impl From<CoreClientConfigTLS> for ClientConfigTLS {
    fn from(c: CoreClientConfigTLS) -> Self {
        Self {
            disabled: c.disabled,
            server_name: c.server_name,
            server_ca_cert: c.server_ca_cert.map(Into::into),
            client_cert: c.client_cert.map(Into::into),
            client_key: c.client_key.map(Into::into),
        }
    }
}

#[derive(Serialize)]
struct ClientConfigCodec {
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
}

impl From<CoreClientConfigCodec> for ClientConfigCodec {
    fn from(c: CoreClientConfigCodec) -> Self {
        Self {
            endpoint: c.endpoint,
            auth: c.auth,
        }
    }
}

#[derive(Serialize)]
struct DataSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
}

impl From<CoreDataSource> for DataSource {
    fn from(c: CoreDataSource) -> Self {
        match c {
            CoreDataSource::Path(p) => Self {
                path: Some(p),
                data: None,
            },
            CoreDataSource::Data(d) => Self {
                path: None,
                data: Some(String::from_utf8_lossy(&d).to_string()),
            },
        }
    }
}

// Helper functions
fn parse_config_source(
    path: *const libc::c_char,
    data: ByteArrayRef,
) -> Result<Option<CoreDataSource>, String> {
    if !path.is_null() {
        match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(path_str) => Ok(Some(CoreDataSource::Path(path_str.to_string()))),
            Err(e) => Err(format!("Invalid path UTF-8: {e}")),
        }
    } else if !data.data.is_null() && data.size > 0 {
        Ok(Some(CoreDataSource::Data(data.to_vec())))
    } else {
        Ok(None)
    }
}

fn parse_env_vars(env_vars: ByteArrayRef) -> Result<Option<HashMap<String, String>>, String> {
    if env_vars.data.is_null() || env_vars.size == 0 {
        return Ok(None);
    }

    let env_json = std::str::from_utf8(env_vars.to_slice())
        .map_err(|e| format!("Invalid env vars UTF-8: {e}"))?;

    serde_json::from_str(env_json)
        .map(Some)
        .map_err(|e| format!("Invalid env vars JSON: {e}"))
}

fn send_result<T: Serialize>(
    result: Result<T, String>,
    user_data: *mut libc::c_void,
    callback: ClientConfigCallback,
) {
    match result {
        Ok(data) => match serde_json::to_vec(&data) {
            Ok(json_bytes) => {
                let result = ByteArray::from_vec(json_bytes);
                unsafe { callback(user_data, result.into_raw(), std::ptr::null()) };
            }
            Err(e) => {
                let err = ByteArray::from_utf8(format!("Failed to serialize: {e}"));
                unsafe { callback(user_data, std::ptr::null(), err.into_raw()) };
            }
        },
        Err(e) => {
            let err = ByteArray::from_utf8(e);
            unsafe { callback(user_data, std::ptr::null(), err.into_raw()) };
        }
    }
}

/// Load all client profiles from given sources
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_config_load(
    path: *const libc::c_char,
    data: ByteArrayRef,
    disable_file: bool,
    config_file_strict: bool,
    env_vars: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: ClientConfigCallback,
) {
    let result = || -> Result<ClientConfig, String> {
        let config_source = parse_config_source(path, data)?;
        let env_vars_map = parse_env_vars(env_vars)?;

        let options = LoadClientConfigOptions {
            config_source: if disable_file { None } else { config_source },
            config_file_strict,
        };

        let core_config = envconfig::load_client_config(options, env_vars_map.as_ref())
            .map_err(|e| e.to_string())?;

        Ok(core_config.into())
    };

    send_result(result(), user_data, callback);
}

/// Load a single client profile from given sources with env overrides
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_config_profile_load(
    profile: *const libc::c_char,
    path: *const libc::c_char,
    data: ByteArrayRef,
    disable_file: bool,
    disable_env: bool,
    config_file_strict: bool,
    env_vars: ByteArrayRef,
    user_data: *mut libc::c_void,
    callback: ClientConfigCallback,
) {
    let result = || -> Result<ClientConfigProfile, String> {
        let profile_name = if !profile.is_null() {
            match unsafe { CStr::from_ptr(profile) }.to_str() {
                Ok(s) => Some(s.to_string()),
                Err(e) => return Err(format!("Invalid profile UTF-8: {e}")),
            }
        } else {
            None
        };

        let config_source = parse_config_source(path, data)?;
        let env_vars_map = parse_env_vars(env_vars)?;

        let options = LoadClientConfigProfileOptions {
            config_source,
            config_file_profile: profile_name,
            config_file_strict,
            disable_file,
            disable_env,
        };

        let profile = envconfig::load_client_config_profile(options, env_vars_map.as_ref())
            .map_err(|e| e.to_string())?;

        Ok(profile.into())
    };

    send_result(result(), user_data, callback);
}
