//! Environment and file-based configuration for Temporal clients.
//!
//! This module provides utilities to load Temporal client configuration from TOML files
//! and environment variables. The configuration supports multiple profiles and various
//! connection settings including TLS, authentication, and codec configuration.
//!
//! ## Environment Variables
//!
//! The following environment variables are supported:
//! - `TEMPORAL_CONFIG_FILE`: Path to the TOML configuration file
//! - `TEMPORAL_PROFILE`: Profile name to use from the configuration file  
//! - `TEMPORAL_ADDRESS`: Temporal server address
//! - `TEMPORAL_NAMESPACE`: Temporal namespace
//! - `TEMPORAL_API_KEY`: API key for authentication
//! - `TEMPORAL_TLS`: Enable/disable TLS (true/false)
//! - `TEMPORAL_TLS_*`: Various TLS configuration options
//! - `TEMPORAL_CODEC_*`: Codec configuration options
//! - `TEMPORAL_GRPC_META_*`: gRPC metadata headers
//!
//! ## TOML Configuration Format
//!
//! ```toml
//! [profile.default]
//! address = "localhost:7233"
//! namespace = "default"
//! api_key = "your-api-key"
//!
//! [profile.default.tls]
//! disabled = false
//! client_cert_path = "/path/to/cert.pem"
//! client_key_path = "/path/to/key.pem"
//!
//! [profile.default.codec]
//! endpoint = "http://localhost:8080"
//! auth = "Bearer token"
//!
//! [profile.default.grpc_meta]
//! custom_header = "value"
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// Default profile name when none is specified
pub const DEFAULT_PROFILE: &str = "default";

/// Default configuration file name
pub const DEFAULT_CONFIG_FILE: &str = "temporal.toml";

/// Errors that can occur during configuration loading
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parsing error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("Invalid UTF-8 data: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("Profile '{0}' not found")]
    ProfileNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Environment variable error: {0}")]
    EnvVar(String),
}

/// ClientConfig represents a client config file.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfig {
    /// Profiles, keyed by profile name
    pub profiles: HashMap<String, ClientConfigProfile>,
}

/// ClientConfigProfile is profile-level configuration for a client.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigProfile {
    /// Client address
    pub address: Option<String>,

    /// Client namespace
    pub namespace: Option<String>,

    /// Client API key. If present and TLS field is None or present and not disabled (i.e. without Disabled as true),
    /// TLS is defaulted to enabled.
    pub api_key: Option<String>,

    /// Optional client TLS config.
    pub tls: Option<ClientConfigTLS>,

    /// Optional client codec config.
    pub codec: Option<ClientConfigCodec>,

    /// Client gRPC metadata (aka headers). When loading from TOML and env var, or writing to TOML, the keys are
    /// lowercased and underscores are replaced with hyphens. This is used for deduplicating/overriding too, so manually
    /// set values that are not normalized may not get overridden with [ClientConfigProfile::apply_env_vars].
    pub grpc_meta: HashMap<String, String>,
}

/// ClientConfigTLS is TLS configuration for a client.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigTLS {
    /// If true, TLS is explicitly disabled. If false/unset, whether TLS is enabled or not depends on other factors such
    /// as whether this struct is present or None, and whether API key exists (which enables TLS by default).
    pub disabled: bool,

    /// Path to client mTLS certificate. Mutually exclusive with ClientCertData.
    pub client_cert_path: Option<String>,

    /// PEM bytes for client mTLS certificate. Mutually exclusive with ClientCertPath.
    pub client_cert_data: Option<Vec<u8>>,

    /// Path to client mTLS key. Mutually exclusive with ClientKeyData.
    pub client_key_path: Option<String>,

    /// PEM bytes for client mTLS key. Mutually exclusive with ClientKeyPath.
    pub client_key_data: Option<Vec<u8>>,

    /// Path to server CA cert override
    pub server_ca_cert_path: Option<String>,

    /// PEM bytes for server CA cert override
    pub server_ca_cert_data: Option<Vec<u8>>,

    /// SNI override
    pub server_name: Option<String>,

    /// True if host verification should be skipped
    pub disable_host_verification: bool,
}

/// Codec configuration for a client
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigCodec {
    /// Remote endpoint for the codec
    pub endpoint: Option<String>,

    /// Auth for the codec
    pub auth: Option<String>,
}

/// Options for loading client configuration
#[derive(Debug)]
pub struct LoadClientConfigOptions<'a> {
    /// Override config file path
    pub config_file_path: Option<String>,

    /// TOML data to load directly (overrides file loading)
    pub config_file_data: Option<Vec<u8>>,

    /// If true, will error if there are unrecognized keys
    pub config_file_strict: bool,

    /// Environment variable lookup implementation
    pub env_lookup: Option<&'a dyn EnvLookup>,
}

/// Options for loading a client configuration profile
#[derive(Debug, Default)]
pub struct LoadClientConfigProfileOptions<'a> {
    /// Override config file path
    pub config_file_path: Option<String>,

    /// TOML data to load directly (overrides file loading)
    pub config_file_data: Option<Vec<u8>>,

    /// Specific profile to use
    pub config_file_profile: Option<String>,

    /// If true, will error if there are unrecognized keys.
    pub config_file_strict: bool,

    /// Disable loading from file
    pub disable_file: bool,

    /// Disable loading from environment variables
    pub disable_env: bool,

    /// Environment variable lookup implementation
    pub env_lookup: Option<&'a dyn EnvLookup>,
}

/// Options for parsing TOML configuration
#[derive(Debug, Default)]
pub struct ClientConfigFromTOMLOptions {
    /// If true, will error if there are unrecognized keys.
    pub strict: bool,
}

/// Trait for environment variable lookup
pub trait EnvLookup: std::fmt::Debug {
    /// Get all environment variables (like `std::env::vars()`)
    fn environ(&self) -> Vec<(String, String)>;

    /// Look up a single environment variable (like `std::env::var()`)
    fn lookup_env(&self, key: &str) -> Result<String, std::env::VarError>;
}

/// Default implementation using actual OS environment
#[derive(Debug, Default)]
pub struct OsEnvLookup;

impl EnvLookup for OsEnvLookup {
    fn environ(&self) -> Vec<(String, String)> {
        std::env::vars().collect()
    }

    fn lookup_env(&self, key: &str) -> Result<String, std::env::VarError> {
        std::env::var(key)
    }
}

/// Load client configuration from TOML. Does not load values from environment variables
/// (but may use environment variables to get which config file to load). This will not fail
/// if the file does not exist.
pub fn load_client_config(options: LoadClientConfigOptions) -> Result<ClientConfig, ConfigError> {
    let conf = ClientConfig::default();

    // Get which bytes to load from TOML
    let data = if let Some(ref data) = options.config_file_data {
        if options.config_file_path.is_some() {
            return Err(ConfigError::InvalidConfig(
                "cannot have data and file path".to_string(),
            ));
        }
        data.clone()
    } else {
        // Determine file path
        let file_path = match options.config_file_path {
            Some(path) => path,
            None => {
                let env_lookup = options.env_lookup.unwrap_or(&OsEnvLookup);
                match env_lookup.lookup_env("TEMPORAL_CONFIG_FILE") {
                    Ok(path) if !path.is_empty() => path,
                    _ => get_default_config_file_path()?,
                }
            }
        };

        // If file doesn't exist, return empty config
        if !Path::new(&file_path).exists() {
            return Ok(conf);
        }

        match fs::read(&file_path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(conf),
            Err(e) => return Err(ConfigError::Io(e)),
        }
    };

    ClientConfig::from_toml(
        &data,
        ClientConfigFromTOMLOptions {
            strict: options.config_file_strict,
        },
    )
}

/// Load a specific client configuration profile
pub fn load_client_config_profile(
    options: LoadClientConfigProfileOptions,
) -> Result<ClientConfigProfile, ConfigError> {
    if options.disable_file && options.disable_env {
        return Err(ConfigError::InvalidConfig(
            "Cannot disable both file and environment loading".to_string(),
        ));
    }

    let mut profile = if options.disable_file {
        ClientConfigProfile::default()
    } else {
        // Load the full config
        let config = load_client_config(LoadClientConfigOptions {
            config_file_path: options.config_file_path.clone(),
            config_file_data: options.config_file_data.clone(),
            config_file_strict: options.config_file_strict,
            env_lookup: options.env_lookup,
        })?;

        // Determine profile name
        let (profile_name, profile_unset) = match &options.config_file_profile {
            Some(profile) => (profile.clone(), false),
            None => {
                let env_lookup = options.env_lookup.unwrap_or(&OsEnvLookup);
                match env_lookup.lookup_env("TEMPORAL_PROFILE") {
                    Ok(profile) if !profile.is_empty() => (profile, false),
                    _ => (DEFAULT_PROFILE.to_string(), true),
                }
            }
        };

        if let Some(prof) = config.profiles.get(&profile_name) {
            prof.clone()
        } else if !profile_unset {
            return Err(ConfigError::ProfileNotFound(profile_name));
        } else {
            ClientConfigProfile::default()
        }
    };

    // Apply environment variables if not disabled
    if !options.disable_env {
        if let Some(env_lookup) = options.env_lookup {
            profile.apply_env_vars_with_lookup(env_lookup)?;
        } else {
            profile.apply_env_vars()?;
        }
    }

    // Apply API key → TLS auto-enabling logic
    profile.apply_api_key_tls_logic();

    Ok(profile)
}

impl ClientConfig {
    /// Load configuration from a TOML string with options. This will replace all profiles within,
    /// it does not do any form of merging.
    pub fn from_toml(
        toml_bytes: &[u8],
        options: ClientConfigFromTOMLOptions,
    ) -> Result<Self, ConfigError> {
        let toml_str = std::str::from_utf8(toml_bytes)?;
        let mut conf = ClientConfig::default();
        if toml_str.trim().is_empty() {
            return Ok(conf);
        }

        if options.strict {
            let toml_conf: internal_toml::strict::StrictTomlClientConfig =
                toml::from_str(toml_str)?;
            toml_conf.apply_to_client_config(&mut conf);
        } else {
            let toml_conf: internal_toml::TomlClientConfig = toml::from_str(toml_str)?;
            toml_conf.apply_to_client_config(&mut conf);
        }
        Ok(conf)
    }

    /// Convert configuration to TOML string.
    pub fn to_toml(&self) -> Result<Vec<u8>, ConfigError> {
        let mut toml_conf = internal_toml::TomlClientConfig::new();
        toml_conf.populate_from_client_config(self);
        Ok(toml::to_string_pretty(&toml_conf)?.into_bytes())
    }
}

impl ClientConfigProfile {
    /// Apply environment variable overrides to this profile
    pub fn apply_env_vars_with_lookup(
        &mut self,
        env_lookup: &dyn EnvLookup,
    ) -> Result<(), ConfigError> {
        // Apply basic settings
        if let Ok(address) = env_lookup.lookup_env("TEMPORAL_ADDRESS") {
            self.address = Some(address);
        }

        if let Ok(namespace) = env_lookup.lookup_env("TEMPORAL_NAMESPACE") {
            self.namespace = Some(namespace);
        }

        if let Ok(api_key) = env_lookup.lookup_env("TEMPORAL_API_KEY") {
            self.api_key = Some(api_key);
        }

        // Apply TLS settings
        self.apply_tls_env_vars_with_lookup(env_lookup)?;

        // Apply codec settings
        self.apply_codec_env_vars_with_lookup(env_lookup)?;

        // Apply gRPC metadata
        self.apply_grpc_meta_env_vars_with_lookup(env_lookup)?;

        Ok(())
    }

    /// Apply environment variable overrides using OS environment
    pub fn apply_env_vars(&mut self) -> Result<(), ConfigError> {
        let env_lookup = OsEnvLookup;
        self.apply_env_vars_with_lookup(&env_lookup)
    }

    fn apply_tls_env_vars_with_lookup(
        &mut self,
        env_lookup: &dyn EnvLookup,
    ) -> Result<(), ConfigError> {
        const TLS_ENV_VARS: &[&str] = &[
            "TEMPORAL_TLS",
            "TEMPORAL_TLS_CLIENT_CERT_PATH",
            "TEMPORAL_TLS_CLIENT_CERT_DATA",
            "TEMPORAL_TLS_CLIENT_KEY_PATH",
            "TEMPORAL_TLS_CLIENT_KEY_DATA",
            "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
            "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
            "TEMPORAL_TLS_SERVER_NAME",
            "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION",
        ];

        if TLS_ENV_VARS
            .iter()
            .any(|k| env_lookup.lookup_env(k).is_ok())
            && self.tls.is_none()
        {
            self.tls = Some(ClientConfigTLS::default());
        }

        if let Some(ref mut tls) = self.tls {
            if let Ok(disabled_str) = env_lookup.lookup_env("TEMPORAL_TLS") {
                if let Some(disabled) = env_var_to_bool(&disabled_str) {
                    tls.disabled = !disabled;
                }
            }

            if let Ok(cert_path) = env_lookup.lookup_env("TEMPORAL_TLS_CLIENT_CERT_PATH") {
                tls.client_cert_path = Some(cert_path);
            }

            if let Ok(cert_data) = env_lookup.lookup_env("TEMPORAL_TLS_CLIENT_CERT_DATA") {
                tls.client_cert_data = Some(cert_data.into_bytes());
            }

            if let Ok(key_path) = env_lookup.lookup_env("TEMPORAL_TLS_CLIENT_KEY_PATH") {
                tls.client_key_path = Some(key_path);
            }

            if let Ok(key_data) = env_lookup.lookup_env("TEMPORAL_TLS_CLIENT_KEY_DATA") {
                tls.client_key_data = Some(key_data.into_bytes());
            }

            if let Ok(ca_path) = env_lookup.lookup_env("TEMPORAL_TLS_SERVER_CA_CERT_PATH") {
                tls.server_ca_cert_path = Some(ca_path);
            }

            if let Ok(ca_data) = env_lookup.lookup_env("TEMPORAL_TLS_SERVER_CA_CERT_DATA") {
                tls.server_ca_cert_data = Some(ca_data.into_bytes());
            }

            if let Ok(server_name) = env_lookup.lookup_env("TEMPORAL_TLS_SERVER_NAME") {
                tls.server_name = Some(server_name);
            }

            if let Ok(disable_verification_str) =
                env_lookup.lookup_env("TEMPORAL_TLS_DISABLE_HOST_VERIFICATION")
            {
                if let Some(disable_verification) = env_var_to_bool(&disable_verification_str) {
                    tls.disable_host_verification = disable_verification;
                }
            }
        }

        Ok(())
    }

    fn apply_codec_env_vars_with_lookup(
        &mut self,
        env_lookup: &dyn EnvLookup,
    ) -> Result<(), ConfigError> {
        const CODEC_ENV_VARS: &[&str] = &["TEMPORAL_CODEC_ENDPOINT", "TEMPORAL_CODEC_AUTH"];
        if CODEC_ENV_VARS
            .iter()
            .any(|k| env_lookup.lookup_env(k).is_ok())
            && self.codec.is_none()
        {
            self.codec = Some(ClientConfigCodec::default());
        }

        if let Some(ref mut codec) = self.codec {
            if let Ok(endpoint) = env_lookup.lookup_env("TEMPORAL_CODEC_ENDPOINT") {
                codec.endpoint = Some(endpoint);
            }

            if let Ok(auth) = env_lookup.lookup_env("TEMPORAL_CODEC_AUTH") {
                codec.auth = Some(auth);
            }
        }

        Ok(())
    }

    fn apply_grpc_meta_env_vars_with_lookup(
        &mut self,
        env_lookup: &dyn EnvLookup,
    ) -> Result<(), ConfigError> {
        // Look for TEMPORAL_GRPC_META_* environment variables
        for (key, value) in env_lookup.environ() {
            if let Some(header_name) = key.strip_prefix("TEMPORAL_GRPC_META_") {
                let normalized_name = normalize_grpc_meta_key(header_name);
                if value.is_empty() {
                    // Empty value removes the header
                    self.grpc_meta.remove(&normalized_name);
                } else {
                    self.grpc_meta.insert(normalized_name, value);
                }
            }
        }

        Ok(())
    }

    /// Apply automatic TLS enabling when API key is present
    pub fn apply_api_key_tls_logic(&mut self) {
        if self.api_key.is_some() && self.tls.is_none() {
            // If API key is present but no TLS config exists, create one with TLS enabled
            self.tls = Some(ClientConfigTLS::default());
        }
    }
}

/// Parse a boolean value from string (supports "true", "false", "1", "0")
fn env_var_to_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Normalize gRPC metadata key (lowercase and replace underscores with hyphens)
fn normalize_grpc_meta_key(key: &str) -> String {
    key.to_lowercase().replace('_', "-")
}

/// Get the default configuration file path
fn get_default_config_file_path() -> Result<String, ConfigError> {
    // Try to get user config directory
    let config_dir = dirs::config_dir()
        .ok_or_else(|| ConfigError::InvalidConfig("failed getting user config dir".to_string()))?;

    let path = config_dir.join("temporalio").join(DEFAULT_CONFIG_FILE);
    Ok(path.to_string_lossy().to_string())
}

mod internal_toml {
    use super::{
        ClientConfig, ClientConfigCodec, ClientConfigProfile, ClientConfigTLS,
        normalize_grpc_meta_key,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// Helper function for serde to skip serializing false booleans
    fn is_false(b: &bool) -> bool {
        !b
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct TomlClientConfig {
        #[serde(default, rename = "profile")]
        profiles: HashMap<String, TomlClientConfigProfile>,
    }

    impl TomlClientConfig {
        pub(super) fn new() -> Self {
            Self {
                profiles: HashMap::new(),
            }
        }

        pub(super) fn apply_to_client_config(&self, conf: &mut ClientConfig) {
            conf.profiles = HashMap::with_capacity(self.profiles.len());
            for (k, v) in &self.profiles {
                conf.profiles.insert(k.clone(), v.to_client_config());
            }
        }

        pub(super) fn populate_from_client_config(&mut self, conf: &ClientConfig) {
            self.profiles = HashMap::with_capacity(conf.profiles.len());
            for (k, v) in &conf.profiles {
                let mut prof = TomlClientConfigProfile::new();
                prof.populate_from_client_config(v);
                self.profiles.insert(k.clone(), prof);
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct TomlClientConfigProfile {
        #[serde(skip_serializing_if = "Option::is_none")]
        address: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        tls: Option<TomlClientConfigTLS>,

        #[serde(skip_serializing_if = "Option::is_none")]
        codec: Option<TomlClientConfigCodec>,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        grpc_meta: HashMap<String, String>,
    }

    impl TomlClientConfigProfile {
        fn new() -> Self {
            Self {
                address: None,
                namespace: None,
                api_key: None,
                tls: None,
                codec: None,
                grpc_meta: HashMap::new(),
            }
        }
        fn to_client_config(&self) -> ClientConfigProfile {
            let mut ret = ClientConfigProfile {
                address: self.address.clone(),
                namespace: self.namespace.clone(),
                api_key: self.api_key.clone(),
                tls: self.tls.as_ref().map(|tls| tls.to_client_config()),
                codec: self.codec.as_ref().map(|codec| codec.to_client_config()),
                grpc_meta: HashMap::new(),
            };

            if !self.grpc_meta.is_empty() {
                ret.grpc_meta = HashMap::with_capacity(self.grpc_meta.len());
                for (k, v) in &self.grpc_meta {
                    ret.grpc_meta.insert(normalize_grpc_meta_key(k), v.clone());
                }
            }
            ret
        }

        fn populate_from_client_config(&mut self, conf: &ClientConfigProfile) {
            self.address = conf.address.clone();
            self.namespace = conf.namespace.clone();
            self.api_key = conf.api_key.clone();

            if let Some(ref tls_conf) = conf.tls {
                let mut toml_tls = TomlClientConfigTLS::new();
                toml_tls.populate_from_client_config(tls_conf);
                self.tls = Some(toml_tls);
            } else {
                self.tls = None;
            }

            if let Some(ref codec_conf) = conf.codec {
                let mut toml_codec = TomlClientConfigCodec::new();
                toml_codec.populate_from_client_config(codec_conf);
                self.codec = Some(toml_codec);
            } else {
                self.codec = None;
            }

            if !conf.grpc_meta.is_empty() {
                self.grpc_meta = HashMap::with_capacity(conf.grpc_meta.len());
                for (k, v) in &conf.grpc_meta {
                    self.grpc_meta.insert(normalize_grpc_meta_key(k), v.clone());
                }
            } else {
                self.grpc_meta.clear();
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct TomlClientConfigTLS {
        #[serde(default, skip_serializing_if = "is_false")]
        disabled: bool,

        #[serde(skip_serializing_if = "Option::is_none")]
        client_cert_path: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        client_cert_data: Option<String>, // String in TOML, not Vec<u8>

        #[serde(skip_serializing_if = "Option::is_none")]
        client_key_path: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        client_key_data: Option<String>, // String in TOML, not Vec<u8>

        #[serde(skip_serializing_if = "Option::is_none")]
        server_ca_cert_path: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        server_ca_cert_data: Option<String>, // String in TOML, not Vec<u8>

        #[serde(skip_serializing_if = "Option::is_none")]
        server_name: Option<String>,

        #[serde(default, skip_serializing_if = "is_false")]
        disable_host_verification: bool,
    }

    impl TomlClientConfigTLS {
        fn new() -> Self {
            Self {
                disabled: false,
                client_cert_path: None,
                client_cert_data: None,
                client_key_path: None,
                client_key_data: None,
                server_ca_cert_path: None,
                server_ca_cert_data: None,
                server_name: None,
                disable_host_verification: false,
            }
        }

        fn to_client_config(&self) -> ClientConfigTLS {
            let string_to_bytes = |s: &Option<String>| {
                s.as_ref().and_then(|val| {
                    if val.is_empty() {
                        None
                    } else {
                        Some(val.as_bytes().to_vec())
                    }
                })
            };

            ClientConfigTLS {
                disabled: self.disabled,
                client_cert_path: self.client_cert_path.clone(),
                client_cert_data: string_to_bytes(&self.client_cert_data),
                client_key_path: self.client_key_path.clone(),
                client_key_data: string_to_bytes(&self.client_key_data),
                server_ca_cert_path: self.server_ca_cert_path.clone(),
                server_ca_cert_data: string_to_bytes(&self.server_ca_cert_data),
                server_name: self.server_name.clone(),
                disable_host_verification: self.disable_host_verification,
            }
        }

        fn populate_from_client_config(&mut self, conf: &ClientConfigTLS) {
            self.disabled = conf.disabled;
            self.client_cert_path = conf.client_cert_path.clone();
            self.client_cert_data = conf
                .client_cert_data
                .as_ref()
                .map(|d| String::from_utf8_lossy(d).into_owned());
            self.client_key_path = conf.client_key_path.clone();
            self.client_key_data = conf
                .client_key_data
                .as_ref()
                .map(|d| String::from_utf8_lossy(d).into_owned());
            self.server_ca_cert_path = conf.server_ca_cert_path.clone();
            self.server_ca_cert_data = conf
                .server_ca_cert_data
                .as_ref()
                .map(|d| String::from_utf8_lossy(d).into_owned());
            self.server_name = conf.server_name.clone();
            self.disable_host_verification = conf.disable_host_verification;
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct TomlClientConfigCodec {
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        auth: Option<String>,
    }

    impl TomlClientConfigCodec {
        fn new() -> Self {
            Self {
                endpoint: None,
                auth: None,
            }
        }
        fn to_client_config(&self) -> ClientConfigCodec {
            ClientConfigCodec {
                endpoint: self.endpoint.clone(),
                auth: self.auth.clone(),
            }
        }

        fn populate_from_client_config(&mut self, conf: &ClientConfigCodec) {
            self.endpoint = conf.endpoint.clone();
            self.auth = conf.auth.clone();
        }
    }

    pub(super) mod strict {
        use super::{
            ClientConfig, ClientConfigCodec, ClientConfigProfile, ClientConfigTLS,
            normalize_grpc_meta_key,
        };
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(crate) struct StrictTomlClientConfig {
            #[serde(default, rename = "profile")]
            profiles: HashMap<String, StrictTomlClientConfigProfile>,
        }

        impl StrictTomlClientConfig {
            pub(crate) fn apply_to_client_config(self, conf: &mut ClientConfig) {
                conf.profiles = HashMap::with_capacity(self.profiles.len());
                for (k, v) in self.profiles {
                    conf.profiles.insert(k, v.into_client_config());
                }
            }
        }

        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct StrictTomlClientConfigProfile {
            #[serde(default)]
            address: Option<String>,
            #[serde(default)]
            namespace: Option<String>,
            #[serde(default)]
            api_key: Option<String>,
            #[serde(default)]
            tls: Option<StrictTomlClientConfigTLS>,
            #[serde(default)]
            codec: Option<StrictTomlClientConfigCodec>,
            #[serde(default)]
            grpc_meta: HashMap<String, String>,
        }

        impl StrictTomlClientConfigProfile {
            fn into_client_config(self) -> ClientConfigProfile {
                let mut ret = ClientConfigProfile {
                    address: self.address,
                    namespace: self.namespace,
                    api_key: self.api_key,
                    tls: self.tls.map(|tls| tls.into_client_config()),
                    codec: self.codec.map(|codec| codec.into_client_config()),
                    grpc_meta: HashMap::new(),
                };

                if !self.grpc_meta.is_empty() {
                    ret.grpc_meta = HashMap::with_capacity(self.grpc_meta.len());
                    for (k, v) in self.grpc_meta {
                        ret.grpc_meta.insert(normalize_grpc_meta_key(&k), v);
                    }
                }
                ret
            }
        }

        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct StrictTomlClientConfigTLS {
            #[serde(default)]
            disabled: bool,
            #[serde(default)]
            client_cert_path: Option<String>,
            #[serde(default)]
            client_cert_data: Option<String>,
            #[serde(default)]
            client_key_path: Option<String>,
            #[serde(default)]
            client_key_data: Option<String>,
            #[serde(default)]
            server_ca_cert_path: Option<String>,
            #[serde(default)]
            server_ca_cert_data: Option<String>,
            #[serde(default)]
            server_name: Option<String>,
            #[serde(default)]
            disable_host_verification: bool,
        }

        impl StrictTomlClientConfigTLS {
            fn into_client_config(self) -> ClientConfigTLS {
                let string_to_bytes = |s: Option<String>| {
                    s.and_then(|val| {
                        if val.is_empty() {
                            None
                        } else {
                            Some(val.as_bytes().to_vec())
                        }
                    })
                };

                ClientConfigTLS {
                    disabled: self.disabled,
                    client_cert_path: self.client_cert_path,
                    client_cert_data: string_to_bytes(self.client_cert_data),
                    client_key_path: self.client_key_path,
                    client_key_data: string_to_bytes(self.client_key_data),
                    server_ca_cert_path: self.server_ca_cert_path,
                    server_ca_cert_data: string_to_bytes(self.server_ca_cert_data),
                    server_name: self.server_name,
                    disable_host_verification: self.disable_host_verification,
                }
            }
        }

        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct StrictTomlClientConfigCodec {
            #[serde(default)]
            endpoint: Option<String>,
            #[serde(default)]
            auth: Option<String>,
        }

        impl StrictTomlClientConfigCodec {
            fn into_client_config(self) -> ClientConfigCodec {
                ClientConfigCodec {
                    endpoint: self.endpoint,
                    auth: self.auth,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Debug, Default)]
    struct MockEnvLookup {
        vars: std::collections::HashMap<String, String>,
    }

    impl MockEnvLookup {
        fn set(&mut self, key: &str, value: &str) -> &mut Self {
            self.vars.insert(key.to_string(), value.to_string());
            self
        }
    }

    impl EnvLookup for MockEnvLookup {
        fn environ(&self) -> Vec<(String, String)> {
            self.vars
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        fn lookup_env(&self, key: &str) -> Result<String, std::env::VarError> {
            self.vars
                .get(key)
                .cloned()
                .ok_or(std::env::VarError::NotPresent)
        }
    }

    #[test]
    fn test_client_config_toml_multiple_profiles() {
        let toml_str = r#"
[profile.default]
address = "localhost:7233"
namespace = "default"
api_key = "test-key"

[profile.default.tls]
disabled = false
client_cert_path = "/path/to/cert"

[profile.prod]
address = "prod.temporal.io:7233"
namespace = "production"
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();

        let default_profile = config.profiles.get("default").unwrap();
        assert_eq!(default_profile.address.as_ref().unwrap(), "localhost:7233");
        assert_eq!(default_profile.namespace.as_ref().unwrap(), "default");
        assert_eq!(default_profile.api_key.as_ref().unwrap(), "test-key");

        let tls = default_profile.tls.as_ref().unwrap();
        assert!(!tls.disabled);
        assert_eq!(tls.client_cert_path.as_ref().unwrap(), "/path/to/cert");

        let prod_profile = config.profiles.get("prod").unwrap();
        assert_eq!(
            prod_profile.address.as_ref().unwrap(),
            "prod.temporal.io:7233"
        );
        assert_eq!(prod_profile.namespace.as_ref().unwrap(), "production");
    }

    #[test]
    fn test_client_config_toml_roundtrip() {
        let mut prof = ClientConfigProfile {
            address: Some("addr".to_string()),
            namespace: Some("ns".to_string()),
            api_key: Some("key".to_string()),
            ..Default::default()
        };
        prof.grpc_meta.insert("k".to_string(), "v".to_string());

        let tls = ClientConfigTLS {
            client_cert_data: Some(b"cert".to_vec()),
            ..Default::default()
        };
        prof.tls = Some(tls);

        let mut conf = ClientConfig::default();
        conf.profiles.insert("default".to_string(), prof);

        let toml_bytes = conf.to_toml().unwrap();
        let new_conf = ClientConfig::from_toml(&toml_bytes, Default::default()).unwrap();
        assert_eq!(conf, new_conf);
    }

    #[test]
    fn test_load_client_config_profile_from_file() {
        // Create temporary file with test data
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
[profile.default]
address = "my-address"
namespace = "my-namespace"
"#
        )
        .unwrap();

        // Test explicit file path
        let env = MockEnvLookup::default();
        let options = LoadClientConfigProfileOptions {
            config_file_path: Some(temp_file.path().to_string_lossy().to_string()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
    }

    #[test]
    fn test_load_client_config_profile_from_env_file_path() {
        // Create temporary file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
[profile.default]
address = "my-address"
namespace = "my-namespace"
"#
        )
        .unwrap();

        // Test loading via TEMPORAL_CONFIG_FILE env var
        let mut env = MockEnvLookup::default();
        env.set("TEMPORAL_CONFIG_FILE", &temp_file.path().to_string_lossy());

        let options = LoadClientConfigProfileOptions {
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
    }

    #[test]
    fn test_load_client_config_profile_with_env_overrides() {
        let toml_str = r#"
[profile.foo]
address = "my-address"
namespace = "my-namespace"
api_key = "my-api-key"
some_future_key = "some future value not handled"

[profile.foo.tls]
disabled = true
client_cert_path = "my-client-cert-path"
client_cert_data = "my-client-cert-data"
client_key_path = "my-client-key-path"
client_key_data = "my-client-key-data"
server_ca_cert_path = "my-server-ca-cert-path"
server_ca_cert_data = "my-server-ca-cert-data"
server_name = "my-server-name"
disable_host_verification = true

[profile.foo.codec]
endpoint = "my-endpoint"
auth = "my-auth"

[profile.foo.grpc_meta]
some-heAder1 = "some-value1"
some-header2 = "some-value2"
some_heaDer3 = "some-value3"
"#;

        // Test without env vars
        let env = MockEnvLookup::default();
        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            config_file_profile: Some("foo".to_string()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key");

        let tls = profile.tls.as_ref().unwrap();
        assert!(tls.disabled);
        assert_eq!(
            tls.client_cert_path.as_ref().unwrap(),
            "my-client-cert-path"
        );
        assert_eq!(
            tls.client_cert_data.as_ref().unwrap(),
            "my-client-cert-data".as_bytes()
        );
        let codec = profile.codec.as_ref().unwrap();
        assert_eq!(codec.endpoint.as_ref().unwrap(), "my-endpoint");
        assert_eq!(codec.auth.as_ref().unwrap(), "my-auth");
        assert_eq!(profile.grpc_meta.len(), 3);
        assert_eq!(
            profile.grpc_meta.get("some-header1").unwrap(),
            "some-value1"
        );

        // Test with env var overrides
        let mut env = MockEnvLookup::default();
        env.set("TEMPORAL_PROFILE", "foo")
            .set("TEMPORAL_ADDRESS", "my-address-new")
            .set("TEMPORAL_NAMESPACE", "my-namespace-new")
            .set("TEMPORAL_API_KEY", "my-api-key-new")
            .set("TEMPORAL_TLS", "true")
            .set("TEMPORAL_TLS_CLIENT_CERT_PATH", "my-client-cert-path-new")
            .set("TEMPORAL_TLS_CLIENT_CERT_DATA", "my-client-cert-data-new")
            .set("TEMPORAL_TLS_CLIENT_KEY_PATH", "my-client-key-path-new")
            .set("TEMPORAL_TLS_CLIENT_KEY_DATA", "my-client-key-data-new")
            .set(
                "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
                "my-server-ca-cert-path-new",
            )
            .set(
                "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
                "my-server-ca-cert-data-new",
            )
            .set("TEMPORAL_TLS_SERVER_NAME", "my-server-name-new")
            .set("TEMPORAL_TLS_DISABLE_HOST_VERIFICATION", "false")
            .set("TEMPORAL_CODEC_ENDPOINT", "my-endpoint-new")
            .set("TEMPORAL_CODEC_AUTH", "my-auth-new")
            .set("TEMPORAL_GRPC_META_SOME_HEADER2", "some-value2-new")
            .set("TEMPORAL_GRPC_META_SOME_HEADER3", "")
            .set("TEMPORAL_GRPC_META_SOME_HEADER4", "some-value4-new");

        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address-new");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace-new");
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key-new");

        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled); // TLS enabled via env var
        assert_eq!(
            tls.client_cert_path.as_ref().unwrap(),
            "my-client-cert-path-new"
        );
        assert_eq!(tls.server_name.as_ref().unwrap(), "my-server-name-new");
        assert!(!tls.disable_host_verification);
        let codec = profile.codec.as_ref().unwrap();
        assert_eq!(codec.endpoint.as_ref().unwrap(), "my-endpoint-new");
        assert_eq!(codec.auth.as_ref().unwrap(), "my-auth-new");
        assert_eq!(
            profile.grpc_meta.get("some-header1").unwrap(),
            "some-value1"
        );
        assert_eq!(
            profile.grpc_meta.get("some-header2").unwrap(),
            "some-value2-new"
        );
        assert!(!profile.grpc_meta.contains_key("some-header3"));
        assert_eq!(
            profile.grpc_meta.get("some-header4").unwrap(),
            "some-value4-new"
        );
    }

    #[test]
    fn test_client_config_toml_full() {
        let toml_str = r#"
[profile.foo]
address = "my-address"
namespace = "my-namespace"
api_key = "my-api-key"
some_future_key = "some future value not handled"

[profile.foo.tls]
disabled = true
client_cert_path = "my-client-cert-path"
client_cert_data = "my-client-cert-data"
client_key_path = "my-client-key-path"
client_key_data = "my-client-key-data"
server_ca_cert_path = "my-server-ca-cert-path"
server_ca_cert_data = "my-server-ca-cert-data"
server_name = "my-server-name"
disable_host_verification = true

[profile.foo.codec]
endpoint = "my-endpoint"
auth = "my-auth"

[profile.foo.grpc_meta]
sOme-hEader_key = "some-value"
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();
        let profile = config.profiles.get("foo").unwrap();

        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key");

        let codec = profile.codec.as_ref().unwrap();
        assert_eq!(codec.endpoint.as_ref().unwrap(), "my-endpoint");
        assert_eq!(codec.auth.as_ref().unwrap(), "my-auth");

        let tls = profile.tls.as_ref().unwrap();
        assert!(tls.disabled);
        assert_eq!(
            tls.client_cert_path.as_ref().unwrap(),
            "my-client-cert-path"
        );
        assert_eq!(
            tls.client_cert_data.as_ref().unwrap(),
            "my-client-cert-data".as_bytes()
        );
        assert_eq!(tls.client_key_path.as_ref().unwrap(), "my-client-key-path");
        assert_eq!(
            tls.client_key_data.as_ref().unwrap(),
            "my-client-key-data".as_bytes()
        );
        assert_eq!(
            tls.server_ca_cert_path.as_ref().unwrap(),
            "my-server-ca-cert-path"
        );
        assert_eq!(
            tls.server_ca_cert_data.as_ref().unwrap(),
            "my-server-ca-cert-data".as_bytes()
        );
        assert_eq!(tls.server_name.as_ref().unwrap(), "my-server-name");
        assert!(tls.disable_host_verification);

        // Note: gRPC meta keys get normalized
        assert_eq!(profile.grpc_meta.len(), 1);
        assert_eq!(
            profile.grpc_meta.get("some-header-key").unwrap(),
            "some-value"
        );

        // Test round-trip serialization
        let toml_out = config.to_toml().unwrap();
        let config2 = ClientConfig::from_toml(&toml_out, Default::default()).unwrap();
        assert_eq!(config, config2);
    }

    #[test]
    fn test_client_config_toml_partial() {
        let toml_str = r#"
[profile.foo]
api_key = "my-api-key"

[profile.foo.tls]
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();
        let profile = config.profiles.get("foo").unwrap();

        assert!(profile.address.is_none());
        assert!(profile.namespace.is_none());
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key");
        assert!(profile.codec.is_none());
        assert!(profile.tls.is_some());

        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled); // default value
        assert!(tls.client_cert_path.is_none());
    }

    #[test]
    fn test_client_config_toml_empty() {
        let config = ClientConfig::from_toml("".as_bytes(), Default::default()).unwrap();
        assert!(config.profiles.is_empty());

        // Test round-trip
        let toml_out = config.to_toml().unwrap();
        let config2 = ClientConfig::from_toml(&toml_out, Default::default()).unwrap();
        assert_eq!(config, config2);
    }

    #[test]
    fn test_profile_not_found() {
        let toml_str = r#"
[profile.existing]
address = "localhost:7233"
"#;

        let mut env = MockEnvLookup::default();
        env.set("TEMPORAL_PROFILE", "nonexistent");

        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            config_file_profile: Some("nonexistent".to_string()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        // Should error because we explicitly asked for a profile that doesn't exist
        let result = load_client_config_profile(options);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::ProfileNotFound(p) if p == "nonexistent"
        ));
    }

    #[test]
    fn test_client_config_toml_strict_unrecognized_field() {
        let toml_str = r#"
[profile.default]
unrecognized_field = "is-bad"
"#;
        let err = ClientConfig::from_toml(
            toml_str.as_bytes(),
            ClientConfigFromTOMLOptions { strict: true },
        )
        .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("unrecognized_field"));
    }

    #[test]
    fn test_client_config_toml_strict_unrecognized_table() {
        let toml_str = r#"
[unrecognized_table]
foo = "bar"
"#;
        let err = ClientConfig::from_toml(
            toml_str.as_bytes(),
            ClientConfigFromTOMLOptions { strict: true },
        )
        .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("unrecognized_table"));
    }

    #[test]
    fn test_default_profile_not_found_is_ok() {
        let toml_str = r#"
[profile.existing]
address = "localhost:7233"
"#;

        let env = MockEnvLookup::default();

        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        // Should not error, just returns an empty profile
        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile, ClientConfigProfile::default());
    }

    #[test]
    fn test_normalize_grpc_meta_key() {
        assert_eq!(normalize_grpc_meta_key("SOME_HEADER"), "some-header");
        assert_eq!(normalize_grpc_meta_key("some_header"), "some-header");
        assert_eq!(normalize_grpc_meta_key("Some_Header"), "some-header");
    }

    #[test]
    fn test_env_var_to_bool() {
        assert_eq!(env_var_to_bool("true"), Some(true));
        assert_eq!(env_var_to_bool("TRUE"), Some(true));
        assert_eq!(env_var_to_bool("1"), Some(true));

        assert_eq!(env_var_to_bool("false"), Some(false));
        assert_eq!(env_var_to_bool("FALSE"), Some(false));
        assert_eq!(env_var_to_bool("0"), Some(false));

        assert_eq!(env_var_to_bool("invalid"), None);
        assert_eq!(env_var_to_bool("yes"), None);
        assert_eq!(env_var_to_bool("no"), None);
    }

    #[test]
    fn test_load_client_config_profile_disables_are_an_error() {
        let options = LoadClientConfigProfileOptions {
            disable_file: true,
            disable_env: true,
            ..Default::default()
        };

        let result = load_client_config_profile(options);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot disable both file and environment")
        );
    }

    #[test]
    fn test_load_client_config_profile_from_env_only() {
        let mut env = MockEnvLookup::default();
        env.set("TEMPORAL_ADDRESS", "env-address")
            .set("TEMPORAL_NAMESPACE", "env-namespace");

        let options = LoadClientConfigProfileOptions {
            disable_file: true,
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "env-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "env-namespace");
    }

    #[test]
    fn test_api_key_tls_auto_enable() {
        // Test 1: When API key is present, TLS should be automatically enabled
        let toml_str = r#"
[profile.default]
api_key = "my-api-key"
"#;

        let env = MockEnvLookup::default();
        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();

        // TLS should be enabled due to API key presence
        assert!(profile.tls.is_some());
        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled);
    }

    #[test]
    fn test_no_api_key_no_tls_is_none() {
        // Test that if no API key is present and no TLS block exists, TLS config is None
        let toml_str = r#"
[profile.default]
address = "some-address"
"#;

        let env = MockEnvLookup::default();
        let options = LoadClientConfigProfileOptions {
            config_file_data: Some(toml_str.as_bytes().to_vec()),
            env_lookup: Some(&env),
            ..Default::default()
        };

        let profile = load_client_config_profile(options).unwrap();

        // TLS should not be enabled
        assert!(profile.tls.is_none());
    }
}
