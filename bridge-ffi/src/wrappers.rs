use log::LevelFilter;
use std::{net::SocketAddr, str::FromStr};
use temporal_sdk_core::{
    Logger, MetricsExporter, OtelCollectorOptions, TelemetryOptionsBuilder, TraceExporter, Url,
};
use temporal_sdk_core_protos::coresdk::bridge;

// Present for try-from only
pub struct InitTelemetryRequest(pub bridge::InitTelemetryRequest);

impl TryFrom<InitTelemetryRequest> for temporal_sdk_core::TelemetryOptions {
    type Error = String;

    fn try_from(InitTelemetryRequest(req): InitTelemetryRequest) -> Result<Self, Self::Error> {
        let mut telemetry_opts = TelemetryOptionsBuilder::default();
        telemetry_opts.tracing_filter(req.tracing_filter.clone());
        match req.logging {
            None => {}
            Some(bridge::init_telemetry_request::Logging::Console(_)) => {
                telemetry_opts.logging(Logger::Console);
            }
            Some(bridge::init_telemetry_request::Logging::Forward(
                bridge::init_telemetry_request::ForwardLoggerOptions { level },
            )) => {
                if let Some(level) = bridge::LogLevel::from_i32(level) {
                    let level = match level {
                        bridge::LogLevel::Unspecified => {
                            return Err("Must specify log level".to_string())
                        }
                        bridge::LogLevel::Off => LevelFilter::Off,
                        bridge::LogLevel::Error => LevelFilter::Error,
                        bridge::LogLevel::Warn => LevelFilter::Warn,
                        bridge::LogLevel::Info => LevelFilter::Info,
                        bridge::LogLevel::Debug => LevelFilter::Debug,
                        bridge::LogLevel::Trace => LevelFilter::Trace,
                    };
                    telemetry_opts.logging(Logger::Forward(level));
                } else {
                    return Err(format!("Unknown LogLevel: {}", level));
                }
            }
        }
        match req.metrics {
            None => {}
            Some(bridge::init_telemetry_request::Metrics::Prometheus(
                bridge::init_telemetry_request::PrometheusOptions {
                    export_bind_address,
                },
            )) => {
                telemetry_opts.metrics(MetricsExporter::Prometheus(
                    SocketAddr::from_str(&export_bind_address)
                        .map_err(|err| format!("invalid Prometheus address: {}", err))?,
                ));
            }
            Some(bridge::init_telemetry_request::Metrics::OtelMetrics(
                bridge::init_telemetry_request::OtelCollectorOptions { url, headers },
            )) => {
                telemetry_opts.metrics(MetricsExporter::Otel(OtelCollectorOptions {
                    url: Url::parse(&url).map_err(|err| {
                        format!("invalid OpenTelemetry collector URL for metrics: {}", err)
                    })?,
                    headers,
                }));
            }
        }
        match req.tracing {
            None => {}
            Some(bridge::init_telemetry_request::Tracing::OtelTracing(
                bridge::init_telemetry_request::OtelCollectorOptions { url, headers },
            )) => {
                telemetry_opts.tracing(TraceExporter::Otel(OtelCollectorOptions {
                    url: Url::parse(&url).map_err(|err| {
                        format!("invalid OpenTelemetry collector URL for tracing: {}", err)
                    })?,
                    headers,
                }));
            }
        }

        telemetry_opts
            .build()
            .map_err(|err| format!("invalid telemetry options: {}", err))
    }
}

pub struct ClientOptions(pub bridge::CreateClientRequest);

impl TryFrom<ClientOptions> for temporal_sdk_core::ClientOptions {
    type Error = String;

    fn try_from(ClientOptions(req): ClientOptions) -> Result<Self, Self::Error> {
        let mut client_opts = temporal_sdk_core::ClientOptionsBuilder::default();
        if !req.target_url.is_empty() {
            client_opts.target_url(
                temporal_sdk_core::Url::parse(&req.target_url)
                    .map_err(|err| format!("invalid target URL: {}", err))?,
            );
        }
        if !req.client_name.is_empty() {
            client_opts.client_name(req.client_name);
        }
        if !req.client_version.is_empty() {
            client_opts.client_version(req.client_version);
        }
        if !req.identity.is_empty() {
            client_opts.identity(req.identity);
        }
        if let Some(req_tls_config) = req.tls_config {
            let mut tls_config = temporal_sdk_core::TlsConfig::default();
            if !req_tls_config.server_root_ca_cert.is_empty() {
                tls_config.server_root_ca_cert = Some(req_tls_config.server_root_ca_cert);
            }
            if !req_tls_config.domain.is_empty() {
                tls_config.domain = Some(req_tls_config.domain);
            }
            if !req_tls_config.client_cert.is_empty()
                || !req_tls_config.client_private_key.is_empty()
            {
                tls_config.client_tls_config = Some(temporal_sdk_core::ClientTlsConfig {
                    client_cert: req_tls_config.client_cert,
                    client_private_key: req_tls_config.client_private_key,
                })
            }
            client_opts.tls_cfg(tls_config);
        }
        if let Some(req_retry_config) = req.retry_config {
            let mut retry_config = temporal_sdk_core::RetryConfig::default();
            if let Some(v) = req_retry_config.initial_interval {
                retry_config.initial_interval =
                    v.try_into().map_err(|_| "invalid initial interval")?;
            }
            if let Some(v) = req_retry_config.randomization_factor {
                retry_config.randomization_factor = v;
            }
            if let Some(v) = req_retry_config.multiplier {
                retry_config.multiplier = v;
            }
            if let Some(v) = req_retry_config.max_interval {
                retry_config.max_interval = v.try_into().map_err(|_| "invalid max interval")?;
            }
            if let Some(v) = req_retry_config.max_elapsed_time {
                retry_config.max_elapsed_time =
                    Some(v.try_into().map_err(|_| "invalid max elapsed time")?);
            }
            if let Some(v) = req_retry_config.max_retries {
                retry_config.max_retries = v as usize;
            }
            client_opts.retry_config(retry_config);
        }
        client_opts
            .build()
            .map_err(|err| format!("invalid client options: {}", err))
    }
}

// Present for try-from only
pub struct WorkerConfig(pub bridge::CreateWorkerRequest);

impl TryFrom<WorkerConfig> for temporal_sdk_core_api::worker::WorkerConfig {
    type Error = String;

    fn try_from(WorkerConfig(req): WorkerConfig) -> Result<Self, Self::Error> {
        let mut config = temporal_sdk_core_api::worker::WorkerConfigBuilder::default();
        if !req.task_queue.is_empty() {
            config.task_queue(req.task_queue);
        }
        if let Some(v) = req.max_cached_workflows {
            config.max_cached_workflows(v as usize);
        }
        if let Some(v) = req.max_outstanding_workflow_tasks {
            config.max_outstanding_workflow_tasks(v as usize);
        }
        if let Some(v) = req.max_outstanding_activities {
            config.max_outstanding_activities(v as usize);
        }
        if let Some(v) = req.max_outstanding_local_activities {
            config.max_outstanding_local_activities(v as usize);
        }
        if let Some(v) = req.max_concurrent_wft_polls {
            config.max_concurrent_wft_polls(v as usize);
        }
        if let Some(v) = req.nonsticky_to_sticky_poll_ratio {
            config.nonsticky_to_sticky_poll_ratio(v);
        }
        if let Some(v) = req.max_concurrent_at_polls {
            config.max_concurrent_at_polls(v as usize);
        }
        config.no_remote_activities(req.no_remote_activities);
        if let Some(v) = req.sticky_queue_schedule_to_start_timeout {
            let v: std::time::Duration = v
                .try_into()
                .map_err(|_| "invalid sticky queue schedule to start timeout".to_string())?;
            config.sticky_queue_schedule_to_start_timeout(v);
        }
        if let Some(v) = req.max_heartbeat_throttle_interval {
            let v: std::time::Duration = v
                .try_into()
                .map_err(|_| "invalid max heartbeat throttle interval".to_string())?;
            config.max_heartbeat_throttle_interval(v);
        }
        if let Some(v) = req.default_heartbeat_throttle_interval {
            let v: std::time::Duration = v
                .try_into()
                .map_err(|_| "invalid default heartbeat throttle interval".to_string())?;
            config.default_heartbeat_throttle_interval(v);
        }
        config
            .build()
            .map_err(|err| format!("invalid request: {}", err))
    }
}
