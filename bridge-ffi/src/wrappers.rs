use std::str::FromStr;
use temporal_sdk_core_protos::coresdk::bridge;

// Present for try-from only
pub struct CoreInitOptions(pub bridge::InitRequest);

impl TryFrom<CoreInitOptions> for temporal_sdk_core::CoreInitOptions {
    type Error = String;

    fn try_from(CoreInitOptions(req): CoreInitOptions) -> Result<Self, Self::Error> {
        let mut core_opts = temporal_sdk_core::CoreInitOptionsBuilder::default();
        if let Some(req_gateway_opts) = req.gateway_options {
            let mut gateway_opts = temporal_sdk_core::ServerGatewayOptionsBuilder::default();
            if !req_gateway_opts.target_url.is_empty() {
                gateway_opts.target_url(
                    temporal_sdk_core::Url::parse(&req_gateway_opts.target_url)
                        .map_err(|err| format!("invalid target URL: {}", err))?,
                );
            }
            if !req_gateway_opts.namespace.is_empty() {
                gateway_opts.namespace(req_gateway_opts.namespace);
            }
            if !req_gateway_opts.client_name.is_empty() {
                gateway_opts.client_name(req_gateway_opts.client_name);
            }
            if !req_gateway_opts.client_version.is_empty() {
                gateway_opts.client_version(req_gateway_opts.client_version);
            }
            if !req_gateway_opts.static_headers.is_empty() {
                gateway_opts.static_headers(req_gateway_opts.static_headers);
            }
            if !req_gateway_opts.identity.is_empty() {
                gateway_opts.identity(req_gateway_opts.identity);
            }
            if !req_gateway_opts.worker_binary_id.is_empty() {
                gateway_opts.worker_binary_id(req_gateway_opts.worker_binary_id);
            }
            if let Some(req_tls_config) = req_gateway_opts.tls_config {
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
                gateway_opts.tls_cfg(tls_config);
            }
            if let Some(req_retry_config) = req_gateway_opts.retry_config {
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
                gateway_opts.retry_config(retry_config);
            }
            core_opts.gateway_opts(
                gateway_opts
                    .build()
                    .map_err(|err| format!("invalid gateway options: {}", err))?,
            );
        }
        if let Some(req_telemetry_opts) = req.telemetry_options {
            let mut telemetry_opts = temporal_sdk_core::TelemetryOptionsBuilder::default();
            if !req_telemetry_opts.otel_collector_url.is_empty() {
                telemetry_opts.otel_collector_url(
                    temporal_sdk_core::Url::parse(&req_telemetry_opts.otel_collector_url)
                        .map_err(|err| format!("invalid OpenTelemetry collector URL: {}", err))?,
                );
            }
            if !req_telemetry_opts.tracing_filter.is_empty() {
                telemetry_opts.tracing_filter(req_telemetry_opts.tracing_filter.clone());
            }
            match req_telemetry_opts.log_forwarding_level() {
                bridge::LogLevel::Unspecified => {}
                bridge::LogLevel::Off => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Off);
                }
                bridge::LogLevel::Error => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Error);
                }
                bridge::LogLevel::Warn => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Warn);
                }
                bridge::LogLevel::Info => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Info);
                }
                bridge::LogLevel::Debug => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Debug);
                }
                bridge::LogLevel::Trace => {
                    telemetry_opts.log_forwarding_level(log::LevelFilter::Trace);
                }
            }
            if !req_telemetry_opts.prometheus_export_bind_address.is_empty() {
                telemetry_opts.prometheus_export_bind_address(
                    std::net::SocketAddr::from_str(
                        &req_telemetry_opts.prometheus_export_bind_address,
                    )
                    .map_err(|err| format!("invalid Prometheus address: {}", err))?,
                );
            }
            core_opts.telemetry_opts(
                telemetry_opts
                    .build()
                    .map_err(|err| format!("invalid telemetry options: {}", err))?,
            );
        }
        core_opts
            .build()
            .map_err(|err| format!("invalid options: {}", err))
    }
}

// Present for try-from only
pub struct WorkerConfig(pub bridge::RegisterWorkerRequest);

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
