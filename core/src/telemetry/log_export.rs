use parking_lot::Mutex;
use ringbuf::{Consumer, HeapRb, Producer};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use temporal_sdk_core_api::CoreLog;
use tracing_subscriber::Layer;

const RB_SIZE: usize = 2048;

pub(super) type CoreLogsOut = Consumer<CoreLog, Arc<HeapRb<CoreLog>>>;

pub(super) struct CoreLogExportLayer {
    logs_in: Mutex<Producer<CoreLog, Arc<HeapRb<CoreLog>>>>,
}

#[derive(Debug)]
struct CoreLogFieldStorage(HashMap<String, serde_json::Value>);

impl CoreLogExportLayer {
    pub(super) fn new() -> (Self, CoreLogsOut) {
        let (lin, lout) = HeapRb::new(RB_SIZE).split();
        (
            Self {
                logs_in: Mutex::new(lin),
            },
            lout,
        )
    }
}

impl<S> Layer<S> for CoreLogExportLayer
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let mut fields = HashMap::new();
        let mut visitor = JsonVisitor(&mut fields);
        attrs.record(&mut visitor);
        let storage = CoreLogFieldStorage(fields);
        let mut extensions = span.extensions_mut();
        extensions.insert(storage);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();

        let mut extensions_mut = span.extensions_mut();
        let custom_field_storage: &mut CoreLogFieldStorage =
            extensions_mut.get_mut::<CoreLogFieldStorage>().unwrap();
        let json_data = &mut custom_field_storage.0;

        let mut visitor = JsonVisitor(json_data);
        values.record(&mut visitor);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = HashMap::new();
        let mut visitor = JsonVisitor(&mut fields);
        event.record(&mut visitor);

        let mut spans = vec![];
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let storage = extensions.get::<CoreLogFieldStorage>().unwrap();
                let field_data = &storage.0;
                for (k, v) in field_data {
                    fields.insert(k.to_string(), v.clone());
                }
                spans.push(span.name().to_string());
            }
        }

        // "message" is the magic default field keyname for the string passed to the event
        let message = fields.remove("message").unwrap_or_default();
        let log = CoreLog {
            target: event.metadata().target().to_string(),
            // This weird as_str dance prevents adding extra quotes
            message: message.as_str().unwrap_or_default().to_string(),
            timestamp: SystemTime::now(),
            level: *event.metadata().level(),
            fields,
            span_contexts: spans,
        };
        let _ = self.logs_in.lock().push(log);
    }
}

struct JsonVisitor<'a>(&'a mut HashMap<String, serde_json::Value>);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(value.to_string()),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(format!("{:?}", value)),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        fetch_global_buffered_logs, telemetry::forward_at_level, telemetry_init, Logger,
        TelemetryOptionsBuilder,
    };
    use std::mem::size_of;
    use temporal_sdk_core_api::CoreLog;
    use tracing::{level_filters::LevelFilter, Level};

    #[instrument(fields(bros = "brohemian"))]
    fn instrumented(thing: &str) {
        warn!("warn");
        info!(foo = "bar", "info");
        debug!("debug");
    }

    #[tokio::test]
    async fn test_forwarding_output() {
        let opts = TelemetryOptionsBuilder::default()
            // .tracing_filter("TRACE".to_string())
            .logging(Logger::Forward {
                core_level: Level::INFO,
                others_level: Level::ERROR,
            })
            .build()
            .unwrap();
        telemetry_init(&opts).unwrap();

        let top_span = span!(Level::INFO, "yayspan", huh = "wat");
        let _guard = top_span.enter();
        info!("Whata?");
        instrumented("hi");
        info!("Donezo");

        let logs = fetch_global_buffered_logs();
        for log in logs {
            println!("{:?}", log);
        }
    }
}