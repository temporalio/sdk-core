use parking_lot::Mutex;
use ringbuf::{Consumer, HeapRb, Producer};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use temporal_sdk_core_api::telemetry::CoreLog;
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
    use crate::{telemetry::construct_filter_string, telemetry_init};
    use temporal_sdk_core_api::telemetry::{CoreTelemetry, Logger, TelemetryOptionsBuilder};
    use tracing::Level;

    #[instrument(fields(bros = "brohemian"))]
    fn instrumented(thing: &str) {
        warn!("warn");
        info!(foo = "bar", "info");
        debug!("debug");
    }

    #[tokio::test]
    async fn test_forwarding_output() {
        let opts = TelemetryOptionsBuilder::default()
            .logging(Logger::Forward {
                filter: construct_filter_string(Level::INFO, Level::WARN),
            })
            .build()
            .unwrap();
        let instance = telemetry_init(opts).unwrap();
        let _g = tracing::subscriber::set_default(instance.trace_subscriber.clone());

        let top_span = span!(Level::INFO, "yayspan", huh = "wat");
        let _guard = top_span.enter();
        info!("Whata?");
        instrumented("hi");
        info!("Donezo");

        let logs = instance.fetch_buffered_logs();
        // Verify debug log was not forwarded
        assert!(!logs.iter().any(|l| l.message == "debug"));
        assert_eq!(logs.len(), 4);
        // Ensure fields are attached to events properly
        let info_msg = &logs[2];
        assert_eq!(info_msg.message, "info");
        assert_eq!(info_msg.fields.len(), 4);
        assert_eq!(info_msg.fields.get("huh"), Some(&"wat".into()));
        assert_eq!(info_msg.fields.get("foo"), Some(&"bar".into()));
        assert_eq!(info_msg.fields.get("bros"), Some(&"brohemian".into()));
        assert_eq!(info_msg.fields.get("thing"), Some(&"hi".into()));
    }
}
