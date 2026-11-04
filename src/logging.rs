//! JSON logs in the shared GrooveMap schema emitted by the Python services
//! (`python-libraries` `common.config.setup_logging`).
//!
//! Each event is one JSON object per line with root keys `timestamp` (RFC 3339 UTC, `Z`),
//! `level`, `logger`, `lineno`, `event`, `service` and `environment`, followed by the event's
//! own fields at the root.
//!
//! Level mapping (tracing -> Python/structlog): ERROR -> `error`, WARN -> `warning`,
//! INFO -> `info`, DEBUG and TRACE -> `debug` (Python has no lower level).
//!
//! Collision policy: the schema/deployment keys (`timestamp`, `level`, `logger`, `lineno`,
//! `event`, `service`, `environment`) and the structural keys `span`, `spans` and `fields`
//! always win at the root. An event field with one of those names is kept, not dropped, under
//! `fields.<name>`. Span context stays nested under `span` (innermost) and `spans` (root first).

use std::fmt;
use std::fmt::Write as _;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

/// Service name written on every line; the same name `health.rs` reports.
pub const SERVICE_NAME: &str = "discogs-ingestion";

const RESERVED: [&str; 10] = ["timestamp", "level", "logger", "lineno", "event", "service", "environment", "span", "spans", "fields"];

/// Format events as shared-schema JSON with the deployment environment resolved once.
pub struct EnvironmentJson {
    environment: String,
}

impl EnvironmentJson {
    /// Resolve the startup setting once; missing/non-Unicode settings use development.
    pub fn new(environment: Option<String>) -> Self {
        Self { environment: environment.unwrap_or_else(|| "development".to_string()) }
    }
}

fn level_name(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "error",
        Level::WARN => "warning",
        Level::INFO => "info",
        Level::DEBUG | Level::TRACE => "debug",
    }
}

#[derive(Default)]
struct Collect {
    message: Option<String>,
    root: Map<String, Value>,
    displaced: Map<String, Value>,
}

impl Collect {
    fn put(&mut self, field: &Field, value: Value) {
        let name = field.name();
        if name == "message" {
            self.message = Some(match value {
                Value::String(text) => text,
                other => other.to_string(),
            });
        } else if RESERVED.contains(&name) {
            self.displaced.insert(name.to_string(), value);
        } else {
            self.root.insert(name.to_string(), value);
        }
    }
}

impl Visit for Collect {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let mut text = String::new();
        let _ = write!(text, "{value:?}");
        self.put(field, Value::String(text));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::String(value.to_string()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        // Non-finite floats have no JSON form; fall back to their text.
        self.put(field, serde_json::Number::from_f64(value).map_or_else(|| Value::String(value.to_string()), Value::Number));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::String(value.to_string()));
    }
}

impl<S, N> FormatEvent<S, N> for EnvironmentJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let metadata = event.metadata();
        let mut collected = Collect::default();
        event.record(&mut collected);

        let mut object = collected.root;
        object.insert("timestamp".into(), chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true).into());
        object.insert("level".into(), level_name(metadata.level()).into());
        object.insert("logger".into(), metadata.target().into());
        object.insert("lineno".into(), metadata.line().map_or(Value::Null, Value::from));
        object.insert("event".into(), collected.message.unwrap_or_default().into());
        object.insert("service".into(), SERVICE_NAME.into());
        object.insert("environment".into(), self.environment.clone().into());
        if !collected.displaced.is_empty() {
            object.insert("fields".into(), Value::Object(collected.displaced));
        }

        if let Some(scope) = ctx.event_scope() {
            let mut spans: Vec<Value> = scope
                .from_root()
                .map(|span| {
                    let mut entry = Map::new();
                    if let Some(fields) = span.extensions().get::<FormattedFields<JsonFields>>()
                        && let Ok(Value::Object(parsed)) = serde_json::from_str::<Value>(fields.as_str())
                    {
                        entry.extend(parsed);
                    }
                    entry.insert("name".into(), span.name().into());
                    Value::Object(entry)
                })
                .collect();
            if let Some(innermost) = spans.last().cloned() {
                object.insert("span".into(), innermost);
                object.insert("spans".into(), Value::Array(std::mem::take(&mut spans)));
            }
        }

        let line = serde_json::to_string(&Value::Object(object)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture(environment: Option<&str>, filter: &str) -> Vec<serde_json::Value> {
        let output = Capture::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::registry().with(tracing_subscriber::EnvFilter::new(filter)).with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(EnvironmentJson::new(environment.map(str::to_string)))
                .with_writer(move || writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("extraction", source = "discogs");
            let _entered = span.enter();
            tracing::info!(environment = "event-specific", records = 7, "quoted \"message\"\nnext line");
            let error = io::Error::other("failed \"file\"\nretry");
            tracing::error!(error = %error, "extraction failed");
            tracing::warn!("warning");
            tracing::debug!("debug");
            tracing::trace!("trace");
        });
        let bytes = output.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap().lines().map(|line| serde_json::from_str(line).unwrap()).collect()
    }

    #[test]
    fn every_event_carries_the_shared_schema_with_fields_at_the_root() {
        let events = capture(Some("production"), "trace");
        assert_eq!(events.len(), 5);
        for event in &events {
            assert_eq!(event["environment"], "production");
            assert_eq!(event["service"], "discogs-ingestion");
            assert!(event["timestamp"].as_str().unwrap().ends_with('Z'));
            assert!(event["logger"].as_str().unwrap().contains("logging"));
            assert!(event["lineno"].is_number());
            assert!(event["event"].is_string());
            assert_eq!(event["span"]["name"], "extraction");
            assert_eq!(event["span"]["source"], "discogs");
            assert_eq!(event["spans"][0]["name"], "extraction");
            for legacy in ["line_number", "target", "threadId"] {
                assert!(event.get(legacy).is_none());
            }
        }
        assert_eq!(events[0]["event"], "quoted \"message\"\nnext line");
        assert_eq!(events[0]["records"], 7);
        assert_eq!(events[1]["error"], "failed \"file\"\nretry");
    }

    #[test]
    fn colliding_event_fields_are_kept_under_fields_and_never_override_the_schema() {
        let events = capture(Some("production"), "trace");
        assert_eq!(events[0]["environment"], "production");
        assert_eq!(events[0]["fields"]["environment"], "event-specific");
        assert!(events[1].get("fields").is_none());
    }

    #[test]
    fn levels_are_lowercase_python_names() {
        let events = capture(Some("production"), "trace");
        let levels: Vec<_> = events.iter().map(|event| event["level"].as_str().unwrap()).collect();
        assert_eq!(levels, ["info", "error", "warning", "debug", "debug"]);
    }

    #[test]
    fn unset_environment_defaults_to_development() {
        let events = capture(None, "info");
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| event["environment"] == "development"));
    }

    #[test]
    fn custom_environment_is_json_escaped_and_preserved() {
        let environment = "staging \"west\"\n雪";
        let events = capture(Some(environment), "info");
        assert!(events.iter().all(|event| event["environment"] == environment));
    }

    #[test]
    fn explicit_empty_environment_is_preserved() {
        let events = capture(Some(""), "info");
        assert!(events.iter().all(|event| event["environment"] == ""));
    }

    #[test]
    fn filtering_still_excludes_less_severe_events() {
        let events = capture(Some("production"), "error");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["level"], "error");
        assert_eq!(events[0]["environment"], "production");
    }
}
