//! JSON logs with deployment context independent of individual event fields.

use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Format, Json, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Retain the standard JSON event format and add the deployment environment at its root.
pub struct EnvironmentJson {
    inner: Format<Json>,
    environment_json: String,
}

impl EnvironmentJson {
    /// Resolve the startup setting once; missing/non-Unicode settings use development.
    pub fn new(environment: Option<String>) -> Self {
        let environment = environment.unwrap_or_else(|| "development".to_string());
        Self {
            inner: tracing_subscriber::fmt::format().with_target(false).with_thread_ids(false).with_line_number(true).json(),
            environment_json: serde_json::Value::String(environment).to_string(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for EnvironmentJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let mut formatted = String::new();
        self.inner.format_event(ctx, Writer::new(&mut formatted), event)?;
        // Keep the standard formatter's fields, spans, escaping and trailing newline intact.
        let fields = formatted.strip_prefix('{').ok_or(fmt::Error)?;
        write!(writer, "{{\"environment\":{},{}", self.environment_json, fields)
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
    fn production_context_is_present_on_every_event_without_overwriting_event_fields() {
        let events = capture(Some("production"), "trace");
        assert_eq!(events.len(), 5);
        for event in &events {
            assert_eq!(event["environment"], "production");
            assert!(event["timestamp"].is_string());
            assert!(event["level"].is_string());
            assert!(event["line_number"].is_number());
            assert_eq!(event["span"]["name"], "extraction");
            assert_eq!(event["span"]["source"], "discogs");
            assert_eq!(event["spans"][0]["name"], "extraction");
            assert!(event.get("target").is_none());
            assert!(event.get("threadId").is_none());
        }
        assert_eq!(events[0]["fields"]["environment"], "event-specific");
        assert_eq!(events[0]["fields"]["records"], 7);
        assert_eq!(events[0]["fields"]["message"], "quoted \"message\"\nnext line");
        assert_eq!(events[1]["fields"]["error"], "failed \"file\"\nretry");
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
        assert_eq!(events[0]["level"], "ERROR");
        assert_eq!(events[0]["environment"], "production");
    }
}
