use super::*;

#[test]
fn test_exchange_names_default_prefix() {
    let mq = MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: String::new(),
        max_retries: 1,
        exchange_prefix: DEFAULT_EXCHANGE_PREFIX.to_string(),
        pacer: None,
    };
    assert_eq!(mq.exchange_name(DataType::Artists), "groovemap-discogs-artists");
    assert_eq!(mq.exchange_name(DataType::Labels), "groovemap-discogs-labels");
    assert_eq!(mq.exchange_name(DataType::Masters), "groovemap-discogs-masters");
    assert_eq!(mq.exchange_name(DataType::Releases), "groovemap-discogs-releases");
}

#[test]
fn test_exchange_names_custom_prefix() {
    let mq = MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: String::new(),
        max_retries: 1,
        exchange_prefix: "groovemap-musicbrainz".to_string(),
        pacer: None,
    };
    assert_eq!(mq.exchange_name(DataType::Artists), "groovemap-musicbrainz-artists");
    assert_eq!(mq.exchange_name(DataType::Labels), "groovemap-musicbrainz-labels");
    assert_eq!(mq.exchange_name(DataType::Releases), "groovemap-musicbrainz-releases");
}

#[test]
fn test_normalize_amqp_url_with_trailing_slash() {
    // Trailing slash should be removed to use default vhost
    let url = "amqp://user:pass@host:5672/";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert_eq!(normalized, "amqp://user:pass@host:5672");
}

#[test]
fn test_normalize_amqp_url_without_trailing_slash() {
    // No trailing slash should remain unchanged
    let url = "amqp://user:pass@host:5672";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert_eq!(normalized, "amqp://user:pass@host:5672");
}

#[test]
fn test_normalize_amqp_url_with_explicit_vhost() {
    // Explicit vhost should be preserved
    let url = "amqp://user:pass@host:5672/groovemap";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert_eq!(normalized, "amqp://user:pass@host:5672/groovemap");
}

#[test]
fn test_normalize_amqp_url_with_encoded_default_vhost() {
    // URL-encoded default vhost %2F should be preserved
    let url = "amqp://user:pass@host:5672/%2F";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert_eq!(normalized, "amqp://user:pass@host:5672/%2F");
}

#[test]
fn test_normalize_amqp_url_minimal() {
    // Minimal URL without credentials
    let url = "amqp://localhost:5672/";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert_eq!(normalized, "amqp://localhost:5672");
}

#[test]
fn test_normalize_amqp_url_invalid() {
    // Invalid URL should return error
    let url = "not-a-valid-url";
    let result = MessageQueue::normalize_amqp_url(url);
    assert!(result.is_err());
}

#[test]
fn test_normalize_amqp_url_empty() {
    let result = MessageQueue::normalize_amqp_url("");
    assert!(result.is_err());
}

#[test]
fn test_normalize_amqp_url_different_ports() {
    let url1 = "amqp://host:5672/";
    let url2 = "amqp://host:15672/";

    let normalized1 = MessageQueue::normalize_amqp_url(url1).unwrap();
    let normalized2 = MessageQueue::normalize_amqp_url(url2).unwrap();

    assert_eq!(normalized1, "amqp://host:5672");
    assert_eq!(normalized2, "amqp://host:15672");
}

#[test]
fn test_normalize_amqp_url_with_query_params() {
    let url = "amqp://host:5672/?heartbeat=30";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    // Query params should be preserved
    assert!(normalized.contains("heartbeat=30"));
}

#[test]
fn test_message_serialization_data() {
    let data_msg = DataMessage { id: "123".to_string(), sha256: "abc".to_string(), data: serde_json::json!({"key": "value"}), raw_xml: None };

    let message = Message::Data(data_msg);
    let serialized = serde_json::to_vec(&message).unwrap();
    let deserialized: Message = serde_json::from_slice(&serialized).unwrap();

    match deserialized {
        Message::Data(msg) => {
            assert_eq!(msg.id, "123");
            assert_eq!(msg.sha256, "abc");
        }
        _ => panic!("Expected Data message"),
    }
}

#[test]
fn test_message_serialization_file_complete() {
    let file_complete_msg =
        FileCompleteMessage { data_type: "artists".to_string(), timestamp: chrono::Utc::now(), total_processed: 100, file: "test.xml".to_string() };

    let message = Message::FileComplete(file_complete_msg.clone());
    let serialized = serde_json::to_vec(&message).unwrap();
    let deserialized: Message = serde_json::from_slice(&serialized).unwrap();

    match deserialized {
        Message::FileComplete(msg) => {
            assert_eq!(msg.data_type, "artists");
            assert_eq!(msg.total_processed, 100);
            assert_eq!(msg.file, "test.xml");
        }
        _ => panic!("Expected FileComplete message"),
    }
}

#[test]
fn test_constants() {
    assert_eq!(DEFAULT_EXCHANGE_PREFIX, "groovemap-discogs");
    assert_eq!(AMQP_EXCHANGE_TYPE, ExchangeKind::Fanout);
}

#[test]
fn test_message_properties_persistent_delivery() {
    let props = MessageQueue::message_properties();
    // delivery_mode 2 = persistent (messages survive broker restart)
    assert_eq!(props.delivery_mode(), &Some(2));
    assert!(props.content_type().is_some());
    let encoding = props.content_encoding().as_ref().expect("content_encoding should be set");
    assert_eq!(encoding.as_str(), "UTF-8", "content_encoding should be UTF-8");
}

#[tokio::test]
async fn test_new_connection_failure() {
    // Use an invalid port so the connection will fail, with only 1 retry to keep the test fast
    let result = MessageQueue::new("amqp://localhost:59999", 1, DEFAULT_EXCHANGE_PREFIX).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("Failed to connect to AMQP broker after retries"), "Unexpected error: {}", err_msg);
}

#[test]
fn test_extraction_complete_message_roundtrip() {
    let mut record_counts = std::collections::HashMap::new();
    record_counts.insert("artists".to_string(), 9957079);
    record_counts.insert("releases".to_string(), 18952204);

    let msg = ExtractionCompleteMessage {
        version: "20260101".to_string(),
        timestamp: chrono::Utc::now(),
        started_at: chrono::Utc::now(),
        record_counts: record_counts.clone(),
    };

    let message = Message::ExtractionComplete(msg);
    let serialized = serde_json::to_vec(&message).unwrap();
    let deserialized: Message = serde_json::from_slice(&serialized).unwrap();

    match deserialized {
        Message::ExtractionComplete(m) => {
            assert_eq!(m.version, "20260101");
            assert_eq!(m.record_counts["artists"], 9957079);
            assert_eq!(m.record_counts["releases"], 18952204);
        }
        _ => panic!("Expected ExtractionComplete message"),
    }
}

#[test]
fn test_normalize_amqp_url_with_credentials_special_chars() {
    // URL with percent-encoded special characters in credentials
    let url = "amqp://user%40domain:p%40ss@host:5672/%2F";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    // Credentials with special chars and explicit %2F vhost should be preserved
    assert!(normalized.contains("user%40domain"));
    assert!(normalized.contains("p%40ss"));
    assert!(normalized.contains("/%2F"));
}

#[test]
fn test_normalize_amqp_url_amqps_scheme() {
    // TLS scheme should be preserved
    let url = "amqps://user:pass@host:5671/";
    let normalized = MessageQueue::normalize_amqp_url(url).unwrap();
    assert!(normalized.starts_with("amqps://"), "Expected amqps:// scheme, got: {}", normalized);
    assert_eq!(normalized, "amqps://user:pass@host:5671");
}

#[test]
fn test_file_complete_message_serialization_format() {
    let file_complete_msg = FileCompleteMessage {
        data_type: "artists".to_string(),
        timestamp: chrono::Utc::now(),
        total_processed: 42,
        file: "discogs_20250101_artists.xml.gz".to_string(),
    };

    let message = Message::FileComplete(file_complete_msg);
    let json_str = serde_json::to_string(&message).unwrap();

    // Verify the tagged enum produces "type": "file_complete"
    assert!(json_str.contains(r#""type":"file_complete""#), "Expected type tag, got: {}", json_str);
    assert!(json_str.contains(r#""data_type":"artists""#), "Expected data_type field, got: {}", json_str);
    assert!(json_str.contains(r#""total_processed":42"#), "Expected total_processed field, got: {}", json_str);
    assert!(json_str.contains(r#""file":"discogs_20250101_artists.xml.gz""#), "Expected file field, got: {}", json_str);
    assert!(json_str.contains(r#""timestamp""#), "Expected timestamp field, got: {}", json_str);
}

#[test]
fn test_data_message_serialization_format() {
    let data_msg =
        DataMessage { id: "456".to_string(), sha256: "deadbeef".to_string(), data: serde_json::json!({"name": "Test Artist"}), raw_xml: None };

    let message = Message::Data(data_msg);
    let json_str = serde_json::to_string(&message).unwrap();

    // Verify the tagged enum produces "type": "data"
    assert!(json_str.contains(r#""type":"data""#), "Expected type tag, got: {}", json_str);
    assert!(json_str.contains(r#""id":"456""#), "Expected id field, got: {}", json_str);
    assert!(json_str.contains(r#""sha256":"deadbeef""#), "Expected sha256 field, got: {}", json_str);
    // DataMessage uses #[serde(flatten)] on data, so fields appear at top level
    assert!(json_str.contains(r#""name":"Test Artist""#), "Expected flattened data field, got: {}", json_str);
}

#[test]
fn test_message_properties_content_type() {
    let props = MessageQueue::message_properties();
    let content_type = props.content_type().as_ref().expect("content_type should be set");
    assert_eq!(content_type.as_str(), "application/json");
}

#[tokio::test]
async fn test_get_channel_with_no_channel_triggers_reconnect() {
    // When channel is None, get_channel should attempt to reconnect.
    // With an invalid URL, reconnection fails — exercising the slow path
    // including the reconnect_mutex lock and double-check pattern.
    let mq = MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: "amqp://localhost:59999".to_string(),
        max_retries: 1,
        exchange_prefix: DEFAULT_EXCHANGE_PREFIX.to_string(),
        pacer: None,
    };

    let result = mq.get_channel().await;
    assert!(result.is_err(), "Expected error when channel is None and reconnect fails");
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("Failed to connect") || err_msg.contains("Failed to get channel"), "Unexpected error: {}", err_msg);
}

#[tokio::test]
async fn test_close_with_no_channel_or_connection_is_ok() {
    // Regression: close() must be a best-effort no-op
    // (never early-return via `?`) when there's nothing to close — the
    // happy-path baseline for the "always attempt both closes" contract.
    let mq = MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: String::new(),
        max_retries: 1,
        exchange_prefix: DEFAULT_EXCHANGE_PREFIX.to_string(),
        pacer: None,
    };

    let result = mq.close().await;
    assert!(result.is_ok(), "close() with no channel/connection must succeed: {:?}", result.err());
    // Both slots must remain drained (taken) after close().
    assert!(mq.channel.read().await.is_none());
    assert!(mq.connection.read().await.is_none());
}

#[tokio::test]
async fn test_new_connection_failure_with_retries() {
    // Use 2 retries to exercise the retry backoff loop (lines 72-75):
    // - First attempt: try_connect fails, retry_count=1 < 2, warn + sleep(1s) + backoff doubled
    // - Second attempt: try_connect fails, retry_count=2 >= 2, return error
    let result = MessageQueue::new("amqp://localhost:59999", 2, DEFAULT_EXCHANGE_PREFIX).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("Failed to connect to AMQP broker after retries"), "Unexpected error: {}", err_msg);
}

// ── Publisher-confirm timeout ────────────────────────────

#[tokio::test]
async fn test_await_confirm_accepts_ack() {
    let result = MessageQueue::await_confirm(async { Ok(Confirmation::Ack(None)) }).await;
    assert!(result.is_ok(), "an acked publish must succeed");
}

#[tokio::test]
async fn test_await_confirm_passes_nack_through() {
    // Nack rejection itself lives in check_confirmation;
    // await_confirm's contract is to surface the confirmation for that check.
    let confirm = MessageQueue::await_confirm(async { Ok(Confirmation::Nack(None)) }).await.expect("a resolved confirm must not error");
    assert!(!confirm.is_ack(), "a nack must surface to the caller for check_confirmation");
}

#[tokio::test(start_paused = true)]
async fn test_await_confirm_times_out_on_stall() {
    // A broker under a memory / disk_free alarm keeps the connection up while
    // withholding acks, so the confirm future never resolves. Without a
    // deadline the publisher task parks forever, the bounded batch channel
    // fills, the batcher blocks, and the whole per-file pipeline wedges —
    // SIGTERM cannot break it because the shutdown flag is only polled between
    // files.
    let stalled = std::future::pending::<lapin::Result<Confirmation>>();

    let result = MessageQueue::await_confirm(stalled).await;

    let err = result.expect_err("a stalled confirm must time out, not hang");
    assert!(err.to_string().contains("Timed out"), "unexpected error: {}", err);
}

#[test]
fn test_publish_confirm_timeout_is_bounded() {
    assert!(PUBLISH_CONFIRM_TIMEOUT > Duration::ZERO);
    assert!(PUBLISH_CONFIRM_TIMEOUT <= Duration::from_secs(60));
}

/// Regression: publishes must be mandatory, otherwise the broker
/// acks and silently drops messages that route to zero queues.
#[test]
fn test_publish_options_are_mandatory() {
    let options = MessageQueue::publish_options();
    assert!(options.mandatory, "publishes must be mandatory so unroutable messages are returned, not dropped");
    assert!(!options.immediate, "immediate is deprecated by RabbitMQ and must stay off");
}

#[test]
fn test_confirmation_ack_without_return() {
    // The healthy case: acked and routed.
    assert!(check_confirmation("groovemap-discogs-artists", true, None).is_ok());
}

#[test]
fn test_confirmation_returned_is_failure() {
    // RabbitMQ answers a mandatory publish that matched no queue with basic.return
    // FOLLOWED BY basic.ack — the ack alone must not be read as delivery.
    let err = check_confirmation("groovemap-discogs-artists", true, Some((312, "NO_ROUTE".to_string())))
        .expect_err("an unroutable message must fail the publish");
    let message = err.to_string();
    assert!(message.contains("groovemap-discogs-artists"), "error should name the exchange: {message}");
    assert!(message.contains("NO_ROUTE") && message.contains("312"), "error should carry the broker's reason: {message}");
}

#[test]
fn test_confirmation_nack_is_failure() {
    let err = check_confirmation("groovemap-discogs-artists", false, None).expect_err("a nack must fail the publish");
    assert!(err.to_string().contains("not acknowledged"), "unexpected error: {err}");
}

#[test]
fn test_confirmation_nack_with_return() {
    // A nack that also carries a returned message reports the routing failure.
    let err = check_confirmation("groovemap-discogs-releases", false, Some((312, "NO_ROUTE".to_string()))).expect_err("must fail");
    assert!(err.to_string().contains("unroutable"), "unexpected error: {err}");
}

/// A publish made from inside a PRODUCER span carries the W3C trace context on the message
/// properties, and a consumer extracting those headers joins the producer's trace.
///
/// There is no way to build a `lapin::Channel` without a broker, so this exercises the
/// publish path at the seam that actually writes the wire frame: the properties every
/// `basic_publish` in this module is handed.
#[test]
fn test_message_properties_carry_the_trace_context() {
    use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;

    opentelemetry::global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let provider = SdkTracerProvider::builder().with_simple_exporter(InMemorySpanExporter::default()).build();
    let subscriber = tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("message-queue-tests")));

    let (properties, trace_id) = tracing::subscriber::with_default(subscriber, || {
        let span = telemetry::publish_span("groovemap-discogs-artists");
        let _entered = span.enter();
        (MessageQueue::message_properties(), span.context().span().span_context().trace_id())
    });

    let headers = properties.headers().as_ref().expect("a traced publish must carry a header table");
    assert!(headers.contains_key(telemetry::TRACEPARENT_HEADER), "traceparent must be on the published properties, saw {headers:?}");

    let extracted = telemetry::extract_trace_context(headers);
    assert_eq!(extracted.span().span_context().trace_id(), trace_id, "a consumer extracting the published headers must join the producer's trace");

    // The rest of the contract is untouched by the injection.
    assert_eq!(properties.delivery_mode(), &Some(2));
    assert_eq!(properties.content_type().as_ref().expect("content_type").as_str(), "application/json");
}

#[test]
fn test_publish_rate_from_env_value() {
    assert_eq!(publish_rate_from(None), Some(DEFAULT_PUBLISH_RATE));
    assert_eq!(publish_rate_from(Some("  ")), Some(DEFAULT_PUBLISH_RATE));
    assert_eq!(publish_rate_from(Some("250")), Some(250.0));
    assert_eq!(publish_rate_from(Some("0")), None, "0 disables pacing");
    assert_eq!(publish_rate_from(Some("fast")), Some(DEFAULT_PUBLISH_RATE), "garbage must not silently disable the limit");
    assert_eq!(publish_rate_from(Some("-5")), Some(DEFAULT_PUBLISH_RATE));
    assert_eq!(publish_rate_from(Some("NaN")), Some(DEFAULT_PUBLISH_RATE));
}

#[tokio::test(start_paused = true)]
async fn test_token_bucket_paces_to_rate() {
    let start = Instant::now();
    let mut bucket = TokenBucket::new(1000.0, start);
    // The burst (100 ms of rate) is free; after that every token costs 1 ms.
    let mut waited = Duration::ZERO;
    for _ in 0..1100 {
        waited += bucket.take(start);
    }
    // All 1100 takes at the same instant: the 1000 beyond the 100-token burst queue up
    // behind each other, so the last one waits ~1 s.
    let last = bucket.take(start);
    assert!(last >= Duration::from_millis(990) && last <= Duration::from_millis(1010), "last wait was {last:?}");
    assert!(waited > Duration::ZERO);
    // After the reservations drain, the bucket is not left in debt.
    assert_eq!(bucket.take(start + Duration::from_secs(5)), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn test_pace_sleeps_so_total_rate_holds() {
    let mq = MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: String::new(),
        max_retries: 1,
        exchange_prefix: DEFAULT_EXCHANGE_PREFIX.to_string(),
        pacer: Some(Arc::new(Mutex::new(TokenBucket::new(1500.0, Instant::now())))),
    };
    let started = Instant::now();
    for _ in 0..3150 {
        mq.pace().await;
    }
    // 3150 records at 1500/s with a 150-token burst: 2 s of pacing.
    let secs = started.elapsed().as_secs_f64();
    assert!((1.95..=2.05).contains(&secs), "3150 paced records took {secs}s");
}

/// Real broker, memory alarm raised: `publish_batch` must return Err at the confirm deadline
/// instead of parking forever (commit 57ec207 parked past 100 s).
///
/// Needs a disposable RabbitMQ. Set `GM_TEST_AMQP_URL` and `GM_TEST_RABBITMQ_CONTAINER` (the
/// docker container name running it); the test raises and then clears the alarm with
/// `rabbitmqctl`. Run: `cargo test --locked publish_batch_errors_under_memory_alarm -- --ignored`.
#[tokio::test]
#[ignore = "needs a disposable real RabbitMQ; see the doc comment"]
async fn publish_batch_errors_under_memory_alarm() {
    let url = std::env::var("GM_TEST_AMQP_URL").expect("GM_TEST_AMQP_URL");
    let container = std::env::var("GM_TEST_RABBITMQ_CONTAINER").expect("GM_TEST_RABBITMQ_CONTAINER");
    let ctl = |args: &[&str]| {
        let out = std::process::Command::new("docker")
            .arg("exec")
            .arg(&container)
            .arg("rabbitmqctl")
            .args(args)
            .output()
            .expect("docker exec");
        assert!(out.status.success(), "rabbitmqctl {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };

    let prefix = format!("alarmtest{}", std::process::id());
    let mq = MessageQueue::new(&url, 3, &prefix).await.expect("connect");
    mq.setup_exchange(DataType::Releases).await.expect("exchange");
    let conn = Connection::connect(&url, ConnectionProperties::default()).await.expect("conn");
    let ch = conn.create_channel().await.expect("ch");
    let queue = ch
        .queue_declare("".into(), QueueDeclareOptions { exclusive: true, ..Default::default() }, FieldTable::default())
        .await
        .expect("queue");
    ch.queue_bind(queue.name().clone(), format!("{prefix}-releases").into(), "".into(), QueueBindOptions::default(), FieldTable::default())
        .await
        .expect("bind");

    let batch = |n: usize| -> Vec<DataMessage> {
        (0..n)
            .map(|i| DataMessage { id: i.to_string(), sha256: "0".repeat(64), data: serde_json::json!({"id": i}), raw_xml: None })
            .collect()
    };

    // Healthy broker: a batch succeeds.
    mq.publish_batch(batch(50), DataType::Releases).await.expect("healthy publish");

    // Raise the memory alarm: the broker blocks publishers while the TCP connection stays up.
    ctl(&["set_vm_memory_high_watermark", "absolute", "1MB"]);
    let started = std::time::Instant::now();
    // Enough data to fill socket buffers and park a write as well as an ack.
    let result = tokio::time::timeout(Duration::from_secs(PUBLISH_CONFIRM_TIMEOUT.as_secs() + 30), async {
        let mut r = Ok(());
        for _ in 0..200 {
            r = mq.publish_batch(batch(500), DataType::Releases).await;
            if r.is_err() {
                break;
            }
        }
        r
    })
    .await;
    let elapsed = started.elapsed();
    ctl(&["set_vm_memory_high_watermark", "0.4"]);
    let result = result.expect("publish_batch parked past the deadline under a memory alarm");
    assert!(result.is_err(), "a publish under a memory alarm must fail, not succeed");
    assert!(
        elapsed >= PUBLISH_CONFIRM_TIMEOUT - Duration::from_secs(1) && elapsed < PUBLISH_CONFIRM_TIMEOUT + Duration::from_secs(10),
        "failed after {elapsed:?}, expected about {PUBLISH_CONFIRM_TIMEOUT:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn test_two_queues_sharing_a_pacer_hold_one_total_rate() {
    let shared: SharedPacer = Some(Arc::new(Mutex::new(TokenBucket::new(1500.0, Instant::now()))));
    let queue = |pacer: SharedPacer| MessageQueue {
        connection: Arc::new(RwLock::new(None)),
        channel: Arc::new(RwLock::new(None)),
        reconnect_mutex: tokio::sync::Mutex::new(()),
        url: String::new(),
        max_retries: 1,
        exchange_prefix: DEFAULT_EXCHANGE_PREFIX.to_string(),
        pacer,
    };
    let (a, b) = (queue(shared.clone()), queue(shared));
    let started = Instant::now();
    let run = |mq: MessageQueue| async move {
        for _ in 0..1575 {
            mq.pace().await;
        }
    };
    tokio::join!(run(a), run(b));
    // 3150 records across two instances at one 1500/s bucket with a 150-token burst: 2 s,
    // not the 1 s two independent buckets would take.
    let secs = started.elapsed().as_secs_f64();
    assert!((1.95..=2.05).contains(&secs), "two instances took {secs}s for 3150 records");
}
