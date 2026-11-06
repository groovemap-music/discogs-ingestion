use anyhow::{Context, Result};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind, options::*, types::FieldTable};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
use tracing::{Instrument, debug, error, info, warn};
use url::Url;

use async_trait::async_trait;

use crate::telemetry;
use crate::types::{DataMessage, DataType, ExtractionCompleteMessage, FileCompleteMessage, Message};

#[cfg(test)]
const DEFAULT_EXCHANGE_PREFIX: &str = crate::generated::catalog_contract::DEFAULT_EXCHANGE_PREFIX;
const AMQP_EXCHANGE_TYPE: ExchangeKind = ExchangeKind::Fanout;

/// Deadline for a publisher-confirm to come back from the broker.
///
/// Publisher confirms are enabled (`confirm_select`), and awaiting the confirm
/// future had no timeout. When RabbitMQ trips a memory or `disk_free` alarm it
/// stops acking publishes while leaving the connection TCP-connected, so
/// `get_channel`'s reconnect path — which only fires on a DISCONNECTED channel —
/// never triggers. The publisher task then parks forever, the bounded
/// batch channel fills, the batcher blocks on `send().await`, and the whole
/// per-file pipeline wedges. The shutdown flag is only polled BETWEEN files, so
/// SIGTERM cannot break it and the container is SIGKILLed.
const PUBLISH_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable that sets the publish pacing, in records per second across all files.
/// `0` disables pacing.
pub const PUBLISH_RATE_ENV: &str = "PUBLISH_MAX_RECORDS_PER_SEC";

/// Default pacing. Consumers write roughly 530 to 800 records a second, so publishing faster
/// only moves records into RabbitMQ queues instead of finishing the load sooner.
const DEFAULT_PUBLISH_RATE: f64 = 1500.0;

/// Burst allowance as a fraction of a second of the configured rate.
const PUBLISH_BURST_SECONDS: f64 = 0.1;

/// Resolve the pacing rate from the raw environment value.
///
/// Returns `None` when pacing is disabled (`0`). An unset, empty or unparseable value falls
/// back to the default rather than silently disabling the limit.
fn publish_rate_from(raw: Option<&str>) -> Option<f64> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        None => Some(DEFAULT_PUBLISH_RATE),
        Some(value) => match value.parse::<f64>() {
            Ok(0.0) => None,
            Ok(rate) if rate.is_finite() && rate > 0.0 => Some(rate),
            _ => {
                warn!("⚠️ Ignoring invalid {}={:?}; using the default of {} records/s", PUBLISH_RATE_ENV, value, DEFAULT_PUBLISH_RATE);
                Some(DEFAULT_PUBLISH_RATE)
            }
        },
    }
}

/// Token bucket that paces publishing. One token per record.
///
/// `take` reserves a token and returns how long the caller must wait before using it, so
/// concurrent publishers share one total rate without holding the lock across an await.
pub struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub(crate) fn new(rate: f64, now: Instant) -> Self {
        let capacity = (rate * PUBLISH_BURST_SECONDS).max(1.0);
        Self { rate, capacity, tokens: capacity, last: now }
    }

    fn take(&mut self, now: Instant) -> Duration {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-self.tokens / self.rate)
        }
    }
}

/// A pacing bucket shared by every publisher that must count against one total rate;
/// `None` means pacing is off.
pub type SharedPacer = Option<Arc<Mutex<TokenBucket>>>;

/// Build the pacing bucket from `PUBLISH_MAX_RECORDS_PER_SEC`. Call once per process and hand
/// the result to every `MessageQueue`, so the rate is a total across files, not per connection.
pub fn pacer_from_env() -> SharedPacer {
    publish_rate_from(std::env::var(PUBLISH_RATE_ENV).ok().as_deref()).map(|rate| Arc::new(Mutex::new(TokenBucket::new(rate, Instant::now()))))
}

/// Decide whether a publisher confirm actually proves delivery.
///
/// A broker ack only proves the message was RECEIVED, not that it was ROUTED: a fanout
/// exchange with zero bound queues acks and drops. Publishing with `mandatory` makes the
/// broker return such a message, and `returned` carries that `basic.return` — which must
/// be treated as a failure so the file stays pending instead of being durably marked
/// Completed while every record was discarded.
///
/// Scope limit: `mandatory` only detects total unroutability. If one consumer has
/// declared and bound its queue but another has not, the message is routable and this
/// check passes — partial-consumer loss is out of scope here.
fn check_confirmation(exchange_name: &str, acked: bool, returned: Option<(u16, String)>) -> Result<()> {
    if let Some((reply_code, reply_text)) = returned {
        return Err(anyhow::anyhow!(
            "Message to exchange '{}' was returned as unroutable ({} {}) — no consumer queue is bound",
            exchange_name,
            reply_code,
            reply_text
        ));
    }

    if !acked {
        return Err(anyhow::anyhow!("Message was not acknowledged by broker"));
    }

    Ok(())
}

#[cfg_attr(feature = "test-support", mockall::automock)]
#[async_trait]
pub trait MessagePublisher: Send + Sync {
    async fn setup_exchange(&self, data_type: DataType) -> Result<()>;
    async fn publish(&self, message: Message, data_type: DataType) -> Result<()>;
    async fn publish_batch(&self, messages: Vec<DataMessage>, data_type: DataType) -> Result<()>;
    async fn send_file_complete(&self, data_type: DataType, file_name: &str, total_processed: u64) -> Result<()>;
    async fn send_extraction_complete(
        &self,
        version: &str,
        started_at: chrono::DateTime<chrono::Utc>,
        record_counts: std::collections::HashMap<String, u64>,
        data_types: &[DataType],
    ) -> Result<()>;
    async fn close(&self) -> Result<()>;
}

pub struct MessageQueue {
    connection: Arc<RwLock<Option<Connection>>>,
    channel: Arc<RwLock<Option<Channel>>>,
    reconnect_mutex: tokio::sync::Mutex<()>,
    url: String,
    max_retries: u32,
    exchange_prefix: String,
    pacer: SharedPacer,
}

impl MessageQueue {
    /// Build the fanout exchange name for a given data type (e.g. "groovemap-discogs-artists")
    fn exchange_name(&self, data_type: DataType) -> String {
        format!("{}-{}", self.exchange_prefix, data_type)
    }

    /// Connect with a pacer of its own, read from the environment. Use `new_with_pacer` when
    /// several queues must share one total rate.
    pub async fn new(url: &str, max_retries: u32, exchange_prefix: &str) -> Result<Self> {
        Self::new_with_pacer(url, max_retries, exchange_prefix, pacer_from_env()).await
    }

    pub async fn new_with_pacer(url: &str, max_retries: u32, exchange_prefix: &str, pacer: SharedPacer) -> Result<Self> {
        let normalized_url = Self::normalize_amqp_url(url)?;

        let mq = Self {
            connection: Arc::new(RwLock::new(None)),
            channel: Arc::new(RwLock::new(None)),
            reconnect_mutex: tokio::sync::Mutex::new(()),
            url: normalized_url,
            max_retries,
            exchange_prefix: exchange_prefix.to_string(),
            pacer,
        };

        mq.connect().await?;
        Ok(mq)
    }

    /// Normalize AMQP URL to ensure compatibility with Python extractor
    ///
    /// Handles the case where a trailing slash in the URL (e.g., amqp://host:5672/)
    /// should be interpreted as the default vhost "/" rather than an empty vhost.
    /// This matches the behavior of Python's aio-pika library.
    pub(crate) fn normalize_amqp_url(url: &str) -> Result<String> {
        let mut parsed_url = Url::parse(url).context("Failed to parse AMQP URL")?;

        let path = parsed_url.path();

        // lapin treats a trailing slash as an empty vhost rather than the default vhost.
        if path == "/" {
            parsed_url.set_path("");
        }

        Ok(parsed_url.to_string())
    }

    async fn connect(&self) -> Result<()> {
        let mut retry_count = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            match self.try_connect().await {
                Ok(_) => {
                    info!("✅ Successfully connected to AMQP broker");
                    return Ok(());
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= self.max_retries {
                        return Err(e).context("Failed to connect to AMQP broker after retries");
                    }
                    warn!("⚠️ Failed to connect to AMQP (attempt {}/{}): {}", retry_count, self.max_retries, e);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn try_connect(&self) -> Result<()> {
        let conn = Connection::connect(&self.url, ConnectionProperties::default()).await.context("Failed to establish AMQP connection")?;

        let channel = conn.create_channel().await.context("Failed to create AMQP channel")?;

        channel.confirm_select(ConfirmSelectOptions::default()).await.context("Failed to enable publisher confirms")?;

        *self.connection.write().await = Some(conn);
        *self.channel.write().await = Some(channel);

        Ok(())
    }

    /// Properties for every published message, carrying the W3C trace context of the
    /// producer span this is called from.
    ///
    /// The header table is attached only when the propagator actually wrote something, so a
    /// build with traces disabled publishes exactly the frame it published before: the
    /// global propagator is then the SDK's no-op and the table stays empty.
    fn message_properties() -> BasicProperties {
        let mut headers = FieldTable::default();
        telemetry::inject_trace_context(&mut headers);

        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_content_encoding("UTF-8".into())
            .with_delivery_mode(2); // Persistent

        if headers.inner().is_empty() {
            properties
        } else {
            properties.with_headers(headers)
        }
    }

    /// Await a publisher-confirm under a bounded deadline.
    ///
    /// A stalled broker (resource alarm / flow control) keeps the connection up
    /// while withholding acks, so an unbounded await never resolves. Timing out
    /// turns the wedge into an error the caller can count and retry, and lets
    /// the task fail cleanly instead of parking the whole pipeline.
    async fn await_confirm<F>(confirm_future: F) -> Result<Confirmation>
    where
        F: std::future::Future<Output = lapin::Result<Confirmation>>,
    {
        match tokio::time::timeout(PUBLISH_CONFIRM_TIMEOUT, confirm_future).await {
            Ok(result) => result.context("Failed to confirm message delivery"),
            Err(_) => {
                error!("❌ Publisher confirm timed out after {:?} — broker may be under a resource alarm", PUBLISH_CONFIRM_TIMEOUT);
                Err(anyhow::anyhow!("Timed out after {:?} waiting for publisher confirm", PUBLISH_CONFIRM_TIMEOUT))
            }
        }
    }

    /// Publish options for every extractor publish.
    ///
    /// `mandatory` is required for correctness, not politeness: a publisher confirm on a
    /// fanout exchange with zero bound queues still returns `basic.ack`, so without this
    /// flag the broker silently drops the message and the extractor records the file as
    /// Completed. With `mandatory` set, an unroutable message comes back as a
    /// `basic.return` attached to the confirmation and is treated as a delivery failure.
    fn publish_options() -> BasicPublishOptions {
        BasicPublishOptions { mandatory: true, ..Default::default() }
    }

    /// Send one already-serialized payload and return its pending confirm without waiting.
    ///
    /// `basic_publish` only writes the frame; the broker's ack arrives later on the
    /// returned `PublisherConfirm`. Splitting the two lets a batch keep many publishes in
    /// flight at once instead of paying one broker round trip (a quorum-queue fsync on every
    /// bound queue) per message.
    ///
    /// The write itself can park too: under a memory alarm RabbitMQ blocks the connection and
    /// stops reading, so the socket buffer fills and `basic_publish` never returns. The send
    /// is therefore bounded by the same deadline as the confirm. Returns the pending confirm
    /// and how long the send took.
    async fn send_payload(&self, channel: &Channel, exchange_name: &str, payload: &[u8]) -> Result<(lapin::PublisherConfirm, Duration)> {
        let started = Instant::now();
        let sent = tokio::time::timeout(
            PUBLISH_CONFIRM_TIMEOUT,
            channel.basic_publish(exchange_name.into(), "".into(), Self::publish_options(), payload, Self::message_properties()),
        )
        .await;
        let elapsed = started.elapsed();
        match sent {
            Ok(result) => Ok((result.context("Failed to publish message")?, elapsed)),
            Err(_) => {
                telemetry::record_publish_confirm(elapsed);
                error!("❌ Publish send timed out after {:?} — broker may be under a resource alarm", PUBLISH_CONFIRM_TIMEOUT);
                Err(anyhow::anyhow!("Timed out after {:?} sending publish", PUBLISH_CONFIRM_TIMEOUT))
            }
        }
    }

    /// Wait for the pacing bucket to release a token. A no-op when pacing is disabled.
    async fn pace(&self) {
        let wait = match &self.pacer {
            Some(pacer) => pacer.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take(Instant::now()),
            None => return,
        };
        if !wait.is_zero() {
            sleep(wait).await;
        }
    }

    /// Wait for one pending confirm and require both a broker ack and proof that the
    /// message was routable. `send_elapsed` is the time `send_payload` already spent, so the
    /// recorded duration covers the whole broker wait for this message (send plus confirm).
    async fn settle_confirm(exchange_name: &str, publish: lapin::PublisherConfirm, send_elapsed: Duration) -> Result<()> {
        // Time only the broker wait, not serialization or pacing — this is the latency the
        // `groovemap.extraction.publish.confirm.duration` histogram is meant to show, and the
        // signal that goes flat when RabbitMQ trips a resource alarm.
        let confirm_started = Instant::now();
        let confirm = Self::await_confirm(publish).await;
        telemetry::record_publish_confirm(send_elapsed + confirm_started.elapsed());
        let confirm = confirm?;

        let acked = confirm.is_ack();
        let returned = confirm.take_message().map(|returned| (returned.reply_code, returned.reply_text.to_string()));

        let result = check_confirmation(exchange_name, acked, returned);
        if result.is_ok() {
            // Only a confirmed, routable message counts as sent.
            telemetry::record_messages_sent(exchange_name, 1);
        }
        result
    }

    /// Publish one already-serialized payload and require both a broker ack and proof
    /// that the message was routable.
    async fn publish_payload(&self, channel: &Channel, exchange_name: &str, payload: &[u8]) -> Result<()> {
        let (publish, send_elapsed) = self.send_payload(channel, exchange_name, payload).await?;
        Self::settle_confirm(exchange_name, publish, send_elapsed).await
    }

    async fn get_channel(&self) -> Result<Channel> {
        // Fast path: check if channel is connected under read lock
        {
            let channel_guard = self.channel.read().await;
            if let Some(channel) = &*channel_guard
                && channel.status().connected()
            {
                return Ok(channel.clone());
            }
        }

        // Slow path: serialize reconnection attempts to prevent multiple
        // concurrent callers from racing to create separate connections.
        let _reconnect_guard = self.reconnect_mutex.lock().await;

        // Re-check after acquiring the mutex — another caller may have
        // already reconnected while we were waiting.
        {
            let channel_guard = self.channel.read().await;
            if let Some(channel) = &*channel_guard
                && channel.status().connected()
            {
                return Ok(channel.clone());
            }
        }

        warn!("⚠️ AMQP channel lost, attempting to reconnect...");
        telemetry::record_reconnect(telemetry::MESSAGING_SYSTEM_RABBITMQ);
        self.connect().await?;

        self.channel.read().await.as_ref().cloned().ok_or_else(|| anyhow::anyhow!("Failed to get channel after reconnection"))
    }
}

#[async_trait]
impl MessagePublisher for MessageQueue {
    async fn setup_exchange(&self, data_type: DataType) -> Result<()> {
        let channel = self.get_channel().await?;
        let exchange_name = self.exchange_name(data_type);

        channel
            .exchange_declare(
                exchange_name.as_str().into(),
                AMQP_EXCHANGE_TYPE,
                ExchangeDeclareOptions { durable: true, auto_delete: false, ..Default::default() },
                FieldTable::default(),
            )
            .await
            .context(format!("Failed to declare fanout exchange for {}", data_type))?;

        debug!("✅ Declared fanout exchange: {}", exchange_name);

        Ok(())
    }

    async fn publish(&self, message: Message, data_type: DataType) -> Result<()> {
        let channel = self.get_channel().await?;
        let exchange_name = self.exchange_name(data_type);
        let payload = serde_json::to_vec(&message).context("Failed to serialize message")?;

        // A control message (file-complete, extraction-complete) is a batch of one, and gets
        // the same PRODUCER span as a data batch so every published frame carries a
        // `traceparent` a consumer can hang its `process` span off.
        let span = telemetry::publish_span(&exchange_name);
        self.publish_payload(&channel, &exchange_name, &payload).instrument(span).await
    }

    async fn publish_batch(&self, messages: Vec<DataMessage>, data_type: DataType) -> Result<()> {
        let channel = self.get_channel().await?;
        let exchange_name = self.exchange_name(data_type);

        // One PRODUCER span per BATCH, not per message: a monthly dump is O(100M) records
        // and a span each would swamp the collector for no added detail. Every message in
        // the batch is injected with this span's context.
        let span = telemetry::publish_span(&exchange_name);

        async move {
            // Pipeline the batch: put every message on the wire first, then settle the
            // confirms in publish order. The guarantee is unchanged — `publish_batch`
            // returns Ok only when every message was acked AND routed, and any nack, return
            // or timeout fails the batch so the file stays pending. Confirm waiting is
            // bounded by the batch size, so memory and in-flight work stay bounded.
            let mut pending = Vec::with_capacity(messages.len());
            for message in messages {
                let payload = serde_json::to_vec(&Message::Data(message)).context("Failed to serialize message")?;

                // Pace before the send so the total publish rate stays near the consumers' write rate.
                self.pace().await;
                pending.push(self.send_payload(&channel, &exchange_name, &payload).await?);
            }

            for (publish, send_elapsed) in pending {
                Self::settle_confirm(&exchange_name, publish, send_elapsed).await?;
            }

            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn send_file_complete(&self, data_type: DataType, file_name: &str, total_processed: u64) -> Result<()> {
        let message =
            FileCompleteMessage { data_type: data_type.to_string(), timestamp: chrono::Utc::now(), total_processed, file: file_name.to_string() };

        self.publish(Message::FileComplete(message), data_type).await?;

        info!("🎉 File processing complete for {}! Total records processed: {}", data_type, total_processed);

        Ok(())
    }

    async fn send_extraction_complete(
        &self,
        version: &str,
        started_at: chrono::DateTime<chrono::Utc>,
        record_counts: std::collections::HashMap<String, u64>,
        data_types: &[DataType],
    ) -> Result<()> {
        let message = ExtractionCompleteMessage { version: version.to_string(), timestamp: chrono::Utc::now(), started_at, record_counts };

        // Publish to specified data type exchanges so every consumer queue receives it
        // Attempt all exchanges before returning, so a single failure doesn't prevent
        // other consumers from receiving the signal
        let mut errors = Vec::new();
        for data_type in data_types {
            if let Err(e) = self.publish(Message::ExtractionComplete(message.clone()), *data_type).await {
                error!("❌ Failed to send extraction_complete to {}: {}", data_type, e);
                errors.push(format!("{}: {}", data_type, e));
            }
        }

        if errors.is_empty() {
            info!("🏁 Extraction complete message sent to all {} exchanges (version: {})", data_types.len(), version,);
        } else {
            let succeeded = data_types.len() - errors.len();
            warn!("⚠️ Extraction complete sent to {}/{} exchanges (version: {})", succeeded, data_types.len(), version,);
            return Err(anyhow::anyhow!("Failed to send extraction_complete to {} exchange(s): {}", errors.len(), errors.join("; ")));
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // Best-effort on both halves: a channel-close error (typical when the
        // broker already dropped the connection) must not skip the connection
        // close. Every call site except one already treats close() as
        // best-effort (`let _ = mq.close().await;`); make the implementation
        // match that contract instead of early-returning via `?` and leaving
        // the connection open.
        if let Some(channel) = self.channel.write().await.take()
            && let Err(e) = channel.close(200, "Normal shutdown".into()).await
        {
            warn!("⚠️ Failed to close AMQP channel cleanly: {}", e);
        }

        if let Some(conn) = self.connection.write().await.take()
            && let Err(e) = conn.close(200, "Normal shutdown".into()).await
        {
            warn!("⚠️ Failed to close AMQP connection cleanly: {}", e);
        }

        info!("🔌 AMQP connection closed");
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/message_queue_tests.rs"]
mod tests;
