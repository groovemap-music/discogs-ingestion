//! Provider-neutral runtime services shared by both ingestion paths.
//!
//! This module owns only mechanics that have identical semantics for Discogs and
//! MusicBrainz. Provider acquisition, parsing, transformation, and orchestration
//! must remain outside this boundary.

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use crate::message_queue::{MessagePublisher, MessageQueue};
use crate::state_marker::StateMarker;
use crate::telemetry;
use crate::types::{DataMessage, DataType, ExtractionProgress};

/// Creates message publishers at the runtime boundary.
#[cfg_attr(feature = "test-support", mockall::automock)]
#[async_trait]
pub trait MessageQueueFactory: Send + Sync {
    async fn create(&self, url: &str, exchange_prefix: &str) -> Result<Arc<dyn MessagePublisher>>;
}

/// Production message-queue factory.
pub struct DefaultMessageQueueFactory;

#[async_trait]
impl MessageQueueFactory for DefaultMessageQueueFactory {
    async fn create(&self, url: &str, exchange_prefix: &str) -> Result<Arc<dyn MessagePublisher>> {
        Ok(Arc::new(MessageQueue::new(url, 3, exchange_prefix).await?))
    }
}

/// State shared across the extractor
#[derive(Debug, Default)]
pub struct ExtractorState {
    pub extraction_progress: ExtractionProgress,
    pub last_extraction_time: HashMap<DataType, Instant>,
    pub completed_files: HashSet<String>,
    pub active_connections: HashMap<DataType, String>,
    pub error_count: u64,
    pub extraction_status: ExtractionStatus,
}

/// Lifecycle status of the extraction process.
///
/// Transitions:
/// - `Idle` — initial state before any extraction runs
/// - `Running` — actively processing a run (set at the top of `process_*_data`)
/// - `Completed` — transient success state set by `process_*_data` at the end of a run
/// - `Waiting` — set by `run_*_loop` right before the periodic sleep; the dominant observable
///   success state during the 5-day wait between checks. Downstream consumers (MusicBrainz
///   extractor waiting on Discogs health, admin dashboard tracker) treat `waiting` as terminal
///   success equivalent to `completed`.
/// - `Failed` — the last run failed; persists through the sleep window so operators can see it,
///   and is overwritten to `Running` when the next attempt begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtractionStatus {
    #[default]
    Idle,
    Running,
    Completed,
    Waiting,
    Failed,
}

impl ExtractionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExtractionStatus::Idle => "idle",
            ExtractionStatus::Running => "running",
            ExtractionStatus::Completed => "completed",
            ExtractionStatus::Waiting => "waiting",
            ExtractionStatus::Failed => "failed",
        }
    }
}

/// Provider-neutral batching configuration.
pub struct BatcherConfig {
    pub batch_size: usize,
    pub data_type: DataType,
    pub state: Arc<RwLock<ExtractorState>>,
    pub state_marker: Arc<tokio::sync::Mutex<StateMarker>>,
    pub marker_path: PathBuf,
    pub file_name: String,
    pub state_save_interval: usize,
}

/// Batch messages for efficient publishing
pub async fn message_batcher(mut receiver: mpsc::Receiver<DataMessage>, sender: mpsc::Sender<Vec<DataMessage>>, config: BatcherConfig) -> Result<()> {
    let BatcherConfig { batch_size, data_type, state, state_marker, marker_path, file_name, state_save_interval } = config;
    let mut batch = Vec::with_capacity(batch_size);
    let mut last_flush = Instant::now();
    let mut total_records = 0u64;
    let mut total_batches = 0u64;
    let mut last_state_save = 0u64;

    loop {
        match tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await {
            Ok(Some(message)) => {
                batch.push(message);
                total_records += 1;

                {
                    let mut s = state.write().await;
                    s.extraction_progress.increment(data_type);
                    s.last_extraction_time.insert(data_type, Instant::now());
                }

                if total_records.is_multiple_of(state_save_interval as u64) && total_records != last_state_save {
                    last_state_save = total_records;
                    let mut marker = state_marker.lock().await;
                    marker.update_file_progress(&file_name, total_records, total_records, total_batches);
                    if let Err(e) = marker.save(&marker_path).await {
                        warn!("⚠️ Failed to save state marker progress: {}", e);
                    } else {
                        debug!("💾 Saved state marker progress: {} records, {} batches for {}", total_records, total_batches, file_name);
                    }
                }

                if batch.len() >= batch_size {
                    let messages = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    // Count extracted records once per batch, not once per record: a monthly
                    // dump is O(100M) records and the total is identical either way.
                    telemetry::record_records(data_type.as_str(), messages.len() as u64);
                    sender.send(messages).await?;
                    total_batches += 1;
                    last_flush = Instant::now();
                }
            }
            Ok(None) => {
                if !batch.is_empty() {
                    telemetry::record_records(data_type.as_str(), batch.len() as u64);
                    sender.send(batch).await?;
                    total_batches += 1;
                }
                break;
            }
            Err(_) => {
                if !batch.is_empty() && last_flush.elapsed() > Duration::from_secs(1) {
                    let messages = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    telemetry::record_records(data_type.as_str(), messages.len() as u64);
                    sender.send(messages).await?;
                    total_batches += 1;
                    last_flush = Instant::now();
                }
            }
        }
    }

    {
        let mut marker = state_marker.lock().await;
        marker.update_file_progress(&file_name, total_records, total_records, total_batches);
        if let Err(e) = marker.save(&marker_path).await {
            warn!("⚠️ Failed to save final state marker progress: {}", e);
        }
    }

    Ok(())
}

/// Publish provider batches through the message-publisher port.
pub async fn message_publisher(
    mut receiver: mpsc::Receiver<Vec<DataMessage>>,
    mq: Arc<dyn MessagePublisher>,
    data_type: DataType,
    state: Arc<RwLock<ExtractorState>>,
) -> Result<()> {
    while let Some(batch) = receiver.recv().await {
        match mq.publish_batch(batch, data_type).await {
            Ok(_) => {
                debug!("✅ Published batch to AMQP");
            }
            Err(e) => {
                error!("❌ Failed to publish batch: {}", e);
                telemetry::record_error(telemetry::Stage::Publish);
                let mut s = state.write().await;
                s.error_count += 1;
                return Err(e).context("Failed to publish batch to AMQP");
            }
        }
    }

    Ok(())
}

/// Progress reporter task
pub(crate) async fn progress_reporter(state: Arc<RwLock<ExtractorState>>, shutdown: Arc<tokio::sync::Notify>) {
    let mut report_count = 0;

    loop {
        let interval = if report_count < 3 { Duration::from_secs(10) } else { Duration::from_secs(30) };

        tokio::select! {
            _ = sleep(interval) => {},
            _ = shutdown.notified() => break,
        }

        report_count += 1;

        let s = state.read().await;
        let total = s.extraction_progress.total();

        let mut stalled = Vec::new();

        for (data_type, last_time) in &s.last_extraction_time {
            let is_completed = s.completed_files.iter().any(|f| f.contains(data_type.as_str()));
            if !is_completed && last_time.elapsed() > Duration::from_secs(120) {
                stalled.push(data_type.to_string());
            }
        }

        if !stalled.is_empty() {
            warn!("⚠️ Stalled extractors detected: {:?}", stalled);
        }

        info!(
            // Every type total() sums over must be listed, otherwise the parts do not add
            // up to the printed total on the MusicBrainz instance (release_groups) and the
            // type that is actually moving is hidden behind an always-zero Masters.
            "📊 Extraction Progress: {} total records (Artists: {}, Labels: {}, Masters: {}, Release Groups: {}, Releases: {})",
            total,
            s.extraction_progress.artists,
            s.extraction_progress.labels,
            s.extraction_progress.masters,
            s.extraction_progress.release_groups,
            s.extraction_progress.releases
        );

        if !s.completed_files.is_empty() {
            info!("🎉 Completed files: {:?}", s.completed_files);
        }

        if !s.active_connections.is_empty() {
            info!("🔗 Active connections: {:?}", s.active_connections.keys().collect::<Vec<_>>());
        }
    }
}

/// Wait for the trigger to be set, consume it, and return its force flag.
pub(crate) async fn wait_for_trigger(trigger: &Arc<tokio::sync::Mutex<Option<bool>>>) -> bool {
    loop {
        {
            let mut t = trigger.lock().await;
            if let Some(force_reprocess) = t.take() {
                return force_reprocess;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Reset a stuck `Running` extraction status to `Failed` after a periodic or API-triggered
/// check returns `Err`.
///
/// `process_discogs_data` / `process_musicbrainz_data` set the status to `Running` up-front but
/// only reset it on their fall-through tail; any early `?` error short-circuits before that reset,
/// leaving the status at `Running`. The periodic loops swallow the `Err` and sleep for
/// `periodic_check_days`, so without this backstop the status stays `Running` for the entire sleep —
/// wedging the manual `/trigger` recovery (health.rs returns 409 `already_running` before enqueuing
/// the trigger), starving the MusicBrainz extractor's `wait_for_discogs_idle` (which treats
/// `running` as busy), and misreporting `/health`. `Failed` is a terminal, non-`Running` state that
/// the periodic loop preserves (it only rewrites `Completed` -> `Waiting`) until the next successful
/// run. (cu2.41)
pub(crate) async fn reset_status_after_failed_check(state: &Arc<RwLock<ExtractorState>>) {
    let mut s = state.write().await;
    s.extraction_status = ExtractionStatus::Failed;
}

/// Spawn a task that watches `shutdown` and flips an `AtomicBool` when it fires.
///
/// `Notify::notified()` consumes its permit and `notify_waiters()` stores none, so long-running
/// processing code cannot await the `Notify` directly without stealing the signal from the loop's
/// outer `select!`. The monitor parks on the `Notify` once (before any multi-hour work) and records
/// the shutdown in a flag that processing code polls between files without side effects. Shared by
/// both the Discogs and MusicBrainz loops. (cu2.44)
pub(crate) fn spawn_shutdown_flag_monitor(shutdown: Arc<tokio::sync::Notify>) -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_for_monitor = flag.clone();
    tokio::spawn(async move {
        shutdown.notified().await;
        flag_for_monitor.store(true, Ordering::SeqCst);
    });
    flag
}

/// Decide the outcome of an initial (first-run) extraction from whether processing succeeded and
/// whether a shutdown was requested.
///
/// The processing functions collapse "interrupted by shutdown" and "processing error" into a single
/// `success: bool` — both surface as `Ok(false)`. Only the initial-run call sites promote `Ok(false)`
/// to `Err`, which sends main into `apply_failure_cooldown` (default 600s) + `exit(1)`. An
/// operator-requested SIGTERM would then be logged as a failure and hang ~10 min — long past Docker's
/// stop grace period, so the container is SIGKILLed instead of stopping cleanly, and orchestrators
/// that key on exit code may restart the service being stopped. A shutdown must therefore
/// short-circuit to `Ok(())` BEFORE the failure check. Shared by the Discogs and MusicBrainz initial
/// runs. (cu2.45)
pub(crate) fn initial_run_outcome(success: bool, shutdown_requested: bool, source_label: &str) -> Result<()> {
    if shutdown_requested {
        info!("🛑 Shutdown requested during initial {source_label} processing — exiting cleanly");
        return Ok(());
    }
    if !success {
        error!("❌ Initial {source_label} processing failed");
        return Err(anyhow::anyhow!("Initial {source_label} processing failed"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
