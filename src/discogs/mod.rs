//! Discogs-owned acquisition, XML transformation, and orchestration.
//!
//! The public operations are the provider capability consumed by the binary
//! composition root. Dependencies point inward to the provider-neutral runtime.

pub mod downloader;
pub mod local_manifest;
pub mod media;
pub mod normalize;
pub mod parser;
pub mod rules;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, sleep};
use tracing::{Instrument, debug, error, info, warn};

use crate::config::ExtractorConfig;
use crate::message_queue::MessagePublisher;
use crate::runtime::{
    BatcherConfig, ExtractionStatus, ExtractorState, MessageQueueFactory, initial_run_outcome, message_batcher, message_publisher, progress_reporter,
    reset_status_after_failed_check, spawn_shutdown_flag_monitor, wait_for_trigger,
};
use crate::state_marker::{PhaseStatus, ProcessingDecision, StateMarker};
use crate::telemetry;
use crate::types::{DataMessage, DataType, ExtractionProgress, calculate_content_hash};

use self::downloader::{DataSource, Downloader};
use self::parser::XmlParser;
use self::rules::{CompiledRulesConfig, FlaggedRecordWriter, QualityReport, Severity, apply_filters, evaluate_rules, should_skip_record};

pub async fn process_discogs_data(
    config: Arc<ExtractorConfig>,
    state: Arc<RwLock<ExtractorState>>,
    shutdown: Arc<tokio::sync::Notify>,
    // Pollable shutdown flag (set by the loop's monitor task on SIGTERM/SIGINT). Checked between
    // files so a signal delivered mid-run stops starting new files instead of being lost. (cu2.44)
    shutdown_flag: Arc<AtomicBool>,
    force_reprocess: bool,
    downloader: &mut dyn DataSource,
    mq_factory: Arc<dyn MessageQueueFactory>,
    compiled_rules: Option<Arc<CompiledRulesConfig>>,
) -> Result<bool> {
    // Record extraction start time for consumer cleanup coordination
    let extraction_started_at = chrono::Utc::now();

    // Mirror of the MusicBrainz guard: never begin a new run (status Running, S3 listing,
    // multi-GB downloads) when shutdown has already been requested. (discogsography-l114)
    if shutdown_flag.load(Ordering::SeqCst) {
        info!("🛑 Shutdown requested, not starting Discogs extraction");
        return Ok(false);
    }

    // Reset progress for new run
    {
        let mut s = state.write().await;
        s.extraction_progress = ExtractionProgress::default();
        s.last_extraction_time.clear();
        s.completed_files.clear();
        s.active_connections.clear();
        s.error_count = 0;
        s.extraction_status = ExtractionStatus::Running;
    }

    // Get file list to determine version
    let available_files = downloader.list_s3_files().await.context("Failed to list S3 files")?;
    let latest_files = downloader.get_latest_monthly_files(&available_files)?;

    if latest_files.is_empty() {
        warn!("⚠️ No data files found");
        let mut s = state.write().await;
        s.extraction_status = ExtractionStatus::Completed;
        return Ok(true);
    }

    // Extract version from first filename
    // `latest_files[0].name` is an S3 object key from the Discogs public bucket — operator-controlled, not user input.
    let first_filename = Path::new(&latest_files[0].name) // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;
    let version = extract_version_from_filename(first_filename).ok_or_else(|| anyhow::anyhow!("Could not extract version from filename"))?;

    info!("📋 Detected Discogs data version: {}", version);

    // Load or create state marker
    let marker_path = StateMarker::file_path(&config.discogs_root, &version);
    let mut state_marker = if force_reprocess {
        info!("🔄 Force reprocess requested, creating new state marker");
        StateMarker::new(version.clone())
    } else {
        StateMarker::load(&marker_path).await?.unwrap_or_else(|| StateMarker::new(version.clone()))
    };

    // Check what to do based on state marker
    let decision = state_marker.should_process();

    match decision {
        ProcessingDecision::Skip => {
            info!("✅ Version {} already processed, skipping", version);
            let mut s = state.write().await;
            s.extraction_status = ExtractionStatus::Completed;
            return Ok(true);
        }
        ProcessingDecision::Reprocess => {
            // Safe to discard the marker: should_process() only returns Reprocess when no
            // file has finished processing (otherwise it returns Continue, so an
            // interrupted download-verification pass cannot wipe processing progress).
            warn!("⚠️ Will re-download and re-process version {}", version);
            state_marker = StateMarker::new(version.clone());
        }
        ProcessingDecision::Continue => {
            info!("🔄 Will continue processing version {}", version);
        }
    }

    // Pass state marker to downloader for tracking download progress
    downloader.set_state_marker(state_marker, marker_path.clone());

    // Download latest data (this will now track timestamps properly)
    let data_files = downloader.download_discogs_data().await.context("Failed to download Discogs data")?;

    // Get state marker back from downloader
    let mut state_marker = downloader.take_state_marker().ok_or_else(|| anyhow::anyhow!("State marker missing after download"))?;

    // Filter out checksum files
    let data_files: Vec<_> = data_files.into_iter().filter(|f| !f.contains("CHECKSUM")).collect();

    if data_files.is_empty() {
        warn!("⚠️ No data files to process");
        let mut s = state.write().await;
        s.extraction_status = ExtractionStatus::Completed;
        return Ok(true);
    }

    // Start processing phase
    if state_marker.processing_phase.status == PhaseStatus::Pending {
        state_marker.start_processing(data_files.len());
        state_marker.save(&marker_path).await?;
        info!("🚀 Starting processing phase: {} total files", data_files.len());
    } else if state_marker.processing_phase.status == PhaseStatus::InProgress {
        // Resume: update total count but do not reset progress counters
        state_marker.processing_phase.files_total = data_files.len();
        state_marker.save(&marker_path).await?;
        info!("🔄 Resuming processing phase: {} total files, {} already completed", data_files.len(), state_marker.processing_phase.files_processed);

        // Rehydrate the per-run progress counters from the persisted per-file progress so the
        // /health endpoint reports true totals for types already completed before the crash.
        // Those files are skipped (pending_files) and never re-incremented this run, so without
        // this they would surface as 0 on the dashboard. (cu2.92)
        {
            let mut s = state.write().await;
            for (file_name, file_state) in &state_marker.processing_phase.progress_by_file {
                if file_state.status == PhaseStatus::Completed
                    && let Some(dt) = extract_data_type(file_name)
                {
                    s.extraction_progress.add(dt, file_state.records_extracted);
                }
            }
        }
    }

    // Use the ORIGINAL processing start time persisted in the state marker for the
    // extraction_complete signal — NOT this (possibly resumed) process's start time.
    // On a resumed run, files already completed in an earlier session are not re-sent,
    // so their rows retain updated_at from that earlier session. Broadcasting the
    // resumed process's now() would make the downstream stale-row purge delete every
    // row written before the restart (mass data loss). start_processing() sets this on
    // the first run and it is preserved (never reset) across InProgress resumes.
    let processing_started_at = state_marker.processing_phase.started_at.unwrap_or(extraction_started_at);

    // Get list of files that still need processing
    let pending_files = state_marker.pending_files(&data_files);

    if pending_files.is_empty() {
        info!("✅ All files already processed");

        // Tracks whether this path still owes a successful extraction_complete
        // broadcast. Stays true when there is nothing to send (the version is
        // already fully Completed from an earlier cycle).
        let mut broadcast_ok = true;

        // Only send extraction_complete on the first completion, not on
        // subsequent periodic checks where the version is already complete.
        if state_marker.summary.overall_status != PhaseStatus::Completed {
            // Mark processing (files are genuinely all done) but NOT extraction yet —
            // extraction is only "complete" once the extraction_complete broadcast lands.
            // Committing the marker fully Completed here (before the AMQP send) is a
            // split-commit ordering bug: a single AMQP failure would flip the marker to
            // Completed, every later cycle would Skip, and the completion signal would be
            // lost forever. Mirror the normal completion path: send first, finalize on
            // success, and on failure leave overall_status non-Completed so the next
            // cycle re-enters via Continue and retries. (cu2.42)
            state_marker.complete_processing();
            state_marker.save(&marker_path).await?;

            // Build record counts from the persisted per-file progress. Use data type
            // names (e.g., "artists") as keys — consistent with the normal completion
            // path so consumers can look up counts reliably.
            let mut record_counts = HashMap::new();
            for (file_name, file_state) in &state_marker.processing_phase.progress_by_file {
                if let Some(dt) = extract_data_type(file_name) {
                    record_counts.insert(dt.to_string(), file_state.records_extracted);
                }
            }

            let mut sent = false;
            match mq_factory.create(&config.amqp_connection, &config.discogs_exchange_prefix).await {
                Ok(mq) => {
                    match mq.send_extraction_complete(&version, processing_started_at, record_counts, &DataType::discogs()).await {
                        Ok(_) => sent = true,
                        Err(e) => error!("❌ Failed to send extraction_complete message: {}", e),
                    }
                    let _ = mq.close().await;
                }
                Err(e) => {
                    error!("❌ Failed to connect to AMQP for extraction_complete: {}", e);
                }
            }

            if sent {
                // Broadcast succeeded — now it is safe to durably mark extraction complete.
                state_marker.complete_extraction();
                state_marker.save(&marker_path).await?;
            } else {
                // Leave overall_status non-Completed so should_process() returns Continue and
                // the next cycle retries the broadcast instead of Skipping forever.
                error!("❌ extraction_complete not sent — leaving version incomplete to retry on next cycle");
                // ...and report the failure out of band too. Reporting Ok(true)/Completed
                // here deferred the retry to the next PERIODIC_CHECK_DAYS sleep (15 days by
                // default) while the health endpoint, dashboard, and logs all claimed
                // success. The normal completion path treats the identical failure as a
                // failure, which escalates to the cooldown-restart on the initial run and
                // retries within minutes (discogsography-d58d).
                broadcast_ok = false;
            }
        }

        let mut s = state.write().await;
        s.extraction_status = if broadcast_ok {
            ExtractionStatus::Completed
        } else {
            ExtractionStatus::Failed
        };
        return Ok(broadcast_ok);
    }

    info!("📋 Files to process: total={}, pending={}, completed={}", data_files.len(), pending_files.len(), data_files.len() - pending_files.len());

    debug!("📋 Pending files list: {:?}", pending_files);

    // Process files concurrently
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_workers)); // Limit concurrent files
    let mut tasks = Vec::new();
    let state_marker_arc = Arc::new(tokio::sync::Mutex::new(state_marker));

    for (idx, file) in pending_files.iter().enumerate() {
        debug!("📋 Spawning task {} for file: {}", idx, file);
        let file = file.clone(); // Clone the filename string
        let config = config.clone();
        let state = state.clone();
        let semaphore = semaphore.clone();
        let marker_path = marker_path.clone();
        let state_marker_arc = state_marker_arc.clone();
        let mq_factory = mq_factory.clone();
        let compiled_rules = compiled_rules.clone();
        let shutdown_flag = shutdown_flag.clone();

        let task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let _permit = semaphore.acquire().await?;
            // Graceful shutdown: stop starting new files once a SIGTERM/SIGINT arrives. Files
            // already past this guard run to completion; skipped files never call
            // start_file_processing, so they stay pending in the state marker and a restart
            // resumes them. (cu2.44)
            if shutdown_flag.load(Ordering::SeqCst) {
                info!("🛑 Shutdown requested — skipping not-yet-started file: {}", file);
                telemetry::record_file_outcome(telemetry::FileOutcome::Skipped);
                return Ok(());
            }
            let mq = mq_factory
                .create(&config.amqp_connection, &config.discogs_exchange_prefix)
                .await
                .context("Failed to connect to message queue")?;

            process_single_file(&file, config, state, state_marker_arc.clone(), marker_path.clone(), mq, compiled_rules).await?;

            info!("✅ Completed processing: {}", file);
            Ok(())
        });

        tasks.push(task);
    }

    info!("📋 Spawned {} tasks for processing", tasks.len());

    // Start progress reporter
    let reporter_state = state.clone();
    let reporter_shutdown = shutdown.clone();
    let reporter = tokio::spawn(async move {
        progress_reporter(reporter_state, reporter_shutdown).await;
    });

    // Wait for all tasks
    let mut success = true;
    for (i, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!("❌ File processing failed: {}", e);
                success = false;
            }
            Err(e) => {
                error!("❌ Task {} panicked: {}", i, e);
                success = false;
            }
        }
    }

    reporter.abort();

    // A shutdown signal delivered mid-run is a graceful stop, not a processing failure: in-flight
    // files (already past the per-task guard) finish, but files skipped by that guard stay pending.
    // Fold it into `success` so the completion/broadcast/status logic below does NOT finalize the
    // run — while logging it as a shutdown rather than an error. (cu2.44)
    let shutdown_requested = shutdown_flag.load(Ordering::SeqCst);
    if shutdown_requested {
        warn!("🛑 Shutdown requested during Discogs processing — saving progress for resume, not finalizing this run");
        success = false;
    }

    // Only mark processing as complete if all tasks succeeded
    {
        let mut state_marker = state_marker_arc.lock().await;
        if success {
            state_marker.complete_processing();
            // Don't mark extraction complete yet — wait until extraction_complete is sent
            state_marker.save(&marker_path).await?;
            info!("✅ Processing phase completed: version {}", state_marker.current_version);
        } else {
            // Save current progress without marking complete — allows restart to resume
            state_marker.save(&marker_path).await?;
            if shutdown_requested {
                info!("🛑 Processing interrupted by shutdown — progress saved for resume, not marking complete");
            } else {
                error!("❌ Processing phase finished with errors — not marking complete");
            }
        }
    } // Drop state_marker lock before re-acquiring below

    // Log completion and send extraction_complete only if all tasks succeeded
    {
        let s = state.read().await;
        info!("🎉 All processing complete! Finished files: {:?}", s.completed_files);
        info!("📊 Final statistics: {} total records extracted", s.extraction_progress.total());

        if success {
            // Build per-type record counts from the PERSISTED per-file progress, NOT the
            // per-run ExtractionProgress. That counter is reset at the top of every
            // process_discogs_data call and only tallies files processed in THIS run, so
            // after a crash-and-resume the types completed in an earlier session (skipped
            // via pending_files) would report 0. progress_by_file holds the true totals for
            // every completed file — matching the all-files-already-processed early path. (cu2.92)
            drop(s); // Release read lock before locking the state marker / async MQ operations
            let record_counts = {
                let state_marker = state_marker_arc.lock().await;
                let mut rc = HashMap::new();
                for (file_name, file_state) in &state_marker.processing_phase.progress_by_file {
                    if let Some(dt) = extract_data_type(file_name) {
                        rc.insert(dt.to_string(), file_state.records_extracted);
                    }
                }
                rc
            };

            // Send extraction_complete to all consumer queues
            match mq_factory.create(&config.amqp_connection, &config.discogs_exchange_prefix).await {
                Ok(mq) => {
                    if let Err(e) = mq.send_extraction_complete(&version, processing_started_at, record_counts, &DataType::discogs()).await {
                        error!("❌ Failed to send extraction_complete message: {}", e);
                        success = false;
                    }
                    let _ = mq.close().await;
                }
                Err(e) => {
                    error!("❌ Failed to connect to AMQP for extraction_complete: {}", e);
                    success = false;
                }
            }
        } else {
            drop(s);
            if shutdown_requested {
                info!("🛑 Skipping extraction_complete broadcast — shutdown in progress");
            } else {
                error!("❌ Skipping extraction_complete broadcast — processing had failures");
            }
        }
    }

    // Mark extraction complete in state marker only after extraction_complete was sent
    if success {
        let mut sm = state_marker_arc.lock().await;
        sm.complete_extraction();
        sm.save(&marker_path).await?;
    }

    // Update extraction status based on result
    {
        let mut s = state.write().await;
        s.extraction_status = if success { ExtractionStatus::Completed } else { ExtractionStatus::Failed };
    }

    Ok(success)
}

/// Process a single file
pub async fn process_single_file(
    file_name: &str,
    config: Arc<ExtractorConfig>,
    state: Arc<RwLock<ExtractorState>>,
    state_marker: Arc<tokio::sync::Mutex<StateMarker>>,
    marker_path: PathBuf,
    mq: Arc<dyn MessagePublisher>,
    compiled_rules: Option<Arc<CompiledRulesConfig>>,
) -> Result<()> {
    // Extract data type from filename
    let data_type = extract_data_type(file_name).ok_or_else(|| anyhow::anyhow!("Invalid file format: {}", file_name))?;

    info!("🚀 Starting extraction of {} from {}", data_type, file_name);

    // Mark file processing as started in state marker
    {
        let mut marker = state_marker.lock().await;
        marker.start_file_processing(file_name);
        marker.save(&marker_path).await?;
        info!("📋 Started file processing in state marker: {}", file_name);
    }

    // The INTERNAL root span for this file's processing. `parse` and every
    // `publish {destination}` below hang off it, and it stays alive until the whole pipeline
    // has drained so its duration is the file's, not the spawn's.
    let extract_span = telemetry::extract_span(data_type.as_str());

    // Declare fanout exchange for this data type
    mq.setup_exchange(data_type).await?;

    // Track active connection
    {
        let mut s = state.write().await;
        s.active_connections.insert(data_type, file_name.to_string());
    }

    // The pipeline below is wrapped so that the two pieces of non-RAII state acquired above
    // — the `active_connections` entry and the per-file AMQP connection — are released on
    // EVERY exit path, not just the success tail. Straight-line `?` propagation used to skip
    // both on any error, leaving a phantom active connection in /metrics until the next run's
    // state reset (periodic_check_days away) and tearing the broker connection down abruptly.
    // The MusicBrainz loop already removes its entry unconditionally; this matches it.
    let pipeline_result: Result<u64> = async {
        // Create channels for processing pipeline
        let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(config.queue_size);
        let (batch_sender, batch_receiver) = mpsc::channel::<Vec<DataMessage>>(100);

        // Start workers — with optional validator stage between parser and batcher
        let file_base_name = Path::new(file_name).file_name().and_then(|n| n.to_str()).unwrap_or(file_name); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
        let version = extract_version_from_filename(file_base_name).unwrap_or_else(|| "unknown".to_string());

        // Always-on normalization/hashing stage sits between the (optional) validator / parser and
        // the batcher, so published records carry the normalized shape and a real sha256 in BOTH the
        // rules and no-rules pipelines. (cu2.43)
        let (normalized_sender, normalized_receiver) = mpsc::channel::<DataMessage>(config.queue_size);

        let validator_handle = if let Some(rules) = compiled_rules {
            let (validated_sender, validated_receiver) = mpsc::channel::<DataMessage>(config.queue_size);
            let rules = rules.clone();
            let discogs_root = config.discogs_root.clone();
            let version_clone = version.clone();
            let data_type_str = data_type.as_str().to_string();

            let handle = tokio::spawn(async move {
                message_validator(parse_receiver, validated_sender, rules, &data_type_str, &discogs_root, &version_clone).await
            });

            // parser -> validator (skip/filter/rules) -> normalizer (normalize + hash) -> batcher
            let normalizer_handle = tokio::spawn(async move { message_normalizer(validated_receiver, normalized_sender, data_type).await });

            let batcher_config = BatcherConfig {
                batch_size: config.batch_size,
                data_type,
                state: state.clone(),
                state_marker: state_marker.clone(),
                marker_path: marker_path.clone(),
                file_name: file_name.to_string(),
                state_save_interval: config.state_save_interval,
            };
            let batcher_handle = tokio::spawn(async move { message_batcher(normalized_receiver, batch_sender, batcher_config).await });

            let parser_handle = tokio::spawn({
                let file_path = config.discogs_root.join(file_name); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
                // Built here, while the root span is current, so it becomes its child; then
                // carried across the spawn boundary, which `tracing` does not do on its own.
                let parse_span = telemetry::parse_span();
                async move {
                    let parser = XmlParser::with_options(data_type, parse_sender, true);
                    parser.parse_file(&file_path).await
                }
                .instrument(parse_span)
            });

            let publisher_handle = tokio::spawn({
                let mq = mq.clone();
                let state = state.clone();
                // The publisher task runs under the root span so each batch's PRODUCER span
                // is a child of this file's `extract`, not a detached root.
                let span = extract_span.clone();
                async move { message_publisher(batch_receiver, mq, data_type, state).await }.instrument(span)
            });

            let (parser_result, validator_result, normalizer_result, batcher_result, publisher_result) =
                tokio::try_join!(parser_handle, handle, normalizer_handle, batcher_handle, publisher_handle)?;
            let total_count = parser_result?;
            let report: QualityReport = validator_result?;
            normalizer_result?;
            batcher_result?;
            publisher_result?;

            if report.has_violations() {
                // file_name comes from S3 file listing (operator-controlled, not user input)
                let version_for_report = extract_version_from_filename(
                    Path::new(file_name).file_name().and_then(|n| n.to_str()).unwrap_or(""), // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
                )
                .unwrap_or_default();
                info!("{}", report.format_summary(&version_for_report));
            }

            Some(total_count)
        } else {
            // parser -> normalizer (normalize + hash) -> batcher — no validator, but normalization
            // and hashing still happen so consumers receive the same shape as the rules path. (cu2.43)
            let parser_handle = tokio::spawn({
                let file_path = config.discogs_root.join(file_name);
                let parse_span = telemetry::parse_span();
                async move {
                    let parser = XmlParser::new(data_type, parse_sender);
                    parser.parse_file(&file_path).await
                }
                .instrument(parse_span)
            });

            let normalizer_handle = tokio::spawn(async move { message_normalizer(parse_receiver, normalized_sender, data_type).await });

            let batcher_config = BatcherConfig {
                batch_size: config.batch_size,
                data_type,
                state: state.clone(),
                state_marker: state_marker.clone(),
                marker_path: marker_path.clone(),
                file_name: file_name.to_string(),
                state_save_interval: config.state_save_interval,
            };
            let batcher_handle = tokio::spawn(async move { message_batcher(normalized_receiver, batch_sender, batcher_config).await });

            let publisher_handle = tokio::spawn({
                let mq = mq.clone();
                let state = state.clone();
                let span = extract_span.clone();
                async move { message_publisher(batch_receiver, mq, data_type, state).await }.instrument(span)
            });

            let total_count = parser_handle.await??;
            normalizer_handle.await??;
            batcher_handle.await??;
            publisher_handle.await??;

            Some(total_count)
        };

        let total_count = validator_handle.unwrap_or(0);

        // Send the file completion message BEFORE marking the file Completed in
        // the state marker. If this AMQP send fails, the marker stays NOT
        // Completed, so pending_files() on the next run still includes this file
        // and send_file_complete is retried — instead of the signal being
        // silently and permanently dropped (the previous "marker first" ordering
        // let an AMQP failure land in the window after the marker was already
        // durably Completed, so a retry would skip the file and never resend).
        mq.send_file_complete(data_type, file_name, total_count).await?;

        // Now that the completion signal has been durably handed off to the
        // broker, mark the file as completed in the state marker.
        {
            let mut marker = state_marker.lock().await;
            marker.complete_file_processing(file_name, total_count);
            marker.save(&marker_path).await?;
            info!("✅ Completed file processing in state marker: {} ({} records)", file_name, total_count);
        }

        Ok(total_count)
    }
    .instrument(extract_span.clone())
    .await;

    // Update state. Only `completed_files` is success-conditional; the active-connection
    // entry must be dropped whether the pipeline succeeded or failed.
    {
        let mut s = state.write().await;
        if pipeline_result.is_ok() {
            s.completed_files.insert(file_name.to_string());
        }
        s.active_connections.remove(&data_type);
    }

    telemetry::record_file_outcome(if pipeline_result.is_ok() {
        telemetry::FileOutcome::Completed
    } else {
        telemetry::FileOutcome::Failed
    });

    // Clean up — best-effort, and unconditional. On the success path a cleanup failure is
    // purely cosmetic (the completion signal was already sent and the marker already
    // committed above), so it must not flip an otherwise fully-successful file to Failed
    // and trigger the failure cooldown. On the failure path this replaces an abrupt
    // connection drop (which RabbitMQ logs as 'client unexpectedly closed TCP connection')
    // with a graceful close.
    if let Err(e) = mq.close().await {
        warn!("⚠️ Failed to cleanly close per-file MQ connection for {}: {}", file_name, e);
    }

    let total_count = pipeline_result?;

    info!("✅ Completed processing {} with {} records", file_name, total_count);
    Ok(())
}

/// Validate Discogs messages against optional data-quality rules.
/// All non-skipped messages are forwarded downstream.
pub async fn message_validator(
    mut receiver: mpsc::Receiver<DataMessage>,
    sender: mpsc::Sender<DataMessage>,
    rules: Arc<CompiledRulesConfig>,
    data_type: &str,
    discogs_root: &Path,
    version: &str,
) -> Result<QualityReport> {
    let mut report = QualityReport::new();
    let mut writer = FlaggedRecordWriter::new(discogs_root, version);

    while let Some(mut message) = receiver.recv().await {
        report.increment_total(data_type);

        // Skip check — records matching skip conditions are not forwarded
        if let Some(skip_info) = should_skip_record(&rules, data_type, &message.data) {
            info!("⏭️ Skipping record {} ({}): {}", message.id, data_type, skip_info.reason);
            report.record_skip(data_type, &message.id, &skip_info.reason);
            writer.write_skip(data_type, &message.id, &skip_info, message.raw_xml.as_deref(), &message.data);
            continue;
        }

        // Apply filters — mutate message data in-place before validation
        let filter_actions = apply_filters(&rules, data_type, &mut message.data);
        for action in &filter_actions {
            info!(
                "🔧 Filtered {} value(s) from {} in {} {}: removed {:?}, reason: {}",
                action.removed_count, action.field, data_type, message.id, action.removed_values, action.reason
            );
        }

        // Evaluate rules on XML-shaped (pre-normalization) data — rules use dot-notation
        // paths like "genres.genre" that match the raw XML structure. Normalization and
        // content hashing are NOT done here anymore: they run unconditionally in
        // `message_normalizer`, downstream of this optional stage, so they happen even when
        // DATA_QUALITY_RULES is unset and this validator never runs. (cu2.43)
        let violations = evaluate_rules(&rules, data_type, &message.data);

        for violation in &violations {
            report.record_violation(data_type, &violation.rule_name, &violation.severity);
            let capture_files = matches!(violation.severity, Severity::Error | Severity::Warning);
            writer.write_violation(data_type, &message.id, violation, message.raw_xml.as_deref(), &message.data, capture_files);
        }
        if sender.send(message).await.is_err() {
            warn!("⚠️ Validator: downstream receiver dropped");
            break;
        }
    }

    writer.flush();
    writer.write_report(&report, version);
    Ok(report)
}

/// Normalize XML-shaped records into the flat, consumer-ready shape and compute the content
/// hash — UNCONDITIONALLY, as an always-on pipeline stage between the parser/validator and the
/// batcher.
///
/// Previously `normalize_record` + `calculate_content_hash` ran only inside `message_validator`,
/// which is spawned only when DATA_QUALITY_RULES is set. Without that env var the no-rules
/// pipeline published raw xmltodict-shaped records (`@`-prefixed keys, `{"name": [...]}`
/// container wrappers) with an empty `sha256`. That crashed graphinator (AttributeError iterating
/// container dicts) and defeated change detection (`"" == ""` skips every future update). Running
/// this stage in both branches keeps the published shape identical regardless of rules config.
/// (cu2.43)
pub async fn message_normalizer(mut receiver: mpsc::Receiver<DataMessage>, sender: mpsc::Sender<DataMessage>, data_type: DataType) -> Result<()> {
    let data_type_str = data_type.as_str();
    while let Some(mut message) = receiver.recv().await {
        // Normalize the XML-shaped JSON into the flat, consumer-ready format, then compute the
        // content hash from the post-normalization data so consumers detect real changes.
        self::normalize::normalize_record(data_type_str, &mut message.data);
        // Releases additionally carry the canonical media block (ADR 0007), computed here —
        // once, at the producer's normalization boundary — from the normalized `formats`
        // list, which stays untouched as the provenance record. It is attached before the
        // hash so the hash covers it and consumers detect a vocabulary-driven change.
        if matches!(data_type, DataType::Releases) {
            self::media::attach_media_block(&mut message.data);
        }
        message.sha256 = calculate_content_hash(&message.data);
        if sender.send(message).await.is_err() {
            warn!("⚠️ Normalizer: downstream receiver dropped");
            break;
        }
    }
    Ok(())
}

/// Extract a Discogs data type from its dump filename.
pub(crate) fn extract_data_type(filename: &str) -> Option<DataType> {
    // Format: discogs_YYYYMMDD_datatype.xml.gz
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() >= 3 {
        let type_part = parts[2].split('.').next()?;
        DataType::from_str(type_part).ok()
    } else {
        None
    }
}

/// Extract version from filename (e.g., "discogs_20260101_artists.xml.gz" -> "20260101")
fn extract_version_from_filename(filename: &str) -> Option<String> {
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() >= 2 { Some(parts[1].to_string()) } else { None }
}

/// Run the Discogs initial extraction and periodic/triggered checks.
pub async fn run_extraction_loop(
    config: Arc<ExtractorConfig>,
    state: Arc<RwLock<ExtractorState>>,
    shutdown: Arc<tokio::sync::Notify>,
    force_reprocess: bool,
    mq_factory: Arc<dyn MessageQueueFactory>,
    trigger: Arc<tokio::sync::Mutex<Option<bool>>>,
    compiled_rules: Option<Arc<CompiledRulesConfig>>,
    local_manifest: Option<PathBuf>,
) -> Result<()> {
    info!("📥 Starting initial data processing...");

    // Convert the one-shot shutdown Notify into a pollable flag. Without this a SIGTERM delivered
    // during process_discogs_data (hours of multi-GB XML) is lost: notify_waiters() wakes only
    // currently-parked waiters and stores no permit, so the periodic loop's fresh shutdown arm
    // never fires and the process enters the multi-day sleep, unstoppable. (cu2.44)
    let shutdown_flag = spawn_shutdown_flag_monitor(shutdown.clone());

    // Local manifests are an explicit operator/test-only, one-shot input seam. They still
    // enter the production processing function below, but never construct the public HTTP
    // downloader and never enter the periodic acquisition loop.
    if let Some(manifest_path) = local_manifest {
        info!("📦 Running one-shot local manifest: {}", manifest_path.display());
        let mut source = local_manifest::LocalManifestSource::from_manifest(&manifest_path, config.discogs_root.clone()).await?;
        let success =
            process_discogs_data(config, state, shutdown, shutdown_flag.clone(), force_reprocess, &mut source, mq_factory, compiled_rules).await?;
        initial_run_outcome(success, shutdown_flag.load(Ordering::SeqCst), "Discogs local manifest")?;
        info!("✅ One-shot local manifest completed successfully");
        return Ok(());
    }

    // Process initial data
    let mut downloader = Downloader::new(config.discogs_root.clone()).await?;
    let success = process_discogs_data(
        config.clone(),
        state.clone(),
        shutdown.clone(),
        shutdown_flag.clone(),
        force_reprocess,
        &mut downloader,
        mq_factory.clone(),
        compiled_rules.clone(),
    )
    .await?;

    // A shutdown during the initial run is a clean exit, not a failure — returning Err would send
    // main into the failure cooldown + non-zero exit. Short-circuit shutdown to Ok. (cu2.44/cu2.45)
    initial_run_outcome(success, shutdown_flag.load(Ordering::SeqCst), "Discogs")?;

    info!("✅ Initial data processing completed successfully");

    // Start periodic check loop
    loop {
        // If a shutdown arrived during processing (or between iterations), stop before sleeping.
        // The periodic select! below also has a shutdown arm for signals that arrive mid-sleep,
        // but this poll catches signals delivered while process_discogs_data was running. (cu2.44)
        if shutdown_flag.load(Ordering::SeqCst) {
            info!("🛑 Shutdown detected, exiting Discogs periodic check loop");
            break;
        }

        // Transition Completed → Waiting before sleeping so downstream observers
        // (MusicBrainz extractor, admin dashboard tracker) can tell the difference
        // between "just finished" and "on periodic schedule". Failed is preserved
        // to keep the failure signal visible until the next attempt.
        {
            let mut s = state.write().await;
            if s.extraction_status == ExtractionStatus::Completed {
                s.extraction_status = ExtractionStatus::Waiting;
            }
        }

        let check_interval = Duration::from_secs(config.periodic_check_days * 24 * 60 * 60);
        info!("⏰ Waiting {} days before next check...", config.periodic_check_days);

        tokio::select! {
            _ = sleep(check_interval) => {
                info!("🔄 Starting periodic check for new or updated Discogs files...");
                let start = Instant::now();

                let mut downloader = match Downloader::new(config.discogs_root.clone()).await {
                    Ok(dl) => dl,
                    Err(e) => {
                        error!("❌ Failed to create downloader for periodic check: {}", e);
                        // The run never starts, so nothing downstream would ever move the
                        // status off Waiting. Record the failure so /health reports a
                        // terminal, non-success state instead of a parked one that the
                        // API's extraction tracker reads as success (discogsography-exnk).
                        reset_status_after_failed_check(&state).await;
                        continue;
                    }
                };
                match process_discogs_data(config.clone(), state.clone(), shutdown.clone(), shutdown_flag.clone(), false, &mut downloader, mq_factory.clone(), compiled_rules.clone()).await {
                    Ok(true) => {
                        info!("✅ Periodic check completed successfully in {:?}", start.elapsed());
                    }
                    Ok(false) => {
                        error!("❌ Periodic check completed with errors");
                    }
                    Err(e) => {
                        error!("❌ Periodic check failed: {}", e);
                        // Backstop: an early `?` in process_discogs_data leaves status at Running.
                        // Reset to Failed so /trigger recovery is not wedged for the whole
                        // periodic sleep and /health reports the failed run. (cu2.41)
                        reset_status_after_failed_check(&state).await;
                    }
                }
            }
            trigger_force_reprocess = wait_for_trigger(&trigger) => {
                info!("🔄 Extraction triggered via API (force_reprocess={})...", trigger_force_reprocess);
                let start = Instant::now();
                let mut downloader = match Downloader::new(config.discogs_root.clone()).await {
                    Ok(dl) => dl,
                    Err(e) => {
                        error!("❌ Failed to create downloader for triggered extraction: {}", e);
                        // wait_for_trigger already CONSUMED the trigger flag, so this run is
                        // lost. Without a status write the extractor stays parked at Waiting
                        // forever and the API's extraction tracker records the phantom run as
                        // finished — with the PREVIOUS run's record counts, which makes the
                        // false success look entirely convincing (discogsography-exnk).
                        reset_status_after_failed_check(&state).await;
                        continue;
                    }
                };
                match process_discogs_data(config.clone(), state.clone(), shutdown.clone(), shutdown_flag.clone(), trigger_force_reprocess, &mut downloader, mq_factory.clone(), compiled_rules.clone()).await {
                    Ok(true) => info!("✅ Triggered extraction completed successfully in {:?}", start.elapsed()),
                    Ok(false) => error!("❌ Triggered extraction completed with errors"),
                    Err(e) => {
                        error!("❌ Triggered extraction failed: {}", e);
                        reset_status_after_failed_check(&state).await; // (cu2.41)
                    }
                }
            }
            _ = shutdown.notified() => {
                info!("🛑 Shutdown requested, stopping periodic checks");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "discogs_tests.rs"]
mod tests;
