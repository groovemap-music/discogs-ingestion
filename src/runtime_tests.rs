use super::*;

#[tokio::test]
async fn test_state_marker_file_tracking() {
    use crate::state_marker::{PhaseStatus, StateMarker};
    use tempfile::TempDir;

    let _temp_dir = TempDir::new().unwrap();
    let mut marker = StateMarker::new("20230101".to_string());

    // Test file start tracking
    marker.start_file_processing("discogs_20230101_artists.xml.gz");
    assert_eq!(marker.processing_phase.current_file, Some("discogs_20230101_artists.xml.gz".to_string()));

    // Test file completion
    marker.complete_file_processing("discogs_20230101_artists.xml.gz", 1000);
    let file_progress = marker.processing_phase.progress_by_file.get("discogs_20230101_artists.xml.gz");
    assert!(file_progress.is_some());
    let progress = file_progress.unwrap();
    assert_eq!(progress.status, PhaseStatus::Completed);
    assert_eq!(progress.records_extracted, 1000);
}

#[tokio::test]
async fn test_state_marker_periodic_updates() {
    use crate::state_marker::StateMarker;

    let mut marker = StateMarker::new("20230101".to_string());
    marker.start_file_processing("discogs_20230101_artists.xml.gz");

    // Simulate periodic record updates (records, messages, batches)
    for i in 1..=3 {
        marker.update_file_progress("discogs_20230101_artists.xml.gz", i * 1000, i * 1000, i * 10);
    }

    let file_progress = marker.processing_phase.progress_by_file.get("discogs_20230101_artists.xml.gz");
    assert!(file_progress.is_some());
    assert_eq!(file_progress.unwrap().records_extracted, 3000);
}

#[tokio::test]
async fn test_state_marker_save_load() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_20230101.json");

    // Create and save marker
    let mut marker = StateMarker::new("20230101".to_string());
    marker.start_file_processing("discogs_20230101_artists.xml.gz");
    marker.complete_file_processing("discogs_20230101_artists.xml.gz", 1500);
    marker.save(&marker_path).await.expect("Failed to save marker");

    // Load marker
    let loaded = StateMarker::load(&marker_path).await.expect("Failed to load marker");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.current_version, "20230101");
    let file_progress = loaded.processing_phase.progress_by_file.get("discogs_20230101_artists.xml.gz");
    assert!(file_progress.is_some());
    assert_eq!(file_progress.unwrap().records_extracted, 1500);
}

#[test]
fn test_extractor_state_default() {
    let state = ExtractorState::default();

    assert_eq!(state.extraction_progress.total(), 0);
    assert!(state.last_extraction_time.is_empty());
    assert!(state.completed_files.is_empty());
    assert!(state.active_connections.is_empty());
    assert_eq!(state.error_count, 0);
}

#[tokio::test]
async fn test_message_batcher_basic() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(10);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_20230101.json");
    let state_marker = Arc::new(tokio::sync::Mutex::new(StateMarker::new("20230101".to_string())));

    // Send some test messages
    for i in 0..5 {
        let message =
            DataMessage { sha256: format!("sha{}", i), data: serde_json::json!({ "test": format!("test{}", i) }), id: i.to_string(), raw_xml: None };
        parse_sender.send(message).await.unwrap();
    }
    drop(parse_sender);

    // Run batcher
    let batcher_config = BatcherConfig {
        batch_size: 3,
        data_type: DataType::Artists,
        state: state.clone(),
        state_marker,
        marker_path,
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: 5000,
    };
    let batcher = message_batcher(parse_receiver, batch_sender, batcher_config);

    // Spawn batcher task
    tokio::spawn(batcher);

    // Collect batches
    let mut total_messages = 0;
    while let Some(batch) = batch_receiver.recv().await {
        total_messages += batch.len();
    }

    assert_eq!(total_messages, 5);

    // Verify state was updated
    let s = state.read().await;
    assert_eq!(s.extraction_progress.artists, 5);
}

#[tokio::test]
async fn test_message_batcher_respects_batch_size() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(100);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(10);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_20230101.json");
    let state_marker = Arc::new(tokio::sync::Mutex::new(StateMarker::new("20230101".to_string())));

    // Send exactly batch_size messages
    let batch_size = 10;
    for i in 0..batch_size {
        let message =
            DataMessage { sha256: format!("sha{}", i), data: serde_json::json!({ "test": format!("test{}", i) }), id: i.to_string(), raw_xml: None };
        parse_sender.send(message).await.unwrap();
    }
    drop(parse_sender);

    // Run batcher
    let batcher_config = BatcherConfig {
        batch_size,
        data_type: DataType::Labels,
        state: state.clone(),
        state_marker,
        marker_path,
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: 5000,
    };
    let batcher = message_batcher(parse_receiver, batch_sender, batcher_config);
    tokio::spawn(batcher);

    // Get first batch
    if let Some(batch) = batch_receiver.recv().await {
        assert_eq!(batch.len(), batch_size);
    }
}

#[tokio::test]
async fn test_message_batcher_timeout_flush() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(10);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_20230101.json");
    let state_marker = Arc::new(tokio::sync::Mutex::new(StateMarker::new("20230101".to_string())));

    // Send fewer messages than batch size
    for i in 0..3 {
        let message =
            DataMessage { sha256: format!("sha{}", i), data: serde_json::json!({ "test": format!("test{}", i) }), id: i.to_string(), raw_xml: None };
        parse_sender.send(message).await.unwrap();
    }

    // Run batcher with large batch size
    let batcher_config = BatcherConfig {
        batch_size: 100,
        data_type: DataType::Masters,
        state: state.clone(),
        state_marker,
        marker_path,
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: 5000,
    };
    let batcher = message_batcher(parse_receiver, batch_sender, batcher_config);
    let batcher_handle = tokio::spawn(batcher);

    // Wait a bit for timeout flush
    tokio::time::sleep(Duration::from_millis(1200)).await;

    drop(parse_sender);

    // Should eventually flush despite not reaching batch size
    let batch = tokio::time::timeout(Duration::from_secs(5), batch_receiver.recv())
        .await
        .expect("Timeout waiting for batch")
        .expect("Channel closed without receiving batch");

    assert_eq!(batch.len(), 3);

    batcher_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_extractor_state_tracks_progress() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    {
        let mut s = state.write().await;
        s.extraction_progress.increment(DataType::Artists);
        s.extraction_progress.increment(DataType::Artists);
        s.extraction_progress.increment(DataType::Labels);
    }

    let s = state.read().await;
    assert_eq!(s.extraction_progress.artists, 2);
    assert_eq!(s.extraction_progress.labels, 1);
    assert_eq!(s.extraction_progress.total(), 3);
}

#[tokio::test]
async fn test_extractor_state_tracks_completed_files() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    {
        let mut s = state.write().await;
        s.completed_files.insert("file1.xml".to_string());
        s.completed_files.insert("file2.xml".to_string());
    }

    let s = state.read().await;
    assert_eq!(s.completed_files.len(), 2);
    assert!(s.completed_files.contains("file1.xml"));
}

#[tokio::test]
async fn test_extractor_state_tracks_active_connections() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    {
        let mut s = state.write().await;
        s.active_connections.insert(DataType::Artists, "processing_artists.xml".to_string());
        s.active_connections.insert(DataType::Labels, "processing_labels.xml".to_string());
    }

    let s = state.read().await;
    assert_eq!(s.active_connections.len(), 2);
    assert_eq!(s.active_connections.get(&DataType::Artists), Some(&"processing_artists.xml".to_string()));
}

#[tokio::test]
async fn test_extractor_state_tracks_errors() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    {
        let mut s = state.write().await;
        s.error_count += 1;
        s.error_count += 1;
    }

    let s = state.read().await;
    assert_eq!(s.error_count, 2);
}

#[tokio::test]
async fn test_extractor_state_last_extraction_time() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    {
        let mut s = state.write().await;
        s.last_extraction_time.insert(DataType::Artists, Instant::now());
        s.last_extraction_time.insert(DataType::Labels, Instant::now());
    }

    let s = state.read().await;
    assert!(s.last_extraction_time.contains_key(&DataType::Artists));
    assert!(s.last_extraction_time.contains_key(&DataType::Labels));
}

#[tokio::test(start_paused = true)]
async fn test_progress_reporter_immediate_shutdown() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();

    let handle = tokio::spawn(async move {
        progress_reporter(state, shutdown_clone).await;
    });

    // Yield to allow the spawned task to enter the select!
    tokio::task::yield_now().await;

    // Signal shutdown before any timer fires
    shutdown.notify_waiters();
    tokio::task::yield_now().await;

    assert!(handle.is_finished());
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_progress_reporter_logs_on_timer_fire() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    {
        let mut s = state.write().await;
        s.extraction_progress.increment(DataType::Artists);
        s.extraction_progress.increment(DataType::Labels);
        s.completed_files.insert("discogs_20260101_artists.xml.gz".to_string());
        s.active_connections.insert(DataType::Labels, "discogs_20260101_labels.xml.gz".to_string());
    }

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
    let state_clone = state.clone();

    let handle = tokio::spawn(async move {
        progress_reporter(state_clone, shutdown_clone).await;
    });

    tokio::task::yield_now().await;

    // Advance past first 10-second report interval
    tokio::time::advance(Duration::from_secs(11)).await;
    // Allow the reporter to run through the logging code
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Reporter should still be running (waiting for next interval)
    assert!(!handle.is_finished());

    shutdown.notify_waiters();
    tokio::task::yield_now().await;

    assert!(handle.is_finished());
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_progress_reporter_interval_increases_after_three_reports() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
    let state_clone = state.clone();

    let handle = tokio::spawn(async move {
        progress_reporter(state_clone, shutdown_clone).await;
    });

    tokio::task::yield_now().await;

    // Fire first 3 short intervals (10s each)
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    // Now on the 4th iteration the interval is 30s; 11s is not enough to fire
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;

    // Should still be running (30s interval, only 11s elapsed)
    assert!(!handle.is_finished());

    shutdown.notify_waiters();
    tokio::task::yield_now().await;
    handle.await.unwrap();
}

#[tokio::test]
async fn test_message_batcher_triggers_state_save() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(100);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(100);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_test.json");
    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_file_processing("test_file.xml.gz");
    let state_marker = Arc::new(tokio::sync::Mutex::new(marker));

    // state_save_interval = 5, send exactly 5 messages to trigger a save
    let save_interval = 5usize;
    for i in 0..save_interval {
        let message = DataMessage { id: i.to_string(), sha256: format!("hash{i}"), data: serde_json::json!({}), raw_xml: None };
        parse_sender.send(message).await.unwrap();
    }
    drop(parse_sender);

    let batcher_config = BatcherConfig {
        batch_size: 100,
        data_type: DataType::Artists,
        state: state.clone(),
        state_marker: state_marker.clone(),
        marker_path: marker_path.clone(),
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: save_interval,
    };

    tokio::spawn(async move {
        message_batcher(parse_receiver, batch_sender, batcher_config).await.ok();
    });

    let mut total = 0;
    while let Some(batch) = batch_receiver.recv().await {
        total += batch.len();
    }
    assert_eq!(total, save_interval);

    // State marker file should have been created by the periodic save
    assert!(marker_path.exists(), "State marker file should be written on periodic save");
}

#[tokio::test(start_paused = true)]
async fn test_progress_reporter_stall_detection() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Set up state: Artists has a last_extraction_time but is NOT in completed_files
    {
        let mut s = state.write().await;
        s.last_extraction_time.insert(DataType::Artists, Instant::now());
        s.extraction_progress.increment(DataType::Artists);
    }

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
    let state_clone = state.clone();

    let handle = tokio::spawn(async move {
        progress_reporter(state_clone, shutdown_clone).await;
    });

    tokio::task::yield_now().await;

    // Advance past the first 10s reporting interval
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // At this point, elapsed time for Artists is ~11s which is < 120s, no stall yet.
    // Advance well past 120s total to trigger stall detection
    tokio::time::advance(Duration::from_secs(120)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // The reporter should have detected the stall (elapsed > 120s, file not completed).
    // We can't easily capture log output, but the code path is exercised.
    // Reporter should still be running.
    assert!(!handle.is_finished());

    shutdown.notify_waiters();
    tokio::task::yield_now().await;

    assert!(handle.is_finished());
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_progress_reporter_with_completed_files_and_active_connections() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Set up state with extraction progress, completed files, and active connections
    {
        let mut s = state.write().await;
        s.extraction_progress.artists = 1000;
        s.extraction_progress.labels = 500;
        s.extraction_progress.masters = 200;
        s.extraction_progress.releases = 300;
        s.completed_files.insert("discogs_20260101_artists.xml.gz".to_string());
        s.active_connections.insert(DataType::Labels, "discogs_20260101_labels.xml.gz".to_string());
    }

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
    let state_clone = state.clone();

    let handle = tokio::spawn(async move {
        progress_reporter(state_clone, shutdown_clone).await;
    });

    tokio::task::yield_now().await;

    // Advance past the first 10s reporting interval to fire the timer
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Reporter should still be running
    assert!(!handle.is_finished());

    // Shutdown
    shutdown.notify_waiters();
    tokio::task::yield_now().await;

    assert!(handle.is_finished());
    handle.await.unwrap();
}

#[tokio::test]
async fn test_message_batcher_multiple_batch_sizes() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(100);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(100);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_test.json");
    let state_marker = Arc::new(tokio::sync::Mutex::new(StateMarker::new("20260101".to_string())));

    // Send 25 messages with batch_size=10 => expect 3 batches (10, 10, 5)
    for i in 0..25 {
        let message =
            DataMessage { id: i.to_string(), sha256: format!("sha{}", i), data: serde_json::json!({ "test": format!("test{}", i) }), raw_xml: None };
        parse_sender.send(message).await.unwrap();
    }
    drop(parse_sender);

    let batcher_config = BatcherConfig {
        batch_size: 10,
        data_type: DataType::Releases,
        state: state.clone(),
        state_marker,
        marker_path,
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: 50000,
    };

    tokio::spawn(async move {
        message_batcher(parse_receiver, batch_sender, batcher_config).await.ok();
    });

    // Collect all batches
    let mut batches = Vec::new();
    while let Some(batch) = batch_receiver.recv().await {
        batches.push(batch.len());
    }

    assert_eq!(batches.len(), 3, "Expected 3 batches, got {}: {:?}", batches.len(), batches);
    assert_eq!(batches[0], 10);
    assert_eq!(batches[1], 10);
    assert_eq!(batches[2], 5);
}

#[tokio::test]
async fn test_message_batcher_empty_input() {
    use crate::state_marker::StateMarker;
    use tempfile::TempDir;

    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (batch_sender, mut batch_receiver) = mpsc::channel::<Vec<DataMessage>>(10);
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let temp_dir = TempDir::new().unwrap();
    let marker_path = temp_dir.path().join(".extraction_status_test.json");
    let state_marker = Arc::new(tokio::sync::Mutex::new(StateMarker::new("20260101".to_string())));

    // Drop sender immediately - zero messages
    drop(parse_sender);

    let batcher_config = BatcherConfig {
        batch_size: 10,
        data_type: DataType::Artists,
        state: state.clone(),
        state_marker,
        marker_path,
        file_name: "test_file.xml.gz".to_string(),
        state_save_interval: 5000,
    };

    let handle = tokio::spawn(async move { message_batcher(parse_receiver, batch_sender, batcher_config).await });

    // Should receive no batches
    let result = batch_receiver.recv().await;
    assert!(result.is_none(), "Should receive no batches for empty input");

    // Batcher should exit cleanly
    let batcher_result = handle.await.unwrap();
    assert!(batcher_result.is_ok(), "Batcher should exit cleanly with no input");
}

// ── message_validator tests ─────────────────────────────────────────

// ── failed-check status reset tests (cu2.41) ────────────────────────

/// Regression for cu2.41: after a periodic/triggered extraction returns `Err`, the loop must
/// reset a stuck `Running` status to `Failed`. Without this the status set up-front in
/// `process_discogs_data` survives an early-`?` error for the whole multi-day periodic sleep,
/// wedging `/trigger` recovery and causing `/health` to misreport the failed run as active.
#[tokio::test]
async fn test_reset_status_after_failed_check_clears_running() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Simulate the stuck state: process_discogs_data set Running, then an early `?` propagated.
    state.write().await.extraction_status = ExtractionStatus::Running;

    reset_status_after_failed_check(&state).await;

    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Failed, "a failed check must not leave the status stuck at Running");
}

/// The reset lands on `Failed` (not `Running`, not `Completed`) so health.rs stops returning 409
/// `already_running` and exposes a terminal, recoverable state.
#[tokio::test]
async fn test_reset_status_after_failed_check_is_not_running() {
    for start in [ExtractionStatus::Running, ExtractionStatus::Completed, ExtractionStatus::Waiting] {
        let state = Arc::new(RwLock::new(ExtractorState::default()));
        state.write().await.extraction_status = start;
        reset_status_after_failed_check(&state).await;
        let got = state.read().await.extraction_status;
        assert_eq!(got, ExtractionStatus::Failed);
        assert_ne!(got, ExtractionStatus::Running);
    }
}

/// discogsography-exnk: a triggered run whose downloader cannot be constructed is LOST —
/// `wait_for_trigger` has already consumed the trigger flag — so the loop must record the
/// failure rather than `continue` with the status untouched. Left at `Waiting`, the
/// extractor is indistinguishable from "parked, finished, back on schedule", and the API's
/// extraction tracker records the phantom run as a success (stamped with the PREVIOUS
/// run's record counts, since extraction_progress is only reset inside process_*_data).
#[tokio::test]
async fn test_lost_trigger_leaves_a_terminal_failed_status_not_waiting() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    state.write().await.extraction_status = ExtractionStatus::Waiting;

    // What the trigger arm now does when Downloader::new returns Err.
    reset_status_after_failed_check(&state).await;

    let got = state.read().await.extraction_status;
    assert_eq!(got, ExtractionStatus::Failed);
    assert_ne!(got, ExtractionStatus::Waiting, "a parked status is read as success by the API tracker");
}

// ── initial-run outcome tests (cu2.45) ──────────────────────────────

/// Regression for cu2.45: a between-files shutdown makes `process_musicbrainz_data` return
/// `Ok(false)`, indistinguishable from a real failure. The initial-run path used to promote that
/// to `Err`, sending main into the 600s failure cooldown + `exit(1)` on a clean operator shutdown.
/// `initial_run_outcome` must return `Ok(())` whenever a shutdown was requested, regardless of the
/// success flag — and only return `Err` for a genuine (non-shutdown) failure.
#[test]
fn test_initial_run_outcome_shutdown_is_not_failure() {
    // Genuine failure, no shutdown → Err (main applies cooldown as intended).
    assert!(initial_run_outcome(false, false, "MusicBrainz").is_err());

    // Shutdown with success == false (the exact cu2.45 scenario) → Ok, NOT a failure.
    assert!(initial_run_outcome(false, true, "MusicBrainz").is_ok());

    // Shutdown with success == true → Ok.
    assert!(initial_run_outcome(true, true, "MusicBrainz").is_ok());

    // Clean success → Ok.
    assert!(initial_run_outcome(true, false, "MusicBrainz").is_ok());
}

/// The same helper governs the Discogs initial run (fix-one-fix-all with cu2.44): a shutdown there
/// must likewise short-circuit to Ok so it never trips the failure cooldown.
#[test]
fn test_initial_run_outcome_discogs_shutdown_is_ok() {
    assert!(initial_run_outcome(false, true, "Discogs").is_ok());
    assert!(initial_run_outcome(false, false, "Discogs").is_err());
}

// ── shutdown-flag monitor tests (cu2.44) ────────────────────────────

/// Regression for cu2.44: the Discogs path lost SIGTERM/SIGINT delivered mid-run because nothing
/// converted the one-shot `Notify` into a pollable flag. `spawn_shutdown_flag_monitor` must flip
/// its `AtomicBool` when the `Notify` fires, so processing code can observe a shutdown between
/// files without consuming the signal the outer `select!` needs.
#[tokio::test]
async fn test_spawn_shutdown_flag_monitor_flips_on_notify() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let flag = spawn_shutdown_flag_monitor(shutdown.clone());

    assert!(!flag.load(Ordering::SeqCst), "flag starts clear");

    // notify_waiters() wakes only currently-parked waiters and stores no permit, so retry until
    // the monitor task has parked and observed the signal (or fail after a bounded wait).
    let mut fired = false;
    for _ in 0..200 {
        shutdown.notify_waiters();
        tokio::task::yield_now().await;
        if flag.load(Ordering::SeqCst) {
            fired = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert!(fired, "monitor must set the shutdown flag once the Notify fires");
}

/// The flag stays clear until the signal actually fires — a spurious early poll must not report a
/// shutdown, otherwise the first file would be skipped on every run.
#[tokio::test]
async fn test_spawn_shutdown_flag_monitor_stays_clear_without_signal() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let flag = spawn_shutdown_flag_monitor(shutdown);
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!flag.load(Ordering::SeqCst), "flag must remain clear until shutdown fires");
}

// ── wait_for_trigger tests ──────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn test_wait_for_trigger_returns_when_triggered() {
    let trigger = Arc::new(tokio::sync::Mutex::new(None::<bool>));
    let trigger_clone = trigger.clone();

    let handle = tokio::spawn(async move { wait_for_trigger(&trigger_clone).await });

    // Advance past a few polling intervals — should NOT return yet
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(!handle.is_finished(), "should still be waiting");

    // Set the trigger with force_reprocess = true
    {
        let mut t = trigger.lock().await;
        *t = Some(true);
    }

    // Advance past one polling interval (500ms) and yield
    tokio::time::advance(Duration::from_millis(600)).await;
    tokio::task::yield_now().await;

    // Should have returned with the force_reprocess value
    let result = handle.await.unwrap();
    assert!(result, "should return true (force_reprocess)");
}

#[tokio::test(start_paused = true)]
async fn test_wait_for_trigger_clears_flag() {
    let trigger = Arc::new(tokio::sync::Mutex::new(Some(false)));

    let result = wait_for_trigger(&trigger).await;

    // Should return false (the force_reprocess value)
    assert!(!result, "should return false (force_reprocess)");

    // Mutex should be None after taking
    assert_eq!(*trigger.lock().await, None);
}

#[tokio::test(start_paused = true)]
async fn test_wait_for_trigger_only_fires_once() {
    let trigger = Arc::new(tokio::sync::Mutex::new(Some(false)));

    // First call should return immediately (trigger is already set)
    let result = wait_for_trigger(&trigger).await;
    assert!(!result, "first call should return false");
    assert_eq!(*trigger.lock().await, None);

    // Second call should block — spawn it and verify it doesn't complete
    let trigger_clone = trigger.clone();
    let handle = tokio::spawn(async move { wait_for_trigger(&trigger_clone).await });

    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(!handle.is_finished(), "second wait should block until re-triggered");

    // Re-trigger with force_reprocess = true
    {
        let mut t = trigger.lock().await;
        *t = Some(true);
    }
    tokio::time::advance(Duration::from_millis(600)).await;
    tokio::task::yield_now().await;
    let result = handle.await.unwrap();
    assert!(result, "second call should return true");
}

// ── extraction_status field in ExtractorState ────────────────────────

#[test]
fn test_extractor_state_default_extraction_status() {
    let state = ExtractorState::default();
    assert_eq!(state.extraction_status, ExtractionStatus::Idle);
}

#[tokio::test]
async fn test_extraction_status_set_to_running() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Simulate what process_discogs_data does at startup
    {
        let mut s = state.write().await;
        s.extraction_progress = ExtractionProgress::default();
        s.last_extraction_time.clear();
        s.completed_files.clear();
        s.active_connections.clear();
        s.error_count = 0;
        s.extraction_status = ExtractionStatus::Running;
    }

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Running);
}

#[tokio::test]
async fn test_extraction_status_set_completed_on_success() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Simulate what process_discogs_data does on success
    {
        let mut s = state.write().await;
        s.extraction_status = ExtractionStatus::Running;
    }
    {
        let mut s = state.write().await;
        let success = true;
        s.extraction_status = if success { ExtractionStatus::Completed } else { ExtractionStatus::Failed };
    }

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Completed);
}

#[tokio::test]
async fn test_extraction_status_set_failed_on_error() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Simulate what process_discogs_data does on failure
    {
        let mut s = state.write().await;
        s.extraction_status = ExtractionStatus::Running;
    }
    {
        let mut s = state.write().await;
        let success = false;
        s.extraction_status = if success { ExtractionStatus::Completed } else { ExtractionStatus::Failed };
    }

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Failed);
}

#[test]
fn test_extraction_status_as_str_all_variants() {
    assert_eq!(ExtractionStatus::Idle.as_str(), "idle");
    assert_eq!(ExtractionStatus::Running.as_str(), "running");
    assert_eq!(ExtractionStatus::Completed.as_str(), "completed");
    assert_eq!(ExtractionStatus::Waiting.as_str(), "waiting");
    assert_eq!(ExtractionStatus::Failed.as_str(), "failed");
}

#[tokio::test]
async fn test_completed_transitions_to_waiting_in_loop() {
    // Simulates the run_*_loop transition block: Completed → Waiting, Failed stays Failed.
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    // Case 1: Completed transitions to Waiting
    state.write().await.extraction_status = ExtractionStatus::Completed;
    {
        let mut s = state.write().await;
        if s.extraction_status == ExtractionStatus::Completed {
            s.extraction_status = ExtractionStatus::Waiting;
        }
    }
    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Waiting);

    // Case 2: Failed does NOT transition — failure signal is preserved through the sleep
    state.write().await.extraction_status = ExtractionStatus::Failed;
    {
        let mut s = state.write().await;
        if s.extraction_status == ExtractionStatus::Completed {
            s.extraction_status = ExtractionStatus::Waiting;
        }
    }
    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Failed);
}
