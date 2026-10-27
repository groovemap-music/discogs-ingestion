//! Discogs orchestration and publication-boundary integration tests.

use extractor::config::ExtractorConfig;
use extractor::discogs_downloader::MockDataSource;
use extractor::extractor::DefaultMessageQueueFactory;
use extractor::extractor::{ExtractionStatus, ExtractorState, message_publisher, process_discogs_data, process_single_file};
use extractor::message_queue::MockMessagePublisher;
use extractor::rules::{CompiledRulesConfig, RulesConfig};
use extractor::state_marker::{PhaseStatus, ProcessingDecision, StateMarker};
use extractor::types::S3FileInfo;
use extractor::types::{DataMessage, DataType};
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;
use tokio::sync::{Mutex, RwLock};

use super::mock_helpers::MockMqFactory;

/// Helper to create a test config with all required fields.
fn test_config(root: &std::path::Path) -> ExtractorConfig {
    ExtractorConfig {
        amqp_connection: "amqp://localhost:5672/%2F".to_string(),
        discogs_root: root.to_path_buf(),
        periodic_check_days: 1,
        health_port: 0,
        max_workers: 2,
        batch_size: 100,
        queue_size: 100,
        progress_log_interval: 1000,
        state_save_interval: 1000,
        data_quality_rules: None,
        discogs_exchange_prefix: "groovemap-discogs".to_string(),
    }
}

#[tokio::test]

async fn test_process_single_file_mq_setup_called() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().withf(|dt| *dt == DataType::Artists).times(1).returning(|_| Ok(()));
    mock_mq.expect_close().times(..).returning(|| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file("discogs_20260101_artists.xml.gz", config, state, state_marker, marker_path, mq, None).await;

    // Error expected — file doesn't exist on disk
    assert!(result.is_err());
}

/// Write a minimal valid (empty) gzip-compressed artists XML file, as parser_test.rs does,
/// so process_single_file can run its full pipeline to a real success.
fn write_empty_artists_gz(path: &std::path::Path) {
    let xml_content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<artists>\n</artists>";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(xml_content.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(path, compressed).unwrap();
}

#[tokio::test]
async fn test_process_single_file_amqp_failure_at_send_file_complete_leaves_marker_not_completed() {
    // Regression: a send_file_complete failure must leave the
    // state marker NOT Completed for this file, so pending_files() still includes it
    // on the next run and the signal is retried — instead of the marker already
    // claiming the file done (the old "marker first" ordering) and the signal being
    // silently and permanently dropped.
    let temp_dir = TempDir::new().unwrap();
    let file_name = "discogs_20260101_artists.xml.gz";
    write_empty_artists_gz(&temp_dir.path().join(file_name));

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().times(1).returning(|_, _, _| Err(anyhow::anyhow!("AMQP connection dropped")));
    mock_mq.expect_close().times(..).returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file(file_name, config, state, state_marker.clone(), marker_path, mq, None).await;

    assert!(result.is_err(), "a send_file_complete failure must propagate as an error");

    let marker = state_marker.lock().await;
    let file_status = marker.processing_phase.progress_by_file.get(file_name);
    assert_ne!(
        file_status.map(|s| s.status),
        Some(extractor::state_marker::PhaseStatus::Completed),
        "the file must NOT be marked Completed when send_file_complete failed — otherwise \
         pending_files() would skip it on the next run and the signal would never be retried"
    );
}

#[tokio::test]
async fn test_process_single_file_close_failure_does_not_fail_the_run() {
    // Regression: a cleanup (mq.close()) failure is purely
    // cosmetic — the completion signal was already sent and the marker already
    // committed — so it must not flip an otherwise fully-successful file to Failed.
    let temp_dir = TempDir::new().unwrap();
    let file_name = "discogs_20260101_artists.xml.gz";
    write_empty_artists_gz(&temp_dir.path().join(file_name));

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().times(1).returning(|_, _, _| Ok(()));
    mock_mq.expect_close().times(1).returning(|| Err(anyhow::anyhow!("channel already closed")));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file(file_name, config, state, state_marker.clone(), marker_path, mq, None).await;

    assert!(result.is_ok(), "a cosmetic close() failure must not fail an otherwise successful file: {:?}", result.err());

    let marker = state_marker.lock().await;
    let file_status = marker.processing_phase.progress_by_file.get(file_name);
    assert_eq!(file_status.map(|s| s.status), Some(extractor::state_marker::PhaseStatus::Completed));
}

#[tokio::test]
async fn test_single_file_amqp_failure_clears_active_connection() {
    // Regression: every error exit of process_single_file must drop
    // the active_connections entry and gracefully close the per-file AMQP connection.
    // Previously both sat on the success tail only, so a failed file left a phantom
    // connection in /metrics until the next run's state reset — periodic_check_days away.
    let temp_dir = TempDir::new().unwrap();
    let file_name = "discogs_20260101_artists.xml.gz";
    write_empty_artists_gz(&temp_dir.path().join(file_name));

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().times(1).returning(|_, _, _| Err(anyhow::anyhow!("AMQP connection dropped")));
    // close() must still be called exactly once on the failure path.
    mock_mq.expect_close().times(1).returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file(file_name, config, state.clone(), state_marker, marker_path, mq, None).await;
    assert!(result.is_err(), "a send_file_complete failure must propagate as an error");

    let s = state.read().await;
    assert!(s.active_connections.is_empty(), "failed file must not leave a phantom active connection: {:?}", s.active_connections);
    assert!(!s.completed_files.contains(file_name), "a failed file must not be recorded as completed");
}

#[tokio::test]
async fn test_single_file_parse_failure_clears_active_connection() {
    // Same contract for a failure raised inside the pipeline itself (missing dump file)
    // rather than at the AMQP handoff.
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().times(0).returning(|_, _, _| Ok(()));
    mock_mq.expect_close().times(1).returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file("discogs_20260101_artists.xml.gz", config, state.clone(), state_marker, marker_path, mq, None).await;
    assert!(result.is_err(), "a missing dump file must fail the pipeline");

    let s = state.read().await;
    assert!(s.active_connections.is_empty(), "failed file must not leave a phantom active connection: {:?}", s.active_connections);
}

#[tokio::test]
async fn test_message_publisher_increments_error_count_on_failure() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_publish_batch().times(1).returning(|_, _| Err(anyhow::anyhow!("AMQP connection lost")));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);
    let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<DataMessage>>(10);

    sender.send(vec![]).await.unwrap();
    drop(sender);

    let result = message_publisher(receiver, mq, DataType::Artists, state.clone()).await;

    assert!(result.is_err());
    let s = state.read().await;
    assert_eq!(s.error_count, 1);
}

#[tokio::test]
async fn test_message_publisher_success_path() {
    let state = Arc::new(RwLock::new(ExtractorState::default()));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_publish_batch().times(3).returning(|_, _| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);
    let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<DataMessage>>(10);

    for _ in 0..3 {
        sender.send(vec![]).await.unwrap();
    }
    drop(sender);

    let result = message_publisher(receiver, mq, DataType::Artists, state.clone()).await;

    assert!(result.is_ok());
    let s = state.read().await;
    assert_eq!(s.error_count, 0);
}

#[tokio::test]
async fn test_process_discogs_data_empty_files() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().times(1).returning(|| Ok(vec![]));
    mock_dl.expect_get_latest_monthly_files().times(1).returning(|_| Ok(vec![]));

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result =
        extractor::extractor::process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None)
            .await;

    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_process_discogs_data_skip_when_already_complete() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Create a fully completed state marker
    let mut marker = StateMarker::new("20260101".to_string());
    marker.complete_processing();
    marker.complete_extraction();
    let marker_path = StateMarker::file_path(temp_dir.path(), "20260101");
    marker.save(&marker_path).await.unwrap();

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result =
        extractor::extractor::process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None)
            .await;

    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_process_discogs_data_force_reprocess_bypasses_skip() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Create a fully completed state marker — force_reprocess should ignore it
    let mut marker = StateMarker::new("20260101".to_string());
    marker.complete_processing();
    marker.complete_extraction();
    let marker_path = StateMarker::file_path(temp_dir.path(), "20260101");
    marker.save(&marker_path).await.unwrap();

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| {
        Ok(vec![
            "discogs_20260101_artists.xml.gz".to_string(),
            "discogs_20260101_labels.xml.gz".to_string(),
            "discogs_20260101_masters.xml.gz".to_string(),
            "discogs_20260101_releases.xml.gz".to_string(),
        ])
    });
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result =
        extractor::extractor::process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), true, &mut mock_dl, factory, None)
            .await;

    // Result may be Ok or Err — key assertion is download_discogs_data was called (verified by mock times(1))
    let _ = result;
}

#[tokio::test]
async fn test_default_mq_factory_create_fails_without_broker() {
    use extractor::extractor::MessageQueueFactory;

    let factory = DefaultMessageQueueFactory;
    // Invalid port so connection fails fast
    let result: anyhow::Result<Arc<dyn extractor::message_queue::MessagePublisher>> = factory.create("amqp://localhost:59999", "groovemap").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_process_discogs_data_take_state_marker_none() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    // Return None to trigger the "State marker missing after download" error
    mock_dl.expect_take_state_marker().times(1).returning(|| None);

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result =
        extractor::extractor::process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), true, &mut mock_dl, factory, None)
            .await;

    assert!(result.is_err());
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("State marker missing after download"), "Unexpected error: {}", err_msg);
}

#[tokio::test]
async fn test_process_discogs_data_no_data_files() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    // Return only CHECKSUM — all get filtered out
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_CHECKSUM.txt".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result =
        extractor::extractor::process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), true, &mut mock_dl, factory, None)
            .await;

    // Should return Ok(true) — "No data files to process"
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_process_discogs_data_all_files_already_processed() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Create a state marker where processing is started but all files are complete
    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_processing(1);
    marker.start_file_processing("discogs_20260101_artists.xml.gz");
    marker.complete_file_processing("discogs_20260101_artists.xml.gz", 1000);

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(move || Some(marker.clone()));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let marker_path = StateMarker::file_path(temp_dir.path(), "20260101");
    let result = extractor::extractor::process_discogs_data(
        config,
        state.clone(),
        shutdown,
        Arc::new(AtomicBool::new(false)),
        true,
        &mut mock_dl,
        factory,
        None,
    )
    .await;

    assert!(result.is_ok());
    assert!(result.unwrap());
    // The success side of the same branch — a landed broadcast still
    // reports success, marks the extraction durably complete, and sets health Completed.
    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Completed);
    let persisted = StateMarker::load(&marker_path).await.unwrap().unwrap();
    assert_eq!(persisted.summary.overall_status, PhaseStatus::Completed);
}

#[tokio::test]
async fn test_process_discogs_data_mq_factory_create_fails_on_all_processed() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_processing(1);
    marker.start_file_processing("discogs_20260101_artists.xml.gz");
    marker.complete_file_processing("discogs_20260101_artists.xml.gz", 1000);

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(move || Some(marker.clone()));

    // Factory that fails to create MQ connection — exercises the error path
    use extractor::extractor::MessageQueueFactory;
    struct FailingMqFactory;
    #[async_trait::async_trait]
    impl MessageQueueFactory for FailingMqFactory {
        async fn create(&self, _url: &str, _exchange_prefix: &str) -> anyhow::Result<Arc<dyn extractor::message_queue::MessagePublisher>> {
            Err(anyhow::anyhow!("AMQP connection refused"))
        }
    }
    let factory = Arc::new(FailingMqFactory);

    let result = extractor::extractor::process_discogs_data(
        config,
        state.clone(),
        shutdown,
        Arc::new(AtomicBool::new(false)),
        true,
        &mut mock_dl,
        factory,
        None,
    )
    .await;

    // An unsent extraction_complete is a FAILURE, exactly as in the
    // normal completion path. Reporting Ok(true)/Completed here deferred the retry to the
    // next periodic sleep (15 days by default) while health, dashboard, and logs all
    // claimed the run had completed.
    assert!(result.is_ok());
    assert!(!result.unwrap(), "a failed extraction_complete broadcast must not report success");
    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Failed);
}

/// Build a MockDataSource for the resumed all-files-already-processed Discogs path that
/// hands `marker` back after the (no-op) download. Mirrors the fixtures used by the
/// all-files-already-processed tests above.
fn mock_dl_returning_marker(marker: StateMarker) -> MockDataSource {
    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_CHECKSUM.txt".to_string(), size: 100 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_masters.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_releases.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(move || Some(marker.clone()));
    mock_dl
}

/// Regression: on the resumed all-files-already-processed
/// completion path, a failed extraction_complete broadcast must NOT flip the state marker
/// to fully Completed — otherwise should_process() returns Skip forever and the completion
/// signal is permanently lost. The marker must stay retryable, and a later successful send
/// must then finalize it.
#[tokio::test]
async fn test_process_discogs_data_resumed_completion_amqp_failure_stays_retryable() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let marker_path = StateMarker::file_path(&config.discogs_root, "20260101");

    // Marker: processing started, the single file complete → resumed-empty branch,
    // overall_status still InProgress (extraction not yet broadcast).
    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_processing(1);
    marker.start_file_processing("discogs_20260101_artists.xml.gz");
    marker.complete_file_processing("discogs_20260101_artists.xml.gz", 1000);
    assert_ne!(marker.summary.overall_status, PhaseStatus::Completed);

    // Phase 1: AMQP broadcast fails (RabbitMQ restarting).
    struct FailingMqFactory;
    #[async_trait::async_trait]
    impl extractor::extractor::MessageQueueFactory for FailingMqFactory {
        async fn create(&self, _url: &str, _prefix: &str) -> anyhow::Result<Arc<dyn extractor::message_queue::MessagePublisher>> {
            Err(anyhow::anyhow!("AMQP connection refused"))
        }
    }
    let mut mock_dl = mock_dl_returning_marker(marker.clone());
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let result = extractor::extractor::process_discogs_data(
        config.clone(),
        state,
        shutdown.clone(),
        Arc::new(AtomicBool::new(false)),
        false,
        &mut mock_dl,
        Arc::new(FailingMqFactory),
        None,
    )
    .await;
    assert!(result.is_ok(), "a send failure is logged, not fatal");

    let persisted = StateMarker::load(&marker_path).await.unwrap().expect("marker must be persisted");
    assert_ne!(
        persisted.summary.overall_status,
        PhaseStatus::Completed,
        "must not mark Completed when extraction_complete failed to send — it would Skip forever"
    );
    assert!(matches!(persisted.should_process(), ProcessingDecision::Continue), "unsent completion must remain retryable (Continue), not Skip");

    // Phase 2: next cycle, RabbitMQ is back — the broadcast succeeds and finalizes the marker.
    let mut mock_ok = MockMessagePublisher::new();
    mock_ok.expect_send_extraction_complete().times(1).returning(|_, _, _, _| Ok(()));
    mock_ok.expect_close().returning(|| Ok(()));
    let ok_factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_ok) });

    let mut mock_dl2 = mock_dl_returning_marker(persisted);
    let state2 = Arc::new(RwLock::new(ExtractorState::default()));
    let result2 = extractor::extractor::process_discogs_data(
        config.clone(),
        state2,
        shutdown,
        Arc::new(AtomicBool::new(false)),
        false,
        &mut mock_dl2,
        ok_factory,
        None,
    )
    .await;
    assert!(result2.is_ok() && result2.unwrap());

    let finalized = StateMarker::load(&marker_path).await.unwrap().expect("marker must be persisted");
    assert_eq!(finalized.summary.overall_status, PhaseStatus::Completed, "a successful retry broadcast must finalize the marker as Completed");
}

/// Regression: after a crash-and-resume, extraction_complete's
/// record_counts must carry the TRUE per-type totals for types completed in an earlier
/// session — not 0. The per-run ExtractionProgress is reset each run, so counts must come
/// from the persisted progress_by_file. This also verifies the /health rehydration: the
/// resumed run's ExtractionProgress reflects the pre-crash totals rather than 0.
#[tokio::test]
async fn test_process_discogs_data_resumed_record_counts_from_persisted_progress() {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Marker from an earlier session: artists + labels completed (real counts), processing
    // still InProgress (complete_processing was never reached before the crash).
    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_processing(2);
    marker.start_file_processing("discogs_20260101_artists.xml.gz");
    marker.complete_file_processing("discogs_20260101_artists.xml.gz", 1000);
    marker.start_file_processing("discogs_20260101_labels.xml.gz");
    marker.complete_file_processing("discogs_20260101_labels.xml.gz", 2000);
    assert_eq!(marker.processing_phase.status, PhaseStatus::InProgress);

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl
        .expect_download_discogs_data()
        .times(1)
        .returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string(), "discogs_20260101_labels.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(move || Some(marker.clone()));

    // Capture the record_counts actually broadcast.
    let captured: Arc<StdMutex<Option<HashMap<String, u64>>>> = Arc::new(StdMutex::new(None));
    let captured_clone = captured.clone();
    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_send_extraction_complete().times(1).returning(move |_version, _started, counts, _types| {
        *captured_clone.lock().unwrap() = Some(counts);
        Ok(())
    });
    mock_mq.expect_close().returning(|| Ok(()));
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = extractor::extractor::process_discogs_data(
        config,
        state.clone(),
        shutdown,
        Arc::new(AtomicBool::new(false)),
        false,
        &mut mock_dl,
        factory,
        None,
    )
    .await;
    assert!(result.is_ok() && result.unwrap());

    // The broadcast must report the true pre-crash totals, NOT 0.
    let counts = captured.lock().unwrap().clone().expect("extraction_complete must have been sent");
    assert_eq!(counts.get("artists").copied(), Some(1000), "artists count must come from persisted progress, not the reset per-run counter");
    assert_eq!(counts.get("labels").copied(), Some(2000), "labels count must come from persisted progress, not the reset per-run counter");

    // /health rehydration: the resumed run's ExtractionProgress reflects the pre-crash totals.
    let s = state.read().await;
    assert_eq!(s.extraction_progress.artists, 1000, "resume must rehydrate artists progress for /health");
    assert_eq!(s.extraction_progress.labels, 2000, "resume must rehydrate labels progress for /health");
}

// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — all files processed, extraction_complete send fails
// (covers extractor.rs line 177)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_all_processed_extraction_complete_send_fails() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // State marker where processing is started and file is already completed
    let mut marker = StateMarker::new("20260101".to_string());
    marker.start_processing(1);
    marker.start_file_processing("discogs_20260101_artists.xml.gz");
    marker.complete_file_processing("discogs_20260101_artists.xml.gz", 1000);

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(move || Some(marker.clone()));

    // MQ that fails on send_extraction_complete
    let mut mock_mq = MockMessagePublisher::new();
    mock_mq
        .expect_send_extraction_complete()
        .returning(|_, _, _, _| Err(anyhow::anyhow!("extraction_complete send failed")));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), true, &mut mock_dl, factory, None).await;

    // The send failed, so the version still owes a broadcast. The
    // early path must report that out of band (Ok(false) + ExtractionStatus::Failed) so the
    // initial-run promotion to Err / cooldown-restart retries within minutes, instead of
    // claiming success and sleeping out a whole periodic cycle.
    assert!(result.is_ok());
    assert!(!result.unwrap(), "a failed extraction_complete broadcast must not report success");
    assert_eq!(state.read().await.extraction_status, ExtractionStatus::Failed);
}

// ──────────────────────────────────────────────────────────────────────────────
// ExtractionStatus as_str coverage
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_extraction_status_as_str_all_variants() {
    assert_eq!(ExtractionStatus::Idle.as_str(), "idle");
    assert_eq!(ExtractionStatus::Running.as_str(), "running");
    assert_eq!(ExtractionStatus::Completed.as_str(), "completed");
    assert_eq!(ExtractionStatus::Waiting.as_str(), "waiting");
    assert_eq!(ExtractionStatus::Failed.as_str(), "failed");
}

// ──────────────────────────────────────────────────────────────────────────────
// Helper: create a gzipped XML file on disk for process_single_file tests
// ──────────────────────────────────────────────────────────────────────────────

fn create_gzipped_xml_file(dir: &std::path::Path, filename: &str, xml_content: &str) {
    let file_path = dir.join(filename);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(xml_content.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(file_path, compressed).unwrap();
}

fn compile_test_rules(yaml: &str) -> Arc<CompiledRulesConfig> {
    let config: RulesConfig = serde_yaml_ng::from_str(yaml).unwrap();
    Arc::new(CompiledRulesConfig::compile(config).unwrap())
}

// ──────────────────────────────────────────────────────────────────────────────
// process_single_file — with rules/validator path (covers extractor.rs 344-395)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_single_file_with_rules_no_violations() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Test Artist</name>
        <profile>Some profile</profile>
    </artist>
    <artist id="2">
        <name>Another Artist</name>
    </artist>
</artists>"#;

    let filename = "discogs_20260101_artists.xml.gz";
    create_gzipped_xml_file(temp_dir.path(), filename, xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let rules = compile_test_rules(
        r#"
rules:
  artists:
    - name: name_required
      field: name
      condition: {type: required}
      severity: error
"#,
    );

    let result = process_single_file(filename, config, state.clone(), state_marker, marker_path, mq, Some(rules)).await;

    assert!(result.is_ok(), "process_single_file with rules should succeed: {:?}", result);

    let s = state.read().await;
    assert!(s.completed_files.contains(filename));
    assert_eq!(s.extraction_progress.artists, 2);
}

#[tokio::test]
async fn test_process_single_file_with_rules_and_violations() {
    let temp_dir = TempDir::new().unwrap();

    // One artist has name, one doesn't — triggers violation
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Good Artist</name>
    </artist>
    <artist id="2">
        <profile>Missing name</profile>
    </artist>
</artists>"#;

    let filename = "discogs_20260101_artists.xml.gz";
    create_gzipped_xml_file(temp_dir.path(), filename, xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let rules = compile_test_rules(
        r#"
rules:
  artists:
    - name: name_required
      field: name
      condition: {type: required}
      severity: error
"#,
    );

    let result = process_single_file(filename, config, state.clone(), state_marker, marker_path, mq, Some(rules)).await;

    assert!(result.is_ok(), "process_single_file with violations should still succeed: {:?}", result);

    let s = state.read().await;
    assert!(s.completed_files.contains(filename));
    assert_eq!(s.extraction_progress.artists, 2);

    // Check that flagged files were written
    let flagged_dir = temp_dir.path().join("flagged").join("20260101").join("artists");
    assert!(flagged_dir.exists(), "Flagged directory should be created for violations");
}

#[tokio::test]
async fn test_process_single_file_without_rules() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<labels>
    <label>
        <id>1</id>
        <name>Test Label</name>
    </label>
</labels>"#;

    let filename = "discogs_20260101_labels.xml.gz";
    create_gzipped_xml_file(temp_dir.path(), filename, xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let state_marker = Arc::new(Mutex::new(StateMarker::new("20260101".to_string())));
    let marker_path = temp_dir.path().join("marker.json");

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let mq: Arc<dyn extractor::message_queue::MessagePublisher> = Arc::new(mock_mq);

    let result = process_single_file(filename, config, state.clone(), state_marker, marker_path, mq, None).await;

    assert!(result.is_ok(), "process_single_file without rules should succeed: {:?}", result);

    let s = state.read().await;
    assert!(s.completed_files.contains(filename));
    assert_eq!(s.extraction_progress.labels, 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — Reprocess decision path (covers extractor.rs 131-132)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_reprocess_decision() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Create a state marker with a failed download phase — triggers Reprocess
    let mut marker = StateMarker::new("20260101".to_string());
    marker.download_phase.status = extractor::state_marker::PhaseStatus::Failed;
    let marker_path = StateMarker::file_path(temp_dir.path(), "20260101");
    marker.save(&marker_path).await.unwrap();

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    // Key assertion: download_discogs_data was called (Reprocess path taken)
    let _ = result;
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — end-to-end with actual file processing
// (covers lines 218-258, 283-290 in extractor.rs)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_end_to_end_success() {
    let temp_dir = TempDir::new().unwrap();

    // Create actual gzipped XML files on disk
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Test Artist</name>
    </artist>
</artists>"#;
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    assert!(result.is_ok(), "End-to-end processing should succeed: {:?}", result);
    assert!(result.unwrap(), "Should return true for successful processing");

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Completed);
    assert!(s.completed_files.contains("discogs_20260101_artists.xml.gz"));
    assert_eq!(s.extraction_progress.artists, 1);
}

/// Regression: a shutdown flag already set when process_discogs_data reaches the file
/// loop must stop it from starting (and therefore finalizing) the run. The not-yet-started file is
/// skipped so it stays pending in the state marker for a clean resume, the run is NOT marked
/// Completed, and it returns Ok(false) — never Err (which would send main into the failure
/// cooldown). Mirrors test_process_discogs_data_end_to_end_success but with the flag pre-set.
#[tokio::test]
async fn test_process_discogs_data_shutdown_before_files_does_not_finalize() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Test Artist</name>
    </artist>
</artists>"#;
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    // Shutdown already requested when the run is entered.
    let shutdown_flag = Arc::new(AtomicBool::new(true));

    // The run bails out before touching the downloader at all:
    // starting a multi-GB download under shutdown guarantees a SIGKILL mid-transfer.
    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .times(0)
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .times(0)
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(0).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(0).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(0).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state.clone(), shutdown, shutdown_flag, false, &mut mock_dl, factory, None).await;

    // Not promoted to Err — a graceful shutdown must not trip main's failure cooldown.
    assert!(result.is_ok(), "shutdown must not surface as Err: {:?}", result);
    assert!(!result.unwrap(), "a shutdown-interrupted run must return false (not complete)");

    let s = state.read().await;
    // Nothing completed, and the run never even flipped the status to Running.
    assert_ne!(s.extraction_status, ExtractionStatus::Completed, "an interrupted run must not report Completed");
    assert_ne!(s.extraction_status, ExtractionStatus::Running, "a run entered under shutdown must not announce itself as Running");
    assert!(!s.completed_files.contains("discogs_20260101_artists.xml.gz"), "skipped file must not be marked completed");
}

#[tokio::test]
async fn test_process_discogs_data_end_to_end_with_rules() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Valid Artist</name>
    </artist>
    <artist id="2">
        <profile>No name here</profile>
    </artist>
</artists>"#;
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let rules = compile_test_rules(
        r#"
rules:
  artists:
    - name: name_required
      field: name
      condition: {type: required}
      severity: error
"#,
    );

    let result =
        process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, Some(rules)).await;

    assert!(result.is_ok(), "End-to-end with rules should succeed: {:?}", result);
    assert!(result.unwrap());

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Completed);
    assert_eq!(s.extraction_progress.artists, 2);
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — send_extraction_complete failure path
// (covers extractor.rs lines 283-284)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_extraction_complete_failure() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Test Artist</name>
    </artist>
</artists>"#;
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    // MQ that fails on send_extraction_complete
    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Err(anyhow::anyhow!("AMQP send failed")));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    // Should return Ok(false) — extraction_complete failure makes success=false
    assert!(result.is_ok());
    assert!(!result.unwrap(), "Should return false when extraction_complete fails");

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Failed);
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — mq_factory.create failure at final extraction_complete
// (covers extractor.rs lines 288-290)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_mq_factory_create_fails_at_extraction_complete() {
    let temp_dir = TempDir::new().unwrap();

    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1">
        <name>Test Artist</name>
    </artist>
</artists>"#;
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", xml_content);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl.expect_download_discogs_data().times(1).returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    // Factory that succeeds for per-file MQ but fails for the final extraction_complete MQ
    use std::sync::atomic::AtomicUsize;
    let call_count = Arc::new(AtomicUsize::new(0));

    struct CountingMqFactory {
        publisher: Arc<dyn extractor::message_queue::MessagePublisher>,
        call_count: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl extractor::extractor::MessageQueueFactory for CountingMqFactory {
        async fn create(&self, _url: &str, _exchange_prefix: &str) -> anyhow::Result<Arc<dyn extractor::message_queue::MessagePublisher>> {
            let count = self.call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                Ok(self.publisher.clone())
            } else {
                Err(anyhow::anyhow!("AMQP connection refused"))
            }
        }
    }

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(CountingMqFactory { publisher: Arc::new(mock_mq), call_count });

    let result = process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    // Should return Ok(false) — extraction_complete MQ connection failure
    assert!(result.is_ok());
    assert!(!result.unwrap(), "Should return false when final MQ create fails");

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Failed);
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — multiple files end-to-end
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_multiple_files() {
    let temp_dir = TempDir::new().unwrap();

    let artists_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists>
    <artist id="1"><name>Artist 1</name></artist>
</artists>"#;
    let labels_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<labels>
    <label><id>1</id><name>Label 1</name></label>
</labels>"#;

    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_artists.xml.gz", artists_xml);
    create_gzipped_xml_file(temp_dir.path(), "discogs_20260101_labels.xml.gz", labels_xml);

    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| {
        Ok(vec![
            S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "data/discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_get_latest_monthly_files().returning(|_| {
        Ok(vec![
            S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 },
            S3FileInfo { name: "discogs_20260101_labels.xml.gz".to_string(), size: 1000 },
        ])
    });
    mock_dl.expect_set_state_marker().times(1).returning(|_, _| ());
    mock_dl
        .expect_download_discogs_data()
        .times(1)
        .returning(|| Ok(vec!["discogs_20260101_artists.xml.gz".to_string(), "discogs_20260101_labels.xml.gz".to_string()]));
    mock_dl.expect_take_state_marker().times(1).returning(|| Some(StateMarker::new("20260101".to_string())));

    let mut mock_mq = MockMessagePublisher::new();
    mock_mq.expect_setup_exchange().returning(|_| Ok(()));
    mock_mq.expect_publish_batch().returning(|_, _| Ok(()));
    mock_mq.expect_send_file_complete().returning(|_, _, _| Ok(()));
    mock_mq.expect_send_extraction_complete().returning(|_, _, _, _| Ok(()));
    mock_mq.expect_close().returning(|| Ok(()));

    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state.clone(), shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    assert!(result.is_ok());
    assert!(result.unwrap());

    let s = state.read().await;
    assert_eq!(s.extraction_status, ExtractionStatus::Completed);
    assert_eq!(s.completed_files.len(), 2);
    assert_eq!(s.extraction_progress.artists, 1);
    assert_eq!(s.extraction_progress.labels, 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — version extraction failure
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_version_extraction_failure() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/invalidfilename".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "invalidfilename".to_string(), size: 1000 }]));

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    assert!(result.is_err());
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — list_s3_files failure
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_list_s3_files_failure() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl.expect_list_s3_files().returning(|| Err(anyhow::anyhow!("HTTP timeout")));

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    assert!(result.is_err());
}

// ──────────────────────────────────────────────────────────────────────────────
// process_discogs_data — download_discogs_data failure
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_process_discogs_data_download_failure() {
    let temp_dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(temp_dir.path()));
    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let mut mock_dl = MockDataSource::new();
    mock_dl
        .expect_list_s3_files()
        .returning(|| Ok(vec![S3FileInfo { name: "data/discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl
        .expect_get_latest_monthly_files()
        .returning(|_| Ok(vec![S3FileInfo { name: "discogs_20260101_artists.xml.gz".to_string(), size: 1000 }]));
    mock_dl.expect_set_state_marker().returning(|_, _| ());
    mock_dl.expect_download_discogs_data().returning(|| Err(anyhow::anyhow!("Download failed")));

    let mock_mq = MockMessagePublisher::new();
    let factory = Arc::new(MockMqFactory { publisher: Arc::new(mock_mq) });

    let result = process_discogs_data(config, state, shutdown, Arc::new(AtomicBool::new(false)), false, &mut mock_dl, factory, None).await;

    assert!(result.is_err());
}
