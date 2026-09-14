use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use extractor::config::ExtractorConfig;
use extractor::extractor::{ExtractorState, process_single_file};
use extractor::message_queue::MessagePublisher;
use extractor::state_marker::StateMarker;
use extractor::types::{DataMessage, DataType, ExtractionCompleteMessage, FileCompleteMessage, Message};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::sync::{Mutex as AsyncMutex, RwLock};

const CONTRACT_ROOT: &str = "contracts/extractor-smoke/v1";

#[derive(Default)]
struct CapturingPublisher {
    payloads: Mutex<Vec<Vec<u8>>>,
}

impl CapturingPublisher {
    fn push(&self, message: &Message) -> Result<()> {
        self.payloads.lock().unwrap().push(serde_json::to_vec(message)?);
        Ok(())
    }

    fn events(&self) -> Vec<Value> {
        self.payloads.lock().unwrap().iter().map(|payload| serde_json::from_slice(payload).unwrap()).collect()
    }
}

#[async_trait]
impl MessagePublisher for CapturingPublisher {
    async fn setup_exchange(&self, data_type: DataType) -> Result<()> {
        assert_eq!(data_type, DataType::Releases);
        Ok(())
    }

    async fn publish(&self, message: Message, _data_type: DataType) -> Result<()> {
        self.push(&message)
    }

    async fn publish_batch(&self, messages: Vec<DataMessage>, data_type: DataType) -> Result<()> {
        assert_eq!(data_type, DataType::Releases);
        for message in messages {
            self.push(&Message::Data(message))?;
        }
        Ok(())
    }

    async fn send_file_complete(&self, data_type: DataType, file_name: &str, total_processed: u64) -> Result<()> {
        self.push(&Message::FileComplete(FileCompleteMessage {
            data_type: data_type.to_string(),
            timestamp: Utc::now(),
            total_processed,
            file: file_name.to_string(),
        }))
    }

    async fn send_extraction_complete(
        &self,
        version: &str,
        started_at: chrono::DateTime<Utc>,
        record_counts: HashMap<String, u64>,
        _data_types: &[DataType],
    ) -> Result<()> {
        self.push(&Message::ExtractionComplete(ExtractionCompleteMessage {
            version: version.to_string(),
            timestamp: Utc::now(),
            started_at,
            record_counts,
        }))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

fn sha256(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap();
    hex::encode(Sha256::digest(bytes))
}

fn expected_events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn normalize_dynamic_fields(events: &mut [Value], manifest: &Value) {
    let fixed_timestamp = manifest["semantic_normalizer"]["replace"]["file_complete.timestamp"].clone();
    for event in events {
        if event["type"] == "file_complete" {
            event["timestamp"] = fixed_timestamp.clone();
        }
    }
}

fn test_config(root: &Path) -> ExtractorConfig {
    ExtractorConfig {
        amqp_connection: "amqp://localhost:5672/%2F".to_string(),
        discogs_root: root.to_path_buf(),
        periodic_check_days: 1,
        health_port: 0,
        max_workers: 1,
        batch_size: 100,
        queue_size: 100,
        progress_log_interval: 1000,
        state_save_interval: 1000,
        data_quality_rules: None,
        discogs_exchange_prefix: "groovemap-discogs".to_string(),
    }
}

#[tokio::test]
async fn versioned_tiny_dump_produces_the_pinned_rabbitmq_event_stream() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let contract_root = repository.join(CONTRACT_ROOT);
    let manifest: Value = serde_json::from_slice(&std::fs::read(contract_root.join("manifest.json")).unwrap()).unwrap();

    assert_eq!(manifest["contract"], "groovemap.discogs-extractor-smoke");
    assert_eq!(manifest["version"], 1);
    assert_eq!(manifest["scope"], "single_file");
    assert_eq!(manifest["license"], "MIT");
    assert_eq!(manifest["data_origin"], "synthetic");
    assert_eq!(manifest["catalog_event_contract"]["name"], "groovemap.catalog-events");
    assert_eq!(manifest["catalog_event_contract"]["version"], 1);
    assert_eq!(manifest["semantic_normalizer"]["version"], 1);

    let input_path = contract_root.join(manifest["input"]["path"].as_str().unwrap());
    let expected_path = contract_root.join(manifest["expected_event_stream"]["path"].as_str().unwrap());
    let schema_path = contract_root.join(manifest["catalog_event_contract"]["schema"].as_str().unwrap());

    assert_eq!(sha256(&input_path), manifest["input"]["sha256"]);
    assert_eq!(sha256(&expected_path), manifest["expected_event_stream"]["sha256"]);
    assert_eq!(sha256(&schema_path), manifest["catalog_event_contract"]["schema_sha256"]);

    let expected = expected_events(&expected_path);
    assert_eq!(expected.len() as u64, manifest["expected_event_stream"]["events"].as_u64().unwrap());
    for event in &expected {
        serde_json::from_value::<Message>(event.clone()).expect("every expected line must match a runtime catalog event type");
    }

    let temp_dir = TempDir::new().unwrap();
    let file_name = manifest["input"]["path"].as_str().unwrap();
    std::fs::copy(&input_path, temp_dir.path().join(file_name)).unwrap();

    let state = Arc::new(RwLock::new(ExtractorState::default()));
    let marker = Arc::new(AsyncMutex::new(StateMarker::new("20000101".to_string())));
    let publisher = Arc::new(CapturingPublisher::default());
    process_single_file(
        file_name,
        Arc::new(test_config(temp_dir.path())),
        state,
        marker,
        temp_dir.path().join("state-marker.json"),
        publisher.clone(),
        None,
    )
    .await
    .unwrap();

    let mut actual = publisher.events();
    normalize_dynamic_fields(&mut actual, &manifest);
    assert_eq!(actual, expected);
}
