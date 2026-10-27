use super::rules::{CompiledRulesConfig, RulesConfig};
use super::*;

#[test]
fn test_extract_data_type() {
    assert_eq!(extract_data_type("discogs_20241201_artists.xml.gz"), Some(DataType::Artists));
    assert_eq!(extract_data_type("discogs_20241201_labels.xml.gz"), Some(DataType::Labels));
    assert_eq!(extract_data_type("invalid_format.xml"), None);
}

#[test]
fn test_extract_data_type_all_types() {
    assert_eq!(extract_data_type("discogs_20241201_artists.xml.gz"), Some(DataType::Artists));
    assert_eq!(extract_data_type("discogs_20241201_labels.xml.gz"), Some(DataType::Labels));
    assert_eq!(extract_data_type("discogs_20241201_masters.xml.gz"), Some(DataType::Masters));
    assert_eq!(extract_data_type("discogs_20241201_releases.xml.gz"), Some(DataType::Releases));
}

#[test]
fn test_extract_data_type_invalid_formats() {
    assert_eq!(extract_data_type("invalid_format.xml"), None);
    assert_eq!(extract_data_type("no_underscores.xml.gz"), None);
    assert_eq!(extract_data_type("discogs_20241201.xml.gz"), None);
    assert_eq!(extract_data_type("discogs_20241201_unknown.xml.gz"), None);
}

// Deprecated ProcessingState tests removed - replaced by StateMarker integration tests below

#[test]
fn test_extract_version_from_filename() {
    assert_eq!(extract_version_from_filename("discogs_20260101_artists.xml.gz"), Some("20260101".to_string()));
    assert_eq!(extract_version_from_filename("discogs_20241201_labels.xml.gz"), Some("20241201".to_string()));
    assert_eq!(extract_version_from_filename("discogs_20230615_masters.xml.gz"), Some("20230615".to_string()));
}

#[test]
fn test_extract_version_from_filename_invalid() {
    // No underscores
    assert_eq!(extract_version_from_filename("nounderscore"), None);
    // Single part with no underscore
    assert_eq!(extract_version_from_filename("singlepart"), None);
    // Empty string
    assert_eq!(extract_version_from_filename(""), None);
    // Single underscore should still work (parts.len() == 2)
    assert_eq!(extract_version_from_filename("discogs_20260101"), Some("20260101".to_string()));
}

#[test]
fn test_extract_data_type_with_path_prefix() {
    // Filenames with path components - the split on '_' still works because
    // the path prefix becomes part of parts[0]
    assert_eq!(extract_data_type("2026/discogs_20260101_artists.xml.gz"), Some(DataType::Artists));
    assert_eq!(extract_data_type("data/discogs_20260101_releases.xml.gz"), Some(DataType::Releases));
    assert_eq!(extract_data_type("some/deep/path/discogs_20260101_masters.xml.gz"), Some(DataType::Masters));
}

#[test]
fn test_extract_data_type_empty_string() {
    assert_eq!(extract_data_type(""), None);
}

#[test]
fn test_extract_data_type_checksum_file() {
    // CHECKSUM is not a valid DataType, so extract_data_type should return None
    assert_eq!(extract_data_type("discogs_20260101_CHECKSUM.txt"), None);
}

fn compile_test_rules(yaml: &str) -> Arc<CompiledRulesConfig> {
    let config: RulesConfig = serde_yaml_ng::from_str(yaml).unwrap();
    Arc::new(CompiledRulesConfig::compile(config).unwrap())
}

#[tokio::test]
async fn test_message_validator_no_violations() {
    use tempfile::TempDir;

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

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, mut validated_receiver) = mpsc::channel::<DataMessage>(10);

    // Send a valid message (has name field)
    let msg = DataMessage { id: "1".to_string(), sha256: "abc".to_string(), data: serde_json::json!({"name": "Aphex Twin"}), raw_xml: None };
    parse_sender.send(msg).await.unwrap();
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "artists", temp_dir.path(), "20260301").await.unwrap();

    // Message should be forwarded downstream
    let received = validated_receiver.recv().await.unwrap();
    assert_eq!(received.id, "1");

    // No more messages
    assert!(validated_receiver.recv().await.is_none());

    // Report should have no violations
    assert!(!report.has_violations());
    assert_eq!(report.total_records["artists"], 1);
}

#[tokio::test]
async fn test_message_validator_with_violations() {
    use tempfile::TempDir;

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

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, mut validated_receiver) = mpsc::channel::<DataMessage>(10);

    // Send a message missing the required name field
    let msg = DataMessage { id: "42".to_string(), sha256: "def".to_string(), data: serde_json::json!({"profile": "test"}), raw_xml: None };
    parse_sender.send(msg).await.unwrap();
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "artists", temp_dir.path(), "20260301").await.unwrap();

    // Message should STILL be forwarded (validator doesn't filter)
    let received = validated_receiver.recv().await.unwrap();
    assert_eq!(received.id, "42");
    assert!(validated_receiver.recv().await.is_none());

    // Report should show violation
    assert!(report.has_violations());
    assert_eq!(report.total_records["artists"], 1);
    let rule_counts = &report.counts["artists"]["name_required"];
    assert_eq!(rule_counts.errors, 1);
}

#[tokio::test]
async fn test_message_validator_multiple_messages() {
    use tempfile::TempDir;

    let rules = compile_test_rules(
        r#"
rules:
  releases:
    - name: title_required
      field: title
      condition: {type: required}
      severity: error
    - name: year_range
      field: year
      condition: {type: range, min: 1900, max: 2100}
      severity: warning
"#,
    );

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, mut validated_receiver) = mpsc::channel::<DataMessage>(10);

    // Message 1: valid
    let msg1 =
        DataMessage { id: "1".to_string(), sha256: "a".to_string(), data: serde_json::json!({"title": "Good Album", "year": "2000"}), raw_xml: None };
    // Message 2: missing title (error) + year out of range (warning)
    let msg2 = DataMessage { id: "2".to_string(), sha256: "b".to_string(), data: serde_json::json!({"year": "1800"}), raw_xml: None };
    // Message 3: has title, year ok
    let msg3 = DataMessage {
        id: "3".to_string(),
        sha256: "c".to_string(),
        data: serde_json::json!({"title": "Another Album", "year": "1999"}),
        raw_xml: None,
    };

    parse_sender.send(msg1).await.unwrap();
    parse_sender.send(msg2).await.unwrap();
    parse_sender.send(msg3).await.unwrap();
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "releases", temp_dir.path(), "20260301").await.unwrap();

    // All 3 messages forwarded
    let mut count = 0;
    while validated_receiver.recv().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 3);

    // Check report
    assert_eq!(report.total_records["releases"], 3);
    assert!(report.has_violations());
    assert_eq!(report.counts["releases"]["title_required"].errors, 1);
    assert_eq!(report.counts["releases"]["year_range"].warnings, 1);
}

#[tokio::test]
async fn test_message_validator_writes_flagged_files() {
    use tempfile::TempDir;

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

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, mut validated_receiver) = mpsc::channel::<DataMessage>(10);

    let raw_xml = b"<artist><profile>test</profile></artist>".to_vec();
    let msg = DataMessage { id: "77".to_string(), sha256: "xyz".to_string(), data: serde_json::json!({"profile": "test"}), raw_xml: Some(raw_xml) };
    parse_sender.send(msg).await.unwrap();
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "artists", temp_dir.path(), "20260301").await.unwrap();

    // Consume forwarded messages
    while validated_receiver.recv().await.is_some() {}

    assert!(report.has_violations());

    // Check flagged files were written
    let flagged_dir = temp_dir.path().join("flagged").join("20260301").join("artists");
    assert!(flagged_dir.join("77.xml").exists(), "Flagged XML should be written");
    assert!(flagged_dir.join("77.json").exists(), "Flagged JSON should be written");
    assert!(flagged_dir.join("violations.jsonl").exists(), "Violations JSONL should be written");

    // Check report file — now scoped to the data type's subdir so concurrent
    // per-file validators no longer overwrite a shared report.
    let report_path = temp_dir.path().join("flagged").join("20260301").join("artists").join("report.txt");
    assert!(report_path.exists(), "Per-type report file should be written");
}

#[tokio::test]
async fn test_message_validator_downstream_dropped() {
    use tempfile::TempDir;

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

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, validated_receiver) = mpsc::channel::<DataMessage>(1);

    // Drop receiver before sending messages
    drop(validated_receiver);

    // Send multiple messages — validator should detect dropped receiver and break
    for i in 0..5 {
        let msg = DataMessage {
            id: i.to_string(),
            sha256: format!("hash{}", i),
            data: serde_json::json!({"name": format!("Artist {}", i)}),
            raw_xml: None,
        };
        parse_sender.send(msg).await.unwrap();
    }
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "artists", temp_dir.path(), "20260301").await.unwrap();

    // Should have processed at least 1 but potentially not all (downstream dropped)
    assert!(*report.total_records.get("artists").unwrap_or(&0) >= 1);
}

#[tokio::test]
async fn test_message_validator_no_rules_for_data_type() {
    use tempfile::TempDir;

    // Rules only for "releases", but we validate "artists"
    let rules = compile_test_rules(
        r#"
rules:
  releases:
    - name: title_required
      field: title
      condition: {type: required}
      severity: error
"#,
    );

    let temp_dir = TempDir::new().unwrap();
    let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(10);
    let (validated_sender, mut validated_receiver) = mpsc::channel::<DataMessage>(10);

    let msg = DataMessage { id: "1".to_string(), sha256: "a".to_string(), data: serde_json::json!({}), raw_xml: None };
    parse_sender.send(msg).await.unwrap();
    drop(parse_sender);

    let report = message_validator(parse_receiver, validated_sender, rules, "artists", temp_dir.path(), "20260301").await.unwrap();

    // Message forwarded
    assert!(validated_receiver.recv().await.is_some());
    assert!(validated_receiver.recv().await.is_none());

    // No violations (no rules for artists)
    assert!(!report.has_violations());
    assert_eq!(report.total_records["artists"], 1);
}

// ── message_normalizer tests ───────────────────────────────

/// Regression: normalization + hashing must run unconditionally, not only inside
/// the optional validator stage. Feeds the normalizer the raw xmltodict shape the parser emits
/// (container-wrapped `members`, `@`-prefixed keys, empty sha256) and asserts the output carries
/// the flat consumer-ready shape and a populated content hash — the exact guarantees the no-rules
/// (else-branch) pipeline previously dropped.
#[tokio::test]
async fn test_message_normalizer_normalizes_and_hashes_without_rules() {
    let (in_sender, in_receiver) = mpsc::channel::<DataMessage>(10);
    let (out_sender, mut out_receiver) = mpsc::channel::<DataMessage>(10);

    let msg = DataMessage {
        id: "1".to_string(),
        sha256: String::new(), // parser emits an empty hash — normalizer must fill it in
        data: serde_json::json!({
            "id": "1",
            "name": "Aphex Twin",
            "members": {"name": [{"@id": "7", "#text": "Richard D. James"}]}
        }),
        raw_xml: None,
    };
    in_sender.send(msg).await.unwrap();
    drop(in_sender);

    message_normalizer(in_receiver, out_sender, DataType::Artists).await.unwrap();

    let got = out_receiver.recv().await.unwrap();

    // Content hash is now populated so downstream change detection works.
    assert!(!got.sha256.is_empty(), "normalizer must populate sha256");

    // members is unwrapped into a flat array of objects with `@`/`#text` keys stripped —
    // the shape graphinator's `[m for m in members if m.get('id')]` requires.
    let members = got.data.get("members").expect("members present").as_array().expect("members is array");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].get("id").unwrap(), "7");
    assert_eq!(members[0].get("name").unwrap(), "Richard D. James");
    assert!(members[0].get("@id").is_none(), "@-prefixed keys must be stripped");

    // No more messages.
    assert!(out_receiver.recv().await.is_none());
}

/// The normalizer must produce byte-identical output regardless of which upstream stage feeds it,
/// so the rules and no-rules pipelines converge on the same published record + hash.
#[tokio::test]
async fn test_message_normalizer_hash_is_deterministic() {
    async fn normalize_one(record: serde_json::Value) -> DataMessage {
        let (in_sender, in_receiver) = mpsc::channel::<DataMessage>(1);
        let (out_sender, mut out_receiver) = mpsc::channel::<DataMessage>(1);
        in_sender.send(DataMessage { id: "1".to_string(), sha256: String::new(), data: record, raw_xml: None }).await.unwrap();
        drop(in_sender);
        message_normalizer(in_receiver, out_sender, DataType::Artists).await.unwrap();
        out_receiver.recv().await.unwrap()
    }

    let record = serde_json::json!({"id": "1", "name": "Aphex Twin", "members": {"name": [{"@id": "7", "#text": "Richard"}]}});
    let a = normalize_one(record.clone()).await;
    let b = normalize_one(record).await;
    assert_eq!(a.sha256, b.sha256);
    assert!(!a.sha256.is_empty());
}
