//! The shipped `extraction-rules.yaml`, run through the real parser and the real validator stage
//! on real-shaped Discogs XML, one record per rule that should trip it.
//!
//! Finding recorded here (discogs-ingestion-y5g): the 20261101 reports said "No data quality
//! violations found" for every type. The rules load, compile and match the parsed record shape,
//! so zero is not a shape bug. But the extractor applies `filters` before `rules`, and three
//! rules watch values a filter has already removed, so they cannot fire in the pipeline:
//! `genre-is-numeric`, the masters `year-out-of-range`, and the releases `year-out-of-range`
//! (a release carries `released`, not `year`, so that rule has nothing to read at all). The
//! filters clean those values silently, so a real fix to the data is invisible in the report.
//! Zero violations for the other seven rules is genuine only when the dump is clean for them;
//! each one is proven below to fire on a record that breaks it.

use extractor::extractor::message_validator;
use extractor::parser::XmlParser;
use extractor::rules::{CompiledRulesConfig, QualityReport, RulesConfig, Severity, evaluate_rules};
use extractor::types::{DataMessage, DataType};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::mpsc;

fn shipped_rules() -> Arc<CompiledRulesConfig> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("extraction-rules.yaml");
    let config = RulesConfig::load(&path).expect("the shipped rules file loads");
    Arc::new(CompiledRulesConfig::compile(config).expect("the shipped rules file compiles"))
}

/// Parse a gzipped dump of `xml` as `data_type` and validate it with the shipped rules, as the
/// extractor does. Returns the forwarded messages and the quality report.
async fn run_pipeline(data_type: DataType, xml: &str) -> (Vec<DataMessage>, QualityReport) {
    let mut file = NamedTempFile::new().unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(xml.as_bytes()).unwrap();
    file.write_all(&encoder.finish().unwrap()).unwrap();
    file.flush().unwrap();

    let (parsed_tx, parsed_rx) = mpsc::channel::<DataMessage>(64);
    let (valid_tx, mut valid_rx) = mpsc::channel::<DataMessage>(64);
    let root = TempDir::new().unwrap();
    let validator = tokio::spawn({
        let rules = shipped_rules();
        let root = root.path().to_path_buf();
        let name = data_type.as_str().to_string();
        async move { message_validator(parsed_rx, valid_tx, rules, &name, &root, "20261101").await }
    });
    XmlParser::new(data_type, parsed_tx).parse_file(file.path()).await.unwrap();
    let mut forwarded = Vec::new();
    while let Some(message) = valid_rx.recv().await {
        forwarded.push(message);
    }
    (forwarded, validator.await.unwrap().unwrap())
}

fn fired(report: &QualityReport, data_type: &str) -> Vec<String> {
    report.counts.get(data_type).map(|rules| rules.keys().cloned().collect()).unwrap_or_default()
}

const BAD_RELEASE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<releases>
  <release id="101" status="Accepted">
    <artists><artist><id>1</id><name>Someone</name></artist></artists>
    <genres><genre>Skiffle</genre></genres>
    <formats><format name="Wax Cylinder" qty="1" text=""><descriptions><description>LP</description></descriptions></format></formats>
    <identifiers><identifier type="Bogus Code" description="" value="X1"/></identifiers>
    <companies><company><id>9</id><name>Acme</name><catno></catno><entity_type>99</entity_type><entity_type_name>Bogus Role</entity_type_name><resource_url>https://api.discogs.com/labels/9</resource_url></company></companies>
    <released>1969-09-26</released>
  </release>
</releases>"#;

#[tokio::test]
async fn a_release_that_breaks_the_release_rules_trips_each_of_them() {
    let (forwarded, report) = run_pipeline(DataType::Releases, BAD_RELEASE).await;
    assert_eq!(forwarded.len(), 1, "a violation never stops a record being published");
    assert_eq!(
        fired(&report, "releases"),
        [
            "company-role-not-recognized",
            "format-not-recognized",
            "genre-not-recognized",
            "identifier-type-not-recognized",
            "missing-title",
        ]
    );
    assert_eq!(report.counts["releases"]["missing-title"].errors, 1);
    assert_eq!(report.counts["releases"]["genre-not-recognized"].warnings, 1);
    assert!(report.format_summary("20261101").contains("missing-title: 1 errors"));
}

#[tokio::test]
async fn a_clean_release_trips_nothing() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<releases>
  <release id="102" status="Accepted">
    <title>Abbey Road</title>
    <genres><genre>Rock</genre></genres>
    <formats><format name="Vinyl" qty="1" text=""/></formats>
    <identifiers><identifier type="Barcode" description="" value="123"/></identifiers>
    <companies><company><id>9</id><name>Acme</name><entity_type>1</entity_type><entity_type_name>Pressed By</entity_type_name></company></companies>
    <released>1969-09-26</released>
  </release>
</releases>"#;
    let (_, report) = run_pipeline(DataType::Releases, xml).await;
    assert!(fired(&report, "releases").is_empty());
}

#[tokio::test]
async fn an_artist_without_a_name_trips_empty_artist_name() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists><artist id="1"><profile>No name here</profile></artist></artists>"#;
    let (forwarded, report) = run_pipeline(DataType::Artists, xml).await;
    assert_eq!(forwarded.len(), 1);
    assert_eq!(fired(&report, "artists"), ["empty-artist-name"]);
}

#[tokio::test]
async fn a_label_without_a_name_trips_empty_label_name() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<labels><label><id>1</id><profile>No name here</profile></label></labels>"#;
    let (forwarded, report) = run_pipeline(DataType::Labels, xml).await;
    assert_eq!(forwarded.len(), 1);
    assert_eq!(fired(&report, "labels"), ["empty-label-name"]);
}

#[tokio::test]
async fn records_marked_do_not_use_are_skipped_not_validated() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<artists><artist id="7"><name>Junk</name><profile>DO NOT USE</profile></artist></artists>"#;
    let (forwarded, report) = run_pipeline(DataType::Artists, xml).await;
    assert!(forwarded.is_empty());
    assert_eq!(report.skipped["artists"].len(), 1);
}

// ── Rules a filter shadows ─────────────────────────────────────────

#[tokio::test]
async fn a_numeric_genre_is_stripped_by_the_filter_before_genre_is_numeric_can_see_it() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<releases><release id="103"><title>T</title><genres><genre>1</genre><genre>Rock</genre></genres></release></releases>"#;
    let (forwarded, report) = run_pipeline(DataType::Releases, xml).await;
    assert!(fired(&report, "releases").is_empty(), "the filter removed the numeric genre before validation");
    assert_eq!(forwarded[0].data["genres"]["genre"], json!(["Rock"]));
}

#[tokio::test]
async fn an_implausible_master_year_is_nullified_by_the_filter_before_year_out_of_range_can_see_it() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<masters><master id="5"><title>T</title><year>197</year></master></masters>"#;
    let (forwarded, report) = run_pipeline(DataType::Masters, xml).await;
    assert!(fired(&report, "masters").is_empty(), "the filter nullified the year before validation");
    assert!(forwarded[0].data["year"].is_null());
}

#[test]
fn the_shadowed_rules_themselves_do_fire_on_an_unfiltered_record() {
    let rules = shipped_rules();
    let genre = evaluate_rules(&rules, "releases", &json!({"@id": "1", "title": "T", "genres": {"genre": "1"}}));
    assert!(genre.iter().any(|v| v.rule_name == "genre-is-numeric" && matches!(v.severity, Severity::Error)));
    let master = evaluate_rules(&rules, "masters", &json!({"@id": "5", "year": "197"}));
    assert!(master.iter().any(|v| v.rule_name == "year-out-of-range"));
    // The releases rule reads `year`, a field a Discogs release does not carry.
    let release = evaluate_rules(&rules, "releases", &json!({"@id": "1", "title": "T", "year": "197"}));
    assert!(release.iter().any(|v| v.rule_name == "year-out-of-range"));
    let real_release: Value = json!({"@id": "1", "title": "T", "released": "0400-01-01"});
    assert!(evaluate_rules(&rules, "releases", &real_release).is_empty(), "no `year` on a real release, so nothing to range-check");
}
