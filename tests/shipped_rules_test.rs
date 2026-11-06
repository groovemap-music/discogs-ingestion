//! The shipped `extraction-rules.yaml`, run through the real parser and the real validator stage
//! on real-shaped Discogs XML, one record per rule that should trip it.
//!
//! Finding (discogs-ingestion-y5g): the 20261101 reports said "No data quality violations found"
//! for every type. The rules load, compile and match the parsed record shape, so zero was not a
//! shape bug. But the extractor applies `filters` before `rules`, and three shipped rules watched
//! values a filter had already repaired (numeric genres, implausible years), so they could never
//! fire, and the repairs appeared nowhere in the report. Now: filters stay first, the shadowed
//! rules are retired, every remaining rule is proven below to fire, and each filter's repairs are
//! counted in the quality report.

use extractor::extractor::message_validator;
use extractor::parser::XmlParser;
use extractor::rules::{CompiledRulesConfig, FilterCondition, QualityReport, RuleCondition, RulesConfig};
use extractor::types::{DataMessage, DataType};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
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

// ── Filters: repairs are counted, and no rule is shadowed by one ────

#[tokio::test]
async fn a_numeric_genre_is_stripped_and_the_repair_is_reported() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<releases><release id="103"><title>T</title><genres><genre>1</genre><genre>Rock</genre></genres></release></releases>"#;
    let (forwarded, report) = run_pipeline(DataType::Releases, xml).await;
    assert!(fired(&report, "releases").is_empty(), "the filter removed the numeric genre before validation");
    assert_eq!(forwarded[0].data["genres"]["genre"], json!(["Rock"]));
    let counts = &report.filtered["releases"][&("genres.genre".to_string(), "Strip legacy numeric genre IDs from upstream data".to_string())];
    assert_eq!((counts.records, counts.values), (1, 1));
    let summary = report.format_summary("20261101");
    assert!(!summary.contains("No data quality violations found"), "a run that repaired values is not all clear:\n{summary}");
    assert!(summary.contains("releases: genres.genre: 1 values in 1 records"), "{summary}");
}

#[tokio::test]
async fn an_implausible_master_year_is_nullified_and_the_repair_is_reported() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<masters><master id="5"><title>T</title><year>197</year></master><master id="6"><title>U</title><year>1999</year></master></masters>"#;
    let (forwarded, report) = run_pipeline(DataType::Masters, xml).await;
    assert!(forwarded[0].data["year"].is_null());
    assert_eq!(forwarded[1].data["year"], json!("1999"));
    let counts = &report.filtered["masters"][&("year".to_string(), "Treat sentinel and implausible years as unknown".to_string())];
    assert_eq!((counts.records, counts.values), (1, 1));
    assert!(report.format_summary("20261101").contains("masters: year: 1 values in 1 records"));
}

#[tokio::test]
async fn an_implausible_release_date_is_nullified_and_the_repair_is_reported() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<releases><release id="104"><title>T</title><released>0400-01-01</released></release></releases>"#;
    let (forwarded, report) = run_pipeline(DataType::Releases, xml).await;
    assert!(forwarded[0].data["released"].is_null());
    let counts = &report.filtered["releases"][&("released".to_string(), "Treat sentinel and implausible release dates as unknown".to_string())];
    assert_eq!((counts.records, counts.values), (1, 1));
}

#[tokio::test]
async fn a_run_with_nothing_to_repair_still_reads_all_clear() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<masters><master id="6"><title>U</title><year>1999</year></master></masters>"#;
    let (_, report) = run_pipeline(DataType::Masters, xml).await;
    assert!(!report.has_filtered_values());
}

#[test]
fn filtered_counts_merge_across_reports() {
    let mut a = QualityReport::new();
    let mut b = QualityReport::new();
    let action = extractor::rules::FilterAction { field: "year".into(), removed_count: 2, removed_values: vec![], reason: "r".into() };
    a.record_filter_actions("masters", std::slice::from_ref(&action));
    b.record_filter_actions("masters", std::slice::from_ref(&action));
    a.merge(b);
    let counts = &a.filtered["masters"][&("year".to_string(), "r".to_string())];
    assert_eq!((counts.records, counts.values), (2, 4));
}

/// A rule whose violations a filter has already repaired away can never fire: a regex rule
/// with the pattern a `remove_matching` filter strips, or a range rule inside the bounds a
/// `nullify_when` filter nullifies outside of, on the same field. (A rule on the same field
/// that catches more than the filter repairs, like `genre-not-recognized`, is not shadowed.)
/// Fails if the shipped file ever ships such a rule again.
#[test]
fn no_shipped_rule_is_shadowed_by_a_filter_on_the_same_field() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("extraction-rules.yaml");
    let config = RulesConfig::load(&path).unwrap();
    for (data_type, rules) in &config.rules {
        let filters = config.filters.get(data_type).map(Vec::as_slice).unwrap_or_default();
        for rule in rules {
            for filter in filters {
                let shadowed = match (filter, &rule.condition) {
                    (FilterCondition::RemoveMatching { field, remove_matching, .. }, RuleCondition::Regex { pattern }) => {
                        *field == rule.field && remove_matching == pattern
                    }
                    (FilterCondition::NullifyWhen { field, nullify_when, .. }, RuleCondition::Range { min, max }) => {
                        *field == rule.field
                            && min.is_none_or(|m| nullify_when.below.is_some_and(|b| m >= b))
                            && max.is_none_or(|m| nullify_when.above.is_some_and(|a| m <= a))
                            && (min.is_some() || max.is_some())
                    }
                    _ => false,
                };
                assert!(!shadowed, "rule {} on {data_type}.{} is shadowed by a filter that repairs the same values first", rule.name, rule.field);
            }
        }
    }
}

#[test]
fn the_shipped_file_has_seven_rules_and_each_is_covered_here() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("extraction-rules.yaml");
    let config = RulesConfig::load(&path).unwrap();
    let mut names: Vec<String> = config.rules.values().flatten().map(|r| r.name.clone()).collect();
    names.sort();
    assert_eq!(
        names,
        [
            "company-role-not-recognized",
            "empty-artist-name",
            "empty-label-name",
            "format-not-recognized",
            "genre-not-recognized",
            "identifier-type-not-recognized",
            "missing-title",
        ],
        "a rule added or removed needs a record here that trips it"
    );
}
