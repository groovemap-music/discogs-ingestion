//! Canonical companies block (ADR 0011) mapper tests.
//!
//! The conformance suite in `fixtures/companies/` is vendored verbatim from the design
//! repository's `taxonomy/company-roles/v1/fixtures/`. Those input/expected pairs are the
//! contract between this producer, the shared Python mapper, and the design repository's
//! reference mapper: all three must reproduce the same block byte for byte. Never edit a
//! fixture to make this code pass — re-vendor the suite when the vocabulary changes.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::discogs::companies::{attach_companies_block, map_discogs_companies};
use crate::discogs::message_normalizer;
use crate::types::{DataMessage, DataType};

/// The vendored vocabulary, read here independently of the mapper so a test can demand the
/// fixtures exercise the vocabulary's whole closed set rather than whichever part the mapper
/// happens to reach.
const VOCABULARY_JSON: &str = include_str!("../../../contracts/catalog-events/vocab/company-roles.json");

/// The vendored conformance suite: 10 pairs at the pinned design commit, every one of them a
/// Discogs pair, since only this producer computes the block.
const FIXTURE_TOTAL: usize = 10;

fn vocabulary() -> Value {
    serde_json::from_str(VOCABULARY_JSON).expect("the vendored company-role vocabulary is valid JSON")
}

fn fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/discogs/tests/fixtures/companies")
}

fn fixtures() -> Vec<(String, Value)> {
    let mut loaded: Vec<(String, Value)> = fs::read_dir(fixture_directory())
        .expect("the vendored company fixture directory is readable")
        .map(|entry| entry.expect("the fixture directory entry is readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
        .map(|path| {
            let name = path.file_name().expect("the fixture has a file name").to_string_lossy().to_string();
            let text = fs::read_to_string(&path).expect("the fixture is readable");
            let value: Value = serde_json::from_str(&text).unwrap_or_else(|error| panic!("{name} is valid JSON: {error}"));
            (name, value)
        })
        .collect();
    loaded.sort_by(|left, right| left.0.cmp(&right.0));
    loaded
}

fn map(input: &Value) -> Value {
    serde_json::to_value(map_discogs_companies(input.get("companies"))).expect("the companies block serializes")
}

// ── Conformance ─────────────────────────────────────────────────────

/// Guard the vendored suite itself: a fixture silently lost or added would otherwise make the
/// conformance test below vacuously weaker.
#[test]
fn test_vendored_fixture_suite_is_complete() {
    let all = fixtures();
    let discogs = all.iter().filter(|(_, fixture)| fixture["provider"] == json!("discogs")).count();
    assert_eq!(all.len(), FIXTURE_TOTAL, "the vendored suite must match the pinned design commit file for file");
    assert_eq!(discogs, FIXTURE_TOTAL, "every company fixture is a discogs pair");
}

/// Every conformance pair: run the fixture's raw input through the mapper and demand the exact
/// block the design repository's reference mapper produces, field for field.
#[test]
fn test_discogs_conformance_fixtures() {
    let mut checked = 0;
    for (name, fixture) in fixtures() {
        let produced = map(&fixture["input"]);
        let expected = &fixture["expected"];
        assert_eq!(
            &produced,
            expected,
            "fixture {name} does not match the reference mapper\n  produced: {}\n  expected: {}",
            serde_json::to_string_pretty(&produced).unwrap_or_default(),
            serde_json::to_string_pretty(expected).unwrap_or_default()
        );
        checked += 1;
    }
    assert_eq!(checked, FIXTURE_TOTAL, "every company fixture must be exercised");
}

/// The suite must reach every role category, so no member of the closed set is mapped only in
/// theory. A category the fixtures never produce is a category no implementation is proved on.
#[test]
fn test_fixtures_cover_every_role_category() {
    let vocabulary = vocabulary();
    let declared: BTreeSet<String> = vocabulary["role_categories"]
        .as_array()
        .expect("the vocabulary declares its categories")
        .iter()
        .map(|entry| entry["id"].as_str().expect("a category id is a string").to_string())
        .collect();

    let produced: BTreeSet<String> = fixtures()
        .iter()
        .flat_map(|(_, fixture)| fixture["expected"]["items"].as_array().cloned().unwrap_or_default())
        .map(|item| item["role_category"].as_str().expect("a category is a string").to_string())
        .collect();

    assert_eq!(produced, declared, "the conformance suite must exercise every role category");
}

/// Every raw role string the vocabulary carries must map to a declared category, so a
/// re-vendoring that introduces a typo fails here rather than downstream.
#[test]
fn test_every_raw_role_maps_to_a_declared_category() {
    let vocabulary = vocabulary();
    let declared: BTreeSet<&str> = vocabulary["role_categories"]
        .as_array()
        .expect("the vocabulary declares its categories")
        .iter()
        .map(|entry| entry["id"].as_str().expect("a category id is a string"))
        .collect();

    let roles = vocabulary["discogs"]["roles"].as_object().expect("the vocabulary maps raw roles");
    assert!(!roles.is_empty(), "the vocabulary must map at least one raw role");
    for (role, category) in roles {
        let category = category.as_str().expect("a mapped category is a string");
        assert!(declared.contains(category), "raw role {role} maps to an undeclared category: {category}");
    }
    assert!(!roles.contains_key("Label"), "the issuing label is not a company credit");
}

// ── Shape guarantees ────────────────────────────────────────────────

/// A release the dump gave no companies still carries the block, empty — never a missing key,
/// so no consumer has to branch on its absence.
#[test]
fn test_absent_input_produces_the_empty_block() {
    let produced = serde_json::to_value(map_discogs_companies(None)).expect("the companies block serializes");
    assert_eq!(produced, json!({"companies_version": "1", "items": [], "role_categories": [], "unmapped": {"roles": []}}));
}

/// A `companies` value that is not a list is treated as no companies rather than panicking.
#[test]
fn test_non_array_input_produces_the_empty_block() {
    for value in [json!(null), json!("Pressed By"), json!({"company": []}), json!(7)] {
        let block = map_discogs_companies(Some(&value));
        assert!(block.items.is_empty(), "{value} must yield no items");
        assert!(block.role_categories.is_empty(), "{value} must yield no categories");
    }
}

/// Every field is present with an explicit null or empty list, so two implementations
/// serialize the same bytes.
#[test]
fn test_every_field_is_always_present() {
    let produced = map(&json!({"companies": [{"name": "Damont", "entity_type_name": "Pressed By"}]}));
    let block = produced.as_object().expect("the block is an object");
    for key in ["companies_version", "items", "role_categories", "unmapped"] {
        assert!(block.contains_key(key), "the block must carry {key}");
    }
    let item = produced["items"][0].as_object().expect("the item is an object");
    for key in ["name", "discogs_id", "role", "role_category", "catno", "source"] {
        assert!(item.contains_key(key), "the item must carry {key}");
    }
    let source = produced["items"][0]["source"].as_object().expect("the source is an object");
    for key in ["provider", "entity_type"] {
        assert!(source.contains_key(key), "the source must carry {key}");
    }
}

// ── Mapping rules ───────────────────────────────────────────────────

/// A raw role the vocabulary does not carry falls to `other` and is additionally recorded, so
/// a new upstream string is measurable rather than invisible.
#[test]
fn test_unmapped_role_falls_to_other_and_is_recorded() {
    let produced = map(&json!({"companies": [{"name": "Sarm West", "entity_type_name": "Remixed At"}]}));
    assert_eq!(produced["items"][0]["role_category"], json!("other"));
    assert_eq!(produced["items"][0]["role"], json!("Remixed At"), "the raw role survives verbatim");
    assert_eq!(produced["unmapped"]["roles"], json!(["Remixed At"]));
}

/// A role the vocabulary knows and deliberately routes to `other` is mapped, not
/// unrecognised, so it never inflates the unmapped report.
#[test]
fn test_role_mapped_to_other_is_not_reported_as_unmapped() {
    let produced = map(&json!({"companies": [{"name": "Stylorouge", "entity_type_name": "Designed At"}]}));
    assert_eq!(produced["items"][0]["role_category"], json!("other"));
    assert_eq!(produced["unmapped"]["roles"], json!([]));
}

/// Unmapped roles are sorted and de-duplicated however often they repeat, and categories are
/// sorted while items keep source order.
#[test]
fn test_unmapped_roles_are_sorted_and_deduplicated() {
    let produced = map(&json!({
        "companies": [
            {"name": "One", "entity_type_name": "Zed At"},
            {"name": "Two", "entity_type_name": "Pressed By"},
            {"name": "Three", "entity_type_name": "Alpha At"},
            {"name": "Four", "entity_type_name": "Zed At"}
        ]
    }));
    assert_eq!(produced["unmapped"]["roles"], json!(["Alpha At", "Zed At"]));
    assert_eq!(produced["role_categories"], json!(["other", "pressing"]));
    let names: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["name"]).collect();
    assert_eq!(names, vec![&json!("One"), &json!("Two"), &json!("Three"), &json!("Four")], "items keep source order");
}

/// The lacquer categories separate the room that cut the transfer master from the audio
/// mastering house, which is the distinction the vocabulary exists to keep.
#[test]
fn test_transfer_master_and_mastering_stay_separate() {
    let produced = map(&json!({
        "companies": [
            {"name": "Utopia Studios", "entity_type_name": "Lacquer Cut At"},
            {"name": "Sony DADC", "entity_type_name": "Glass Mastered At"},
            {"name": "Record Industry", "entity_type_name": "Plated At"},
            {"name": "Abbey Road Studios", "entity_type_name": "Mastered At"}
        ]
    }));
    let categories: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["role_category"]).collect();
    assert_eq!(categories, vec![&json!("lacquer"), &json!("lacquer"), &json!("lacquer"), &json!("mastering")]);
}

/// The dump states the label id as element text and the API states it as a number; both read
/// as the same id, and an id Discogs uses for "no label" is not one.
#[test]
fn test_discogs_id_reads_both_shapes_and_rejects_the_absent_one() {
    let produced = map(&json!({
        "companies": [
            {"name": "A", "entity_type_name": "Pressed By", "id": 31234},
            {"name": "B", "entity_type_name": "Pressed By", "id": "31234"},
            {"name": "C", "entity_type_name": "Pressed By", "id": 0},
            {"name": "D", "entity_type_name": "Pressed By", "id": "plant"},
            {"name": "E", "entity_type_name": "Pressed By"}
        ]
    }));
    let ids: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["discogs_id"]).collect();
    assert_eq!(ids, vec![&json!(31234), &json!(31234), &json!(null), &json!(null), &json!(null)]);
}

/// The numeric relationship code is carried as the digit string the schema admits; anything
/// else is `null`.
#[test]
fn test_entity_type_keeps_only_a_numeric_code() {
    let produced = map(&json!({
        "companies": [
            {"name": "A", "entity_type_name": "Pressed By", "entity_type": "17"},
            {"name": "B", "entity_type_name": "Pressed By", "entity_type": 17},
            {"name": "C", "entity_type_name": "Pressed By", "entity_type": "plant"},
            {"name": "D", "entity_type_name": "Pressed By", "entity_type": ""},
            {"name": "E", "entity_type_name": "Pressed By"}
        ]
    }));
    let codes: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["source"]["entity_type"]).collect();
    assert_eq!(codes, vec![&json!("17"), &json!("17"), &json!(null), &json!(null), &json!(null)]);
}

/// A company entry may carry its own number, such as a plant job number; a blank one is null.
#[test]
fn test_company_catalogue_number_is_trimmed_or_null() {
    let produced = map(&json!({
        "companies": [
            {"name": "EMI Uden", "entity_type_name": "Pressed By", "catno": "  S-12345  "},
            {"name": "Damont", "entity_type_name": "Pressed By", "catno": ""},
            {"name": "MPO", "entity_type_name": "Pressed By"}
        ]
    }));
    assert_eq!(produced["items"][0]["catno"], json!("S-12345"));
    assert_eq!(produced["items"][1]["catno"], json!(null));
    assert_eq!(produced["items"][2]["catno"], json!(null));
}

/// An entry that never named a credit is skipped entirely — not even as an unmapped value,
/// which would report a role nobody stated.
#[test]
fn test_malformed_entries_are_skipped_without_being_reported() {
    let produced = map(&json!({
        "companies": [
            null,
            [],
            "Pressed By",
            5,
            {"name": "   ", "entity_type_name": "Pressed By"},
            {"name": "Damont"},
            {"name": "Damont", "entity_type_name": "   "},
            {"name": "Damont", "entity_type_name": 17}
        ]
    }));
    assert_eq!(produced["items"], json!([]));
    assert_eq!(produced["unmapped"]["roles"], json!([]));
}

/// An upstream string that happens to name a built-in map member must still land in
/// `unmapped` rather than resolving to something the vocabulary never declared.
#[test]
fn test_prototype_like_role_names_are_not_mapped() {
    for name in ["constructor", "toString", "hasOwnProperty", "__proto__"] {
        let produced = map(&json!({"companies": [{"name": "A", "entity_type_name": name}]}));
        assert_eq!(produced["items"][0]["role_category"], json!("other"), "{name} must not resolve to an inherited member");
        assert_eq!(produced["unmapped"]["roles"], json!([name]));
    }
}

// ── Attachment ──────────────────────────────────────────────────────

/// The block takes the `companies` key; the issuing label stays a label relation and is never
/// read as a credit.
#[test]
fn test_attach_replaces_the_raw_list_and_ignores_labels() {
    let mut record = json!({
        "id": "1",
        "companies": [{"name": "Damont", "entity_type_name": "Pressed By", "entity_type": "17", "id": "12345"}],
        "labels": [{"name": "RCA", "catno": "PB 41447", "entity_type_name": "Label", "id": "895"}]
    });
    attach_companies_block(&mut record);

    assert_eq!(record["companies"]["companies_version"], json!("1"));
    assert_eq!(record["companies"]["items"].as_array().expect("items is a list").len(), 1, "the issuing label is not a credit");
    assert_eq!(record["companies"]["items"][0]["role"], json!("Pressed By"), "the raw role survives in the block");
    assert_eq!(record["companies"]["items"][0]["discogs_id"], json!(12345));
    assert_eq!(record["labels"][0]["name"], json!("RCA"), "labels are left exactly as they were");
}

/// A record that is not an object is left alone rather than panicking.
#[test]
fn test_attach_ignores_a_non_object_record() {
    let mut record = json!("not a record");
    attach_companies_block(&mut record);
    assert_eq!(record, json!("not a record"));
}

// ── The normalization boundary ──────────────────────────────────────

async fn normalize_one(data_type: DataType, record: Value) -> DataMessage {
    let (in_sender, in_receiver) = mpsc::channel::<DataMessage>(1);
    let (out_sender, mut out_receiver) = mpsc::channel::<DataMessage>(1);
    in_sender
        .send(DataMessage { id: "1".to_string(), sha256: String::new(), data: record, raw_xml: None })
        .await
        .expect("the message is queued");
    drop(in_sender);
    message_normalizer(in_receiver, out_sender, data_type).await.expect("the normalizer runs");
    out_receiver.recv().await.expect("the normalizer emits the message")
}

/// The normalizer attaches the block to the XML-shaped release the parser emits, mapping the
/// list after it has been unwrapped and de-prefixed. The dump's string id and numeric code
/// arrive exactly this way.
#[tokio::test]
async fn test_normalizer_attaches_the_block_to_releases() {
    let record = json!({
        "id": "1",
        "title": "A Release",
        "companies": {"company": [
            {"id": "12345", "name": "Damont", "catno": "", "entity_type": "17", "entity_type_name": "Pressed By"},
            {"id": "266218", "name": "Utopia Studios", "entity_type": "30", "entity_type_name": "Lacquer Cut At"}
        ]}
    });
    let got = normalize_one(DataType::Releases, record).await;

    assert_eq!(got.data["companies"]["items"][0]["discogs_id"], json!(12345));
    assert_eq!(got.data["companies"]["items"][0]["role_category"], json!("pressing"));
    assert_eq!(got.data["companies"]["items"][0]["source"]["entity_type"], json!("17"));
    assert_eq!(got.data["companies"]["items"][0]["catno"], json!(null));
    assert_eq!(got.data["companies"]["items"][1]["role_category"], json!("lacquer"));
    assert_eq!(got.data["companies"]["role_categories"], json!(["lacquer", "pressing"]));
    assert!(!got.sha256.is_empty(), "the normalizer populates the content hash");
}

/// A release the dump gave no companies carries the empty block, not a missing key.
#[tokio::test]
async fn test_normalizer_attaches_the_empty_block_when_nothing_is_stated() {
    let got = normalize_one(DataType::Releases, json!({"id": "1", "title": "A Release"})).await;
    assert_eq!(got.data["companies"], json!({"companies_version": "1", "items": [], "role_categories": [], "unmapped": {"roles": []}}));
}

/// Only releases carry a block: the other data types have no company credits.
#[tokio::test]
async fn test_normalizer_attaches_no_block_to_other_types() {
    for data_type in [DataType::Artists, DataType::Labels, DataType::Masters] {
        let got = normalize_one(data_type, json!({"id": "1", "name": "Aphex Twin"})).await;
        assert!(got.data.get("companies").is_none(), "{data_type:?} must not carry a companies block");
    }
}

/// The block is attached before the hash, so a changed manufacturing credit changes the
/// content hash consumers key change detection on.
#[tokio::test]
async fn test_hash_covers_the_companies_block() {
    let release = |plant: &str| {
        json!({
            "id": "1",
            "title": "A Release",
            "companies": {"company": [{"id": "1", "name": plant, "entity_type": "17", "entity_type_name": "Pressed By"}]}
        })
    };
    let first = normalize_one(DataType::Releases, release("Damont")).await;
    let second = normalize_one(DataType::Releases, release("EMI Uden")).await;
    let first_again = normalize_one(DataType::Releases, release("Damont")).await;

    assert_ne!(first.sha256, second.sha256, "a different pressing plant must change the hash");
    assert_eq!(first.sha256, first_again.sha256, "the same record must hash the same");
}

/// A role that changes category changes the hash even when the raw bytes barely move.
#[tokio::test]
async fn test_hash_covers_a_changed_role_category() {
    let release = |role: &str| {
        json!({
            "id": "1",
            "title": "A Release",
            "companies": {"company": [{"id": "1", "name": "A Room", "entity_type_name": role}]}
        })
    };
    let lacquer = normalize_one(DataType::Releases, release("Lacquer Cut At")).await;
    let mastering = normalize_one(DataType::Releases, release("Mastered At")).await;

    assert_ne!(lacquer.sha256, mastering.sha256);
    assert_eq!(lacquer.data["companies"]["items"][0]["role_category"], json!("lacquer"));
    assert_eq!(mastering.data["companies"]["items"][0]["role_category"], json!("mastering"));
}

/// Attaching the block is what makes the hash differ: the same record hashed without it would
/// collide with one whose companies differ only through the vocabulary.
#[tokio::test]
async fn test_hash_differs_from_the_same_record_without_a_block() {
    let record = json!({
        "id": "1",
        "title": "A Release",
        "companies": {"company": [{"id": "1", "name": "Damont", "entity_type_name": "Pressed By"}]}
    });
    let with_block = normalize_one(DataType::Releases, record).await;

    let mut without_block = with_block.data.clone();
    without_block.as_object_mut().expect("the record is an object").remove("companies");
    assert_ne!(with_block.sha256, crate::types::calculate_content_hash(&without_block), "the hash must cover the companies block");
}
