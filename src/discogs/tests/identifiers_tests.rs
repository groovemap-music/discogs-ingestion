//! Canonical identifiers block (ADR 0011) mapper tests.
//!
//! The conformance suite in `fixtures/identifiers/` is vendored verbatim from the design
//! repository's `taxonomy/identifiers/v1/fixtures/`. Those input/expected pairs are the
//! contract between this producer, the shared Python mapper, and the design repository's
//! reference mapper: all three must reproduce the same block byte for byte. Never edit a
//! fixture to make this code pass — re-vendor the suite when the vocabulary changes.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::discogs::identifiers::{attach_identifiers_block, map_discogs_identifiers};
use crate::discogs::message_normalizer;
use crate::types::{DataMessage, DataType};

/// The vendored vocabulary, read here independently of the mapper so a test can demand the
/// fixtures exercise the vocabulary's whole closed set rather than whichever part the mapper
/// happens to reach.
const VOCABULARY_JSON: &str = include_str!("../../../contracts/catalog-events/vocab/identifier-types.json");

/// The vendored conformance suite: 10 pairs at the pinned design commit, every one of them a
/// Discogs pair, since only this producer computes the block.
const FIXTURE_TOTAL: usize = 10;

fn vocabulary() -> Value {
    serde_json::from_str(VOCABULARY_JSON).expect("the vendored identifier vocabulary is valid JSON")
}

fn fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/discogs/tests/fixtures/identifiers")
}

fn fixtures() -> Vec<(String, Value)> {
    let mut loaded: Vec<(String, Value)> = fs::read_dir(fixture_directory())
        .expect("the vendored identifier fixture directory is readable")
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

/// Map a raw release the way the fixtures express it: an input object carrying `identifiers`,
/// `labels`, both, or neither.
fn map(input: &Value) -> Value {
    serde_json::to_value(map_discogs_identifiers(input.get("identifiers"), input.get("labels"))).expect("the identifiers block serializes")
}

// ── Conformance ─────────────────────────────────────────────────────

/// Guard the vendored suite itself: a fixture silently lost or added would otherwise make the
/// conformance test below vacuously weaker.
#[test]
fn test_vendored_fixture_suite_is_complete() {
    let all = fixtures();
    let discogs = all.iter().filter(|(_, fixture)| fixture["provider"] == json!("discogs")).count();
    assert_eq!(all.len(), FIXTURE_TOTAL, "the vendored suite must match the pinned design commit file for file");
    assert_eq!(discogs, FIXTURE_TOTAL, "every identifier fixture is a discogs pair");
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
    assert_eq!(checked, FIXTURE_TOTAL, "every identifier fixture must be exercised");
}

/// The suite must reach every canonical type, so no member of the closed set is mapped only
/// in theory. A type the fixtures never produce is a type no implementation is proved on.
#[test]
fn test_fixtures_cover_every_canonical_type() {
    let vocabulary = vocabulary();
    let declared: BTreeSet<String> = vocabulary["identifier_types"]
        .as_array()
        .expect("the vocabulary declares its types")
        .iter()
        .map(|entry| entry["id"].as_str().expect("a type id is a string").to_string())
        .collect();

    let produced: BTreeSet<String> = fixtures()
        .iter()
        .flat_map(|(_, fixture)| fixture["expected"]["items"].as_array().cloned().unwrap_or_default())
        .map(|item| item["type"].as_str().expect("an item type is a string").to_string())
        .collect();

    assert_eq!(produced, declared, "the conformance suite must exercise every canonical identifier type");
}

/// Every alias namespace must be minted by at least one fixture, so a normalization rule
/// cannot rot unnoticed.
#[test]
fn test_fixtures_mint_every_alias_namespace() {
    let vocabulary = vocabulary();
    let declared: BTreeSet<String> = vocabulary["alias_namespaces"]
        .as_array()
        .expect("the vocabulary declares its namespaces")
        .iter()
        .map(|entry| entry["provider"].as_str().expect("a provider is a string").to_string())
        .collect();

    let minted: BTreeSet<String> = fixtures()
        .iter()
        .flat_map(|(_, fixture)| fixture["expected"]["aliases"].as_array().cloned().unwrap_or_default())
        .map(|alias| alias["provider"].as_str().expect("a provider is a string").to_string())
        .collect();

    assert_eq!(minted, declared, "the conformance suite must mint an alias in every namespace");
}

// ── Shape guarantees ────────────────────────────────────────────────

/// A release the dump gave neither identifiers nor labels still carries the block, empty —
/// never a missing key, so no consumer has to branch on its absence.
#[test]
fn test_absent_input_produces_the_empty_block() {
    let produced = serde_json::to_value(map_discogs_identifiers(None, None)).expect("the identifiers block serializes");
    assert_eq!(produced, json!({"identifiers_version": "1", "items": [], "types": [], "aliases": [], "unmapped": {"types": []}}));
}

/// A field that is not a list is treated as no field rather than panicking.
#[test]
fn test_non_array_input_produces_the_empty_block() {
    for value in [json!(null), json!("Barcode"), json!({"identifier": []}), json!(7)] {
        let block = map_discogs_identifiers(Some(&value), Some(&value));
        assert!(block.items.is_empty(), "{value} must yield no items");
        assert!(block.aliases.is_empty(), "{value} must yield no aliases");
    }
}

/// Every field is present with an explicit null or empty list, so two implementations
/// serialize the same bytes.
#[test]
fn test_every_field_is_always_present() {
    let produced = map(&json!({"identifiers": [{"type": "Barcode", "value": "5012394144777"}]}));
    let block = produced.as_object().expect("the block is an object");
    for key in ["identifiers_version", "items", "types", "aliases", "unmapped"] {
        assert!(block.contains_key(key), "the block must carry {key}");
    }
    let item = produced["items"][0].as_object().expect("the item is an object");
    for key in ["type", "value", "description", "source"] {
        assert!(item.contains_key(key), "the item must carry {key}");
    }
    let source = produced["items"][0]["source"].as_object().expect("the source is an object");
    for key in ["provider", "type", "field"] {
        assert!(source.contains_key(key), "the source must carry {key}");
    }
}

// ── Mapping rules ───────────────────────────────────────────────────

/// A raw type the vocabulary does not carry maps to `other` and is additionally recorded, so
/// a new upstream string is measurable rather than invisible.
#[test]
fn test_unmapped_type_falls_to_other_and_is_recorded() {
    let produced = map(&json!({"identifiers": [{"type": "Depósito Legal", "value": "M-12345-1987"}]}));
    assert_eq!(produced["items"][0]["type"], json!("other"));
    assert_eq!(produced["items"][0]["source"]["type"], json!("Depósito Legal"), "the raw string survives on the item");
    assert_eq!(produced["unmapped"]["types"], json!(["Depósito Legal"]));
}

/// A type the vocabulary knows and deliberately routes to `other` is mapped, not
/// unrecognised, so it never inflates the unmapped report.
#[test]
fn test_type_mapped_to_other_is_not_reported_as_unmapped() {
    let produced = map(&json!({"identifiers": [{"type": "ISRC", "value": "GBAYE0601498"}]}));
    assert_eq!(produced["items"][0]["type"], json!("other"));
    assert_eq!(produced["unmapped"]["types"], json!([]));
}

/// Unmapped types are sorted and de-duplicated however often they repeat.
#[test]
fn test_unmapped_types_are_sorted_and_deduplicated() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Zed Code", "value": "1"},
            {"type": "Alpha Code", "value": "2"},
            {"type": "Zed Code", "value": "3"}
        ]
    }));
    assert_eq!(produced["unmapped"]["types"], json!(["Alpha Code", "Zed Code"]));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 3, "each entry is still its own item");
}

/// Items keep source order, with the label-derived catalogue numbers after the
/// identifier-derived entries, even though `types` is sorted.
#[test]
fn test_items_keep_source_order_with_catalogue_numbers_last() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Rights Society", "value": "GEMA"},
            {"type": "Barcode", "value": "5012394144777"}
        ],
        "labels": [{"name": "RCA", "catno": "PB 41447"}]
    }));
    let types: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["type"]).collect();
    assert_eq!(types, vec![&json!("rights_society"), &json!("barcode"), &json!("catalog_number")]);
    assert_eq!(produced["types"], json!(["barcode", "catalog_number", "rights_society"]), "types are sorted and de-duplicated");
    assert_eq!(produced["items"][2]["source"]["field"], json!("labels[].catno"));
    assert_eq!(produced["items"][2]["source"]["type"], json!(null), "no raw type names a catalogue number");
}

/// The absent-catalogue-number marker contributes nothing, in either case.
#[test]
fn test_absent_catalogue_number_marker_is_skipped() {
    for catno in ["none", "NONE", "None", "  none  "] {
        let produced = map(&json!({"labels": [{"name": "Not On Label", "catno": catno}]}));
        assert_eq!(produced["items"], json!([]), "{catno} must not become a catalogue number");
    }
}

/// Each namespace applies its own declared rule, and an empty result mints nothing.
#[test]
fn test_alias_normalization_follows_the_namespace_rule() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Barcode", "value": "5 012394-144777"},
            {"type": "Matrix / Runout", "value": "  PB 41447  A2 utopia  "}
        ],
        "labels": [{"name": "Factory", "catno": "fac  73"}]
    }));
    assert_eq!(
        produced["aliases"],
        json!([
            {"provider": "barcode", "external_id": "5012394144777"},
            {"provider": "catalog_number", "external_id": "FAC 73"},
            {"provider": "matrix", "external_id": "PB 41447 A2 utopia"}
        ]),
        "digits only, upper-cased and collapsed, and collapsed with case preserved"
    );
}

/// A barcode with no digits at all mints nothing rather than an empty alias.
#[test]
fn test_alias_with_no_normalized_value_mints_nothing() {
    let produced = map(&json!({"identifiers": [{"type": "Barcode", "value": "not-a-barcode"}]}));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 1, "the item is still carried");
    assert_eq!(produced["aliases"], json!([]));
}

/// A type that mints nothing is stored in the block only.
#[test]
fn test_non_minting_types_produce_no_alias() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Label Code", "value": "LC 0316"},
            {"type": "ASIN", "value": "B000001234"},
            {"type": "Other", "value": "A-1"}
        ]
    }));
    assert_eq!(produced["aliases"], json!([]));
}

/// The same value written two ways stays two items and becomes one alias.
#[test]
fn test_duplicate_alias_values_collapse_to_one_alias() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Barcode", "value": "0123456789012"},
            {"type": "Barcode", "value": "012-345-678-9012"}
        ]
    }));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 2);
    assert_eq!(produced["aliases"], json!([{"provider": "barcode", "external_id": "0123456789012"}]));
}

/// An entry that never named an identifier is skipped entirely — not even as an unmapped
/// value, which would report a type nobody stated.
#[test]
fn test_malformed_entries_are_skipped_without_being_reported() {
    let produced = map(&json!({
        "identifiers": [
            null,
            ["Barcode"],
            "Barcode",
            7,
            {"type": "Barcode", "value": "   "},
            {"value": "5012394144777"},
            {"type": "   ", "value": "5012394144777"},
            {"type": 7, "value": "5012394144777"}
        ],
        "labels": [null, "Sub Pop", {"name": "Sub Pop", "catno": "   "}, {"name": "Sub Pop"}]
    }));
    assert_eq!(produced["items"], json!([]));
    assert_eq!(produced["unmapped"]["types"], json!([]));
}

/// A blank description is `null`, and a stated one is trimmed and kept.
#[test]
fn test_description_is_trimmed_or_null() {
    let produced = map(&json!({
        "identifiers": [
            {"type": "Matrix / Runout", "value": "A-1", "description": "  A side runout  "},
            {"type": "Matrix / Runout", "value": "B-1", "description": "   "},
            {"type": "Matrix / Runout", "value": "C-1"}
        ]
    }));
    assert_eq!(produced["items"][0]["description"], json!("A side runout"));
    assert_eq!(produced["items"][1]["description"], json!(null));
    assert_eq!(produced["items"][2]["description"], json!(null));
}

/// An upstream string that happens to name a built-in map member must still land in
/// `unmapped` rather than resolving to something the vocabulary never declared.
#[test]
fn test_prototype_like_type_names_are_not_mapped() {
    for name in ["constructor", "toString", "hasOwnProperty", "__proto__"] {
        let produced = map(&json!({"identifiers": [{"type": name, "value": "1"}]}));
        assert_eq!(produced["items"][0]["type"], json!("other"), "{name} must not resolve to an inherited member");
        assert_eq!(produced["unmapped"]["types"], json!([name]));
    }
}

// ── Attachment ──────────────────────────────────────────────────────

/// The block takes the `identifiers` key and reads `labels`, which stays untouched as the
/// provenance record for the catalogue numbers.
#[test]
fn test_attach_replaces_the_raw_list_and_keeps_labels() {
    let mut record = json!({
        "id": "1",
        "identifiers": [{"type": "Barcode", "value": "5012394144777"}],
        "labels": [{"name": "RCA", "catno": "PB 41447"}]
    });
    attach_identifiers_block(&mut record);

    assert_eq!(record["identifiers"]["identifiers_version"], json!("1"));
    assert_eq!(record["identifiers"]["items"][0]["source"]["type"], json!("Barcode"), "the raw type survives in the block");
    assert_eq!(record["labels"], json!([{"name": "RCA", "catno": "PB 41447"}]), "labels are left exactly as they were");
}

/// A record that is not an object is left alone rather than panicking.
#[test]
fn test_attach_ignores_a_non_object_record() {
    let mut record = json!("not a record");
    attach_identifiers_block(&mut record);
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
/// list after it has been unwrapped and de-prefixed.
#[tokio::test]
async fn test_normalizer_attaches_the_block_to_releases() {
    let record = json!({
        "id": "1",
        "title": "A Release",
        "identifiers": {"identifier": [{"@type": "Barcode", "@value": "5 012394 144777", "@description": "Scanned"}]},
        "labels": {"label": [{"@name": "RCA", "@catno": "PB 41447", "@id": "895"}]}
    });
    let got = normalize_one(DataType::Releases, record).await;

    assert_eq!(got.data["identifiers"]["items"][0]["type"], json!("barcode"));
    assert_eq!(got.data["identifiers"]["items"][0]["value"], json!("5 012394 144777"), "the value is carried as received");
    assert_eq!(got.data["identifiers"]["items"][0]["description"], json!("Scanned"));
    assert_eq!(got.data["identifiers"]["items"][1]["type"], json!("catalog_number"));
    assert_eq!(
        got.data["identifiers"]["aliases"],
        json!([
            {"provider": "barcode", "external_id": "5012394144777"},
            {"provider": "catalog_number", "external_id": "PB 41447"}
        ])
    );
    assert_eq!(got.data["labels"][0]["catno"], json!("PB 41447"), "the normalized labels survive");
    assert!(!got.sha256.is_empty(), "the normalizer populates the content hash");
}

/// A release the dump gave no identifiers and no labels carries the empty block, not a
/// missing key.
#[tokio::test]
async fn test_normalizer_attaches_the_empty_block_when_nothing_is_stated() {
    let got = normalize_one(DataType::Releases, json!({"id": "1", "title": "A Release"})).await;
    assert_eq!(got.data["identifiers"], json!({"identifiers_version": "1", "items": [], "types": [], "aliases": [], "unmapped": {"types": []}}));
}

/// Only releases carry a block: the other data types have no identifiers.
#[tokio::test]
async fn test_normalizer_attaches_no_block_to_other_types() {
    for data_type in [DataType::Artists, DataType::Labels, DataType::Masters] {
        let got = normalize_one(data_type, json!({"id": "1", "name": "Aphex Twin"})).await;
        assert!(got.data.get("identifiers").is_none(), "{data_type:?} must not carry an identifiers block");
    }
}

/// The block is attached before the hash, so a changed marking changes the content hash
/// consumers key change detection on.
#[tokio::test]
async fn test_hash_covers_the_identifiers_block() {
    let release = |barcode: &str| {
        json!({
            "id": "1",
            "title": "A Release",
            "identifiers": {"identifier": [{"@type": "Barcode", "@value": barcode}]}
        })
    };
    let first = normalize_one(DataType::Releases, release("5012394144777")).await;
    let second = normalize_one(DataType::Releases, release("0075992758123")).await;
    let first_again = normalize_one(DataType::Releases, release("5012394144777")).await;

    assert_ne!(first.sha256, second.sha256, "a different barcode must change the hash");
    assert_eq!(first.sha256, first_again.sha256, "the same record must hash the same");
}

/// Attaching the block is what makes the hash differ: the same record hashed without it would
/// collide with one whose identifiers differ only through the vocabulary.
#[tokio::test]
async fn test_hash_differs_from_the_same_record_without_a_block() {
    let record = json!({
        "id": "1",
        "title": "A Release",
        "identifiers": {"identifier": [{"@type": "Barcode", "@value": "5012394144777"}]}
    });
    let with_block = normalize_one(DataType::Releases, record).await;

    let mut without_block = with_block.data.clone();
    without_block.as_object_mut().expect("the record is an object").remove("identifiers");
    assert_ne!(with_block.sha256, crate::types::calculate_content_hash(&without_block), "the hash must cover the identifiers block");
}
