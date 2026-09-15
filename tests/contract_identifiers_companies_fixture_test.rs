//! Guards the Discogs `releases` contract fixture's `identifiers` and `companies` blocks
//! against the mappers (ADR 0011).
//!
//! `contracts/catalog-events/definitions/discogs.json` carries a hand-authored payload and
//! the two blocks it is expected to produce; `just contract` copies them, verbatim, into
//! `contracts/catalog-events/v1/fixtures/discogs-releases.data.json`.
//!
//! Unlike the media block, which sits beside the raw `formats` list it derives from, each of
//! these blocks takes the key its raw list held, so the fixture carries no separate raw input
//! to replay. It does not need one: every raw value survives inside the block, so the raw
//! list is rebuilt from the items' own provenance fields and fed back through the mapper.
//! Producing the same block from its own provenance is a round trip that fails the moment a
//! type routes differently, an alias normalises differently, a list sorts differently, or an
//! unmapped value stops being recorded.

use extractor::discogs::companies::map_discogs_companies;
use extractor::discogs::identifiers::map_discogs_identifiers;
use serde_json::{Value, json};

const RELEASES_FIXTURE: &str = include_str!("../contracts/catalog-events/v1/fixtures/discogs-releases.data.json");

fn fixture() -> Value {
    serde_json::from_str(RELEASES_FIXTURE).expect("the releases fixture is valid JSON")
}

/// Rebuild the raw `identifiers` list from the block's own items: the identifier-derived ones
/// carry the raw type under `source.type` and the value as received. The label-derived
/// catalogue numbers are not rebuilt — they come from the fixture's own `labels` list, which
/// is published beside the block.
fn raw_identifiers(block: &Value) -> Value {
    let items = block["items"].as_array().expect("the block carries items");
    let rebuilt: Vec<Value> = items
        .iter()
        .filter(|item| item["source"]["field"] == json!("identifiers"))
        .map(|item| json!({"type": item["source"]["type"], "value": item["value"], "description": item["description"]}))
        .collect();
    Value::Array(rebuilt)
}

/// Rebuild the raw `companies` list from the block's own items: the raw role survives
/// verbatim as `role` and the numeric relationship code under `source.entity_type`.
fn raw_companies(block: &Value) -> Value {
    let items = block["items"].as_array().expect("the block carries items");
    let rebuilt: Vec<Value> = items
        .iter()
        .map(|item| {
            json!({
                "name": item["name"],
                "id": item["discogs_id"],
                "catno": item["catno"],
                "entity_type": item["source"]["entity_type"],
                "entity_type_name": item["role"],
            })
        })
        .collect();
    Value::Array(rebuilt)
}

#[test]
fn test_releases_fixture_identifiers_match_the_mapper() {
    let fixture = fixture();
    let expected = fixture.get("identifiers").expect("the releases fixture carries the expected identifiers block");
    let labels = fixture.get("labels").expect("the releases fixture carries a labels payload");

    let produced =
        serde_json::to_value(map_discogs_identifiers(Some(&raw_identifiers(expected)), Some(labels))).expect("the identifiers block serializes");

    assert_eq!(
        &produced,
        expected,
        "the fixture's identifiers block no longer matches src/discogs/identifiers.rs -- regenerate it with \
         `just contract` after updating definitions/discogs.json\n  produced: {}\n  expected: {}",
        serde_json::to_string_pretty(&produced).unwrap_or_default(),
        serde_json::to_string_pretty(expected).unwrap_or_default()
    );
}

#[test]
fn test_releases_fixture_companies_match_the_mapper() {
    let fixture = fixture();
    let expected = fixture.get("companies").expect("the releases fixture carries the expected companies block");

    let produced = serde_json::to_value(map_discogs_companies(Some(&raw_companies(expected)))).expect("the companies block serializes");

    assert_eq!(
        &produced,
        expected,
        "the fixture's companies block no longer matches src/discogs/companies.rs -- regenerate it with \
         `just contract` after updating definitions/discogs.json\n  produced: {}\n  expected: {}",
        serde_json::to_string_pretty(&produced).unwrap_or_default(),
        serde_json::to_string_pretty(expected).unwrap_or_default()
    );
}

/// The fixture must document a block that actually says something: an empty one would let
/// the round trip above pass while proving nothing about the vocabularies.
#[test]
fn test_releases_fixture_documents_a_populated_shape() {
    let fixture = fixture();
    assert!(fixture["identifiers"]["items"].as_array().is_some_and(|items| items.len() >= 2), "the fixture must document more than one identifier");
    assert!(
        fixture["identifiers"]["aliases"].as_array().is_some_and(|aliases| !aliases.is_empty()),
        "the fixture must document the aliases a release mints"
    );
    assert!(
        fixture["identifiers"]["types"].as_array().is_some_and(|types| types.contains(&json!("catalog_number"))),
        "the fixture must document the catalogue number lifted from the label entries"
    );
    assert!(fixture["companies"]["items"].as_array().is_some_and(|items| items.len() >= 2), "the fixture must document more than one company credit");
}
