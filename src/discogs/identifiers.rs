//! Canonical identifiers block (ADR 0011).
//!
//! A Discogs release carries an `identifiers` list of `{type, value, description}` entries
//! whose type is a raw Discogs string — `Barcode`, `Matrix / Runout`, `Label Code`, and a
//! tail of narrower ones — and a `labels` list whose entries carry the catalogue number the
//! issuing label assigned. Both arrive intact in the raw record because the XML parser is a
//! generic element-to-JSON converter, and nothing downstream could read them: barcode lookup
//! was impossible against a catalog that holds every barcode it was given.
//!
//! This module maps both onto the closed vocabulary vendored at
//! `contracts/catalog-events/vocab/identifier-types.json` and produces the canonical
//! `identifiers` block the producer attaches to every `releases` event, once, at the
//! normalization boundary, so the content hash covers it.
//!
//! The block replaces the normalized raw list at the `identifiers` key rather than sitting
//! beside it under a second name: ADR 0011 names the event field `identifiers`, and the
//! persistence contract indexes `data->'identifiers'` as the block. Nothing is lost —
//! every raw value survives inside the block, the received value under `items[].value` and
//! the raw Discogs type string under `items[].source.type`, which is the provenance record
//! the decision asks for.
//!
//! The vocabulary — never this code — decides routing, so a new upstream type string is a
//! re-vendoring, not a code change. A type the vocabulary does not know maps to `other` and
//! is additionally recorded under `unmapped.types`; a type it knows and deliberately routes
//! to `other` (`ISRC`, `Price Code`, the SID codes) is not, because it is mapped rather than
//! unrecognised.
//!
//! Behaviour is fixed by the conformance fixtures in `tests/fixtures/identifiers/`, which the
//! design repository's reference mapper (`scripts/validation-policy.mjs`) also has to
//! satisfy. The two implementations must agree exactly; change this file only alongside those
//! fixtures.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The vendored vocabulary, compiled in. `contracts/generate.py --check` fails the build gate
/// when these bytes drift from the digest recorded beside them, so parsing it is infallible
/// in practice.
const IDENTIFIER_TYPES_JSON: &str = include_str!("../../contracts/catalog-events/vocab/identifier-types.json");

/// The one canonical type with no raw Discogs string behind it: it is lifted from the
/// catalogue number a label entry carries, and the vocabulary's `discogs.types` mapping is
/// forbidden from routing anything to it.
const CATALOG_NUMBER_TYPE: &str = "catalog_number";

/// The field a catalogue-number item records as its origin. The vocabulary states it as
/// `discogs.catalog_number_field`; this constant is only the fallback when a future
/// vocabulary omits it.
const CATALOG_NUMBER_FIELD: &str = "labels[].catno";

/// The field a raw identifier item records as its origin.
const IDENTIFIER_FIELD: &str = "identifiers";

// ── The vendored vocabulary ─────────────────────────────────────────

/// A canonical identifier type. `alias_provider` names the ADR 0009 namespace the type mints
/// into, or `null` when the type is stored in the block only.
#[derive(Debug, Deserialize)]
struct IdentifierTypeDefinition {
    id: String,
    #[serde(default)]
    alias_provider: Option<String>,
}

/// An alias namespace: which canonical type mints into it, and the normalization applied to
/// a value before it becomes an `external_id`.
#[derive(Debug, Deserialize)]
struct AliasNamespace {
    provider: String,
    #[serde(rename = "type")]
    type_id: String,
    normalization: String,
}

/// The Discogs-specific section: which fields the values come from, the absent-catalogue-number
/// marker, and the raw type strings each canonical type maps from.
#[derive(Debug, Deserialize)]
struct DiscogsVocabulary {
    #[serde(default)]
    identifier_field: Option<String>,
    #[serde(default)]
    catalog_number_field: Option<String>,
    #[serde(default)]
    absent_catalog_number: Option<String>,
    types: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct VocabularyDocument {
    vocabulary_version: String,
    unmapped_type: String,
    identifier_types: Vec<IdentifierTypeDefinition>,
    alias_namespaces: Vec<AliasNamespace>,
    discogs: DiscogsVocabulary,
}

/// The vocabulary indexed for lookup, parsed once per process.
#[derive(Debug)]
struct Vocabulary {
    vocabulary_version: String,
    unmapped_type: String,
    /// Canonical type id -> the alias namespace it mints into, for the three types that do.
    namespaces: HashMap<String, AliasNamespace>,
    identifier_field: String,
    catalog_number_field: String,
    absent_catalog_number: String,
    types: HashMap<String, String>,
}

impl From<VocabularyDocument> for Vocabulary {
    fn from(document: VocabularyDocument) -> Self {
        // A namespace counts only when the type that declares it agrees, so a vocabulary can
        // never mint into a namespace its own type list does not point at.
        let declared: HashMap<&str, Option<&str>> =
            document.identifier_types.iter().map(|entry| (entry.id.as_str(), entry.alias_provider.as_deref())).collect();
        let namespaces: HashMap<String, AliasNamespace> = document
            .alias_namespaces
            .into_iter()
            .filter(|namespace| declared.get(namespace.type_id.as_str()) == Some(&Some(namespace.provider.as_str())))
            .map(|namespace| (namespace.type_id.clone(), namespace))
            .collect();
        Vocabulary {
            vocabulary_version: document.vocabulary_version,
            unmapped_type: document.unmapped_type,
            namespaces,
            identifier_field: document.discogs.identifier_field.unwrap_or_else(|| IDENTIFIER_FIELD.to_string()),
            catalog_number_field: document.discogs.catalog_number_field.unwrap_or_else(|| CATALOG_NUMBER_FIELD.to_string()),
            absent_catalog_number: document.discogs.absent_catalog_number.unwrap_or_default().to_lowercase(),
            types: document.discogs.types,
        }
    }
}

fn vocabulary() -> &'static Vocabulary {
    static VOCABULARY: OnceLock<Vocabulary> = OnceLock::new();
    VOCABULARY.get_or_init(|| {
        let document: VocabularyDocument =
            serde_json::from_str(IDENTIFIER_TYPES_JSON).expect("the vendored identifier vocabulary is valid JSON in the expected shape");
        Vocabulary::from(document)
    })
}

// ── The canonical block ─────────────────────────────────────────────

/// Where one item came from: the provider, the raw Discogs type string (`null` for a
/// catalogue number, which no raw type names), and the field it was lifted from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentifierSource {
    pub provider: String,
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    pub field: String,
}

/// One entry per source identifier, in source order, with the label-derived catalogue
/// numbers after the identifier-derived entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentifierItem {
    #[serde(rename = "type")]
    pub type_id: String,
    pub value: String,
    pub description: Option<String>,
    pub source: IdentifierSource,
}

/// One `provider_aliases` row this release mints under ADR 0009, already normalised by its
/// namespace's declared rule.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct IdentifierAlias {
    pub provider: String,
    pub external_id: String,
}

/// Raw type strings the vocabulary did not recognise. Sorted and de-duplicated, never
/// dropped, so coverage is measurable from the published events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct IdentifierUnmapped {
    pub types: Vec<String>,
}

/// The canonical `identifiers` block attached to every `releases` event.
///
/// Every field is always present: an empty list when there is nothing to say. `types`,
/// `aliases`, and `unmapped.types` are sorted and de-duplicated, so two implementations
/// serialise byte-identical output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentifiersBlock {
    pub identifiers_version: String,
    pub items: Vec<IdentifierItem>,
    pub types: Vec<String>,
    pub aliases: Vec<IdentifierAlias>,
    pub unmapped: IdentifierUnmapped,
}

// ── Input handling ──────────────────────────────────────────────────

/// The trimmed string a field holds, or `None` when the field is absent, not a string, or
/// blank. Only a string counts: the dump states every one of these as an XML attribute or an
/// element's text, so a number where a string belongs is a malformed entry, not a value.
fn trimmed(entry: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    let text = entry.get(key)?.as_str()?.trim();
    if text.is_empty() { None } else { Some(text.to_string()) }
}

/// The entries of a list field, or nothing at all when the field is absent or is not a list.
fn entries(value: Option<&Value>) -> &[Value] {
    value.and_then(Value::as_array).map_or(&[], Vec::as_slice)
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

/// Apply an alias namespace's declared normalization.
///
/// The three rules are the vocabulary's closed set. A rule this build does not know mints
/// nothing rather than guessing: an `external_id` computed under the wrong rule is a wrong
/// identity, and the vocabulary declares a new rule only in a new version directory.
fn normalize_alias_value(normalization: &str, value: &str) -> Option<String> {
    let normalized = match normalization {
        "digits_only" => value.chars().filter(char::is_ascii_digit).collect(),
        "collapse_space" => collapse_space(value),
        "upper_collapse_space" => collapse_space(value).to_uppercase(),
        _ => return None,
    };
    if normalized.is_empty() { None } else { Some(normalized) }
}

/// Trim the value and collapse every run of whitespace to one space, preserving case.
fn collapse_space(value: &str) -> String {
    value.split_whitespace().collect::<Vec<&str>>().join(" ")
}

// ── The mapper ──────────────────────────────────────────────────────

/// Map a normalized Discogs `identifiers` list and `labels` list onto the canonical block.
///
/// A missing or non-array field (a release the dump gave no identifier and no label at all)
/// yields the empty block rather than no block, so every release carries the same shape.
///
/// An entry that is not a plain object, or that lacks a type or a value once trimmed, is
/// skipped entirely and contributes nothing — not even an unmapped value, since it never
/// named an identifier.
pub fn map_discogs_identifiers(identifiers: Option<&Value>, labels: Option<&Value>) -> IdentifiersBlock {
    let vocabulary = vocabulary();
    let mut items: Vec<IdentifierItem> = Vec::new();
    let mut unmapped: Vec<String> = Vec::new();

    for entry in entries(identifiers) {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let (Some(raw_type), Some(value)) = (trimmed(entry, "type"), trimmed(entry, "value")) else {
            continue;
        };
        let mapped = vocabulary.types.get(&raw_type);
        if mapped.is_none() {
            unmapped.push(raw_type.clone());
        }
        items.push(IdentifierItem {
            type_id: mapped.cloned().unwrap_or_else(|| vocabulary.unmapped_type.clone()),
            value,
            description: trimmed(entry, "description"),
            source: IdentifierSource { provider: "discogs".to_string(), type_name: Some(raw_type), field: vocabulary.identifier_field.clone() },
        });
    }

    // The issuing label's catalogue number is the one canonical type no raw identifier string
    // names, so that every namespace minting an alias has an entry in one list.
    for entry in entries(labels) {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let Some(catalog_number) = trimmed(entry, "catno") else {
            continue;
        };
        if catalog_number.to_lowercase() == vocabulary.absent_catalog_number {
            continue;
        }
        items.push(IdentifierItem {
            type_id: CATALOG_NUMBER_TYPE.to_string(),
            value: catalog_number,
            description: None,
            source: IdentifierSource { provider: "discogs".to_string(), type_name: None, field: vocabulary.catalog_number_field.clone() },
        });
    }

    let mut aliases: Vec<IdentifierAlias> = items
        .iter()
        .filter_map(|item| {
            let namespace = vocabulary.namespaces.get(&item.type_id)?;
            let external_id = normalize_alias_value(&namespace.normalization, &item.value)?;
            Some(IdentifierAlias { provider: namespace.provider.clone(), external_id })
        })
        .collect();
    aliases.sort();
    aliases.dedup();

    IdentifiersBlock {
        identifiers_version: vocabulary.vocabulary_version.clone(),
        types: sorted_unique(items.iter().map(|item| item.type_id.clone()).collect()),
        items,
        aliases,
        unmapped: IdentifierUnmapped { types: sorted_unique(unmapped) },
    }
}

/// Attach the canonical `identifiers` block to a normalized `releases` record, in place.
///
/// The block takes the `identifiers` key, which the normalized raw list held: ADR 0011 names
/// the event field `identifiers`, and every raw value survives inside the block. `labels` is
/// left untouched — it is read here and stays the provenance record for the catalogue
/// numbers. Callers run this after normalization and before the content hash, so the hash
/// covers the block.
pub fn attach_identifiers_block(record: &mut Value) {
    let Some(map) = record.as_object_mut() else {
        return;
    };
    let block = map_discogs_identifiers(map.get("identifiers"), map.get("labels"));
    let Ok(value) = serde_json::to_value(&block) else {
        return;
    };
    map.insert("identifiers".to_string(), value);
}

#[cfg(test)]
#[path = "tests/identifiers_tests.rs"]
mod tests;
