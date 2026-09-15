//! Canonical companies block (ADR 0011).
//!
//! A Discogs release carries a `companies` list naming who made the physical article: the
//! pressing plant, the room that cut the lacquer, the mastering house, the distributor, the
//! rights holders. Each entry carries a name, a Discogs label id, an optional catalogue
//! number, a numeric `entity_type`, and the `entity_type_name` string that names the
//! relationship. That is the manufacturing-chain evidence two of the collector lenses are
//! built on, and it rode uncleaned in the raw record.
//!
//! This module maps that list onto the closed role vocabulary vendored at
//! `contracts/catalog-events/vocab/company-roles.json` and produces the canonical `companies`
//! block the producer attaches to every `releases` event, once, at the normalization
//! boundary, so the content hash covers it.
//!
//! The block replaces the normalized raw list at the `companies` key rather than sitting
//! beside it under a second name: ADR 0011 names the event field `companies`, and the
//! persistence contract indexes `data->'companies'` as the block. Nothing is lost — the raw
//! `entity_type_name` survives verbatim as `items[].role` and the numeric `entity_type` under
//! `items[].source`, which is the provenance record the decision asks for.
//!
//! The issuing label is deliberately not here. It is a label relation, not a company credit,
//! and the vocabulary records `labels` as the excluded field; a release with labels and no
//! companies carries the empty block.
//!
//! Behaviour is fixed by the conformance fixtures in `tests/fixtures/companies/`, which the
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
const COMPANY_ROLES_JSON: &str = include_str!("../../contracts/catalog-events/vocab/company-roles.json");

// ── The vendored vocabulary ─────────────────────────────────────────

/// The Discogs-specific section: the raw `entity_type_name` strings each category maps from.
/// The document's `company_field` and `excluded_field` state where the list comes from and
/// that the issuing label is not one; neither routes anything, so neither is read here.
#[derive(Debug, Deserialize)]
struct DiscogsVocabulary {
    roles: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct VocabularyDocument {
    vocabulary_version: String,
    unmapped_category: String,
    discogs: DiscogsVocabulary,
}

/// The vocabulary indexed for lookup, parsed once per process.
#[derive(Debug)]
struct Vocabulary {
    vocabulary_version: String,
    unmapped_category: String,
    roles: HashMap<String, String>,
}

impl From<VocabularyDocument> for Vocabulary {
    fn from(document: VocabularyDocument) -> Self {
        Vocabulary { vocabulary_version: document.vocabulary_version, unmapped_category: document.unmapped_category, roles: document.discogs.roles }
    }
}

fn vocabulary() -> &'static Vocabulary {
    static VOCABULARY: OnceLock<Vocabulary> = OnceLock::new();
    VOCABULARY.get_or_init(|| {
        let document: VocabularyDocument =
            serde_json::from_str(COMPANY_ROLES_JSON).expect("the vendored company-role vocabulary is valid JSON in the expected shape");
        Vocabulary::from(document)
    })
}

// ── The canonical block ─────────────────────────────────────────────

/// The provider fields exactly as received, kept as the provenance record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompanySource {
    pub provider: String,
    pub entity_type: Option<String>,
}

/// One entry per source company, in source order.
///
/// `role` is the raw Discogs string, preserved verbatim; `role_category` is the vocabulary's
/// verdict on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompanyItem {
    pub name: String,
    pub discogs_id: Option<u64>,
    pub role: String,
    pub role_category: String,
    pub catno: Option<String>,
    pub source: CompanySource,
}

/// Raw role strings the vocabulary did not recognise. Sorted and de-duplicated, never
/// dropped, so coverage is measurable from the published events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CompanyUnmapped {
    pub roles: Vec<String>,
}

/// The canonical `companies` block attached to every `releases` event.
///
/// Every field is always present: `null` or an empty list when unknown. `role_categories` and
/// `unmapped.roles` are sorted and de-duplicated, so two implementations serialise
/// byte-identical output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompaniesBlock {
    pub companies_version: String,
    pub items: Vec<CompanyItem>,
    pub role_categories: Vec<String>,
    pub unmapped: CompanyUnmapped,
}

// ── Input handling ──────────────────────────────────────────────────

/// The trimmed string a field holds, or `None` when the field is absent, not a string, or
/// blank.
fn trimmed(entry: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    let text = entry.get(key)?.as_str()?.trim();
    if text.is_empty() { None } else { Some(text.to_string()) }
}

/// The Discogs label id of the company, or `None` when it is absent or unusable.
///
/// The dump states it as element text, so it arrives as a string, while the Discogs API and
/// the design fixtures state it as a number; both are read here, and both must be a whole
/// number of at least one — Discogs uses `0` for "no label", which is not an id.
fn discogs_id(entry: &serde_json::Map<String, Value>) -> Option<u64> {
    match entry.get("id")? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
    .filter(|id| *id >= 1)
}

/// The numeric relationship code as received, or `None` when Discogs sent none or sent
/// something that is not a number. The block schema admits only a digit string.
fn entity_type(entry: &serde_json::Map<String, Value>) -> Option<String> {
    let code = match entry.get("entity_type")? {
        Value::String(text) => text.trim().to_string(),
        Value::Number(number) if number.is_u64() || number.is_i64() => number.to_string(),
        _ => return None,
    };
    if !code.is_empty() && code.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(code)
    } else {
        None
    }
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

// ── The mapper ──────────────────────────────────────────────────────

/// Map a normalized Discogs `companies` list onto the canonical block.
///
/// A missing or non-array `companies` (a release the dump gave no company at all) yields the
/// empty block rather than no block, so every release carries the same shape.
///
/// An entry that is not a plain object, or that lacks a name or a role once trimmed, is
/// skipped entirely and contributes nothing — not even an unmapped value, since it never
/// named a credit.
pub fn map_discogs_companies(companies: Option<&Value>) -> CompaniesBlock {
    let vocabulary = vocabulary();
    let mut items: Vec<CompanyItem> = Vec::new();
    let mut unmapped: Vec<String> = Vec::new();

    let entries: &[Value] = companies.and_then(Value::as_array).map_or(&[], Vec::as_slice);
    for entry in entries {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let (Some(name), Some(role)) = (trimmed(entry, "name"), trimmed(entry, "entity_type_name")) else {
            continue;
        };
        let category = vocabulary.roles.get(&role);
        if category.is_none() {
            unmapped.push(role.clone());
        }
        items.push(CompanyItem {
            name,
            discogs_id: discogs_id(entry),
            role,
            role_category: category.cloned().unwrap_or_else(|| vocabulary.unmapped_category.clone()),
            catno: trimmed(entry, "catno"),
            source: CompanySource { provider: "discogs".to_string(), entity_type: entity_type(entry) },
        });
    }

    CompaniesBlock {
        companies_version: vocabulary.vocabulary_version.clone(),
        role_categories: sorted_unique(items.iter().map(|item| item.role_category.clone()).collect()),
        items,
        unmapped: CompanyUnmapped { roles: sorted_unique(unmapped) },
    }
}

/// Attach the canonical `companies` block to a normalized `releases` record, in place.
///
/// The block takes the `companies` key, which the normalized raw list held: ADR 0011 names
/// the event field `companies`, and every raw value survives inside the block. Callers run
/// this after normalization and before the content hash, so the hash covers the block.
pub fn attach_companies_block(record: &mut Value) {
    let Some(map) = record.as_object_mut() else {
        return;
    };
    let block = map_discogs_companies(map.get("companies"));
    let Ok(value) = serde_json::to_value(&block) else {
        return;
    };
    map.insert("companies".to_string(), value);
}

#[cfg(test)]
#[path = "tests/companies_tests.rs"]
mod tests;
