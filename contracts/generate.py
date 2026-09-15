"""Generate the Discogs-owned catalog event contract and language bindings."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EVENTS_ROOT = Path(__file__).resolve().parent / "catalog-events"
DEFINITION_PATH = EVENTS_ROOT / "definitions" / "discogs.json"
CONTRACT_ROOT = EVENTS_ROOT / "v1"
SCHEMA_PATH = CONTRACT_ROOT / "schemas" / "event.schema.json"
SOURCE = "discogs"

# The vocabularies are vendored verbatim from the design repository (see
# contracts/catalog-events/README.md); they are never generated or rewritten here.
VOCAB_ROOT = EVENTS_ROOT / "vocab"
VENDORED_VOCAB_SOURCE_PATH = VOCAB_ROOT / "source.json"


def json_text(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def source_manifest(definition: dict[str, Any]) -> dict[str, Any]:
    source = definition["source"]
    schema_digest = sha256(SCHEMA_PATH.read_bytes()).hexdigest()
    prefix = source["default_exchange_prefix"]
    entities = source["entities"]
    consumers = definition["consumers"]
    exchanges = {entity: f"{prefix}-{entity}" for entity in entities}
    queues = {
        consumer: {
            entity: {
                "name": f"{prefix}-{consumer}-{entity}",
                "dead_letter_exchange": f"{prefix}-{consumer}-{entity}.dlx",
                "dead_letter_queue": f"{prefix}-{consumer}-{entity}.dlq",
            }
            for entity in entities
        }
        for consumer in consumers
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "contract": "groovemap.catalog-events",
        "version": 1,
        "event_schema": "schemas/event.schema.json",
        "event_schema_sha256": schema_digest,
        "exchange": {"kind": "fanout", "name_template": "{exchange_prefix}-{entity}"},
        "queue": {
            "name_template": "{exchange_prefix}-{consumer}-{entity}",
            "dead_letter_exchange_template": "{queue}.dlx",
            "dead_letter_queue_template": "{queue}.dlq",
        },
        "sources": {SOURCE: {key: value for key, value in source.items() if key != "name"}},
        "consumers": {consumer: {"source": SOURCE} for consumer in consumers},
        "fixture_payloads": {SOURCE: definition["fixture_payloads"]},
        "runtime_identifiers": {"source": SOURCE, "exchanges": exchanges, "queues": queues},
    }


def render_rust(manifest: dict[str, Any]) -> str:
    source = manifest["sources"][SOURCE]
    entities = ", ".join(json.dumps(item) for item in source["entities"])
    consumers = ", ".join(json.dumps(item) for item in sorted(manifest["consumers"]))
    return f"""// Generated from contracts/catalog-events/definitions/discogs.json; do not edit.

pub const CONTRACT_NAME: &str = {json.dumps(manifest["contract"])};
pub const CONTRACT_VERSION: u32 = {manifest["version"]};
pub const SOURCE: &str = {json.dumps(SOURCE)};
pub const AMQP_EXCHANGE_TYPE: &str = {json.dumps(manifest["exchange"]["kind"])};
pub const EXCHANGE_PREFIX_ENV: &str = {json.dumps(source["exchange_prefix_env"])};
pub const DEFAULT_EXCHANGE_PREFIX: &str = {json.dumps(source["default_exchange_prefix"])};
pub const ENTITY_TYPES: &[&str] = &[{entities}];
pub const CONSUMERS: &[&str] = &[{consumers}];
"""


def render_python(manifest: dict[str, Any]) -> str:
    source = manifest["sources"][SOURCE]
    return f'''"""Generated from contracts/catalog-events/definitions/discogs.json; do not edit."""

from os import getenv

CONTRACT_NAME = {json.dumps(manifest["contract"])}
CONTRACT_VERSION = {manifest["version"]}
SOURCE = {json.dumps(SOURCE)}
AMQP_EXCHANGE_TYPE = {json.dumps(manifest["exchange"]["kind"])}
ENTITY_TYPES = {json.dumps(source["entities"])}
CONSUMERS = {json.dumps(sorted(manifest["consumers"]))}
EXCHANGE_PREFIX = getenv({json.dumps(source["exchange_prefix_env"])}, {json.dumps(source["default_exchange_prefix"])})


def exchange_name(entity: str) -> str:
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown Discogs entity: {{entity}}")
    return f"{{EXCHANGE_PREFIX}}-{{entity}}"


def queue_name(consumer: str, entity: str) -> str:
    if consumer not in CONSUMERS:
        raise ValueError(f"Unknown Discogs consumer: {{consumer}}")
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown Discogs entity: {{entity}}")
    return f"{{EXCHANGE_PREFIX}}-{{consumer}}-{{entity}}"
'''


def fixture(name: str, payload: dict[str, Any]) -> str:
    return json_text({"type": "data", "id": f"contract-{SOURCE}-{name}", "sha256": "", **payload})


def render_all() -> dict[Path, str]:
    definition = json.loads(DEFINITION_PATH.read_text(encoding="utf-8"))
    if definition["source"]["name"] != SOURCE:
        raise ValueError("definition is not Discogs-owned")
    manifest = source_manifest(definition)
    rendered = {
        CONTRACT_ROOT / "contract.json": json_text(manifest),
        CONTRACT_ROOT / "bindings" / "python" / "catalog_contract.py": render_python(manifest),
        ROOT / "src" / "generated" / "catalog_contract.rs": render_rust(manifest),
        CONTRACT_ROOT / "fixtures" / "file-complete.json": json_text({
            "type": "file_complete", "data_type": "artists", "file": "contract-artists.xml.gz",
            "timestamp": "2000-01-01T00:00:00Z", "total_processed": 1,
        }),
        CONTRACT_ROOT / "fixtures" / "extraction-complete.json": json_text({
            "type": "extraction_complete", "version": "contract-fixture",
            "started_at": "2000-01-01T00:00:00Z", "timestamp": "2000-01-01T00:00:01Z",
            "record_counts": {"artists": 1},
        }),
    }
    for name, payload in definition["fixture_payloads"].items():
        rendered[CONTRACT_ROOT / "fixtures" / f"{SOURCE}-{name}.data.json"] = fixture(name, payload)
    return rendered


def vendored_vocab_errors() -> list[str]:
    """Check every vendored vocabulary against its recorded digest.

    ``vocab/source.json`` lists one record per vendored file -- the media taxonomy
    (ADR 0007) and the identifier-type and company-role vocabularies (ADR 0011).
    Each record names the design commit and the SHA-256 the copy was taken from.

    This never writes anything -- it only reports drift so ``contract-check``
    fails loudly instead of silently accepting a stale, missing, or unrecorded
    vendor copy. A vocabulary file present in ``vocab/`` without a record is drift
    too: it would otherwise be compiled into the mapper unverified.
    """
    errors: list[str] = []
    if not VENDORED_VOCAB_SOURCE_PATH.exists():
        errors.append(f"missing vendor source record: {VENDORED_VOCAB_SOURCE_PATH.relative_to(ROOT)}")
        return errors
    try:
        records = json.loads(VENDORED_VOCAB_SOURCE_PATH.read_text(encoding="utf-8"))["vocabularies"]
        recorded = {str(record["file"]): str(record["sha256"]) for record in records}
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        errors.append(f"invalid vendor source record {VENDORED_VOCAB_SOURCE_PATH.relative_to(ROOT)}: {exc}")
        return errors
    for name, expected_digest in sorted(recorded.items()):
        path = VOCAB_ROOT / name
        if not path.exists():
            errors.append(f"missing vendored vocabulary: {path.relative_to(ROOT)}")
            continue
        actual_digest = sha256(path.read_bytes()).hexdigest()
        if actual_digest != expected_digest:
            errors.append(
                f"vendored vocabulary digest mismatch: {path.relative_to(ROOT)} has sha256 {actual_digest}, "
                f"but {VENDORED_VOCAB_SOURCE_PATH.relative_to(ROOT)} records {expected_digest}"
            )
    for path in sorted(VOCAB_ROOT.glob("*.json")):
        if path != VENDORED_VOCAB_SOURCE_PATH and path.name not in recorded:
            errors.append(
                f"unrecorded vendored vocabulary: {path.relative_to(ROOT)} has no entry in "
                f"{VENDORED_VOCAB_SOURCE_PATH.relative_to(ROOT)}"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        rendered = render_all()
    except (KeyError, TypeError, ValueError) as exc:
        sys.stderr.write(f"invalid Discogs contract: {exc}\n")
        return 1
    expected = {*rendered, SCHEMA_PATH}
    stale: list[Path] = []
    for path, content in rendered.items():
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    actual = {path for path in CONTRACT_ROOT.rglob("*") if path.is_file()}
    stale.extend(sorted(actual - expected))
    if stale:
        sys.stderr.write("stale Discogs contract artifacts:\n")
        sys.stderr.write("".join(f"  {path.relative_to(ROOT)}\n" for path in stale))
        return 1
    vocab_errors = vendored_vocab_errors()
    if vocab_errors:
        sys.stderr.write("vendored vocabularies are out of date:\n")
        sys.stderr.write("".join(f"  {error}\n" for error in vocab_errors))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
