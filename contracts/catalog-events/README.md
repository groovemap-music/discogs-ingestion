# Discogs catalog event contract

This repository owns the Discogs `groovemap.catalog-events/v1` producer contract.
The maintained source is `definitions/discogs.json`; `v1/` contains the generated
manifest, Python binding, JSON fixtures, and shared event schema.

```bash
just contract
just contract-check
```

The contract preserves the `groovemap-discogs` exchange prefix and the artists, labels,
masters, and releases vocabulary. Consumer repositories promote artifacts from a
reviewed immutable commit rather than editing generated output.

## The `media` field

Every `releases` event carries a `media` block alongside the raw, provider-shaped
`formats` list it was derived from. `media` maps that list onto the provider-neutral
vocabulary vendored in `vocab/media-taxonomy.json` -- one canonical family, medium,
and set of attributes per physical or digital unit the release ships on -- so every
consumer reads one shape instead of re-deriving it from Discogs' free-text `formats`
independently. See [ADR 0007, "Canonical media taxonomy and media-neutral product
core"](https://github.com/groovemap-music/design/blob/main/docs/adr/0007-canonical-media-taxonomy.md)
for the rationale, and `src/discogs/media.rs` for the mapper that produces it.

`media` is **additive within v1**: existing fields and the event schema are unchanged,
and the field is optional for a consumer to read. `fixture_payloads.releases` in
`definitions/discogs.json` carries a representative `formats` payload and the exact
`media` block the mapper produces for it, so `just contract` regenerates a fixture that
documents the shape and `just contract-check` guards it from drifting out of date.

## Vendored vocabularies

`vocab/` holds the vocabularies this producer maps against. None of them is generated.
Each is owned by the `design` repository and vendored here verbatim, byte for byte:

| Vendored file | Design path | Record |
| --- | --- | --- |
| `vocab/media-taxonomy.json` | `taxonomy/media/v1/media-taxonomy.json` | ADR 0007 |
| `vocab/identifier-types.json` | `taxonomy/identifiers/v1/identifier-types.json` | ADR 0011 |
| `vocab/company-roles.json` | `taxonomy/company-roles/v1/company-roles.json` | ADR 0011 |

`vocab/source.json` beside them records, for each copy, the design commit and the
SHA-256 digest it was vendored from:

```json
{
  "vocabularies": [
    {
      "file": "<vendored file name>",
      "source": "design",
      "path": "<design-repo-relative path>",
      "commit": "<40-char design repo commit sha>",
      "sha256": "<sha256 of the vendored file>"
    }
  ]
}
```

`just contract-check` fails if a recorded vocabulary is missing, if its digest no longer
matches its record, or if `vocab/` holds a vocabulary file no record names -- an
unrecorded copy would otherwise be compiled into a mapper unverified. `just contract`
never writes or rewrites either the vocabularies or `source.json`; all of them are edited
only by hand, as part of a deliberate re-vendor.

### Updating a vendored copy

1. In the design repository, pick the reviewed commit that should become the new source
   and confirm the vocabulary file's digest:

   ```bash
   git -C <path-to-design-repo> rev-parse <commit>
   shasum -a 256 <path-to-design-repo>/taxonomy/media/v1/media-taxonomy.json
   ```

2. Copy the file byte for byte into this repository:

   ```bash
   cp <path-to-design-repo>/taxonomy/media/v1/media-taxonomy.json \
      contracts/catalog-events/vocab/media-taxonomy.json
   ```

3. Update that file's entry in `vocab/source.json` with the new commit sha (full 40
   characters) and digest.
4. Run `just check` (or at least `just contract-check`) to confirm the vendored copy and
   the source record agree.

Never write an absolute host path into `source.json` or any tracked file -- only the
commit sha, the design-repo-relative path, and the digest.
