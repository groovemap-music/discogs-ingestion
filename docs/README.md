# Discogs ingestion documentation

- [Extraction architecture](extraction.md) — download, XML parsing, normalization,
  data-quality rules, publication, and independent scheduling.
- [Extraction rules](extraction-rules-guide.md) — skip, filter, validation, and diagnostics.
- [State-marker system](state-marker-system.md) — restart, durability, and checksum provenance.
- [Periodic state-marker checkpoints](state-marker-periodic-updates.md) — recovery guarantees.
- [Downloader reliability contract](downloader-reliability-contract.md) — timeout, retry,
  cleanup, restart, integrity, and paired-repository comparison guarantees.
- [Runtime identity](runtime-identity.md) — repository, image, service, and RabbitMQ names.
- [Publication readiness](publication-readiness.md) — release-history and approval gates.
- [Catalog event contract](../contracts/catalog-events/README.md) — generated artifacts.
- [Extractor smoke contract](../contracts/extractor-smoke/README.md) — versioned synthetic dump and expected RabbitMQ events.
- [Producer normalization decision](decisions/0001-producer-normalization-boundary.md).

The MusicBrainz producer is maintained independently in `groovemap-music/musicbrainz-ingestion`.
