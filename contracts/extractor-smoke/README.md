# Discogs extractor smoke contract

`v1/manifest.json` is the extractor-owned, released-image smoke-test contract.
It pairs one deterministic, gzip-compressed Discogs-shaped input with the
RabbitMQ event stream the normal extraction pipeline must publish.
The v1 stream covers the file-level `data` and `file_complete` events emitted
by `process_single_file`; run-level `extraction_complete` is outside its scope.

The record is synthetic and was authored for this repository. It contains no
Discogs dump data, user data, or credentials and is distributed under the
repository's MIT license. Consumers should read the fixture directly from a
reviewed release or immutable repository commit rather than copying production
or private data into their own repositories.

## Comparing an event stream

Parse each line of `v1/expected-events.ndjson` as JSON and preserve line order.
Apply the manifest's `semantic_normalizer` to the observed stream before
comparing JSON values:

1. For an event whose `type` is `file_complete`, replace `timestamp` with
   `2000-01-01T00:00:00Z`.
2. Do not remove, rename, sort, or otherwise rewrite any field or event.

The timestamp is the only nondeterministic field in this single-file contract.
JSON object member order is not significant; event order and array order are.
The expected events otherwise pin the producer's normalized payload, content
hash, completion signal, and catalog-event schema version.
