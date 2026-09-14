# Downloader reliability contract

The Discogs downloader owns these observable acquisition guarantees. The focused Rust
contract suite in `src/discogs/tests/downloader_reliability_contract_tests.rs` makes them
mechanical without contacting Discogs or requiring a live dump.

| Invariant | Discogs contract |
| --- | --- |
| Timeout | Connection setup and every idle body-read interval are bounded to 120 seconds. There is deliberately no total deadline for a multi-gigabyte transfer that continues making progress. |
| Retry and backoff | A post-connect download failure receives at most three attempts, with exponential waits of 2 seconds then 4 seconds in production. HTTP 429/503 handling remains inside the polite HTTP client and does not consume these attempts. |
| Partial cleanup | A failed attempt's destination is removed before retry; the final partial destination is removed after the third failure. Failed bytes never enter checksum metadata. |
| Restart semantics | A failed file restarts from byte zero. The client sends no `Range` request. Across process restarts, a file is skipped only when it exists and its SHA-256 matches durable `.discogs_metadata.json`; missing or mismatched metadata forces a fresh download. |
| Integrity | Successful response bytes are SHA-256 hashed. When the monthly Discogs `CHECKSUM` is available, a mismatch fails the run and deletes both the file and its in-memory trust record. Fetching the published checksum is currently best-effort. |
| Terminal error | Exhaustion returns an error retaining the final transport/HTTP cause. The batch-level error chain also names the failed dump file. |

## Comparison with musicbrainz-ingestion

The independently maintained MusicBrainz downloader has the same 120-second connect/read
bounds, three post-connect attempts, 2-second exponential-backoff base, cleanup on failure,
restart-from-zero behavior, SHA-256 verification, and cause-preserving terminal errors.
Those are the invariants a paired-repository verification may compare.

The storage protocols intentionally differ:

- MusicBrainz requires `SHA256SUMS`, streams tar extraction into a sibling `.tmp`, and
  atomically publishes the extracted entity only after verification. It resumes a version by
  skipping each already-published entity.
- Discogs stores the upstream gzip directly at its final filename while streaming. A crash may
  leave that path partial, but it has no trusted metadata, so the next run restarts it. Its
  monthly `CHECKSUM` lookup is best-effort because older or transiently incomplete Discogs
  listings may omit it.

The comparison is behavioral. Repository-local constants and implementation structure are not
a shared source contract and should not be synchronized by copying source hashes.
