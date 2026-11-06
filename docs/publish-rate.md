# Publish rate: what limited the extractor

Bead gm-discogs-ingestion-a4w. During the 2026-11-06 fresh ingest the extractor published
about 520 records a second at 6 to 8 percent CPU while consumers kept up and RabbitMQ showed
no alarm or flow control.

## Cause

`MessageQueue::publish_batch` awaited the publisher confirm of every message before sending
the next. Each message therefore cost one full broker round trip, including the quorum-queue
fsync on every bound queue. The extractor was idle between messages, which is why CPU was low
and the rate tracked the number of bound queues rather than the parser.

## Measurements

Disposable `rabbitmq:4-management@sha256:c200fd49...` (the deployment pin), the real
`MessageQueue` publisher, real release events from `contracts/extractor-smoke`, durable quorum
queues bound to the fanout exchange, batch size 100. Reproduce with
`cargo run --release --example publish_rate -- amqp://guest:guest@127.0.0.1:5672 100000 100 2 120`.

| Bound queues | Before (confirm per message) | After (confirms pipelined per batch) |
| --- | --- | --- |
| 1 | 1,095/s | 18,397/s |
| 2 | 641 to 793/s | 16,088/s |

The broker used 0.9 percent CPU during the "before" runs, confirming the wait, not broker
capacity, was the limit. Every queue held every published message after each "after" run.

## Change and delivery guarantees

`publish_batch` now writes the whole batch, then settles the confirms in order.

Same as before: persistent delivery, `mandatory` routing, and the rule that the batch returns
Ok only when every message was acked and routable (an unroutable batch still fails with
`312 NO_ROUTE`, verified on the broker).

Changed:

- **The 30 s deadline now covers the send as well as the confirm.** Pipelining moved the
  blocking point. Under a memory alarm RabbitMQ blocks the connection and stops reading, so
  `basic_publish` itself parks, and the confirm timeout never started. `send_payload` is now
  wrapped in `tokio::time::timeout(PUBLISH_CONFIRM_TIMEOUT, ...)`. Measured on a real broker
  with `vm_memory_high_watermark` set to 1 MB: the pipelined commit 57ec207 parked past 60 s
  (the test's outer deadline); this change returns `Err` at about 30 s, like `main`. The test is
  `publish_batch_errors_under_memory_alarm` (ignored by default; it needs a disposable broker,
  see its doc comment).
- **`groovemap.extraction.publish.confirm.duration` now records send time plus confirm time**
  per message, so a send stalled by an alarm shows up in the histogram instead of leaving it
  flat. A send that times out records its 30 s wait.
- **On a failure part-way through a batch, later messages of that batch may already have been
  sent.** The file stays pending and is re-published, so this is covered by the existing
  at-least-once behaviour and the consumers' idempotent handling by `id` and `sha256`.

## Pacing

Consumers write about 530 to 800 records a second, so publishing at 15,000 a second only moves
records into RabbitMQ queues. `publish_batch` takes a token from a shared bucket before each
send. `DefaultMessageQueueFactory` owns one bucket and gives the same `Arc<Mutex<TokenBucket>>` to every
`MessageQueue` it creates, so the ceiling is a total across all files, not per connection.

| Setting | Behaviour |
| --- | --- |
| `PUBLISH_MAX_RECORDS_PER_SEC` unset | 1500 records/s |
| a positive number | that many records/s (burst of 100 ms of the rate) |
| `0` | pacing off |
| anything else | warning, default of 1500 |

Measured with the same harness (30,000 records, batch 100, 2 queues, real broker):

| `PUBLISH_MAX_RECORDS_PER_SEC` | Rate | Time |
| --- | --- | --- |
| `0` (off) | 14,823/s | 2.02 s |
| unset / `1500` | 1,507/s | 19.90 s |
| `500` | 501/s | 59.90 s |

Every queue held all 30,000 messages in each run.

Four files publishing at once (the extractor's case), each its own `MessageQueue`, 32,000
records, default setting: 1 file 1,507/s, 4 files 1,507/s (21.2 s both). With one bucket per
connection the same four files measured 5,949/s.

## Real-broker tests

`just test-integration` starts a disposable RabbitMQ at the deployment pin (`--user rabbitmq`),
runs the `#[ignore]`d broker tests, including the memory-alarm test, and removes the container
and its volumes. CI runs it through `integration-command`.
