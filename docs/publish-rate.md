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

`publish_batch` now writes the whole batch, then settles the confirms in order. Unchanged:
persistent delivery, `mandatory` routing, the 30 s confirm timeout, and the rule that the
batch returns Ok only when every message was acked and routable (an unroutable batch still
fails with `312 NO_ROUTE`, verified on the broker). One difference: on a failure part-way
through a batch, later messages of that batch may already have been sent. The file stays
pending and is re-published, so this is covered by the existing at-least-once behaviour and
the consumers' idempotent handling by `id` and `sha256`.
