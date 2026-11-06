//! Measures the extractor's real publisher against a real RabbitMQ.
//!
//! Usage: publish_rate <amqp-url> <messages> <batch-size> <queues> <deadline-secs> [files]
//!
//! With `files` > 1 the messages are split across that many concurrent `MessageQueue`s that
//! share one pacer, like the extractor's files do.
//!
//! Declares `<queues>` durable quorum queues bound to the fanout exchange, publishes
//! `<messages>` real release events through `MessageQueue::publish_batch`, then prints the rate.
use extractor::message_queue::{MessagePublisher, MessageQueue};
use extractor::types::{DataMessage, DataType};
use lapin::{
    Connection, ConnectionProperties,
    options::*,
    types::{AMQPValue, FieldTable},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (url, total, batch, queues, deadline) =
        (a[1].clone(), a[2].parse::<usize>()?, a[3].parse::<usize>()?, a[4].parse::<usize>()?, a[5].parse::<u64>()?);
    let prefix = format!("bench{}", std::process::id());

    let line = std::fs::read_to_string("contracts/extractor-smoke/v1/expected-events.ndjson")?;
    let template: serde_json::Value = serde_json::from_str(line.lines().next().unwrap())?;

    let mq = MessageQueue::new(&url, 3, &prefix).await?;
    mq.setup_exchange(DataType::Releases).await?;

    let conn = Connection::connect(&url, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    let exchange = format!("{prefix}-releases");
    for q in 0..queues {
        let name = format!("{prefix}-q{q}");
        let mut args = FieldTable::default();
        args.insert("x-queue-type".into(), AMQPValue::LongString("quorum".into()));
        ch.queue_declare(name.as_str().into(), QueueDeclareOptions { durable: true, ..Default::default() }, args).await?;
        ch.queue_bind(name.as_str().into(), exchange.as_str().into(), "".into(), QueueBindOptions::default(), FieldTable::default())
            .await?;
    }

    let files: usize = a.get(6).map(|v| v.parse()).transpose()?.unwrap_or(1);
    let pacer = extractor::message_queue::pacer_from_env();
    let mut queues_mq = vec![];
    for _ in 0..files {
        let q = MessageQueue::new_with_pacer(&url, 3, &prefix, pacer.clone()).await?;
        queues_mq.push(Arc::new(q));
    }
    let started = Instant::now();
    let run = async {
        let per_file = total / files;
        let mut tasks = vec![];
        for (f, q) in queues_mq.iter().cloned().enumerate() {
            let template = template.clone();
            tasks.push(tokio::spawn(async move {
                let mut sent = 0usize;
                while sent < per_file {
                    let n = batch.min(per_file - sent);
                    let msgs: Vec<DataMessage> = (0..n)
                        .map(|i| {
                            let id = format!("{f}-{}", sent + i);
                            let mut data = template.clone();
                            data["id"] = id.clone().into();
                            DataMessage { id, sha256: "0".repeat(64), data, raw_xml: None }
                        })
                        .collect();
                    q.publish_batch(msgs, DataType::Releases).await?;
                    sent += n;
                }
                anyhow::Ok(sent)
            }));
        }
        let mut sent = 0;
        for t in tasks {
            sent += t.await??;
        }
        anyhow::Ok(sent)
    };
    let sent = tokio::time::timeout(Duration::from_secs(deadline), run)
        .await
        .map_err(|_| anyhow::anyhow!("DEADLINE EXCEEDED: treated as failure"))??;
    let secs = started.elapsed().as_secs_f64();
    println!("RESULT messages={sent} files={files} batch={batch} queues={queues} secs={secs:.2} rate={:.0}/s", sent as f64 / secs);

    for q in 0..queues {
        let name = format!("{prefix}-q{q}");
        let st = ch
            .queue_declare(name.as_str().into(), QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default())
            .await?;
        println!("queue {name} depth={}", st.message_count());
        ch.queue_delete(name.as_str().into(), QueueDeleteOptions::default()).await?;
    }
    ch.exchange_delete(exchange.as_str().into(), ExchangeDeleteOptions::default()).await?;
    mq.close().await?;
    Ok(())
}
