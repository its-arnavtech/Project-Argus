//! Chunk 4 validation: sends a small subset of the real Chunk 1 data through
//! the full IngestionEngine + EventHubSink pipeline to the real
//! "transactions" hub, then reads it back via ConsumerClient to confirm
//! delivery. Deliberately capped (default 3,000 events, override with
//! ARGUS_EVENT_LIMIT) -- the dev namespace is provisioned at 1 TU
//! (~1MB/s ingress); the full ~590K-row real corpus is Chunk 11's job once
//! throughput units are bumped for real load testing, not this chunk's.
//!
//! Run with: cargo run --example eventhub_validate --release

use azure_core::time::Duration as AzureDuration;
use azure_identity::DeveloperToolsCredential;
use azure_messaging_eventhubs::{
    ConsumerClient, OpenReceiverOptions, ProducerClient, StartLocation, StartPosition,
};
use futures::stream::StreamExt;
use ingestion::{fetch_pii_salt, EventHubSink, IngestionEngine};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

const DEFAULT_INPUT_JSONL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../data/simulated/funds_transfer_raw.jsonl"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let namespace_hostname = std::env::var("ARGUS_EVENTHUB_NAMESPACE")
        .unwrap_or_else(|_| "evhns-argus-dev-to614f.servicebus.windows.net".to_string());
    let eventhub_name =
        std::env::var("ARGUS_EVENTHUB_NAME").unwrap_or_else(|_| "transactions".to_string());
    let limit: usize = std::env::var("ARGUS_EVENT_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);

    println!("[VALIDATE] namespace={namespace_hostname} eventhub={eventhub_name} limit={limit}");

    // A plain ProducerClient (separate from EventHubSink's internal one) just
    // to capture each partition's starting sequence number before we send --
    // that's what lets us read back only what this run actually produced.
    let credential = DeveloperToolsCredential::new(None)?;
    let marker_producer = ProducerClient::builder()
        .with_application_id("argus-validate-marker".to_string())
        .open(&namespace_hostname, &eventhub_name, credential.clone())
        .await?;

    let properties = marker_producer.get_eventhub_properties().await?;
    let mut start_sequence: HashMap<String, i64> = HashMap::new();
    for partition_id in &properties.partition_ids {
        let p = marker_producer
            .get_partition_properties(partition_id)
            .await?;
        start_sequence.insert(partition_id.clone(), p.last_enqueued_sequence_number);
    }
    println!(
        "[VALIDATE] partitions={:?} start_sequence={:?}",
        properties.partition_ids, start_sequence
    );
    marker_producer.close().await?;

    // Send `limit` real events through the full ingestion engine + EventHubSink.
    let salt = fetch_pii_salt().await?;
    let sink = Arc::new(EventHubSink::new(&namespace_hostname, &eventhub_name).await?);
    let engine = Arc::new(IngestionEngine::new(sink, salt));

    let file = tokio::fs::File::open(DEFAULT_INPUT_JSONL).await?;
    let mut lines = BufReader::new(file).lines();
    let mut handles = Vec::new();
    let mut sent = 0usize;
    while sent < limit {
        let Some(line) = lines.next_line().await? else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        match engine.process_stream_event(line.as_bytes()).await {
            Ok(handle) => handles.push(handle),
            Err(e) => eprintln!("[VALIDATE] enrich error: {e}"),
        }
        sent += 1;
    }
    for h in handles {
        let _ = h.await;
    }
    println!("[VALIDATE] sent {sent} events");

    // Read back from every partition concurrently, starting at each one's
    // captured sequence number, idling out after receive_timeout of silence.
    let credential = DeveloperToolsCredential::new(None)?;
    let consumer = Arc::new(
        ConsumerClient::builder()
            .with_application_id("argus-validate-consumer".to_string())
            .open(&namespace_hostname, eventhub_name.clone(), credential)
            .await?,
    );

    let mut read_handles = Vec::new();
    for (partition_id, seq) in start_sequence.clone() {
        let consumer = Arc::clone(&consumer);
        read_handles.push(tokio::spawn(async move {
            let receiver = consumer
                .open_receiver_on_partition(
                    partition_id.clone(),
                    Some(OpenReceiverOptions {
                        start_position: Some(StartPosition {
                            location: StartLocation::SequenceNumber(seq),
                            ..Default::default()
                        }),
                        receive_timeout: Some(AzureDuration::seconds(20)),
                        ..Default::default()
                    }),
                )
                .await?;
            let mut stream = receiver.stream_events();
            let mut count = 0usize;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(_) => count += 1,
                    Err(e) => {
                        eprintln!("[VALIDATE] receive error on {partition_id}: {e}");
                        break;
                    }
                }
            }
            drop(stream);
            receiver.close().await?;
            println!("[VALIDATE] partition {partition_id}: received {count}");
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(count)
        }));
    }

    let mut total_received = 0usize;
    for h in read_handles {
        match h.await {
            Ok(Ok(count)) => total_received += count,
            Ok(Err(e)) => return Err(format!("partition read error: {e}").into()),
            Err(e) => return Err(format!("join error: {e}").into()),
        }
    }
    // `consumer` is shared (Arc) across the spawned readers above; by now all
    // of them have finished and dropped their clones, so this is the sole
    // reference. `ConsumerClient::close` takes `self` by value, which an Arc
    // won't hand out without `try_unwrap` -- simpler to just let Drop close
    // the connection, same as the SDK's own documented Drop behavior.
    drop(consumer);

    println!("[VALIDATE] TOTAL sent={sent} received={total_received}");
    if total_received < sent {
        eprintln!(
            "[VALIDATE] WARNING: received ({total_received}) < sent ({sent}) -- some events may still be in flight or were dropped."
        );
    }
    Ok(())
}
