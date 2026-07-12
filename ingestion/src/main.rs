//! Thin binary entrypoint: wires up a `Sink` from env vars, reads
//! newline-delimited `RawTransaction` JSON, and runs it through
//! `IngestionEngine`. `ARGUS_SINK=eventhub` (Chunk 4) sends to the real
//! Event Hubs namespace via Azure AD auth -- set `ARGUS_EVENT_LIMIT` when
//! using it against the full real corpus so a 1-TU dev namespace doesn't
//! get blasted with all ~590K rows at once.
//!
//! Chunk 10 additions:
//!   - PII salt comes from Key Vault (fetch_pii_salt), not an env var.
//!   - Exhausted-retry sends are dead-lettered (ARGUS_DEAD_LETTER_PATH,
//!     default dead_letter.jsonl).
//!   - ARGUS_MODE=service keeps the process alive after draining input (or
//!     with no input file at all) instead of exiting -- Container Apps
//!     treats a fast-exiting container as a crash loop. The deployed
//!     service's steady state is idle-until-fed; the demo's transaction
//!     source is batch replay, not a live upstream feed.

use ingestion::{
    fetch_pii_salt, DeadLetter, EventHubSink, IngestionEngine, LatencyRecorder, LocalFileSink,
    Sink, StdoutSink,
};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

const DEFAULT_INPUT_JSONL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../data/simulated/funds_transfer_raw.jsonl"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service_mode = std::env::var("ARGUS_MODE").as_deref() == Ok("service");
    let salt = fetch_pii_salt().await?;

    let measure_latency = std::env::var("ARGUS_MEASURE_LATENCY").as_deref() == Ok("1");
    let latency_recorder = measure_latency.then(|| Arc::new(LatencyRecorder::new()));
    // Chunk 11: a SEPARATE recorder for batch-send round-trip time alone
    // (excludes client-side queueing) -- see event_hub_sink.rs's doc on
    // why the two numbers answer different questions.
    let batch_latency_recorder = measure_latency.then(|| Arc::new(LatencyRecorder::new()));

    let sink: Arc<dyn Sink> = match std::env::var("ARGUS_SINK").as_deref() {
        Ok("file") => {
            let path = std::env::var("ARGUS_SINK_FILE_PATH")
                .unwrap_or_else(|_| "ingested_output.jsonl".to_string());
            println!("[INITIALIZATION] Sink: LocalFileSink -> {path}");
            Arc::new(LocalFileSink::new(&path).await?)
        }
        Ok("eventhub") => {
            let namespace_hostname = std::env::var("ARGUS_EVENTHUB_NAMESPACE")
                .unwrap_or_else(|_| "evhns-argus-dev-to614f.servicebus.windows.net".to_string());
            let eventhub_name =
                std::env::var("ARGUS_EVENTHUB_NAME").unwrap_or_else(|_| "transactions".to_string());
            println!("[INITIALIZATION] Sink: EventHubSink -> {namespace_hostname}/{eventhub_name}");
            Arc::new(
                EventHubSink::new_with_batch_latency_recorder(
                    &namespace_hostname,
                    &eventhub_name,
                    batch_latency_recorder.clone(),
                )
                .await?,
            )
        }
        _ => {
            println!("[INITIALIZATION] Sink: StdoutSink");
            Arc::new(StdoutSink)
        }
    };

    let dead_letter_path =
        std::env::var("ARGUS_DEAD_LETTER_PATH").unwrap_or_else(|_| "dead_letter.jsonl".to_string());
    let dead_letter = Arc::new(DeadLetter::new(&dead_letter_path).await?);
    println!("[INITIALIZATION] Dead-letter path: {dead_letter_path}");

    let mut engine = IngestionEngine::new(sink, salt).with_dead_letter(dead_letter);
    if let Some(lat) = latency_recorder.clone() {
        engine = engine.with_latency_recorder(lat);
    }
    let engine = Arc::new(engine);

    let input_path =
        std::env::var("ARGUS_INPUT_JSONL").unwrap_or_else(|_| DEFAULT_INPUT_JSONL.to_string());

    let run_start = std::time::Instant::now();
    let file = match tokio::fs::File::open(&input_path).await {
        Ok(f) => Some(f),
        Err(e) if service_mode => {
            println!(
                "[INITIALIZATION] no input file at {input_path} ({e}) -- service mode, idling."
            );
            None
        }
        Err(e) => {
            return Err(format!(
                "failed to open {input_path}: {e} (run data/scripts/export_ingestion_jsonl.py first)"
            )
            .into());
        }
    };

    if let Some(file) = file {
        println!("[INITIALIZATION] Launching Argus ingestion engine, reading from {input_path}");
        let mut lines = BufReader::new(file).lines();

        let limit: Option<usize> = std::env::var("ARGUS_EVENT_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok());
        if let Some(limit) = limit {
            println!("[INITIALIZATION] Capping this run at {limit} events (ARGUS_EVENT_LIMIT).");
        }

        // Post-Chunk-11: optional pacing. An unpaced replay saturates the
        // client queue by construction, so per-event LATENCY-FULL there
        // measures backlog drain (~corpus/throughput/2), not what an event
        // experiences at a sustainable arrival rate -- which is what the
        // PDD's <45ms ingestion-latency SLO is actually about. Pacing
        // releases events at a fixed rate below capacity so LATENCY-FULL
        // becomes a true arrival-to-ingested figure.
        let pace: Option<f64> = std::env::var("ARGUS_PACE_EVENTS_PER_SEC")
            .ok()
            .and_then(|v| v.parse().ok());
        if let Some(p) = pace {
            println!(
                "[INITIALIZATION] Pacing input at {p} events/sec (ARGUS_PACE_EVENTS_PER_SEC)."
            );
        }
        let pace_start = std::time::Instant::now();

        let mut handles = Vec::new();
        let mut count = 0usize;
        while let Some(line) = lines.next_line().await? {
            if limit.is_some_and(|limit| count >= limit) {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            if let Some(p) = pace {
                let due = pace_start + std::time::Duration::from_secs_f64(count as f64 / p);
                let now = std::time::Instant::now();
                if due > now {
                    tokio::time::sleep(due - now).await;
                }
            }
            match engine.process_stream_event(line.as_bytes()).await {
                Ok(handle) => handles.push(handle),
                Err(e) => eprintln!("[INGESTION ERROR] {e}"),
            }
            count += 1;
        }

        for handle in handles {
            let _ = handle.await;
        }

        let elapsed = run_start.elapsed().as_secs_f64();
        let events_per_sec = if elapsed > 0.0 {
            count as f64 / elapsed
        } else {
            0.0
        };
        println!(
            "[DRAIN COMPLETE] Ingested {count} events in {elapsed:.3}s ({events_per_sec:.1} events/sec)."
        );

        if let Some(lat) = latency_recorder {
            let stats = lat.stats().await;
            println!(
                "[LATENCY-FULL] (includes client-side queueing under this run's concurrency) count={} mean_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
                stats.count,
                stats.mean_micros / 1000.0,
                stats.p95_micros as f64 / 1000.0,
                stats.p99_micros as f64 / 1000.0
            );
        }
        if let Some(lat) = batch_latency_recorder {
            let stats = lat.stats().await;
            println!(
                "[LATENCY-BATCH] (send_batch round trip only, excludes queueing) count={} mean_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
                stats.count,
                stats.mean_micros / 1000.0,
                stats.p95_micros as f64 / 1000.0,
                stats.p99_micros as f64 / 1000.0
            );
        }
    }

    if service_mode {
        println!("[SERVICE] input drained; staying alive (scale-to-zero handles shutdown).");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            println!("[SERVICE] heartbeat: alive and idle.");
        }
    }

    Ok(())
}
