//! Thin binary entrypoint: wires up a `Sink` from env vars, reads
//! newline-delimited `RawTransaction` JSON from `data/simulated/` (Chunk 1's
//! export, see data/scripts/export_ingestion_jsonl.py), and runs it through
//! `IngestionEngine`. `ARGUS_SINK=eventhub` (Chunk 4) sends to the real
//! Event Hubs namespace via Azure AD auth -- set `ARGUS_EVENT_LIMIT` when
//! using it against the full real corpus so a 1-TU dev namespace doesn't
//! get blasted with all ~590K rows at once.

use ingestion::{pii_salt_from_env, EventHubSink, IngestionEngine, LocalFileSink, Sink, StdoutSink};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

const DEFAULT_INPUT_JSONL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../data/simulated/funds_transfer_raw.jsonl"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let salt = pii_salt_from_env();

    let sink: Arc<dyn Sink> = match std::env::var("ARGUS_SINK").as_deref() {
        Ok("file") => {
            let path = std::env::var("ARGUS_SINK_FILE_PATH")
                .unwrap_or_else(|_| "ingested_output.jsonl".to_string());
            println!("[INITIALIZATION] Sink: LocalFileSink -> {path}");
            Arc::new(LocalFileSink::new(&path).await?)
        }
        Ok("eventhub") => {
            let namespace_hostname = std::env::var("ARGUS_EVENTHUB_NAMESPACE").unwrap_or_else(|_| {
                "evhns-argus-dev-to614f.servicebus.windows.net".to_string()
            });
            let eventhub_name =
                std::env::var("ARGUS_EVENTHUB_NAME").unwrap_or_else(|_| "transactions".to_string());
            println!("[INITIALIZATION] Sink: EventHubSink -> {namespace_hostname}/{eventhub_name}");
            Arc::new(EventHubSink::new(&namespace_hostname, &eventhub_name).await?)
        }
        _ => {
            println!("[INITIALIZATION] Sink: StdoutSink");
            Arc::new(StdoutSink)
        }
    };

    let engine = Arc::new(IngestionEngine::new(sink, salt));

    let input_path =
        std::env::var("ARGUS_INPUT_JSONL").unwrap_or_else(|_| DEFAULT_INPUT_JSONL.to_string());
    println!("[INITIALIZATION] Launching Argus ingestion engine, reading from {input_path}");

    let file = tokio::fs::File::open(&input_path).await.map_err(|e| {
        format!(
            "failed to open {input_path}: {e} (run data/scripts/export_ingestion_jsonl.py first)"
        )
    })?;
    let mut lines = BufReader::new(file).lines();

    let limit: Option<usize> = std::env::var("ARGUS_EVENT_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok());
    if let Some(limit) = limit {
        println!("[INITIALIZATION] Capping this run at {limit} events (ARGUS_EVENT_LIMIT).");
    }

    let mut handles = Vec::new();
    let mut count = 0usize;
    while let Some(line) = lines.next_line().await? {
        if limit.is_some_and(|limit| count >= limit) {
            break;
        }
        if line.trim().is_empty() {
            continue;
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

    println!("[DRAIN COMPLETE] Ingested {count} events.");
    Ok(())
}
