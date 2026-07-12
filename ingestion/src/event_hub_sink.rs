//! `EventHubSink`: sends enriched transactions to the real "transactions"
//! Event Hub via Azure AD (Entra ID) authentication -- no static connection
//! strings or keys anywhere in this file.
//!
//! API confirmed against azure_messaging_eventhubs v0.15.0's actual source
//! (docs.rs's rendered pages didn't resolve cleanly via automated fetch;
//! verified against the crate's own tests/examples in the azure-sdk-for-rust
//! GitHub repo instead) rather than assumed from training data -- see
//! context.md's Architectural Decisions Log for what differed from
//! expectations (`DeveloperToolsCredential` replacing the
//! `DefaultAzureCredential` name older SDK generations used).
//!
//! Chunk 11 addition: internal batching. `send_event` (one event, one AMQP
//! round trip) was the whole story through Chunk 10's validation runs
//! (a few thousand events at a time), which never surfaced a problem. At
//! real load (the 590K-row corpus, target 10,000 events/sec) it fell over
//! hard -- ~15 events/sec, 6+ GB of resident memory from unbounded spawned
//! tasks queuing on a single connection's sender. Found empirically (not
//! assumed): the crate has a real batch API (`create_batch` /
//! `try_add_event_data` / `send_batch`, confirmed in its own source), which
//! amortizes the AMQP round trip over many events instead of paying it once
//! per event. The public `Sink::send` contract (one payload in, one
//! `Result` out) is unchanged -- batching is an internal implementation
//! detail: callers' payloads are queued, grouped by a short time/size
//! window, sent as one `EventDataBatch`, and each caller's `Result` resolves
//! once its batch lands.

use crate::{azure_credential, LatencyRecorder, Sink, SinkError};
use async_trait::async_trait;
use azure_core::time::Duration as AzureDuration;
use azure_messaging_eventhubs::{EventDataBatchOptions, ProducerClient, RetryOptions};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{mpsc, oneshot, Semaphore};

/// Attempts around the batch send itself, on top of (not instead of) the
/// SDK's own internal `RetryOptions` -- see the module doc for the split
/// between this Sink-layer retry and the SDK's own AMQP link/connection
/// recovery.
const SEND_MAX_ATTEMPTS: u32 = 3;
const SEND_INITIAL_BACKOFF: StdDuration = StdDuration::from_millis(200);

/// Batching window: gather whatever arrives within this much wall time (or
/// until a batch fills, whichever first) before flushing. Small enough that
/// a lone event under light load still moves in ~10ms, large enough that
/// sustained high-rate traffic gets real amortization (hundreds of ~250-300
/// byte events comfortably fit in one Standard-tier message envelope).
const BATCH_WINDOW: StdDuration = StdDuration::from_millis(10);

/// Post-Chunk-11 fix: batches are now dispatched CONCURRENTLY (pipelined),
/// bounded by this many in flight at once -- Chunk 11 measured that
/// sequential dispatch (one batch awaited at a time) was the dominant
/// latency cost: at ~170ms round trip it caps at ~6 batches/sec, so under
/// full-corpus load events queued 16-17 SECONDS client-side before their
/// batch even started sending. Why 8: sustaining Chunk 11's measured
/// ~15,000 events/sec at the observed ~3,200-event batch size needs ~5
/// batches/sec; 8 in flight raises the dispatch ceiling to ~47/sec (mean
/// RTT) -- ample headroom without approximating the unbounded task spawn
/// that caused Chunk 11's 6GB memory blowup. Bounded via Semaphore, so
/// when all permits are taken the gather loop itself backpressures
/// naturally instead of piling up in-flight sends. Tradeoff, stated
/// honestly: batches may now complete out of order, so cross-batch event
/// ordering is no longer guaranteed -- nothing downstream depends on
/// cross-event ordering (velocity is computed at ingest time, inference
/// applies events commutatively), and Event Hubs partitions never
/// guaranteed cross-partition order anyway.
const MAX_IN_FLIGHT_BATCHES: usize = 8;

struct BatchItem {
    payload: String,
    reply: oneshot::Sender<Result<(), SinkError>>,
}

pub struct EventHubSink {
    tx: mpsc::UnboundedSender<BatchItem>,
}

impl EventHubSink {
    /// Opens a producer connection to `eventhub_name` in `namespace_hostname`
    /// (e.g. "evhns-argus-dev-to614f.servicebus.windows.net") using the
    /// shared credential chain from `lib.rs` (Chunk 10): the Container App's
    /// system-assigned managed identity when running in Azure (its "Azure
    /// Event Hubs Data Sender" grant), the developer-tools/az-CLI chain
    /// locally (the Chunk 4 dev-only grant). Both are `TokenCredential`
    /// implementations, so nothing else in this sink changes between
    /// environments.
    pub async fn new(namespace_hostname: &str, eventhub_name: &str) -> Result<Self, SinkError> {
        Self::new_with_batch_latency_recorder(namespace_hostname, eventhub_name, None).await
    }

    /// Same as `new`, plus an optional recorder for BATCH-level round-trip
    /// time (the `send_batch` call itself, excluding client-side queueing).
    /// Chunk 11: under real concurrent load, the per-event latency
    /// `IngestionEngine`/`LatencyRecorder` measures includes time spent
    /// queued behind other in-flight batches -- genuinely part of real
    /// end-to-end latency under load, but easy to mistake for the network/
    /// broker's own latency if that's the only number reported. This
    /// second recorder disentangles the two so both can be reported
    /// honestly instead of picking one framing.
    pub async fn new_with_batch_latency_recorder(
        namespace_hostname: &str,
        eventhub_name: &str,
        batch_latency: Option<Arc<LatencyRecorder>>,
    ) -> Result<Self, SinkError> {
        let credential = azure_credential()
            .map_err(|e| SinkError(format!("failed to build Azure AD credential: {e}")))?;

        let producer = ProducerClient::builder()
            .with_application_id("argus-ingestion".to_string())
            .with_retry_options(RetryOptions {
                max_retries: 5,
                initial_delay: AzureDuration::milliseconds(200),
                max_delay: AzureDuration::seconds(10),
                ..Default::default()
            })
            .open(namespace_hostname, eventhub_name, credential)
            .await
            .map_err(|e| SinkError(format!("failed to open Event Hubs producer: {e}")))?;

        let producer = Arc::new(producer);
        let (tx, rx) = mpsc::unbounded_channel::<BatchItem>();
        tokio::spawn(Self::batch_loop(producer, rx, batch_latency));

        Ok(Self { tx })
    }

    async fn batch_loop(
        producer: Arc<ProducerClient>,
        mut rx: mpsc::UnboundedReceiver<BatchItem>,
        batch_latency: Option<Arc<LatencyRecorder>>,
    ) {
        let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT_BATCHES));
        loop {
            let Some(first) = rx.recv().await else {
                break; // sender side (the EventHubSink) was dropped
            };
            let mut items = vec![first];

            let window = tokio::time::sleep(BATCH_WINDOW);
            tokio::pin!(window);
            loop {
                tokio::select! {
                    biased;
                    maybe_item = rx.recv() => {
                        match maybe_item {
                            Some(item) => items.push(item),
                            None => break,
                        }
                    }
                    _ = &mut window => break,
                }
            }

            // Acquire a permit BEFORE spawning: when MAX_IN_FLIGHT_BATCHES
            // sends are already outstanding, this gather loop blocks here,
            // which is the backpressure. acquire_owned can only fail if the
            // semaphore is closed, which never happens here.
            let permit = Arc::clone(&in_flight)
                .acquire_owned()
                .await
                .expect("in-flight semaphore closed unexpectedly");
            let producer = Arc::clone(&producer);
            let batch_latency = batch_latency.clone();
            tokio::spawn(async move {
                Self::flush(&producer, items, batch_latency.as_ref()).await;
                drop(permit);
            });
        }
    }

    /// Packs `items` into as many `EventDataBatch`es as needed (one, almost
    /// always, at these payload sizes) and sends each with the Sink-layer
    /// retry. Every item gets a reply either way -- no caller is left
    /// hanging on a dropped oneshot.
    async fn flush(
        producer: &Arc<ProducerClient>,
        items: Vec<BatchItem>,
        batch_latency: Option<&Arc<LatencyRecorder>>,
    ) {
        let mut items = items.into_iter().peekable();
        while items.peek().is_some() {
            let mut batch = match producer
                .create_batch(Some(EventDataBatchOptions::default()))
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    let msg = format!("failed to create batch: {e}");
                    for item in items {
                        let _ = item.reply.send(Err(SinkError(msg.clone())));
                    }
                    return;
                }
            };

            let mut in_this_batch = Vec::new();
            while let Some(item) = items.peek() {
                match batch.try_add_event_data(item.payload.clone(), None) {
                    Ok(true) => {
                        in_this_batch.push(items.next().unwrap());
                    }
                    Ok(false) => break, // batch full; remainder starts a new one
                    Err(e) => {
                        let item = items.next().unwrap();
                        let _ = item
                            .reply
                            .send(Err(SinkError(format!("event rejected from batch: {e}"))));
                    }
                }
            }

            if batch.is_empty() {
                continue;
            }

            let mut attempt = 0u32;
            let mut backoff = SEND_INITIAL_BACKOFF;
            let send_start = std::time::Instant::now();
            let result = loop {
                match producer.send_batch(batch, None).await {
                    Ok(()) => break Ok(()),
                    Err(_) if attempt + 1 < SEND_MAX_ATTEMPTS => {
                        attempt += 1;
                        eprintln!(
                            "[EVENTHUB_SINK] batch send failed (attempt {attempt}/{SEND_MAX_ATTEMPTS}) -- retrying in {backoff:?}"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        // EventDataBatch isn't Clone/reusable after a failed
                        // send attempt in this API -- rebuild it from the
                        // same payloads for the retry.
                        let rebuilt = match producer
                            .create_batch(Some(EventDataBatchOptions::default()))
                            .await
                        {
                            Ok(b) => b,
                            Err(e) => break Err(format!("failed to rebuild batch for retry: {e}")),
                        };
                        for item in &in_this_batch {
                            let _ = rebuilt.try_add_event_data(item.payload.clone(), None);
                        }
                        batch = rebuilt;
                        continue;
                    }
                    Err(e) => {
                        break Err(format!(
                            "batch send failed after {SEND_MAX_ATTEMPTS} attempts: {e}"
                        ))
                    }
                }
            };

            if let Some(lat) = batch_latency {
                lat.record(send_start.elapsed().as_micros() as u64).await;
            }

            match result {
                Ok(()) => {
                    for item in in_this_batch {
                        let _ = item.reply.send(Ok(()));
                    }
                }
                Err(msg) => {
                    for item in in_this_batch {
                        let _ = item.reply.send(Err(SinkError(msg.clone())));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Sink for EventHubSink {
    async fn send(&self, payload: &str) -> Result<(), SinkError> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(BatchItem {
                payload: payload.to_string(),
                reply,
            })
            .map_err(|_| SinkError("EventHubSink batch loop is no longer running".to_string()))?;

        reply_rx
            .await
            .map_err(|_| SinkError("EventHubSink batch loop dropped the reply".to_string()))?
    }
}
