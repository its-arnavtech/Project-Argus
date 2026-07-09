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

use crate::{Sink, SinkError};
use async_trait::async_trait;
use azure_core::time::Duration as AzureDuration;
use azure_identity::DeveloperToolsCredential;
use azure_messaging_eventhubs::{ProducerClient, RetryOptions, SendEventOptions};
use std::time::Duration as StdDuration;

/// Attempts around the `send_event` call itself, on top of (not instead of)
/// the SDK's own internal `RetryOptions` -- this is Chunk 4's "basic
/// transient-error retry" requirement: a few attempts with exponential
/// backoff at the Sink layer, since this now talks to a real network.
/// `RetryOptions` below governs the SDK's own AMQP link/connection recovery,
/// a separate, lower layer.
const SEND_MAX_ATTEMPTS: u32 = 3;
const SEND_INITIAL_BACKOFF: StdDuration = StdDuration::from_millis(200);

pub struct EventHubSink {
    producer: ProducerClient,
}

impl EventHubSink {
    /// Opens a producer connection to `eventhub_name` in `namespace_hostname`
    /// (e.g. "evhns-argus-dev-to614f.servicebus.windows.net") using
    /// `DeveloperToolsCredential`, which tries `AzureCliCredential` first --
    /// this is what makes the Chunk 4 dev-only RBAC grant ("Azure Event Hubs
    /// Data Sender" on the current az CLI identity) actually work locally.
    /// Chunk 10 swaps this for the Container App's managed identity in
    /// production; no code change needed here, since both are
    /// `TokenCredential` implementations behind the same `Arc<dyn ...>`.
    pub async fn new(namespace_hostname: &str, eventhub_name: &str) -> Result<Self, SinkError> {
        let credential = DeveloperToolsCredential::new(None)
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

        Ok(Self { producer })
    }
}

#[async_trait]
impl Sink for EventHubSink {
    async fn send(&self, payload: &str) -> Result<(), SinkError> {
        let mut attempt = 0u32;
        let mut backoff = SEND_INITIAL_BACKOFF;
        loop {
            let result = self
                .producer
                .send_event(payload.to_string(), Some(SendEventOptions { partition_id: None }))
                .await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) if attempt + 1 < SEND_MAX_ATTEMPTS => {
                    attempt += 1;
                    eprintln!(
                        "[EVENTHUB_SINK] send failed (attempt {attempt}/{SEND_MAX_ATTEMPTS}): {e} -- retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => {
                    return Err(SinkError(format!(
                        "send_event failed after {SEND_MAX_ATTEMPTS} attempts: {e}"
                    )));
                }
            }
        }
    }
}
