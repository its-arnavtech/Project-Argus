//! Argus ingestion engine core: transaction structs, PII masking, and the
//! `Sink` abstraction. Split from `main.rs` so this logic is unit-testable
//! (see `docs/specs/POC_Blueprint.md` section 2 for the original illustrative
//! single-file mock this replaces).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

mod event_hub_sink;
pub use event_hub_sink::EventHubSink;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawTransaction {
    pub transaction_id: String,
    pub source_account: String,
    pub target_account: String,
    pub amount: f64,
    pub asset_type: String,
    pub device_id: String,
    pub ip_address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EnrichedTransaction {
    pub transaction_id: String,
    pub source_account: String,
    pub target_account: String,
    pub amount: f64,
    pub asset_type: String,
    pub device_hash: String,
    pub ip_masked: String,
    pub ingestion_timestamp: u64,
    pub velocity_score_1m: u32,
}

#[derive(Debug)]
pub struct SinkError(pub String);

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sink error: {}", self.0)
    }
}

impl std::error::Error for SinkError {}

/// Abstracts "where enriched transactions go". `IngestionEngine` only ever
/// depends on this trait, never on a concrete transport -- Chunk 4 adds an
/// `EventHubSink` wrapping the real `azure_messaging_eventhubs` crate as a
/// third implementation alongside these two, without touching any engine
/// logic in this file (see context.md Architectural Decisions Log).
#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, payload: &str) -> Result<(), SinkError>;
}

/// Writes each payload to stdout, one line per event.
pub struct StdoutSink;

#[async_trait]
impl Sink for StdoutSink {
    async fn send(&self, payload: &str) -> Result<(), SinkError> {
        println!("{payload}");
        Ok(())
    }
}

/// Appends each payload as a line to a local file. Holds the file handle
/// behind a `tokio::sync::Mutex` so concurrent `send` calls (dispatched via
/// `tokio::spawn`) serialize their writes instead of interleaving.
pub struct LocalFileSink {
    file: Mutex<tokio::fs::File>,
}

impl LocalFileSink {
    pub async fn new(path: &str) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

#[async_trait]
impl Sink for LocalFileSink {
    async fn send(&self, payload: &str) -> Result<(), SinkError> {
        let mut file = self.file.lock().await;
        file.write_all(payload.as_bytes())
            .await
            .map_err(|e| SinkError(e.to_string()))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| SinkError(e.to_string()))?;
        Ok(())
    }
}

pub const PII_SALT_ENV_VAR: &str = "ARGUS_PII_SALT";
const DEV_FALLBACK_SALT: &str = "argus-dev-salt-CHANGE-ME";

/// Reads the PII hashing salt from `ARGUS_PII_SALT`. This is a placeholder
/// today -- Chunk 10 wires it to an Azure Key Vault secret per
/// `docs/specs/PDD_Production_Guide.md` section 5. Falls back to an
/// obviously-fake dev salt (with a loud warning) rather than failing to
/// start, since Chunk 2 is local-only and has no Key Vault to fall back to
/// yet.
pub fn pii_salt_from_env() -> String {
    match std::env::var(PII_SALT_ENV_VAR) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!(
                "[WARN] {PII_SALT_ENV_VAR} not set -- using an insecure default salt \
                 (local/dev only, do not use this path in production)."
            );
            DEV_FALLBACK_SALT.to_string()
        }
    }
}

/// SHA-256, salted. The POC blueprint's illustrative snippet hashed
/// `device_id` with MD5 -- broken, and not what ships here.
/// `docs/specs/PDD_Production_Guide.md` section 5 is the authoritative spec
/// for PII masking ("SHA-256 salted tokens"), and that's what this follows.
pub fn hash_device_id(device_id: &str, salt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(device_id.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Truncates an IPv4 dotted-quad to its /24 (zeroes the last octet).
/// Malformed input maps to "0.0.0.0" rather than passing raw PII through.
pub fn mask_ip(ip_address: &str) -> String {
    let parts: Vec<&str> = ip_address.split('.').collect();
    if parts.len() == 4 {
        format!("{}.{}.{}.0", parts[0], parts[1], parts[2])
    } else {
        "0.0.0.0".to_string()
    }
}

pub struct IngestionEngine {
    sink: Arc<dyn Sink>,
    pii_salt: String,
}

impl IngestionEngine {
    pub fn new(sink: Arc<dyn Sink>, pii_salt: String) -> Self {
        Self { sink, pii_salt }
    }

    /// Parses, masks/hashes, and enriches one raw event, then hands the
    /// enriched payload off to the sink via `tokio::spawn` so a slow sink
    /// never backpressures the caller. Returns the spawned task's
    /// `JoinHandle` -- callers that want fire-and-forget semantics can drop
    /// it; callers that need to know dispatch completed (tests, a graceful
    /// shutdown drain) can await it.
    pub async fn process_stream_event(
        &self,
        raw_data: &[u8],
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        let tx: RawTransaction = serde_json::from_slice(raw_data)?;

        let device_hash = hash_device_id(&tx.device_id, &self.pii_salt);
        let ip_masked = mask_ip(&tx.ip_address);
        let ingestion_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let enriched = EnrichedTransaction {
            transaction_id: tx.transaction_id,
            source_account: tx.source_account,
            target_account: tx.target_account,
            amount: tx.amount,
            asset_type: tx.asset_type,
            device_hash,
            ip_masked,
            ingestion_timestamp,
            // Real-time velocity scoring needs a shared cache (e.g. Redis)
            // fed by prior events for the same account -- out of scope for
            // the ingestion engine itself; wired in when that cache exists.
            velocity_score_1m: 0,
        };

        let payload = serde_json::to_string(&enriched)?;
        let sink = Arc::clone(&self.sink);

        let handle = tokio::spawn(async move {
            if let Err(e) = sink.send(&payload).await {
                eprintln!("[INGESTION ERROR] Failed to forward to sink: {e}");
            }
        });

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn mask_ip_truncates_last_octet() {
        assert_eq!(mask_ip("192.168.1.145"), "192.168.1.0");
    }

    #[test]
    fn mask_ip_handles_malformed_input() {
        assert_eq!(mask_ip("not-an-ip"), "0.0.0.0");
        assert_eq!(mask_ip("1.2.3"), "0.0.0.0");
        assert_eq!(mask_ip(""), "0.0.0.0");
    }

    #[test]
    fn hash_device_id_is_deterministic() {
        let a = hash_device_id("MAC-A1B2C3", "salt-1");
        let b = hash_device_id("MAC-A1B2C3", "salt-1");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_device_id_differs_by_input() {
        let a = hash_device_id("MAC-A1B2C3", "salt-1");
        let b = hash_device_id("MAC-DIFFERENT", "salt-1");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_device_id_differs_by_salt() {
        // Same device_id, different salt -> different hash. This is what
        // makes the hash non-reversible without the salt (which lives in
        // Key Vault from Chunk 10 onward, not alongside the data).
        let a = hash_device_id("MAC-A1B2C3", "salt-1");
        let b = hash_device_id("MAC-A1B2C3", "salt-2");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_device_id_is_sha256_shaped_and_non_reversible() {
        let device_id = "MAC-A1B2C3";
        let hash = hash_device_id(device_id, "salt-1");
        // SHA-256 hex digest: 64 hex chars.
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        // Sanity check: the original identifier shouldn't appear verbatim
        // in its own hash (true for any real hash function; catches a
        // regression to a no-op/identity "hash").
        assert!(!hash.contains(device_id));
    }

    struct CapturingSink {
        captured: StdMutex<Vec<String>>,
    }

    impl CapturingSink {
        fn new() -> Self {
            Self {
                captured: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for CapturingSink {
        async fn send(&self, payload: &str) -> Result<(), SinkError> {
            self.captured.lock().unwrap().push(payload.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn enrichment_maps_all_fields_correctly() {
        let sink = Arc::new(CapturingSink::new());
        let engine = IngestionEngine::new(sink.clone(), "test-salt".to_string());

        let raw = br#"{"transaction_id":"TX-000001","source_account":"ACC-8832","target_account":"ACC-1092","amount":48500.0,"asset_type":"USD","device_id":"MAC-A1B2C3","ip_address":"192.168.1.145"}"#;

        let handle = engine.process_stream_event(raw).await.unwrap();
        handle.await.unwrap();

        let captured = sink.captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let enriched: EnrichedTransaction = serde_json::from_str(&captured[0]).unwrap();

        assert_eq!(enriched.transaction_id, "TX-000001");
        assert_eq!(enriched.source_account, "ACC-8832");
        assert_eq!(enriched.target_account, "ACC-1092");
        assert_eq!(enriched.amount, 48500.0);
        assert_eq!(enriched.asset_type, "USD");
        assert_eq!(enriched.ip_masked, "192.168.1.0");
        assert_eq!(
            enriched.device_hash,
            hash_device_id("MAC-A1B2C3", "test-salt")
        );
        assert!(enriched.ingestion_timestamp > 0);
    }

    #[tokio::test]
    async fn throughput_local_file_sink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("throughput.jsonl");
        let sink = Arc::new(LocalFileSink::new(path.to_str().unwrap()).await.unwrap());
        let engine = IngestionEngine::new(sink, "throughput-test-salt".to_string());

        let n = 20_000usize;
        let start = std::time::Instant::now();
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let raw = format!(
                r#"{{"transaction_id":"TX-{i:06}","source_account":"ACC-1","target_account":"ACC-2","amount":100.0,"asset_type":"USD","device_id":"DEV-1","ip_address":"10.0.0.1"}}"#
            );
            let handle = engine.process_stream_event(raw.as_bytes()).await.unwrap();
            handles.push(handle);
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed();
        let events_per_sec = n as f64 / elapsed.as_secs_f64();

        println!(
            "[THROUGHPUT] {n} events in {elapsed:?} => {events_per_sec:.0} events/sec (LocalFileSink)"
        );

        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.lines().count(), n);
        assert!(events_per_sec > 0.0);
    }
}
