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
pub const KEY_VAULT_URL_ENV_VAR: &str = "ARGUS_KEY_VAULT_URL";
pub const PII_SALT_SECRET_NAME: &str = "argus-pii-salt";
const DEFAULT_KEY_VAULT_URL: &str = "https://kv-argus-dev-to614f.vault.azure.net/";

/// Entra credential chain shared by every Azure client in this crate:
/// managed identity when running in Azure (Container Apps sets
/// IDENTITY_ENDPOINT), the developer-tools chain (az CLI) locally. Both
/// implement `TokenCredential`, so callers never care which one they got --
/// the production/dev auth split lives entirely here.
pub fn azure_credential() -> Result<
    Arc<dyn azure_core::credentials::TokenCredential>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    if std::env::var("IDENTITY_ENDPOINT").is_ok() || std::env::var("MSI_ENDPOINT").is_ok() {
        let cred: Arc<dyn azure_core::credentials::TokenCredential> =
            azure_identity::ManagedIdentityCredential::new(None)?;
        Ok(cred)
    } else {
        let cred: Arc<dyn azure_core::credentials::TokenCredential> =
            azure_identity::DeveloperToolsCredential::new(None)?;
        Ok(cred)
    }
}

/// Fetches the PII hashing salt (Chunk 10, replacing Chunk 2's env-var
/// placeholder): primary source is the Key Vault secret `argus-pii-salt`
/// (RBAC + Entra token, no static credential), per
/// `docs/specs/PDD_Production_Guide.md` section 5's Key Vault requirement.
/// `ARGUS_PII_SALT` remains ONLY as an explicit local/offline override
/// (unit tests, air-gapped dev) and says so loudly; there is no silent
/// insecure default anymore -- no vault and no override means startup
/// fails, which is correct for a production credential path.
pub async fn fetch_pii_salt() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(v) = std::env::var(PII_SALT_ENV_VAR) {
        if !v.is_empty() {
            eprintln!(
                "[WARN] using {PII_SALT_ENV_VAR} env override for the PII salt -- \
                 local/offline use only; production fetches from Key Vault."
            );
            return Ok(v);
        }
    }
    let vault_url =
        std::env::var(KEY_VAULT_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_KEY_VAULT_URL.to_string());
    let credential = azure_credential()?;
    let client = azure_security_keyvault_secrets::SecretClient::new(&vault_url, credential, None)?;
    let secret = client
        .get_secret(PII_SALT_SECRET_NAME, None)
        .await?
        .into_model()?;
    let value = secret
        .value
        .ok_or_else(|| format!("Key Vault secret {PII_SALT_SECRET_NAME} has no value"))?;
    println!("[INITIALIZATION] PII salt loaded from Key Vault ({vault_url})");
    Ok(value)
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

/// Real trailing-window velocity (Chunk 10, closing the stub open since
/// Chunk 2): per-account count of events whose *arrival* time falls inside
/// the trailing 60 seconds, maintained as an in-process sliding window.
///
/// Documented tradeoffs (an in-process cache was chosen deliberately over a
/// shared state store like Redis, which isn't budget-justified here):
///   - state RESETS ON RESTART (first events after a restart under-count);
///   - state is PER-REPLICA -- if KEDA scales the app past one replica,
///     each replica sees only its own share of an account's traffic;
///   - the window is over INGESTION ARRIVAL time (RawTransaction carries no
///     event timestamp), so batch replays of historical data measure replay
///     rate, not historical rate -- correct semantics for a live stream,
///     which is what the deployed service exists for.
pub struct VelocityTracker {
    windows: std::sync::Mutex<std::collections::HashMap<String, std::collections::VecDeque<u64>>>,
}

const VELOCITY_WINDOW_SECS: u64 = 60;

impl VelocityTracker {
    pub fn new() -> Self {
        Self {
            windows: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Records an event for `account` at `now_secs` and returns the count of
    /// that account's events in the trailing window (including this one).
    pub fn record_and_count(&self, account: &str, now_secs: u64) -> u32 {
        let mut windows = self.windows.lock().expect("velocity lock poisoned");
        let dq = windows.entry(account.to_string()).or_default();
        dq.push_back(now_secs);
        let cutoff = now_secs.saturating_sub(VELOCITY_WINDOW_SECS);
        while dq.front().is_some_and(|&t| t < cutoff) {
            dq.pop_front();
        }
        dq.len() as u32
    }
}

impl Default for VelocityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Dead-letter destination for events whose sink dispatch exhausted every
/// retry (Chunk 10): each failure is appended as one JSON line carrying the
/// enriched payload, the sink error, and a timestamp -- failures are never
/// silently dropped. A file, not a separate Event Hub / Service Bus queue:
/// a second messaging resource isn't budget-justified for this build (noted
/// as a deliberate scope decision in context.md). In the container, the
/// file lives on ephemeral storage -- adequate for inspection/alerting via
/// stderr+Log Analytics, not durable across replica restarts; that caveat
/// travels with the decision.
pub struct DeadLetter {
    file: Mutex<tokio::fs::File>,
    path: String,
}

impl DeadLetter {
    pub async fn new(path: &str) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_string(),
        })
    }

    pub async fn write(&self, payload: &str, error: &str) {
        let record = serde_json::json!({
            "dead_lettered_at": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "error": error,
            "payload": payload,
        });
        let mut file = self.file.lock().await;
        let line = format!("{record}\n");
        // write_all + flush: tokio's File buffers internally, and write_all
        // returning does NOT mean the bytes reached the OS -- found
        // empirically (intermittent empty reads immediately after a
        // completed write). A dead-letter record that can evaporate in a
        // buffer defeats the point, so every record is flushed through.
        let result = async {
            file.write_all(line.as_bytes()).await?;
            file.flush().await
        }
        .await;
        if let Err(e) = result {
            // Last-resort visibility: the dead-letter itself failed. stderr
            // is captured by Log Analytics in the deployed container.
            eprintln!(
                "[DEAD-LETTER FAILURE] could not persist to {}: {e}; payload={payload}",
                self.path
            );
        }
    }
}

/// Chunk 11: real per-event send latency (true enqueue-to-ingest -- the
/// wall-clock duration of the actual `sink.send().await` call, not an
/// amortized batch-total/event-count approximation like Chunk 7's
/// inference number). Gated behind an explicit opt-in
/// (`IngestionEngine::with_latency_recorder`) so normal operation pays
/// zero overhead for a Vec no one reads.
pub struct LatencyRecorder {
    samples_micros: Mutex<Vec<u64>>,
}

#[derive(Debug, Clone, Copy)]
pub struct LatencyStats {
    pub count: usize,
    pub mean_micros: f64,
    pub p95_micros: u64,
    pub p99_micros: u64,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            samples_micros: Mutex::new(Vec::new()),
        }
    }

    pub async fn record(&self, micros: u64) {
        self.samples_micros.lock().await.push(micros);
    }

    pub async fn stats(&self) -> LatencyStats {
        let mut samples = self.samples_micros.lock().await.clone();
        samples.sort_unstable();
        let count = samples.len();
        if count == 0 {
            return LatencyStats {
                count: 0,
                mean_micros: 0.0,
                p95_micros: 0,
                p99_micros: 0,
            };
        }
        let mean_micros = samples.iter().sum::<u64>() as f64 / count as f64;
        let p95_idx = ((count as f64) * 0.95).floor() as usize;
        let p99_idx = ((count as f64) * 0.99).floor() as usize;
        LatencyStats {
            count,
            mean_micros,
            p95_micros: samples[p95_idx.min(count - 1)],
            p99_micros: samples[p99_idx.min(count - 1)],
        }
    }
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IngestionEngine {
    sink: Arc<dyn Sink>,
    pii_salt: String,
    velocity: Arc<VelocityTracker>,
    dead_letter: Option<Arc<DeadLetter>>,
    latency: Option<Arc<LatencyRecorder>>,
}

impl IngestionEngine {
    pub fn new(sink: Arc<dyn Sink>, pii_salt: String) -> Self {
        Self {
            sink,
            pii_salt,
            velocity: Arc::new(VelocityTracker::new()),
            dead_letter: None,
            latency: None,
        }
    }

    pub fn with_dead_letter(mut self, dead_letter: Arc<DeadLetter>) -> Self {
        self.dead_letter = Some(dead_letter);
        self
    }

    pub fn with_latency_recorder(mut self, latency: Arc<LatencyRecorder>) -> Self {
        self.latency = Some(latency);
        self
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

        // Real trailing-60s per-account velocity (see VelocityTracker for
        // the in-process-state tradeoffs) -- counted for the initiating
        // (source) account, including this event.
        let velocity_score_1m = self
            .velocity
            .record_and_count(&tx.source_account, ingestion_timestamp);

        let enriched = EnrichedTransaction {
            transaction_id: tx.transaction_id,
            source_account: tx.source_account,
            target_account: tx.target_account,
            amount: tx.amount,
            asset_type: tx.asset_type,
            device_hash,
            ip_masked,
            ingestion_timestamp,
            velocity_score_1m,
        };

        let payload = serde_json::to_string(&enriched)?;
        let sink = Arc::clone(&self.sink);
        let dead_letter = self.dead_letter.clone();
        let latency = self.latency.clone();

        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = sink.send(&payload).await;
            if let Some(lat) = &latency {
                lat.record(start.elapsed().as_micros() as u64).await;
            }
            if let Err(e) = result {
                eprintln!("[INGESTION ERROR] Failed to forward to sink: {e}");
                if let Some(dl) = dead_letter {
                    dl.write(&payload, &e.to_string()).await;
                }
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
    fn velocity_counts_within_window() {
        let v = VelocityTracker::new();
        assert_eq!(v.record_and_count("ACC-1", 1000), 1);
        assert_eq!(v.record_and_count("ACC-1", 1010), 2);
        assert_eq!(v.record_and_count("ACC-1", 1059), 3);
        // Different account: independent window.
        assert_eq!(v.record_and_count("ACC-2", 1059), 1);
    }

    #[test]
    fn velocity_expires_events_older_than_60s() {
        let v = VelocityTracker::new();
        assert_eq!(v.record_and_count("ACC-1", 1000), 1);
        assert_eq!(v.record_and_count("ACC-1", 1030), 2);
        // Window at t=1085 is [1025, 1085]: 1000 expired, 1030 retained.
        assert_eq!(v.record_and_count("ACC-1", 1085), 2);
        // Window at t=1101 is [1041, 1101]: 1000 and 1030 both expired,
        // only 1085 and this event remain.
        assert_eq!(v.record_and_count("ACC-1", 1101), 2);
        // Far future: everything prior expired.
        assert_eq!(v.record_and_count("ACC-1", 9999), 1);
    }

    #[tokio::test]
    async fn dead_letter_captures_failed_sends() {
        struct AlwaysFailSink;
        #[async_trait]
        impl Sink for AlwaysFailSink {
            async fn send(&self, _payload: &str) -> Result<(), SinkError> {
                Err(SinkError("synthetic outage".into()))
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let dl_path = dir.path().join("dl.jsonl");
        let dl = Arc::new(DeadLetter::new(dl_path.to_str().unwrap()).await.unwrap());
        let engine =
            IngestionEngine::new(Arc::new(AlwaysFailSink), "salt".into()).with_dead_letter(dl);

        let raw = br#"{"transaction_id":"TX-DL-1","source_account":"ACC-1","target_account":"ACC-2","amount":10.0,"asset_type":"USD","device_id":"DEV-1","ip_address":"10.0.0.1"}"#;
        let handle = engine.process_stream_event(raw).await.unwrap();
        handle.await.unwrap();

        let contents = std::fs::read_to_string(&dl_path).unwrap();
        let record: serde_json::Value =
            serde_json::from_str(contents.lines().next().unwrap()).unwrap();
        assert_eq!(record["error"], "sink error: synthetic outage");
        assert!(record["payload"].as_str().unwrap().contains("TX-DL-1"));
        assert!(record["dead_lettered_at"].as_u64().unwrap() > 0);
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
