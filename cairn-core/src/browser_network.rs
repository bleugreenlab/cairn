use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::security::sanitize::{truncate_string, truncate_utf8};
use crate::security::{RedactionPolicy, Sanitizer};

const MAX_RECORDS_PER_BROWSER: usize = 500;
const MAX_BYTES_PER_BROWSER: usize = 8 * 1024 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 32 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
/// Bound on a captured error string, applied after sanitization.
const MAX_ERROR_CHARS: usize = 4096;
/// Bound on a captured initiator stack, applied after sanitization.
const MAX_STACK_CHARS: usize = 8192;
/// Ceiling on ONE capture, applied at both ends of the pipeline: the webview
/// plugin refuses a larger payload from the page, and the archive refuses one
/// that reaches it anyway. Batching does not relax it — it is per capture, not
/// per invoke.
pub const MAX_CAPTURE_PAYLOAD_BYTES: usize = 256 * 1024;

/// Captures one `submit_browser_network_capture` invoke may carry.
///
/// A busy page emits network events continuously, and one invoke per event made
/// this the largest blocking-command consumer in the runner (CAIRN-3787). The
/// webview plugin therefore coalesces a page's events and flushes them as one
/// batch; this bound governs both ends of that pipeline — the plugin cuts a
/// batch here, and the runner rejects anything larger as a protocol error.
pub const MAX_CAPTURES_PER_BATCH: usize = 128;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserNetworkEntry {
    pub(crate) id: String,
    pub(crate) ts: i64,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    pub(crate) has_details: bool,
    pub(crate) truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum CapturedBody {
    Json {
        value: Value,
        #[serde(default)]
        truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_size: Option<u64>,
    },
    Text {
        text: String,
        #[serde(default)]
        truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_size: Option<u64>,
    },
    Form {
        fields: Vec<FormField>,
        #[serde(default)]
        truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_size: Option<u64>,
    },
    BinaryOmitted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size: Option<u64>,
    },
    CrossOriginOmitted,
    Unsupported {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Unavailable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl Default for CapturedBody {
    fn default() -> Self {
        Self::Unavailable { reason: None }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file: Option<FileMetadata>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    size: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkTiming {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) redirect_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) worker_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) dns_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) connect_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tls_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) response_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) total_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) protocol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) transfer_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) encoded_body_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) decoded_body_size: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedirectMetadata {
    #[serde(default)]
    pub(crate) redirected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) final_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    aggregate_ms: Option<f64>,
    #[serde(default)]
    hop_chain_available: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitiatorMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) initiator_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) document_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserNetworkRecord {
    pub(crate) id: String,
    pub(crate) ts: i64,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) request_headers: Vec<(String, String)>,
    #[serde(default)]
    pub(crate) request_body: CapturedBody,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<u16>,
    #[serde(default)]
    pub(crate) response_headers: Vec<(String, String)>,
    #[serde(default)]
    pub(crate) response_body: CapturedBody,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) timing: NetworkTiming,
    #[serde(default)]
    pub(crate) redirect: RedirectMetadata,
    #[serde(default)]
    pub(crate) initiator: InitiatorMetadata,
}

impl BrowserNetworkRecord {
    fn summary(&self) -> BrowserNetworkEntry {
        let truncated =
            body_is_truncated(&self.request_body) || body_is_truncated(&self.response_body);
        BrowserNetworkEntry {
            id: self.id.clone(),
            ts: self.ts,
            method: self.method.clone(),
            url: self.url.clone(),
            kind: self.kind.clone(),
            status: self.status,
            duration_ms: self.timing.total_ms,
            size: self
                .timing
                .transfer_size
                .or(self.timing.encoded_body_size)
                .or_else(|| body_original_size(&self.response_body)),
            error: self.error.clone(),
            has_details: true,
            truncated,
        }
    }
}

fn body_is_truncated(body: &CapturedBody) -> bool {
    matches!(
        body,
        CapturedBody::Json {
            truncated: true,
            ..
        } | CapturedBody::Text {
            truncated: true,
            ..
        } | CapturedBody::Form {
            truncated: true,
            ..
        }
    )
}

fn body_original_size(body: &CapturedBody) -> Option<u64> {
    match body {
        CapturedBody::Json { original_size, .. }
        | CapturedBody::Text { original_size, .. }
        | CapturedBody::Form { original_size, .. } => *original_size,
        CapturedBody::BinaryOmitted { size, .. } => *size,
        _ => None,
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ArchiveError {
    #[error("network capture request id is invalid")]
    InvalidId,
    #[error("network capture payload is too large")]
    PayloadTooLarge,
    #[error("browser network capture belongs to an expired webview generation")]
    ExpiredGeneration,
    #[error("network capture request id already exists")]
    DuplicateId,
}

#[derive(Default)]
struct BrowserArchive {
    generation: Option<String>,
    order: VecDeque<String>,
    records: HashMap<String, (BrowserNetworkRecord, usize)>,
    bytes: usize,
}

#[derive(Default)]
pub struct BrowserNetworkArchive {
    browsers: Mutex<HashMap<String, BrowserArchive>>,
}

impl BrowserNetworkArchive {
    pub fn activate(&self, browser_id: &str, generation: &str) {
        let mut all = self
            .browsers
            .lock()
            .expect("browser network archive poisoned");
        let archive = all.entry(browser_id.to_string()).or_default();
        if archive.generation.as_deref() != Some(generation) {
            archive.order.clear();
            archive.records.clear();
            archive.bytes = 0;
            archive.generation = Some(generation.to_string());
        }
    }

    pub fn insert_json_for_generation(
        &self,
        browser_id: &str,
        generation: &str,
        payload: &str,
        policy: &RedactionPolicy,
    ) -> Result<BrowserNetworkEntry, ArchiveError> {
        if payload.len() > MAX_CAPTURE_PAYLOAD_BYTES {
            return Err(ArchiveError::PayloadTooLarge);
        }
        let record = serde_json::from_str(payload).map_err(|_| ArchiveError::PayloadTooLarge)?;
        self.insert_inner(browser_id, Some(generation), record, policy)
    }

    pub fn insert(
        &self,
        browser_id: &str,
        record: BrowserNetworkRecord,
        policy: &RedactionPolicy,
    ) -> Result<BrowserNetworkEntry, ArchiveError> {
        self.insert_inner(browser_id, None, record, policy)
    }

    fn insert_inner(
        &self,
        browser_id: &str,
        generation: Option<&str>,
        mut record: BrowserNetworkRecord,
        policy: &RedactionPolicy,
    ) -> Result<BrowserNetworkEntry, ArchiveError> {
        if !is_valid_request_id(&record.id) {
            return Err(ArchiveError::InvalidId);
        }
        sanitize_record(&mut record, policy);
        bound_body(&mut record.request_body, MAX_REQUEST_BODY_BYTES);
        bound_body(&mut record.response_body, MAX_RESPONSE_BODY_BYTES);
        let byte_size = serde_json::to_vec(&record)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        if byte_size > MAX_CAPTURE_PAYLOAD_BYTES {
            return Err(ArchiveError::PayloadTooLarge);
        }
        let summary = record.summary();
        let mut all = self
            .browsers
            .lock()
            .expect("browser network archive poisoned");
        let archive = match generation {
            Some(generation) => {
                let archive = all
                    .get_mut(browser_id)
                    .ok_or(ArchiveError::ExpiredGeneration)?;
                if archive.generation.as_deref() != Some(generation) {
                    return Err(ArchiveError::ExpiredGeneration);
                }
                archive
            }
            None => all.entry(browser_id.to_string()).or_default(),
        };
        if archive.records.contains_key(&record.id) {
            return Err(ArchiveError::DuplicateId);
        }
        archive.bytes += byte_size;
        archive.order.push_back(record.id.clone());
        archive
            .records
            .insert(record.id.clone(), (record, byte_size));
        while archive.records.len() > MAX_RECORDS_PER_BROWSER
            || archive.bytes > MAX_BYTES_PER_BROWSER
        {
            let Some(id) = archive.order.pop_front() else {
                break;
            };
            if let Some((_, bytes)) = archive.records.remove(&id) {
                archive.bytes = archive.bytes.saturating_sub(bytes);
            }
        }
        Ok(summary)
    }

    pub(crate) fn list(&self, browser_id: &str, limit: Option<usize>) -> Vec<BrowserNetworkEntry> {
        let all = self
            .browsers
            .lock()
            .expect("browser network archive poisoned");
        let Some(archive) = all.get(browser_id) else {
            return Vec::new();
        };
        let skip = limit
            .map(|limit| archive.order.len().saturating_sub(limit))
            .unwrap_or(0);
        archive
            .order
            .iter()
            .skip(skip)
            .filter_map(|id| archive.records.get(id).map(|(record, _)| record.summary()))
            .collect()
    }

    pub fn get(&self, browser_id: &str, request_id: &str) -> Option<BrowserNetworkRecord> {
        self.browsers
            .lock()
            .expect("browser network archive poisoned")
            .get(browser_id)
            .and_then(|archive| archive.records.get(request_id))
            .map(|(record, _)| record.clone())
    }

    pub fn clear(&self, browser_id: &str) {
        self.browsers
            .lock()
            .expect("browser network archive poisoned")
            .remove(browser_id);
    }
}

/// Restores the webview generations that remain live across a runner restart.
///
/// The archive itself is intentionally runtime-only, but the desktop process and
/// its webviews outlive a runner bounce. Persisted open rows are therefore the
/// authoritative startup inventory for which generations may resume capture.
pub async fn restore_open_generations(
    db: &crate::storage::LocalDb,
    archive: &BrowserNetworkArchive,
) -> Result<usize, String> {
    let browsers = crate::browsers::list_running_browsers(db).await?;
    for browser in &browsers {
        archive.activate(&browser.id, &browser.webview_label);
    }
    Ok(browsers.len())
}

fn is_valid_request_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 160
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~'))
}

/// Sanitize one captured request/response against the shared structural policy.
///
/// Captured page traffic is untrusted third-party data, so this is the one
/// caller that runs the sanitizer in `ExactAndStructural` mode: over-redacting
/// a header here costs nothing, while under-redacting hands a page's bearer
/// token to whoever reads the archive.
fn sanitize_record(record: &mut BrowserNetworkRecord, policy: &RedactionPolicy) {
    let mut sanitizer = Sanitizer::structural(policy);
    record.method = record
        .method
        .chars()
        .take(32)
        .collect::<String>()
        .to_uppercase();
    record.url = sanitizer.url(&record.url);
    record.request_headers = sanitizer.headers(&record.request_headers);
    record.response_headers = sanitizer.headers(&record.response_headers);
    sanitize_body(&mut record.request_body, policy, &mut sanitizer);
    sanitize_body(&mut record.response_body, policy, &mut sanitizer);
    if let Some(error) = &mut record.error {
        sanitizer.text_in_place(error);
        truncate_string(error, MAX_ERROR_CHARS);
    }
    if let Some(url) = &mut record.redirect.final_url {
        *url = sanitizer.url(url);
    }
    if let Some(url) = &mut record.initiator.document_url {
        *url = sanitizer.url(url);
    }
    if let Some(stack) = &mut record.initiator.stack {
        sanitizer.text_in_place(stack);
        truncate_string(stack, MAX_STACK_CHARS);
    }
}

fn sanitize_body(body: &mut CapturedBody, policy: &RedactionPolicy, sanitizer: &mut Sanitizer<'_>) {
    match body {
        CapturedBody::Json { value, .. } => sanitizer.json(value),
        CapturedBody::Text { text, .. } => sanitizer.text_in_place(text),
        CapturedBody::Form { fields, .. } => {
            fields.truncate(256);
            for field in fields {
                field.name = field.name.chars().take(256).collect();
                if let Some(value) = &mut field.value {
                    if policy.is_sensitive(&field.name) {
                        *value = crate::security::REDACTED.to_string();
                    } else {
                        sanitizer.text_in_place(value);
                    }
                }
                if let Some(file) = &mut field.file {
                    file.name = file.name.chars().take(512).collect();
                    if let Some(mime) = &mut file.mime_type {
                        truncate_string(mime, 256);
                    }
                }
            }
        }
        CapturedBody::BinaryOmitted { mime_type, .. } => {
            if let Some(mime) = mime_type {
                truncate_string(mime, 256);
            }
        }
        CapturedBody::Unsupported { description }
        | CapturedBody::Unavailable {
            reason: description,
        } => {
            if let Some(description) = description {
                sanitizer.text_in_place(description);
                truncate_string(description, 1024);
            }
        }
        CapturedBody::CrossOriginOmitted => {}
    }
}

fn bound_body(body: &mut CapturedBody, max_bytes: usize) {
    match body {
        CapturedBody::Text {
            text,
            truncated,
            original_size,
        } => {
            let actual = text.len();
            if actual > max_bytes {
                truncate_utf8(text, max_bytes);
                *truncated = true;
                original_size.get_or_insert(actual as u64);
            }
        }
        CapturedBody::Json {
            value,
            truncated,
            original_size,
        } => {
            let encoded = serde_json::to_string(value).unwrap_or_default();
            if encoded.len() > max_bytes {
                let encoded_size = encoded.len() as u64;
                let mut text = encoded;
                truncate_utf8(&mut text, max_bytes);
                *value = Value::String(text);
                *truncated = true;
                original_size.get_or_insert(encoded_size);
            }
        }
        CapturedBody::Form {
            fields,
            truncated,
            original_size,
        } => {
            let actual = serde_json::to_vec(fields)
                .map(|value| value.len())
                .unwrap_or(0);
            while serde_json::to_vec(fields)
                .map(|value| value.len())
                .unwrap_or(0)
                > max_bytes
            {
                if fields.pop().is_none() {
                    break;
                }
                *truncated = true;
            }
            if *truncated {
                original_size.get_or_insert(actual as u64);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> BrowserNetworkRecord {
        BrowserNetworkRecord {
            id: id.to_string(),
            ts: 1,
            method: "post".to_string(),
            url: "https://example.test/x?token=raw&ok=yes".to_string(),
            kind: Some("fetch".to_string()),
            request_headers: vec![(
                "Authorization".to_string(),
                "Bearer raw-secret-token".to_string(),
            )],
            request_body: CapturedBody::Json {
                value: serde_json::json!({"nested": {"PaSsWoRd": "raw"}, "safe": "ok"}),
                truncated: false,
                original_size: None,
            },
            status: Some(200),
            response_headers: Vec::new(),
            response_body: CapturedBody::Text {
                text: "access_token=raw-value".to_string(),
                truncated: false,
                original_size: None,
            },
            error: None,
            timing: NetworkTiming {
                total_ms: Some(12.0),
                ..Default::default()
            },
            redirect: RedirectMetadata::default(),
            initiator: InitiatorMetadata::default(),
        }
    }

    #[test]
    fn sanitizes_default_and_configured_names_recursively() {
        let archive = BrowserNetworkArchive::default();
        let policy = RedactionPolicy::new(["tenantCode".to_string()]);
        let mut value = record("realm-1");
        value.request_body = CapturedBody::Json {
            value: serde_json::json!({"nested": {"tenantCode": "raw", "TOKEN": "raw"}}),
            truncated: false,
            original_size: None,
        };
        archive.insert("browser", value, &policy).unwrap();
        let stored = archive.get("browser", "realm-1").unwrap();
        assert!(!serde_json::to_string(&stored).unwrap().contains("raw"));
        assert!(stored.url.contains("%5BREDACTED%5D"));
    }

    #[test]
    fn duplicate_ids_are_rejected_and_survivors_keep_ids() {
        let archive = BrowserNetworkArchive::default();
        let policy = RedactionPolicy::default();
        archive
            .insert("browser", record("realm-1"), &policy)
            .unwrap();
        assert_eq!(
            archive.insert("browser", record("realm-1"), &policy),
            Err(ArchiveError::DuplicateId)
        );
        assert_eq!(archive.list("browser", None)[0].id, "realm-1");
    }

    #[test]
    fn bounds_text_bodies_and_marks_truncation() {
        let archive = BrowserNetworkArchive::default();
        let policy = RedactionPolicy::default();
        let mut value = record("realm-2");
        value.response_body = CapturedBody::Text {
            text: "x".repeat(MAX_RESPONSE_BODY_BYTES + 10),
            truncated: false,
            original_size: None,
        };
        let summary = archive.insert("browser", value, &policy).unwrap();
        assert!(summary.truncated);
        let stored = archive.get("browser", "realm-2").unwrap();
        let CapturedBody::Text {
            text,
            original_size,
            ..
        } = stored.response_body
        else {
            panic!()
        };
        assert_eq!(text.len(), MAX_RESPONSE_BODY_BYTES);
        assert_eq!(original_size, Some((MAX_RESPONSE_BODY_BYTES + 10) as u64));
    }

    #[test]
    fn close_and_reopen_atomically_reject_old_generation_inserts() {
        use std::sync::{Arc, Barrier};

        let archive = Arc::new(BrowserNetworkArchive::default());
        archive.activate("browser", "old-generation");
        let barrier = Arc::new(Barrier::new(17));
        let mut threads = Vec::new();
        for index in 0..16 {
            let archive = archive.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                let _ = archive.insert_json_for_generation(
                    "browser",
                    "old-generation",
                    &serde_json::to_string(&record(&format!("old-{index}"))).unwrap(),
                    &RedactionPolicy::default(),
                );
            }));
        }
        barrier.wait();
        archive.clear("browser");
        archive.activate("browser", "new-generation");
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(archive.list("browser", None).is_empty());
        assert_eq!(
            archive.insert_json_for_generation(
                "browser",
                "old-generation",
                &serde_json::to_string(&record("late-old")).unwrap(),
                &RedactionPolicy::default(),
            ),
            Err(ArchiveError::ExpiredGeneration)
        );
    }

    #[test]
    fn clear_expires_handles() {
        let archive = BrowserNetworkArchive::default();
        archive
            .insert("browser", record("realm-3"), &RedactionPolicy::default())
            .unwrap();
        archive.clear("browser");
        assert!(archive.get("browser", "realm-3").is_none());
    }

    #[test]
    fn evicts_oldest_records_by_count_without_renumbering_survivors() {
        let archive = BrowserNetworkArchive::default();
        for index in 0..=MAX_RECORDS_PER_BROWSER {
            archive
                .insert(
                    "browser",
                    record(&format!("realm-{index}")),
                    &RedactionPolicy::default(),
                )
                .unwrap();
        }
        let entries = archive.list("browser", None);
        assert_eq!(entries.len(), MAX_RECORDS_PER_BROWSER);
        assert_eq!(entries.first().unwrap().id, "realm-1");
        assert_eq!(
            entries.last().unwrap().id,
            format!("realm-{MAX_RECORDS_PER_BROWSER}")
        );
    }

    #[test]
    fn evicts_oldest_records_when_aggregate_bytes_are_exceeded() {
        let archive = BrowserNetworkArchive::default();
        for index in 0..150 {
            let mut value = record(&format!("large-{index}"));
            value.response_body = CapturedBody::Text {
                text: "x".repeat(MAX_RESPONSE_BODY_BYTES),
                truncated: false,
                original_size: None,
            };
            archive
                .insert("browser", value, &RedactionPolicy::default())
                .unwrap();
        }
        let entries = archive.list("browser", None);
        assert!(entries.len() < 150);
        assert!(archive.get("browser", "large-0").is_none());
        assert!(archive.get("browser", "large-149").is_some());
    }
}
