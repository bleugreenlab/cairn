use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_RECORDS_PER_BROWSER: usize = 500;
const MAX_BYTES_PER_BROWSER: usize = 8 * 1024 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 32 * 1024;
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
const MAX_CAPTURE_PAYLOAD_BYTES: usize = 256 * 1024;
const REDACTED: &str = "[REDACTED]";

const BUILTIN_SENSITIVE_NAMES: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "apikey",
    "api_key",
    "x-api-key",
    "password",
    "passwd",
    "secret",
    "client-secret",
    "client_secret",
    "session",
    "session-id",
    "session_id",
    "access-token",
    "access_token",
    "refresh-token",
    "refresh_token",
    "id-token",
    "id_token",
    "token",
];

#[derive(Debug, Clone)]
pub struct RedactionPolicy {
    sensitive_names: HashSet<String>,
}

impl Default for RedactionPolicy {
    fn default() -> Self {
        Self::new(std::iter::empty())
    }
}

impl RedactionPolicy {
    pub fn new(extra_names: impl IntoIterator<Item = String>) -> Self {
        let mut sensitive_names = BUILTIN_SENSITIVE_NAMES
            .iter()
            .map(|name| normalize_name(name))
            .collect::<HashSet<_>>();
        sensitive_names.extend(extra_names.into_iter().map(|name| normalize_name(&name)));
        Self { sensitive_names }
    }

    fn is_sensitive(&self, name: &str) -> bool {
        let normalized = normalize_name(name);
        self.sensitive_names.contains(&normalized)
            || normalized.ends_with("token")
            || normalized.ends_with("secret")
            || normalized.ends_with("password")
            || normalized.ends_with("sessionid")
            || normalized.ends_with("apikey")
    }
}

fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

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

fn sanitize_record(record: &mut BrowserNetworkRecord, policy: &RedactionPolicy) {
    record.method = record
        .method
        .chars()
        .take(32)
        .collect::<String>()
        .to_uppercase();
    record.url = sanitize_url(&record.url, policy);
    record.request_headers = sanitize_headers(&record.request_headers, policy);
    record.response_headers = sanitize_headers(&record.response_headers, policy);
    sanitize_body(&mut record.request_body, policy);
    sanitize_body(&mut record.response_body, policy);
    if let Some(error) = &mut record.error {
        *error = redact_unstructured(error);
        truncate_string(error, 4096);
    }
    if let Some(url) = &mut record.redirect.final_url {
        *url = sanitize_url(url, policy);
    }
    if let Some(url) = &mut record.initiator.document_url {
        *url = sanitize_url(url, policy);
    }
    if let Some(stack) = &mut record.initiator.stack {
        *stack = redact_unstructured(stack);
        truncate_string(stack, 8192);
    }
}

fn sanitize_url(raw: &str, policy: &RedactionPolicy) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        return redact_unstructured(raw);
    };
    if !url.username().is_empty() {
        let _ = url.set_username(REDACTED);
    }
    if url.password().is_some() {
        let _ = url.set_password(Some(REDACTED));
    }
    // URL fragments are client-only and frequently contain OAuth credentials.
    // They are not required to identify the network request, so omit them.
    url.set_fragment(None);
    let pairs = url
        .query_pairs()
        .map(|(name, value)| {
            let value = if policy.is_sensitive(&name) {
                REDACTED.to_string()
            } else {
                redact_unstructured(&value)
            };
            (name.into_owned(), value)
        })
        .collect::<Vec<_>>();
    url.set_query(None);
    if !pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(pairs);
    }
    url.to_string()
}

fn sanitize_headers(
    headers: &[(String, String)],
    policy: &RedactionPolicy,
) -> Vec<(String, String)> {
    headers
        .iter()
        .take(128)
        .map(|(name, value)| {
            let clean_name = name.chars().take(256).collect::<String>();
            let clean_value = if policy.is_sensitive(name) {
                REDACTED.to_string()
            } else {
                let mut value = redact_unstructured(value);
                truncate_string(&mut value, 8192);
                value
            };
            (clean_name, clean_value)
        })
        .collect()
}

fn sanitize_body(body: &mut CapturedBody, policy: &RedactionPolicy) {
    match body {
        CapturedBody::Json { value, .. } => redact_json(value, policy),
        CapturedBody::Text { text, .. } => *text = redact_unstructured(text),
        CapturedBody::Form { fields, .. } => {
            fields.truncate(256);
            for field in fields {
                field.name = field.name.chars().take(256).collect();
                if let Some(value) = &mut field.value {
                    *value = if policy.is_sensitive(&field.name) {
                        REDACTED.to_string()
                    } else {
                        redact_unstructured(value)
                    };
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
                *description = redact_unstructured(description);
                truncate_string(description, 1024);
            }
        }
        CapturedBody::CrossOriginOmitted => {}
    }
}

fn redact_json(value: &mut Value, policy: &RedactionPolicy) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if policy.is_sensitive(key) {
                    *value = Value::String(REDACTED.to_string());
                } else {
                    redact_json(value, policy);
                }
            }
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_json(value, policy)),
        Value::String(text) => *text = redact_unstructured(text),
        _ => {}
    }
}

fn redact_unstructured(text: &str) -> String {
    let bearer = Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}").expect("valid regex");
    let jwt = Regex::new(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
        .expect("valid regex");
    let assignment = Regex::new(r"(?i)\b(token|secret|password|api[_-]?key|access[_-]?token|refresh[_-]?token)\s*[:=]\s*[^\s,;&]+" ).expect("valid regex");
    let text = bearer.replace_all(text, REDACTED);
    let text = jwt.replace_all(&text, REDACTED);
    assignment.replace_all(&text, REDACTED).into_owned()
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

fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

fn truncate_string(value: &mut String, max_chars: usize) {
    if value.chars().count() > max_chars {
        *value = value.chars().take(max_chars).collect();
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
    fn strips_url_userinfo_and_fragments_at_the_host_boundary() {
        let policy = RedactionPolicy::default();
        let sanitized = sanitize_url(
            "https://user:password@example.test/path?safe=yes#access_token=fragment-secret",
            &policy,
        );
        assert!(!sanitized.contains("user"));
        assert!(!sanitized.contains("password"));
        assert!(!sanitized.contains("fragment-secret"));
        assert!(!sanitized.contains('#'));
        assert!(sanitized.contains("safe=yes"));
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
