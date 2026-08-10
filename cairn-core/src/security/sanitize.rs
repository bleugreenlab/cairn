//! The one sanitizer. Exact registered values first, structural heuristics only
//! where a false positive is free.
//!
//! This is the structural policy that shipped for browser network capture
//! (CAIRN-2692), lifted out of `browser_network` and given an exact-value stage
//! in front of it. Browser capture now calls it with the same behaviour it had
//! before; the model and transcript crossings call it in [`SanitizeMode::ExactOnly`].
//!
//! Ordering matters and is fixed here: **sanitize, then bound**. Truncating
//! first would leave a partial credential in the kept prefix and hide the rest
//! from the scrubber; truncating after means every byte was inspected.

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use regex::Regex;
use serde_json::Value;

use super::registry::{registry, Detections, RegistrySnapshot};
use super::REDACTED;

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

/// Field, header, and query-parameter names whose *values* are redacted
/// wholesale under [`SanitizeMode::ExactAndStructural`].
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

    pub fn is_sensitive(&self, name: &str) -> bool {
        let normalized = normalize_name(name);
        self.sensitive_names.contains(&normalized)
            || normalized.ends_with("token")
            || normalized.ends_with("secret")
            || normalized.ends_with("password")
            || normalized.ends_with("sessionid")
            || normalized.ends_with("apikey")
    }
}

/// The redaction policy in force for `config_dir`, rebuilt only when the
/// settings file changes.
///
/// Building a policy reads and YAML-parses the whole settings file and
/// re-normalizes every sensitive name. That cost belongs to a settings edit,
/// not to a captured request, so the built policy is cached behind the settings
/// file's modification stamp — which, unlike a save hook, also notices an edit
/// made outside this process. The cache holds a single entry because a process
/// serves exactly one config directory; a second directory (only tests do this)
/// simply rebuilds.
pub fn redaction_policy(config_dir: &Path) -> Arc<RedactionPolicy> {
    let stamp = crate::config::settings::settings_file_stamp(config_dir);
    let mut cache = REDACTION_POLICY
        .lock()
        .expect("redaction policy cache poisoned");
    if let Some(cached) = cache.as_ref() {
        if cached.config_dir == config_dir && cached.stamp == stamp {
            return Arc::clone(&cached.policy);
        }
    }
    let policy = Arc::new(RedactionPolicy::new(
        crate::config::settings::load_browser_network_sensitive_names(config_dir),
    ));
    *cache = Some(CachedRedactionPolicy {
        config_dir: config_dir.to_path_buf(),
        stamp,
        policy: Arc::clone(&policy),
    });
    policy
}

static REDACTION_POLICY: Mutex<Option<CachedRedactionPolicy>> = Mutex::new(None);

struct CachedRedactionPolicy {
    config_dir: PathBuf,
    stamp: Option<(SystemTime, u64)>,
    policy: Arc<RedactionPolicy>,
}

fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// How aggressively to sanitize. See the module docs on why the choice is not
/// "always the strongest one".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SanitizeMode {
    /// Replace only values a producer registered, plus their bounded derived
    /// forms. Safe for the agent's own observed work, where a false positive is
    /// silent corruption of legitimate content.
    ExactOnly,
    /// Exact values, then field/header/URL/shape heuristics. For untrusted
    /// third-party payloads where over-redacting costs nothing.
    ExactAndStructural,
}

/// One sanitization pass, carrying the registry view it started from and the
/// non-secret detections it accumulated.
///
/// A `Sanitizer` takes its registry snapshot once at construction, so a whole
/// record is sanitized against one consistent set of registered values even
/// while another thread registers or releases one.
pub struct Sanitizer<'a> {
    snapshot: Arc<RegistrySnapshot>,
    policy: Option<&'a RedactionPolicy>,
    found: Detections,
}

impl Sanitizer<'static> {
    /// Exact-value sanitization against the process registry. The mode used at
    /// the model, transcript, and live-event crossings.
    pub fn exact() -> Self {
        Self {
            snapshot: registry().snapshot(),
            policy: None,
            found: Detections::new(),
        }
    }
}

impl<'a> Sanitizer<'a> {
    /// Exact values plus the structural policy. For untrusted third-party
    /// payloads (browser network capture).
    pub fn structural(policy: &'a RedactionPolicy) -> Self {
        Self {
            snapshot: registry().snapshot(),
            policy: Some(policy),
            found: Detections::new(),
        }
    }

    /// Build against an explicit registry view. Tests use this to exercise the
    /// sanitizer without touching process-global state.
    pub fn with_snapshot(
        snapshot: Arc<RegistrySnapshot>,
        policy: Option<&'a RedactionPolicy>,
    ) -> Self {
        Self {
            snapshot,
            policy,
            found: Detections::new(),
        }
    }

    pub fn mode(&self) -> SanitizeMode {
        match self.policy {
            Some(_) => SanitizeMode::ExactAndStructural,
            None => SanitizeMode::ExactOnly,
        }
    }

    /// Whether this pass can possibly change anything. An exact-only pass over
    /// an empty registry is a no-op, and every guarded crossing checks this
    /// before walking a payload.
    pub fn is_noop(&self) -> bool {
        self.policy.is_none() && self.snapshot.is_empty()
    }

    pub fn detections(&self) -> &Detections {
        &self.found
    }

    pub fn into_detections(self) -> Detections {
        self.found
    }

    /// Sanitize a string, borrowing it back unchanged when nothing matched.
    pub fn text<'t>(&mut self, text: &'t str) -> Cow<'t, str> {
        let base = self.exact_text(text);
        if self.policy.is_none() {
            return base;
        }
        match redact_unstructured(&base, &mut self.found) {
            Some(redacted) => Cow::Owned(redacted),
            None => base,
        }
    }

    /// Registered exact values only, regardless of mode.
    ///
    /// Used where structural heuristics would corrupt an already-structured
    /// value — notably a rendered URL, whose `token=…` query shape the assignment
    /// heuristic would collapse into a single marker after the query pairs were
    /// already handled individually.
    fn exact_text<'t>(&mut self, text: &'t str) -> Cow<'t, str> {
        self.snapshot
            .scrub_str(text, &mut self.found)
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(text))
    }

    pub fn text_in_place(&mut self, text: &mut String) {
        if let Cow::Owned(replaced) = self.text(text.as_str()) {
            *text = replaced;
        }
    }

    pub fn opt_text_in_place(&mut self, text: &mut Option<String>) {
        if let Some(text) = text.as_mut() {
            self.text_in_place(text);
        }
    }

    /// Recursively sanitize a JSON value in place.
    ///
    /// Object keys are inspected only in structural mode: a key is metadata
    /// about the value, and redacting by key name is exactly the heuristic that
    /// must not fire on the agent's own content.
    pub fn json(&mut self, value: &mut Value) {
        match value {
            Value::Object(map) => {
                for (key, child) in map.iter_mut() {
                    if self
                        .policy
                        .is_some_and(|policy| policy.is_sensitive(key.as_str()))
                    {
                        self.found.record_structural();
                        *child = Value::String(REDACTED.to_string());
                    } else {
                        self.json(child);
                    }
                }
            }
            Value::Array(values) => values.iter_mut().for_each(|value| self.json(value)),
            Value::String(text) => self.text_in_place(text),
            _ => {}
        }
    }

    /// Sanitize a URL: userinfo, fragment, and sensitive query values.
    ///
    /// A URL that will not parse is treated as unstructured text rather than
    /// passed through, so a malformed credential-bearing string is not a bypass.
    pub fn url(&mut self, raw: &str) -> String {
        let Ok(mut url) = reqwest::Url::parse(raw) else {
            return self.text(raw).into_owned();
        };
        let structural = self.policy.is_some();
        if structural {
            if !url.username().is_empty() {
                self.found.record_structural();
                let _ = url.set_username(REDACTED);
            }
            if url.password().is_some() {
                self.found.record_structural();
                let _ = url.set_password(Some(REDACTED));
            }
            // URL fragments are client-only and frequently carry OAuth
            // credentials. They are not required to identify the request.
            url.set_fragment(None);
        }
        let pairs = url
            .query_pairs()
            .map(|(name, value)| {
                let value = if self
                    .policy
                    .is_some_and(|policy| policy.is_sensitive(name.as_ref()))
                {
                    self.found.record_structural();
                    REDACTED.to_string()
                } else {
                    self.text(value.as_ref()).into_owned()
                };
                (name.into_owned(), value)
            })
            .collect::<Vec<_>>();
        url.set_query(None);
        if !pairs.is_empty() {
            url.query_pairs_mut().extend_pairs(pairs);
        }
        let rendered = url.to_string();
        // The host and path can still literally contain a registered credential.
        // Exact-only: the query component was already handled pair by pair, and
        // re-running the structural heuristics over the assembled string would
        // swallow the whole query.
        self.exact_text(&rendered).into_owned()
    }

    /// Sanitize header pairs, bounding names and values after sanitization.
    pub fn headers(&mut self, headers: &[(String, String)]) -> Vec<(String, String)> {
        headers
            .iter()
            .take(MAX_HEADERS)
            .map(|(name, value)| {
                let clean_name = name.chars().take(MAX_HEADER_NAME_CHARS).collect::<String>();
                let clean_value = if self
                    .policy
                    .is_some_and(|policy| policy.is_sensitive(name.as_str()))
                {
                    self.found.record_structural();
                    REDACTED.to_string()
                } else {
                    let mut value = self.text(value.as_str()).into_owned();
                    truncate_string(&mut value, MAX_HEADER_VALUE_CHARS);
                    value
                };
                (clean_name, clean_value)
            })
            .collect()
    }
}

const MAX_HEADERS: usize = 128;
const MAX_HEADER_NAME_CHARS: usize = 256;
const MAX_HEADER_VALUE_CHARS: usize = 8192;

/// Shaped-credential heuristics: `Bearer`/`Basic` blobs, JWTs, and
/// `name=value` assignments to a sensitive-looking name.
///
/// These patterns are compiled once. Rebuilding them per call (as the original
/// browser implementation did) put three regex compilations on the path of
/// every captured header, body string, and stack frame.
fn redact_unstructured(text: &str, found: &mut Detections) -> Option<String> {
    static BEARER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}").expect("valid regex")
    });
    static JWT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
            .expect("valid regex")
    });
    static ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?i)\b(token|secret|password|api[_-]?key|access[_-]?token|refresh[_-]?token)\s*[:=]\s*[^\s,;&]+",
        )
        .expect("valid regex")
    });

    let mut changed = false;
    let mut out = Cow::Borrowed(text);
    for pattern in [&*BEARER, &*JWT, &*ASSIGNMENT] {
        if !pattern.is_match(&out) {
            continue;
        }
        changed = true;
        found.record_structural();
        out = Cow::Owned(pattern.replace_all(&out, REDACTED).into_owned());
    }
    changed.then(|| out.into_owned())
}

/// Truncate to a character count. Bounding runs *after* sanitization; see the
/// module docs.
pub fn truncate_string(value: &mut String, max_chars: usize) {
    if value.chars().count() > max_chars {
        *value = value.chars().take(max_chars).collect();
    }
}

/// Truncate to a byte count without splitting a character.
pub fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::registry::SecretRegistry;
    use crate::security::secret::{SecretCategory, SecretId, SecretMaterial};

    const SECRET: &str = "sk-live-Qa9Zm2Xp7Lr4";

    fn with_secret() -> SecretRegistry {
        let registry = SecretRegistry::new();
        registry
            .register(
                SecretId::new("test"),
                SecretCategory::CallbackCredential,
                "unit test",
                SecretMaterial::from_string(SECRET.to_string()),
            )
            .expect("registerable")
            .retain_for_process();
        registry
    }

    fn exact(registry: &SecretRegistry) -> Sanitizer<'static> {
        Sanitizer::with_snapshot(registry.snapshot(), None)
    }

    #[test]
    fn exact_mode_leaves_ordinary_content_alone() {
        let registry = with_secret();
        let mut sanitizer = exact(&registry);
        // Shaped like a credential, but not a registered one: exact mode must
        // not touch the agent's own file content.
        let text = "api_key: ${LINEAR_KEY}\nBearer abcdefghijkl";
        assert_eq!(sanitizer.text(text), Cow::Borrowed(text));
        assert!(sanitizer.detections().is_empty());
    }

    #[test]
    fn structural_mode_redacts_shapes_and_sensitive_names() {
        let registry = with_secret();
        let policy = RedactionPolicy::default();
        let mut sanitizer = Sanitizer::with_snapshot(registry.snapshot(), Some(&policy));
        assert_eq!(
            sanitizer.text("Authorization: Bearer abcdefghijklmnop"),
            "Authorization: [REDACTED]"
        );
        let mut value = serde_json::json!({"nested": {"PaSsWoRd": "raw"}, "safe": "ok"});
        sanitizer.json(&mut value);
        assert_eq!(value["nested"]["PaSsWoRd"], serde_json::json!(REDACTED));
        assert_eq!(value["safe"], serde_json::json!("ok"));
    }

    #[test]
    fn exact_values_are_removed_from_every_json_position() {
        let registry = with_secret();
        let mut sanitizer = exact(&registry);
        let mut value = serde_json::json!({
            "plain": SECRET,
            "nested": [{"deep": format!("prefix {SECRET} suffix")}],
            "unrelated": "9f1c2b8e4d6a0f3b5c7e9a1d2f4b6c8e",
        });
        sanitizer.json(&mut value);
        let rendered = serde_json::to_string(&value).unwrap();
        assert!(!rendered.contains(SECRET));
        assert!(rendered.contains("9f1c2b8e4d6a0f3b5c7e9a1d2f4b6c8e"));
        assert!(sanitizer.detections().has_exact());
    }

    #[test]
    fn urls_lose_userinfo_fragments_and_registered_values() {
        let registry = with_secret();
        let policy = RedactionPolicy::default();
        let mut sanitizer = Sanitizer::with_snapshot(registry.snapshot(), Some(&policy));
        let sanitized =
            sanitizer.url("https://user:password@example.test/path?safe=yes#access_token=frag");
        assert!(!sanitized.contains("user"));
        assert!(!sanitized.contains("password"));
        assert!(!sanitized.contains("frag"));
        assert!(!sanitized.contains('#'));
        assert!(sanitized.contains("safe=yes"));

        let mut sanitizer = exact(&registry);
        let sanitized = sanitizer.url(&format!("https://example.test/p?q={SECRET}"));
        assert!(!sanitized.contains(SECRET));
    }

    #[test]
    fn false_positive_guards_hold_for_hashes_signatures_and_ids() {
        let registry = SecretRegistry::new();
        let policy = RedactionPolicy::default();
        let mut sanitizer = Sanitizer::with_snapshot(registry.snapshot(), Some(&policy));
        for value in [
            "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2",
            "f7d6a7a84a958f847a91f491cdb9908192b9a338",
            "MEUCIQDx9k2mQpS1w7Yq3nJ5rTt8vHb0aWcZgLkM2NpQeRfAiEA",
            "01J8ZK5T7X9WQ2N4M6P8R0S2T4",
            "the quick brown fox jumps over the lazy dog",
        ] {
            assert_eq!(sanitizer.text(value), Cow::Borrowed(value), "{value}");
        }
    }

    #[test]
    fn redaction_policy_is_reused_until_the_settings_file_changes() {
        let config_dir = tempfile::tempdir().unwrap();
        let settings = config_dir.path().join("settings.yaml");
        std::fs::write(&settings, "browserNetworkSensitiveNames:\n  - x-tenant\n").unwrap();

        let first = redaction_policy(config_dir.path());
        let second = redaction_policy(config_dir.path());
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged settings file must not rebuild the policy"
        );
        assert!(first.is_sensitive("x-tenant"));

        // An edit made OUTSIDE this process is still picked up, because the
        // cache keys on the file's modification stamp rather than on a save
        // hook. The replacement is the same length as the original, so only the
        // modification time can distinguish the two.
        std::fs::write(&settings, "browserNetworkSensitiveNames:\n  - x-region\n").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&settings)
            .unwrap()
            .set_modified(SystemTime::now() + std::time::Duration::from_secs(5))
            .unwrap();

        let rebuilt = redaction_policy(config_dir.path());
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert!(rebuilt.is_sensitive("x-region"));
        assert!(!rebuilt.is_sensitive("x-tenant"));
    }

    #[test]
    fn sanitization_runs_before_bounding() {
        let registry = with_secret();
        let mut sanitizer = exact(&registry);
        // A credential sitting past the bound must still be removed, which is
        // only true if sanitization sees the whole value first.
        let mut text = format!("{}{SECRET}", "x".repeat(64));
        sanitizer.text_in_place(&mut text);
        truncate_utf8(&mut text, 4096);
        assert!(!text.contains(SECRET));
        assert!(text.contains(REDACTED));
    }
}
