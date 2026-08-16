//! The process-local registry of credentials whose plaintext must not appear in
//! observed output.
//!
//! A credential producer registers a resolved value *before* injecting or using
//! it and holds the returned [`SecretGuard`] for as long as any output carrying
//! that value can still cross a guarded boundary. Registration stores derived
//! forms for matching; it never persists plaintext anywhere durable.
//!
//! The registry is process-local by design. A registered value is only useful
//! for scrubbing output produced by *this* process, and shipping the set of
//! live credentials anywhere would be the exact disclosure this module exists to
//! prevent.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use zeroize::Zeroizing;

use super::secret::{MatchRule, SecretCategory, SecretId, SecretMaterial};

/// Why a value was refused registration.
///
/// Refusal is loud rather than silent: a producer that thinks it registered a
/// credential and did not would believe in a guarantee it does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RegistrationRefused {
    /// The value is empty.
    #[error("secret value is empty")]
    Empty,
    /// The value is too short or too repetitive to scrub for without generating
    /// false positives. See `secret::MIN_REGISTERABLE_LEN`.
    #[error("secret value is below the length/variety threshold for safe scrubbing")]
    BelowThreshold,
}

/// Non-secret description of one registration, for diagnostics and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretMetadata {
    pub id: SecretId,
    pub category: SecretCategory,
    /// Where the value came from, as a producer-chosen label — never the value.
    pub provenance: String,
    /// Unix seconds after which the value is known stale, when the producer
    /// knows. Advisory: expiry does not unregister, because output produced
    /// before expiry can still be crossing a boundary after it.
    pub expires_at: Option<i64>,
    /// How many live guards hold this registration.
    pub holders: usize,
}

/// One recognized byte form of one registered credential.
struct Needle {
    id: SecretId,
    category: SecretCategory,
    rule: MatchRule,
    bytes: Zeroizing<Vec<u8>>,
}

/// An immutable view of the registry that scrub operations run against.
///
/// Scrubbing happens on hot paths and concurrently with registration, so
/// callers take an `Arc` of this rather than holding a lock across a scan.
pub struct RegistrySnapshot {
    needles: Vec<Needle>,
    max_len: usize,
    alphabet: ByteSet,
}

/// Which byte values occur in any registered form.
///
/// Coarse by construction and process-local: it says nothing about order,
/// length, or which credential a byte came from, and it is never serialized.
/// It exists so a streaming scrubber can answer "could a match still be forming
/// at the end of this buffer?" from the bytes it already has.
#[derive(Clone, Copy)]
struct ByteSet([bool; 256]);

impl ByteSet {
    fn empty() -> Self {
        Self([false; 256])
    }

    fn insert(&mut self, byte: u8) {
        self.0[byte as usize] = true;
    }

    fn contains(&self, byte: u8) -> bool {
        self.0[byte as usize]
    }
}

impl RegistrySnapshot {
    fn empty() -> Self {
        Self {
            needles: Vec::new(),
            max_len: 0,
            alphabet: ByteSet::empty(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.needles.is_empty()
    }

    /// Length of the longest recognized form. A streaming scrubber must withhold
    /// at least `max_len - 1` bytes to catch a value split across chunks.
    pub fn max_needle_len(&self) -> usize {
        self.max_len
    }

    /// Length of the longest suffix of `buf` that is a *proper prefix* of some
    /// registered form, capped at `cap`.
    ///
    /// This is exactly the run that could still be the head of a value whose
    /// tail is in the next chunk: a match spanning a chunk boundary has its head
    /// as a suffix of what is held and a prefix of the form it belongs to.
    /// Everything before it is settled and can go out now, which is what lets a
    /// streaming scrubber stay usable on a live terminal.
    ///
    /// The alphabet is a pre-filter, not the answer. A prefix of a form ends on
    /// one of that form's own bytes, so a final byte outside the alphabet rules
    /// out every length at once and costs one lookup. Answering from the
    /// alphabet *alone* is far too weak to be a release rule: the callback
    /// credential registers as 32 raw random bytes, and its derived percent and
    /// base64 forms between them cover most of printable ASCII, so "is this byte
    /// in the alphabet" is nearly always yes. Under that rule a terminal
    /// withholds ordinary keystroke echo and the shell looks frozen — the exact
    /// failure this release rule exists to avoid.
    pub fn unsettled_suffix_len(&self, buf: &[u8], cap: usize) -> usize {
        let Some(last) = buf.last() else {
            return 0;
        };
        if !self.alphabet.contains(*last) {
            return 0;
        }
        let mut longest = 0;
        for needle in &self.needles {
            // Only a *proper* prefix is unsettled. A complete match was already
            // replaced by the scrub pass over this same buffer.
            let limit = cap.min(needle.bytes.len().saturating_sub(1)).min(buf.len());
            for length in (longest + 1..=limit).rev() {
                if buf[buf.len() - length..] == needle.bytes[..length] {
                    longest = length;
                    break;
                }
            }
        }
        longest
    }

    /// Replace every registered form in `buf`, recording what matched.
    ///
    /// Returns `None` when nothing matched, so callers on hot paths avoid an
    /// allocation in the overwhelmingly common case.
    pub fn scrub_bytes(&self, buf: &[u8], found: &mut Detections) -> Option<Vec<u8>> {
        if self.needles.is_empty() || buf.is_empty() {
            return None;
        }

        // Collect EVERY match of EVERY needle before replacing anything.
        //
        // Replacing as we scan cannot be made correct. Consuming a match
        // advances the cursor past it, so a different registered value that
        // starts inside that match but extends past its end is never looked
        // for again and its tail is emitted in plaintext. Taking the earliest
        // match, or the longest at a given offset, narrows that window without
        // closing it, because the two values need not share a start offset at
        // all. Union of covered spans is the only formulation where no byte of
        // any registered value can survive.
        let mut spans: Vec<(usize, usize, &Needle)> = Vec::new();
        for needle in &self.needles {
            let len = needle.bytes.len();
            let mut from = 0usize;
            while from + len <= buf.len() {
                let Some(offset) = memchr::memmem::find(&buf[from..], &needle.bytes) else {
                    break;
                };
                let at = from + offset;
                spans.push((at, at + len, needle));
                // `at + 1`, not `at + len`: a needle with an internal period
                // can match itself overlapping, and both occurrences must join
                // one covered span rather than leaving a plaintext fragment
                // between them.
                from = at + 1;
            }
        }
        if spans.is_empty() {
            return None;
        }
        // Ascending by start, longest first on a tie, so a single left-to-right
        // pass can union them.
        spans.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));

        let mut out = Vec::with_capacity(buf.len() + super::REDACTION_MARKER_BYTES.len());
        let mut emitted = 0usize;
        let mut covered: Option<(usize, usize)> = None;
        let flush = |out: &mut Vec<u8>, emitted: &mut usize, start: usize, end: usize| {
            out.extend_from_slice(&buf[*emitted..start]);
            out.extend_from_slice(super::REDACTION_MARKER_BYTES);
            *emitted = end;
        };
        for (start, end, needle) in spans {
            found.record(&needle.id, needle.category, needle.rule);
            covered = Some(match covered {
                // Overlaps (or is contained by) the span being built: widen it,
                // so a value straddling two others is replaced whole.
                Some((open, close)) if start < close => (open, close.max(end)),
                Some((open, close)) => {
                    flush(&mut out, &mut emitted, open, close);
                    (start, end)
                }
                None => (start, end),
            });
        }
        if let Some((open, close)) = covered {
            flush(&mut out, &mut emitted, open, close);
        }
        out.extend_from_slice(&buf[emitted..]);
        Some(out)
    }

    /// Replace every registered form in `text`.
    ///
    /// Every recognized form is either the credential's own valid UTF-8 bytes or
    /// pure ASCII, and UTF-8 is self-synchronizing, so a match can only begin and
    /// end on a character boundary. The result is therefore still valid UTF-8;
    /// the defensive fallback fails closed rather than trusting that argument.
    pub fn scrub_str(&self, text: &str, found: &mut Detections) -> Option<String> {
        let scrubbed = self.scrub_bytes(text.as_bytes(), found)?;
        match String::from_utf8(scrubbed) {
            Ok(text) => Some(text),
            Err(_) => Some(super::REDACTED.to_string()),
        }
    }
}

/// One aggregated match from a scrub pass.
///
/// Non-secret by construction: identity, rule, and count only — never the value
/// and never the surrounding context, which would leak the value's neighbours.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Detection {
    /// The registered credential that matched. `None` for a structural
    /// heuristic, which by definition matched a *shape*, not a known value.
    pub secret_id: Option<SecretId>,
    pub category: Option<SecretCategory>,
    pub rule: MatchRule,
    pub count: usize,
}

/// What a scrub pass matched, aggregated per secret and rule.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Detections {
    entries: Vec<Detection>,
}

impl Detections {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn record(&mut self, id: &SecretId, category: SecretCategory, rule: MatchRule) {
        self.bump(Some(id.clone()), Some(category), rule);
    }

    /// Record a structural (non-exact) match, which has no registered identity.
    pub fn record_structural(&mut self) {
        self.bump(None, None, MatchRule::Structural);
    }

    fn bump(
        &mut self,
        secret_id: Option<SecretId>,
        category: Option<SecretCategory>,
        rule: MatchRule,
    ) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.secret_id == secret_id && entry.rule == rule)
        {
            entry.count += 1;
            return;
        }
        self.entries.push(Detection {
            secret_id,
            category,
            rule,
            count: 1,
        });
    }

    pub fn entries(&self) -> &[Detection] {
        &self.entries
    }

    /// Whether any *registered exact value* matched. Structural heuristics do
    /// not count: they are advisory and must never reject authored input.
    pub fn has_exact(&self) -> bool {
        self.entries.iter().any(|entry| entry.rule.is_exact())
    }
}

struct Entry {
    category: SecretCategory,
    provenance: String,
    expires_at: Option<i64>,
    holders: usize,
    forms: Vec<(MatchRule, Zeroizing<Vec<u8>>)>,
}

/// The process-local secret registry.
pub struct SecretRegistry {
    entries: Mutex<HashMap<SecretId, Entry>>,
    snapshot: RwLock<Arc<RegistrySnapshot>>,
    /// Lock-free fast path: scrubbing an empty registry must not pay for a lock,
    /// because every guarded crossing consults it on every call.
    live: AtomicUsize,
}

impl Default for SecretRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            snapshot: RwLock::new(Arc::new(RegistrySnapshot::empty())),
            live: AtomicUsize::new(0),
        }
    }

    /// Whether anything is registered. Cheap enough to call per crossing.
    pub fn is_empty(&self) -> bool {
        self.live.load(Ordering::Acquire) == 0
    }

    /// Take an immutable view for a scrub pass.
    pub fn snapshot(&self) -> Arc<RegistrySnapshot> {
        self.snapshot
            .read()
            .expect("secret registry snapshot poisoned")
            .clone()
    }

    /// Register a resolved credential, returning the guard that keeps it live.
    ///
    /// Registering the same id twice takes a second hold on it rather than
    /// replacing it, so two overlapping users of one credential cannot
    /// unregister each other's protection. The forms are recomputed from the
    /// newest material, which is what a rotated value needs.
    pub fn register(
        &self,
        id: SecretId,
        category: SecretCategory,
        provenance: impl Into<String>,
        material: SecretMaterial,
    ) -> Result<SecretGuard<'_>, RegistrationRefused> {
        if material.is_empty() {
            return Err(RegistrationRefused::Empty);
        }
        if !material.is_scrubbable() {
            return Err(RegistrationRefused::BelowThreshold);
        }
        let forms = material.derived_forms().into_vec();
        {
            let mut entries = self.entries.lock().expect("secret registry poisoned");
            match entries.get_mut(&id) {
                Some(entry) => {
                    entry.holders += 1;
                    entry.forms = forms;
                }
                None => {
                    entries.insert(
                        id.clone(),
                        Entry {
                            category,
                            provenance: provenance.into(),
                            expires_at: None,
                            holders: 1,
                            forms,
                        },
                    );
                }
            }
            self.rebuild(&entries);
        }
        Ok(SecretGuard {
            registry: self,
            id,
            release_on_drop: true,
        })
    }

    /// Record an advisory expiry for an already-registered secret.
    pub fn set_expiry(&self, id: &SecretId, expires_at: Option<i64>) {
        let mut entries = self.entries.lock().expect("secret registry poisoned");
        if let Some(entry) = entries.get_mut(id) {
            entry.expires_at = expires_at;
        }
    }

    /// Non-secret inventory of what is registered.
    pub fn metadata(&self) -> Vec<SecretMetadata> {
        let entries = self.entries.lock().expect("secret registry poisoned");
        let mut out: Vec<SecretMetadata> = entries
            .iter()
            .map(|(id, entry)| SecretMetadata {
                id: id.clone(),
                category: entry.category,
                provenance: entry.provenance.clone(),
                expires_at: entry.expires_at,
                holders: entry.holders,
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    fn release(&self, id: &SecretId) {
        let mut entries = self.entries.lock().expect("secret registry poisoned");
        let drop_entry = match entries.get_mut(id) {
            Some(entry) => {
                entry.holders = entry.holders.saturating_sub(1);
                entry.holders == 0
            }
            None => false,
        };
        if drop_entry {
            entries.remove(id);
        }
        self.rebuild(&entries);
    }

    /// Rebuild the scan view. Called under the entries lock so a concurrent
    /// register/release pair cannot publish a snapshot that reflects neither.
    fn rebuild(&self, entries: &HashMap<SecretId, Entry>) {
        let mut needles: Vec<Needle> = Vec::new();
        for (id, entry) in entries {
            for (rule, bytes) in &entry.forms {
                needles.push(Needle {
                    id: id.clone(),
                    category: entry.category,
                    rule: *rule,
                    bytes: bytes.clone(),
                });
            }
        }
        // Longest first: a value that contains another registered value must be
        // replaced whole, never left as a redacted fragment plus plaintext tail.
        needles.sort_by(|a, b| {
            b.bytes
                .len()
                .cmp(&a.bytes.len())
                .then_with(|| a.id.cmp(&b.id))
        });
        let max_len = needles
            .first()
            .map(|needle| needle.bytes.len())
            .unwrap_or(0);
        let mut alphabet = ByteSet::empty();
        for needle in &needles {
            for byte in needle.bytes.iter() {
                alphabet.insert(*byte);
            }
        }
        self.live.store(needles.len(), Ordering::Release);
        *self
            .snapshot
            .write()
            .expect("secret registry snapshot poisoned") = Arc::new(RegistrySnapshot {
            needles,
            max_len,
            alphabet,
        });
    }
}

/// The process-local registry every crossing consults.
pub fn registry() -> &'static SecretRegistry {
    static REGISTRY: OnceLock<SecretRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SecretRegistry::new)
}

/// Keeps one registration alive.
///
/// Dropping the guard releases this holder's claim; the value stops being
/// scrubbed for once the last holder drops. A producer whose credential lives
/// for the process (the MCP callback secret) calls
/// [`SecretGuard::retain_for_process`].
#[must_use = "dropping the guard immediately unregisters the secret"]
pub struct SecretGuard<'a> {
    registry: &'a SecretRegistry,
    id: SecretId,
    release_on_drop: bool,
}

impl SecretGuard<'_> {
    pub fn id(&self) -> &SecretId {
        &self.id
    }

    /// Keep this registration for the lifetime of the process.
    ///
    /// For a credential that is injected into every agent process and can appear
    /// in output at any later moment, "until the process exits" is the honest
    /// lifetime; a narrower one would unregister while output still carrying the
    /// value is in flight.
    pub fn retain_for_process(mut self) {
        self.release_on_drop = false;
    }
}

/// Holds an id and a release flag — never material — so it is safe to format.
impl std::fmt::Debug for SecretGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretGuard")
            .field("id", &self.id)
            .field("release_on_drop", &self.release_on_drop)
            .finish()
    }
}

impl Drop for SecretGuard<'_> {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.registry.release(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material(value: &str) -> SecretMaterial {
        SecretMaterial::from_string(value.to_string())
    }

    fn local() -> SecretRegistry {
        SecretRegistry::new()
    }

    #[test]
    fn refuses_values_that_would_generate_false_positives() {
        let registry = local();
        assert_eq!(
            registry
                .register(
                    SecretId::new("a"),
                    SecretCategory::ConfiguredMcp,
                    "test",
                    material("short")
                )
                .unwrap_err(),
            RegistrationRefused::BelowThreshold
        );
        assert_eq!(
            registry
                .register(
                    SecretId::new("b"),
                    SecretCategory::ConfiguredMcp,
                    "test",
                    material("")
                )
                .unwrap_err(),
            RegistrationRefused::Empty
        );
        assert!(registry.is_empty());
    }

    #[test]
    fn snapshot_scrubs_raw_and_encoded_forms() {
        let registry = local();
        let _guard = registry
            .register(
                SecretId::new("cred"),
                SecretCategory::CallbackCredential,
                "test",
                material("sk-live-9fA3xQ2mZ7/w"),
            )
            .unwrap();
        let snapshot = registry.snapshot();
        let mut found = Detections::new();
        let scrubbed = snapshot
            .scrub_str("before sk-live-9fA3xQ2mZ7/w after", &mut found)
            .unwrap();
        assert_eq!(scrubbed, "before [REDACTED] after");
        assert!(found.has_exact());

        let mut found = Detections::new();
        let percent = snapshot
            .scrub_str("u=sk-live-9fA3xQ2mZ7%2Fw&x=1", &mut found)
            .unwrap();
        assert_eq!(percent, "u=[REDACTED]&x=1");

        let mut found = Detections::new();
        assert!(snapshot.scrub_str("nothing to see", &mut found).is_none());
        assert!(found.is_empty());
    }

    #[test]
    fn overlapping_holders_do_not_unregister_each_other() {
        let registry = local();
        let first = registry
            .register(
                SecretId::new("shared"),
                SecretCategory::ConfiguredMcp,
                "one",
                material("aBcDeF0123456789"),
            )
            .unwrap();
        let second = registry
            .register(
                SecretId::new("shared"),
                SecretCategory::ConfiguredMcp,
                "two",
                material("aBcDeF0123456789"),
            )
            .unwrap();
        assert_eq!(registry.metadata()[0].holders, 2);
        drop(first);
        assert_eq!(registry.metadata()[0].holders, 1);
        assert!(!registry.is_empty());
        drop(second);
        assert!(registry.is_empty());
        assert!(registry.metadata().is_empty());
    }

    #[test]
    fn metadata_never_carries_the_value() {
        let registry = local();
        let _guard = registry
            .register(
                SecretId::new("cred"),
                SecretCategory::BatchCapability,
                "per-batch relay capability",
                material("9f8a7b6c5d4e3f21"),
            )
            .unwrap();
        let rendered = format!("{:?}", registry.metadata());
        assert!(!rendered.contains("9f8a7b6c5d4e3f21"));
        assert!(rendered.contains("per-batch relay capability"));
    }

    #[test]
    fn concurrent_registration_and_scrubbing_stay_consistent() {
        use std::sync::Barrier;

        let registry = local();
        let barrier = Arc::new(Barrier::new(8));
        std::thread::scope(|scope| {
            for index in 0..8 {
                let registry = &registry;
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let guard = registry
                        .register(
                            SecretId::new(format!("cred-{index}")),
                            SecretCategory::ConfiguredMcp,
                            "test",
                            material(&format!("value-{index}-aBcDeF0123456789")),
                        )
                        .unwrap();
                    let snapshot = registry.snapshot();
                    let mut found = Detections::new();
                    let text = format!("x value-{index}-aBcDeF0123456789 y");
                    assert_eq!(
                        snapshot.scrub_str(&text, &mut found).unwrap(),
                        "x [REDACTED] y"
                    );
                    guard.retain_for_process();
                });
            }
        });
        assert_eq!(registry.metadata().len(), 8);
    }

    /// Regression for the needle-major scan (CAIRN-3822 review finding 1).
    ///
    /// Two registered credentials, the shorter one *earlier* in the buffer. The
    /// old scan found the longer needle first, copied everything before it
    /// verbatim, and emitted the shorter credential in the clear.
    #[test]
    fn a_shorter_secret_earlier_in_the_buffer_is_not_skipped() {
        let registry = local();
        let short = "shortAAA12345";
        let long = "muchLongerSecretValue987654";
        let _a = registry
            .register(
                SecretId::new("short"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(short),
            )
            .unwrap();
        let _b = registry
            .register(
                SecretId::new("long"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(long),
            )
            .unwrap();

        let snapshot = registry.snapshot();
        let mut found = Detections::new();
        let scrubbed = snapshot
            .scrub_str(&format!("first={short} second={long}"), &mut found)
            .unwrap();
        assert_eq!(scrubbed, "first=[REDACTED] second=[REDACTED]");
        assert!(!scrubbed.contains(short));
        assert!(!scrubbed.contains(long));
    }

    /// The single-credential form of the same defect, which is the one that
    /// needs no unusual configuration: base64 is always longer than raw, so any
    /// output carrying both forms used to leak the raw one.
    #[test]
    fn one_secret_present_raw_and_encoded_loses_both() {
        use base64::Engine;

        let registry = local();
        let secret = "sk-live-Qa9Zm2Xp7Lr4";
        let _guard = registry
            .register(
                SecretId::new("both-forms"),
                SecretCategory::CallbackCredential,
                "test",
                material(secret),
            )
            .unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(secret);

        let snapshot = registry.snapshot();
        let mut found = Detections::new();
        let scrubbed = snapshot
            .scrub_str(&format!("raw={secret} enc={encoded}"), &mut found)
            .unwrap();
        assert_eq!(scrubbed, "raw=[REDACTED] enc=[REDACTED]");
        assert!(!scrubbed.contains(secret));
        assert!(!scrubbed.contains(&encoded));
    }

    /// Order-independence: the same two values in the opposite arrangement, and
    /// repeated, must scrub identically.
    #[test]
    fn many_occurrences_in_any_order_are_all_replaced() {
        let registry = local();
        let short = "shortAAA12345";
        let long = "muchLongerSecretValue987654";
        let _a = registry
            .register(
                SecretId::new("short"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(short),
            )
            .unwrap();
        let _b = registry
            .register(
                SecretId::new("long"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(long),
            )
            .unwrap();

        let snapshot = registry.snapshot();
        for text in [
            format!("{long} then {short}"),
            format!("{short}{long}"),
            format!("a {short} b {long} c {short} d"),
        ] {
            let mut found = Detections::new();
            let scrubbed = snapshot.scrub_str(&text, &mut found).unwrap();
            assert!(!scrubbed.contains(short), "leaked short in {text}");
            assert!(!scrubbed.contains(long), "leaked long in {text}");
        }
    }

    #[test]
    fn longest_form_wins_so_no_plaintext_tail_survives() {
        let registry = local();
        let _short = registry
            .register(
                SecretId::new("short"),
                SecretCategory::ConfiguredMcp,
                "test",
                material("abc123XYZ789"),
            )
            .unwrap();
        let _long = registry
            .register(
                SecretId::new("long"),
                SecretCategory::ConfiguredMcp,
                "test",
                material("abc123XYZ789-tail-9876"),
            )
            .unwrap();
        let snapshot = registry.snapshot();
        let mut found = Detections::new();
        assert_eq!(
            snapshot
                .scrub_str("v=abc123XYZ789-tail-9876;", &mut found)
                .unwrap(),
            "v=[REDACTED];"
        );
    }

    /// Two DISTINCT registered values that only partially overlap: the shorter
    /// starts earlier, the longer starts inside it and extends past its end.
    ///
    /// A scrubber that consumes the earliest match and advances past it emits
    /// the longer value's tail in the clear, because the cursor has already
    /// moved beyond that value's start offset. Unreachable while only
    /// Cairn-owned credentials were registered; reachable once a value an
    /// external party influences can become a needle (CAIRN-3825).
    #[test]
    fn partially_overlapping_distinct_secrets_are_both_covered() {
        let registry = local();
        // `earlier` ends inside `later`, and `later` starts inside `earlier`.
        let earlier = "Q7wm2ZxA-head-KKp3";
        let later = "head-KKp3-tail-Vn8s4L";
        let _a = registry
            .register(
                SecretId::new("earlier"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(earlier),
            )
            .unwrap();
        let _b = registry
            .register(
                SecretId::new("later"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(later),
            )
            .unwrap();
        let snapshot = registry.snapshot();

        let text = "x=Q7wm2ZxA-head-KKp3-tail-Vn8s4L;";
        let mut found = Detections::new();
        let scrubbed = snapshot.scrub_str(text, &mut found).unwrap();
        assert_eq!(scrubbed, "x=[REDACTED];");
        // Not just "no full value survives" — no fragment of either does.
        assert!(!scrubbed.contains("tail"), "leaked a tail: {scrubbed}");
        assert!(!scrubbed.contains("head"), "leaked a head: {scrubbed}");
        assert!(found.has_exact());
    }

    /// The union is over the whole buffer, not over one run: a covered span
    /// followed by ordinary text followed by another covered span keeps the
    /// text between them.
    #[test]
    fn json_string_form_overlaps_raw_and_preserves_detection_metadata() {
        let registry = local();
        let secret = "SYNTH-Q7\"m2Zx9-RedTeam";
        let id = SecretId::new("json-string");
        let _guard = registry
            .register(
                id.clone(),
                SecretCategory::ConfiguredMcp,
                "test",
                material(secret),
            )
            .unwrap();
        let encoded = serde_json::to_string(secret).unwrap();
        let escaped = &encoded[1..encoded.len() - 1];
        let mut found = Detections::new();
        let scrubbed = registry
            .snapshot()
            .scrub_str(&format!("raw={secret}; json={escaped}"), &mut found)
            .unwrap();

        assert_eq!(scrubbed, "raw=[REDACTED]; json=[REDACTED]");
        assert!(found.entries().iter().any(|entry| {
            entry.secret_id.as_ref() == Some(&id)
                && entry.category == Some(SecretCategory::ConfiguredMcp)
                && entry.rule == MatchRule::ExactJsonString
                && entry.count == 1
        }));
    }

    #[test]
    fn separate_matches_stay_separate_and_keep_the_text_between_them() {
        let registry = local();
        let value = "sk-live-9fA3xQ2mZ7";
        let _guard = registry
            .register(
                SecretId::new("one"),
                SecretCategory::ConfiguredMcp,
                "test",
                material(value),
            )
            .unwrap();
        let snapshot = registry.snapshot();
        let mut found = Detections::new();
        assert_eq!(
            snapshot
                .scrub_str(&format!("a={value} b={value} end"), &mut found)
                .unwrap(),
            "a=[REDACTED] b=[REDACTED] end"
        );
    }
}
