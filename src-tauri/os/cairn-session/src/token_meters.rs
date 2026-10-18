//! Model-aware token meters used by core transcript and context accounting.
//!
//! Three meters exist. Claude's tokenizer is unpublished, so Claude text is
//! counted by `cairn-tokenize`'s estimator; OpenAI's o200k is exact through
//! `tiktoken-rs`; and an open-weight model whose tokenizer Cairn pins
//! (`cairn_models::types::tokenizers`) is counted exactly by its own tokenizer
//! through toks ([`ExactTokenMeter`]).
//!
//! Which tokenizer a model uses is catalog knowledge: discovery attaches a
//! [`TokenizerRef`] to each offer it can pin, and the orchestrator publishes
//! those into this module's offer index ([`set_offer_tokenizers`]) whenever a
//! provider's catalog is stored. Selection asks the index, then the static
//! `(provider, model)` pairs on the pins, and otherwise keeps the estimating
//! rules it always had.
//!
//! An exact selection never blocks or fails a read. The first sighting of a
//! tokenizer starts one background load (fetch the pinned files, verify them,
//! load them into toks) and the read is counted by the estimating meter the
//! old rules would have picked; reads switch to the exact meter once it is
//! loaded. A fetch failure retries after a backoff; a digest mismatch or a toks
//! refusal leaves that tokenizer on the estimating meter for the life of the
//! process. Either is logged once per tokenizer.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant};

use cairn_common::token_meter::{Cut, TokenMeter};
use cairn_models::tokenizer_store::{PinnedSource, TokenizerStore};
use cairn_models::types::catalog::DiscoveredModel;
use cairn_models::types::tokenizers::{self, TokenizerRef};
use cairn_tokenize::Family;
use tiktoken_rs::{o200k_base, CoreBPE};

use crate::agent_process::process::AgentProcessState;
use cairn_db::storage::{DbResult, RowExt};
use cairn_db::turso::{params, Connection};

pub struct ClaudeTokenMeter;
pub struct O200kTokenMeter;

pub static CLAUDE_TOKEN_METER: ClaudeTokenMeter = ClaudeTokenMeter;
pub static O200K_TOKEN_METER: O200kTokenMeter = O200kTokenMeter;

impl TokenMeter for ClaudeTokenMeter {
    fn count(&self, text: &str) -> u32 {
        cairn_tokenize::count(text, Family::V5)
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        let cut = cairn_tokenize::cut(text, budget, Family::V5);
        Cut {
            byte_offset: cut.byte_offset,
            tokens: cut.tokens,
        }
    }
}

fn o200k() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| o200k_base().expect("o200k_base BPE initializes"))
}

impl TokenMeter for O200kTokenMeter {
    fn count(&self, text: &str) -> u32 {
        o200k()
            .encode_ordinary(text)
            .len()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        let tokens = o200k().encode_ordinary(text);
        if tokens.len() <= budget as usize {
            return Cut {
                byte_offset: text.len(),
                tokens: tokens.len() as u32,
            };
        }
        let token_bytes: Vec<usize> = o200k()
            ._decode_native_and_split(tokens)
            .map(|bytes| bytes.len())
            .collect();
        let start = token_bytes
            .iter()
            .take(budget.saturating_sub(1) as usize)
            .sum::<usize>();
        let end = token_bytes
            .iter()
            .take(budget as usize + 1)
            .sum::<usize>()
            .min(text.len());
        let mut best = Cut {
            byte_offset: 0,
            tokens: 0,
        };
        for byte_offset in text
            .char_indices()
            .map(|(at, _)| at)
            .chain(std::iter::once(text.len()))
            .filter(|at| *at >= start && *at <= end)
        {
            let count = self.count(&text[..byte_offset]);
            if count <= budget && byte_offset >= best.byte_offset {
                best = Cut {
                    byte_offset,
                    tokens: count,
                };
            }
        }
        best
    }
}

/// A meter over a model's own tokenizer, through toks. Built only by the
/// registry, from bytes verified against a pin.
pub struct ExactTokenMeter {
    tokenizer: cairn_toks::Tokenizer,
}

impl TokenMeter for ExactTokenMeter {
    // toks fails only on a text past its 512 MiB per-call limit; such a text
    // is measured by o200k, the nearest exact byte-level tokenizer, rather
    // than reported as zero.
    fn count(&self, text: &str) -> u32 {
        self.tokenizer
            .count(text)
            .unwrap_or_else(|_| O200K_TOKEN_METER.count(text))
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        match self.tokenizer.cut(text, budget) {
            Ok(cut) => Cut {
                byte_offset: cut.byte_offset,
                tokens: cut.tokens,
            },
            Err(_) => O200K_TOKEN_METER.cut(text, budget),
        }
    }
}

/// Which tokenizer a backend/model pair routes to.
///
/// The routing decision is a value rather than only the `&'static dyn
/// TokenMeter` it selects, because a trait object cannot answer "which meter is
/// this". The estimating meters are zero-sized statics, so every `&dyn
/// TokenMeter` to them shares one data address; and the vtable half of the
/// pointer is no identity either, since the compiler may emit a separate vtable
/// per codegen unit, so two coercions of the same static can compare unequal.
/// Comparing the returned references is therefore meaningless in both
/// directions, and naming the decision is what makes the routing table
/// assertable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MeterKind {
    Claude,
    O200k,
    /// The pinned tokenizer with this digest, and the estimating meter that
    /// counts until it is loaded (or for good, if it cannot be).
    Exact {
        digest: [u8; 32],
        fallback: Estimate,
    },
}

/// The meters that need no tokenizer file, which the old routing rules pick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Estimate {
    Claude,
    O200k,
}

impl From<Estimate> for MeterKind {
    fn from(estimate: Estimate) -> Self {
        match estimate {
            Estimate::Claude => MeterKind::Claude,
            Estimate::O200k => MeterKind::O200k,
        }
    }
}

impl MeterKind {
    fn meter(self) -> &'static dyn TokenMeter {
        match self {
            MeterKind::Claude => &CLAUDE_TOKEN_METER,
            MeterKind::O200k => &O200K_TOKEN_METER,
            MeterKind::Exact { digest, fallback } => registry()
                .meter(digest)
                .unwrap_or_else(|| MeterKind::from(fallback).meter()),
        }
    }
}

/// The routing table: a pinned tokenizer when the offer has one, else the
/// estimating rules. A selection that knows only the backend never has an
/// offer, so it keeps the estimating rules exactly.
fn meter_kind_for_backend_model(backend: Option<&str>, model: Option<&str>) -> MeterKind {
    let fallback = estimate_for_backend_model(backend, model);
    if let (Some(backend), Some(model)) = (backend, model) {
        if let Some(tokenizer) = tokenizer_for_offer(backend, model) {
            return MeterKind::Exact {
                digest: tokenizer.sha256,
                fallback,
            };
        }
    }
    fallback.into()
}

/// The estimating rules. Missing and unrecognized backends retain Cairn's
/// Claude-default semantics.
fn estimate_for_backend_model(backend: Option<&str>, model: Option<&str>) -> Estimate {
    if let Some(model) = model.map(str::to_ascii_lowercase) {
        if model.contains("claude") {
            return Estimate::Claude;
        }
        let model_name = model.rsplit('/').next().unwrap_or(&model);
        let reasoning_family = ["o1", "o3", "o4"]
            .iter()
            .any(|family| model_name == *family || model_name.starts_with(&format!("{family}-")));
        if model.contains("gpt-") || model.contains("codex") || reasoning_family {
            return Estimate::O200k;
        }
    }

    match backend
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("codex" | "openai") => Estimate::O200k,
        _ => Estimate::Claude,
    }
}

/// Offer tokenizers by provider key, then by the model's native id and its
/// catalog id. A cache of catalog state with one writer,
/// [`set_offer_tokenizers`].
type OfferIndex = HashMap<String, HashMap<String, TokenizerRef>>;

fn offer_index() -> &'static RwLock<OfferIndex> {
    static INDEX: OnceLock<RwLock<OfferIndex>> = OnceLock::new();
    INDEX.get_or_init(Default::default)
}

/// Publish a provider's catalog into the offer index, replacing what that
/// provider published before. Call it wherever a provider's catalog is
/// stored, so the index never drifts from the catalog.
pub fn set_offer_tokenizers(provider: &str, models: &[DiscoveredModel]) {
    let mut offers = HashMap::new();
    for model in models {
        if let Some(tokenizer) = &model.tokenizer {
            offers.insert(model.model.clone(), tokenizer.clone());
            offers.insert(model.id.clone(), tokenizer.clone());
        }
    }
    let provider = provider.trim().to_ascii_lowercase();
    let mut index = offer_index()
        .write()
        .unwrap_or_else(PoisonError::into_inner);
    if offers.is_empty() {
        index.remove(&provider);
    } else {
        index.insert(provider, offers);
    }
}

fn tokenizer_for_offer(backend: &str, model: &str) -> Option<TokenizerRef> {
    let provider = backend.trim().to_ascii_lowercase();
    let published = offer_index()
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&provider)
        .and_then(|offers| offers.get(model))
        .cloned();
    // The pins' static pairs cover a provider before its first catalog
    // refresh, and runs recorded before this process started.
    published.or_else(|| tokenizers::tokenizer_for(&provider, model, None))
}

/// Where a pinned tokenizer comes from. The registry's only dependency, so
/// tests can load fixtures without a network.
trait TokenizerSource: Send + Sync {
    fn load(&self, digest: &[u8; 32]) -> Result<cairn_toks::Tokenizer, LoadFailure>;
}

#[derive(Debug)]
struct LoadFailure {
    reason: String,
    /// Whether a later attempt could succeed: a failed download, not a
    /// refusal or a mismatch.
    retryable: bool,
}

impl LoadFailure {
    fn terminal(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retryable: false,
        }
    }
}

/// The shipped pins, fetched through the verified store.
struct PinnedTokenizers {
    store: TokenizerStore,
}

impl TokenizerSource for PinnedTokenizers {
    fn load(&self, digest: &[u8; 32]) -> Result<cairn_toks::Tokenizer, LoadFailure> {
        if !cairn_toks::NATIVE {
            // Checked before fetching: there is nothing to load the files into.
            return Err(LoadFailure::terminal("toks has no build for this target"));
        }
        let pin = tokenizers::pin_for_digest(digest)
            .ok_or_else(|| LoadFailure::terminal("no shipped pin has this digest"))?;
        let source = self.store.ensure(pin).map_err(|error| LoadFailure {
            reason: error.to_string(),
            retryable: error.is_retryable(),
        })?;
        let tokenizer = match source {
            PinnedSource::Bytes(bytes) => cairn_toks::Tokenizer::from_json(&bytes),
            // The snapshot holds exactly the pin's verified files and lives
            // until this load returns.
            PinnedSource::Dir(snapshot) => cairn_toks::Tokenizer::from_dir(snapshot.path()),
        }
        .map_err(|error| LoadFailure::terminal(error.to_string()))?;
        // A second check that toks parsed the verified buffer. A directory's
        // digest is its manifest's, which toks does not compute.
        if !pin.is_directory() && tokenizer.source_sha256() != *digest {
            return Err(LoadFailure::terminal(
                "toks reports a different source digest",
            ));
        }
        Ok(tokenizer)
    }
}

enum Entry {
    Loading,
    /// Leaked: bounded by the number of pins, and the meter API hands out
    /// `&'static` references.
    Ready(&'static ExactTokenMeter),
    Unavailable {
        retry_at: Option<Instant>,
    },
}

struct Registry {
    source: Box<dyn TokenizerSource>,
    retry_after: Duration,
    entries: Mutex<HashMap<[u8; 32], Entry>>,
    warned: Mutex<HashSet<[u8; 32]>>,
    loads: AtomicUsize,
    warnings: AtomicUsize,
}

impl Registry {
    fn new(source: Box<dyn TokenizerSource>, retry_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            source,
            retry_after,
            entries: Mutex::new(HashMap::new()),
            warned: Mutex::new(HashSet::new()),
            loads: AtomicUsize::new(0),
            warnings: AtomicUsize::new(0),
        })
    }

    /// The exact meter if it is loaded. Never blocks on a load: an absent or
    /// retry-due tokenizer starts one in the background and answers `None`.
    fn meter(self: &Arc<Self>, digest: [u8; 32]) -> Option<&'static dyn TokenMeter> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        match entries.get(&digest) {
            Some(Entry::Ready(meter)) => return Some(*meter),
            Some(Entry::Loading) => return None,
            Some(Entry::Unavailable {
                retry_at: Some(retry_at),
            }) if Instant::now() >= *retry_at => {}
            Some(Entry::Unavailable { .. }) => return None,
            None => {}
        }
        entries.insert(digest, Entry::Loading);
        drop(entries);

        let registry = self.clone();
        let spawned = std::thread::Builder::new()
            .name("token-meter-load".to_string())
            .spawn(move || {
                registry.loads.fetch_add(1, Ordering::SeqCst);
                let loaded = registry.source.load(&digest);
                registry.complete(digest, loaded);
            });
        if let Err(error) = spawned {
            self.complete(
                digest,
                Err(LoadFailure {
                    reason: format!("could not start the load: {error}"),
                    retryable: true,
                }),
            );
        }
        None
    }

    fn complete(&self, digest: [u8; 32], loaded: Result<cairn_toks::Tokenizer, LoadFailure>) {
        let entry = match loaded {
            Ok(tokenizer) => {
                log::info!(
                    "exact token meter ready for tokenizer {} ({})",
                    describe(&digest),
                    tokenizer.name()
                );
                Entry::Ready(Box::leak(Box::new(ExactTokenMeter { tokenizer })))
            }
            Err(failure) => {
                self.report(&digest, &failure);
                Entry::Unavailable {
                    retry_at: failure.retryable.then(|| Instant::now() + self.retry_after),
                }
            }
        };
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(digest, entry);
    }

    fn report(&self, digest: &[u8; 32], failure: &LoadFailure) {
        let first = self
            .warned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(*digest);
        let what = describe(digest);
        let reason = &failure.reason;
        if !first {
            log::debug!("exact token meter for tokenizer {what} still unavailable: {reason}");
        } else if cairn_toks::NATIVE {
            self.warnings.fetch_add(1, Ordering::SeqCst);
            log::warn!("exact token meter for tokenizer {what} unavailable, counting with the estimating meter: {reason}");
        } else {
            self.warnings.fetch_add(1, Ordering::SeqCst);
            log::info!("exact token meters are not built for this target; tokenizer {what} uses the estimating meter");
        }
    }
}

fn describe(digest: &[u8; 32]) -> String {
    let hex: String = digest
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    match tokenizers::pin_for_digest(digest) {
        Some(pin) => format!("{} {}… ({})", pin.family, hex, pin.repo),
        None => format!("{hex}…"),
    }
}

/// How long a failed download waits before the next read retries it.
const FETCH_RETRY_AFTER: Duration = Duration::from_secs(10 * 60);

fn registry() -> &'static Arc<Registry> {
    static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let root: PathBuf = cairn_common::paths::cairn_home()
            .join("models")
            .join("tokenizers");
        Registry::new(
            Box::new(PinnedTokenizers {
                store: TokenizerStore::new(root),
            }),
            FETCH_RETRY_AFTER,
        )
    })
}

/// Select a meter from durable or explicitly supplied backend/model metadata.
pub fn meter_for_backend_model(
    backend: Option<&str>,
    model: Option<&str>,
) -> &'static dyn TokenMeter {
    meter_kind_for_backend_model(backend, model).meter()
}

/// Select the meter for a live process. The run id is the registry boundary:
/// an absent process deliberately falls through to the Claude default.
pub fn meter_for_process(
    process_state: &AgentProcessState,
    run_id: &str,
) -> &'static dyn TokenMeter {
    let backend = process_state.get_backend(run_id);
    let model = process_state.get_model(run_id);
    meter_for_backend_model(backend.as_deref(), model.as_deref())
}

/// Resolve the persisted identity for ingestion and historical backfills, where
/// the live process registry is intentionally not in scope. Backend identity is
/// canonical on `sessions`; the requested model is retained on the owning job.
pub async fn meter_for_run_conn(
    conn: &Connection,
    run_id: &str,
) -> DbResult<&'static dyn TokenMeter> {
    let mut rows = conn
        .query(
            "SELECT s.backend, j.model
             FROM runs r
             LEFT JOIN sessions s ON s.id = r.session_id
             LEFT JOIN jobs j ON j.id = r.job_id
             WHERE r.id = ?1
             LIMIT 1",
            params![run_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(meter_for_backend_model(None, None));
    };
    let backend = row.opt_text(0)?;
    let model = row.opt_text(1)?;
    Ok(meter_for_backend_model(
        backend.as_deref(),
        model.as_deref(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Text the two tokenizers disagree about, so a meter can be identified by
    /// what it does. The disagreement is asserted before it is relied on.
    const DIVERGENT: &str =
        "Tokenizer divergence probe: \u{2726} \u{3b1}\u{3b2}\u{3b3} 1234567890, \
         snake_case::path/to/thing, \u{65e5}\u{672c}\u{8a9e}";

    #[test]
    fn backend_selection_defaults_unknown_to_claude() {
        assert_eq!(meter_kind_for_backend_model(None, None), MeterKind::Claude);
        for backend in ["openrouter", "ollama"] {
            assert_eq!(
                meter_kind_for_backend_model(Some(backend), Some("auto")),
                MeterKind::Claude
            );
        }
        assert_eq!(
            meter_kind_for_backend_model(Some("future-backend"), None),
            MeterKind::Claude
        );
    }

    #[test]
    fn compatible_backends_use_o200k_unless_model_is_claude() {
        for backend in ["codex", "openai"] {
            assert_eq!(
                meter_kind_for_backend_model(Some(backend), None),
                MeterKind::O200k
            );
        }
        assert_eq!(
            meter_kind_for_backend_model(Some("openrouter"), Some("anthropic/claude-sonnet-4-5")),
            MeterKind::Claude
        );
        for model in [
            "openai/o3-mini",
            "openai/o4-mini",
            "openai/o1-preview",
            "o3",
        ] {
            assert_eq!(
                meter_kind_for_backend_model(Some("openrouter"), Some(model)),
                MeterKind::O200k
            );
        }
    }

    /// The routing table above is only worth pinning if the public selector
    /// hands back the meter its decision names, so this crosses the two by
    /// behavior — the one identity a trait object here can actually carry.
    #[test]
    fn the_public_selector_returns_the_meter_its_decision_names() {
        let claude = CLAUDE_TOKEN_METER.count(DIVERGENT);
        let o200k = O200K_TOKEN_METER.count(DIVERGENT);
        assert_ne!(
            claude, o200k,
            "the probe text must tell the two tokenizers apart"
        );

        for (backend, model) in [
            (None, None),
            (Some("openrouter"), Some("anthropic/claude-sonnet-4-5")),
            (Some("codex"), None),
            (Some("openrouter"), Some("openai/o3-mini")),
        ] {
            let expected = match meter_kind_for_backend_model(backend, model) {
                MeterKind::Claude => claude,
                MeterKind::O200k => o200k,
                MeterKind::Exact { .. } => panic!("no pinned tokenizer for {model:?}"),
            };
            assert_eq!(
                meter_for_backend_model(backend, model).count(DIVERGENT),
                expected,
                "backend {backend:?} model {model:?}"
            );
        }
    }

    #[test]
    fn adapters_delegate_count_and_cut_to_their_tokenizers() {
        let text = "hello, tokenizer";
        assert_eq!(
            CLAUDE_TOKEN_METER.count(text),
            cairn_tokenize::count(text, Family::V5)
        );
        assert_eq!(O200K_TOKEN_METER.cut("aaaaaaaa", 1).byte_offset, 8);
        let cut = O200K_TOKEN_METER.cut(text, 2);
        assert!(cut.tokens <= 2);
        assert!(text.is_char_boundary(cut.byte_offset));
        assert_eq!(
            cut.tokens,
            O200K_TOKEN_METER.count(&text[..cut.byte_offset])
        );
    }

    fn digest_of(family: &str) -> [u8; 32] {
        tokenizers::pins()
            .iter()
            .find(|pin| pin.family == family)
            .unwrap()
            .digest_bytes()
    }

    fn offer(id: &str, tokenizer: Option<TokenizerRef>) -> DiscoveredModel {
        let mut model = cairn_models::types::catalog::claude_models().remove(0);
        model.id = format!("catalog/{id}");
        model.model = id.to_string();
        model.tokenizer = tokenizer;
        model
    }

    #[test]
    fn shipped_opencode_go_presets_route_to_their_own_tokenizers() {
        for (model, family) in [
            ("kimi-k3", "kimi-k3"),
            ("glm-5.2", "glm-5"),
            ("mimo-v2.5", "mimo-v2.5"),
        ] {
            assert_eq!(
                meter_kind_for_backend_model(Some("opencode-go"), Some(model)),
                MeterKind::Exact {
                    digest: digest_of(family),
                    fallback: Estimate::Claude,
                },
                "{model}"
            );
        }
    }

    #[test]
    fn a_backend_alone_never_selects_an_exact_tokenizer() {
        assert_eq!(
            meter_kind_for_backend_model(Some("opencode-go"), None),
            MeterKind::Claude
        );
        assert_eq!(
            meter_kind_for_backend_model(None, Some("kimi-k3")),
            MeterKind::Claude
        );
        // A tier name is not a model id.
        assert_eq!(
            meter_kind_for_backend_model(Some("opencode-go"), Some("lg")),
            MeterKind::Claude
        );
    }

    #[test]
    fn published_catalog_tokenizers_route_by_native_and_catalog_id() {
        // A provider key no other test publishes, since the index is global.
        let provider = "custom:acc_index_test";
        let gemma = tokenizers::tokenizer_for("x", "y", Some("google/gemma-4-26B-A4B-it")).unwrap();
        set_offer_tokenizers(
            provider,
            &[
                offer("gemma-4-26b", Some(gemma.clone())),
                offer("plain", None),
            ],
        );
        let exact = MeterKind::Exact {
            digest: gemma.sha256,
            fallback: Estimate::Claude,
        };
        assert_eq!(
            meter_kind_for_backend_model(Some(provider), Some("gemma-4-26b")),
            exact
        );
        assert_eq!(
            meter_kind_for_backend_model(
                Some("CUSTOM:acc_index_test"),
                Some("catalog/gemma-4-26b")
            ),
            exact
        );
        assert_eq!(
            meter_kind_for_backend_model(Some(provider), Some("plain")),
            MeterKind::Claude
        );

        // Republishing replaces the provider's entries; nothing lingers.
        set_offer_tokenizers(provider, &[offer("gemma-4-26b", None)]);
        assert_eq!(
            meter_kind_for_backend_model(Some(provider), Some("gemma-4-26b")),
            MeterKind::Claude
        );
    }

    #[test]
    fn an_exact_selection_keeps_the_estimate_its_backend_would_use() {
        let provider = "openai-compatible-index-test";
        let glm = tokenizers::tokenizer_for("x", "y", Some("zai-org/GLM-5.3")).unwrap();
        set_offer_tokenizers(provider, &[offer("glm-codex-tuned", Some(glm.clone()))]);
        assert_eq!(
            meter_kind_for_backend_model(Some(provider), Some("glm-codex-tuned")),
            MeterKind::Exact {
                digest: glm.sha256,
                fallback: Estimate::O200k,
            }
        );
        set_offer_tokenizers(provider, &[]);
    }

    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../cairn-toks/fixtures/bytelevel.json"
    );

    /// Loads the cairn-toks fixture for every digest, or fails as told.
    struct FixtureSource {
        failure: Option<(String, bool)>,
    }

    impl TokenizerSource for FixtureSource {
        fn load(&self, _digest: &[u8; 32]) -> Result<cairn_toks::Tokenizer, LoadFailure> {
            if let Some((reason, retryable)) = &self.failure {
                return Err(LoadFailure {
                    reason: reason.clone(),
                    retryable: *retryable,
                });
            }
            let bytes = std::fs::read(FIXTURE).unwrap();
            cairn_toks::Tokenizer::from_json(&bytes)
                .map_err(|error| LoadFailure::terminal(error.to_string()))
        }
    }

    fn settle(registry: &Registry, loads: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while registry.loads.load(Ordering::SeqCst) < loads
            || matches!(
                registry.entries.lock().unwrap().values().next(),
                Some(Entry::Loading)
            )
        {
            assert!(
                Instant::now() < deadline,
                "the background load never finished"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn the_registry_answers_the_fallback_until_the_exact_meter_loads() {
        let registry = Registry::new(Box::new(FixtureSource { failure: None }), FETCH_RETRY_AFTER);
        let digest = [7u8; 32];
        assert!(
            registry.meter(digest).is_none(),
            "the first sighting never blocks"
        );
        settle(&registry, 1);
        let meter = registry.meter(digest);
        if cairn_toks::NATIVE {
            let meter = meter.expect("the fixture loads");
            let tokenizer =
                cairn_toks::Tokenizer::from_json(&std::fs::read(FIXTURE).unwrap()).unwrap();
            let text = "The quick brown fox jumps over the lazy dog.";
            assert_eq!(meter.count(text), tokenizer.count(text).unwrap());
            assert_eq!(registry.warnings.load(Ordering::SeqCst), 0);
        } else {
            assert!(
                meter.is_none(),
                "a stub target stays on the estimating meter"
            );
            assert_eq!(registry.warnings.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            registry.loads.load(Ordering::SeqCst),
            1,
            "one load per tokenizer"
        );
    }

    #[test]
    fn a_terminal_failure_falls_back_for_good_and_logs_once() {
        for reason in [
            "tokenizer.json is not the pinned file",
            "toks refused the tokenizer (-3): model dropout (not 0.0)",
            "toks has no build for this target",
        ] {
            let registry = Registry::new(
                Box::new(FixtureSource {
                    failure: Some((reason.to_string(), false)),
                }),
                Duration::ZERO,
            );
            let digest = [9u8; 32];
            assert!(registry.meter(digest).is_none());
            settle(&registry, 1);
            for _ in 0..5 {
                assert!(registry.meter(digest).is_none(), "{reason}");
            }
            assert_eq!(
                registry.loads.load(Ordering::SeqCst),
                1,
                "{reason}: never retried"
            );
            assert_eq!(
                registry.warnings.load(Ordering::SeqCst),
                1,
                "{reason}: logged once"
            );
        }
    }

    #[test]
    fn a_failed_download_is_retried_after_the_backoff_but_logged_once() {
        let registry = Registry::new(
            Box::new(FixtureSource {
                failure: Some(("tokenizer download failed: offline".to_string(), true)),
            }),
            Duration::ZERO,
        );
        let digest = [3u8; 32];
        assert!(registry.meter(digest).is_none());
        settle(&registry, 1);
        assert!(
            registry.meter(digest).is_none(),
            "a due retry starts another load"
        );
        settle(&registry, 2);
        assert_eq!(registry.loads.load(Ordering::SeqCst), 2);
        assert_eq!(registry.warnings.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exact_and_o200k_meters_keep_the_contract() {
        let texts = [
            "",
            "hello, tokenizer",
            DIVERGENT,
            "fn main() {\n    println!(\"hi\");\n}\n",
            "\u{65e5}\u{672c}\u{8a9e} cafe\u{301} \u{1f600}  spaces\t\ttabs",
        ];
        let mut meters: Vec<Box<dyn TokenMeter>> = vec![Box::new(O200kTokenMeter)];
        if cairn_toks::NATIVE {
            let tokenizer =
                cairn_toks::Tokenizer::from_json(&std::fs::read(FIXTURE).unwrap()).unwrap();
            meters.push(Box::new(ExactTokenMeter { tokenizer }));
        }
        for meter in &meters {
            for text in texts {
                let total = meter.count(text);
                for budget in [0, 1, total / 2, total.saturating_sub(1), total, total + 3] {
                    cairn_common::token_meter::assert_meter_contract(meter.as_ref(), text, budget);
                }
            }
        }
    }
}
