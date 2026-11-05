//! Model-aware token meters used by core transcript and context accounting.
//!
//! Three meters exist. Claude's tokenizer is unpublished, so Claude text is
//! counted by `cairn-tokenize`'s estimator; OpenAI's o200k is exact, through
//! toks over the gpt-oss copy `cairn-toks` embeds where toks is linked and
//! through `tiktoken-rs` elsewhere ([`O200kTokenMeter`]); and an open-weight
//! model whose tokenizer Cairn pins (`cairn_models::types::tokenizers`) is
//! counted exactly by its own tokenizer through toks ([`ExactTokenMeter`]).
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
use cairn_db::storage::TrackedConnection;
use cairn_db::storage::{DbResult, RowExt};
use cairn_db::turso::params;

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

impl TokenMeter for O200kTokenMeter {
    fn count(&self, text: &str) -> u32 {
        O200k::selected().count(text)
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        O200k::selected().cut(text, budget)
    }
}

/// The encoder under [`O200kTokenMeter`]. Both give exactly tiktoken's
/// `encode_ordinary` ids (the equivalence tests below hold them to it), so
/// which one counts changes the cost of a count and never its value.
#[derive(Clone, Copy)]
enum O200k {
    /// gpt-oss's o200k tokenizer through toks, about a hundred times faster.
    /// Every target toks links.
    Toks(&'static cairn_toks::Tokenizer),
    /// Targets without toks (Windows, Intel macOS), and any text past toks'
    /// 512 MiB per-call limit.
    Tiktoken,
}

impl O200k {
    fn selected() -> Self {
        static SELECTED: OnceLock<O200k> = OnceLock::new();
        *SELECTED.get_or_init(|| match cairn_toks::o200k() {
            Ok(tokenizer) => O200k::Toks(tokenizer),
            Err(cairn_toks::LoadError::UnsupportedTarget) => O200k::Tiktoken,
            // The embedded file is fixed at build time and loaded by this
            // crate's tests, so this is a broken build rather than bad input;
            // counts stay exact on tiktoken-rs.
            Err(error) => {
                log::error!("o200k counts with tiktoken-rs: {error}");
                O200k::Tiktoken
            }
        })
    }

    fn count(self, text: &str) -> u32 {
        match self {
            O200k::Toks(tokenizer) => tokenizer
                .count(text)
                .unwrap_or_else(|_| O200k::Tiktoken.count(text)),
            O200k::Tiktoken => tiktoken_o200k()
                .encode_ordinary(text)
                .len()
                .try_into()
                .unwrap_or(u32::MAX),
        }
    }

    /// The byte length of each of `text`'s tokens, in order.
    fn token_bytes(self, text: &str) -> Vec<usize> {
        match self {
            O200k::Toks(tokenizer) => match tokenizer.encode(text) {
                Ok(ids) => ids
                    .into_iter()
                    .map(|id| tokenizer.token_bytes(id).map_or(0, <[u8]>::len))
                    .collect(),
                Err(_) => O200k::Tiktoken.token_bytes(text),
            },
            O200k::Tiktoken => {
                let bpe = tiktoken_o200k();
                bpe._decode_native_and_split(bpe.encode_ordinary(text))
                    .map(|bytes| bytes.len())
                    .collect()
            }
        }
    }

    fn cut(self, text: &str, budget: u32) -> Cut {
        let token_bytes = self.token_bytes(text);
        if token_bytes.len() <= budget as usize {
            return Cut {
                byte_offset: text.len(),
                tokens: token_bytes.len() as u32,
            };
        }
        let start = token_bytes
            .iter()
            .take(budget.saturating_sub(1) as usize)
            .sum::<usize>();
        let end = token_bytes
            .iter()
            .take(budget as usize + 1)
            .sum::<usize>()
            .min(text.len());
        // Each candidate prefix is recounted from the last pre-token split at or
        // before the window rather than from the start of the text: the prefix's
        // tokens before the split are the text's own.
        let split = o200k_split_before(text, start);
        let (split, split_tokens) =
            tokens_before(&token_bytes, split).map_or((0, 0), |tokens| (split, tokens));
        let mut best = Cut {
            byte_offset: 0,
            tokens: 0,
        };
        for byte_offset in (start..=end).filter(|at| text.is_char_boundary(*at)) {
            let count = split_tokens + self.count(&text[split..byte_offset]);
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

fn tiktoken_o200k() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| o200k_base().expect("o200k_base BPE initializes"))
}

/// The last offset at or before `limit` where o200k's pre-tokenizer must split
/// `text`, or 0 when there is none. See [`o200k_must_split`] for where that is.
fn o200k_split_before(text: &str, limit: usize) -> usize {
    let mut end = limit.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    for (at, before) in text[..end].char_indices().rev() {
        let split = at + before.len_utf8();
        if o200k_must_split(before, &text[split..]) {
            return split;
        }
    }
    0
}

/// Whether every prefix of a text that reaches past the point between `before`
/// and `rest` pre-tokenizes, under the o200k pattern, as the text before that
/// point followed by `rest`'s prefix on its own.
///
/// That holds where no alternative of the pattern can run across the point,
/// because the pattern has no lookbehind and BPE never merges across
/// pre-tokens. Two such points are recognized:
///
/// - Between a non-whitespace character and whitespace other than `\r` or
///   `\n`. The word, number and punctuation alternatives end at whitespace,
///   except a punctuation run's `[\r\n/]*` tail, which takes only `\r` and `\n`.
/// - Right after a newline, when `rest` does not open with `/` and its leading
///   whitespace holds no further `\r` or `\n`. The word alternatives exclude
///   `\r\n`, a whitespace run that holds a newline matches `\s*[\r\n]+` and ends
///   at its last newline, and the punctuation tail could take only a `/`.
fn o200k_must_split(before: char, rest: &str) -> bool {
    let Some(next) = rest.chars().next() else {
        return false;
    };
    if before == '\n' {
        return next != '/'
            && rest
                .chars()
                .take_while(|ch| ch.is_whitespace())
                .all(|ch| ch != '\r' && ch != '\n');
    }
    !before.is_whitespace() && next.is_whitespace() && next != '\r' && next != '\n'
}

/// How many of a text's tokens, given by byte length, lie before `split`, or
/// `None` when `split` falls inside a token.
fn tokens_before(token_bytes: &[usize], split: usize) -> Option<u32> {
    let mut end = 0;
    for (index, bytes) in token_bytes.iter().enumerate() {
        if end >= split {
            return (end == split).then_some(index as u32);
        }
        end += bytes;
    }
    (end == split).then_some(token_bytes.len() as u32)
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
    conn: &TrackedConnection,
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

    /// Every o200k backend this target builds: tiktoken-rs always, toks where
    /// it links. The tests below hold each one to the same answers.
    fn o200k_backends() -> Vec<(&'static str, O200k)> {
        let mut backends = vec![("tiktoken", O200k::Tiktoken)];
        if cairn_toks::NATIVE {
            let toks = cairn_toks::o200k().expect("the embedded o200k tokenizer loads");
            backends.push(("toks", O200k::Toks(toks)));
        }
        backends
    }

    /// o200k's cut with every candidate prefix recounted from the start of the
    /// text: the definition the split recount must reproduce exactly.
    fn o200k_cut_by_whole_prefix(o200k: O200k, text: &str, budget: u32) -> Cut {
        let token_bytes = o200k.token_bytes(text);
        if token_bytes.len() <= budget as usize {
            return o200k.cut(text, budget);
        }
        let start: usize = token_bytes
            .iter()
            .take(budget.saturating_sub(1) as usize)
            .sum();
        let end = token_bytes
            .iter()
            .take(budget as usize + 1)
            .sum::<usize>()
            .min(text.len());
        let mut best = Cut {
            byte_offset: 0,
            tokens: 0,
        };
        for byte_offset in (start..=end).filter(|at| text.is_char_boundary(*at)) {
            let count = o200k.count(&text[..byte_offset]);
            if count <= budget && byte_offset >= best.byte_offset {
                best = Cut {
                    byte_offset,
                    tokens: count,
                };
            }
        }
        best
    }

    /// The split-point cases cairn/5074 pinned, as numbered multi-line text:
    /// `/`, `\r\n`, blank and whitespace-only lines, no-break space, next-line,
    /// ideographic space, tabs, CJK, combining marks and emoji.
    fn split_point_text() -> String {
        let lines = [
            "fn main() {",
            "    let x = compute(&input[3..], 9)?;  ",
            "",
            "/// doc comment",
            "/path/to/file",
            "};",
            "\t\tindented",
            "   ",
            "a/b//c",
            "\u{65e5}\u{672c}\u{8a9e} cafe\u{301} \u{1f600}",
            "12345678901",
            "don't stop",
            "a;\u{a0}b\u{85}c \t d",
            "x  ,  y\u{3000}z",
        ];
        (0..240)
            .map(|i| {
                let line = lines[(i * 7) % lines.len()];
                let ending = if i % 5 == 0 { "\r\n" } else { "\n" };
                if i % 3 == 0 {
                    format!("{line}{ending}")
                } else {
                    format!("{:>6}\t{line}{ending}", i + 1)
                }
            })
            .collect()
    }

    #[test]
    fn o200k_cut_recounted_from_a_split_matches_a_whole_prefix_recount() {
        let text = split_point_text();
        // The same lines run together, so only splits inside a line apply.
        let one_line = text.replace(['\r', '\n'], " ");
        for (name, o200k) in o200k_backends() {
            for text in [&text, &one_line] {
                let total = o200k.count(text);
                for budget in (0..total).step_by(23) {
                    assert_eq!(
                        o200k.cut(text, budget),
                        o200k_cut_by_whole_prefix(o200k, text, budget),
                        "{name}: budget {budget}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_o200k_split_never_precedes_a_slash_or_another_newline() {
        assert_eq!(o200k_split_before("a;\n/b", 5), 0);
        assert_eq!(o200k_split_before("ab\n  \nc", 7), 6);
        assert_eq!(o200k_split_before("ab\n  c", 6), 3);
        assert_eq!(o200k_split_before("ab\r\nc", 5), 4);
        assert_eq!(o200k_split_before("one word", 8), 3);
        assert_eq!(o200k_split_before(";\r\n", 3), 0);
    }

    /// Text where the two o200k backends could plausibly part: special-token
    /// spellings from gpt-oss's file (toks must encode them as text, as
    /// `encode_ordinary` does), and the pre-tokenizer's whitespace, slash,
    /// contraction, digit, joiner and control-character boundaries.
    const O200K_EDGES: &[&str] = &[
        "",
        "<|endoftext|>",
        "<|startoftext|><|start|>user<|message|>hi<|end|><|start|>assistant",
        "<|reserved_200000|> <|endofprompt|><|return|><|call|><|constrain|><|channel|>",
        "text<|endoftext|>more text <|endoftext|>\n<|endoftext",
        "\r\n\r\n\n  \n\t\n\r\r \r",
        "a\u{a0}\u{a0}b\u{85}\u{85}c\u{3000}\u{3000}d\u{2028}e\u{200b}f\u{feff}g",
        "   leading and trailing   ",
        "//\n/\n///path\r\n/x \n/",
        "1234567890123 3.14159 1e10 0x1F \u{661}\u{662}\u{663}",
        "I'M HE'LL they'RE 's 't don\u{2019}t",
        "\u{1f468}\u{200d}\u{1f469}\u{200d}\u{1f467} \u{1f1fa}\u{1f1f8} \u{2764}\u{fe0f}",
        "e\u{301}\u{302}\u{303} \u{915}\u{94d}\u{937} \u{e01}\u{e31}",
        "\u{0}\u{1}\u{1b}[0m\u{7f}\u{80}\u{9f}\u{a0}",
        "\u{e000}\u{f8ff}\u{fffd}\u{10ffff}",
    ];

    /// A small deterministic generator, so a failure reproduces from its seed.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % n as u64) as usize
        }
    }

    /// Short texts of random code points across every UTF-8 length and the
    /// classes the pattern distinguishes, joined by the edge fragments.
    fn random_o200k_texts(count: usize) -> Vec<String> {
        const RANGES: &[(u32, u32)] = &[
            (0x20, 0x7e),
            (0x20, 0x7e),
            (0x00, 0x20),
            (0x80, 0x24f),
            (0x300, 0x36f),
            (0x370, 0x52f),
            (0x600, 0x6ff),
            (0x900, 0x97f),
            (0x2000, 0x206f),
            (0x3000, 0x30ff),
            (0x4e00, 0x9fff),
            (0xac00, 0xd7a3),
            (0x1f300, 0x1faff),
            (0xe000, 0xf8ff),
            (0xfff0, 0xffff),
        ];
        let mut rng = Rng(0x0200_0005_eed5);
        (0..count)
            .map(|_| {
                let mut text = String::new();
                for _ in 0..rng.below(48) {
                    if rng.below(6) == 0 {
                        text.push_str(O200K_EDGES[rng.below(O200K_EDGES.len())]);
                    } else {
                        let (lo, hi) = RANGES[rng.below(RANGES.len())];
                        let code = lo + rng.below((hi - lo + 1) as usize) as u32;
                        text.extend(char::from_u32(code));
                    }
                }
                text
            })
            .collect()
    }

    /// What the toks backend is chosen on: exactly tiktoken's
    /// `encode_ordinary` ids, not only its counts, over cairn-tokenize's
    /// held-out corpus (Multipl-E and Rosetta), the pinned split-point text,
    /// the edges above and random text, then the same counts, token byte
    /// lengths and cuts through each backend's own paths. The whole corpus as
    /// one text runs toks' part sums against tiktoken's single encode.
    #[test]
    fn the_toks_o200k_backend_is_tiktokens_encoder() {
        if !cairn_toks::NATIVE {
            assert!(cairn_toks::o200k().is_err(), "no toks, no embedded file");
            assert!(matches!(O200k::selected(), O200k::Tiktoken));
        } else {
            assert!(matches!(O200k::selected(), O200k::Toks(_)));
        }
        let selected = O200k::selected();
        if let O200k::Toks(tokenizer) = selected {
            assert!(tokenizer.has_certified_cuts());
        }
        let held_out = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cairn-tokenize/tests/fixtures/held_out.jsonl"
        ));
        let mut texts: Vec<String> = held_out
            .lines()
            .map(|line| {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                row["text"].as_str().unwrap().to_string()
            })
            .collect();
        assert!(texts.len() > 250, "the held-out corpus is all there");
        texts.push(split_point_text());
        let documents = texts.len();
        texts.extend(O200K_EDGES.iter().map(|edge| edge.to_string()));
        let corpus = texts.concat();
        assert!(
            corpus.len() > 8 * 64 * 1024,
            "long enough for many 64 KiB certified parts: {} bytes",
            corpus.len()
        );
        texts.push(corpus);
        texts.extend(random_o200k_texts(4000));

        let (toks, tiktoken) = (selected, O200k::Tiktoken);
        for (index, text) in texts.iter().enumerate() {
            let expected = tiktoken_o200k().encode_ordinary(text);
            let ids = match toks {
                O200k::Toks(tokenizer) => tokenizer.encode(text).unwrap(),
                O200k::Tiktoken => tiktoken_o200k().encode_ordinary(text),
            };
            if ids != expected {
                let at = ids
                    .iter()
                    .zip(&expected)
                    .take_while(|(a, b)| a == b)
                    .count();
                panic!(
                    "text {index}: ids part at token {at} of {}: toks {:?} vs tiktoken {:?}",
                    expected.len(),
                    &ids[at..(at + 4).min(ids.len())],
                    &expected[at..(at + 4).min(expected.len())],
                );
            }
            assert_eq!(toks.count(text), expected.len() as u32, "text {index}");
            assert_eq!(
                toks.token_bytes(text),
                tiktoken.token_bytes(text),
                "text {index}"
            );
        }

        // The cut is the same algorithm over the same counts, so it must land
        // on the same byte; checked on every held-out document and the pinned
        // split-point text at budgets across each.
        for (index, text) in texts.iter().take(documents).enumerate() {
            let total = tiktoken.count(text);
            for budget in [
                0,
                1,
                2,
                total / 7,
                total / 3,
                total / 2,
                total.saturating_sub(1),
                total,
            ] {
                assert_eq!(
                    toks.cut(text, budget),
                    tiktoken.cut(text, budget),
                    "text {index}, budget {budget}"
                );
            }
        }
    }

    /// One o200k backend as a meter, to drive the renderer with.
    struct Backend(O200k);

    impl TokenMeter for Backend {
        fn count(&self, text: &str) -> u32 {
            self.0.count(text)
        }

        fn cut(&self, text: &str, budget: u32) -> Cut {
            self.0.cut(text, budget)
        }
    }

    /// A read body of one of six shapes: numbered code, prose, CJK lines,
    /// dense JSON, one line with no newline, and CRLF text with blank and
    /// whitespace-only lines and the added tokens' text.
    fn golden_body(shape: usize, bytes: usize) -> String {
        let mut out = String::new();
        let mut i = 0usize;
        while out.len() < bytes {
            match shape {
                0 => out.push_str(&format!(
                    "{:>6}\t    let v_{i} = f(&input[{i}..], {i} * 3)?; // step {i}\n",
                    i + 1
                )),
                1 => out.push_str(&format!(
                    "Paragraph {i}: budgets matter most where windows are smallest. "
                )),
                2 => out.push_str(&format!(
                    "\u{7b2c}{i}\u{6bb5}\u{ff1a}\u{30c8}\u{30fc}\u{30af}\u{30f3}\u{4e88}\u{7b97}\u{3002}\u{4e2d}\u{6587}\n"
                )),
                3 => out.push_str(&format!("{{\"id\":{i},\"k\":[{i},{i},\"x/y\"]}},")),
                4 => out.push_str(&format!("word{i} ")),
                _ => out.push_str(&format!(
                    "line {i}<|endoftext|>\r\n\r\n   \r\n\t/{i}\u{a0}\u{3000}\n"
                )),
            }
            i += 1;
        }
        let mut end = bytes.min(out.len());
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out
    }

    const GOLDEN_SIZES: &[usize] = &[600, 6_000, 40_000, 120_000, 200_000];
    const GOLDEN_BUDGETS: &[u32] = &[0, 1, 7, 100, 1_000, 5_000, 20_000];

    /// The renderer meets a meter only through `count` and `cut`, so with the
    /// two backends answering alike every read renders byte-identically:
    /// bounded renders, fixed segment budgets, `Context` batches and the
    /// legacy character path, over six body shapes at five sizes.
    #[test]
    fn reads_render_byte_identically_under_either_o200k_backend() {
        use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
        use cairn_db::storage::render::{
            assemble, assemble_legacy_chars, render_segment, render_segment_bounded, ReadBudget,
        };

        let selected = O200k::selected();
        assert_eq!(matches!(selected, O200k::Toks(_)), cairn_toks::NATIVE);
        let (toks, tiktoken) = (Backend(selected), Backend(O200k::Tiktoken));
        let segment = |uri: String, body: &str| {
            ReadSegment::text(
                body.to_string(),
                SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line),
            )
        };
        let mut cases = 0;
        for &bytes in GOLDEN_SIZES {
            let bodies: Vec<String> = (0..6).map(|shape| golden_body(shape, bytes)).collect();
            for (shape, body) in bodies.iter().enumerate() {
                let uri = format!("file:shape{shape}-{bytes}.txt");
                assert_eq!(
                    render_segment_bounded(segment(uri.clone(), body), &toks).text,
                    render_segment_bounded(segment(uri.clone(), body), &tiktoken).text,
                    "bounded render of shape {shape} at {bytes} bytes"
                );
                for &budget in GOLDEN_BUDGETS {
                    assert_eq!(
                        render_segment(segment(uri.clone(), body), budget, &toks).text,
                        render_segment(segment(uri.clone(), body), budget, &tiktoken).text,
                        "shape {shape} at {bytes} bytes, budget {budget}"
                    );
                }
                cases += 1 + GOLDEN_BUDGETS.len();
            }
            let batch = || {
                bodies
                    .iter()
                    .enumerate()
                    .map(|(shape, body)| segment(format!("file:part{shape}.txt"), body))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                assemble(batch(), ReadBudget::Context, &toks),
                assemble(batch(), ReadBudget::Context, &tiktoken),
                "Context batch at {bytes} bytes"
            );
            assert_eq!(
                assemble_legacy_chars(batch(), &toks),
                assemble_legacy_chars(batch(), &tiktoken),
                "legacy batch at {bytes} bytes"
            );
            cases += 2;
        }
        eprintln!("{cases} renders identical under both o200k backends");
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
