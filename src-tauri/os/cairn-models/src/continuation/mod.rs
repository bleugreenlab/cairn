//! Continuation: how an existing provider conversation carries the next turn,
//! and what to send when it should not.
//!
//! Three parts, each usable without the others:
//!
//! - the policy below, which decides between native resume and a rebuilt
//!   session from readings a host already has (backend key, resume handle,
//!   last provider request, window occupancy);
//! - [`Transcript`], a provider-neutral record of a conversation, which a host
//!   assembles from its own storage or from [`crate::session::events::SessionEvent`]s
//!   through [`TranscriptBuilder`];
//! - [`render_digest`], the bounded projection a rebuilt session is seeded
//!   with, and [`verbatim_bytes`], the measure of what native resume re-sends,
//!   which together price a cold rebuild through [`price_cold_rebuild`].
//!
//! # Session replacement policy (CAIRN-2534, CAIRN-3388, CAIRN-4618, CAIRN-4757)
//!
//! Two questions are asked here, and keeping them apart is the whole policy.
//!
//! CAN THIS CONVERSATION CARRY THE NEXT TURN? Never a function of the clock. A
//! Cairn session is replaced only when the native provider conversation cannot
//! carry the next turn. There are exactly five causes, and every one of them is a
//! positive observation rather than an inference:
//!
//!   1. the job's model moved to a provider that cannot serve the old resume
//!      handle ([`ContinueAction::RotateToBackend`]);
//!   2. the provider refused to resolve the session when asked — reported by the
//!      backend on the resume attempt itself, and turned into a rebuild by the
//!      host's digest-reseed fallback (Cairn's lives in `finalize`), which also
//!      says so in the transcript. The facet's Claude signal for it is
//!      [`crate::wire::claude_stream::stderr_reports_unknown_session`]; Codex has
//!      none yet;
//!   3. the provider's own accounting says the model's window is running out
//!      ([`window_pressure_reached`]);
//!   4. an operator asked for a reset (Cairn's `resume_job_from_digest`);
//!   5. the provider was launched under this session's id and never confirmed
//!      it ([`ContinueAction::RotateUnconfirmedStart`]). A provider that keys a
//!      fresh conversation by the caller's id (Claude's `--session-id`) creates
//!      it at launch, so a launch rejected before confirmation leaves a
//!      conversation the same id can never start again.
//!
//! Elapsed time is not on that list and must never rejoin it. A prompt cache
//! expiring means the next request pays a cache write; it does not mean the
//! conversation is gone, and both production backends resume straight across it.
//! Rebuilding on idleness therefore cost a standing thread its native thread on
//! every wake: 63% of the 1,181 replacements on the reference install were
//! elapsed inactivity alone (CAIRN-4618).
//!
//! WHICH OF THE TWO AVAILABLE CONTINUATIONS IS CHEAPER? A function of the clock,
//! because provider prompt-cache pricing is. While the prefix is still
//! cache-readable, native resume costs roughly a tenth of ordinary input and
//! nothing else comes close. Once it is cold, both paths pay cache creation at
//! the same per-token rate, and the only thing separating them is how many tokens
//! each sends ([`prefix_cache_state`], [`cold_reseed_pays`]).
//!
//! The cache is also per organization. Anthropic isolates prompt caches by
//! organization, and separate subscriptions are separate organizations, so a
//! session whose next launch authenticates as a different account than its last
//! request reads cold however recent that request was ([`prefix_cache_reading`]).
//! That is still a cost reading, never a viability one: the conversation resumes
//! on the new account either way, and the same savings guard decides whether a
//! rebuild is worth it. An account Cairn cannot attribute on either side changes
//! nothing, because unknown is not cold.
//!
//! The second question may never answer the first. A cold prefix does not replace
//! a conversation on its own — it earns the right to be priced, and the savings
//! guard refuses the rebuild unless it materially lowers the bill. The
//! conversation is spent only when what it buys has been measured.
//!
//! The census that used to close this comment read the two size fields of
//! Cairn's thread seed backwards and reported expiry compactions as reclaiming
//! 0.47% of a transcript. `source_bytes` is what the folded turns weigh verbatim
//! and `candidate_bytes` is what the chapter lines standing in for them weigh, so
//! the same numbers say those compactions replaced ~195KB of verbatim turns with
//! ~921 bytes of table of contents — a ~99.5% reduction of the region they
//! folded. The code was never wrong; only the prose was (CAIRN-4757).

mod digest;
mod transcript;

pub use digest::{
    cleaned_tool_name, count_prefixed_diff_lines, figure_for_read_result, figure_for_run_result,
    first_nonempty_line, first_result_line, generic_rows, json_value_to_display, line_count,
    render_digest, render_verbatim, single_line_preview, summarize_scalar, summarize_tool_input,
    truncate_with_info, verbatim_bytes, DigestOptions, DigestRow, RowContext, ToolRenderers,
    ERROR_LINE_PREVIEW, ROW_LABEL_PREVIEW,
};
pub use transcript::{Entry, Marker, ToolCall, ToolOutcome, Transcript, TranscriptBuilder, Turn};

use crate::types::catalog::{CLAUDE_FAMILY_BACKEND, CODEX_BACKEND};

/// Which provider's prompt cache a session's prefix lives in, as far as
/// continuation policy distinguishes them.
///
/// Deliberately not derived from the spawn CLI
/// ([`crate::interface::session::Cli`]): OpenRouter and custom accounts run
/// through the Claude CLI, and keying the horizon on it would silently lend
/// Claude's hour to every one of them. Nor is it read through a host's backend
/// resolution whose catch-all answers Claude. [`ResumeBackend::Other`] is what
/// makes "a backend without a known cache horizon stays on its own policy" true
/// by construction rather than by review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeBackend {
    Claude,
    Codex,
    /// Every backend Cairn has no published cache horizon for.
    Other,
}

impl ResumeBackend {
    /// Read a persisted backend key (`sessions.backend` in Cairn). Matching is
    /// case-insensitive, so `Claude` and `CODEX` are recognized.
    pub fn from_key(persisted: &str) -> Self {
        if persisted.eq_ignore_ascii_case(CLAUDE_FAMILY_BACKEND) {
            ResumeBackend::Claude
        } else if persisted.eq_ignore_ascii_case(CODEX_BACKEND) {
            ResumeBackend::Codex
        } else {
            ResumeBackend::Other
        }
    }
}

/// How long a provider keeps this session's prefix cache-readable after the last
/// request that touched it. `None` means there is no published horizon for this
/// backend, so its prefix is never treated as cold.
pub fn prompt_cache_horizon_secs(backend: ResumeBackend) -> Option<i64> {
    match backend {
        // Operator-specified.
        ResumeBackend::Claude => Some(60 * 60),
        // OpenAI's published cached-prefix eligibility for GPT-5.6: 30 minutes
        // from the latest cache write or reuse. Longer retention happens but is
        // not a pricing guarantee, so it is not a horizon.
        // https://developers.openai.com/api/docs/guides/prompt-caching.md
        ResumeBackend::Codex => Some(30 * 60),
        ResumeBackend::Other => None,
    }
}

/// Whether the provider's prompt cache still holds this session's prefix.
///
/// This is an economic reading, never a viability one. Nothing here says whether
/// the conversation can continue — both backends resume across a cold cache —
/// only which of the two continuations the next request should pay for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixCacheState {
    /// Still cache-readable. Native resume costs roughly a tenth of ordinary
    /// input, so no rebuild can compete and none is considered.
    Warm,
    /// Past the provider's horizon. Both continuations now pay cache creation at
    /// the same per-token rate, so the cheaper one has to be chosen rather than
    /// assumed.
    Cold,
    /// No declared horizon for this backend, or no provider request on record to
    /// measure from. Absence of evidence is not evidence of a cold cache, so
    /// this never causes a rebuild.
    Unknown,
}

impl PrefixCacheState {
    pub fn as_str(self) -> &'static str {
        match self {
            PrefixCacheState::Warm => "warm",
            PrefixCacheState::Cold => "cold",
            PrefixCacheState::Unknown => "unknown",
        }
    }
}

/// Read the prefix's cache state from the provider's own horizon and the last
/// request that actually touched it.
///
/// `>=` is the boundary: Claude is warm through 59:59 and cold at 60:00, Codex
/// warm through 29:59 and cold at 30:00.
pub fn prefix_cache_state(
    backend: ResumeBackend,
    last_provider_request_at: Option<i64>,
    now: i64,
) -> PrefixCacheState {
    let (Some(horizon), Some(last)) =
        (prompt_cache_horizon_secs(backend), last_provider_request_at)
    else {
        return PrefixCacheState::Unknown;
    };
    if now.saturating_sub(last) >= horizon {
        PrefixCacheState::Cold
    } else {
        PrefixCacheState::Warm
    }
}

/// Why a prefix reads cold, carried onto the boundary of the continuation it
/// priced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdCause {
    /// The provider's horizon has passed since the last request.
    HorizonElapsed,
    /// The next launch authenticates as a different account than the last
    /// request ran on, and the cache does not cross organizations.
    AccountChanged,
}

impl ColdCause {
    pub fn as_str(self) -> &'static str {
        match self {
            ColdCause::HorizonElapsed => "horizon_elapsed",
            ColdCause::AccountChanged => "account_changed",
        }
    }
}

/// A prefix cache reading together with why it is cold, when it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefixReading {
    pub state: PrefixCacheState,
    pub cold_cause: Option<ColdCause>,
}

/// Read the prefix's cache state from the clock and from the accounts on both
/// sides of the continuation: the one the last provider request ran on, and the
/// one the next launch will authenticate as.
///
/// Two known, different accounts read cold whatever the clock says. Anything
/// else is [`prefix_cache_state`]'s reading unchanged, so an account Cairn could
/// not attribute on either side never makes a prefix cold.
pub fn prefix_cache_reading(
    backend: ResumeBackend,
    last_provider_request_at: Option<i64>,
    last_request_account: Option<&str>,
    launch_account: Option<&str>,
    now: i64,
) -> PrefixReading {
    if last_provider_request_at.is_some() {
        if let (Some(last), Some(launch)) = (last_request_account, launch_account) {
            if last != launch {
                return PrefixReading {
                    state: PrefixCacheState::Cold,
                    cold_cause: Some(ColdCause::AccountChanged),
                };
            }
        }
    }
    let state = prefix_cache_state(backend, last_provider_request_at, now);
    PrefixReading {
        state,
        cold_cause: (state == PrefixCacheState::Cold).then_some(ColdCause::HorizonElapsed),
    }
}

/// A cold rebuild must at least halve what the next request sends. Below that,
/// native resume costs about the same money and keeps the conversation, so
/// rebuilding is a pure loss.
///
/// A constant rather than a setting: it answers one question with one number,
/// and a knob here would drift against the window-pressure threshold without
/// ever being tuned.
pub const COLD_RESEED_MIN_SAVINGS_RATIO: f64 = 0.5;

/// The refusal recorded when a cold rebuild would not have paid for itself.
pub const RESEED_SKIPPED_INSUFFICIENT_SAVINGS: &str = "insufficient_savings";

/// The refusal recorded when a cold thread had nothing settled to fold. Distinct
/// from an unprofitable rebuild: this one is refused on continuity rather than
/// on price, and no amount of saving would change it.
pub const RESEED_SKIPPED_NOTHING_TO_FOLD: &str = "nothing_to_fold";

/// Whether rebuilding is worth what it spends, on a cold prefix.
///
/// Once the cache is cold both continuations pay cache creation at the same
/// per-token rate, so the ratio of bytes *is* the ratio of cost. What a rebuild
/// spends — the verbatim reasoning that produced a conclusion, which no seed can
/// reconstruct — is spent in full every time and is never recovered, so the
/// saving has to be large enough to be worth naming.
pub fn cold_reseed_pays(seed_bytes: i64, prefix_bytes: i64) -> bool {
    let saved = prefix_bytes.saturating_sub(seed_bytes);
    saved > 0 && saved as f64 >= prefix_bytes as f64 * COLD_RESEED_MIN_SAVINGS_RATIO
}

/// Whether a session has filled enough of its window to have to give
/// something up.
///
/// Deliberately not an economic question. Bytes can say what a rebuild costs in
/// money; they cannot say what it costs the thread, which is its own reasoning
/// and its sense of continuity — the thinking that produced a conclusion does not
/// survive into the seed, only the conclusion does. That cost is large, fixed,
/// and paid in full every time, so no amount of saved prefix earns it back. A
/// warm session therefore rebuilds only when the window is running out, which is
/// the one situation where not rebuilding is not an option either.
///
/// Occupancy is `(used, window)` from the provider's own accounting of the
/// request just made. That is the opposite of using a token counter to price a
/// seed never sent: here the question *is* what the last request weighed.
pub fn window_pressure_reached(occupancy: Option<(i64, i64)>, threshold: f64) -> bool {
    // No reading, no rebuild. The alternative — estimating occupancy from bytes
    // against an assumed window — is a second, cruder measure standing in for the
    // real one, which is exactly the substitution that made the previous trigger
    // fire on everything. When the host cannot see the window, the provider's own
    // limit is the backstop, and it fails loudly rather than silently degrading
    // the thread.
    let Some((used, window)) = occupancy else {
        return false;
    };
    if window <= 0 {
        return false;
    }
    used as f64 >= window as f64 * threshold
}

/// Whether an open session resumes natively and so must NOT be preempted by an
/// automatic rebuild at all. Codex sessions carrying a native thread id resume
/// via `thread/resume`; rotating them to a fresh session would wipe the Codex
/// thread and restart the reply at the system prompt (CAIRN-2598), and the
/// app-server manages its own window server-side. Claude and handle-less
/// sessions reach the window-pressure check.
pub fn prefers_native_resume(backend: ResumeBackend, has_handle: bool) -> bool {
    backend == ResumeBackend::Codex && has_handle
}

/// Why a continuation took the path it did, carried onto the session boundary.
///
/// A cold prefix has two legitimate outcomes — rebuild, or keep the conversation
/// because the rebuild would not pay — and they are indistinguishable in the
/// transcript without this. Recording it is what lets a census separate them
/// afterwards instead of re-deriving them from timestamps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CacheDecision {
    /// The operation actually performed: `native_resume` or `digest_reseed`.
    pub decision: &'static str,
    pub cache_state: &'static str,
    /// Set only when a cold rebuild was declined, naming why.
    pub reseed_skipped: Option<&'static str>,
    /// What native resume re-sends: the prefix the provider recreates.
    pub prefix_bytes: Option<i64>,
    /// What the rebuild would have sent in its place.
    pub candidate_seed_bytes: Option<i64>,
    /// The ratio the refusal was measured against.
    pub threshold: Option<f64>,
    /// Why the prefix read cold.
    pub cold_cause: Option<ColdCause>,
}

impl CacheDecision {
    /// A rebuild that cache economics chose. The boundary already carries the
    /// delivered seed size, so this adds only the cause.
    ///
    /// The state is fixed rather than passed in, because there is only one that
    /// can reach here: a warm or unmeasurable prefix never selects a rebuild, so
    /// a `CacheDecision` on a boundary always means the prefix was cold. A
    /// window-pressure rebuild carries no `CacheDecision` at all — the clock had
    /// no part in it, and saying `cacheState: warm` there would put a cache
    /// reading on a decision cache economics did not make.
    pub fn rebuilt() -> Self {
        Self {
            decision: "digest_reseed",
            cache_state: PrefixCacheState::Cold.as_str(),
            reseed_skipped: None,
            prefix_bytes: None,
            candidate_seed_bytes: None,
            threshold: None,
            cold_cause: None,
        }
    }

    /// Name why the prefix read cold. Applied by the host that took the
    /// reading, so the pricing functions stay ignorant of where it came from.
    pub fn with_cold_cause(mut self, cause: Option<ColdCause>) -> Self {
        self.cold_cause = cause;
        self
    }

    /// A cold prefix that was priced and kept. Both measurements ride along,
    /// because the refusal is only reviewable if the numbers behind it are.
    pub fn refused(cache: PrefixCacheState, prefix_bytes: i64, candidate_seed_bytes: i64) -> Self {
        Self {
            decision: "native_resume",
            cache_state: cache.as_str(),
            reseed_skipped: Some(RESEED_SKIPPED_INSUFFICIENT_SAVINGS),
            prefix_bytes: Some(prefix_bytes),
            candidate_seed_bytes: Some(candidate_seed_bytes),
            threshold: Some(COLD_RESEED_MIN_SAVINGS_RATIO),
            cold_cause: None,
        }
    }

    /// A cold thread whose composition would fold nothing. Refused on continuity
    /// rather than on price, so it carries no measurement: there was no rebuild
    /// worth weighing, at any saving.
    pub fn refused_nothing_to_fold(cache: PrefixCacheState) -> Self {
        Self {
            decision: "native_resume",
            cache_state: cache.as_str(),
            reseed_skipped: Some(RESEED_SKIPPED_NOTHING_TO_FOLD),
            prefix_bytes: None,
            candidate_seed_bytes: None,
            threshold: None,
            cold_cause: None,
        }
    }
}

/// The continuation action for an open session on a user reply. This is the pure
/// decision; the caller performs the rotation / reseed / native-resume work.
#[derive(Debug, PartialEq, Eq)]
pub enum ContinueAction {
    /// The requested model implies a different backend than the session was
    /// started on — rotate to a fresh session on that backend (the old backend's
    /// resume handle is invalid on the new one). Carries the target backend key.
    RotateToBackend(String),
    /// Replace a session whose provider was launched under its id but never
    /// confirmed a handle. Retrying under the same id would be refused, so the
    /// job continues on a successor carrying a fresh id.
    RotateUnconfirmedStart,
    /// Retry a provisional session whose backend startup never confirmed a
    /// native resume handle. The session identity and transcript survive.
    RetryStart,
    /// Ask [`reseed_step`], falling back to native resume.
    MaybeReseed,
    /// Native backend resume of the open session.
    Resume,
}

/// Decide how an open session continues on a user reply. A cross-backend model
/// change replaces a session outright, because it invalidates the resume handle.
/// A session without a confirmed backend handle retries its original startup,
/// unless `unconfirmed_spawned_launch` reports that the provider was already
/// launched under this session's id — the host reads that only for providers
/// that key a fresh conversation by Cairn's id — in which case the same id
/// cannot start again and the session is replaced; a session that resumes
/// natively (Codex with a stored thread id) resumes directly; everything else
/// asks the window-pressure rebuild, which nearly always declines.
///
/// The backend keys are the persisted ones, compared exactly: rotation needs the
/// target key as the host stores it.
///
/// An edited agent prompt is deliberately absent. Both production backends
/// reassemble the whole instruction stack on every process spawn — Claude writes
/// it to the file passed as `--system-prompt-file` alongside `--resume`, Codex
/// puts it in `thread/resume`'s own `baseInstructions` — so an edit reaches the
/// *existing* session on its next turn. Rotating for it bought nothing and cost
/// the native thread (CAIRN-4618).
///
/// `prefix_cache` is the one input a clock reaches, and it decides cost rather
/// than viability: it can send a session to [`ContinueAction::MaybeReseed`]
/// to be priced, and nothing more (CAIRN-4757).
pub fn decide_continue_action(
    session_backend: &str,
    session_backend_id: Option<&str>,
    desired_backend: Option<&str>,
    prefix_cache: PrefixCacheState,
    unconfirmed_spawned_launch: bool,
) -> ContinueAction {
    if let Some(want) = desired_backend.filter(|want| *want != session_backend) {
        return ContinueAction::RotateToBackend(want.to_string());
    }
    if session_backend_id.is_none() {
        if unconfirmed_spawned_launch {
            return ContinueAction::RotateUnconfirmedStart;
        }
        return ContinueAction::RetryStart;
    }
    // A cold prefix is the one thing that makes a natively-resumable session
    // worth pricing: past the horizon both paths pay cache creation, so the
    // cheaper one has to be chosen rather than assumed. This sits ABOVE the
    // native-resume preference deliberately — below it, a Codex session would
    // short-circuit to `Resume` and never reach the pricing decision at all.
    // Reaching `MaybeReseed` is not itself a rebuild: the savings guard there
    // refuses one that would not pay.
    if prefix_cache == PrefixCacheState::Cold {
        return ContinueAction::MaybeReseed;
    }
    if prefers_native_resume(
        ResumeBackend::from_key(session_backend),
        session_backend_id.is_some(),
    ) {
        return ContinueAction::Resume;
    }
    ContinueAction::MaybeReseed
}

/// What [`ContinueAction::MaybeReseed`] does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReseedStep {
    /// The window is running out: rebuild whatever it costs. The clock had no
    /// part in this, so the boundary records no [`CacheDecision`].
    RebuildForWindow,
    /// The prefix is cold: compose the seed, measure both sides, and let
    /// [`price_cold_rebuild`] decide.
    PriceColdRebuild,
    /// Native resume of the same conversation, however long it has waited.
    Resume,
}

/// The reseed step's ordering rule. Window pressure is asked first, because it
/// is an obligation rather than a choice; only then does a cold prefix earn a
/// price. Everything else resumes natively.
pub fn reseed_step(
    occupancy: Option<(i64, i64)>,
    threshold: f64,
    prefix_cache: PrefixCacheState,
) -> ReseedStep {
    if window_pressure_reached(occupancy, threshold) {
        ReseedStep::RebuildForWindow
    } else if prefix_cache == PrefixCacheState::Cold {
        ReseedStep::PriceColdRebuild
    } else {
        ReseedStep::Resume
    }
}

/// The verdict on a cold prefix, with the [`CacheDecision`] its boundary records.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColdRebuild {
    /// The seed at least halves the request: rebuild.
    Pays(CacheDecision),
    /// It does not: keep the conversation, and record the numbers that said so.
    Declined(CacheDecision),
}

/// Price a cold rebuild: `seed_bytes` is what the rebuild would send (the
/// framed seed exactly as delivered), `prefix_bytes` what native resume makes
/// the provider recreate ([`verbatim_bytes`]).
pub fn price_cold_rebuild(seed_bytes: i64, prefix_bytes: i64) -> ColdRebuild {
    if cold_reseed_pays(seed_bytes, prefix_bytes) {
        ColdRebuild::Pays(CacheDecision::rebuilt())
    } else {
        ColdRebuild::Declined(CacheDecision::refused(
            PrefixCacheState::Cold,
            prefix_bytes,
            seed_bytes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn horizon(key: &str) -> Option<i64> {
        prompt_cache_horizon_secs(ResumeBackend::from_key(key))
    }

    /// The line CAIRN-4618 drew, held in the form CAIRN-4757 needs it.
    ///
    /// There used to be a staleness threshold that treated an idle hour as
    /// evidence the conversation was gone. It was not, and removing it was right.
    /// A clock is back, but it reaches a different question: it can say which
    /// continuation is cheaper, and it may never say whether one is possible.
    #[test]
    fn elapsed_time_prices_the_continuation_and_never_judges_the_conversation() {
        // The viability trigger takes a window reading and a threshold. A session
        // idle since the epoch, holding almost none of its window, is resumed
        // rather than rebuilt — there is nowhere for a duration to enter.
        assert!(!window_pressure_reached(Some((1_000, 200_000)), 0.8));
        assert!(window_pressure_reached(Some((190_000, 200_000)), 0.8));

        // The clock reaches the decision through exactly one door, and what comes
        // through it is a price, not a verdict. A cold prefix sends the session to
        // be priced; it does not replace anything on its own.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some("claude"),
                PrefixCacheState::Cold,
                false
            ),
            ContinueAction::MaybeReseed
        );
        // And a rebuild past that door still has to show its saving.
        assert!(!cold_reseed_pays(9_000, 10_000));
    }

    /// The horizons are declared per provider, and — the part that actually
    /// needs guarding — a backend with no published horizon answers `None`
    /// rather than inheriting one.
    ///
    /// A host's backend resolution may answer Claude for an unknown name. Had
    /// the horizon been read through it, every one of the names below would have
    /// silently borrowed Claude's hour.
    #[test]
    fn prompt_cache_horizon_is_declared_per_provider() {
        assert_eq!(horizon("claude"), Some(3_600));
        assert_eq!(horizon("Claude"), Some(3_600));
        assert_eq!(horizon("codex"), Some(1_800));
        assert_eq!(horizon("CODEX"), Some(1_800));
        for unpriced in [
            "openrouter",
            "ollama",
            "opencode-go",
            "actual",
            "acme-account-key",
        ] {
            assert_eq!(
                horizon(unpriced),
                None,
                "{unpriced} borrowed a cache horizon nobody published for it"
            );
        }
    }

    #[test]
    fn the_cache_horizon_boundary_belongs_to_the_provider() {
        let at = |backend: &str, elapsed: i64| {
            prefix_cache_state(ResumeBackend::from_key(backend), Some(0), elapsed)
        };

        // Claude: warm through 59:59, cold at 60:00.
        assert_eq!(at("claude", 59 * 60), PrefixCacheState::Warm);
        assert_eq!(at("claude", 3_599), PrefixCacheState::Warm);
        assert_eq!(at("claude", 3_600), PrefixCacheState::Cold);
        assert_eq!(at("claude", 10 * 24 * 60 * 60), PrefixCacheState::Cold);

        // Codex: warm through 29:59, cold at 30:00 — its own constant, not a
        // share of Claude's.
        assert_eq!(at("codex", 29 * 60), PrefixCacheState::Warm);
        assert_eq!(at("codex", 1_799), PrefixCacheState::Warm);
        assert_eq!(at("codex", 1_800), PrefixCacheState::Cold);
        // At 45 minutes the two providers disagree, which is the whole point of
        // declaring them separately.
        assert_eq!(at("claude", 45 * 60), PrefixCacheState::Warm);
        assert_eq!(at("codex", 45 * 60), PrefixCacheState::Cold);

        // A backend with no published horizon never expires, however long it
        // waits.
        assert_eq!(
            at("openrouter", 10 * 24 * 60 * 60),
            PrefixCacheState::Unknown
        );

        // Nor does a session that has never made a provider request. Absence of
        // evidence is not evidence of a cold cache.
        assert_eq!(
            prefix_cache_state(ResumeBackend::Claude, None, 10 * 24 * 60 * 60),
            PrefixCacheState::Unknown
        );
    }

    /// Seconds after the last request, a launch on another account reads cold:
    /// the new organization holds no cache for this prefix. Removing the account
    /// comparison makes this read warm.
    #[test]
    fn an_account_change_reads_the_prefix_cold_inside_the_horizon() {
        let reading = prefix_cache_reading(ResumeBackend::Claude, Some(0), Some("a"), Some("b"), 5);
        assert_eq!(reading.state, PrefixCacheState::Cold);
        assert_eq!(reading.cold_cause, Some(ColdCause::AccountChanged));
        assert_eq!(
            decide_continue_action("claude", Some("sess"), Some("claude"), reading.state, false),
            ContinueAction::MaybeReseed
        );
        // Codex too: the cold reading outranks its native-resume preference.
        let codex = prefix_cache_reading(ResumeBackend::Codex, Some(0), Some("a"), Some("b"), 5);
        assert_eq!(codex.state, PrefixCacheState::Cold);
        // And past the horizon an account change still names itself.
        let late =
            prefix_cache_reading(ResumeBackend::Claude, Some(0), Some("a"), Some("b"), 7_200);
        assert_eq!(late.cold_cause, Some(ColdCause::AccountChanged));
    }

    #[test]
    fn the_same_or_an_unknown_account_keeps_todays_reading() {
        for (last, launch) in [
            (Some("a"), Some("a")),
            (None, Some("b")),
            (Some("a"), None),
            (None, None),
        ] {
            for now in [5, 3_599, 3_600, 7_200] {
                let reading =
                    prefix_cache_reading(ResumeBackend::Claude, Some(0), last, launch, now);
                let today = prefix_cache_state(ResumeBackend::Claude, Some(0), now);
                assert_eq!(reading.state, today, "{last:?} -> {launch:?} at {now}");
                assert_eq!(
                    reading.cold_cause,
                    (today == PrefixCacheState::Cold).then_some(ColdCause::HorizonElapsed)
                );
            }
        }
        // No request on record: there is no prefix to have moved.
        let none = prefix_cache_reading(ResumeBackend::Claude, None, Some("a"), Some("b"), 5);
        assert_eq!(none.state, PrefixCacheState::Unknown);
        assert_eq!(none.cold_cause, None);
    }

    /// A cold Codex prefix must reach the pricing decision at all. Warm, it
    /// short-circuits to native resume and never gets there — which is correct
    /// while the cache is readable and was the whole of the behaviour before.
    #[test]
    fn a_cold_codex_prefix_reaches_the_pricing_decision() {
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-x"),
                Some("codex"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::Resume
        );
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-x"),
                Some("codex"),
                PrefixCacheState::Unknown,
                false
            ),
            ContinueAction::Resume,
            "an unmeasurable prefix must keep today's behaviour exactly"
        );
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-x"),
                Some("codex"),
                PrefixCacheState::Cold,
                false
            ),
            ContinueAction::MaybeReseed
        );
    }

    /// The savings guard, against the numbers that made it necessary.
    #[test]
    fn a_cold_rebuild_must_show_what_it_saves() {
        // CAIRN-4618's census, read correctly: an expiry compaction replaced
        // ~195,077 bytes of verbatim turns with ~921 bytes of chapter lines. The
        // seed still carries the recent window and the arc, so call the delivered
        // seed 20 KB — the rebuild sends a ninth of what native resume would.
        let saved = 195_077 - 921;
        let seed_bytes = 20_000;
        assert!(cold_reseed_pays(seed_bytes, seed_bytes + saved));

        // A thread with nothing foldable saves nothing, so the ratio test refuses
        // it without needing a second guard beside it.
        assert!(!cold_reseed_pays(20_000, 20_000));
        assert!(!cold_reseed_pays(20_000, 19_000));

        // And the boundary itself: exactly halving pays, a hair under does not.
        assert!(cold_reseed_pays(5_000, 10_000));
        assert!(!cold_reseed_pays(5_001, 10_000));
        assert_eq!(COLD_RESEED_MIN_SAVINGS_RATIO, 0.5);
    }

    #[test]
    fn a_session_rebuilds_only_when_its_window_is_running_out() {
        assert!(!window_pressure_reached(Some((159_000, 200_000)), 0.8));
        assert!(
            !window_pressure_reached(Some((160_000, 1_000_000)), 0.8),
            "a 1M Claude session must not rotate at the old 200k threshold"
        );
        assert!(
            window_pressure_reached(Some((800_000, 1_000_000)), 0.8),
            "a 1M Claude session rotates at four fifths of its real window"
        );
        assert!(
            window_pressure_reached(Some((160_000, 200_000)), 0.8),
            "the threshold is inclusive: at four fifths of the window, give something up"
        );

        // The threshold is configurable, and it is the only thing that moves the
        // boundary — nothing about the size of what would be dropped does, and
        // nothing about how long the session has been waiting.
        assert!(!window_pressure_reached(Some((120_000, 200_000)), 0.8));
        assert!(window_pressure_reached(Some((120_000, 200_000)), 0.6));
    }

    #[test]
    fn a_session_never_rebuilds_because_it_would_be_cheaper() {
        // CAIRN-3404's six rotations, all fired by the old byte rule. Their
        // common feature is that none of them was near the window: the thread was
        // using a fraction of its context and rebuilding anyway, losing its own
        // reasoning six times in one afternoon to reclaim between 1.1 KB and
        // 18 KB. Occupancy is what decides now, so every one holds — and would
        // hold no matter how lopsided the byte counts were.
        for used in [8_000, 20_000, 40_000, 80_000, 120_000, 155_000] {
            assert!(
                !window_pressure_reached(Some((used, 200_000)), 0.8),
                "a session at {used} tokens of a 200k window gave up its \
                 continuity for a size saving"
            );
        }
    }

    #[test]
    fn a_session_with_no_reading_is_left_alone() {
        // Estimating occupancy from bytes against an assumed window would be a
        // cruder measure standing in for the real one, which is the substitution
        // that made the previous trigger fire on everything. Without a reading
        // the provider's own limit is the backstop, and it fails loudly — which
        // is now also what happens to a session whose cache has long expired,
        // because expiry no longer forces a rebuild past a missing reading.
        assert!(!window_pressure_reached(None, 0.8));

        // A window the backend reports as zero is not a window at 0% full.
        assert!(!window_pressure_reached(Some((500_000, 0)), 0.8));
    }

    #[test]
    fn codex_reply_with_thread_id_resumes_not_reseeds() {
        // The CAIRN-2598 regression: an open Codex child-task session whose
        // backend matches the requested one and whose native thread id is stored
        // must resume, never rotate to a fresh session (which restarts at the
        // system prompt and wipes history).
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-existing"),
                Some("codex"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::Resume
        );
        // With no desired backend supplied (nothing to compare), a Codex session
        // with a thread id still resumes rather than reseeding.
        assert_eq!(
            decide_continue_action(
                "codex",
                Some("thread-existing"),
                None,
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::Resume
        );
    }

    #[test]
    fn matching_backend_without_native_handle_retries_startup() {
        // A confirmed Claude session (Claude does not have Codex's native-resume
        // preference) reaches the window-pressure check, which resumes it
        // natively unless its window is actually running out.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess-abc"),
                Some("claude"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::MaybeReseed
        );
        // A handle-less open session never completed its backend startup. Retry
        // that startup under the same session instead of trying to resume an
        // identifier that does not exist. A cold prefix does not change that:
        // there is no conversation to price yet.
        assert_eq!(
            decide_continue_action(
                "claude",
                None,
                Some("claude"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RetryStart
        );
        assert_eq!(
            decide_continue_action("codex", None, Some("codex"), PrefixCacheState::Cold, false),
            ContinueAction::RetryStart
        );
    }

    #[test]
    fn a_launched_but_unconfirmed_session_rotates_instead_of_retrying() {
        // The provider was launched under this session's id and never
        // confirmed it: a retry under the same id is refused, so the session is
        // replaced. Neither a cold nor a warm prefix changes that.
        for cache in [
            PrefixCacheState::Warm,
            PrefixCacheState::Cold,
            PrefixCacheState::Unknown,
        ] {
            assert_eq!(
                decide_continue_action("claude", None, Some("claude"), cache, true),
                ContinueAction::RotateUnconfirmedStart
            );
        }
        // Without that observation the same handle-less session keeps today's
        // identity-preserving retry.
        assert_eq!(
            decide_continue_action(
                "claude",
                None,
                Some("claude"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RetryStart
        );
        // A provider change still outranks it: the successor is minted on the
        // new backend either way, and that rotation carries the right reason.
        assert_eq!(
            decide_continue_action("claude", None, Some("codex"), PrefixCacheState::Warm, true),
            ContinueAction::RotateToBackend("codex".to_string())
        );
        // A confirmed handle is never second-guessed by the reading.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some("claude"),
                PrefixCacheState::Warm,
                true
            ),
            ContinueAction::MaybeReseed
        );
    }

    #[test]
    fn cross_backend_model_change_rotates() {
        // This is what the hardcoded-"claude" child session bug produced every
        // reply: desired "codex" vs stored "claude" rotated to a fresh session.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some("codex"),
                PrefixCacheState::Warm,
                false
            ),
            ContinueAction::RotateToBackend("codex".to_string())
        );
        // A provider change outranks the pricing question: the old resume handle
        // is invalid on the new backend whatever its cache says.
        assert_eq!(
            decide_continue_action(
                "claude",
                Some("sess"),
                Some("codex"),
                PrefixCacheState::Cold,
                false
            ),
            ContinueAction::RotateToBackend("codex".to_string())
        );
    }

    #[test]
    fn native_resume_predicate_is_codex_with_handle_only() {
        assert!(prefers_native_resume(
            ResumeBackend::from_key("codex"),
            true
        ));
        assert!(prefers_native_resume(
            ResumeBackend::from_key("Codex"),
            true
        ));
        assert!(!prefers_native_resume(
            ResumeBackend::from_key("codex"),
            false
        ));
        assert!(!prefers_native_resume(
            ResumeBackend::from_key("claude"),
            true
        ));
    }

    #[test]
    fn window_pressure_outranks_a_cold_prefix_in_the_reseed_step() {
        let full = Some((190_000, 200_000));
        let roomy = Some((20_000, 200_000));
        assert_eq!(
            reseed_step(full, 0.8, PrefixCacheState::Cold),
            ReseedStep::RebuildForWindow,
            "an obligation is asked before a choice"
        );
        assert_eq!(
            reseed_step(full, 0.8, PrefixCacheState::Warm),
            ReseedStep::RebuildForWindow
        );
        assert_eq!(
            reseed_step(roomy, 0.8, PrefixCacheState::Cold),
            ReseedStep::PriceColdRebuild
        );
        assert_eq!(
            reseed_step(None, 0.8, PrefixCacheState::Cold),
            ReseedStep::PriceColdRebuild
        );
        for state in [PrefixCacheState::Warm, PrefixCacheState::Unknown] {
            assert_eq!(reseed_step(roomy, 0.8, state), ReseedStep::Resume);
            assert_eq!(reseed_step(None, 0.8, state), ReseedStep::Resume);
        }
    }

    #[test]
    fn pricing_a_cold_rebuild_records_both_outcomes() {
        let ColdRebuild::Pays(rebuilt) = price_cold_rebuild(4_000, 10_000) else {
            panic!("a seed under half the prefix pays");
        };
        assert_eq!(rebuilt, CacheDecision::rebuilt());
        assert_eq!(rebuilt.decision, "digest_reseed");
        assert_eq!(rebuilt.cache_state, "cold");
        assert_eq!(rebuilt.reseed_skipped, None);
        assert_eq!(rebuilt.prefix_bytes, None);

        let ColdRebuild::Declined(kept) = price_cold_rebuild(6_000, 10_000) else {
            panic!("a seed over half the prefix is declined");
        };
        assert_eq!(kept.decision, "native_resume");
        assert_eq!(kept.cache_state, "cold");
        assert_eq!(
            kept.reseed_skipped,
            Some(RESEED_SKIPPED_INSUFFICIENT_SAVINGS)
        );
        assert_eq!(kept.prefix_bytes, Some(10_000));
        assert_eq!(kept.candidate_seed_bytes, Some(6_000));
        assert_eq!(kept.threshold, Some(COLD_RESEED_MIN_SAVINGS_RATIO));
    }
}
