//! Per-turn semantic vectors: the corpus the search semantic lane retrieves
//! over, and the sweep that keeps it current.
//!
//! ## Why a turn
//!
//! A turn is the smallest stretch of a conversation that holds a complete
//! exchange — an ask and its answer. The two stores either side of it are the
//! wrong grain for "where did we discuss this?": `resource_embeddings` folds a
//! whole session into one vector, so a long session blurs every topic it
//! touched, while a single event is a fragment of a thought whose vocabulary
//! only makes sense beside its neighbors.
//!
//! ## Why the turn's text, not a pool of its events' vectors
//!
//! The embed worker already computes a vector per event, so pooling those would
//! be free on the live path. It is the wrong choice anyway, because the corpus
//! has to be built ONE way. Nothing persisted those per-event vectors, so
//! backfilling history by pooling would mean re-embedding every historical
//! event individually — more gateway calls than one per turn, not fewer, since
//! a turn holds many events. Pairing a pooled live path with a
//! single-embedding backfill instead would put two differently-constructed
//! vector families into one cosine-comparable space, and a query vector can
//! only be compared honestly against one construction.
//!
//! Embedding the turn's text also gives the model the turn as one document —
//! the question and its answer together — rather than a centroid that drifts
//! toward the corpus mean as a turn grows, which is exactly where a hotspot
//! matters most.
//!
//! ## One pass for live and history
//!
//! Because there is one construction there is one code path: [`embed_pending`]
//! takes the newest turns that have ended and carry no vector yet, and embeds
//! them. Run from the worker's timer it is the live path; run repeatedly it
//! walks history recent-first. It is idempotent, resumable, and self-healing —
//! a turn missed by a failed pass is simply still pending on the next one.

use std::collections::HashMap;

use super::client::{EmbeddingClient, InputType, COHERE_MODEL};
use super::extract_embeddable_text;
use super::vector;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::{params, Value};

/// Width of a stored span vector.
///
/// Cohere Embed v4 is Matryoshka-style, so the gateway's `dims` parameter
/// returns a genuinely reduced vector rather than a truncation the caller has
/// to renormalize. 256 dimensions keeps a 100k-turn workspace in the tens of
/// megabytes, which is small enough that a project-scoped brute-force cosine
/// scan needs no vector index.
pub const SPAN_DIMS: u32 = 256;

/// Character budget for one turn's text.
///
/// Generous enough to carry a substantial exchange, bounded so a single
/// runaway turn cannot dominate a batch's request size.
const TURN_TEXT_BUDGET: usize = 16_000;

/// Ceiling on turns per gateway call.
const EMBED_BATCH: usize = 16;

/// Ceiling on characters per gateway call.
///
/// Batching by count alone is what makes an oversized request possible: a batch
/// of sixteen turns is trivial when they are short and enormous when they are
/// all at the per-turn budget. Bounding the request itself means no batch can
/// be too large to accept, so a gateway failure is transient and retrying is
/// the right response — rather than a poison batch the sweep re-forms and
/// re-fails forever, since it always takes the same newest-first page.
const EMBED_BATCH_CHARS: usize = 48_000;

/// Turns considered by one sweep pass. One pass is one bounded unit of work;
/// history is walked by repeating passes, not by one long one.
pub const SWEEP_LIMIT: usize = 64;

/// How long to wait after a pass that found nothing. This is the steady-state
/// cadence, so it also bounds how stale the lane can be: a turn becomes
/// semantically searchable within about a minute of ending.
const SWEEP_IDLE_SECS: u64 = 60;

/// How long to wait between passes while there is still history to walk. Short
/// enough that a first backfill finishes in well under an hour, long enough
/// that it does not monopolize the gateway that live embedding shares.
const SWEEP_BACKLOG_PAUSE_SECS: u64 = 2;

/// How long to wait after a failing pass. A gateway that is refusing calls
/// should not be retried on the backlog cadence.
const SWEEP_ERROR_BACKOFF_SECS: u64 = 300;

/// `dims` marking a turn whose prose could not be established — an archived
/// event that did not reconstruct.
///
/// It has to be a ROW, not just a counter in the pass summary. A deferred turn
/// is still pending, and the walk is newest-first, so it is by construction
/// newer than everything not yet reached: left unmarked it reappears at the
/// head of every subsequent page, and once enough accumulate the page is
/// entirely deferred, the sweep re-forms it every couple of seconds forever and
/// never reaches the rest of history. That is precisely the poison-batch
/// hazard [`EMBED_BATCH_CHARS`] exists to prevent, arriving by another door.
///
/// Distinct from the `dims = 0` tombstone because it means the opposite thing:
/// a tombstone is a settled answer, this is an unsettled one, and
/// [`Scope::Deferred`] retries it on the slow cadence. Both are excluded from
/// retrieval by `dims > 0`.
const DEFERRED_DIMS: i32 = -1;

/// How often a caught-up sweep re-examines all of history.
///
/// The history scan is an anti-join over every turn, so a pass that finds
/// nothing still costs one index probe per turn ever recorded. Running that
/// once a minute forever would make idle cost grow with the size of history
/// rather than with the work there is to do. It cannot be dropped entirely: a
/// turn that was still running when the sweep passed its position becomes
/// eligible later without ever crossing the live boundary, so only a full scan
/// finds it. This is the bound on how long such a turn waits.
const HISTORY_RECONCILE_SECS: u64 = 1800;

/// Event types whose prose describes what a turn was ABOUT.
///
/// Tool results are excluded deliberately: they are bulk (file contents,
/// command output, search results) that would swamp the turn's actual subject,
/// and they are the same rows the embed worker has always declined to embed.
/// `text` is the legacy spelling of an assistant message.
const CONTENT_EVENT_TYPES: &[&str] = &["assistant", "user", "text"];

/// A turn awaiting a vector, with the coordinates the stored row denormalizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTurn {
    pub turn_id: String,
    pub session_id: String,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
    pub sequence: i32,
}

/// One turn's vector as the query path reads it back.
#[derive(Debug, Clone)]
pub struct TurnVector {
    pub turn_id: String,
    pub session_id: String,
    pub job_id: Option<String>,
    pub sequence: i32,
    pub embedding: Vec<f32>,
}

/// What one sweep pass did, for the caller's log line.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepSummary {
    /// Turns that received a vector.
    pub embedded: usize,
    /// Turns recorded as carrying no embeddable prose.
    pub tombstoned: usize,
    /// Turns left pending because whether they carry prose could not be
    /// established — an archived event that did not reconstruct. Recording
    /// those as empty would erase them from the corpus permanently.
    pub deferred: usize,
    /// Turns still pending after this pass — true means there is more work, so
    /// the caller can keep passes coming.
    pub remaining: bool,
}

/// Which turns a sweep pass will consider.
///
/// Two independent questions decide a pass, and conflating them strands turns:
///
/// - **Consent.** Uploading transcripts that predate this install is
///   irreversible, so [`Scope::All`] requires a connected account. A turn this
///   install watched happen is different in kind: the embed worker already puts
///   every live event's text through the same gateway everywhere, which is how
///   vibe colors exist when logged out, so persisting the vector changes what
///   is KEPT, not what is sent.
/// - **Correctness.** A turn can miss the pass that reaches it — still running
///   at the time, unreadable, or its write failed — and the fast pass only
///   looks above the newest embedded turn, so such a turn falls permanently
///   below the floor. Recovering it means re-scanning from this install's own
///   boundary, which is not a history upload and needs no account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Un-embedded turns created at or after `since`, newest first.
    ///
    /// The fast pass floors this at the newest embedded turn; the reconcile
    /// pass floors it at this install's participation boundary, which is what
    /// recovers a turn stranded below the fast floor.
    Since(i64),
    /// Every un-embedded turn, including transcripts predating this install.
    /// Requires a connected account.
    All,
    /// Turns an earlier pass could not read, retried. They carry a marker row
    /// so they stay OUT of the scopes above; see [`DEFERRED_DIMS`].
    ///
    /// `since` floors the retry at this install's boundary when no account is
    /// connected. A turn is deferred because an archived event would not
    /// reconstruct, and archival is what makes a turn old, so this population
    /// is overwhelmingly pre-boundary — the very transcripts [`Scope::All`] is
    /// gated to protect. Like every other scope, it has to answer both
    /// questions above, not just the correctness one.
    Deferred { since: Option<i64> },
}

/// Run the sweep forever on the current tokio runtime.
///
/// This is its own task rather than another arm of the embed worker's select
/// loop, because a first backfill walks tens of thousands of turns and that
/// must not stall the live coloring and position folding the worker owes each
/// incoming event.
///
/// The loop is self-pacing: while history remains it comes straight back for
/// another pass, and once it is caught up it settles onto the idle cadence,
/// which is also the freshness bound for newly ended turns.
/// `has_account` reports a CONNECTED ACCOUNT, not merely a usable gateway
/// token. Every install registers an anonymous device token, so a token alone
/// says nothing about consent; see [`Scope`].
pub fn spawn_turn_embedding_sweep(
    client: EmbeddingClient,
    db: std::sync::Arc<LocalDb>,
    has_account: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
) {
    tokio::spawn(async move {
        let mut last_reconcile = std::time::Instant::now();
        let mut catching_up = true;
        loop {
            // Everything below is scoped by this boundary, so an install that
            // has not established one yet does nothing at all rather than
            // guessing. Re-read each loop because establishing it can fail.
            let Some(boundary) = install_boundary(&db).await else {
                tokio::time::sleep(std::time::Duration::from_secs(SWEEP_ERROR_BACKOFF_SECS)).await;
                continue;
            };

            // Fast pass: the newest edge, so a turn that just ended becomes
            // searchable promptly, ahead of anything still being walked.
            let live_floor = live_boundary(&db, boundary).await;
            let mut wait = match run_pass(&db, &client, Scope::Since(live_floor)).await {
                Some(remaining) if remaining => SWEEP_BACKLOG_PAUSE_SECS,
                Some(_) => SWEEP_IDLE_SECS,
                None => SWEEP_ERROR_BACKOFF_SECS,
            };

            // Wider pass, on a slower cadence: an anti-join whose cost tracks
            // how much history exists rather than how much work there is, so a
            // caught-up pass finding nothing must not pay it every minute.
            let due = catching_up
                || last_reconcile.elapsed()
                    >= std::time::Duration::from_secs(HISTORY_RECONCILE_SECS);
            if due {
                last_reconcile = std::time::Instant::now();
                // With an account this subsumes reconciliation. Without one it
                // narrows to this install's own region — which still has to be
                // reconciled, or a turn that missed its pass is stranded
                // forever on exactly the installs the consent split protects.
                let scope = if has_account() {
                    Scope::All
                } else {
                    Scope::Since(boundary)
                };
                match run_pass(&db, &client, scope).await {
                    Some(remaining) => {
                        catching_up = remaining;
                        if remaining {
                            wait = SWEEP_BACKLOG_PAUSE_SECS;
                        }
                    }
                    None => wait = SWEEP_ERROR_BACKOFF_SECS,
                }
                // Retry whatever could not be read before. Bounded and out of
                // the way: these carry marker rows, so they never crowd the
                // pages above.
                //
                // Floored by the same consent decision as the pass above, and
                // not merely for symmetry: a turn is deferred because an
                // archived event would not reconstruct, and archival is what
                // makes a turn old, so the deferred population is overwhelmingly
                // PRE-boundary. Unfloored, disconnecting an account would leave
                // this quietly retrying uploads of exactly the transcripts the
                // gate exists to protect.
                let deferred = Scope::Deferred {
                    since: (!has_account()).then_some(boundary),
                };
                if run_pass(&db, &client, deferred).await.is_none() {
                    wait = SWEEP_ERROR_BACKOFF_SECS;
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
        }
    });
}

/// When this install began participating in the transcript, established once on
/// first sweep and never moved. `None` means it is not established yet, so the
/// caller must do nothing this loop and try again.
///
/// Everything at or after it is this install's own region, which is reconciled
/// regardless of account. Everything before it is pre-existing history, which
/// is not uploaded without one.
///
/// Every read here is matched rather than flattened, because the boundary is
/// durable and a wrong one is not recoverable. A failed read must never look
/// like an empty `turns` table: both would yield `0`, and while `0` is exactly
/// right for a fresh install with nothing to protect, on an existing workspace
/// it means "all of history is in scope" — permanently inverting the operator's
/// ruling on the strength of one transient error. The seed row already
/// distinguishes the states: NULL is "not established", which is what a failure
/// must leave behind.
async fn install_boundary(db: &LocalDb) -> Option<i64> {
    match db
        .query_one(
            "SELECT install_boundary FROM turn_embedding_state WHERE id = 1",
            (),
            |row| row.opt_i64(0),
        )
        .await
    {
        Ok(Some(boundary)) => return Some(boundary),
        // NULL: never established, so fall through and establish it.
        Ok(None) => {}
        Err(error) => {
            log::warn!("turn embeddings: reading the install boundary failed: {error}");
            return None;
        }
    }

    // First sweep on this install. Anchor to the newest turn that already
    // exists: everything up to here predates our participation. No turns at
    // all legitimately means zero — there is no history to protect.
    let newest = match db
        .query_one("SELECT MAX(created_at) FROM turns", (), |row| {
            row.opt_i64(0)
        })
        .await
    {
        Ok(newest) => newest.unwrap_or(0),
        Err(error) => {
            log::warn!("turn embeddings: establishing the install boundary failed: {error}");
            return None;
        }
    };
    if let Err(error) = db
        .execute(
            "UPDATE turn_embedding_state SET install_boundary = ?1 WHERE id = 1",
            params![newest],
        )
        .await
    {
        log::warn!("turn embeddings: recording the install boundary failed: {error}");
        return None;
    }
    Some(newest)
}

/// Run one pass, logging what it did. `None` means the pass failed; `Some(more)`
/// reports whether work remains in that scope.
async fn run_pass(db: &LocalDb, client: &EmbeddingClient, scope: Scope) -> Option<bool> {
    match embed_pending(db, client, scope, SWEEP_LIMIT).await {
        Ok(Some(summary)) => {
            if summary.embedded > 0 || summary.tombstoned > 0 || summary.deferred > 0 {
                log::debug!(
                    "turn embeddings ({scope:?}): {} embedded, {} empty, {} deferred, more: {}",
                    summary.embedded,
                    summary.tombstoned,
                    summary.deferred,
                    summary.remaining,
                );
            }
            Some(summary.remaining)
        }
        // No gateway token at all. The lane stays as sparse as it is and search
        // keeps answering from the full-text index alone.
        Ok(None) => Some(false),
        Err(error) => {
            log::warn!("turn embeddings ({scope:?}) failed: {error}");
            None
        }
    }
}

/// The floor for the fast pass: the newest turn already embedded, or the
/// install boundary when nothing has been embedded yet.
///
/// This one DOES flatten a read error into its fallback, and the difference
/// from [`install_boundary`] is the fallback, not the pattern. Falling back to
/// the install boundary only widens this pass within the install's own region,
/// where the wider reconcile pass is already permitted to go — it costs a
/// larger scan, never a broader upload. Nothing here is persisted either, so a
/// bad value lasts one loop.
async fn live_boundary(db: &LocalDb, started_at: i64) -> i64 {
    db.query_one(
        "SELECT MAX(t.created_at) FROM turn_embeddings te
           JOIN turns t ON t.id = te.turn_id",
        (),
        |row| row.opt_i64(0),
    )
    .await
    .ok()
    .flatten()
    .unwrap_or(started_at)
}

/// Embed the newest turns in `scope` that have ended and have no vector yet.
///
/// Returns `Ok(None)` when no gateway token is available at all: nothing is
/// read, nothing is written, and the semantic lane simply stays as sparse as it
/// was. That is the silent degrade — search falls back to full text.
///
/// An error leaves every turn in the pass pending. That is the point: a
/// tombstone must mean "swept successfully, no prose found" and nothing else,
/// because nothing ever clears one.
pub async fn embed_pending(
    db: &LocalDb,
    client: &EmbeddingClient,
    scope: Scope,
    limit: usize,
) -> Result<Option<SweepSummary>, String> {
    // Ask the cheap question first. Discovering the absence of a token at the
    // gateway call would mean loading and reconstructing a page of turns on
    // every pass, forever, to learn nothing.
    if !client.has_token() {
        return Ok(None);
    }

    let pending = load_pending_turns(db, scope, limit)
        .await
        .map_err(|error| format!("loading pending turns: {error}"))?;
    if pending.is_empty() {
        return Ok(Some(SweepSummary::default()));
    }

    // A read failure here must NOT reach the tombstone path below. Reporting
    // "these turns have no text" when the truth is "I could not tell you" would
    // erase a page of history from the corpus permanently, and the only trace
    // would be a log line.
    let prose = load_turn_texts(db, &pending)
        .await
        .map_err(|error| format!("loading turn text: {error}"))?;
    let mut summary = SweepSummary {
        remaining: pending.len() == limit,
        ..SweepSummary::default()
    };

    let mut texts: HashMap<String, String> = HashMap::new();
    let mut with_text: Vec<PendingTurn> = Vec::new();
    let mut empty: Vec<PendingTurn> = Vec::new();
    let mut deferred: Vec<PendingTurn> = Vec::new();
    for turn in pending {
        match prose.get(&turn.turn_id) {
            Some(found) if !found.text.is_empty() => {
                texts.insert(turn.turn_id.clone(), found.text.clone());
                with_text.push(turn);
            }
            // Nothing extractable, and an archived event that did not
            // reconstruct could be the reason. Its blobs may arrive later, so
            // this is recorded as unsettled rather than as an answer.
            Some(found) if found.uncertain => deferred.push(turn),
            _ => empty.push(turn),
        }
    }

    // Marking a deferral is what lets the sweep move past it. Counting it
    // would leave the turn at the head of every future page.
    for turn in &deferred {
        if let Err(error) = upsert_turn_vector(db, turn, &[], DEFERRED_DIMS).await {
            log::warn!(
                "turn embeddings: deferral for {} failed: {error}",
                turn.turn_id
            );
        } else {
            summary.deferred += 1;
        }
    }

    // A turn of pure tool results has nothing to embed. Record that fact so the
    // turn is not reconsidered by every future pass.
    for turn in &empty {
        if let Err(error) = upsert_turn_vector(db, turn, &[], 0).await {
            log::warn!(
                "turn embeddings: tombstone for {} failed: {error}",
                turn.turn_id
            );
        } else {
            summary.tombstoned += 1;
        }
    }

    for chunk in batches(&with_text, &texts) {
        let batch: Vec<String> = chunk
            .iter()
            .map(|turn| texts.remove(&turn.turn_id).unwrap_or_default())
            .collect();
        match client
            .embed(batch, InputType::SearchDocument, Some(SPAN_DIMS))
            .await
        {
            // The token lapsed mid-pass: leave every remaining turn pending and
            // say so, rather than tombstoning turns we simply could not reach.
            Ok(None) => return Ok(None),
            Ok(Some(vectors)) if vectors.len() != chunk.len() => {
                return Err(format!(
                    "embed gateway returned {} vectors for {} turns",
                    vectors.len(),
                    chunk.len()
                ));
            }
            Ok(Some(vectors)) => {
                for (turn, embedding) in chunk.iter().zip(vectors.iter()) {
                    let bytes = vector::to_bytes(embedding);
                    match upsert_turn_vector(db, turn, &bytes, embedding.len() as i32).await {
                        Ok(()) => summary.embedded += 1,
                        Err(error) => log::warn!(
                            "turn embeddings: upsert for {} failed: {error}",
                            turn.turn_id
                        ),
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }

    Ok(Some(summary))
}

/// Split turns into gateway calls bounded by BOTH count and total characters.
///
/// A single turn always forms a batch even if it alone exceeds the character
/// bound, because the per-turn budget already caps it and a turn that could
/// never be batched could never be embedded.
fn batches<'a>(
    turns: &'a [PendingTurn],
    texts: &HashMap<String, String>,
) -> Vec<&'a [PendingTurn]> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut chars = 0;
    for index in 0..turns.len() {
        let length = texts.get(&turns[index].turn_id).map_or(0, String::len);
        let full =
            index - start >= EMBED_BATCH || (index > start && chars + length > EMBED_BATCH_CHARS);
        if full {
            batches.push(&turns[start..index]);
            start = index;
            chars = 0;
        }
        chars += length;
    }
    if start < turns.len() {
        batches.push(&turns[start..]);
    }
    batches
}

/// The newest ended turns carrying no vector.
///
/// A turn still `pending` or `running` is excluded because it can still gain
/// events; every other state is terminal (a resumption opens a SUCCESSOR turn,
/// it does not reopen this one). Newest first, so a workspace's useful history
/// becomes searchable before its archaeology.
async fn load_pending_turns(
    db: &LocalDb,
    scope: Scope,
    limit: usize,
) -> DbResult<Vec<PendingTurn>> {
    let row_to_turn = |row: &cairn_db::turso::Row| {
        Ok(PendingTurn {
            turn_id: row.text(0)?,
            session_id: row.text(1)?,
            job_id: row.opt_text(2)?,
            project_id: row.opt_text(3)?,
            sequence: row.i64(4)? as i32,
        })
    };
    const COLUMNS: &str =
        "SELECT t.id, t.session_id, COALESCE(t.job_id, r.job_id), r.project_id, t.sequence
           FROM turns t
           LEFT JOIN runs r ON r.id = t.run_id";
    // A turn with ANY row — vector, tombstone, or deferral — is out of these
    // scopes. That exclusion is what stops a deferral from re-forming the same
    // page forever.
    const PENDING: &str = "WHERE t.state NOT IN ('pending', 'running')
            AND NOT EXISTS (SELECT 1 FROM turn_embeddings te WHERE te.turn_id = t.id)";
    match scope {
        // Bounded by the index range, so its cost tracks how many turns have
        // happened since the floor, not how much history exists.
        Scope::Since(since) => {
            db.query_all(
                &format!(
                    "{COLUMNS} {PENDING} AND t.created_at >= ?1 \
                     ORDER BY t.created_at DESC LIMIT ?2"
                ),
                params![since, limit as i64],
                row_to_turn,
            )
            .await
        }
        Scope::All => {
            db.query_all(
                &format!("{COLUMNS} {PENDING} ORDER BY t.created_at DESC LIMIT ?1"),
                params![limit as i64],
                row_to_turn,
            )
            .await
        }
        // Least-recently-attempted first, so retries rotate rather than
        // hammering the same unreadable turns every reconcile.
        Scope::Deferred { since } => {
            db.query_all(
                &format!(
                    "{COLUMNS} JOIN turn_embeddings te ON te.turn_id = t.id \
                     WHERE te.dims = ?1 AND t.created_at >= ?2 \
                     ORDER BY te.updated_at ASC LIMIT ?3"
                ),
                params![
                    DEFERRED_DIMS as i64,
                    since.unwrap_or(i64::MIN),
                    limit as i64
                ],
                row_to_turn,
            )
            .await
        }
    }
}

/// Assemble each turn's embeddable text from its content events.
///
/// Archived events reconstruct first — a compressed row's inline `data` holds a
/// stub, and embedding the stub would poison the vector with storage
/// bookkeeping instead of the conversation. This mirrors what the search index
/// does before indexing an archived event.
async fn load_turn_texts(
    db: &LocalDb,
    turns: &[PendingTurn],
) -> DbResult<HashMap<String, TurnProse>> {
    if turns.is_empty() {
        return Ok(HashMap::new());
    }
    let turn_ids: Vec<String> = turns.iter().map(|turn| turn.turn_id.clone()).collect();
    let events = load_turn_events(db, &turn_ids).await?;

    let (ids, events): (Vec<String>, Vec<crate::models::Event>) = events.into_iter().unzip();
    let events = crate::storage::reconstruct_events(db, events).await;

    let mut parts: HashMap<String, Vec<String>> = HashMap::new();
    let mut uncertain: HashMap<String, bool> = HashMap::new();
    for (turn_id, event) in ids.into_iter().zip(events) {
        let text = extract_embeddable_text(&event.data)
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty());
        match text {
            Some(text) => parts.entry(turn_id).or_default().push(text),
            // An archived event yielding nothing is ambiguous: either it holds
            // no prose, or its reconstruction failed and the prose is still out
            // there. `reconstruct_events` rewrites `data` in place and leaves
            // `storage_mode` set either way, so the two are indistinguishable
            // here — which is exactly why this is recorded as doubt rather than
            // resolved as absence.
            None => {
                let entry = uncertain.entry(turn_id).or_insert(false);
                *entry |= event.storage_mode.is_some();
            }
        }
    }

    let mut prose: HashMap<String, TurnProse> = parts
        .into_iter()
        .map(|(turn_id, parts)| {
            (
                turn_id,
                TurnProse {
                    text: fit_to_budget(parts, TURN_TEXT_BUDGET),
                    uncertain: false,
                },
            )
        })
        .collect();
    for (turn_id, was_archived) in uncertain {
        prose.entry(turn_id).or_insert(TurnProse {
            text: String::new(),
            uncertain: was_archived,
        });
    }
    Ok(prose)
}

/// A turn's embeddable text, and whether its absence can be trusted.
struct TurnProse {
    text: String,
    /// The turn yielded no text AND held an archived event, so "no prose" may
    /// really be "could not reconstruct". Never tombstone on this.
    uncertain: bool,
}

/// A turn's most representative content event, for rendering a result row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExcerpt {
    pub event_id: String,
    pub event_type: String,
    pub text: String,
}

/// The most substantial content event of each turn, with a plain excerpt.
///
/// The semantic lane retrieves a whole turn, but a result row shows a passage.
/// The turn's longest content event is its most representative one, and using
/// it also gives the row the event id every other transcript result carries,
/// so a semantic hit addresses its conversation exactly like a text hit does.
pub async fn turn_excerpts(
    db: &LocalDb,
    turn_ids: &[String],
    excerpt_chars: usize,
) -> HashMap<String, TurnExcerpt> {
    if turn_ids.is_empty() {
        return HashMap::new();
    }
    // A failure here drops the affected hits from the lane, which degrades the
    // answer to full text. That is safe in a way the sweep's equivalent is not:
    // nothing is persisted, so the next search simply tries again.
    let events = match load_turn_events(db, turn_ids).await {
        Ok(events) => events,
        Err(error) => {
            log::debug!("semantic search: loading turn excerpts failed: {error}");
            return HashMap::new();
        }
    };
    let (ids, events): (Vec<String>, Vec<crate::models::Event>) = events.into_iter().unzip();
    let events = crate::storage::reconstruct_events(db, events).await;

    let mut best: HashMap<String, TurnExcerpt> = HashMap::new();
    for (turn_id, event) in ids.into_iter().zip(events) {
        let Some(text) = extract_embeddable_text(&event.data) else {
            continue;
        };
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let longer = best
            .get(&turn_id)
            .is_none_or(|current| current.text.len() < text.len());
        if longer {
            best.insert(
                turn_id,
                TurnExcerpt {
                    event_id: event.id.clone(),
                    event_type: event.event_type.clone(),
                    text: text.chars().take(excerpt_chars).collect(),
                },
            );
        }
    }
    best
}

/// Load the content events of each turn in sequence order, paired with the turn
/// they belong to.
async fn load_turn_events(
    db: &LocalDb,
    turn_ids: &[String],
) -> DbResult<Vec<(String, crate::models::Event)>> {
    use crate::runs::queries::{event_from_row, EVENT_COLUMNS, EVENT_COLUMN_COUNT};

    let turn_placeholders = (1..=turn_ids.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let type_placeholders = (1..=CONTENT_EVENT_TYPES.len())
        .map(|index| format!("?{}", index + turn_ids.len()))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {EVENT_COLUMNS}, turn_id
           FROM events
          WHERE turn_id IN ({turn_placeholders})
            AND event_type IN ({type_placeholders})
          ORDER BY sequence ASC"
    );
    let values: Vec<Value> = turn_ids
        .iter()
        .map(|id| Value::Text(id.clone()))
        .chain(
            CONTENT_EVENT_TYPES
                .iter()
                .map(|kind| Value::Text((*kind).to_string())),
        )
        .collect();

    db.query_all(&sql, values, move |row| {
        let event = event_from_row(row)?;
        let turn_id = row.text(EVENT_COLUMN_COUNT)?;
        Ok((turn_id, event))
    })
    .await
}

/// Join `parts` under a character budget, sharing it fairly.
///
/// Head-truncating the joined text would keep only a turn's opening, and a
/// turn's conclusion is often where its subject is actually named. So short
/// parts are kept whole and the remaining budget is divided among the long
/// ones: every part of the turn is represented, and no single long message
/// crowds the rest out.
fn fit_to_budget(parts: Vec<String>, budget: usize) -> String {
    const SEPARATOR: &str = "\n\n";
    if parts.is_empty() {
        return String::new();
    }
    let separators = SEPARATOR.len() * parts.len().saturating_sub(1);
    let content_budget = budget.saturating_sub(separators);
    if parts.iter().map(String::len).sum::<usize>() <= content_budget {
        return parts.join(SEPARATOR);
    }

    // Water-fill shortest first: each pass offers an equal share of what is
    // left, and whatever a short part does not use raises the share for the
    // parts still to be allotted.
    let mut order: Vec<usize> = (0..parts.len()).collect();
    order.sort_by_key(|index| parts[*index].len());
    let mut allowance = vec![0usize; parts.len()];
    let mut remaining_budget = content_budget;
    for (allotted, index) in order.into_iter().enumerate() {
        let share = remaining_budget / (parts.len() - allotted);
        let take = parts[index].len().min(share);
        allowance[index] = take;
        remaining_budget -= take;
    }

    parts
        .iter()
        .zip(allowance)
        .map(|(part, take)| truncate_chars(part, take))
        .collect::<Vec<_>>()
        .join(SEPARATOR)
}

/// Truncate to at most `max_bytes`, never splitting a character.
fn truncate_chars(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let end = text
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    &text[..end]
}

/// Persist one turn's vector. An empty `embedding` with `dims = 0` is the
/// tombstone for a turn that carried no embeddable prose.
async fn upsert_turn_vector(
    db: &LocalDb,
    turn: &PendingTurn,
    embedding: &[u8],
    dims: i32,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO turn_embeddings(
             turn_id, session_id, job_id, project_id, sequence,
             embedding, model, dims, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
         ON CONFLICT(turn_id) DO UPDATE SET
             embedding = excluded.embedding,
             model = excluded.model,
             dims = excluded.dims,
             updated_at = excluded.updated_at",
        params![
            turn.turn_id.as_str(),
            turn.session_id.as_str(),
            turn.job_id.clone(),
            turn.project_id.clone(),
            turn.sequence as i64,
            embedding.to_vec(),
            COHERE_MODEL,
            dims as i64,
            now
        ],
    )
    .await
    .map(|_| ())
}

/// Every stored span vector, optionally narrowed to one project.
///
/// Tombstones (`dims = 0`) are excluded — they record a sweep, not a vector.
/// A row whose width does not match the caller's query vector is dropped by the
/// scan rather than compared, so a model or dimension change degrades to
/// "fewer semantic hits" instead of nonsense similarities.
pub async fn load_vectors(db: &LocalDb, project_id: Option<&str>) -> DbResult<Vec<TurnVector>> {
    let row_to_vector = |row: &cairn_db::turso::Row| {
        Ok(TurnVector {
            turn_id: row.text(0)?,
            session_id: row.text(1)?,
            job_id: row.opt_text(2)?,
            sequence: row.i64(3)? as i32,
            embedding: vector::from_bytes(&row.blob(4)?),
        })
    };
    const COLUMNS: &str = "SELECT turn_id, session_id, job_id, sequence, embedding
                             FROM turn_embeddings";
    match project_id {
        Some(project_id) => {
            db.query_all(
                &format!("{COLUMNS} WHERE project_id = ?1 AND dims > 0"),
                params![project_id.to_string()],
                row_to_vector,
            )
            .await
        }
        None => {
            db.query_all(&format!("{COLUMNS} WHERE dims > 0"), (), row_to_vector)
                .await
        }
    }
}

// ===== retrieval =====

/// One turn ranked against a query by cosine similarity.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredTurn {
    pub turn_id: String,
    pub session_id: String,
    pub job_id: Option<String>,
    pub sequence: i32,
    pub similarity: f32,
}

/// How long a resident copy of the vectors is trusted before it is reloaded.
///
/// Matched to the sweep's idle cadence: a newly embedded turn becomes
/// retrievable about as fast as it becomes embedded, and no faster, which is
/// invisible to anyone searching.
const CACHE_TTL_SECS: u64 = 60;

/// Cache key standing for "every project".
const ALL_PROJECTS: &str = "";

// This lane cannot tell you whether it found anything, and does not try.
//
// A dense retriever always returns nearest neighbors, and the geometry of
// "nearest" looks the same whether or not the query has a real answer. Both
// obvious guards were measured against this workspace's history, and both
// failed:
//
// - Absolute similarity. A nonsense query's best hit scored 0.308 while
//   genuine matches for a real query scored as low as 0.287. Cosine magnitude
//   tracks the query's own shape, not relevance, so scores are comparable
//   within a query and not across queries.
// - Prominence above the corpus. Normalizing to a z-score over the query's own
//   distribution fares no better: `asdfjkl qwertyuiop` peaked at +4.75 standard
//   deviations, above most genuine matches for a real query. The distributions
//   are simply the same shape.
//
// So there is no threshold to tune, and the judgment is made where the evidence
// exists: `crate::search` runs this lane only when the text index found
// something. A query whose words appear nowhere in the corpus gets silence,
// which is both the honest answer and the one search already gave.

struct CachedVectors {
    loaded_at: std::time::Instant,
    vectors: std::sync::Arc<Vec<TurnVector>>,
}

/// The semantic lane's read side: embeds a query and ranks stored span vectors
/// against it.
///
/// The vectors live resident because the corpus is both small enough to scan
/// exhaustively and too large to re-read per query — the command palette
/// searches as the operator types, and a workspace's vectors are tens of
/// megabytes. Holding them makes each query pure arithmetic over a few tens of
/// thousands of short vectors.
pub struct SemanticSearch {
    client: EmbeddingClient,
    cache: tokio::sync::RwLock<HashMap<String, CachedVectors>>,
    /// Serializes cache misses. Without it, two searches arriving after the TTL
    /// lapses each load the whole corpus — tens of megabytes, twice — which a
    /// fast typist reaches routinely through the debounced palette.
    loading: tokio::sync::Mutex<()>,
}

impl SemanticSearch {
    pub fn new(client: EmbeddingClient) -> Self {
        Self {
            client,
            cache: tokio::sync::RwLock::new(HashMap::new()),
            loading: tokio::sync::Mutex::new(()),
        }
    }

    /// Rank stored turns against `query`, best first.
    ///
    /// Returns `None` whenever the lane cannot answer — no account connected,
    /// no vectors stored yet, or a gateway failure. Every one is the same
    /// instruction to the caller: serve the full-text answer unchanged. The
    /// lane is additive, never required.
    ///
    /// It does NOT judge whether its answer is any good; it cannot. See the
    /// note on that above.
    pub async fn rank_turns(
        &self,
        db: &LocalDb,
        project_id: Option<&str>,
        query: &str,
        top_k: usize,
    ) -> Option<Vec<ScoredTurn>> {
        if query.trim().is_empty() || top_k == 0 {
            return None;
        }
        let vectors = self.vectors(db, project_id).await?;
        if vectors.is_empty() {
            return None;
        }

        // Asymmetric embedding is the validated retrieval win: the corpus is
        // embedded as documents, the query as a query.
        let query_vector = match self
            .client
            .embed(
                vec![query.to_string()],
                InputType::SearchQuery,
                Some(SPAN_DIMS),
            )
            .await
        {
            Ok(Some(mut vectors)) => vectors.pop()?,
            Ok(None) => return None,
            Err(error) => {
                log::debug!("semantic search: embedding the query failed: {error}");
                return None;
            }
        };

        let mut scored: Vec<ScoredTurn> = vectors
            .iter()
            .filter(|turn| turn.embedding.len() == query_vector.len())
            .filter_map(|turn| {
                let similarity = vector::cosine_similarity(&query_vector, &turn.embedding);
                // A negative cosine is actively opposed, never a candidate.
                (similarity > 0.0).then(|| ScoredTurn {
                    turn_id: turn.turn_id.clone(),
                    session_id: turn.session_id.clone(),
                    job_id: turn.job_id.clone(),
                    sequence: turn.sequence,
                    similarity,
                })
            })
            .collect();
        if scored.is_empty() {
            return None;
        }
        scored.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.turn_id.cmp(&b.turn_id))
        });
        scored.truncate(top_k);
        Some(scored)
    }

    /// The resident vectors for a scope, reloading a stale or absent copy.
    async fn vectors(
        &self,
        db: &LocalDb,
        project_id: Option<&str>,
    ) -> Option<std::sync::Arc<Vec<TurnVector>>> {
        let key = project_id.unwrap_or(ALL_PROJECTS).to_string();
        if let Some(fresh) = self.cached(&key).await {
            return Some(fresh);
        }
        // Whoever wins the race loads; the rest re-check and take that result.
        let _guard = self.loading.lock().await;
        if let Some(fresh) = self.cached(&key).await {
            return Some(fresh);
        }
        let loaded = match load_vectors(db, project_id).await {
            Ok(loaded) => std::sync::Arc::new(loaded),
            Err(error) => {
                log::debug!("semantic search: loading span vectors failed: {error}");
                return None;
            }
        };
        self.cache.write().await.insert(
            key,
            CachedVectors {
                loaded_at: std::time::Instant::now(),
                vectors: loaded.clone(),
            },
        );
        Some(loaded)
    }

    /// The cached vectors for `key` if they are still within the TTL.
    async fn cached(&self, key: &str) -> Option<std::sync::Arc<Vec<TurnVector>>> {
        let cache = self.cache.read().await;
        let entry = cache.get(key)?;
        (entry.loaded_at.elapsed() < std::time::Duration::from_secs(CACHE_TTL_SECS))
            .then(|| entry.vectors.clone())
    }
}

/// How many turns have never been considered. Excludes turns already settled
/// (a vector or a tombstone) and turns marked deferred, which are retried
/// through [`Scope::Deferred`] rather than counted here. Not on any hot path.
pub async fn pending_count(db: &LocalDb) -> DbResult<i64> {
    db.query_one(
        "SELECT COUNT(*) FROM turns t
          WHERE t.state NOT IN ('pending', 'running')
            AND NOT EXISTS (SELECT 1 FROM turn_embeddings te WHERE te.turn_id = t.id)",
        (),
        |row| row.i64(0),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_parts_join_whole_under_budget() {
        let parts = vec!["alpha".to_string(), "beta".to_string()];
        assert_eq!(fit_to_budget(parts, 100), "alpha\n\nbeta");
    }

    #[test]
    fn empty_parts_yield_empty_text() {
        assert_eq!(fit_to_budget(vec![], 100), "");
    }

    #[test]
    fn a_long_part_is_trimmed_while_short_ones_survive_whole() {
        // The budget cannot hold everything, so the long part gives way — but
        // the short parts, which cost almost nothing, are kept intact. A plain
        // head truncation of the joined text would have dropped them entirely.
        let parts = vec![
            "short".to_string(),
            "x".repeat(500),
            "also short".to_string(),
        ];
        let fitted = fit_to_budget(parts, 100);
        assert!(fitted.starts_with("short\n\n"));
        assert!(fitted.ends_with("\n\nalso short"));
        assert!(fitted.len() <= 100);
    }

    #[test]
    fn every_part_is_represented_when_all_are_long() {
        let parts = vec!["a".repeat(400), "b".repeat(400), "c".repeat(400)];
        let fitted = fit_to_budget(parts, 90);
        assert!(fitted.contains('a'));
        assert!(fitted.contains('b'));
        assert!(fitted.contains('c'));
        assert!(fitted.len() <= 90);
    }

    #[test]
    fn batches_are_bounded_by_characters_not_only_by_count() {
        let turns: Vec<PendingTurn> = (0..4)
            .map(|index| PendingTurn {
                turn_id: format!("turn-{index}"),
                session_id: "s".to_string(),
                job_id: None,
                project_id: None,
                sequence: index,
            })
            .collect();
        let texts: HashMap<String, String> = turns
            .iter()
            .map(|turn| (turn.turn_id.clone(), "x".repeat(EMBED_BATCH_CHARS / 2 + 1)))
            .collect();

        // Four turns is well under the count ceiling, but two of them already
        // overflow the request.
        let batches = batches(&turns, &texts);
        assert_eq!(batches.len(), 4);
    }

    #[test]
    fn short_turns_fill_a_batch_up_to_the_count_ceiling() {
        let turns: Vec<PendingTurn> = (0..EMBED_BATCH as i32 + 3)
            .map(|index| PendingTurn {
                turn_id: format!("turn-{index}"),
                session_id: "s".to_string(),
                job_id: None,
                project_id: None,
                sequence: index,
            })
            .collect();
        let texts: HashMap<String, String> = turns
            .iter()
            .map(|turn| (turn.turn_id.clone(), "tiny".to_string()))
            .collect();

        let batches = batches(&turns, &texts);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), EMBED_BATCH);
        assert_eq!(batches[1].len(), 3);
    }

    #[test]
    fn truncation_never_splits_a_character() {
        // Each `é` is two bytes, so a 5-byte budget must stop at 4.
        assert_eq!(truncate_chars("ééé", 5), "éé");
        assert_eq!(truncate_chars("ééé", 6), "ééé");
    }

    // ===== database-backed =====

    use crate::api::ApiConfig;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("cairn-turn-embeddings-turso.db").await
    }

    /// A client with no device token at all.
    fn tokenless_client() -> EmbeddingClient {
        EmbeddingClient::new(ApiConfig::default(), std::sync::Arc::new(|| None))
    }

    /// A client holding a token. Safe for turns that need no gateway call — a
    /// turn with no prose forms no batch, so nothing is sent.
    fn tokened_client() -> EmbeddingClient {
        EmbeddingClient::new(
            ApiConfig::default(),
            std::sync::Arc::new(|| Some("test-token".to_string())),
        )
    }

    async fn seed_project(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'proj', '/tmp/project', 1, 1);
            ",
        )
        .await
        .unwrap();
    }

    /// Seed one turn with `events` as `(event_type, content)`, at `created_at`.
    async fn seed_turn(
        db: &LocalDb,
        turn_id: &str,
        state: &str,
        created_at: i64,
        events: &[(&str, &str)],
    ) {
        let run_id = format!("run-{turn_id}");
        let session_id = format!("session-{turn_id}");
        db.execute(
            "INSERT INTO runs(id, project_id, status, session_id, created_at, updated_at)
             VALUES (?1, 'project-1', 'live', ?2, ?3, ?3)",
            params![run_id.as_str(), session_id.as_str(), created_at],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, 1, ?4, ?5, ?5)",
            params![
                turn_id,
                session_id.as_str(),
                run_id.as_str(),
                state,
                created_at
            ],
        )
        .await
        .unwrap();
        for (index, (event_type, content)) in events.iter().enumerate() {
            db.execute(
                "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp,
                                    event_type, data, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?6)",
                params![
                    format!("{turn_id}-event-{index}"),
                    run_id.as_str(),
                    session_id.as_str(),
                    turn_id,
                    index as i64,
                    created_at,
                    *event_type,
                    serde_json::json!({ "content": content }).to_string()
                ],
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_turns_are_ended_ones_without_a_vector_newest_first() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-old", "complete", 100, &[("assistant", "a")]).await;
        seed_turn(&db, "turn-new", "complete", 300, &[("assistant", "b")]).await;
        // Still in flight: it can gain more events, so embedding it now would
        // capture half a thought.
        seed_turn(&db, "turn-live", "running", 400, &[("assistant", "c")]).await;
        // Already embedded.
        seed_turn(&db, "turn-done", "complete", 200, &[("assistant", "d")]).await;
        upsert_turn_vector(
            &db,
            &PendingTurn {
                turn_id: "turn-done".to_string(),
                session_id: "session-turn-done".to_string(),
                job_id: None,
                project_id: Some("project-1".to_string()),
                sequence: 1,
            },
            &vector::to_bytes(&[0.5, 0.5]),
            2,
        )
        .await
        .unwrap();

        let pending = load_pending_turns(&db, Scope::All, 10).await.unwrap();
        let ids: Vec<&str> = pending.iter().map(|turn| turn.turn_id.as_str()).collect();
        assert_eq!(ids, vec!["turn-new", "turn-old"]);
        assert_eq!(pending[0].project_id.as_deref(), Some("project-1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_tombstoned_turn_is_never_reconsidered() {
        let db = migrated_db().await;
        seed_project(&db).await;
        // Nothing but a tool result: no prose to embed, ever.
        seed_turn(
            &db,
            "turn-tools",
            "complete",
            100,
            &[("tool_result", "12345 rows")],
        )
        .await;

        assert_eq!(pending_count(&db).await.unwrap(), 1);
        let summary = embed_pending(&db, &tokened_client(), Scope::All, 10)
            .await
            .unwrap()
            .expect("a turn with no text needs no gateway call");
        assert_eq!(summary.tombstoned, 1);
        assert_eq!(summary.embedded, 0);
        assert_eq!(pending_count(&db).await.unwrap(), 0);

        // The tombstone is not a vector: the scan must not return it.
        assert!(load_vectors(&db, Some("project-1"))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn without_an_account_nothing_is_written_and_the_turn_stays_pending() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(
            &db,
            "turn-prose",
            "complete",
            100,
            &[("assistant", "scheduling fairness")],
        )
        .await;

        // The silent degrade: no token, no call, no tombstone. The turn must
        // stay pending so it is picked up once a token exists — recording it as
        // empty would lose it permanently.
        assert_eq!(
            embed_pending(&db, &tokenless_client(), Scope::All, 10)
                .await
                .unwrap(),
            None
        );
        assert_eq!(pending_count(&db).await.unwrap(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_live_scope_leaves_pre_existing_history_alone() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-old", "complete", 100, &[("assistant", "a")]).await;
        seed_turn(&db, "turn-new", "complete", 300, &[("assistant", "b")]).await;

        // An install with no connected account never walks back through
        // transcripts that predate it; only turns it watched happen are in
        // scope. Uploading history is the one part that cannot be undone.
        let live = load_pending_turns(&db, Scope::Since(200), 10)
            .await
            .unwrap();
        assert_eq!(
            live.iter().map(|t| t.turn_id.as_str()).collect::<Vec<_>>(),
            vec!["turn-new"]
        );

        let all = load_pending_turns(&db, Scope::All, 10).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_boundary_that_cannot_be_read_is_never_established() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-1", "complete", 100, &[("assistant", "a")]).await;
        // Force the read to fail. A discarded error here would be
        // indistinguishable from an empty `turns` table, and both would yield
        // 0 — which on a workspace with history means "all of it is in scope",
        // permanently, from one transient error.
        db.execute_batch("DROP TABLE turn_embedding_state")
            .await
            .unwrap();

        assert_eq!(install_boundary(&db).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_fresh_install_with_no_turns_legitimately_has_a_zero_boundary() {
        // The value an error must never be confused with: zero is correct here
        // precisely because there is no history to protect.
        let db = migrated_db().await;
        assert_eq!(install_boundary(&db).await, Some(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deferred_retries_respect_the_consent_boundary() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-old", "complete", 100, &[("assistant", "a")]).await;
        seed_turn(&db, "turn-new", "complete", 500, &[("assistant", "b")]).await;
        for turn_id in ["turn-old", "turn-new"] {
            upsert_turn_vector(
                &db,
                &PendingTurn {
                    turn_id: turn_id.to_string(),
                    session_id: format!("session-{turn_id}"),
                    job_id: None,
                    project_id: Some("project-1".to_string()),
                    sequence: 1,
                },
                &[],
                DEFERRED_DIMS,
            )
            .await
            .unwrap();
        }

        // Deferrals come from archived events that would not reconstruct, and
        // archival is what makes a turn old — so this population is mostly
        // pre-boundary. Unfloored, it would retry uploading exactly the
        // transcripts the account gate protects.
        let floored = load_pending_turns(&db, Scope::Deferred { since: Some(300) }, 10)
            .await
            .unwrap();
        assert_eq!(
            floored
                .iter()
                .map(|t| t.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn-new"]
        );

        let unfloored = load_pending_turns(&db, Scope::Deferred { since: None }, 10)
            .await
            .unwrap();
        assert_eq!(unfloored.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_install_boundary_is_written_once_and_never_moves() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-before", "complete", 100, &[("assistant", "a")]).await;

        // Anchored to what already existed: everything up to here predates
        // this install's participation.
        assert_eq!(install_boundary(&db).await, Some(100));

        // A turn arriving later must not move it, or the region needing
        // reconciliation would shrink out from under the turns inside it.
        seed_turn(&db, "turn-after", "complete", 500, &[("assistant", "b")]).await;
        assert_eq!(install_boundary(&db).await, Some(100));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_unreadable_turn_is_never_tombstoned() {
        let db = migrated_db().await;
        seed_project(&db).await;
        // An archived event whose reconstruction failed yields no text, which
        // is indistinguishable from having none. Tombstoning it would erase a
        // recoverable turn from the corpus forever.
        seed_turn(&db, "turn-archived", "complete", 100, &[("assistant", "")]).await;
        db.execute(
            "UPDATE events SET storage_mode = 'zstd' WHERE turn_id = 'turn-archived'",
            (),
        )
        .await
        .unwrap();

        let summary = embed_pending(&db, &tokened_client(), Scope::All, 10)
            .await
            .unwrap()
            .expect("the pass itself succeeds");
        assert_eq!(summary.deferred, 1);
        assert_eq!(summary.tombstoned, 0);

        // Retryable, but through its own scope — not by sitting in the page.
        let retryable = load_pending_turns(&db, Scope::Deferred { since: None }, 10)
            .await
            .unwrap();
        assert_eq!(retryable.len(), 1);
        assert_eq!(retryable[0].turn_id, "turn-archived");
        // And not a vector: a deferral is an unsettled answer, not a hit.
        assert!(load_vectors(&db, None).await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_deferred_turn_does_not_block_the_sweep_from_reaching_older_ones() {
        let db = migrated_db().await;
        seed_project(&db).await;
        // The newest turn is unreadable. Because the walk is newest-first, an
        // unmarked deferral would head every subsequent page forever — fill
        // the page with them and the sweep never reaches anything else.
        seed_turn(
            &db,
            "turn-unreadable",
            "complete",
            300,
            &[("assistant", "")],
        )
        .await;
        db.execute(
            "UPDATE events SET storage_mode = 'zstd' WHERE turn_id = 'turn-unreadable'",
            (),
        )
        .await
        .unwrap();
        seed_turn(
            &db,
            "turn-older",
            "complete",
            100,
            &[("tool_result", "bulk")],
        )
        .await;

        // One pass, page size one: it can only reach the unreadable turn.
        let first = embed_pending(&db, &tokened_client(), Scope::All, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.deferred, 1);

        // The next pass must get PAST it. This is the claim that matters: the
        // deferral left the page rather than re-forming it.
        let second = embed_pending(&db, &tokened_client(), Scope::All, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.tombstoned, 1, "the sweep reached the older turn");
        assert_eq!(second.deferred, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_turn_stranded_below_the_live_floor_is_recovered_without_an_account() {
        let db = migrated_db().await;
        seed_project(&db).await;
        // The long turn was still running when the sweep reached its position,
        // so a shorter, newer turn was embedded first and the live floor moved
        // past it. In Cairn this is ordinary: a node's turn is routinely
        // outlived by the sub-agent turns it spawned.
        seed_turn(
            &db,
            "turn-long",
            "complete",
            100,
            &[("assistant", "substance")],
        )
        .await;
        seed_turn(
            &db,
            "turn-short",
            "complete",
            300,
            &[("assistant", "brief")],
        )
        .await;
        upsert_turn_vector(
            &db,
            &PendingTurn {
                turn_id: "turn-short".to_string(),
                session_id: "session-turn-short".to_string(),
                job_id: None,
                project_id: Some("project-1".to_string()),
                sequence: 1,
            },
            &vector::to_bytes(&[1.0]),
            1,
        )
        .await
        .unwrap();

        // The fast pass cannot see it: that is the strand.
        let live_floor = live_boundary(&db, 0).await;
        assert_eq!(live_floor, 300);
        assert!(load_pending_turns(&db, Scope::Since(live_floor), 10)
            .await
            .unwrap()
            .is_empty());

        // Reconciling from the install's own boundary recovers it, and that
        // scope is NOT account-gated — otherwise an install that never signed
        // in would lose its longest turns permanently.
        let recovered = load_pending_turns(&db, Scope::Since(0), 10).await.unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|t| t.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn-long"]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_turns_excerpt_is_its_longest_content_event() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(
            &db,
            "turn-1",
            "complete",
            100,
            &[
                ("user", "ok"),
                (
                    "assistant",
                    "the substantial answer that carries the subject",
                ),
                ("tool_result", &"noise".repeat(100)),
            ],
        )
        .await;

        let excerpts = turn_excerpts(&db, &["turn-1".to_string()], 150).await;
        let excerpt = excerpts.get("turn-1").expect("turn has content");
        assert_eq!(excerpt.event_type, "assistant");
        assert_eq!(
            excerpt.text,
            "the substantial answer that carries the subject"
        );
        // The tool result is longer than everything, and still excluded: it is
        // bulk, not subject.
        assert!(!excerpt.text.contains("noise"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stored_vectors_round_trip_through_the_project_scan() {
        let db = migrated_db().await;
        seed_project(&db).await;
        seed_turn(&db, "turn-1", "complete", 100, &[("assistant", "a")]).await;
        upsert_turn_vector(
            &db,
            &PendingTurn {
                turn_id: "turn-1".to_string(),
                session_id: "session-turn-1".to_string(),
                job_id: Some("job-1".to_string()),
                project_id: Some("project-1".to_string()),
                sequence: 7,
            },
            &vector::to_bytes(&[0.25, -0.5, 1.0]),
            3,
        )
        .await
        .unwrap();

        let scoped = load_vectors(&db, Some("project-1")).await.unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].embedding, vec![0.25, -0.5, 1.0]);
        assert_eq!(scoped[0].sequence, 7);
        assert_eq!(scoped[0].job_id.as_deref(), Some("job-1"));

        assert!(load_vectors(&db, Some("other-project"))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(load_vectors(&db, None).await.unwrap().len(), 1);
    }
}
