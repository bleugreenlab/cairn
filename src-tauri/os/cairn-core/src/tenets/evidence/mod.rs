//! Automatic tenet evidence: concluded work judged against standing tenets.
//!
//! A conclusion (an issue reaching a terminal resolution, a thread era folding
//! into a generated summary) is snapshotted as one evaluation event in the same
//! write that concludes it. A background worker embeds the event, takes the
//! nearest approved tenets of the project, and asks the models facet's
//! `decide` verb whether the work supports or contradicts each. Verdicts whose
//! measured probability clears the floor become evidence rows. With no
//! decision-capable model configured, evidence is not gathered at all: there
//! is no fallback to a model reporting its own certainty.
//! When the decayed weight of a tenet's contradictions that no wake has counted
//! yet crosses a threshold, the thread that proposed it is woken to supersede
//! it or correct course, and that wake consumes exactly the rows it counted.
//!
//! Both tables replicate to team replicas, where a compare-and-swap is atomic
//! only on one machine. So nothing here races across machines: an event is
//! evaluated only by the machine that wrote its conclusion
//! (`owner_device_id`), and a wake fires only on the machine owning the newest
//! unconsumed contradiction, once that row has had time to reach every
//! replica. Row ids derive from each row's logical key, so a residual double
//! write is a primary-key upsert on replay rather than a unique-key error.
//!
//! Agents never write evidence; this module is its only writer.

pub mod events;
pub mod judge;
pub mod vectors;
pub(crate) mod wake;
pub(crate) mod worker;

#[cfg(test)]
mod tests;

use std::time::Duration;

pub use vectors::tenet_embed_text;
pub use worker::spawn;

/// The evidence table, as `db-change` names it.
pub const EVIDENCE_TABLE: &str = "tenet_evidence";

/// How many nearest tenets one event is judged against.
pub const KNN_K: usize = 5;
/// The cosine similarity below which a tenet is not judged at all. Measured
/// on 2026-10-17 against 1,108 approved `cairn` tenets and 20 merged issues:
/// each conclusion's best match fell between 0.52 and 0.73 (median 0.60),
/// while all pairs had a median of 0.325 and a p99 of 0.534. At 0.55 about
/// half of conclusions reach the judge.
pub const KNN_FLOOR: f32 = 0.55;
/// The measured probability of the chosen sign below which a verdict is not
/// recorded.
pub const CONFIDENCE_FLOOR: f64 = 0.7;
/// The decayed contradiction weight that wakes the proposing thread.
pub const WAKE_THRESHOLD: f64 = 1.2;
/// A contradiction's weight halves every this many days.
pub const HALF_LIFE_DAYS: f64 = 30.0;
/// How old the newest unconsumed contradiction must be before its owner may
/// fire the wake. Several team pull cycles (3 s) plus the push backstop (30 s),
/// so every online replica agrees on which row is newest.
pub const WAKE_SETTLE_SECS: i64 = 120;
/// Failed evaluation attempts before an event is given up on.
pub const MAX_ATTEMPTS: i64 = 5;
/// The cap on an event's text snapshot, in characters.
pub const EVENT_TEXT_CHARS: usize = 4000;
/// How many tenets one embedding request carries.
pub const EMBED_CHUNK: usize = 64;
/// How often the worker sweeps for due events and owed wakes without a hint.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Seconds to wait after the n-th failed attempt (1-based) before retrying.
pub const BACKOFF_SECS: [i64; 4] = [60, 300, 1800, 7200];
/// A tenet with evidence this recent ranks slightly higher in a seed.
pub const RECENT_EVIDENCE_SECS: i64 = 14 * 86_400;

/// A UUID derived from `prefix:key`, for a row whose identity is its logical
/// key. Two machines writing the same logical row produce the same primary
/// key, which team-replica replay applies as an upsert.
pub fn stable_id(prefix: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{prefix}:{key}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes)
        .into_uuid()
        .to_string()
}

/// Cut `text` to at most `max` characters.
pub(crate) fn cap_chars(mut text: String, max: usize) -> String {
    if let Some((index, _)) = text.char_indices().nth(max) {
        text.truncate(index);
    }
    text
}

/// A UTC calendar date for a unix-seconds instant.
pub(crate) fn utc_date(at: i64, format: &str) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|date| date.format(format).to_string())
        .unwrap_or_else(|| at.to_string())
}

/// Announce a change to the evidence table to the desktop.
pub(crate) fn emit_evidence_changed(emitter: &dyn crate::services::EventEmitter) {
    if let Err(error) = emitter.emit(
        "db-change",
        serde_json::json!({"table": EVIDENCE_TABLE, "action": "insert"}),
    ) {
        tracing::warn!(%error, "failed to emit tenet_evidence db-change notification");
    }
}
