//! The proposing thread's disposition of one evidence row: judged, stands.
//!
//! The judge can misread concluded work. A thread reading the wake's evidence
//! may conclude its tenet stands because the judge was wrong, not because the
//! work should change. A dismissal records that judgment on the row itself:
//! which thread dismissed it, when, and why. The row is never deleted or
//! re-signed, because the dismissal and its reason are, with supersessions,
//! the only production labels the judge gets.
//!
//! A dismissed contradiction no longer counts toward the contradiction wake.
//! One a wake already counted stays counted: dismissing is not retracting a
//! message already sent. Either sign may be dismissed; a dismissed `supports`
//! row changes no weight and still records that the judge misread.
//!
//! The judgment is the proposing thread's, as a withdrawal is, and it is made
//! once: a second dismissal of the same row is refused with the first quoted.

use cairn_db::turso::params;

use crate::services::EventEmitter;
use crate::storage::{DbResult, LocalDb, RowExt};
use crate::tenets::read::row_conn;

/// What a dismissal asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dismissal {
    /// The evidence row's source URI, as the tenet's Evidence section lists it.
    pub source_uri: String,
    /// Why the tenet stands: how the judge misread the work.
    pub reason: String,
}

/// A recorded dismissal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dismissed {
    pub number: i64,
    /// The row's source URI as stored.
    pub source_uri: String,
    /// `supports` or `contradicts`.
    pub sign: String,
    /// A contradiction a wake already counted, which dismissing does not
    /// retract.
    pub already_counted: bool,
}

pub const EMPTY_REASON_REFUSAL: &str =
    "a dismissal needs a reason: why the tenet stands, that is, how the judge misread the work";

struct EvidenceRow {
    id: String,
    source_uri: String,
    sign: String,
    consumed: bool,
    dismissed_at: Option<i64>,
    dismissed_by: Option<String>,
    dismissed_reason: Option<String>,
}

/// `uri` in canonical form, so a caller may name a source in any spelling the
/// URI grammar accepts.
fn canonical(uri: &str) -> String {
    cairn_common::uri::parse_uri(uri)
        .map(|resource| resource.to_uri())
        .unwrap_or_else(|| uri.to_string())
}

async fn evidence_conn(
    conn: &cairn_db::storage::TrackedConnection,
    tenet_id: &str,
) -> DbResult<Vec<EvidenceRow>> {
    let mut rows = conn
        .query(
            "SELECT id, source_uri, sign, consumed_by_wake, dismissed_at, dismissed_by_thread_id,
                    dismissed_reason
             FROM tenet_evidence WHERE tenet_id = ?1
             ORDER BY evaluated_at DESC, id",
            params![tenet_id],
        )
        .await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(EvidenceRow {
            id: row.text(0)?,
            source_uri: row.text(1)?,
            sign: row.text(2)?,
            consumed: row.opt_text(3)?.is_some(),
            dismissed_at: row.opt_i64(4)?,
            dismissed_by: row.opt_text(5)?,
            dismissed_reason: row.opt_text(6)?,
        });
    }
    Ok(out)
}

/// `project/name` of a thread, or its id when the row is gone.
async fn thread_label_conn(
    conn: &cairn_db::storage::TrackedConnection,
    thread_id: &str,
) -> DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT p.key, t.name FROM threads t JOIN projects p ON p.id = t.project_id
             WHERE t.id = ?1",
            params![thread_id],
        )
        .await?;
    Ok(match rows.next().await? {
        Some(row) => format!("{}/{}", row.text(0)?, row.text(1)?),
        None => thread_id.to_string(),
    })
}

/// Record that the evidence on tenet `number` from `dismissal.source_uri`
/// misreads the work. Only the thread that proposed the tenet may.
pub async fn dismiss(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    project_id: &str,
    number: i64,
    dismissing_thread_id: &str,
    dismissal: Dismissal,
) -> Result<Dismissed, String> {
    let source = dismissal.source_uri.trim().to_string();
    if source.is_empty() {
        return Err(
            "a dismissal names the evidence row by its source URI, as the tenet's Evidence section lists it"
                .into(),
        );
    }
    let reason = dismissal.reason.trim().to_string();
    if reason.is_empty() {
        return Err(EMPTY_REASON_REFUSAL.into());
    }
    let project_id = project_id.to_string();
    let thread_id = dismissing_thread_id.to_string();
    let outcome = db
        .write(|conn| {
            let project_id = project_id.clone();
            let thread_id = thread_id.clone();
            let source = source.clone();
            let reason = reason.clone();
            Box::pin(async move {
                let Some(tenet) = row_conn(conn, &project_id, number).await? else {
                    return Ok(Err(format!("no tenet #{number} in this project")));
                };
                if tenet.proposing_thread_id != thread_id {
                    return Ok(Err(format!(
                        "only the thread that proposed tenet #{number} can dismiss its evidence"
                    )));
                }
                let rows = evidence_conn(conn, &tenet.id).await?;
                let wanted = canonical(&source);
                let Some(row) = rows
                    .iter()
                    .find(|row| row.source_uri == source || canonical(&row.source_uri) == wanted)
                else {
                    if rows.is_empty() {
                        return Ok(Err(format!(
                            "tenet #{number} has no evidence to dismiss"
                        )));
                    }
                    let sources: Vec<&str> =
                        rows.iter().map(|row| row.source_uri.as_str()).collect();
                    return Ok(Err(format!(
                        "tenet #{number} has no evidence from {source}; its evidence comes from {}",
                        sources.join(", ")
                    )));
                };
                if let Some(at) = row.dismissed_at {
                    let by = match &row.dismissed_by {
                        Some(by) => thread_label_conn(conn, by).await?,
                        None => "an unrecorded thread".to_string(),
                    };
                    return Ok(Err(format!(
                        "the {} evidence from {} on tenet #{number} was already dismissed by {by} on {}: {}. A dismissal is recorded once; it is not re-litigated by overwriting",
                        row.sign,
                        row.source_uri,
                        super::utc_date(at, "%Y-%m-%d"),
                        row.dismissed_reason.as_deref().unwrap_or("(no reason recorded)")
                    )));
                }
                let stamped = conn
                    .execute(
                        "UPDATE tenet_evidence
                         SET dismissed_at = ?1, dismissed_by_thread_id = ?2, dismissed_reason = ?3
                         WHERE id = ?4 AND dismissed_at IS NULL",
                        params![now, thread_id.as_str(), reason.as_str(), row.id.as_str()],
                    )
                    .await?;
                if stamped != 1 {
                    return Ok(Err(format!(
                        "the evidence from {} on tenet #{number} was dismissed while this dismissal was being recorded",
                        row.source_uri
                    )));
                }
                Ok(Ok(Dismissed {
                    number,
                    source_uri: row.source_uri.clone(),
                    sign: row.sign.clone(),
                    already_counted: row.sign == "contradicts" && row.consumed,
                }))
            })
        })
        .await
        .map_err(|error| format!("Failed to dismiss tenet evidence: {error}"))??;
    super::emit_evidence_changed(emitter, "update");
    crate::tenets::emit_changed(emitter, &project_id, "update");
    Ok(outcome)
}
