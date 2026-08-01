//! The durable conflict resolution session.
//!
//! A conflicting rebase is rolled back the instant it is detected, which is the
//! safety property and is not negotiable. The cost is that the evidence dies
//! with it: after the rollback the branch sits on its own clean content and a
//! later probe finds nothing to enumerate. [`crate::jj::ConflictDiagnostic`]
//! captures that evidence inside the window; this module is where it becomes
//! durable, so the session outlives the reconcile pass that discovered it and
//! can be read, acted on, and closed later.
//!
//! What is stored is coordinates and classification, never patches. The three
//! commits are immutable, so either side of the merge is recomputable on demand
//! and a stored patch could only age. What is NOT recomputable — which paths jj
//! recorded as conflicting, versus which the incoming change merely carries — is
//! exactly what the child inventory table holds.

use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

use crate::jj::{ConflictCondition, ConflictDiagnostic, IncomingClassification};

/// Schema version of the persisted diagnostic. Bumped when the meaning of a
/// stored column changes, so a session written by an older build is recognizable
/// rather than silently misread.
pub(crate) const CONFLICT_DIAGNOSTIC_VERSION: i64 = 1;

/// Whether conflict markers are present in the agent's checkout.
///
/// This records what the executor CONFIRMED, never what was requested. A wake or
/// a resource may only tell an agent to resolve markers when this reads
/// [`MarkerState::Materialized`] — instructing someone to act on state the
/// machinery has not made true is the defect this distinction exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum MarkerState {
    /// No materialization has been attempted.
    #[default]
    NotMaterialized,
    /// Requested, not yet confirmed. The durable reconcile worker retries it.
    Pending,
    /// The executor wrote the files and said so.
    Materialized,
    /// Materialization was attempted and refused or failed; see the diagnostic.
    Failed,
}

impl MarkerState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotMaterialized => "not_materialized",
            Self::Pending => "pending",
            Self::Materialized => "materialized",
            Self::Failed => "failed",
        }
    }

    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("pending") => Self::Pending,
            Some("materialized") => Self::Materialized,
            Some("failed") => Self::Failed,
            _ => Self::NotMaterialized,
        }
    }
}

/// Whether the session is still awaiting resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ResolutionState {
    /// The branch still needs to absorb the incoming change.
    #[default]
    Open,
    /// The branch was replayed onto the base cleanly and published.
    Resolved,
    /// A newer session at a newer destination replaced this one.
    Superseded,
}

impl ResolutionState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
            Self::Superseded => "superseded",
        }
    }
}

/// What advanced the base. Carried into the session so the resource can name the
/// incoming change rather than describing an anonymous "the base moved".
#[derive(Debug, Clone, Default)]
pub(crate) struct IncomingIdentity {
    pub(crate) base_branch: String,
    pub(crate) pr_number: Option<i64>,
    /// Rendered issue reference, e.g. `CAIRN-3352`.
    pub(crate) issue: Option<String>,
}

/// One file the incoming change touches, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionFile {
    pub(crate) path: String,
    /// jj's own name-status word (`M`, `A`, `D`, `R`, …), or `C` for a path jj
    /// named as conflicting but the inventory did not.
    pub(crate) status: String,
    pub(crate) classification: String,
    /// How materialization treated this path, once confirmed.
    pub(crate) marker_disposition: Option<String>,
}

impl SessionFile {
    pub(crate) fn is_conflicting(&self) -> bool {
        self.classification == IncomingClassification::Conflicting.as_str()
    }
}

/// A loaded resolution session: the durable form of one rolled-back conflict.
#[derive(Debug, Clone)]
pub(crate) struct ConflictSession {
    pub(crate) intent_id: String,
    pub(crate) bookmark: String,
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) target_branch: String,
    pub(crate) destination_commit: String,
    pub(crate) diagnostic_version: i64,
    pub(crate) condition: String,
    pub(crate) base: Option<String>,
    pub(crate) ours: Option<String>,
    pub(crate) theirs: Option<String>,
    pub(crate) conflicted_tip: Option<String>,
    pub(crate) incoming: IncomingIdentity,
    pub(crate) marker_state: MarkerState,
    pub(crate) marker_diagnostic: Option<String>,
    pub(crate) updated_at: i64,
    pub(crate) files: Vec<SessionFile>,
}

impl ConflictSession {
    /// The same three-way fingerprint the wake deduplicates on. A request naming
    /// a fingerprint that no longer matches is acting on a stale view.
    pub(crate) fn fingerprint(&self) -> String {
        let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "?".to_string());
        format!(
            "{}:{}:{}",
            field(&self.base),
            field(&self.ours),
            field(&self.theirs)
        )
    }

    pub(crate) fn conflicting(&self) -> impl Iterator<Item = &SessionFile> {
        self.files.iter().filter(|file| file.is_conflicting())
    }

    pub(crate) fn clean_on_retry(&self) -> impl Iterator<Item = &SessionFile> {
        self.files.iter().filter(|file| !file.is_conflicting())
    }

    pub(crate) fn is_base_drift(&self) -> bool {
        self.condition == ConflictCondition::BaseDrift.as_str()
    }

    /// Whether this session was written by a build that agrees with this one
    /// about what the stored columns mean. A session from another version is
    /// rendered with its identity and inventory but without interpretation —
    /// silently reading it as current is how a stale schema tells a confident
    /// lie.
    pub(crate) fn version_is_current(&self) -> bool {
        self.diagnostic_version == CONFLICT_DIAGNOSTIC_VERSION
    }
}

/// Persist the diagnostic captured before the rollback onto the reconcile item
/// that already exists for this branch, and replace its file inventory.
///
/// Called immediately after [`super::base_advance::persist_reconcile_item`] has
/// written the item row, so the UPDATE always has a row to land on. A conflict
/// re-detected at the same destination overwrites in place rather than
/// accumulating: the coordinates are the same merge, so there is one session.
pub(crate) async fn record_conflict_session(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    diagnostic: &ConflictDiagnostic,
    incoming: &IncomingIdentity,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET
           diagnostic_version = ?3,
           conflict_condition = ?4,
           base_commit = ?5,
           ours_commit = ?6,
           theirs_commit = ?7,
           conflicted_tip = ?8,
           incoming_pr_number = ?9,
           incoming_issue = ?10,
           incoming_base_branch = ?11,
           marker_state = COALESCE(marker_state, ?12),
           resolution_state = ?13
         WHERE intent_id = ?1 AND bookmark = ?2",
        params![
            intent_id,
            bookmark,
            CONFLICT_DIAGNOSTIC_VERSION,
            diagnostic.condition.as_str(),
            diagnostic.base.as_deref(),
            diagnostic.ours.as_deref(),
            diagnostic.theirs.as_deref(),
            diagnostic.conflicted_tip.as_deref(),
            incoming.pr_number,
            incoming.issue.as_deref(),
            incoming.base_branch.as_str(),
            MarkerState::NotMaterialized.as_str(),
            ResolutionState::Open.as_str(),
        ],
    )
    .await
    .map_err(|error| format!("persist conflict session: {error}"))?;

    db.execute(
        "DELETE FROM jj_reconcile_incoming_files WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, bookmark],
    )
    .await
    .map_err(|error| format!("clear conflict session inventory: {error}"))?;

    for file in &diagnostic.incoming {
        db.execute(
            "INSERT INTO jj_reconcile_incoming_files
               (intent_id, bookmark, path, status, classification)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(intent_id, bookmark, path) DO UPDATE SET
               status = excluded.status,
               classification = excluded.classification",
            params![
                intent_id,
                bookmark,
                file.path.as_str(),
                file.status.as_str(),
                file.classification.as_str(),
            ],
        )
        .await
        .map_err(|error| format!("persist conflict session inventory: {error}"))?;
    }
    Ok(())
}

/// Supersede every other open session for this branch, so exactly one is active.
///
/// A base that advances twice produces two intents; the older session's
/// coordinates describe a merge nobody will perform again.
pub(crate) async fn supersede_stale_sessions(
    db: &LocalDb,
    bookmark: &str,
    keep_intent_id: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET resolution_state = ?3
         WHERE bookmark = ?1 AND intent_id <> ?2 AND resolution_state = ?4",
        params![
            bookmark,
            keep_intent_id,
            ResolutionState::Superseded.as_str(),
            ResolutionState::Open.as_str()
        ],
    )
    .await
    .map_err(|error| format!("supersede stale conflict sessions: {error}"))?;
    Ok(())
}

const SESSION_COLUMNS: &str = "i.intent_id, i.bookmark, n.project_id, n.store_path, \
     n.target_branch, n.destination_commit, i.diagnostic_version, i.conflict_condition, \
     i.base_commit, i.ours_commit, i.theirs_commit, i.conflicted_tip, i.incoming_pr_number, \
     i.incoming_issue, i.incoming_base_branch, i.marker_state, i.marker_diagnostic, \
     i.resolution_state, i.updated_at";

/// The active resolution session for a branch, if one is open.
pub(crate) async fn load_active_session(
    db: &LocalDb,
    bookmark: &str,
) -> Result<Option<ConflictSession>, String> {
    let bookmark_owned = bookmark.to_string();
    let sql = format!(
        "SELECT {SESSION_COLUMNS}
         FROM jj_reconcile_items i
         JOIN jj_reconcile_intents n ON n.id = i.intent_id
         WHERE i.bookmark = ?1 AND i.resolution_state = '{}'
           AND i.diagnostic_version IS NOT NULL
         ORDER BY i.updated_at DESC LIMIT 1",
        ResolutionState::Open.as_str()
    );
    let mut session = db
        .read(move |conn| {
            let bookmark = bookmark_owned.clone();
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, params![bookmark]).await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(ConflictSession {
                    intent_id: row.text(0)?,
                    bookmark: row.text(1)?,
                    project_id: row.text(2)?,
                    store_path: row.text(3)?,
                    target_branch: row.text(4)?,
                    destination_commit: row.text(5)?,
                    diagnostic_version: row.opt_i64(6)?.unwrap_or(0),
                    condition: row
                        .opt_text(7)?
                        .unwrap_or_else(|| ConflictCondition::ContentConflict.as_str().to_string()),
                    base: row.opt_text(8)?,
                    ours: row.opt_text(9)?,
                    theirs: row.opt_text(10)?,
                    conflicted_tip: row.opt_text(11)?,
                    incoming: IncomingIdentity {
                        pr_number: row.opt_i64(12)?,
                        issue: row.opt_text(13)?,
                        base_branch: row.opt_text(14)?.unwrap_or_default(),
                    },
                    marker_state: MarkerState::parse(row.opt_text(15)?.as_deref()),
                    marker_diagnostic: row.opt_text(16)?,
                    updated_at: row.opt_i64(18)?.unwrap_or(0),
                    files: Vec::new(),
                }))
            })
        })
        .await
        .map_err(|error| format!("load conflict session: {error}"))?;

    if let Some(session) = session.as_mut() {
        session.files = load_session_files(db, &session.intent_id, &session.bookmark).await?;
    }
    Ok(session)
}

/// The incoming change's complete file inventory for one session, path-ordered.
pub(crate) async fn load_session_files(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
) -> Result<Vec<SessionFile>, String> {
    let intent_id = intent_id.to_string();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        let intent_id = intent_id.clone();
        let bookmark = bookmark.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT path, status, classification, marker_disposition
                     FROM jj_reconcile_incoming_files
                     WHERE intent_id = ?1 AND bookmark = ?2
                     ORDER BY path",
                    params![intent_id, bookmark],
                )
                .await?;
            let mut files = Vec::new();
            while let Some(row) = rows.next().await? {
                files.push(SessionFile {
                    path: row.text(0)?,
                    status: row.text(1)?,
                    classification: row.text(2)?,
                    marker_disposition: row.opt_text(3)?,
                });
            }
            Ok(files)
        })
    })
    .await
    .map_err(|error| format!("load conflict session inventory: {error}"))
}

/// Record what materialization did, keyed to what the executor confirmed.
///
/// `dispositions` is empty for every state but [`MarkerState::Materialized`];
/// only a confirmed materialization knows how each path was treated.
pub(crate) async fn record_marker_state(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
    state: MarkerState,
    diagnostic: Option<&str>,
    dispositions: &[(String, String)],
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET marker_state = ?3, marker_diagnostic = ?4
         WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, bookmark, state.as_str(), diagnostic],
    )
    .await
    .map_err(|error| format!("record marker state: {error}"))?;
    for (path, disposition) in dispositions {
        db.execute(
            "UPDATE jj_reconcile_incoming_files SET marker_disposition = ?4
             WHERE intent_id = ?1 AND bookmark = ?2 AND path = ?3",
            params![intent_id, bookmark, path.as_str(), disposition.as_str()],
        )
        .await
        .map_err(|error| format!("record marker disposition: {error}"))?;
    }
    Ok(())
}

/// Close every open session for a branch that has just absorbed its base.
pub(crate) async fn close_open_sessions_for_branch(
    db: &LocalDb,
    bookmark: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET resolution_state = ?2, marker_state = ?3
         WHERE bookmark = ?1 AND resolution_state = ?4",
        params![
            bookmark,
            ResolutionState::Resolved.as_str(),
            MarkerState::NotMaterialized.as_str(),
            ResolutionState::Open.as_str()
        ],
    )
    .await
    .map_err(|error| format!("close open conflict sessions: {error}"))?;
    Ok(())
}
