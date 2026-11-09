//! The per-branch catch-up mode: the single owner of whether a store bookmark
//! catches up with its base by ONE merge commit instead of a replay.
//!
//! The mode is narrow by construction. It names one bookmark of one store; it
//! is set and cleared only on the operator's authority (the `/rebase`
//! `catch-up-mode` action asks for it); every set, clear, expiry and use is
//! recorded in `branch_catch_up_events` and logged with the branch name; and it
//! ends by itself when the branch lands or its owning issue resolves. A branch
//! without a row runs exactly the replay it always has.
//!
//! With the mode come two small records the landing gate reads: the declared
//! review-required path globs, fixed when the mode is set, and the base commit
//! the owner has reviewed the incoming changes through, which only the owner
//! (or the operator) moves forward with `mark-reviewed`.

use std::path::Path;

use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// The one mode a row can hold.
pub(crate) const MODE_MERGE: &str = "merge";

/// Quarantine kind of a merge catch-up that conflicted and was undone. Its
/// fingerprint is `tip:dest`: the same pair is never attempted twice, and a
/// newer destination re-attempts, which costs one merge.
pub(crate) const MERGE_CONFLICT_QUARANTINE: &str = "merge_catch_up_conflict";

/// The audit vocabulary of `branch_catch_up_events.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatchUpEventKind {
    Set,
    Cleared,
    Expired,
    Merged,
    MergeConflicted,
    MergeRefused,
    LandingMerged,
    LandingRefusedReview,
    Reviewed,
}

impl CatchUpEventKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Cleared => "cleared",
            Self::Expired => "expired",
            Self::Merged => "merged",
            Self::MergeConflicted => "merge_conflicted",
            Self::MergeRefused => "merge_refused",
            Self::LandingMerged => "landing_merged",
            Self::LandingRefusedReview => "landing_refused_review",
            Self::Reviewed => "reviewed",
        }
    }
}

/// One branch's catch-up mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchUpMode {
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) bookmark: String,
    pub(crate) job_id: String,
    pub(crate) set_by: String,
    pub(crate) set_at: i64,
    pub(crate) review_paths: Vec<String>,
    pub(crate) reviewed_through: String,
    pub(crate) reviewed_by: String,
    pub(crate) reviewed_at: i64,
}

impl CatchUpMode {
    /// The declared review-required paths among `paths`, in input order.
    pub(crate) fn review_hits(&self, paths: &[String]) -> Vec<String> {
        review_path_hits(&self.review_paths, paths)
    }
}

/// One recorded event, for the rebase page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchUpEvent {
    pub(crate) kind: String,
    pub(crate) actor: String,
    pub(crate) detail: String,
    pub(crate) at: i64,
}

/// What setting the mode records.
#[derive(Debug, Clone)]
pub(crate) struct SetCatchUpMode<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) store: &'a Path,
    pub(crate) bookmark: &'a str,
    pub(crate) job_id: &'a str,
    pub(crate) actor: &'a str,
    pub(crate) review_paths: &'a [String],
    pub(crate) reviewed_through: &'a str,
}

fn glob_matcher(pattern: &str) -> Result<globset::GlobMatcher, String> {
    globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|error| format!("review path `{pattern}` is not a valid glob: {error}"))
}

/// Validate the declared review-path globs: each non-empty and compilable. A
/// glob matches whole repository-relative paths, `*` within one directory
/// level and `**` across levels; a glob ending in `/` covers everything below
/// that directory.
pub(crate) fn validate_review_paths(paths: &[String]) -> Result<(), String> {
    for pattern in paths {
        if pattern.trim().is_empty() {
            return Err("a review path cannot be empty".to_string());
        }
        glob_matcher(&normalized_glob(pattern))?;
    }
    Ok(())
}

fn normalized_glob(pattern: &str) -> String {
    if pattern.ends_with('/') {
        format!("{pattern}**")
    } else {
        pattern.to_string()
    }
}

/// The entries of `paths` that any of `globs` matches, in input order.
pub(crate) fn review_path_hits(globs: &[String], paths: &[String]) -> Vec<String> {
    let matchers: Vec<globset::GlobMatcher> = globs
        .iter()
        .filter_map(|pattern| glob_matcher(&normalized_glob(pattern)).ok())
        .collect();
    paths
        .iter()
        .filter(|path| {
            matchers
                .iter()
                .any(|matcher| matcher.is_match(path.as_str()))
        })
        .cloned()
        .collect()
}

fn store_key(store: &Path) -> String {
    store.to_string_lossy().into_owned()
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

async fn load_rows(
    db: &LocalDb,
    project_id: &str,
    store: &str,
    bookmark: Option<&str>,
) -> Result<Vec<CatchUpMode>, String> {
    let project_id = project_id.to_string();
    let store = store.to_string();
    let bookmark = bookmark.map(ToOwned::to_owned);
    db.read(move |conn| {
        let project_id = project_id.clone();
        let store = store.clone();
        let bookmark = bookmark.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT project_id, store_path, bookmark, job_id, set_by, set_at, review_paths,
                            reviewed_through, reviewed_by, reviewed_at
                       FROM branch_catch_up_modes
                      WHERE project_id = ?1 AND store_path = ?2
                        AND (?3 IS NULL OR bookmark = ?3)",
                    params![project_id, store, bookmark],
                )
                .await?;
            let mut modes = Vec::new();
            while let Some(row) = rows.next().await? {
                let review_paths: Vec<String> =
                    serde_json::from_str(&row.text(6)?).unwrap_or_default();
                modes.push(CatchUpMode {
                    project_id: row.text(0)?,
                    store_path: row.text(1)?,
                    bookmark: row.text(2)?,
                    job_id: row.text(3)?,
                    set_by: row.text(4)?,
                    set_at: row.i64(5)?,
                    review_paths,
                    reviewed_through: row.text(7)?,
                    reviewed_by: row.text(8)?,
                    reviewed_at: row.i64(9)?,
                });
            }
            Ok(modes)
        })
    })
    .await
    .map_err(|error| format!("load catch-up mode: {error}"))
}

/// Why a mode has ended by itself, if it has: its owning job is gone, its
/// issue resolved, or the branch landed.
async fn expiry_reason(db: &LocalDb, mode: &CatchUpMode) -> Result<Option<String>, String> {
    let job_id = mode.job_id.clone();
    let project_id = mode.project_id.clone();
    let bookmark = mode.bookmark.clone();
    db.read(move |conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let bookmark = bookmark.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT
                       (SELECT COUNT(*) FROM jobs WHERE id = ?1),
                       (SELECT i.status FROM jobs j JOIN issues i ON i.id = j.issue_id
                         WHERE j.id = ?1),
                       (SELECT COUNT(*) FROM merge_requests
                         WHERE project_id = ?2 AND source_branch = ?3 AND status = 'merged')",
                    params![job_id, project_id, bookmark],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            if row.i64(0)? == 0 {
                return Ok(Some("its owning job no longer exists".to_string()));
            }
            if let Some(status) = row.opt_text(1)? {
                if matches!(status.as_str(), "merged" | "closed") {
                    return Ok(Some(format!("its issue resolved ({status})")));
                }
            }
            if row.i64(2)? > 0 {
                return Ok(Some("the branch landed".to_string()));
            }
            Ok(None)
        })
    })
    .await
    .map_err(|error| format!("check catch-up mode expiry: {error}"))
}

/// The branch's catch-up mode, if it has one that is still in force. A mode
/// whose owner has finished is expired here, lazily, with an `expired` event,
/// so no reader ever acts on a mode that should have ended.
pub(crate) async fn mode_for(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
) -> Result<Option<CatchUpMode>, String> {
    let Some(mode) = load_rows(db, project_id, &store_key(store), Some(bookmark))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    if let Some(reason) = expiry_reason(db, &mode).await? {
        remove(db, &mode, CatchUpEventKind::Expired, "system", &reason).await?;
        return Ok(None);
    }
    Ok(Some(mode))
}

/// Record one event in the branch's audit trail, and log it.
pub(crate) async fn record_event(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
    kind: CatchUpEventKind,
    actor: &str,
    detail: &str,
) -> Result<(), String> {
    log::info!(
        "catch-up[{bookmark}] {} by {actor}: {detail}",
        kind.as_str()
    );
    db.execute(
        "INSERT INTO branch_catch_up_events
           (project_id, store_path, bookmark, kind, actor, detail, at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            project_id,
            store_key(store),
            bookmark,
            kind.as_str(),
            actor,
            detail,
            now()
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record catch-up event: {error}"))
}

/// The branch's most recent events, newest first.
pub(crate) async fn recent_events(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
    limit: i64,
) -> Result<Vec<CatchUpEvent>, String> {
    let project_id = project_id.to_string();
    let store = store_key(store);
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        let project_id = project_id.clone();
        let store = store.clone();
        let bookmark = bookmark.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT kind, actor, detail, at FROM branch_catch_up_events
                      WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3
                      ORDER BY at DESC, id DESC LIMIT ?4",
                    params![project_id, store, bookmark, limit],
                )
                .await?;
            let mut events = Vec::new();
            while let Some(row) = rows.next().await? {
                events.push(CatchUpEvent {
                    kind: row.text(0)?,
                    actor: row.text(1)?,
                    detail: row.text(2)?,
                    at: row.i64(3)?,
                });
            }
            Ok(events)
        })
    })
    .await
    .map_err(|error| format!("load catch-up events: {error}"))
}

/// Set (or re-set) a branch's merge mode. The caller has established the
/// operator's authority, resolved `reviewed_through` to a commit in the
/// branch's and the default branch's shared history, and refused the default
/// branch.
pub(crate) async fn set(db: &LocalDb, request: SetCatchUpMode<'_>) -> Result<CatchUpMode, String> {
    validate_review_paths(request.review_paths)?;
    let at = now();
    let review_paths = serde_json::to_string(request.review_paths)
        .map_err(|error| format!("encode review paths: {error}"))?;
    db.execute(
        "INSERT INTO branch_catch_up_modes
           (project_id, store_path, bookmark, mode, job_id, set_by, set_at, review_paths,
            reviewed_through, reviewed_by, reviewed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?6, ?7)
         ON CONFLICT(project_id, store_path, bookmark) DO UPDATE SET
           job_id = excluded.job_id,
           set_by = excluded.set_by,
           set_at = excluded.set_at,
           review_paths = excluded.review_paths,
           reviewed_through = excluded.reviewed_through,
           reviewed_by = excluded.reviewed_by,
           reviewed_at = excluded.reviewed_at",
        params![
            request.project_id,
            store_key(request.store),
            request.bookmark,
            MODE_MERGE,
            request.job_id,
            request.actor,
            at,
            review_paths.as_str(),
            request.reviewed_through
        ],
    )
    .await
    .map_err(|error| format!("set catch-up mode: {error}"))?;
    record_event(
        db,
        request.project_id,
        request.store,
        request.bookmark,
        CatchUpEventKind::Set,
        request.actor,
        &format!(
            "mode merge; review paths {review_paths}; reviewed through {}",
            request.reviewed_through
        ),
    )
    .await?;
    // A replay-era cooldown on this branch describes a shape it no longer
    // takes; the next pass attempts the merge.
    release_quarantine(
        db,
        request.project_id,
        request.store,
        request.bookmark,
        None,
    )
    .await?;
    Ok(CatchUpMode {
        project_id: request.project_id.to_string(),
        store_path: store_key(request.store),
        bookmark: request.bookmark.to_string(),
        job_id: request.job_id.to_string(),
        set_by: request.actor.to_string(),
        set_at: at,
        review_paths: request.review_paths.to_vec(),
        reviewed_through: request.reviewed_through.to_string(),
        reviewed_by: request.actor.to_string(),
        reviewed_at: at,
    })
}

async fn release_quarantine(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
    merge_kinds_only: Option<()>,
) -> Result<(), String> {
    let sql = if merge_kinds_only.is_some() {
        "DELETE FROM jj_reconcile_quarantines
          WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3
            AND failure_kind IN (?4, ?5)"
    } else {
        "DELETE FROM jj_reconcile_quarantines
          WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3
            AND failure_kind IN ('rolled_back_conflict', 'reconcile_timeout', ?4, ?5)"
    };
    db.execute(
        sql,
        params![
            project_id,
            store_key(store),
            bookmark,
            MERGE_CONFLICT_QUARANTINE,
            crate::jj::MERGE_CATCH_UP_REFUSED
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("release catch-up quarantine: {error}"))
}

async fn remove(
    db: &LocalDb,
    mode: &CatchUpMode,
    kind: CatchUpEventKind,
    actor: &str,
    detail: &str,
) -> Result<bool, String> {
    let removed = db
        .execute(
            "DELETE FROM branch_catch_up_modes
              WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
            params![
                mode.project_id.as_str(),
                mode.store_path.as_str(),
                mode.bookmark.as_str()
            ],
        )
        .await
        .map_err(|error| format!("clear catch-up mode: {error}"))?;
    if removed == 0 {
        return Ok(false);
    }
    let store = Path::new(&mode.store_path);
    record_event(
        db,
        &mode.project_id,
        store,
        &mode.bookmark,
        kind,
        actor,
        detail,
    )
    .await?;
    // A merge-era cooldown would otherwise park the replay the branch now
    // takes again.
    release_quarantine(db, &mode.project_id, store, &mode.bookmark, Some(())).await?;
    Ok(true)
}

/// Clear a branch's mode. Returns whether there was one.
pub(crate) async fn clear(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
    actor: &str,
    detail: &str,
) -> Result<bool, String> {
    let Some(mode) = load_rows(db, project_id, &store_key(store), Some(bookmark))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(false);
    };
    remove(db, &mode, CatchUpEventKind::Cleared, actor, detail).await
}

/// End the mode of every branch an issue's jobs own, inside the transaction
/// that resolves the issue: an `cleared` event by `system` for each, the mode
/// rows removed, and any merge-era cooldown released.
pub(crate) async fn clear_for_issue_conn(
    conn: &cairn_db::storage::TrackedConnection,
    issue_id: &str,
    resolution: &str,
    now: i64,
) -> crate::storage::DbResult<()> {
    let detail = format!("its issue resolved ({resolution})");
    conn.execute(
        "INSERT INTO branch_catch_up_events
           (project_id, store_path, bookmark, kind, actor, detail, at)
         SELECT m.project_id, m.store_path, m.bookmark, 'cleared', 'system', ?2, ?3
           FROM branch_catch_up_modes m JOIN jobs j ON j.id = m.job_id
          WHERE j.issue_id = ?1",
        params![issue_id, detail.as_str(), now],
    )
    .await?;
    conn.execute(
        "DELETE FROM jj_reconcile_quarantines
          WHERE failure_kind IN (?2, ?3)
            AND EXISTS (SELECT 1 FROM branch_catch_up_modes m JOIN jobs j ON j.id = m.job_id
                         WHERE j.issue_id = ?1
                           AND m.project_id = jj_reconcile_quarantines.project_id
                           AND m.store_path = jj_reconcile_quarantines.store_path
                           AND m.bookmark = jj_reconcile_quarantines.bookmark)",
        params![
            issue_id,
            MERGE_CONFLICT_QUARANTINE,
            crate::jj::MERGE_CATCH_UP_REFUSED
        ],
    )
    .await?;
    conn.execute(
        "DELETE FROM branch_catch_up_modes
          WHERE job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
        params![issue_id],
    )
    .await?;
    Ok(())
}

/// End a landed branch's mode, right after its landing is recorded. Advisory:
/// the lazy expiry in [`mode_for`] ends it on the next read regardless.
pub(crate) async fn end_on_landing(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &str,
    branch: &str,
) {
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    if let Err(error) = clear(
        db,
        project_id,
        &store,
        branch,
        "system",
        "the branch landed",
    )
    .await
    {
        log::warn!("catch-up[{branch}] could not end its mode after landing: {error}");
    }
}

/// Record that the owner reviewed the incoming base changes through `through`.
/// The caller has proven `through` lies in both the branch's and the default
/// branch's history and at or after the current mark.
pub(crate) async fn mark_reviewed(
    db: &LocalDb,
    mode: &CatchUpMode,
    through: &str,
    actor: &str,
) -> Result<(), String> {
    let at = now();
    db.execute(
        "UPDATE branch_catch_up_modes
            SET reviewed_through = ?4, reviewed_by = ?5, reviewed_at = ?6
          WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
        params![
            mode.project_id.as_str(),
            mode.store_path.as_str(),
            mode.bookmark.as_str(),
            through,
            actor,
            at
        ],
    )
    .await
    .map_err(|error| format!("mark catch-up review: {error}"))?;
    record_event(
        db,
        &mode.project_id,
        Path::new(&mode.store_path),
        &mode.bookmark,
        CatchUpEventKind::Reviewed,
        actor,
        &format!("reviewed through {through} (was {})", mode.reviewed_through),
    )
    .await
}

/// The landing gate's question: which declared review paths has the base
/// changed since the owner's review mark, as of `target_tip`?
///
/// `Err` is a refusal reason: the mark is not in the target's history, so
/// nothing can be measured from it, and the gate fails closed.
pub(crate) fn unreviewed_base_paths(
    jj: &crate::jj::JjEnv,
    store: &Path,
    mode: &CatchUpMode,
    target_tip: &str,
) -> Result<Vec<String>, String> {
    if !crate::jj::revset_descends_from(jj, store, target_tip, &mode.reviewed_through) {
        return Err(format!(
            "the owner's review mark {} is not in the history of the target tip {target_tip}, so \
             what the base changed since the review cannot be measured",
            mode.reviewed_through
        ));
    }
    let changed = crate::jj::base_side_paths(jj, store, &mode.reviewed_through, target_tip);
    Ok(mode.review_hits(&changed))
}

/// [`mode_for`] against the database that owns the project, for callers that
/// hold an orchestrator rather than a database (the landing).
pub(crate) async fn mode_for_project(
    orch: &crate::orchestrator::Orchestrator,
    project_id: &str,
    store: &Path,
    bookmark: &str,
) -> Result<Option<CatchUpMode>, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| {
            format!("resolve the project's database for its catch-up mode: {error}")
        })?;
    mode_for(&db, project_id, store, bookmark).await
}

/// The landing's review gate for a merge-mode branch, judged against the
/// pinned `target_tip` before anything is merged. A landing whose base changed
/// a declared review path since the owner's mark is refused with the paths
/// named, an event, and one Steer wake to the owner per target tip; a delta
/// only on undeclared paths passes. A mark that is not in the target's
/// history cannot be measured from and is refused (fail closed).
///
/// This refusal is an approval policy the operator chose, for one bookmark:
/// setting the mode turns it on, the operator declares its paths, it asks for
/// no evidence or check verdict (only the owner's own mark), every refusal
/// names the act that clears it, and it ends with the mode.
pub(crate) async fn landing_review_gate(
    orch: &crate::orchestrator::Orchestrator,
    mode: &CatchUpMode,
    jj: &crate::jj::JjEnv,
    store: &Path,
    target: &str,
    target_tip: &str,
) -> Result<(), String> {
    let measured = {
        let (jj, store, mode, tip) = (
            jj.clone(),
            store.to_path_buf(),
            mode.clone(),
            target_tip.to_string(),
        );
        tokio::task::spawn_blocking(move || unreviewed_base_paths(&jj, &store, &mode, &tip))
            .await
            .map_err(|error| format!("review gate worker failed: {error}"))?
    };
    let source = mode.bookmark.as_str();
    let mark = mode.reviewed_through.as_str();
    let reason = match measured {
        Ok(paths) if paths.is_empty() => return Ok(()),
        Ok(paths) => format!(
            "Refusing to merge `{source}` into `{target}`: `{target}` changed review-required \
             paths since the owner's review through {mark}:\n{}\n(base range {mark}..{target_tip}). \
             Review them, then write cairn:~/rebase {{action:\"mark-reviewed\", \
             through:\"{target_tip}\"}}, and merge again. Nothing was merged.",
            list_paths(&paths)
        ),
        Err(why) => format!(
            "Refusing to merge `{source}` into `{target}`: {why}. Review what `{target}` brought \
             in, then write cairn:~/rebase {{action:\"mark-reviewed\", \
             through:\"{target_tip}\"}}, and merge again. Nothing was merged."
        ),
    };
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &mode.project_id)
        .await
        .map_err(|error| format!("{reason}\n(also: resolve the project's database: {error})"))?;
    if let Err(error) = record_event(
        &db,
        &mode.project_id,
        store,
        source,
        CatchUpEventKind::LandingRefusedReview,
        "system",
        &reason,
    )
    .await
    {
        log::warn!("catch-up[{source}] {error}");
    }
    if let Some(run_id) = crate::messages::delivery::latest_run_for_job(&db, &mode.job_id) {
        if let Err(error) = crate::messages::delivery::queue_system_direct_once(
            orch,
            &run_id,
            &reason,
            crate::messages::queued::DeliveryUrgency::Steer,
            &format!("catch-up-review:{target_tip}"),
        ) {
            log::warn!(
                "catch-up[{source}] could not wake the owner about the review gate: {error}"
            );
        }
    }
    Err(reason)
}

/// Record a landing's catch-up merge in the branch's audit trail. Advisory.
pub(crate) async fn record_landing_merged(
    orch: &crate::orchestrator::Orchestrator,
    mode: &CatchUpMode,
    store: &Path,
    target: &str,
    landed: Option<&str>,
) {
    let result = async {
        let db = crate::execution::routing::owning_db_for_project(&orch.db, &mode.project_id)
            .await
            .map_err(|error| error.to_string())?;
        record_event(
            &db,
            &mode.project_id,
            store,
            &mode.bookmark,
            CatchUpEventKind::LandingMerged,
            "system",
            &format!(
                "landed on `{target}` by merge and bind, without a replay; `{target}` is now {}",
                landed.unwrap_or("<unreported>")
            ),
        )
        .await
    }
    .await;
    if let Err(error) = result {
        log::warn!(
            "catch-up[{}] could not record its landing: {error}",
            mode.bookmark
        );
    }
}

/// How many base-side paths a merge note lists before eliding the rest.
const LISTED_PATHS: usize = 60;

fn list_paths(paths: &[String]) -> String {
    let listed: Vec<&str> = paths
        .iter()
        .take(LISTED_PATHS)
        .map(String::as_str)
        .collect();
    let elided = paths.len().saturating_sub(listed.len());
    let mut out = listed
        .iter()
        .map(|path| format!("  - {path}"))
        .collect::<Vec<_>>()
        .join("\n");
    if elided > 0 {
        out.push_str(&format!("\n  - … and {elided} more"));
    }
    out
}

/// The note a clean catch-up merge sends its owner: the merge, the base range
/// it brought in, and the base-side paths, review-required ones first.
pub(crate) fn merge_note(
    branch: &str,
    target: &str,
    merged: &crate::jj::MergedCatchUp,
    hits: &[String],
    others: &[String],
) -> String {
    let mut note = format!(
        "[Base branch update] `{branch}` caught up with `{target}` by merge (this branch's \
         catch-up mode). Merge commit {} has parents your previous tip and {}; it brought in \
         the base range {}..{}. No commit of your branch was rewritten.",
        merged.merge, merged.new_base, merged.old_base, merged.new_base
    );
    if hits.is_empty() {
        note.push_str("\nNone of the base-side changes is on a review-required path.");
    } else {
        note.push_str(&format!(
            "\nReview-required paths the base changed ({}):\n{}",
            hits.len(),
            list_paths(hits)
        ));
    }
    if !others.is_empty() {
        note.push_str(&format!(
            "\nOther base-side paths ({}):\n{}",
            others.len(),
            list_paths(others)
        ));
    }
    if !hits.is_empty() {
        note.push_str(&format!(
            "\nA textually clean merge is not a reviewed merge: nothing judged what these base \
             changes mean for this branch. Review the incoming review-path changes and \
             `mark-reviewed` when done; landing is refused until then:\n  write cairn:~/rebase \
             {{action:\"mark-reviewed\", through:\"{}\"}}",
            merged.new_base
        ));
    }
    note
}

/// Record a merge-mode branch's reconcile outcome in its audit trail and, for
/// a clean merge, tell the owner what came in: a Steer wake when the base
/// changed a declared review path, otherwise a passive note that wakes nobody.
/// Advisory: a failure here is logged and never undoes the graph move.
pub(crate) async fn announce_reconcile_outcome(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    target: &str,
    report: &crate::jj::ReconcileReport,
) {
    let result: Result<(), String> = async {
        let Some(mode) = mode_for(db, project_id, store, branch).await? else {
            return Ok(());
        };
        if let Some(merged) = report.merged_clean.get(branch) {
            let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
            let read_store = store.to_path_buf();
            let (old_base, new_base) = (merged.old_base.clone(), merged.new_base.clone());
            let paths = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
                let jj =
                    jj.pin_immutable_read(&read_store, &[old_base.as_str(), new_base.as_str()])?;
                Ok(crate::jj::base_side_paths(
                    &jj,
                    &read_store,
                    &old_base,
                    &new_base,
                ))
            })
            .await
            .map_err(|error| format!("base-side path worker failed: {error}"))??;
            let hits = mode.review_hits(&paths);
            let others: Vec<String> = paths
                .iter()
                .filter(|path| !hits.contains(path))
                .cloned()
                .collect();
            let note = merge_note(branch, target, merged, &hits, &others);
            // A clean demand catch-up is learned at the owner's next turn;
            // review-path hits gate landing, but never wake an idle builder.
            let urgency = crate::messages::queued::DeliveryUrgency::Passive;
            let jobs = super::base_advance::load_on_branch_jobs(db, project_id, branch).await?;
            for job in jobs {
                match crate::messages::delivery::latest_run_for_job(db, &job.id) {
                    Some(run_id) => crate::messages::delivery::queue_system_direct_once(
                        orch,
                        &run_id,
                        &note,
                        urgency,
                        &format!(
                            "catch-up-merge:{project_id}:{branch}:{}:{}",
                            merged.new_base, job.id
                        ),
                    )?,
                    None => log::info!(
                        "catch-up[{branch}] no run of job {} to tell about merge {}",
                        job.id,
                        merged.merge
                    ),
                }
            }
            record_event(
                db,
                project_id,
                store,
                branch,
                CatchUpEventKind::Merged,
                "system",
                &format!(
                    "merge {} brought in {}..{}: {} base-side path(s), {} on review paths{}",
                    merged.merge,
                    merged.old_base,
                    merged.new_base,
                    paths.len(),
                    hits.len(),
                    if merged.resolved {
                        "; carried the owner's committed resolution"
                    } else {
                        ""
                    }
                ),
            )
            .await?;
        } else if let Some(diagnostic) = report.conflict_diagnostics.get(branch) {
            let paths = diagnostic.conflicting_paths();
            record_event(
                db,
                project_id,
                store,
                branch,
                CatchUpEventKind::MergeConflicted,
                "system",
                &format!(
                    "merge of {} with {} (base {}) conflicted on {} path(s); the branch is \
                     unchanged and a resolution session is open",
                    diagnostic.ours.as_deref().unwrap_or("?"),
                    diagnostic.theirs.as_deref().unwrap_or("?"),
                    diagnostic.base.as_deref().unwrap_or("?"),
                    paths.len()
                ),
            )
            .await?;
        } else if let Some(reason) = report.merge_refused.get(branch) {
            // A refused merge never succeeds on its own: until the tip or the
            // base changes, every attempt meets the same ambiguous base. The
            // owner is woken once per refusal (it names the destination and the
            // candidate bases, so a new pair is a new key), not left to find it
            // on the rebase page.
            if let Some(run_id) = crate::messages::delivery::latest_run_for_job(db, &mode.job_id) {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                (branch, reason.as_str()).hash(&mut hasher);
                crate::messages::delivery::queue_system_direct_once(
                    orch,
                    &run_id,
                    &format!(
                        "[Base branch update] `{branch}` could not catch up with `{target}`: \
                         {reason}\nThe branch is unchanged and will not be merged again until \
                         its tip or the base changes. See cairn:~/rebase."
                    ),
                    crate::messages::queued::DeliveryUrgency::Steer,
                    &format!("catch-up-refused:{:016x}", hasher.finish()),
                )?;
            }
            record_event(
                db,
                project_id,
                store,
                branch,
                CatchUpEventKind::MergeRefused,
                "system",
                reason,
            )
            .await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        log::warn!("catch-up[{branch}] could not record or announce its merge outcome: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_merge_note_names_the_merge_the_range_and_review_hits_first() {
        let merged = crate::jj::MergedCatchUp {
            merge: "m".into(),
            old_base: "a".into(),
            new_base: "b".into(),
            resolved: false,
        };
        let hits = vec!["src/authorization/policy.rs".to_string()];
        let others = vec!["README.md".to_string()];
        let note = merge_note("agent/x", "main", &merged, &hits, &others);
        assert!(note.contains("Merge commit m"));
        assert!(note.contains("a..b"));
        let hit = note.find("src/authorization/policy.rs").unwrap();
        let other = note.find("README.md").unwrap();
        assert!(hit < other, "review paths come first: {note}");
        assert!(note.contains("A textually clean merge is not a reviewed merge"));
        assert!(note.contains("mark-reviewed"));

        let quiet = merge_note("agent/x", "main", &merged, &[], &others);
        assert!(quiet.contains("None of the base-side changes is on a review-required path"));
        assert!(!quiet.contains("mark-reviewed"));
    }

    #[test]
    fn review_globs_match_whole_paths_by_directory_level() {
        let globs = vec![
            "src-tauri/os/cairn-core/src/authorization/**".to_string(),
            "src-tauri/os/cairn-sandbox/".to_string(),
            "*.toml".to_string(),
        ];
        let paths: Vec<String> = [
            "src-tauri/os/cairn-core/src/authorization/policy.rs",
            "src-tauri/os/cairn-core/src/authorization/nested/deep.rs",
            "src-tauri/os/cairn-sandbox/src/lib.rs",
            "src-tauri/os/cairn-core/src/resources/rebase.rs",
            "Cargo.toml",
            "src-tauri/Cargo.toml",
        ]
        .iter()
        .map(|path| path.to_string())
        .collect();
        assert_eq!(
            review_path_hits(&globs, &paths),
            vec![
                "src-tauri/os/cairn-core/src/authorization/policy.rs".to_string(),
                "src-tauri/os/cairn-core/src/authorization/nested/deep.rs".to_string(),
                "src-tauri/os/cairn-sandbox/src/lib.rs".to_string(),
                "Cargo.toml".to_string(),
            ]
        );
        assert!(review_path_hits(&[], &paths).is_empty());
    }

    async fn seeded_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('job-1', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', 'agent/one', 'main', 'builder', 1, 1);",
        )
        .await
        .unwrap();
        db
    }

    async fn set_mode(db: &LocalDb) -> CatchUpMode {
        set(
            db,
            SetCatchUpMode {
                project_id: "proj-1",
                store: Path::new("/store"),
                bookmark: "agent/one",
                job_id: "job-1",
                actor: "operator (approved e1)",
                review_paths: &["src/authorization/**".to_string()],
                reviewed_through: "base0",
            },
        )
        .await
        .unwrap()
    }

    async fn kinds(db: &LocalDb) -> Vec<String> {
        recent_events(db, "proj-1", Path::new("/store"), "agent/one", 20)
            .await
            .unwrap()
            .into_iter()
            .rev()
            .map(|event| event.kind)
            .collect()
    }

    #[tokio::test]
    async fn a_mode_is_set_reviewed_and_listed_and_names_one_bookmark() {
        let db = seeded_db("catch-up-set.db").await;
        let mode = set_mode(&db).await;
        let store = Path::new("/store");
        assert_eq!(
            mode_for(&db, "proj-1", store, "agent/one").await.unwrap(),
            Some(mode.clone())
        );
        assert_eq!(
            mode_for(&db, "proj-1", store, "agent/other").await.unwrap(),
            None,
            "the mode covers one bookmark"
        );
        mark_reviewed(&db, &mode, "base1", "cairn://p/proj/1/1/builder")
            .await
            .unwrap();
        let reviewed = mode_for(&db, "proj-1", store, "agent/one")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reviewed.reviewed_through, "base1");
        assert_eq!(reviewed.reviewed_by, "cairn://p/proj/1/1/builder");
        assert_eq!(
            reviewed.review_paths,
            vec!["src/authorization/**".to_string()]
        );
        assert_eq!(kinds(&db).await, vec!["set", "reviewed"]);

        assert!(clear(&db, "proj-1", store, "agent/one", "operator", "done")
            .await
            .unwrap());
        assert_eq!(
            mode_for(&db, "proj-1", store, "agent/one").await.unwrap(),
            None
        );
        assert_eq!(kinds(&db).await, vec!["set", "reviewed", "cleared"]);
    }

    #[tokio::test]
    async fn a_mode_expires_lazily_when_the_issue_resolves_or_the_branch_lands() {
        let db = seeded_db("catch-up-expiry.db").await;
        let store = Path::new("/store");
        set_mode(&db).await;
        db.execute_script("UPDATE issues SET status = 'closed' WHERE id = 'issue-1';")
            .await
            .unwrap();
        assert_eq!(
            mode_for(&db, "proj-1", store, "agent/one").await.unwrap(),
            None
        );
        assert_eq!(kinds(&db).await, vec!["set", "expired"]);

        db.execute_script("UPDATE issues SET status = 'active' WHERE id = 'issue-1';")
            .await
            .unwrap();
        set_mode(&db).await;
        db.execute_script(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES ('mr-1', 'job-1', 'proj-1', 'issue-1', 'PR', 'agent/one', 'main', 'merged', 1, 1);",
        )
        .await
        .unwrap();
        assert!(mode_for(&db, "proj-1", store, "agent/one")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn resolving_the_issue_ends_the_mode_in_its_transaction() {
        let db = seeded_db("catch-up-issue.db").await;
        set_mode(&db).await;
        let later = now() + 1;
        db.write(|conn| {
            Box::pin(async move { clear_for_issue_conn(conn, "issue-1", "merged", later).await })
        })
        .await
        .unwrap();
        assert_eq!(
            mode_for(&db, "proj-1", Path::new("/store"), "agent/one")
                .await
                .unwrap(),
            None
        );
        assert_eq!(kinds(&db).await, vec!["set", "cleared"]);
    }

    #[test]
    fn malformed_review_globs_are_refused() {
        assert!(validate_review_paths(&["src/[".to_string()]).is_err());
        assert!(validate_review_paths(&[" ".to_string()]).is_err());
        assert!(validate_review_paths(&["src/**".to_string()]).is_ok());
    }
}
