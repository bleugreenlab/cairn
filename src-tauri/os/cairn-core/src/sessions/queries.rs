//! Session CRUD operations and rotation logic.

use crate::models::Session;
#[cfg(test)]
use crate::models::SessionStatus;
use crate::services::EventEmitter;
#[cfg(test)]
use crate::storage::RowExt;
use crate::storage::{DbError, LocalDb};
use cairn_common::ids;
use cairn_db::turso::params;
pub use cairn_session::sessions::*;

/// Why a job stopped using the session it was on.
///
/// Rotation is one of the most common ways a conversation ends, so the
/// predecessor row records *which* rotation ended it rather than a single opaque
/// constant. `sessions.terminal_reason` is the durable answer to "why did this
/// conversation stop", and every rotation trigger already knows its own answer
/// at the call site (CAIRN-4613).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotationReason {
    /// An explicit model edit moved the job to another provider.
    ProviderChange,
    /// The job's model moved to a provider the session's backend cannot serve.
    BackendChange,
    /// A cold or forced reseed rebuilt the conversation from its own digest.
    Reseed,
    /// A thread compacted its era into a seed and continued on a fresh session.
    Compaction,
    /// The provider's prompt cache went cold, and the bounded seed was measured
    /// to cost materially less than recreating the whole prefix (CAIRN-4757).
    /// Separable from [`RotationReason::Reseed`] in a census precisely because
    /// this is the one cause a clock participates in.
    CacheExpiry,
    /// A session an older build closed as
    /// `provider_silence_already_terminal_reconciled` was continued onto a
    /// successor. Legacy data only — CAIRN-4618 stopped watchdog reconciliation
    /// from closing anything, so no session minted since can reach this. The
    /// source is already terminal when this rotation runs, so the close below
    /// leaves it alone and this reason is never the one written; it names the
    /// call site rather than an outcome.
    RecoveryReplacement,
}

impl RotationReason {
    /// The value written to the predecessor's `sessions.terminal_reason`.
    pub(crate) fn terminal_reason(self) -> &'static str {
        match self {
            RotationReason::ProviderChange => "provider_change",
            RotationReason::BackendChange => "rotated_backend_change",
            RotationReason::Reseed => "rotated_reseed",
            RotationReason::Compaction => "rotated_compaction",
            RotationReason::CacheExpiry => "rotated_cache_expiry",
            RotationReason::RecoveryReplacement => "rotated_after_recovery",
        }
    }
}

/// The rest of a provider change's durable state, committed by the same
/// transaction as the rotation itself.
///
/// A model edit that crosses providers is one transition, not three writes that
/// usually happen together. When `jobs.model` could commit ahead of the
/// compare-and-swap, a failed rotation left the job naming the new provider
/// while still pointing at an open session recorded on the old one — the exact
/// stale-backend state the rotation exists to prevent — and a snapshot edit
/// could strand an arbitrary prefix of its jobs there with no coherent retry,
/// since repeating the edit would see already-mutated model state.
#[derive(Clone)]
pub(crate) struct ProviderChangeCommit {
    /// The job's new model, applied with the swap that makes it runnable.
    pub model: Option<String>,
    /// The run whose transcript carries the boundary for this replacement.
    /// `None` for a job that has never run, which has no transcript to mark.
    pub run_id: Option<String>,
    /// The turn the boundary belongs to, when the run has one.
    pub turn_id: Option<String>,
}

/// Rotate while explicitly recording the backend that will serve the successor.
/// Seeded rebuilds use this even when the provider did not change: a model edit
/// can precede a reseed, so inheriting the predecessor's label is not truthful.
pub(crate) async fn rotate_job_session_with_backend(
    db: &LocalDb,
    old: &Session,
    job_id: &str,
    backend: &str,
    reason: RotationReason,
    commit: Option<ProviderChangeCommit>,
    emitter: &dyn EventEmitter,
) -> Result<Session, String> {
    if matches!(
        reason,
        RotationReason::ProviderChange | RotationReason::BackendChange
    ) && old.backend.eq_ignore_ascii_case(backend)
    {
        log::error!(
            "refusing same-provider session rotation for {}: {} -> {}",
            old.id,
            old.backend,
            backend
        );
        return Err(format!(
            "refusing same-provider session rotation: session {} already runs on {}",
            old.id, backend
        ));
    }
    rotate_session(
        db,
        old,
        job_id,
        Some(backend.to_string()),
        reason,
        commit,
        emitter,
    )
    .await
}

/// Mint the successor session, hand the job to it, close the predecessor, and
/// announce the job change.
///
/// All four are one write. The successor only exists because the job is moving
/// to it, so there is no rotation that leaves the job behind — and the
/// predecessor's close is only honest because the same transaction re-pointed
/// the job away from it.
///
/// The emit is taken here rather than at the call sites because rotation is the
/// one write that swaps `jobs.current_session_id` out from under whatever is
/// already on screen. Every surface that reads a node through a session id — the
/// chat minimap's skyline bars, the context-usage meter, the last-event digest —
/// keys off the job's *current* session, so without this emit they keep polling
/// a session nobody writes to again and go silently stale until a reload
/// (CAIRN-3262). Owning the emit inside the rotation seam is what makes that
/// impossible to forget when a fourth rotation trigger is added.
async fn rotate_session(
    db: &LocalDb,
    source: &Session,
    job_id: &str,
    target_backend: Option<String>,
    reason: RotationReason,
    commit: Option<ProviderChangeCommit>,
    emitter: &dyn EventEmitter,
) -> Result<Session, String> {
    let source = source.clone();
    let job_id = job_id.to_string();
    let new_id = ids::mint_session_id().into_string();
    // The successor inherits the source backend unless a switch was requested.
    let backend = target_backend.unwrap_or_else(|| source.backend.clone());

    let rotated = db
        .write(|conn| {
            let source = source.clone();
            let job_id = job_id.clone();
            let new_id = new_id.clone();
            let backend = backend.clone();
            let commit = commit.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                let session = create_with_id_and_lineage_conn(
                    conn,
                    &new_id,
                    Some(job_id.as_str()),
                    None,
                    &backend,
                    Some(&source.id),
                    source.sequence + 1,
                )
                .await?;

                let rows = conn
                    .execute(
                        "UPDATE jobs
                         SET current_session_id = ?1, updated_at = ?2
                         WHERE id = ?3 AND current_session_id = ?4",
                        params![new_id.as_str(), now, job_id.as_str(), source.id.as_str()],
                    )
                    .await?;

                if rows == 0 {
                    conn.execute(
                        "DELETE FROM sessions WHERE id = ?1",
                        params![new_id.as_str()],
                    )
                    .await?;
                    return Err(DbError::internal(
                        "Concurrent session rotation detected; current_session_id already moved",
                    ));
                }

                conn.execute(
                    "UPDATE sessions SET replaced_by_id = ?1, updated_at = ?2 WHERE id = ?3",
                    params![new_id.as_str(), now, source.id.as_str()],
                )
                .await?;

                // Being replaced is terminal for the predecessor: the job no longer
                // points at it, so no turn will ever run there again. Closing it in
                // this same transaction is what lets a reader take `sessions.status`
                // at face value — there is no window in which both the predecessor
                // and its successor read open (CAIRN-4613).
                //
                // The `status = 'open'` guard is the same one every other closer
                // carries, and here it is load-bearing rather than defensive: the
                // legacy recovery rotation starts from a session an older build
                // already closed with the reason it actually died of, and that
                // reason describes the death better than the rotation that followed
                // it does.
                conn.execute(
                    "UPDATE sessions
                     SET status = 'closed', terminal_reason = ?1, closed_at = ?2, updated_at = ?2
                     WHERE id = ?3 AND status = 'open'",
                    params![reason.terminal_reason(), now, source.id.as_str()],
                )
                .await?;

                // The rest of a provider change rides in this transaction, so
                // the model the job will launch, the session it will launch
                // under, and the marker explaining the seam either all land or
                // none do.
                if let Some(commit) = commit.as_ref() {
                    if let Some(model) = commit.model.as_deref() {
                        conn.execute(
                            "UPDATE jobs SET model = ?1, updated_at = ?2 WHERE id = ?3",
                            params![model, now, job_id.as_str()],
                        )
                        .await?;
                    }
                    if let Some(run_id) = commit.run_id.as_deref() {
                        crate::messages::transcript::insert_session_boundary_conn(
                            conn,
                            run_id,
                            Some(source.id.as_str()),
                            commit.turn_id.as_deref(),
                            serde_json::json!({
                                "kind": "provider_change",
                                "fromBackend": source.backend,
                                "toBackend": backend,
                                "model": commit.model,
                            }),
                        )
                        .await?;
                    }
                }

                Ok(session)
            })
        })
        .await
        .map_err(|e| format!("Failed to rotate session: {e}"))?;

    let _ = emitter.emit(
        "db-change",
        crate::notify::job_db_change_for_id(db, &job_id, "update").await,
    );

    Ok(rotated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;

    #[tokio::test]
    async fn account_repin_compare_and_set_preserves_the_first_concurrent_winner() {
        let db = crate::storage::migrated_test_db("session-account-repin-cas.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj', 'ws', 'Project', 'proj', '/tmp/proj', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES ('job', 'proj', 'running', 'session', 1, 1);
             INSERT INTO sessions(id, job_id, backend, account_id, status, sequence, created_at, updated_at)
               VALUES ('session', 'job', 'claude', 'account-a', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed a pinned provider session");

        assert!(
            compare_and_set_account_id(&db, "session", "account-a", "account-b")
                .await
                .expect("first replacement wins")
        );
        assert!(
            !compare_and_set_account_id(&db, "session", "account-a", "account-c")
                .await
                .expect("stale replacement loses")
        );
        assert_eq!(
            get(&db, "session")
                .await
                .expect("read durable winner")
                .account_id
                .as_deref(),
            Some("account-b")
        );
    }

    /// Seed a node whose job carries `model` and whose open session runs on
    /// `claude`, so a provider change to `codex` has somewhere to fail.
    async fn seed_provider_change(file: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(file).await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('ws-pc', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('proj-pc', 'ws-pc', 'Project', 'PC', '/tmp/pc', 1, 1);
             INSERT INTO jobs(id, project_id, status, model, current_session_id, created_at, updated_at)
              VALUES ('job-pc', 'proj-pc', 'running', 'claude-opus-4', 'sess-pc-1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-pc-1', 'job-pc', 'claude', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed a node mid-conversation on claude");
        db
    }

    async fn job_model(db: &LocalDb) -> Option<String> {
        db.query_text("SELECT model FROM jobs WHERE id = 'job-pc'", ())
            .await
            .expect("read the job's model")
    }

    async fn current_session(db: &LocalDb) -> Option<String> {
        db.query_text(
            "SELECT current_session_id FROM jobs WHERE id = 'job-pc'",
            (),
        )
        .await
        .expect("read the job's current session")
    }

    async fn count(db: &LocalDb, sql: &str) -> i64 {
        db.query_all(sql, (), |row| row.i64(0))
            .await
            .expect("count rows")
            .first()
            .copied()
            .unwrap_or_default()
    }

    fn provider_change_to_codex() -> Option<ProviderChangeCommit> {
        Some(ProviderChangeCommit {
            model: Some("gpt-5-codex".to_string()),
            run_id: Some("run-pc".to_string()),
            turn_id: None,
        })
    }

    /// The model and the boundary are written BY the rotation, not around it.
    ///
    /// This is the half of the invariant the rollback tests cannot show on their
    /// own: if the model is written by the edit surface beforehand, a rotation
    /// that never touches it still leaves the old value here, and every failure
    /// test would pass while the real composition stayed separable.
    #[tokio::test]
    async fn a_provider_change_writes_the_model_and_boundary_with_the_swap() {
        let db = seed_provider_change("provider-change-writes-with-swap.db").await;
        db.execute_script(
            "INSERT INTO runs(id, job_id, status, created_at, updated_at)
              VALUES ('run-pc', 'job-pc', 'live', 1, 1);",
        )
        .await
        .expect("the job has a run whose transcript carries the boundary");
        let source = get(&db, "sess-pc-1").await.expect("source session");

        let successor = rotate_job_session_with_backend(
            &db,
            &source,
            "job-pc",
            "codex",
            RotationReason::ProviderChange,
            provider_change_to_codex(),
            &CapturingEmitter::new(),
        )
        .await
        .expect("the provider change commits");

        assert_eq!(successor.backend, "codex");
        assert_eq!(
            job_model(&db).await.as_deref(),
            Some("gpt-5-codex"),
            "the rotation must carry the model the successor will launch"
        );
        assert_eq!(
            current_session(&db).await.as_deref(),
            Some(successor.id.as_str()),
            "the job must point at the successor"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM events WHERE event_type = 'system:session_boundary'"
            )
            .await,
            1,
            "exactly one boundary marks the replacement"
        );
    }

    /// A provider change that loses the compare-and-swap must leave NOTHING
    /// behind — not the model it was going to launch, not the backend it was
    /// going to launch on, and not the divider announcing the switch.
    ///
    /// This is the state the operation existed to prevent and used to produce:
    /// the model was committed by the edit surface before the rotation ran, so a
    /// lost race left `jobs.model` naming a provider that `sessions.backend`
    /// contradicted, and the job could only refuse to start.
    #[tokio::test]
    async fn a_lost_swap_leaves_the_model_session_and_boundary_all_unchanged() {
        let db = seed_provider_change("provider-change-lost-swap.db").await;
        let source = get(&db, "sess-pc-1").await.expect("source session");

        // Someone else rotates the job first: the swap this call is about to
        // attempt can no longer match.
        db.execute_script(
            "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-pc-2', 'job-pc', 'claude', 'open', 2, 1, 1);
             UPDATE jobs SET current_session_id = 'sess-pc-2' WHERE id = 'job-pc';",
        )
        .await
        .expect("a concurrent rotation moves the job");

        let error = rotate_job_session_with_backend(
            &db,
            &source,
            "job-pc",
            "codex",
            RotationReason::ProviderChange,
            provider_change_to_codex(),
            &CapturingEmitter::new(),
        )
        .await
        .expect_err("a rotation that lost the swap must fail");
        assert!(
            error.contains("Concurrent session rotation detected"),
            "unexpected failure: {error}"
        );

        assert_eq!(
            job_model(&db).await.as_deref(),
            Some("claude-opus-4"),
            "the model must not survive the transition that failed to carry it"
        );
        assert_eq!(
            current_session(&db).await.as_deref(),
            Some("sess-pc-2"),
            "the losing rotation must not re-point the job"
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM sessions WHERE backend = 'codex'").await,
            0,
            "no successor may be left on the new provider"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM events WHERE event_type = 'system:session_boundary'"
            )
            .await,
            0,
            "a replacement that did not happen must not be announced"
        );
    }

    /// If the boundary cannot be written, the replacement it describes must not
    /// exist either. The marker and the rotation are one fact.
    ///
    /// The failure is injected by aiming the boundary at a run that does not
    /// exist, so the event insert is the one statement in the transaction that
    /// cannot succeed while every other statement can.
    #[tokio::test]
    async fn a_boundary_that_cannot_be_written_rolls_back_the_whole_transition() {
        let db = seed_provider_change("provider-change-unwritable-boundary.db").await;
        let source = get(&db, "sess-pc-1").await.expect("source session");

        let error = rotate_job_session_with_backend(
            &db,
            &source,
            "job-pc",
            "codex",
            RotationReason::ProviderChange,
            provider_change_to_codex(),
            &CapturingEmitter::new(),
        )
        .await
        .expect_err("a rotation whose boundary cannot be written must fail");
        assert!(
            error.contains("Failed to rotate session"),
            "unexpected failure: {error}"
        );

        assert_eq!(
            job_model(&db).await.as_deref(),
            Some("claude-opus-4"),
            "the model must roll back with the rotation it belonged to"
        );
        assert_eq!(
            current_session(&db).await.as_deref(),
            Some("sess-pc-1"),
            "the job must still point at the session it never left"
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM sessions").await,
            1,
            "the successor must not outlive the transaction that minted it"
        );
        let (status, backend) = db
            .query_all(
                "SELECT status, backend FROM sessions WHERE id = 'sess-pc-1'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .expect("read the predecessor")
            .pop()
            .expect("the predecessor still exists");
        assert_eq!(
            (status.as_str(), backend.as_str()),
            ("open", "claude"),
            "the predecessor must not be closed by a replacement that rolled back"
        );
    }

    #[tokio::test]
    async fn rotating_the_active_session_announces_the_job_change() {
        // Every surface that reads a node through a session id keys off
        // `jobs.current_session_id`. Rotation swaps it, so rotation owes the
        // frontend a scoped `jobs` emit — without it the chat minimap, the
        // context meter, and the last-event digest keep reading a session that
        // will never be written to again (CAIRN-3262).
        let db = crate::storage::migrated_test_db("session-rotation-emit.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('ws-rot', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('proj-rot', 'ws-rot', 'Project', 'ROT', '/tmp/rot', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
              VALUES ('job-rot', 'proj-rot', 'running', 'sess-rot-1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-rot-1', 'job-rot', 'claude', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed a node on its first session");

        let source = get(&db, "sess-rot-1").await.expect("source session");
        let emitter = CapturingEmitter::new();
        let successor = rotate_job_session_with_backend(
            &db,
            &source,
            "job-rot",
            "claude",
            RotationReason::Reseed,
            None,
            &emitter,
        )
        .await
        .expect("rotation succeeds");

        assert_eq!(successor.parent_session_id.as_deref(), Some("sess-rot-1"));

        let changes = emitter.events_named("db-change");
        assert_eq!(changes.len(), 1, "exactly one announcement: {changes:?}");
        assert_eq!(changes[0]["table"], "jobs");
        assert_eq!(changes[0]["jobId"], "job-rot");
        // The frontend invalidates precisely from these ids and has no
        // cache-scan fallback, so the payload must carry project scope too.
        assert_eq!(changes[0]["projectId"], "proj-rot");

        let current = db
            .query_one(
                "SELECT current_session_id FROM jobs WHERE id = ?1",
                params!["job-rot"],
                |row| row.text(0),
            )
            .await
            .expect("read back the active session");
        assert_eq!(current, successor.id);
    }

    #[tokio::test]
    async fn session_rotation_inherits_channel_delivery_cursor() {
        let db = crate::storage::migrated_test_db("session-rotation-channel-cursor.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Project', 'proj', '/tmp/proj', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES ('i', 'p', 1, 'Issue', 'active', 1, 1);
             INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
               VALUES ('j', 'p', 'i', 'running', 's1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('s1', 'j', 'claude', 'open', 1, 1, 1);
             INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
               VALUES ('old', 'issue', 'proj/1', 'planner', 'before rotation', 2);
             UPDATE sessions
                SET channel_cursor_rowid = (SELECT rowid FROM messages WHERE id = 'old')
              WHERE id = 's1';",
        )
        .await
        .expect("seed a delivered channel message");

        let source = get(&db, "s1").await.expect("source session");
        let successor = rotate_job_session_with_backend(
            &db,
            &source,
            "j",
            "claude",
            RotationReason::Compaction,
            None,
            &CapturingEmitter::new(),
        )
        .await
        .expect("manual compaction rotates the session");

        let inherited_cursor = db
            .query_opt_i64(
                "SELECT channel_cursor_rowid FROM sessions WHERE id = ?1",
                params![successor.id.as_str()],
            )
            .await
            .expect("read successor cursor");
        assert!(
            inherited_cursor.is_some(),
            "successor forgot the delivery cursor"
        );

        let pending = crate::orchestrator::session::pending_channel_messages_for_job(&db, "j", 20)
            .await
            .expect("list pending messages after rotation");
        assert!(
            pending.is_empty(),
            "a message delivered before rotation was offered again"
        );

        db.execute(
            "INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
             VALUES ('new', 'issue', 'proj/1', 'planner', 'after rotation', 3)",
            (),
        )
        .await
        .expect("insert a post-rotation message");

        let pending = crate::orchestrator::session::pending_channel_messages_for_job(&db, "j", 20)
            .await
            .expect("list newly pending messages");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "after rotation");
    }

    #[tokio::test]
    async fn rotation_closes_the_session_it_replaced() {
        // A rotated-out session used to keep reading `open` forever with a NULL
        // terminal reason, so "does this node have a live session" answered yes
        // for every predecessor a job had ever held (CAIRN-4613).
        let db = crate::storage::migrated_test_db("session-rotation-closes-source.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('proj', 'ws', 'Project', 'proj', '/tmp/proj', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
              VALUES ('job', 'proj', 'running', 'sess-1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-1', 'job', 'claude', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed a node on its first session");

        let source = get(&db, "sess-1").await.expect("source session");
        let successor = rotate_job_session_with_backend(
            &db,
            &source,
            "job",
            "codex",
            RotationReason::BackendChange,
            None,
            &CapturingEmitter::new(),
        )
        .await
        .expect("a cross-provider model change rotates the session");

        let replaced = get(&db, "sess-1").await.expect("read back the predecessor");
        assert_eq!(replaced.status, SessionStatus::Closed);
        assert_eq!(
            replaced.terminal_reason.as_deref(),
            Some("rotated_backend_change"),
            "the predecessor must name the rotation that ended it"
        );
        assert!(
            replaced.closed_at.is_some(),
            "a closed session records when it closed"
        );
        assert_eq!(
            replaced.replaced_by_id.as_deref(),
            Some(successor.id.as_str())
        );

        let live = get(&db, &successor.id)
            .await
            .expect("read back the successor");
        assert_eq!(
            live.status,
            SessionStatus::Open,
            "exactly one session of a lineage reads open, and it is the newest"
        );
        assert_eq!(
            live.backend, "codex",
            "the successor records the provider that will actually spawn"
        );
        assert!(
            live.backend_id.is_none(),
            "the provider handle is unknown until the successor starts"
        );
    }

    #[tokio::test]
    async fn cross_provider_rotation_refuses_a_same_provider_target() {
        let db = crate::storage::migrated_test_db("session-rotation-provider-guard.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj', 'ws', 'Project', 'proj', '/tmp/proj', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES ('job', 'proj', 'running', 'sess-1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
               VALUES ('sess-1', 'job', 'claude', 'native-handle', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed an open Claude session");

        let source = get(&db, "sess-1").await.expect("source session");
        let error = rotate_job_session_with_backend(
            &db,
            &source,
            "job",
            "claude",
            RotationReason::BackendChange,
            None,
            &CapturingEmitter::new(),
        )
        .await
        .expect_err("a claude-to-claude replacement is a lifecycle bug");

        assert!(error.contains("refusing same-provider session rotation"));
        let unchanged = get(&db, "sess-1").await.expect("source remains current");
        assert_eq!(unchanged.status, SessionStatus::Open);
        assert_eq!(unchanged.backend_id.as_deref(), Some("native-handle"));
        assert_eq!(
            db.query_opt_text("SELECT current_session_id FROM jobs WHERE id = 'job'", (),)
                .await
                .expect("read current session")
                .as_deref(),
            Some("sess-1")
        );
    }

    #[tokio::test]
    async fn rotation_leaves_an_already_terminal_predecessor_its_own_reason() {
        // Watchdog recovery in older builds closed the session it reconciled
        // with the reason it actually died of, and a continuation then rotated
        // off that closed row. That reason describes the death better than the
        // rotation that followed it does, so the close is guarded on
        // `status = 'open'` rather than overwriting (CAIRN-4613).
        let db = crate::storage::migrated_test_db("session-rotation-keeps-reason.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES ('proj', 'ws', 'Project', 'proj', '/tmp/proj', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
              VALUES ('job', 'proj', 'running', 'sess-1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, terminal_reason,
                                  sequence, created_at, closed_at, updated_at)
              VALUES ('sess-1', 'job', 'codex', 'closed',
                      'provider_silence_already_terminal_reconciled', 1, 1, 9, 9);",
        )
        .await
        .expect("seed a session an older build closed on provider silence");

        let source = get(&db, "sess-1").await.expect("source session");
        rotate_job_session_with_backend(
            &db,
            &source,
            "job",
            "codex",
            RotationReason::RecoveryReplacement,
            None,
            &CapturingEmitter::new(),
        )
        .await
        .expect("a continuation rotates off the reconciled session");

        let replaced = get(&db, "sess-1").await.expect("read back the predecessor");
        assert_eq!(
            replaced.terminal_reason.as_deref(),
            Some("provider_silence_already_terminal_reconciled"),
            "the rotation must not overwrite a real cause of death"
        );
        assert_eq!(
            replaced.closed_at,
            Some(9),
            "nor move when the conversation actually ended"
        );
    }
}
