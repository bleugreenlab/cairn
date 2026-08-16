//! Shared PTY and persisted terminal host logic for Cairn hosts.

use crate::db::DbState;
use crate::mcp::handlers::slug::slugify;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::ResidencyFence;
use cairn_db::turso::params;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyCommandSnapshot {
    busy: bool,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
}

fn emit_terminal_change(orch: &Orchestrator, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": action}),
    );
}

/// Durable running rows are attachable only while their session exists in this
/// host. A new host starts with an empty PtyState, so settle runner-local orphans
/// as exited and fence executor-backed orphans into recovery before any terminal
/// query can advertise them.
pub async fn prepare_terminal_recovery(orch: &Orchestrator) -> Result<u64, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|error| error.to_string())?
        .keys()
        .cloned()
        .collect();
    let updated = orch.db.local.write(|conn| {
        let sessions = sessions.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, session_id, residency_holder FROM job_terminals
                     WHERE status IN ('running', 'recovering')",
                    (),
                )
                .await?;
            let mut local_orphans = Vec::new();
            let mut resident_orphans = Vec::new();
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                if !sessions.contains(&row.text(1)?) {
                    if row.opt_text(2)?.is_some() {
                        resident_orphans.push(id);
                    } else {
                        local_orphans.push(id);
                    }
                }
            }
            drop(rows);
            let mut updated = 0;
            let now = chrono::Utc::now().timestamp();
            for id in local_orphans {
                updated += conn.execute(
                    "UPDATE job_terminals
                     SET status = 'exited', exited_at = COALESCE(exited_at, ?2)
                     WHERE id = ?1 AND status IN ('running', 'recovering')",
                    params![id.as_str(), now],
                ).await?;
            }
            for id in resident_orphans {
                updated += conn.execute(
                    "UPDATE job_terminals SET status = 'recovering' WHERE id = ?1 AND status = 'running'",
                    (id.as_str(),),
                ).await?;
            }
            Ok(updated)
        })
    }).await.map_err(|error| error.to_string())?;
    if updated > 0 {
        emit_terminal_change(orch, "update");
    }
    crate::mcp::handlers::terminal::reconcile_exited_terminal_wakes(orch).await?;
    Ok(updated)
}

#[derive(Clone)]
struct RecoverableTerminal {
    id: String,
    session_id: String,
    status: String,
    residency_holder: String,
    incarnation_id: String,
    cell_epoch: u64,
    transition_started_at: Option<i64>,
}

async fn recoverable_terminals(db: &LocalDb) -> DbResult<Vec<RecoverableTerminal>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, session_id, status, residency_holder, residency_incarnation_id, cell_epoch, exited_at
             FROM job_terminals
             WHERE status IN ('recovering', 'closing') AND residency_holder IS NOT NULL",
                    (),
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(RecoverableTerminal {
                    id: row.text(0)?,
                    session_id: row.text(1)?,
                    status: row.text(2)?,
                    residency_holder: row.text(3)?,
                    incarnation_id: row.text(4)?,
                    cell_epoch: row.i64(5)? as u64,
                    transition_started_at: row.opt_i64(6)?,
                });
            }
            Ok(terminals)
        })
    })
    .await
}

async fn delete_closing_terminal(db: &LocalDb, terminal: &RecoverableTerminal) -> DbResult<bool> {
    let terminal = terminal.clone();
    db.write(|conn| {
        let terminal = terminal.clone();
        Box::pin(async move {
            Ok(conn
                .execute(
                    "DELETE FROM job_terminals
             WHERE id = ?1 AND status = 'closing' AND residency_holder = ?2
               AND residency_incarnation_id = ?3 AND cell_epoch = ?4",
                    params![
                        terminal.id,
                        terminal.residency_holder,
                        terminal.incarnation_id,
                        terminal.cell_epoch as i64
                    ],
                )
                .await?
                > 0)
        })
    })
    .await
}

/// Reconcile durable recovery/close intents against the fleet's complete
/// advertised fence. Missing owners remain fenced until the full recovery grace.
async fn reconcile_terminal_lifecycle(orch: &Orchestrator) -> Result<u64, String> {
    let rows = recoverable_terminals(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    let snapshot = orch.fleet.snapshot();
    let mut changed = 0;
    for row in rows {
        let fence = snapshot.cells.iter().find_map(|cell| {
            let residency = cell.residency.as_ref()?;
            (residency.holder.storage_key() == row.residency_holder
                && residency.incarnation_id == row.incarnation_id
                && cell.cell_epoch == row.cell_epoch)
                .then(|| ResidencyFence {
                    holder: residency.holder.clone(),
                    incarnation_id: row.incarnation_id.clone(),
                    cell_epoch: row.cell_epoch,
                })
        });
        if row.status == "recovering" {
            if let Some(fence) = fence {
                match crate::mcp::handlers::terminal::restart_residency_terminals(orch, fence).await
                {
                    Ok(_) => changed += 1,
                    Err(error) => {
                        tracing::warn!(terminal_id = %row.id, %error, "terminal recovery retry failed")
                    }
                }
            } else if terminal_owner_recovery_window_elapsed()
                && resolve_missing_terminal_lease(
                    &orch.db.local,
                    &row.residency_holder,
                    &row.incarnation_id,
                    row.cell_epoch,
                )
                .await
                .map_err(|error| error.to_string())?
            {
                changed += 1;
            }
        } else if let Some(_fence) = fence {
            match crate::mcp::handlers::terminal::release_terminal_by_session(orch, &row.session_id)
                .await
            {
                Ok(()) => {
                    if delete_closing_terminal(&orch.db.local, &row)
                        .await
                        .map_err(|e| e.to_string())?
                    {
                        changed += 1;
                    }
                }
                Err(error) => {
                    tracing::warn!(terminal_id = %row.id, %error, "terminal close retry failed")
                }
            }
        } else if row.transition_started_at.is_some_and(|started| {
            chrono::Utc::now().timestamp().saturating_sub(started)
                >= terminal_owner_recovery_duration().as_secs() as i64
        }) && delete_closing_terminal(&orch.db.local, &row)
            .await
            .map_err(|e| e.to_string())?
        {
            changed += 1;
        }
    }
    if changed > 0 {
        emit_terminal_change(orch, "update");
        crate::mcp::handlers::terminal::reconcile_exited_terminal_wakes(orch).await?;
    }
    Ok(changed)
}

pub fn schedule_terminal_lifecycle_recovery(orch: Orchestrator) {
    TERMINAL_OWNER_RECOVERY_STARTED.get_or_init(Instant::now);
    tokio::spawn(async move {
        loop {
            if let Err(error) = reconcile_terminal_lifecycle(&orch).await {
                tracing::warn!(%error, "terminal lifecycle reconciliation failed");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SearchIndex;

    async fn seeded_lease_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO job_terminals
                 (id, session_id, command, status, created_at, slug,
                  residency_holder, residency_incarnation_id, cell_epoch, process_generation)
             VALUES
                 ('exited', 's-exited', 'true', 'exited', 1, 'exited', 'lease-exited', 'inc-exited', 1, 4),
                 ('running', 's-running', 'true', 'running', 1, 'running', 'lease-running', 'inc-running', 2, 5);",
        )
        .await
        .unwrap();
        db
    }

    async fn active_terminal_db() -> DbState {
        let local = Arc::new(crate::storage::migrated_test_db("terminal-host-active.db").await);
        local
            .execute_script(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
                 INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p', 'w', 'P', 'p', '/tmp', 1, 1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i', 'p', 1, 'I', 'active', 'active', 'none', 1, 1);
                 INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j', 'p', 'i', 'running', 'job-session', 1, 1);
                 INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES('t', 'p', 'thread-ux', 'active', 'none', 1, 1);
                 INSERT INTO jobs(id, project_id, thread_id, status, uri_segment, node_name, created_at, updated_at) VALUES('t-job', 'p', 't', 'idle', 'thread', 'Thread', 1, 1);
                 INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('thread-terminal', 't-job', 'thread-session', 'thread command', 'running', 10, 'thread-term');
                 INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('job-terminal', 'j', 'job-session', 'job command', 'running', 20, 'job');
                 INSERT INTO job_terminals(id, project_id, session_id, command, status, created_at, slug) VALUES('project-terminal', 'p', 'project-session', 'project command', 'running', 30, 'project');",
            )
            .await
            .unwrap();
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        DbState::new(local, index)
    }

    #[tokio::test]
    async fn startup_cleanup_clears_only_exited_terminal_fences() {
        let db = seeded_lease_db("terminal-host-startup-cleanup.db").await;
        let advertised = HashSet::from([("lease-exited".to_string(), "inc-exited".to_string(), 1)]);
        assert_eq!(
            clear_unadvertised_exited_terminal_lease_bindings(&db, &advertised)
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            clear_unadvertised_exited_terminal_lease_bindings(&db, &HashSet::new())
                .await
                .unwrap(),
            1
        );

        let exited: (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'exited'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        let running: (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(exited, ("exited".to_string(), None));
        assert_eq!(
            running,
            ("running".to_string(), Some("lease-running".to_string()))
        );
    }

    #[tokio::test]
    async fn missing_lease_resolution_is_fenced_and_idempotent() {
        let db = seeded_lease_db("terminal-host-missing-lease.db").await;
        db.execute_script("UPDATE job_terminals SET status = 'recovering' WHERE id = 'running';")
            .await
            .unwrap();
        assert!(
            !resolve_missing_terminal_lease(&db, "lease-running", "old-incarnation", 2)
                .await
                .unwrap()
        );
        assert!(
            resolve_missing_terminal_lease(&db, "lease-running", "inc-running", 2)
                .await
                .unwrap()
        );
        assert!(
            !resolve_missing_terminal_lease(&db, "lease-running", "inc-running", 2)
                .await
                .unwrap()
        );

        let (status, residency_holder, exited_at): (String, Option<String>, Option<i64>) = db
            .query_one(
                "SELECT status, residency_holder, exited_at FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(status, "exited");
        assert_eq!(residency_holder, None);
        assert!(exited_at.is_some());
    }

    /// The transition that makes recovery reachable at all. Before this, a
    /// terminal whose executor went away was finalized as exited, so the
    /// reconcile loop — which acts only on `recovering` rows — never saw it and
    /// the restart had to be asked for by hand.
    #[tokio::test]
    async fn an_executor_loss_fences_a_running_terminal_into_recovery() {
        let db = Arc::new(seeded_lease_db("terminal-host-mark-recovering.db").await);

        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-running".into())
                .await
                .unwrap(),
            TerminalRecovery::Fenced
        );
        // A second delivery of the same death moves nothing — but it must say
        // that recovery already holds the terminal, not merely that this call
        // changed no row. The caller releases the environment on the second
        // answer and retains it on the first, so collapsing them costs the
        // pending restart the cell it was going to start in.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-running".into())
                .await
                .unwrap(),
            TerminalRecovery::AlreadyRecovering
        );
        // A terminal that already ended is not dragged back into recovery, and
        // is reported as something recovery does not hold.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-exited".into())
                .await
                .unwrap(),
            TerminalRecovery::NotRecoverable
        );
        // A session with no row at all is likewise nothing to recover.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-absent".into())
                .await
                .unwrap(),
            TerminalRecovery::NotRecoverable
        );

        let (status, holder): (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(status, "recovering");
        // The environment binding survives: it is what the restart starts into,
        // and clearing it here would leave recovery with nowhere to go.
        assert_eq!(holder, Some("lease-running".to_string()));
    }

    #[test]
    fn owner_recovery_window_uses_startup_not_terminal_exit_time() {
        let started = Instant::now();
        assert!(!owner_recovery_window_elapsed(
            started,
            started + Duration::from_secs(599)
        ));
        assert!(owner_recovery_window_elapsed(
            started,
            started + Duration::from_secs(600)
        ));
    }

    #[tokio::test]
    async fn running_terminals_include_scope_creation_time_in_newest_first_order() {
        let db = active_terminal_db().await;

        let terminals = get_running_terminals(&db).await.unwrap();

        assert_eq!(terminals.len(), 3);
        assert_eq!(terminals[0].id, "project-terminal");
        assert_eq!(terminals[0].job_id, None);
        assert_eq!(terminals[0].created_at, 30);
        assert_eq!(terminals[1].id, "job-terminal");
        assert_eq!(terminals[1].job_id.as_deref(), Some("j"));
        assert_eq!(terminals[1].created_at, 20);
        assert_eq!(terminals[2].id, "thread-terminal");
        assert_eq!(terminals[2].created_at, 10);
    }

    /// This listing is what draws the facet strip, so a terminal missing from it
    /// is a terminal the user cannot reach. A thread's job has no issue and no
    /// execution, and joining them inwards dropped every thread-owned terminal:
    /// the operator's thread terminal ran, accepted typing, and had no chip
    /// (CAIRN-3841).
    #[tokio::test]
    async fn a_threads_running_terminal_is_listed_and_names_its_thread() {
        let db = active_terminal_db().await;

        let terminals = get_running_terminals(&db).await.unwrap();

        let thread = terminals
            .iter()
            .find(|terminal| terminal.id == "thread-terminal")
            .expect("a thread's running terminal belongs in the running listing");
        assert_eq!(thread.job_id.as_deref(), Some("t-job"));
        assert_eq!(thread.slug.as_deref(), Some("thread-term"));
        assert_eq!(thread.project_key, "p");
        assert_eq!(thread.project_id, "p");
        // No issue coordinate to report, and the thread named instead of it.
        assert_eq!(thread.issue_id, None);
        assert_eq!(thread.issue_number, None);
        assert_eq!(thread.exec_seq, None);
        assert_eq!(thread.thread_name.as_deref(), Some("thread-ux"));

        // An issue node's terminal keeps its own coordinate and claims no thread.
        let node = terminals
            .iter()
            .find(|terminal| terminal.id == "job-terminal")
            .expect("an issue node's terminal is still listed");
        assert_eq!(node.issue_number, Some(1));
        assert_eq!(node.thread_name, None);
    }
}

pub(crate) const TERMINAL_HEARTBEAT_TIMEOUT_MS: u64 = 5 * 60 * 1000;
pub(crate) const TERMINAL_RECLAIM_GRACE_MS: u64 = 5 * 60 * 1000;
static TERMINAL_OWNER_RECOVERY_STARTED: OnceLock<Instant> = OnceLock::new();

fn terminal_owner_recovery_duration() -> Duration {
    Duration::from_millis(TERMINAL_HEARTBEAT_TIMEOUT_MS + TERMINAL_RECLAIM_GRACE_MS)
}

fn owner_recovery_window_elapsed(started: Instant, now: Instant) -> bool {
    now.duration_since(started) >= terminal_owner_recovery_duration()
}

pub(crate) fn terminal_owner_recovery_window_elapsed() -> bool {
    TERMINAL_OWNER_RECOVERY_STARTED
        .get()
        .is_some_and(|started| owner_recovery_window_elapsed(*started, Instant::now()))
}

/// Start the owner-loss clock for leases retained by executors across this host's
/// restart. Cleanup waits the full heartbeat plus reclaim window regardless of
/// when the terminal process itself exited.
pub fn schedule_exited_terminal_lease_recovery(orch: Orchestrator) {
    let started = *TERMINAL_OWNER_RECOVERY_STARTED.get_or_init(Instant::now);
    tokio::spawn(async move {
        let deadline = started + terminal_owner_recovery_duration();
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        let advertised: HashSet<(String, String, u64)> = orch
            .fleet
            .snapshot()
            .cells
            .into_iter()
            .filter_map(|cell| {
                let residency = cell.residency.as_ref()?;
                Some((
                    residency.holder.storage_key(),
                    residency.incarnation_id.clone(),
                    cell.cell_epoch,
                ))
            })
            .collect();
        match clear_unadvertised_exited_terminal_lease_bindings(&orch.db.local, &advertised).await {
            Ok(count) if count > 0 => log::info!(
                "Cleared {count} terminal lease binding(s) after owner-recovery grace elapsed"
            ),
            Ok(_) => {}
            Err(error) => log::warn!("Could not clear terminal lease bindings: {error}"),
        }
    });
}

async fn clear_unadvertised_exited_terminal_lease_bindings(
    db: &LocalDb,
    advertised: &HashSet<(String, String, u64)>,
) -> DbResult<u64> {
    let advertised = advertised.clone();
    db.write(|conn| {
        let advertised = advertised.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT residency_holder, residency_incarnation_id, cell_epoch
                     FROM job_terminals
                     WHERE status = 'exited' AND residency_holder IS NOT NULL",
                    (),
                )
                .await?;
            let mut stale = Vec::new();
            while let Some(row) = rows.next().await? {
                let fence = (row.text(0)?, row.text(1)?, row.i64(2)? as u64);
                if !advertised.contains(&fence) {
                    stale.push(fence);
                }
            }
            drop(rows);
            let mut updated = 0;
            for (residency_holder, incarnation_id, cell_epoch) in stale {
                updated += conn
                    .execute(
                        "UPDATE job_terminals
                         SET residency_holder = NULL, residency_incarnation_id = NULL, cell_epoch = NULL,
                             process_generation = NULL
                         WHERE status = 'exited' AND residency_holder = ?1
                           AND residency_incarnation_id = ?2 AND cell_epoch = ?3",
                        params![residency_holder, incarnation_id, cell_epoch as i64],
                    )
                    .await?;
            }
            Ok(updated)
        })
    })
    .await
}

/// What fencing a terminal into recovery found.
///
/// A terminal's death can be delivered more than once — the fleet synthesizes a
/// process event from every snapshot that still carries the exited entry, and
/// the executor reports the exit itself — so "did this call move the row" and
/// "is this terminal recovery's" are different questions. Answering them
/// together is what would let a second delivery of one death conclude that
/// recovery had declined the terminal, and finalize it out from under the
/// restart already pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalRecovery {
    /// This call moved a live terminal into recovery. It is recovery's now.
    Fenced,
    /// The terminal was already in recovery: another delivery of the same death
    /// got here first, and the restart it set up is still coming.
    AlreadyRecovering,
    /// No terminal of this session is in a state recovery can take — it settled
    /// by some other path — so the caller finalizes as it would any exit.
    NotRecoverable,
}

/// Fence a terminal whose executor went away into recovery, which is what makes
/// [`reconcile_terminal_lifecycle`] start it again once an executor advertises
/// the environment holding it.
pub(crate) async fn mark_terminal_recovering(
    db: Arc<LocalDb>,
    session_id: String,
) -> DbResult<TerminalRecovery> {
    db.write(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            if conn
                .execute(
                    "UPDATE job_terminals SET status = 'recovering'
                     WHERE session_id = ?1 AND status = 'running'",
                    params![session_id.as_str()],
                )
                .await?
                > 0
            {
                return Ok(TerminalRecovery::Fenced);
            }
            // Nothing moved, which means either recovery already holds this
            // terminal or there is nothing here to recover. Only the first must
            // stop the caller from finalizing, so the two are read apart rather
            // than collapsed into one "no".
            let mut rows = conn
                .query(
                    "SELECT status FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![session_id.as_str()],
                )
                .await?;
            let status = rows.next().await?.map(|row| row.text(0)).transpose()?;
            Ok(match status.as_deref() {
                Some("recovering") => TerminalRecovery::AlreadyRecovering,
                _ => TerminalRecovery::NotRecoverable,
            })
        })
    })
    .await
}

/// Resolve a lease incarnation that the executor fleet permanently reports as
/// missing. The fence predicates prevent an old refresh response from clearing a
/// replacement incarnation acquired concurrently.
pub(crate) async fn resolve_missing_terminal_lease(
    db: &LocalDb,
    residency_holder: &str,
    incarnation_id: &str,
    cell_epoch: u64,
) -> DbResult<bool> {
    let residency_holder = residency_holder.to_string();
    let incarnation_id = incarnation_id.to_string();
    db.write(|conn| {
        let residency_holder = residency_holder.clone();
        let incarnation_id = incarnation_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let updated = conn
                .execute(
                    "UPDATE job_terminals
                     SET status = 'exited', exited_at = COALESCE(exited_at, ?4),
                         residency_holder = NULL, residency_incarnation_id = NULL, cell_epoch = NULL,
                         process_generation = NULL
                     WHERE status = 'recovering' AND residency_holder = ?1
                       AND residency_incarnation_id = ?2 AND cell_epoch = ?3",
                    params![
                        residency_holder.as_str(),
                        incarnation_id.as_str(),
                        cell_epoch as i64,
                        now
                    ],
                )
                .await?;
            Ok(updated > 0)
        })
    })
    .await
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTerminal {
    id: String,
    job_id: Option<String>,
    project_id: Option<String>,
    run_id: Option<String>,
    session_id: String,
    command: String,
    title: Option<String>,
    description: Option<String>,
    status: String,
    exit_code: Option<i32>,
    created_at: i64,
    exited_at: Option<i64>,
    pub slug: Option<String>,
    residency_holder: Option<String>,
    residency_incarnation_id: Option<String>,
    cell_epoch: Option<u64>,
    process_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveTerminal {
    id: String,
    session_id: String,
    command: String,
    job_id: Option<String>,
    issue_id: Option<String>,
    issue_number: Option<i32>,
    project_id: String,
    slug: Option<String>,
    node_name: Option<String>,
    exec_seq: Option<i32>,
    project_key: String,
    created_at: i64,
    /// The thread this terminal's job belongs to, when its owner is a thread
    /// rather than an issue node. A thread has no issue, so `issue_number` is
    /// then `None` and there is no issue coordinate to route or label by; this
    /// names where the terminal lives instead. It takes precedence over the
    /// other coordinates rather than being a fallback for their absence: a
    /// thread's *task* runs in a synthetic delegation execution and so still
    /// reports an `exec_seq`.
    thread_name: Option<String>,
}

fn terminal_from_row(row: &cairn_db::turso::Row) -> DbResult<JobTerminal> {
    Ok(JobTerminal {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        run_id: row.opt_text(3)?,
        session_id: row.text(4)?,
        command: row.text(5)?,
        title: row.opt_text(6)?,
        description: row.opt_text(7)?,
        status: row.text(8)?,
        exit_code: row.opt_i64(9)?.map(|value| value as i32),
        created_at: row.i64(10)?,
        exited_at: row.opt_i64(11)?,
        slug: row.opt_text(12)?,
        residency_holder: row.opt_text(13)?,
        residency_incarnation_id: row.opt_text(14)?,
        cell_epoch: row.opt_i64(15)?.map(|value| value as u64),
        process_generation: row.opt_i64(16)?.map(|value| value as u64),
    })
}

const TERMINAL_SELECT: &str = "
    SELECT id, job_id, project_id, run_id, session_id, command, title,
           description, status, exit_code, created_at, exited_at, slug,
           residency_holder, residency_incarnation_id, cell_epoch, process_generation
    FROM job_terminals
";

pub async fn create_pty(
    orch: &Orchestrator,
    cwd: String,
    cols: u16,
    rows: u16,
    shell: Option<String>,
) -> Result<String, String> {
    let slug = format!("scratch-{}", Uuid::new_v4());
    let resource =
        crate::mcp::handlers::terminal::terminal_resource_for_cwd(&orch.db.local, &cwd, slug)
            .await?;
    crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        None,
        "Terminal".to_string(),
        shell,
        cairn_common::executor_protocol::ResidentPtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await
}

/// Deliver what an operator typed or pasted into the terminal pane.
///
/// This is command delivery into the same job residence an agent's append targets,
/// so it takes the same alignment-aware path: pressing Enter after a logical write
/// must execute against the new content, not against whatever the shell's checkout
/// held when it spawned. The cost lands per delivered line rather than per
/// keystroke, because the delimiter predicate gates it.
///
/// `submit: false` — the pane sends exactly what the user produced, including the
/// `\r` an Enter key emits, and appending a newline here would submit a line the
/// user had not finished.
pub async fn write_pty(
    orch: &Orchestrator,
    session_id: String,
    data: String,
) -> Result<(), String> {
    crate::mcp::handlers::terminal::deliver_terminal_input(orch, &session_id, &data, false)
        .await
        .map(|_| ())
}

pub async fn resize_pty(
    orch: &Orchestrator,
    session_id: String,
    cols: u16,
    rows: u16,
) -> Result<(), String> {
    crate::mcp::handlers::terminal::resize_terminal_by_session(orch, &session_id, cols, rows).await
}

pub async fn close_pty(orch: &Orchestrator, session_id: String) -> Result<(), String> {
    crate::mcp::handlers::terminal::stop_terminal_by_session(orch, &session_id).await
}

pub fn check_pty_session_exists(orch: &Orchestrator, session_id: String) -> Result<bool, String> {
    Ok(orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .contains_key(&session_id))
}

pub fn get_pty_buffer(orch: &Orchestrator, session_id: String) -> Result<String, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let session = sessions.get(&session_id).ok_or("Session not found")?;
    let session = session.lock().map_err(|e| e.to_string())?;
    match &session.output_buffer {
        Some(buffer) => {
            let bytes: Vec<u8> = buffer
                .lock()
                .map_err(|e| e.to_string())?
                .iter()
                .copied()
                .collect();
            Ok(String::from_utf8_lossy(&bytes).to_string())
        }
        None => Ok(String::new()),
    }
}

pub fn get_pty_command_states(
    orch: &Orchestrator,
    session_ids: Vec<String>,
) -> Result<HashMap<String, PtyCommandSnapshot>, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let mut states = HashMap::new();
    for session_id in session_ids {
        let snapshot = sessions
            .get(&session_id)
            .and_then(|session| session.lock().ok())
            .and_then(|session| {
                session
                    .command_state
                    .as_ref()
                    .and_then(|state| state.lock().ok())
                    .map(|state| PtyCommandSnapshot {
                        busy: state.busy,
                        exit_code: state.last_exit_code,
                        duration_ms: state.last_duration_ms,
                    })
            })
            .unwrap_or(PtyCommandSnapshot {
                busy: false,
                exit_code: None,
                duration_ms: None,
            });
        states.insert(session_id, snapshot);
    }
    Ok(states)
}

async fn delete_terminal(db: Arc<LocalDb>, terminal_id: String) -> Result<(), String> {
    db.write(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM job_terminals WHERE id = ?1",
                params![terminal_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_terminal_by_session(
    db: Arc<LocalDb>,
    session_id: String,
) -> Result<JobTerminal, String> {
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{TERMINAL_SELECT} WHERE session_id = ?1 LIMIT 1"),
                    params![session_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| terminal_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("Terminal not found: {session_id}")))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_job_terminals(db: Arc<LocalDb>, job_id: String) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{TERMINAL_SELECT} WHERE job_id = ?1 ORDER BY created_at ASC"),
                    params![job_id.as_str()],
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(terminal_from_row(&row)?);
            }
            Ok(terminals)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn slug_exists(
    conn: &cairn_db::turso::Connection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = if let Some(job_id) = job_id {
        conn.query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            params![job_id, slug],
        )
        .await?
    } else if let Some(project_id) = project_id {
        conn.query("SELECT COUNT(*) FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2", params![project_id, slug]).await?
    } else {
        return Ok(false);
    };
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing slug count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn generate_slug_from_title(
    db: Arc<LocalDb>,
    job_id: Option<String>,
    project_id: Option<String>,
    title: String,
) -> Result<String, String> {
    let base_slug = slugify(&title);
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let base_slug = base_slug.clone();
        Box::pin(async move {
            let mut counter = 1;
            let mut candidate = base_slug.clone();
            loop {
                if !slug_exists(conn, job_id.as_deref(), project_id.as_deref(), &candidate).await? {
                    return Ok(candidate);
                }
                counter += 1;
                candidate = format!("{base_slug}-{counter}");
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn create_job_terminal(
    orch: &Orchestrator,
    job_id: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        Some(job_id.clone()),
        None,
        title.clone(),
    )
    .await?;
    let resource =
        crate::mcp::handlers::terminal::terminal_resource_for_job(&orch.db.local, &job_id, slug)
            .await?;
    let session_id = crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        initial_command,
        title,
        None,
        cairn_common::executor_protocol::ResidentPtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await?;
    load_terminal_by_session(orch.db.local.clone(), session_id).await
}

pub async fn get_job_terminals(db: &DbState, job_id: String) -> Result<Vec<JobTerminal>, String> {
    Ok(load_job_terminals(db.local.clone(), job_id)
        .await?
        .into_iter()
        .filter(|terminal| terminal.status == "running")
        .collect())
}

async fn terminal_close_session(db: Arc<LocalDb>, terminal_id: String) -> Result<String, String> {
    db.read(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id FROM job_terminals WHERE id = ?1 LIMIT 1",
                    params![terminal_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!("Terminal not found: {terminal_id}")));
            };
            row.text(0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn close_job_terminal(orch: &Orchestrator, terminal_id: String) -> Result<(), String> {
    let session_id = match terminal_close_session(orch.db.local.clone(), terminal_id.clone()).await
    {
        Ok(session_id) => session_id,
        Err(error) if error.contains("Terminal not found") => return Ok(()),
        Err(error) => return Err(error),
    };
    let transitioned = orch
        .db
        .local
        .write(|conn| {
            let terminal_id = terminal_id.clone();
            Box::pin(async move {
                Ok(conn
                    .execute(
                        "UPDATE job_terminals SET status = 'closing',
                             exited_at = COALESCE(exited_at, ?2)
                         WHERE id = ?1 AND status IN ('running', 'recovering')",
                        (terminal_id.as_str(), chrono::Utc::now().timestamp()),
                    )
                    .await?)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if transitioned > 0 {
        emit_terminal_change(orch, "update");
    }
    if crate::mcp::handlers::terminal::release_terminal_by_session(orch, &session_id)
        .await
        .is_ok()
    {
        delete_terminal(orch.db.local.clone(), terminal_id).await?;
        emit_terminal_change(orch, "delete");
    }
    Ok(())
}

pub async fn get_running_terminals(db: &DbState) -> Result<Vec<ActiveTerminal>, String> {
    db.local.read(|conn| Box::pin(async move {
        let mut all = Vec::new();
        // Owner-aware in the same shape as `home_uri_for_job_conn`: a thread's
        // job has no issue and no execution, so joining them inwards silently
        // dropped every thread-owned terminal from this listing. That listing is
        // what draws the facet strip, so a thread terminal ran with no chip to
        // reach it by (CAIRN-3841).
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, jt.job_id, j.issue_id, i.number, COALESCE(i.project_id, j.project_id), jt.slug, j.node_name, e.seq, p.key, jt.created_at, t.name FROM job_terminals jt JOIN jobs j ON j.id = jt.job_id LEFT JOIN issues i ON i.id = j.issue_id LEFT JOIN jobs parent ON parent.id = j.parent_job_id LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, parent.thread_id) JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id) LEFT JOIN executions e ON e.id = j.execution_id WHERE jt.status = 'running' AND jt.job_id IS NOT NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(11)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: row.opt_text(3)?, issue_id: row.opt_text(4)?, issue_number: row.opt_i64(5)?.map(|v| v as i32), project_id: row.text(6)?, slug: row.opt_text(7)?, node_name: row.opt_text(8)?, exec_seq: row.opt_i64(9)?.map(|v| v as i32), project_key: row.text(10)?, created_at: row.i64(11)?, thread_name: row.opt_text(12)? }));
        }
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, p.id, p.key, jt.slug, jt.created_at FROM job_terminals jt JOIN projects p ON p.id = jt.project_id WHERE jt.status = 'running' AND jt.project_id IS NOT NULL AND jt.job_id IS NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(6)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: None, issue_id: None, issue_number: None, project_id: row.text(3)?, slug: row.opt_text(5)?, node_name: None, exec_seq: None, project_key: row.text(4)?, created_at: row.i64(6)?, thread_name: None }));
        }
        all.sort_by_key(|(created_at, _)| std::cmp::Reverse(*created_at));
        Ok(all.into_iter().map(|(_, terminal)| terminal).collect())
    })).await.map_err(|error| error.to_string())
}

pub async fn create_project_terminal(
    orch: &Orchestrator,
    project_id: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        None,
        Some(project_id.clone()),
        title.clone(),
    )
    .await?;
    let resource = crate::mcp::handlers::terminal::terminal_resource_for_project(
        &orch.db.local,
        &project_id,
        slug,
    )
    .await?;
    let session_id = crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        initial_command,
        title,
        None,
        cairn_common::executor_protocol::ResidentPtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await?;
    load_terminal_by_session(orch.db.local.clone(), session_id).await
}

pub async fn open_project_terminal(
    orch: &Orchestrator,
    project_id: String,
) -> Result<JobTerminal, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    for terminal in
        load_running_project_terminals(orch.db.local.clone(), project_id.clone()).await?
    {
        if sessions.contains(&terminal.session_id) {
            return Ok(terminal);
        }
    }
    create_project_terminal(orch, project_id, "Terminal".to_string(), None).await
}

async fn load_running_project_terminals(
    db: Arc<LocalDb>,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(&format!("{TERMINAL_SELECT} WHERE project_id = ?1 AND status = 'running' ORDER BY created_at DESC"), params![project_id.as_str()]).await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? { terminals.push(terminal_from_row(&row)?); }
            Ok(terminals)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn get_project_terminals(
    orch: &Orchestrator,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    Ok(
        load_running_project_terminals(orch.db.local.clone(), project_id)
            .await?
            .into_iter()
            .filter(|terminal| sessions.contains(&terminal.session_id))
            .collect(),
    )
}
