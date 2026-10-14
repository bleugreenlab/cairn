//! Durable terminal recovery and lease-incarnation fences.
use cairn_db::{
    storage::{DbResult, LocalDb, RowExt},
    turso::params,
};
use std::{collections::HashSet, sync::Arc};
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
pub enum TerminalRecovery {
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
/// the host lifecycle reconciliation start it again once an executor advertises
/// the environment holding it.
pub async fn mark_terminal_recovering(
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
pub async fn resolve_missing_terminal_lease(
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

pub async fn clear_unadvertised_exited_terminal_lease_bindings(
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
