//! Terminal resource and PTY-session machinery for MCP handlers.
//!
//! Long-lived terminal resources are managed separately from synchronous `run`
//! batches, but share the same process/sandbox primitives where behavior overlaps.

use super::run::MAX_BUFFER_SIZE;
use crate::services::{ensure_submitted_line, get_default_shell, PtySession};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{
    CellOccupant, CellOwnerRef, CellPriority, LifetimeLeaseAcquireRequest,
    LifetimeLeaseDeclaration, LifetimeLeaseFence, LifetimeLeaseOperation, LifetimeLeaseOwner,
    LifetimeLeaseOwnerKind, LifetimeLeaseResult, LifetimeOwnerDeathPolicy, LifetimeProcessEvent,
    LifetimeProcessEventKind, LifetimeProcessIoMode, LifetimeProcessSpec, LifetimeProcessStatus,
    LifetimePtySize, LifetimeSandboxPolicy, ProcessSandboxMode, RepositoryLocator,
    ResourceReservation, ResourceReservationSource,
};
use cairn_common::ids;
use cairn_common::uri::CairnResource;
use cairn_db::turso::params;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::time::SystemTime;
use uuid::Uuid;

use crate::orchestrator::Orchestrator;

/// PTY data event payload (mirrors Tauri-side PtyDataPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyDataPayload {
    pub session_id: String,
    pub data: String,
}

/// Undo every runner- and executor-side artifact created while starting a
/// terminal. Creation owns the lease until it returns success, so no failure
/// path may leave retained admission or a half-bound terminal row behind.
async fn rollback_terminal_creation(
    orch: &Orchestrator,
    session_id: &str,
    fence: &LifetimeLeaseFence,
) {
    if let Ok(mut handlers) = orch.pty_state.lifetime_handlers.lock() {
        handlers.remove(&(fence.lease_id.clone(), session_id.to_string()));
    }
    remove_pty_session(&orch.pty_state, session_id);

    let session_id_owned = session_id.to_string();
    if let Err(error) = orch
        .db
        .local
        .write(|conn| {
            let session_id = session_id_owned.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM job_terminals WHERE session_id = ?1",
                    (session_id.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
    {
        tracing::warn!(%session_id, %error, "failed to remove terminal row during creation rollback");
    }

    let binding = crate::services::LeaseTerminalBinding {
        fence: fence.clone(),
        process_key: session_id.to_string(),
        process_generation: 0,
    };
    let release_error = release_terminal_process(orch, &binding).await.err();
    let lease_id = fence.lease_id.clone();
    let has_siblings = orch
        .db
        .local
        .read(|conn| {
            let lease_id = lease_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM job_terminals WHERE lease_id = ?1 AND status = 'running' LIMIT 1",
                        (lease_id.as_str(),),
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await
        .unwrap_or(true);
    if !has_siblings {
        // No connected cell means there cannot be a visible sibling process to
        // preserve. Release is fenced and therefore harmless if the lease already
        // disappeared or was replaced concurrently.
        let release = orch
            .fleet
            .operate_lifetime_lease(
                orch,
                LifetimeLeaseOperation::Release {
                    fence: fence.clone(),
                },
            )
            .await;
        tracing::debug!(lease_id = %fence.lease_id, error = ?release_error, result = ?release, "terminal creation rollback released an owner lease with no persisted siblings");
    }
}

pub async fn restart_terminal_lease(
    orch: &Orchestrator,
    fence: LifetimeLeaseFence,
) -> Result<String, String> {
    let lease_id = fence.lease_id.clone();
    let restartable_processes: std::collections::HashSet<String> = orch
        .fleet
        .snapshot()
        .cells
        .into_iter()
        .filter_map(|cell| {
            cell.occupant
                .and_then(|occupant| occupant.lifetime().cloned())
        })
        .filter(|lease| lease.declaration.lease_id == lease_id)
        .flat_map(|lease| lease.processes.into_iter())
        .filter_map(|(key, process)| match process.status {
            LifetimeProcessStatus::Starting | LifetimeProcessStatus::Running { .. } => Some(key),
            LifetimeProcessStatus::Exited {
                restartable: true,
                executor_lost: true,
                ..
            } => Some(key),
            _ => None,
        })
        .collect();
    let slugs = terminal_slugs_for_lease(&orch.db.local, &fence, &restartable_processes).await?;
    if slugs.is_empty() {
        return Err(format!("No terminal row is bound to lease {lease_id}"));
    }
    let mut sessions = Vec::with_capacity(slugs.len());
    for slug in slugs {
        sessions.push(restart_terminal_process(orch, fence.clone(), &slug).await?);
    }
    Ok(sessions.join(","))
}

async fn terminal_slugs_for_lease(
    db: &LocalDb,
    fence: &LifetimeLeaseFence,
    restartable_processes: &std::collections::HashSet<String>,
) -> Result<Vec<String>, String> {
    let fence = fence.clone();
    let restartable_processes = restartable_processes.clone();
    db.read(|conn| {
        let fence = fence.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT slug, status, session_id FROM job_terminals
                 WHERE lease_id = ?1 AND lease_incarnation_id = ?2 AND lease_epoch = ?3
                 ORDER BY created_at, slug",
                    (
                        fence.lease_id.as_str(),
                        fence.incarnation_id.as_str(),
                        fence.lease_epoch as i64,
                    ),
                )
                .await?;
            let mut slugs = Vec::new();
            while let Some(row) = rows.next().await? {
                let status = row.text(1)?;
                let session_id = row.text(2)?;
                if status != "closing"
                    && (status == "running"
                        || status == "recovering"
                        || restartable_processes.contains(&session_id))
                {
                    slugs.push(row.text(0)?);
                }
            }
            Ok(slugs)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn restart_terminal_process(
    orch: &Orchestrator,
    fence: LifetimeLeaseFence,
    slug: &str,
) -> Result<String, String> {
    let lease_id = fence.lease_id.clone();
    let incarnation_id = fence.incarnation_id.clone();
    let lease_epoch = fence.lease_epoch;
    let slug = slug.to_string();
    let row = orch
        .db
        .local
        .read(|conn| {
            let lease_id = lease_id.clone();
            let slug = slug.clone();
            let incarnation_id = incarnation_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id, project_id, slug, command, title, lease_id
                 FROM job_terminals
                 WHERE slug = ?2 AND ((lease_id = ?1 AND lease_incarnation_id = ?3 AND lease_epoch = ?4)
                    OR (status = 'exited'
                        AND ('terminal:' || COALESCE(job_id, project_id)) = ?1))
                 LIMIT 1",
                        (lease_id.as_str(), slug.as_str(), incarnation_id.as_str(), lease_epoch as i64),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!(
                        "No terminal row is bound to lease {lease_id}"
                    ))
                })?;
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.opt_text(4)?,
                    row.opt_text(5)?,
                ))
            })
        })
        .await
        .map_err(|error| error.to_string())?;

    let resource = if let Some(job_id) = row.0.as_deref() {
        terminal_resource_for_job(&orch.db.local, job_id, row.2.clone()).await?
    } else {
        terminal_resource_for_project(
            &orch.db.local,
            row.1
                .as_deref()
                .ok_or("project terminal row has no project")?,
            row.2.clone(),
        )
        .await?
    };
    let mut target = resolve_terminal_resource_target(&orch.db.local, &resource)
        .await
        .map_err(|error| error.to_string())?;
    ensure_terminal_slug_available(&orch.db.local, &target)
        .await
        .map_err(|error| error.to_string())?;
    let cell = orch.fleet.snapshot().cells.into_iter().find(|cell| {
        cell.lease_epoch == fence.lease_epoch
            && cell
                .occupant
                .as_ref()
                .and_then(CellOccupant::lifetime)
                .is_some_and(|lease| {
                    lease.declaration.lease_id == fence.lease_id
                        && lease.incarnation_id == fence.incarnation_id
                        && lease.declaration.owner == fence.owner
                })
    });
    let Some(cell) = cell else {
        if !crate::terminal_host::terminal_owner_recovery_window_elapsed() {
            return Err(
                "terminal lease fence is stale while its executor may still reclaim it".into(),
            );
        }
        if row.5.is_some() {
            crate::terminal_host::resolve_missing_terminal_lease(
                &orch.db.local,
                &fence.lease_id,
                &fence.incarnation_id,
                fence.lease_epoch,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        let target = materialize_terminal_lease(orch, target).await?;
        let interactive = row.4.is_some();
        return spawn_terminal_session(
            orch,
            resource,
            target,
            if interactive { String::new() } else { row.3 },
            None,
            !interactive,
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await;
    };
    let lease = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .ok_or("terminal lease has no lifetime state")?;
    if lease.phase == cairn_common::executor_protocol::LifetimeLeasePhase::AwaitingReclaim {
        let reclaimed = orch
            .fleet
            .operate_lifetime_lease(
                orch,
                LifetimeLeaseOperation::Reclaim {
                    fence: fence.clone(),
                },
            )
            .await;
        if !matches!(reclaimed, LifetimeLeaseResult::State { .. }) {
            return Err(format!("failed to reclaim terminal lease: {reclaimed:?}"));
        }
    }
    target.cwd = cell.path;
    // Reconnecting to (or reclaiming) an existing lease can find a checkout the
    // bookmark advanced past while the executor was disconnected. Refresh to the
    // current tip before spawning so the terminal never reports running against
    // stale source. Project/user terminals (no branch) run in the live checkout and
    // skip the gate.
    if let Some(tip) = resolve_job_terminal_tip(orch, &target).await? {
        ensure_terminal_checkout_current(orch, &fence, &tip).await?;
    }
    target.lease = Some(fence);
    let interactive = row.4.is_some();
    spawn_terminal_session(
        orch,
        resource,
        target,
        if interactive { String::new() } else { row.3 },
        None,
        !interactive,
        None,
        LifetimePtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await
}

pub(crate) async fn release_terminal_by_session(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let binding = terminal_binding_by_session(orch, session_id).await?;
    release_terminal_process(orch, &binding).await?;
    if let Ok(mut sessions) = orch.pty_state.sessions.lock() {
        sessions.remove(session_id);
    }
    if let Ok(mut handlers) = orch.pty_state.lifetime_handlers.lock() {
        handlers.remove(&(binding.fence.lease_id.clone(), binding.process_key.clone()));
    }
    Ok(())
}

async fn release_terminal_process(
    orch: &Orchestrator,
    binding: &crate::services::LeaseTerminalBinding,
) -> Result<(), String> {
    let stop = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::StopProcess {
                fence: binding.fence.clone(),
                process_key: binding.process_key.clone(),
            },
        )
        .await;
    let cell = match stop {
        LifetimeLeaseResult::State { cell } => cell,
        other => orch
            .fleet
            .snapshot()
            .cells
            .into_iter()
            .find(|cell| {
                cell.lease_epoch == binding.fence.lease_epoch
                    && cell
                        .occupant
                        .as_ref()
                        .and_then(CellOccupant::lifetime)
                        .is_some_and(|lease| {
                            lease.declaration.lease_id == binding.fence.lease_id
                                && lease.incarnation_id == binding.fence.incarnation_id
                        })
            })
            .ok_or_else(|| format!("Failed to stop terminal before release: {other:?}"))?,
    };
    let release_group = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .is_some_and(|lease| {
            lease.processes.values().all(|process| {
                !matches!(
                    process.status,
                    LifetimeProcessStatus::Starting | LifetimeProcessStatus::Running { .. }
                )
            })
        });
    if release_group {
        let release = orch
            .fleet
            .operate_lifetime_lease(
                orch,
                LifetimeLeaseOperation::Release {
                    fence: binding.fence.clone(),
                },
            )
            .await;
        if !matches!(release, LifetimeLeaseResult::Released { .. }) {
            return Err(format!("Failed to release terminal lease: {release:?}"));
        }
    }
    Ok(())
}

pub(crate) async fn terminal_resource_for_cwd(
    db: &LocalDb,
    cwd: &str,
    slug: String,
) -> Result<CairnResource, String> {
    let cwd = cwd.to_string();
    let match_row = db
        .read(|conn| {
            let cwd = cwd.clone();
            Box::pin(async move {
                let mut jobs = conn
                    .query(
                        "SELECT id FROM jobs
                         WHERE worktree_path IS NOT NULL
                           AND (?1 = worktree_path OR ?1 LIKE worktree_path || '/%')
                         ORDER BY length(worktree_path) DESC LIMIT 1",
                        (cwd.as_str(),),
                    )
                    .await?;
                if let Some(row) = jobs.next().await? {
                    return Ok((Some(row.text(0)?), None));
                }
                let mut projects = conn
                    .query(
                        "SELECT id FROM projects
                         WHERE ?1 = repo_path OR ?1 LIKE repo_path || '/%'
                         ORDER BY length(repo_path) DESC LIMIT 1",
                        (cwd.as_str(),),
                    )
                    .await?;
                Ok((
                    None,
                    projects.next().await?.map(|row| row.text(0)).transpose()?,
                ))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if let Some(job_id) = match_row.0 {
        terminal_resource_for_job(db, &job_id, slug).await
    } else if let Some(project_id) = match_row.1 {
        terminal_resource_for_project(db, &project_id, slug).await
    } else {
        Err(format!(
            "Terminal cwd is not inside a managed project or agent worktree: {cwd}"
        ))
    }
}

pub(crate) async fn resize_terminal_by_session(
    orch: &Orchestrator,
    session_id: &str,
    cols: u16,
    rows: u16,
) -> Result<(), String> {
    let binding = terminal_binding_by_session(orch, session_id).await?;
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::ResizePty {
                fence: binding.fence,
                process_key: binding.process_key,
                process_generation: binding.process_generation,
                size: LifetimePtySize {
                    rows,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                },
            },
        )
        .await;
    if matches!(result, LifetimeLeaseResult::State { .. }) {
        Ok(())
    } else {
        Err(format!("Failed to resize terminal: {result:?}"))
    }
}

fn terminal_binding_from_cells(
    cells: &[cairn_common::executor_protocol::PersistentCellState],
    lease_id: &str,
    incarnation_id: &str,
    lease_epoch: u64,
    slug: Option<&str>,
    process_generation: Option<u64>,
) -> Option<crate::services::LeaseTerminalBinding> {
    cells.iter().find_map(|cell| {
        if cell.lease_epoch != lease_epoch {
            return None;
        }
        let lease = cell.occupant.as_ref().and_then(CellOccupant::lifetime)?;
        if lease.declaration.lease_id != lease_id || lease.incarnation_id != incarnation_id {
            return None;
        }
        let process_key = slug
            .filter(|slug| lease.processes.contains_key(*slug))
            .map(str::to_owned)
            .or_else(|| {
                let mut matches = lease.processes.iter().filter(|(_, process)| {
                    process_generation.is_none_or(|generation| process.generation == generation)
                });
                let (process_key, _) = matches.next()?;
                matches.next().is_none().then(|| process_key.clone())
            })?;
        let process_generation = lease.processes.get(&process_key)?.generation;
        Some(crate::services::LeaseTerminalBinding {
            fence: LifetimeLeaseFence {
                lease_id: lease.declaration.lease_id.clone(),
                owner: lease.declaration.owner.clone(),
                incarnation_id: lease.incarnation_id.clone(),
                lease_epoch: cell.lease_epoch,
            },
            process_key,
            process_generation,
        })
    })
}

async fn persisted_terminal_binding_by_session(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<crate::services::LeaseTerminalBinding, String> {
    let session_id = session_id.to_string();
    let (slug, lease_id, incarnation_id, lease_epoch, process_generation) = orch
        .db
        .local
        .read(|conn| {
            let session_id = session_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT slug, lease_id, lease_incarnation_id, lease_epoch, process_generation
                         FROM job_terminals
                         WHERE session_id = ?1 AND status = 'running'
                         LIMIT 1",
                        (session_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!(
                        "Terminal session not running: {session_id}"
                    ))
                })?;
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_i64(3)?.map(|value| value as u64),
                    row.opt_i64(4)?.map(|value| value as u64),
                ))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let lease_id = lease_id
        .ok_or_else(|| format!("Terminal session has no retained executor lease: {session_id}"))?;
    let incarnation_id = incarnation_id.ok_or_else(|| {
        format!("Terminal session has no retained lease incarnation: {session_id}")
    })?;
    let lease_epoch = lease_epoch
        .ok_or_else(|| format!("Terminal session has no retained lease epoch: {session_id}"))?;
    terminal_binding_from_cells(
        &orch.fleet.snapshot().cells,
        &lease_id,
        &incarnation_id,
        lease_epoch,
        slug.as_deref(),
        process_generation,
    )
    .ok_or_else(|| format!("Terminal executor lease is not connected: {session_id}"))
}

async fn terminal_binding_by_session(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<crate::services::LeaseTerminalBinding, String> {
    let session = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|error| error.to_string())?
        .get(session_id)
        .cloned();
    if let Some(session) = session {
        return session
            .lock()
            .map_err(|error| error.to_string())?
            .lease
            .clone()
            .ok_or_else(|| "This terminal has no executor process binding".to_string());
    }
    persisted_terminal_binding_by_session(orch, session_id).await
}

pub(crate) async fn stop_terminal_by_session(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let binding = terminal_binding_by_session(orch, session_id).await?;
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::StopProcess {
                fence: binding.fence,
                process_key: binding.process_key,
            },
        )
        .await;
    if matches!(result, LifetimeLeaseResult::State { .. }) {
        Ok(())
    } else {
        Err(format!("Failed to stop terminal: {result:?}"))
    }
}

pub(crate) async fn terminal_resource_for_job(
    db: &LocalDb,
    job_id: &str,
    slug: String,
) -> Result<CairnResource, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number, e.seq, j.uri_segment, parent.uri_segment
                     FROM jobs j
                     JOIN projects p ON p.id = j.project_id
                     JOIN issues i ON i.id = j.issue_id
                     JOIN executions e ON e.id = j.execution_id
                     LEFT JOIN jobs parent ON parent.id = j.parent_job_id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::Row(format!("Job not found: {job_id}")))?;
            let project = row.text(0)?;
            let number = row.i64(1)? as i32;
            let exec_seq = row.i64(2)? as i32;
            let segment = row.text(3)?;
            Ok(match row.opt_text(4)? {
                Some(node_id) => CairnResource::TaskTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name: segment,
                    slug,
                },
                None => CairnResource::NodeTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id: segment,
                    slug,
                },
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn terminal_resource_for_project(
    db: &LocalDb,
    project_id: &str,
    slug: String,
) -> Result<CairnResource, String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT key FROM projects WHERE id = ?1 LIMIT 1",
                    (project_id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row(format!("Project not found: {project_id}"))
            })?;
            Ok(CairnResource::ProjectTerminal {
                project: row.text(0)?,
                slug,
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn create_interactive_terminal_from_resource(
    orch: &Orchestrator,
    resource: CairnResource,
    initial_command: Option<String>,
    title: String,
    shell: Option<String>,
    size: LifetimePtySize,
) -> Result<String, String> {
    let started_at = std::time::Instant::now();
    let target = resolve_terminal_resource_target(&orch.db.local, &resource)
        .await
        .map_err(|error| error.to_string())?;
    let resolution_ms = started_at.elapsed().as_millis();
    ensure_terminal_slug_available(&orch.db.local, &target)
        .await
        .map_err(|error| error.to_string())?;
    let lease_started_at = std::time::Instant::now();
    let target = materialize_terminal_lease(orch, target).await?;
    let lease_ms = lease_started_at.elapsed().as_millis();
    let spawn_started_at = std::time::Instant::now();
    let session_id = spawn_terminal_session(
        orch,
        resource,
        target,
        initial_command.unwrap_or_default(),
        None,
        false,
        shell,
        size,
    )
    .await?;
    let spawn_ms = spawn_started_at.elapsed().as_millis();
    let session = session_id.clone();
    orch.db
        .local
        .write(|conn| {
            let session = session.clone();
            let title = title.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE job_terminals SET title = ?1 WHERE session_id = ?2",
                    (title.as_str(), session.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let total_ms = started_at.elapsed().as_millis();
    if total_ms >= 2_000 {
        tracing::warn!(
            total_ms,
            resolution_ms,
            lease_ms,
            spawn_ms,
            "interactive terminal creation was slow"
        );
    }
    Ok(session_id)
}

async fn materialize_terminal_lease(
    orch: &Orchestrator,
    mut target: TerminalResourceTarget,
) -> Result<TerminalResourceTarget, String> {
    let (lease_id, owner, request) = terminal_lease_acquisition(orch, &target).await?;
    // The freshly resolved bookmark tip lives in the declaration before the fleet's
    // idempotent route resolution can overwrite initial_base_commit with a reused
    // lease's pinned commit. Capture it now so the residency gate below refreshes to
    // the true current head, not the pinned identity commit.
    let resolved_tip = request.declaration.initial_base_commit.clone();
    // Job terminals run in a managed cell (a resolved job branch); project/user
    // terminals run in the externally owned live checkout (ExistingCheckout), which
    // is at HEAD by definition and needs no residency gate.
    let job_terminal = target.branch.is_some();
    let result = orch
        .fleet
        .operate_lifetime_lease(orch, LifetimeLeaseOperation::Acquire { request })
        .await;
    let LifetimeLeaseResult::State { cell } = result else {
        return Err(format!("failed to acquire terminal lease: {result:?}"));
    };
    let fence = {
        let lease = cell
            .occupant
            .as_ref()
            .and_then(CellOccupant::lifetime)
            .ok_or_else(|| {
                "terminal lease acquisition returned no lifetime occupant".to_string()
            })?;
        LifetimeLeaseFence {
            lease_id,
            owner,
            incarnation_id: lease.incarnation_id.clone(),
            lease_epoch: cell.lease_epoch,
        }
    };
    let cwd = cell.path;
    if job_terminal {
        // Force the managed checkout to the current bookmark tip before the shell
        // spawns. The executor judges currency against the actual checkout HEAD and
        // is the residency authority, so send unconditionally and let it self-heal a
        // lease whose recorded base lies. Fail closed: release the freshly acquired
        // lease so no stale terminal is left behind.
        if let Err(error) = ensure_terminal_checkout_current(orch, &fence, &resolved_tip).await {
            let _ = orch
                .fleet
                .operate_lifetime_lease(
                    orch,
                    LifetimeLeaseOperation::Release {
                        fence: fence.clone(),
                    },
                )
                .await;
            return Err(error);
        }
    }
    target.cwd = cwd;
    target.lease = Some(fence);
    Ok(target)
}

/// Force a job terminal's managed checkout to `tip` before its shell spawns. The
/// fleet pins a reused lease route to its originally declared commit for lease
/// identity, and the executor never re-materializes an idle cell beyond the acquire
/// binding, so without this gate a reconnected or reacquired terminal can run
/// against stale source. The executor is the residency authority (it judges the
/// request against the actual checkout HEAD); any failure propagates so terminal
/// creation aborts before the shell spawns and before a running row is inserted.
async fn ensure_terminal_checkout_current(
    orch: &Orchestrator,
    fence: &LifetimeLeaseFence,
    tip: &str,
) -> Result<(), String> {
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::RefreshCheckout {
                fence: fence.clone(),
                base_commit: tip.to_string(),
            },
        )
        .await;
    match result {
        LifetimeLeaseResult::State { .. } => Ok(()),
        other => Err(format!(
            "failed to refresh terminal checkout to {tip}: {other:?}"
        )),
    }
}

/// Resolve the current committed tip of a job terminal's managed branch. Returns
/// `None` for project/user terminals (no branch), which run in the live checkout
/// and skip the residency gate.
async fn resolve_job_terminal_tip(
    orch: &Orchestrator,
    target: &TerminalResourceTarget,
) -> Result<Option<String>, String> {
    let Some(branch) = target.branch.clone() else {
        return Ok(None);
    };
    let repo_path = target.repo_path.clone();
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let tip = tokio::task::spawn_blocking(move || {
        let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
        let store = crate::jj::project_store_dir(&config_dir, std::path::Path::new(&repo_path));
        crate::jj::bookmark_commit(&jj, &store, &branch).ok_or_else(|| {
            format!("agent terminal branch `{branch}` does not resolve to a committed head")
        })
    })
    .await
    .map_err(|error| format!("terminal branch resolution task failed: {error}"))??;
    Ok(Some(tip))
}

async fn terminal_lease_acquisition(
    orch: &Orchestrator,
    target: &TerminalResourceTarget,
) -> Result<(String, LifetimeLeaseOwner, LifetimeLeaseAcquireRequest), String> {
    let owner_id = target
        .job_id
        .clone()
        .unwrap_or_else(|| target.project_id.clone());
    let branch = target.branch.clone();
    let repo_path = target.repo_path.clone();
    let project_id = target.project_id.clone();
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let job_terminal = target.job_id.is_some();
    let (repository, base_commit, purpose) = tokio::task::spawn_blocking(move || {
        if let Some(branch) = branch {
            let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
            let store = crate::jj::project_store_dir(&config_dir, std::path::Path::new(&repo_path));
            let base_commit =
                crate::jj::bookmark_commit(&jj, &store, &branch).ok_or_else(|| {
                    format!("agent terminal branch `{branch}` does not resolve to a committed head")
                })?;
            Ok((
                RepositoryLocator::ColocatedPath {
                    project_id: project_id.clone(),
                    repository_id: project_id.clone(),
                    absolute_path: repo_path.clone(),
                },
                base_commit,
                "agent terminals".to_string(),
            ))
        } else {
            let output = std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&repo_path)
                .output()
                .map_err(|error| format!("resolve project checkout head: {error}"))?;
            if !output.status.success() {
                return Err(format!(
                    "resolve project checkout head: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
            Ok((
                RepositoryLocator::ExistingCheckout {
                    project_id: project_id.clone(),
                    repository_id: project_id,
                    absolute_path: repo_path,
                },
                String::from_utf8_lossy(&output.stdout).trim().to_string(),
                if job_terminal {
                    "agent terminals".to_string()
                } else {
                    "project terminals".to_string()
                },
            ))
        }
    })
    .await
    .map_err(|error| format!("terminal VCS resolution task failed: {error}"))??;
    let disk_growth_bytes = if matches!(repository, RepositoryLocator::ExistingCheckout { .. }) {
        0
    } else {
        1024 * 1024 * 1024
    };
    let owner = LifetimeLeaseOwner {
        kind: LifetimeLeaseOwnerKind::Terminal,
        owner_id: owner_id.clone(),
    };
    let lease_id = format!("terminal:{owner_id}");
    let request = LifetimeLeaseAcquireRequest {
        declaration: LifetimeLeaseDeclaration {
            lease_id: lease_id.clone(),
            owner: owner.clone(),
            owner_ref: target.owner_ref.clone(),
            name: "terminals".to_string(),
            purpose,
            repository,
            initial_base_commit: base_commit,
            resource_reservation: ResourceReservation {
                memory_bytes: 64 * 1024 * 1024,
                disk_growth_bytes,
                // The executor rejects lifetime leases declaring zero concurrency
                // units (InvalidDeclaration), so a terminal must reserve one.
                concurrency_units: 1,
                source: ResourceReservationSource::Declared,
            },
            owner_death_policy: LifetimeOwnerDeathPolicy {
                heartbeat_timeout_ms: crate::terminal_host::TERMINAL_HEARTBEAT_TIMEOUT_MS,
                reclaim_grace_ms: crate::terminal_host::TERMINAL_RECLAIM_GRACE_MS,
            },
        },
        priority: CellPriority::AgentInteractive,
        deadline_unix_ms: SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            + 30_000,
    };
    Ok((lease_id, owner, request))
}

/// PTY exit event payload (mirrors Tauri-side PtyExitPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyExitPayload {
    pub session_id: String,
    pub exit_code: Option<i32>,
}

/// Event payload when agent spawns a background terminal.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTerminalCreatedPayload {
    pub run_id: String,
    pub session_id: String,
    pub command: String,
    pub description: Option<String>,
}

#[derive(Clone)]
struct TerminalResourceTarget {
    slug: String,
    job_id: Option<String>,
    project_id: String,
    run_id: Option<String>,
    cwd: String,
    branch: Option<String>,
    repo_path: String,
    owner_ref: Option<CellOwnerRef>,
    lease: Option<LifetimeLeaseFence>,
}

async fn resolve_terminal_resource_target(
    db: &LocalDb,
    resource: &CairnResource,
) -> DbResult<TerminalResourceTarget> {
    let resource = resource.clone();
    db.read(|conn| {
        Box::pin(async move {
            match resource {
                CairnResource::NodeTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    slug,
                } => {
                    let job_id = find_terminal_target_job_id(conn, &project, number, exec_seq, &node_id)
                        .await?
                        .ok_or_else(|| {
                            crate::storage::DbError::Row(format!(
                                "No node job found for terminal target {project}-{number}/{exec_seq}/{node_id}"
                            ))
                        })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id,
                                   j.branch, p.repo_path
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id.clone()),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                        branch: row.opt_text(3)?,
                        repo_path: row.text(4)?,
                        owner_ref: Some(CellOwnerRef {
                            project_id: row.text(0)?, project_key: Some(project.clone()),
                            issue_number: Some(number), job_id: Some(job_id.clone()),
                            execution_seq: Some(exec_seq), node_kind: Some(node_id),
                        }),
                        lease: None,
                    })
                }
                CairnResource::TaskTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name,
                    slug,
                } => {
                    let job_id = find_task_terminal_target_job_id(
                        conn,
                        &project,
                        number,
                        exec_seq,
                        &node_id,
                        &task_name,
                    )
                    .await?
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!(
                            "No task job found for terminal target {project}-{number}/{exec_seq}/{node_id}/task/{task_name}"
                        ))
                    })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id,
                                   j.branch, p.repo_path
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id.clone()),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                        branch: row.opt_text(3)?,
                        repo_path: row.text(4)?,
                        owner_ref: Some(CellOwnerRef {
                            project_id: row.text(0)?,
                            project_key: Some(project.clone()),
                            issue_number: Some(number),
                            job_id: Some(job_id.clone()),
                            execution_seq: Some(exec_seq),
                            node_kind: Some(task_name),
                        }),
                        lease: None,
                    })
                }
                CairnResource::ProjectTerminal { project, slug } => {
                    let lookup_key = project.to_uppercase();
                    let mut rows = conn
                        .query(
                            "SELECT id, repo_path FROM projects WHERE key = ?1 LIMIT 1",
                            (lookup_key.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No project found for key {project}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: None,
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: None,
                        branch: None,
                        repo_path: row.text(1)?,
                        owner_ref: None,
                        lease: None,
                    })
                }
                _ => Err(crate::storage::DbError::Row(
                    "Resource is not a terminal URI".to_string(),
                )),
            }
        })
    })
    .await
}

async fn ensure_terminal_slug_available(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            // Only a *running* terminal blocks the slug. Exited rows linger (for
            // post-exit reads and already-exited wake subscribes) and are
            // reclaimed on insert, so they must not block re-creating the slug.
            let exists = match job_id.as_deref() {
                Some(job_id) => terminal_slug_running_for_job(conn, job_id, &slug).await?,
                None => terminal_slug_running_for_project(conn, &project_id, &slug).await?,
            };
            if exists {
                return Err(crate::storage::DbError::Row(format!(
                    "A running terminal already exists in this scope: {slug}"
                )));
            }
            Ok(())
        })
    })
    .await
}

async fn insert_terminal_resource(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    session_id: &str,
    command: &str,
    description: Option<&str>,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let run_id = target.run_id.clone();
    let slug = target.slug.clone();
    let session_id = session_id.to_string();
    let command = command.to_string();
    let description = description.map(ToOwned::to_owned);
    let lease = target.lease.clone();

    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let run_id = run_id.clone();
        let slug = slug.clone();
        let session_id = session_id.clone();
        let command = command.clone();
        let description = description.clone();
        let lease = lease.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let id = Uuid::new_v4().to_string();
            // Reclaim a lingering exited row for this (scope, slug). The
            // create-availability check only blocks running slugs, so an exited
            // row could still collide with the unique (scope, slug) index.
            match job_id.as_deref() {
                Some(job_id) => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
                        (job_id, slug.as_str()),
                    )
                    .await?;
                }
                None => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2",
                        (project_id.as_str(), slug.as_str()),
                    )
                    .await?;
                }
            }
            conn.execute(
                "
                INSERT INTO job_terminals (
                    id, job_id, project_id, run_id, session_id, command, title,
                    description, status, exit_code, created_at, exited_at, slug,
                    lease_id, lease_incarnation_id, lease_epoch
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'running', NULL, ?8, NULL, ?9, ?10, ?11, ?12)
                ",
                params![
                    id.as_str(),
                    job_id.as_deref(),
                    if job_id.is_some() {
                        None
                    } else {
                        Some(project_id.as_str())
                    },
                    run_id.as_deref(),
                    session_id.as_str(),
                    command.as_str(),
                    description.as_deref(),
                    now,
                    slug.as_str(),
                    lease.as_ref().map(|fence| fence.lease_id.as_str()),
                    lease.as_ref().map(|fence| fence.incarnation_id.as_str()),
                    lease.as_ref().map(|fence| fence.lease_epoch as i64),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

async fn find_terminal_target_job_id(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut issue_rows = conn
        .query(
            "
            SELECT i.id
            FROM issues i
            JOIN projects p ON i.project_id = p.id
            WHERE p.key = ?1 AND i.number = ?2
            LIMIT 1
            ",
            (lookup_key.as_str(), issue_number),
        )
        .await?;

    let Some(issue_row) = issue_rows.next().await? else {
        return Ok(None);
    };
    let issue_id = issue_row.text(0)?;

    let mut exec_rows = conn
        .query(
            "
            SELECT id
            FROM executions
            WHERE issue_id = ?1 AND seq = ?2
            LIMIT 1
            ",
            (issue_id.as_str(), exec_seq),
        )
        .await?;

    let Some(exec_row) = exec_rows.next().await? else {
        return Ok(None);
    };
    let exec_id = exec_row.text(0)?;

    let mut exact_rows = conn
        .query(
            "
            SELECT id
            FROM jobs
            WHERE issue_id = ?1
              AND execution_id = ?2
              AND parent_job_id IS NULL
              AND uri_segment = ?3
            LIMIT 1
            ",
            (issue_id.as_str(), exec_id.as_str(), node_id),
        )
        .await?;

    if let Some(row) = exact_rows.next().await? {
        return Ok(Some(row.text(0)?));
    }

    Ok(None)
}

async fn find_task_terminal_target_job_id(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    parent_node_id: &str,
    task_name: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT child.id
            FROM jobs parent
            JOIN jobs child ON child.parent_job_id = parent.id
            JOIN issues i ON parent.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON parent.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND parent.parent_job_id IS NULL
              AND parent.uri_segment = ?4
              AND child.uri_segment = ?5
            LIMIT 1
            ",
            (
                lookup_key.as_str(),
                issue_number,
                exec_seq,
                parent_node_id,
                task_name,
            ),
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

async fn terminal_slug_exists_for_job(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_exists_for_project(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(*)
            FROM job_terminals
            WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
            ",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// Whether a *running* terminal with this slug exists in the job scope. Used by
/// create-availability so a lingering exited row does not block re-use.
async fn terminal_slug_running_for_job(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE job_id = ?1 AND slug = ?2 AND status = 'running'",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_running_for_project(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2 AND status = 'running'",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// A job-owned terminal row, loaded for terminal-exit wake subscribe validation
/// and the already-exited immediate-fire path.
pub(crate) struct TerminalWakeRow {
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i64,
    pub exited_at: Option<i64>,
    pub output_tail: Option<String>,
    /// The live PTY session id, used to reach the session's in-memory output
    /// buffer and phrase-watcher registry when subscribing an output wake.
    pub session_id: Option<String>,
}

/// Look up a terminal by job scope + slug for wake subscription.
pub(crate) async fn lookup_terminal_for_wake(
    db: &LocalDb,
    job_id: &str,
    slug: &str,
) -> DbResult<Option<TerminalWakeRow>> {
    let job_id = job_id.to_string();
    let slug = slug.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code, created_at, exited_at, output_tail, session_id
                     FROM job_terminals WHERE job_id = ?1 AND slug = ?2 LIMIT 1",
                    params![job_id.as_str(), slug.as_str()],
                )
                .await?;
            terminal_wake_row_from_rows(&mut rows).await
        })
    })
    .await
}

async fn lookup_terminal_for_wake_by_session_id(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<TerminalWakeRow>> {
    let session_id = session_id.to_string();
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code, created_at, exited_at, output_tail, session_id
                     FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![session_id.as_str()],
                )
                .await?;
            terminal_wake_row_from_rows(&mut rows).await
        })
    })
    .await
}

async fn terminal_wake_row_from_rows(
    rows: &mut cairn_db::turso::Rows,
) -> DbResult<Option<TerminalWakeRow>> {
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(TerminalWakeRow {
        status: row.text(0)?,
        exit_code: row.opt_i64(1)?.map(|code| code as i32),
        created_at: row.i64(2)?,
        exited_at: row.opt_i64(3)?,
        output_tail: row.opt_text(4)?,
        session_id: row.opt_text(5)?,
    }))
}

/// List the terminal slugs owned by a job, for a precise unknown-slug error.
pub(crate) async fn list_job_terminal_slugs(db: &LocalDb, job_id: &str) -> DbResult<Vec<String>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT slug FROM job_terminals
                     WHERE job_id = ?1 AND slug IS NOT NULL ORDER BY slug",
                    params![job_id.as_str()],
                )
                .await?;
            let mut slugs = Vec::new();
            while let Some(row) = rows.next().await? {
                slugs.push(row.text(0)?);
            }
            Ok(slugs)
        })
    })
    .await
}

pub(crate) async fn subscribe_terminal_exit_wake_once(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    uri: &str,
    row: Option<&TerminalWakeRow>,
    session_id: Option<&str>,
    created_by: &str,
) -> Result<TerminalWakeSubscriptionOutcome, String> {
    // Store the canonical terminal URI as the source ref, not the bare slug:
    // slugs are unique only per job, so a slug ref would cross-match other jobs'
    // same-slug terminals. The route side keys on the same canonical URI.
    let fact_kinds = vec![crate::orchestrator::wakes::FACT_KIND_TERMINAL_EXIT.to_string()];
    crate::orchestrator::wakes::subscribe_one_shot(
        &orch.db.local,
        job_id,
        "process",
        Some(uri),
        Some(&fact_kinds),
        created_by,
    )
    .await?;

    // Re-read status AFTER persisting the subscription to close the spawn/persist
    // race: a process that exits before this persist routes its exit wake with no
    // subscriber (dropped), and the pre-persist `row` still reads `running`. Since
    // finalize commits `status='exited'` before routing the exit wake, a fresh
    // read here observes that exit and re-routes it, so the agent is never
    // stranded when the process dies before the subscription exists.
    //
    // Propagate a read failure rather than swallowing it: when the exit already
    // fired and was dropped before the subscription persisted, this read is the
    // only recovery path, so treating an error as "still running" would report
    // an active wake that no future exit event will ever satisfy. Surfacing the
    // error lets the caller tell the agent to subscribe manually instead.
    let fresh = match session_id {
        Some(session_id) => lookup_terminal_for_wake_by_session_id(&orch.db.local, session_id)
            .await
            .map_err(|error| error.to_string())?,
        None => None,
    };
    let exited = fresh
        .as_ref()
        .filter(|row| row.status == "exited")
        .or_else(|| row.filter(|row| row.status == "exited"));
    if let Some(exit_row) = exited {
        let runtime_secs = exit_row
            .exited_at
            .map(|exited| (exited - exit_row.created_at).max(0));
        crate::orchestrator::wakes::route_terminal_exit_async(
            orch,
            slug,
            uri,
            exit_row.exit_code,
            runtime_secs,
            exit_row.output_tail.as_deref(),
        )
        .await?;
        Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued)
    } else {
        Ok(TerminalWakeSubscriptionOutcome::ExitSubscribed)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn subscribe_terminal_output_wake_once(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    uri: &str,
    phrase: &str,
    row: Option<&TerminalWakeRow>,
    session_id: Option<&str>,
    created_by: &str,
) -> Result<TerminalWakeSubscriptionOutcome, String> {
    // Persist first: the subscription is a durable, terminal-scoped property,
    // not bound to whichever PTY session is live right now. A (re)starting
    // session hydrates it, so it survives the worktree-fence approval respawn.
    let sub = crate::orchestrator::wakes::subscribe_terminal_output_one_shot(
        &orch.db.local,
        job_id,
        uri,
        phrase,
        created_by,
    )
    .await?;

    // Re-read the terminal's status AFTER persisting the subscription to close a
    // spawn/persist race: a process that exits between the PTY start and this
    // persist routes its exit wake before any subscriber exists (dropped), and
    // the pre-persist `row` snapshot still reads `running`. Because finalize
    // commits `status='exited'` before routing the exit wake, a fresh read here
    // is guaranteed to observe that exit, so we re-route it and the agent waiting
    // on a phrase is never stranded when the build dies before the phrase prints.
    //
    // Propagate a read failure rather than swallowing it: this read is the sole
    // recovery path when the exit fired and was dropped before the subscription
    // persisted, so treating an error as "still running" would falsely report an
    // active output wake that no future exit event will satisfy.
    let fresh = match session_id {
        Some(session_id) => lookup_terminal_for_wake_by_session_id(&orch.db.local, session_id)
            .await
            .map_err(|error| error.to_string())?,
        None => None,
    };
    let exited = fresh
        .as_ref()
        .filter(|row| row.status == "exited")
        .or_else(|| row.filter(|row| row.status == "exited"));
    if let Some(exit_row) = exited {
        let runtime_secs = exit_row
            .exited_at
            .map(|exited| (exited - exit_row.created_at).max(0));
        crate::orchestrator::wakes::route_terminal_exit_async(
            orch,
            slug,
            uri,
            exit_row.exit_code,
            runtime_secs,
            exit_row.output_tail.as_deref(),
        )
        .await?;
        return Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued);
    }

    let live_session_id = session_id.or_else(|| row.and_then(|row| row.session_id.as_deref()));
    if let Some(session_id) = live_session_id {
        match register_terminal_output_watcher(orch, session_id, &sub.id, job_id, phrase, uri) {
            OutputWatchRegistration::AlreadyPresent { excerpt } => {
                crate::orchestrator::wakes::route_terminal_output_async(
                    orch,
                    job_id,
                    slug,
                    uri,
                    phrase,
                    excerpt.as_deref(),
                )
                .await?;
                return Ok(TerminalWakeSubscriptionOutcome::OutputAlreadyQueued);
            }
            OutputWatchRegistration::Registered => {
                return Ok(TerminalWakeSubscriptionOutcome::OutputRegistered);
            }
            OutputWatchRegistration::NotLive => {}
        }
    }

    Ok(TerminalWakeSubscriptionOutcome::OutputPersisted)
}

/// Whether a terminal with this slug exists in any state (running or exited).
/// The promote `run-N` auto-allocation scan uses this so exited run slugs stay
/// taken and run numbers remain monotonic.
async fn terminal_slug_taken_any(db: &LocalDb, target: &TerminalResourceTarget) -> DbResult<bool> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            match job_id.as_deref() {
                Some(job_id) => terminal_slug_exists_for_job(conn, job_id, &slug).await,
                None => terminal_slug_exists_for_project(conn, &project_id, &slug).await,
            }
        })
    })
    .await
}

fn block_on_background_db<T, F>(fut: F) -> DbResult<T>
where
    T: Send,
    F: std::future::Future<Output = DbResult<T>> + Send,
{
    let run = move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(crate::storage::DbError::from)?
            .block_on(fut)
    };
    if tokio::runtime::Handle::try_current().is_ok() {
        // Lifetime events are delivered from the runner's Tokio transport.
        // A nested runtime would panic there, so cross a real thread boundary
        // while preserving the finalizer's synchronous exactly-once contract.
        std::thread::scope(|scope| {
            scope
                .spawn(run)
                .join()
                .map_err(|_| crate::storage::DbError::Row("terminal finalizer panicked".into()))?
        })
    } else {
        run()
    }
}

/// Context loaded while marking a terminal exited: the row's `created_at` (for
/// runtime) and, for a nonzero exit on a job-owned terminal, the injection
/// target (the owning job's current session + the exit code).
struct TerminalExitContext {
    created_at: Option<i64>,
    injection: Option<(String, i32)>,
}

/// Mark a terminal row exited and retain its output tail (always), and load the
/// context the exit path needs. Supersedes the old delete-on-exit: the row now
/// lingers as `status='exited'` so post-exit reads and already-exited wake
/// subscribes can still see it. Mark-exited is decoupled from the nonzero-only
/// injection-target lookup so the two concerns are independent.
///
/// The UPDATE is gated on `status='running'`: it is the single idempotency guard
/// for finalize. Returns `Ok(None)` when no running row matched (already exited
/// or gone), so every caller — the reader EOF, the promoted watcher, and the
/// external kill entry point — converges here without ever finalizing twice with
/// different codes.
async fn mark_terminal_exited_and_load_context(
    db: Arc<LocalDb>,
    terminal_session_id: String,
    job_id: Option<String>,
    exit_code: Option<i32>,
    output_tail: Option<String>,
) -> DbResult<Option<TerminalExitContext>> {
    db.write(|conn| {
        let terminal_session_id = terminal_session_id.clone();
        let job_id = job_id.clone();
        let output_tail = output_tail.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let transitioned = conn
                .execute(
                    "UPDATE job_terminals
                 SET status = 'exited', exit_code = ?2, exited_at = ?3, output_tail = ?4
                 WHERE session_id = ?1 AND status = 'running'",
                    params![
                        terminal_session_id.as_str(),
                        exit_code.map(|code| code as i64),
                        now,
                        output_tail.as_deref()
                    ],
                )
                .await?;
            if transitioned == 0 {
                return Ok(None);
            }

            let mut rows = conn
                .query(
                    "SELECT created_at FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![terminal_session_id.as_str()],
                )
                .await?;
            let created_at = rows.next().await?.map(|row| row.i64(0)).transpose()?;
            drop(rows);

            // Nonzero-exit injection target: the owning job's current session.
            let injection = match (job_id, exit_code.filter(|code| *code != 0)) {
                (Some(job_id), Some(code)) => {
                    let mut rows = conn
                        .query(
                            "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                            params![job_id.as_str()],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.opt_text(0))
                        .transpose()?
                        .flatten()
                        .map(|session_id| (session_id, code))
                }
                _ => None,
            };

            Ok(Some(TerminalExitContext {
                created_at,
                injection,
            }))
        })
    })
    .await
}

/// Capture the last ~2000 chars of a terminal's buffer for the retained
/// `output_tail` and the wake message.
fn capture_output_tail(buffer: &Arc<Mutex<VecDeque<u8>>>) -> Option<String> {
    let bytes: Vec<u8> = buffer
        .lock()
        .map(|b| b.iter().copied().collect())
        .unwrap_or_default();
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&bytes);
    let chars: Vec<char> = text.chars().collect();
    let start = chars.len().saturating_sub(2000);
    let tail: String = chars[start..].iter().collect();
    if tail.trim().is_empty() {
        None
    } else {
        Some(tail)
    }
}

/// Outcome of registering an output-phrase watcher on a live terminal session.
pub(crate) enum OutputWatchRegistration {
    /// The phrase is already present in the terminal's current buffer; the
    /// caller should fire the wake immediately rather than register a watcher.
    AlreadyPresent { excerpt: Option<String> },
    /// A live watcher was registered; it fires when the phrase next appears.
    Registered,
    /// No live agent PTY session backs this terminal, so its output cannot be
    /// watched (e.g. a promoted-run or already-finalized session).
    NotLive,
}

/// Register a phrase watcher on a live terminal session, after first scanning
/// the session's current output buffer so an already-printed phrase fires
/// immediately instead of waiting for new output.
pub(crate) fn register_terminal_output_watcher(
    orch: &Orchestrator,
    session_id: &str,
    subscription_id: &str,
    job_id: &str,
    phrase: &str,
    terminal_uri: &str,
) -> OutputWatchRegistration {
    let session_arc = {
        let Ok(sessions) = orch.pty_state.sessions.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        match sessions.get(session_id) {
            Some(arc) => arc.clone(),
            None => return OutputWatchRegistration::NotLive,
        }
    };
    let (watchers, buffer) = {
        let Ok(session) = session_arc.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        let Some(watchers) = session.output_watchers.clone() else {
            return OutputWatchRegistration::NotLive;
        };
        (watchers, session.output_buffer.clone())
    };
    if let Some(buffer) = buffer {
        let bytes: Vec<u8> = buffer
            .lock()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        let scan = crate::services::scan_for_phrase("", &text, phrase);
        if scan.matched_excerpt.is_some() {
            return OutputWatchRegistration::AlreadyPresent {
                excerpt: scan.matched_excerpt,
            };
        }
    }
    // Bind to a local so the lock-guard temporary drops here, before the
    // function's `watchers`/`buffer` locals (a tail-position match would keep
    // the guard's borrow alive past their drop — E0597).
    let outcome = match watchers.lock() {
        Ok(mut guard) => {
            // Replace any existing watcher for this subscription (a re-subscribe
            // may change the phrase) so the session never carries a stale one,
            // and a subscribe that lands on a session already hydrated for this
            // same row does not double-register it.
            guard.retain(|w| w.subscription_id != subscription_id);
            guard.push(crate::services::TerminalOutputWatcher {
                subscription_id: subscription_id.to_string(),
                job_id: job_id.to_string(),
                phrase: phrase.to_string(),
                carry: String::new(),
                terminal_uri: terminal_uri.to_string(),
            });
            OutputWatchRegistration::Registered
        }
        Err(_) => OutputWatchRegistration::NotLive,
    };
    outcome
}

async fn lookup_terminal_session_id_for_target(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<Option<String>> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = if let Some(job_id) = job_id {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE job_id = ?1 AND slug = ?2
                    LIMIT 1
                    ",
                    (job_id.as_str(), slug.as_str()),
                )
                .await?
            } else {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
                    LIMIT 1
                    ",
                    (project_id.as_str(), slug.as_str()),
                )
                .await?
            };
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
}

pub(crate) async fn activate_promoted_executor_terminal(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    fence: LifetimeLeaseFence,
    process_key: String,
    command: &str,
    output: Vec<u8>,
    process_generation: u64,
) -> Result<cairn_common::executor_protocol::PromotedTerminalProcess, String> {
    let mut target = TerminalResourceTarget {
        slug: String::new(),
        job_id: Some(ctx.job_id.clone()),
        project_id: ctx.project_id.clone(),
        run_id: Some(ctx.run_id.clone()),
        cwd: String::new(),
        branch: None,
        repo_path: String::new(),
        owner_ref: None,
        lease: Some(fence.clone()),
    };
    for n in 1..=10_000 {
        target.slug = format!("run-{n}");
        if !terminal_slug_taken_any(&orch.db.local, &target)
            .await
            .unwrap_or(true)
        {
            break;
        }
    }
    if target.slug.is_empty() {
        return Err("could not allocate a promoted terminal slug".into());
    }
    let uri = build_promoted_terminal_uri(orch, ctx, &target.slug).await;
    let session_id = ids::mint_session_id().into_string();
    let event_process_key = process_key.clone();
    let mut terminal_log = crate::scratch::TerminalLog::open(&ctx.job_id, &target.slug);
    if let Some(log) = terminal_log.as_mut() {
        // The executor snapshots every byte observed before adoption and sends
        // later bytes as lifetime events. Persist the snapshot before installing
        // the event handler so post-exit reads preserve the same exact boundary
        // as the live ring buffer without duplicating either side.
        log.append(&output);
    }
    let output_buffer = Arc::new(Mutex::new(VecDeque::from(output)));
    let last_output_at = Arc::new(Mutex::new(SystemTime::now()));
    let session = Arc::new(Mutex::new(PtySession {
        master: None,
        writer: None,
        lease: Some(crate::services::LeaseTerminalBinding {
            fence: fence.clone(),
            process_key,
            process_generation,
        }),
        child: Box::new(crate::services::RemoteTerminalChild),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at: Some(last_output_at.clone()),
        command_state: None,
        output_watchers: None,
    }));
    orch.pty_state
        .sessions
        .lock()
        .map_err(|error| error.to_string())?
        .insert(session_id.clone(), session.clone());
    if let Err(error) =
        insert_terminal_resource(&orch.db.local, &target, &session_id, command, None).await
    {
        orch.pty_state
            .sessions
            .lock()
            .ok()
            .map(|mut sessions| sessions.remove(&session_id));
        return Err(format!("Database error: {error}"));
    }
    if !orch
        .pty_state
        .lifetime_subscription_installed
        .swap(true, Ordering::SeqCst)
    {
        let state = orch.pty_state.clone();
        orch.fleet.subscribe_lifetime_process_events(move |event| {
            let handler = state.lifetime_handlers.lock().ok().and_then(|handlers| {
                handlers
                    .get(&(event.lease_id.clone(), event.process_key.clone()))
                    .cloned()
            });
            if let Some(handler) = handler {
                handler(event);
            }
        });
    }
    let terminal_log = Arc::new(Mutex::new(terminal_log));
    let event_orch = orch.clone();
    let event_sid = session_id.clone();
    let event_fence = fence.clone();
    let event_buffer = output_buffer.clone();
    let event_last = last_output_at.clone();
    let event_command = command.to_string();
    let event_job = ctx.job_id.clone();
    let event_ref = target.slug.clone();
    let event_uri = uri.clone();
    let handler_process_key = event_process_key.clone();
    let handler = Arc::new(move |event: LifetimeProcessEvent| {
        if event.incarnation_id != event_fence.incarnation_id
            || event.lease_epoch != event_fence.lease_epoch
            || event.process_key != event_process_key
            || event.process_generation != process_generation
        {
            return;
        }
        match event.event {
            LifetimeProcessEventKind::Output { data, .. } => {
                let text = String::from_utf8_lossy(&data).into_owned();
                emit_terminal_data(
                    &*event_orch.services.emitter,
                    &event_sid,
                    &text,
                    &event_buffer,
                    &terminal_log,
                );
                if let Ok(mut last) = event_last.lock() {
                    *last = SystemTime::now();
                }
            }
            LifetimeProcessEventKind::State {
                status: LifetimeProcessStatus::Exited { exit_code, .. },
            } => {
                finalize_terminal_session(
                    &event_orch,
                    &event_sid,
                    exit_code,
                    &event_buffer,
                    &event_command,
                    Some(event_job.clone()),
                    &event_ref,
                    &event_uri,
                );
                if let Ok(mut handlers) = event_orch.pty_state.lifetime_handlers.lock() {
                    handlers.remove(&(event_fence.lease_id.clone(), event_process_key.clone()));
                }
            }
            _ => {}
        }
    });
    orch.pty_state
        .lifetime_handlers
        .lock()
        .map_err(|error| error.to_string())?
        .insert((fence.lease_id.clone(), handler_process_key), handler);

    let heartbeat_orch = orch.clone();
    let heartbeat_session = session_id.clone();
    let heartbeat_fence = fence.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            let still_bound = heartbeat_orch
                .pty_state
                .sessions
                .lock()
                .ok()
                .and_then(|sessions| sessions.get(&heartbeat_session).cloned())
                .is_some();
            if !still_bound {
                break;
            }
            let renewed = heartbeat_orch
                .fleet
                .operate_lifetime_lease(
                    &heartbeat_orch,
                    LifetimeLeaseOperation::Renew {
                        fence: heartbeat_fence.clone(),
                    },
                )
                .await;
            if !matches!(renewed, LifetimeLeaseResult::State { .. }) {
                break;
            }
        }
    });
    let wake_subscribed = subscribe_promoted_terminal_exit_wake(orch, &ctx.job_id, &uri).await;
    let _ = orch.services.emitter.emit(
        "agent-terminal-created",
        serde_json::to_value(AgentTerminalCreatedPayload {
            run_id: ctx.run_id.clone(),
            session_id,
            command: command.to_string(),
            description: None,
        })
        .unwrap_or_default(),
    );
    Ok(cairn_common::executor_protocol::PromotedTerminalProcess {
        fence,
        slug: target.slug,
        uri,
        wake_subscribed,
    })
}

async fn subscribe_promoted_terminal_exit_wake(
    orch: &Orchestrator,
    job_id: &str,
    terminal_uri: &str,
) -> bool {
    match subscribe_terminal_exit_wake_once(
        orch,
        job_id,
        terminal_uri,
        terminal_uri,
        None,
        None,
        "agent",
    )
    .await
    {
        Ok(_) => true,
        Err(error) => {
            log::warn!(
                "failed to subscribe promoted terminal exit wake for {terminal_uri}: {error}"
            );
            false
        }
    }
}

/// Build the canonical readable/killable URI for a promoted terminal.
async fn build_promoted_terminal_uri(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    slug: &str,
) -> String {
    if let (Some(num), Some(seq)) = (ctx.issue_number, ctx.exec_seq) {
        if let Some(parent_segment) =
            crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            if let Some(task_segment) =
                crate::jobs::queries::task_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
            {
                return cairn_common::uri::build_task_terminal_uri(
                    &ctx.project_key,
                    num,
                    seq,
                    &parent_segment,
                    &task_segment,
                    slug,
                );
            }
        }
        if let Some(segment) =
            crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            return cairn_common::uri::build_node_terminal_uri(
                &ctx.project_key,
                num,
                seq,
                &segment,
                slug,
            );
        }
    }
    cairn_common::uri::build_project_terminal_uri(&ctx.project_key, slug)
}
/// Remove a dead terminal session from the live PTY map without killing (the
/// child has already exited). Idempotent: removing an absent session no-ops.
fn remove_pty_session(pty_state: &crate::services::PtyState, session_id: &str) {
    if let Ok(mut sessions) = pty_state.sessions.lock() {
        sessions.remove(session_id);
    }
}

/// The single terminal end-of-life sink. Attempts the conditional mark-exited
/// first; on a real `running`→`exited` transition it emits `pty-exit` +
/// `db-change`, routes the rich `terminal_exit` wake, runs nonzero-exit
/// injection, and finally drops the dead session from the live map. When no row
/// transitioned (already exited, or gone) it is a pure no-op beyond dropping the
/// session — no second `pty-exit`, wake, or injection. This makes finalize
/// idempotent across every caller (reader EOF, promoted watcher, external kill,
/// fence-deny, reader-error) at the DB boundary.
#[allow(clippy::too_many_arguments)]
fn finalize_terminal_session(
    orch: &Orchestrator,
    session_id: &str,
    exit_code: Option<i32>,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
    job_id: Option<String>,
    process_ref: &str,
    detail_uri: &str,
) {
    let output_tail = capture_output_tail(buffer);

    let context = match block_on_background_db(mark_terminal_exited_and_load_context(
        orch.db.local.clone(),
        session_id.to_string(),
        job_id,
        exit_code,
        output_tail.clone(),
    )) {
        Ok(Some(context)) => context,
        // No running row matched: another path already finalized (or the row is
        // gone). Stay idempotent — drop the dead session and return without a
        // second pty-exit/wake/injection.
        Ok(None) => {
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
        Err(error) => {
            log::warn!("failed to mark terminal exited for {process_ref}: {error}");
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
    };

    let _ = orch.services.emitter.emit(
        "pty-exit",
        serde_json::to_value(PtyExitPayload {
            session_id: session_id.to_string(),
            exit_code,
        })
        .unwrap_or_default(),
    );

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );

    let runtime_secs = context
        .created_at
        .map(|created| (chrono::Utc::now().timestamp() - created).max(0));

    if let Err(error) = crate::orchestrator::wakes::route_terminal_exit(
        orch,
        process_ref,
        detail_uri,
        exit_code,
        runtime_secs,
        output_tail.as_deref(),
    ) {
        log::warn!("failed to route terminal-exit wake for {process_ref}: {error}");
    }

    if let Some((injection_session_id, code)) = context.injection {
        send_terminal_exit_context(
            orch,
            &injection_session_id,
            session_id,
            code,
            buffer,
            command,
        );
    }

    // The process is dead and finalize has captured everything it needs from the
    // session (buffer/exit were passed in). Drop it from the live map.
    remove_pty_session(&orch.pty_state, session_id);
}

/// Exit code recorded for a terminal we intentionally killed: the SIGKILL
/// convention (128 + 9). A killed-mid-run command must never read as success.
/// The fields finalize-by-session needs from a `job_terminals` row.
/// Stop an executor-hosted terminal from a synchronous lifecycle path. The
/// executor's durable `ProcessExited` event remains the sole finalization source.
pub fn finalize_terminal_by_session_id(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let orch = orch.clone();
    let session_id = session_id.to_string();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime.block_on(stop_terminal_by_session(&orch, &session_id))
    })
    .join()
    .map_err(|_| "terminal finalize thread panicked".to_string())?
}

#[derive(Clone, Debug)]
pub(crate) enum TerminalWakeSpec {
    Exit,
    Output(String),
}

impl TerminalWakeSpec {
    pub(crate) fn dry_run_suffix(&self) -> String {
        match self {
            Self::Exit => " (wake on exit)".to_string(),
            Self::Output(phrase) => format!(" (wake on output \"{phrase}\")"),
        }
    }
}

pub(crate) enum TerminalWakeSubscriptionOutcome {
    ExitSubscribed,
    ExitAlreadyQueued,
    OutputRegistered,
    OutputAlreadyQueued,
    OutputPersisted,
}

pub(crate) async fn create_terminal_from_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
    command: &str,
    description: Option<&str>,
    wake: Option<TerminalWakeSpec>,
    caller_run_id: Option<&str>,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let subscriber_job_id =
        match (&wake, target.job_id.as_deref()) {
            (Some(_), Some(job_id)) => Some(job_id.to_string()),
            (Some(_), None) => {
                let run_id = caller_run_id.ok_or_else(|| {
                    "payload.wake requires an agent caller when creating a project terminal"
                        .to_string()
                })?;
                Some(job_id_for_run(&orch.db.local, run_id).await?.ok_or_else(|| {
                format!("payload.wake requires an agent caller; no job found for run {run_id}")
            })?)
            }
            (None, _) => None,
        };
    ensure_terminal_slug_available(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?;
    let target = materialize_terminal_lease(orch, target).await?;
    let one_shot = matches!(wake, Some(TerminalWakeSpec::Exit));
    let session_id = spawn_terminal_session(
        orch,
        resource.clone(),
        target,
        command.to_string(),
        description.map(ToOwned::to_owned),
        one_shot,
        None,
        LifetimePtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await?;

    let slug = resource_slug(resource);
    let uri = resource.to_uri();
    let Some(wake) = wake else {
        return Ok(format!("Started terminal {slug}"));
    };
    let job_id = subscriber_job_id.expect("wake subscriber resolved before spawn");
    let row = lookup_terminal_for_wake_by_session_id(&orch.db.local, &session_id)
        .await
        .map_err(|error| error.to_string());
    let wake_result = match (row.as_ref(), &wake) {
        (Ok(row), TerminalWakeSpec::Exit) => {
            subscribe_terminal_exit_wake_once(
                orch,
                &job_id,
                &slug,
                &uri,
                row.as_ref(),
                Some(&session_id),
                "agent",
            )
            .await
        }
        (Ok(row), TerminalWakeSpec::Output(phrase)) => {
            subscribe_terminal_output_wake_once(
                orch,
                &job_id,
                &slug,
                &uri,
                phrase,
                row.as_ref(),
                Some(&session_id),
                "agent",
            )
            .await
        }
        (Err(error), _) => Err(error.clone()),
    };

    match (&wake, wake_result) {
        (
            TerminalWakeSpec::Exit,
            Ok(
                TerminalWakeSubscriptionOutcome::ExitSubscribed
                | TerminalWakeSubscriptionOutcome::ExitAlreadyQueued,
            ),
        ) => Ok(format!(
            "Started terminal {slug}; subscribed to exit — end your turn to resume when it finishes ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued)) => Ok(format!(
            "Started terminal {slug}; it already exited before \"{phrase}\" appeared, so resume is queued for turn end ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::OutputRegistered | TerminalWakeSubscriptionOutcome::OutputPersisted)) => Ok(format!(
            "Started terminal {slug}; watching output for \"{phrase}\" (also wakes on exit) — end your turn to resume when it appears ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::OutputAlreadyQueued)) => Ok(format!(
            "Started terminal {slug}; output \"{phrase}\" already appeared, so resume is queued for turn end ({uri})"
        )),
        (_, Err(error)) => Ok(format!(
            "Started terminal {slug}; automatic wake could not be registered ({error}). Subscribe via cairn:~/wakes: {{subscribe:{{kind:\"terminal\",ref:\"{uri}\",on:{}}}}}",
            match wake {
                TerminalWakeSpec::Exit => "\"exit\"".to_string(),
                TerminalWakeSpec::Output(phrase) => format!("\"output\",phrase:\"{phrase}\""),
            }
        )),
        _ => Ok(format!(
            "Started terminal {slug}; automatic wake registered ({uri})"
        )),
    }
}

async fn job_id_for_run(db: &LocalDb, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            row.opt_text(0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

fn resource_slug(resource: &CairnResource) -> String {
    match resource {
        CairnResource::NodeTerminal { slug, .. }
        | CairnResource::ProjectTerminal { slug, .. }
        | CairnResource::TaskTerminal { slug, .. } => slug.clone(),
        _ => "terminal".to_string(),
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn spawn_terminal_session(
    orch: &Orchestrator,
    resource: CairnResource,
    target: TerminalResourceTarget,
    command: String,
    description: Option<String>,
    one_shot: bool,
    shell: Option<String>,
    size: LifetimePtySize,
) -> Result<String, String> {
    let fence = target
        .lease
        .clone()
        .ok_or("terminal directory has no lifetime lease")?;
    if !orch
        .pty_state
        .lifetime_subscription_installed
        .swap(true, Ordering::SeqCst)
    {
        let state = orch.pty_state.clone();
        orch.fleet.subscribe_lifetime_process_events(move |event| {
            let handler = state.lifetime_handlers.lock().ok().and_then(|handlers| {
                handlers
                    .get(&(event.lease_id.clone(), event.process_key.clone()))
                    .cloned()
            });
            if let Some(handler) = handler {
                handler(event);
            }
        });
    }

    let session_id = ids::mint_session_id().into_string();
    let output_buffer = Arc::new(Mutex::new(VecDeque::new()));
    let last_output_at = Arc::new(Mutex::new(SystemTime::now()));
    let output_watchers = Arc::new(Mutex::new(Vec::new()));
    let session = Arc::new(Mutex::new(PtySession {
        master: None,
        writer: None,
        lease: Some(crate::services::LeaseTerminalBinding {
            fence: fence.clone(),
            process_key: session_id.clone(),
            process_generation: 0,
        }),
        child: Box::new(crate::services::RemoteTerminalChild),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at: Some(last_output_at.clone()),
        command_state: None,
        output_watchers: Some(output_watchers.clone()),
    }));
    let session_insert = orch
        .pty_state
        .sessions
        .lock()
        .map(|mut sessions| {
            sessions.insert(session_id.clone(), session.clone());
        })
        .map_err(|error| error.to_string());
    if let Err(error) = session_insert {
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err(error);
    }

    // The durable binding must predate handler registration and StartProcess.
    // An executor may emit Exited before StartProcess returns; the finalizer
    // therefore needs a running row to transition before any event can arrive.
    if let Err(error) = insert_terminal_resource(
        &orch.db.local,
        &target,
        &session_id,
        &command,
        description.as_deref(),
    )
    .await
    {
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err(format!("Database error: {error}"));
    }
    let heartbeat_orch = orch.clone();
    let heartbeat_session = session_id.clone();
    let heartbeat_fence = fence.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            let still_bound = heartbeat_orch
                .pty_state
                .sessions
                .lock()
                .ok()
                .and_then(|sessions| sessions.get(&heartbeat_session).cloned())
                .and_then(|session| session.lock().ok().and_then(|guard| guard.lease.clone()))
                .is_some_and(|binding| binding.fence == heartbeat_fence);
            if !still_bound {
                break;
            }
            let renewed = heartbeat_orch
                .fleet
                .operate_lifetime_lease(
                    &heartbeat_orch,
                    LifetimeLeaseOperation::Renew {
                        fence: heartbeat_fence.clone(),
                    },
                )
                .await;
            if !matches!(renewed, LifetimeLeaseResult::State { .. }) {
                tracing::warn!(
                    lease_id = %heartbeat_fence.lease_id,
                    result = ?renewed,
                    "terminal lease heartbeat failed"
                );
                break;
            }
        }
    });

    let detail_uri = resource.to_uri();
    if let Ok(persisted) =
        crate::orchestrator::wakes::list_terminal_output_watchers(&orch.db.local, &detail_uri).await
    {
        if let Ok(mut watchers) = output_watchers.lock() {
            for (subscription_id, job_id, phrase, terminal_uri) in persisted {
                watchers.push(crate::services::TerminalOutputWatcher {
                    subscription_id,
                    job_id,
                    phrase,
                    carry: String::new(),
                    terminal_uri,
                });
            }
        }
    }
    let terminal_log =
        Arc::new(Mutex::new(target.job_id.as_deref().and_then(|job| {
            crate::scratch::TerminalLog::open(job, &target.slug)
        })));
    let event_orch = orch.clone();
    let event_sid = session_id.clone();
    let event_fence = fence.clone();
    let event_session = session.clone();
    let event_buffer = output_buffer.clone();
    let event_last = last_output_at.clone();
    let event_watchers = output_watchers.clone();
    let event_log = terminal_log.clone();
    let event_command = command.clone();
    let event_job = target.job_id.clone();
    let event_process_ref = resource_slug(&resource);
    let event_uri = detail_uri.clone();
    let handler = Arc::new(move |event: LifetimeProcessEvent| {
        if event.incarnation_id != event_fence.incarnation_id
            || event.lease_epoch != event_fence.lease_epoch
        {
            return;
        }
        let matches = event_session
            .lock()
            .ok()
            .and_then(|s| s.lease.as_ref().map(|l| l.process_generation))
            .is_some_and(|generation| generation == 0 || generation == event.process_generation);
        if !matches {
            return;
        }
        match event.event {
            LifetimeProcessEventKind::Output { data, .. } => {
                let text = String::from_utf8_lossy(&data).into_owned();
                emit_terminal_data(
                    &*event_orch.services.emitter,
                    &event_sid,
                    &text,
                    &event_buffer,
                    &event_log,
                );
                if let Ok(mut last) = event_last.lock() {
                    *last = SystemTime::now();
                }
                crate::orchestrator::wakes::scan_and_route_terminal_output(
                    &event_orch,
                    &event_watchers,
                    &text,
                );
            }
            LifetimeProcessEventKind::State {
                status:
                    LifetimeProcessStatus::Exited {
                        exit_code,
                        executor_lost,
                        ..
                    },
            } => {
                if executor_lost {
                    emit_terminal_data(&*event_orch.services.emitter, &event_sid,
                            "\r\nExecutor disconnected; restart reuses the retained terminal lease.\r\n",
                            &event_buffer, &event_log);
                }
                finalize_terminal_session(
                    &event_orch,
                    &event_sid,
                    exit_code,
                    &event_buffer,
                    &event_command,
                    event_job.clone(),
                    &event_process_ref,
                    &event_uri,
                );
                if let Ok(mut handlers) = event_orch.pty_state.lifetime_handlers.lock() {
                    handlers.remove(&(event_fence.lease_id.clone(), event_sid.clone()));
                }
                let release_orch = event_orch.clone();
                let release_binding = crate::services::LeaseTerminalBinding {
                    fence: event_fence.clone(),
                    process_key: event_sid.clone(),
                    process_generation: event.process_generation,
                };
                tokio::spawn(async move {
                    if let Err(error) =
                        release_terminal_process(&release_orch, &release_binding).await
                    {
                        tracing::warn!(process_key = %release_binding.process_key, %error, "failed to release settled terminal process");
                    }
                });
            }
            _ => {}
        }
    });
    let handler_insert = orch
        .pty_state
        .lifetime_handlers
        .lock()
        .map(|mut handlers| {
            handlers.insert((fence.lease_id.clone(), session_id.clone()), handler);
        })
        .map_err(|error| error.to_string());
    if let Err(error) = handler_insert {
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err(error);
    }

    let mut env: Vec<(String, String)> = std::env::vars().collect();
    env.push(("PATH".into(), crate::env::agent_shell_path()));
    env.push(("CAIRN_WORKTREE".into(), target.cwd.clone()));
    env.push((
        "CAIRN_CALLBACK_URL".into(),
        format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port),
    ));
    env.push((
        "UV_CACHE_DIR".into(),
        crate::env::uv_cache_dir().to_string_lossy().into_owned(),
    ));
    if let Ok(secret) = orch.mcp_auth.get_secret_for_mcp() {
        env.push(("CAIRN_MCP_SECRET".into(), secret));
    }
    if let Some(run_id) = target.run_id.as_deref() {
        env.push(("CAIRN_RUN_ID".into(), run_id.to_string()));
    }
    if let Some(branch) = target.branch.as_deref() {
        // A job terminal's managed cell is a detached-HEAD checkout with no .jj
        // markers, so branch-keyed tooling (bun dev:instance, bun run changelog)
        // resolves the branch from this variable. Mirrors detached_route_env on
        // the run path. Project terminals carry no branch and run in the live
        // checkout, where git resolves the branch directly.
        env.push(("CAIRN_WORKTREE_BRANCH".into(), branch.to_string()));
    }
    let resolved_sandbox = super::run::build_run_sandbox_policy(
        orch,
        &target.cwd,
        target.run_id.as_deref(),
        Some(&target.project_id),
        (!command.is_empty()).then_some(command.as_str()),
    )
    .await;
    let sandbox_mode = if resolved_sandbox.is_some() {
        if target.branch.is_some() {
            ProcessSandboxMode::Confined
        } else {
            ProcessSandboxMode::ReadOnlyCheckout
        }
    } else {
        ProcessSandboxMode::Unconfined
    };
    let sandbox_policy = resolved_sandbox.map(|(policy, _)| LifetimeSandboxPolicy {
        worktree: policy.worktree.to_string_lossy().into_owned(),
        writable_extra: policy
            .writable_extra
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        deny_read: policy
            .deny_read
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        writable_regex: policy.writable_regex,
        worktree_writable: policy.worktree_writable,
    });
    let program = shell.unwrap_or_else(get_default_shell);
    let args = if one_shot {
        if cfg!(windows) {
            vec!["-Command".to_string(), command.clone()]
        } else {
            vec!["-c".to_string(), command.clone()]
        }
    } else {
        Vec::new()
    };
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::StartProcess {
                fence: fence.clone(),
                process_key: session_id.clone(),
                process: LifetimeProcessSpec {
                    program,
                    args,
                    cwd: String::new(),
                    cwd_root: cairn_common::executor_protocol::LifetimeProcessCwdRoot::Checkout,
                    env,
                    sandbox_mode,
                    sandbox_policy,
                    runtime_assets: Vec::new(),
                    io: LifetimeProcessIoMode::Pty { size },
                },
            },
        )
        .await;
    let LifetimeLeaseResult::State { cell } = result else {
        let error = format!("failed to start terminal PTY: {result:?}");
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err(error);
    };
    let Some(generation) = cell
        .occupant
        .as_ref()
        .and_then(CellOccupant::lifetime)
        .and_then(|lease| lease.processes.get(&session_id))
        .map(|process| process.generation)
    else {
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err("terminal PTY start returned no process generation".to_string());
    };
    if let Ok(mut guard) = session.lock() {
        if let Some(binding) = guard.lease.as_mut() {
            binding.process_generation = generation;
        }
    }

    if let Err(error) =
        persist_terminal_process_generation(&orch.db.local, &target, generation).await
    {
        rollback_terminal_creation(orch, &session_id, &fence).await;
        return Err(format!("Database error: {error}"));
    }
    if !one_shot && !command.is_empty() {
        let input = ensure_submitted_line(&command).into_owned();
        if let Err(error) = write_terminal_input_by_session(orch, &session_id, &input).await {
            rollback_terminal_creation(orch, &session_id, &fence).await;
            return Err(error);
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );
    if let Some(run_id) = target.run_id.clone() {
        let _ = orch.services.emitter.emit(
            "agent-terminal-created",
            serde_json::to_value(AgentTerminalCreatedPayload {
                run_id,
                session_id: session_id.clone(),
                command,
                description,
            })
            .unwrap_or_default(),
        );
    }
    Ok(session_id)
}

async fn persist_terminal_process_generation(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    generation: u64,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
        let updated = if let Some(job_id) = job_id.as_deref() {
            conn.execute(
                "UPDATE job_terminals SET process_generation = ?1 WHERE job_id = ?2 AND slug = ?3",
                params![generation as i64, job_id, slug.as_str()],
            ).await?
        } else {
            conn.execute(
                "UPDATE job_terminals SET process_generation = ?1 WHERE project_id = ?2 AND job_id IS NULL AND slug = ?3",
                params![generation as i64, project_id.as_str(), slug.as_str()],
            ).await?
        };
        if updated != 1 {
            return Err(crate::storage::DbError::Row(format!(
                "terminal generation binding disappeared for slug {slug}"
            )));
        }
        Ok(())
        })
    }).await
}

fn emit_terminal_data(
    emitter: &dyn crate::services::EventEmitter,
    session_id: &str,
    data: &str,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    log: &Arc<Mutex<Option<crate::scratch::TerminalLog>>>,
) {
    let _ = emitter.emit(
        "pty-data",
        serde_json::to_value(PtyDataPayload {
            session_id: session_id.to_string(),
            data: data.to_string(),
        })
        .unwrap_or_default(),
    );
    if let Ok(mut buf_guard) = buffer.lock() {
        buf_guard.extend(data.as_bytes());
        while buf_guard.len() > MAX_BUFFER_SIZE {
            buf_guard.pop_front();
        }
    }
    // Tee the same bytes the ring buffer received to the persisted log, so a
    // post-exit read shows exactly what a live read displayed — including
    // synthetic Cairn messages (fence denials, restart notices), not just raw PTY
    // output. No-op when the terminal has no log (project terminal, or the file
    // could not be opened).
    if let Ok(mut guard) = log.lock() {
        if let Some(terminal_log) = guard.as_mut() {
            terminal_log.append(data.as_bytes());
        }
    }
}

pub(crate) async fn write_terminal_input_by_session(
    orch: &Orchestrator,
    session_id: &str,
    content: &str,
) -> Result<(), String> {
    let session = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| format!("Failed to access sessions: {e}"))?
        .get(session_id)
        .cloned()
        .ok_or_else(|| format!("Terminal session not running: {session_id}"))?;
    let binding = {
        let guard = session
            .lock()
            .map_err(|e| format!("Failed to lock terminal session: {e}"))?;
        guard
            .lease
            .clone()
            .ok_or_else(|| "This terminal has no executor process binding".to_string())?
    };
    let result = orch
        .fleet
        .operate_lifetime_lease(
            orch,
            LifetimeLeaseOperation::WriteProcessInput {
                fence: binding.fence,
                process_key: binding.process_key,
                process_generation: binding.process_generation,
                data: content.as_bytes().to_vec(),
            },
        )
        .await;
    if matches!(result, LifetimeLeaseResult::State { .. }) {
        Ok(())
    } else {
        Err(format!("Failed to write terminal input: {result:?}"))
    }
}

pub async fn append_terminal_input(
    orch: &Orchestrator,
    resource: &CairnResource,
    content: &str,
    submit: bool,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    let to_write = if submit {
        ensure_submitted_line(content)
    } else {
        std::borrow::Cow::Borrowed(content)
    };
    write_terminal_input_by_session(orch, &session_id, &to_write).await?;

    Ok(format!(
        "Sent {} chars to terminal {}",
        to_write.len(),
        target.slug
    ))
}

pub async fn delete_terminal_by_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    // Stop the fenced executor process but retain the lifetime lease and
    // materialization. The executor's ProcessExited event converges on the
    // canonical conditional finalizer and makes the same slug restartable.
    stop_terminal_by_session(orch, &session_id).await?;

    Ok(format!("Stopped terminal {}", target.slug))
}

/// Get exit code from PTY session using non-blocking try_wait.
/// Send terminal context to Claude via stdin when a background process exits with error.
fn send_terminal_exit_context(
    orch: &Orchestrator,
    session_id: &str,
    _terminal_session_id: &str,
    exit_code: i32,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
) {
    // Get recent output from buffer
    let output = {
        let buf_guard = buffer.lock().unwrap();
        let bytes: Vec<u8> = buf_guard.iter().copied().collect();
        String::from_utf8_lossy(&bytes).to_string()
    };

    // Truncate for display
    let cmd_display: String = command.chars().take(100).collect();
    let truncated_output: String = output.chars().take(2000).collect();

    let content = format!(
        "[Terminal Update] Background command '{}' exited with code {}:\n```\n{}\n```",
        cmd_display, exit_code, truncated_output
    );

    // Find the process and send via stdin
    let process_state = orch.process_state.clone();

    let run_id = match process_state.find_process_by_session(session_id) {
        Some(rid) => rid,
        None => {
            log::info!(
                "No active process for session {}, skipping terminal context",
                &session_id[..8.min(session_id.len())]
            );
            return;
        }
    };

    match crate::backends::stdin::send_user_message(
        &process_state,
        &run_id,
        &content,
        session_id,
        None,
        None,
    ) {
        Ok(()) => {
            log::info!(
                "Sent terminal context to session {}: command='{}' exit_code={}",
                &session_id[..8.min(session_id.len())],
                cmd_display,
                exit_code
            );
        }
        Err(e) => {
            log::warn!("Failed to send terminal context: {}", e);
        }
    }
}

#[cfg(test)]
mod terminal_finalize_tests {
    use super::*;
    use crate::db::DbState;
    use crate::fleet::Fleet;
    use crate::models::Fence;
    use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
    use crate::storage::SearchIndex;
    use cairn_common::executor_protocol::{
        CellCheckoutKind, ExecutorMessage, ExecutorSubstrateReport, FleetSnapshot,
        LifetimeLeaseFailureKind, LifetimeLeasePhase, LifetimeLeaseState, LifetimeProcessState,
        PersistentCellLifecycle, PersistentCellState,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    fn git(path: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn init_git_checkout(path: &std::path::Path) -> String {
        git(path, &["init", "-q", "-b", "main"]);
        git(path, &["config", "user.email", "test@cairn.local"]);
        git(path, &["config", "user.name", "Cairn Test"]);
        std::fs::write(path.join("README"), "fixture\n").unwrap();
        git(path, &["add", "README"]);
        git(path, &["commit", "-q", "-m", "fixture"]);
        git(path, &["rev-parse", "HEAD"])
    }

    fn lease_target(
        repo_path: &std::path::Path,
        branch: Option<String>,
        owner_ref: CellOwnerRef,
    ) -> TerminalResourceTarget {
        TerminalResourceTarget {
            slug: "direct".into(),
            job_id: Some("job-7".into()),
            project_id: "p".into(),
            run_id: Some("run-7".into()),
            cwd: repo_path.to_string_lossy().into_owned(),
            branch,
            repo_path: repo_path.to_string_lossy().into_owned(),
            owner_ref: Some(owner_ref),
            lease: None,
        }
    }

    #[tokio::test]
    async fn ambient_job_terminal_acquisition_preserves_owner_and_uses_live_checkout_head() {
        let db = crate::storage::migrated_test_db("term_ambient_acquire.db").await;
        let orch = test_orchestrator(db);
        let checkout = tempdir().unwrap();
        let head = init_git_checkout(checkout.path());
        let owner_ref = CellOwnerRef {
            project_id: "p".into(),
            project_key: Some("P".into()),
            issue_number: Some(7),
            job_id: Some("job-7".into()),
            execution_seq: Some(2),
            node_kind: Some("builder".into()),
        };
        let target = lease_target(checkout.path(), None, owner_ref.clone());

        let (lease_id, owner, request) = terminal_lease_acquisition(&orch, &target).await.unwrap();

        assert_eq!(lease_id, "terminal:job-7");
        assert_eq!(owner.owner_id, "job-7");
        assert_eq!(request.declaration.owner, owner);
        assert_eq!(request.declaration.owner_ref, Some(owner_ref));
        assert_eq!(request.declaration.initial_base_commit, head);
        assert_eq!(
            request.declaration.repository,
            RepositoryLocator::ExistingCheckout {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: checkout.path().to_string_lossy().into_owned(),
            }
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn managed_job_terminal_acquisition_resolves_bookmark_and_uses_colocated_path() {
        let db = crate::storage::migrated_test_db("term_managed_acquire.db").await;
        let orch = test_orchestrator(db);
        let checkout = tempdir().unwrap();
        init_git_checkout(checkout.path());
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, checkout.path());
        crate::jj::ensure_project_store(&jj, &store, checkout.path()).unwrap();
        let expected = crate::jj::bookmark_commit(&jj, &store, "main").unwrap();
        let target = lease_target(
            checkout.path(),
            Some("main".into()),
            CellOwnerRef::default(),
        );

        let (_, _, request) = terminal_lease_acquisition(&orch, &target).await.unwrap();

        assert_eq!(request.declaration.initial_base_commit, expected);
        assert_eq!(
            request.declaration.repository,
            RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: checkout.path().to_string_lossy().into_owned(),
            }
        );
    }

    struct SharedCapturingEmitter(Arc<CapturingEmitter>);

    impl crate::services::EventEmitter for SharedCapturingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.0.emit(event, payload)
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.0.emit_empty(event)
        }
    }

    /// Run an async block on a throwaway current-thread runtime. Each call
    /// creates and drops its own runtime, so the test thread carries no ambient
    /// runtime between calls — finalize's internal `block_on` then runs safely.
    fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    fn test_orchestrator_with_emitter(db: LocalDb) -> (Orchestrator, Arc<CapturingEmitter>) {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let emitter = Arc::new(CapturingEmitter::new());
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_emitter(SharedCapturingEmitter(emitter.clone()))
                .build(),
        );
        (
            Orchestrator::builder(db_state, services, config_dir).build(),
            emitter,
        )
    }

    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',7,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES('e','rec','i','p','running',1,2);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, created_at, updated_at) VALUES('j','p','i','e','builder','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, parent_job_id, uri_segment, status, created_at, updated_at) VALUES('task-j','p','i','e','j','Explore','running',1,1);
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('t','j','s1','sleep 100','running',1,'run-1');
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('task-t','task-j','task-s1','sleep 100','running',1,'run-1');
            ",
        )
        .await
        .unwrap();
    }

    async fn seed_terminal_fence(orch: &Orchestrator, fence: Fence) {
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "rec",
                "name": "Recipe",
                "description": null,
                "trigger": "manual",
                "nodes": [],
                "edges": []
            },
            "agents": {
                "build": {
                    "id": "build",
                    "name": "Build",
                    "description": "Build",
                    "prompt": "prompt",
                    "tools": [],
                    "disallowedTools": null,
                    "skills": null,
                    "fence": fence
                }
            },
            "skills": {},
            "triggerContext": {
                "issueId": "i",
                "projectId": "p",
                "triggerType": "manual"
            },
            "delegatedPackets": [],
            "createdAt": 1
        })
        .to_string();
        orch.db
            .local
            .execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = 'e'",
                params![snapshot.as_str()],
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET agent_config_id = 'build' WHERE id = 'j'",
                (),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
                 VALUES('run-terminal-policy','i','p','j','running',1,1)",
                (),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shared_lease_restart_excludes_completed_one_shots() {
        let db = crate::storage::migrated_test_db("term_shared_restart_set.db").await;
        seed(&db).await;
        db.execute_script(
            "UPDATE job_terminals SET status = 'exited', lease_id = 'terminal:j', lease_incarnation_id = 'inc', lease_epoch = 7 WHERE id = 't';
             INSERT INTO job_terminals(id, job_id, session_id, command, title, status, created_at, slug, lease_id)
             VALUES('interactive','j','s2','','Shell','running',2,'shell','terminal:j');
             INSERT INTO job_terminals(id, job_id, session_id, command, title, status, created_at, slug, lease_id)
             VALUES('watch','j','s3','bun test --watch','Watch','running',3,'watch','terminal:j');",
        )
        .await
        .unwrap();

        db.execute_script("UPDATE job_terminals SET lease_incarnation_id = 'inc', lease_epoch = 7 WHERE lease_id = 'terminal:j';").await.unwrap();
        let fence = LifetimeLeaseFence {
            lease_id: "terminal:j".into(),
            owner: LifetimeLeaseOwner {
                kind: LifetimeLeaseOwnerKind::Terminal,
                owner_id: "j".into(),
            },
            incarnation_id: "inc".into(),
            lease_epoch: 7,
        };
        let slugs = terminal_slugs_for_lease(&db, &fence, &std::collections::HashSet::new())
            .await
            .unwrap();

        assert_eq!(slugs, vec!["shell", "watch"]);
    }

    async fn read_terminal(
        db: &LocalDb,
        session_id: &str,
    ) -> (String, Option<i32>, Option<String>) {
        let session_id = session_id.to_string();
        db.query_opt(
            "SELECT status, exit_code, output_tail FROM job_terminals WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_i64(1)?.map(|v| v as i32),
                    row.opt_text(2)?,
                ))
            },
        )
        .await
        .unwrap()
        .expect("terminal row present")
    }

    fn node_uri() -> String {
        cairn_common::uri::build_node_terminal_uri("P", 7, 2, "builder", "run-1")
    }

    #[derive(Clone, Copy)]
    enum FakeTerminalStart {
        ImmediateExit,
        FailStart,
        DeleteBindingBeforeResponse,
        FailRefresh,
    }

    fn test_terminal_target(slug: &str) -> (TerminalResourceTarget, LifetimeLeaseFence) {
        let owner = LifetimeLeaseOwner {
            kind: LifetimeLeaseOwnerKind::Terminal,
            owner_id: "j".into(),
        };
        let fence = LifetimeLeaseFence {
            lease_id: "terminal:j".into(),
            owner,
            incarnation_id: "incarnation".into(),
            lease_epoch: 7,
        };
        (
            TerminalResourceTarget {
                slug: slug.into(),
                job_id: Some("j".into()),
                project_id: "p".into(),
                run_id: None,
                cwd: "/cell".into(),
                branch: Some("branch".into()),
                repo_path: "/tmp".into(),
                owner_ref: None,
                lease: Some(fence.clone()),
            },
            fence,
        )
    }

    async fn spawn_and_capture_terminal_process(
        orch: &Orchestrator,
        mut target: TerminalResourceTarget,
        fence: LifetimeLeaseFence,
        command: &str,
    ) -> LifetimeProcessSpec {
        target.run_id = Some("run-terminal-policy".into());
        let (executor, _, _, start_process) =
            attach_fake_terminal_executor(orch, fence, FakeTerminalStart::ImmediateExit);
        spawn_terminal_session(
            orch,
            node_terminal_resource(&target.slug),
            target,
            command.to_string(),
            None,
            true,
            Some("/bin/sh".into()),
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();
        let process = start_process
            .lock()
            .unwrap()
            .clone()
            .expect("terminal spawn must submit a process specification");
        executor.abort();
        process
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_spawn_honors_fence_allow_as_unconfined() {
        let db = crate::storage::migrated_test_db("term_policy_allow.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        seed_terminal_fence(&orch, Fence::Allow).await;
        let checkout = tempdir().unwrap();
        std::fs::create_dir(checkout.path().join(".jj")).unwrap();
        let (mut target, fence) = test_terminal_target("policy-allow");
        target.cwd = checkout.path().to_string_lossy().into_owned();

        let process =
            spawn_and_capture_terminal_process(&orch, target, fence, "bun dev:instance").await;

        assert_eq!(process.sandbox_mode, ProcessSandboxMode::Unconfined);
        assert!(process.sandbox_policy.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_spawn_honors_fence_ask_as_worktree_confined() {
        let db = crate::storage::migrated_test_db("term_policy_ask.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        seed_terminal_fence(&orch, Fence::Ask).await;
        let checkout = tempdir().unwrap();
        std::fs::create_dir(checkout.path().join(".jj")).unwrap();
        let (mut target, fence) = test_terminal_target("policy-ask");
        target.cwd = checkout.path().to_string_lossy().into_owned();

        let process =
            spawn_and_capture_terminal_process(&orch, target, fence, "bun dev:instance").await;

        assert_eq!(process.sandbox_mode, ProcessSandboxMode::Confined);
        let policy = process
            .sandbox_policy
            .expect("ask fence must carry a policy");
        assert_eq!(policy.worktree, checkout.path().to_string_lossy());
        assert!(policy.worktree_writable);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_spawn_keeps_branchless_live_checkout_read_only() {
        let db = crate::storage::migrated_test_db("term_policy_readonly.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        seed_terminal_fence(&orch, Fence::Allow).await;
        let checkout = tempdir().unwrap();
        let (mut target, fence) = test_terminal_target("policy-readonly");
        target.cwd = checkout.path().to_string_lossy().into_owned();
        target.branch = None;

        let process =
            spawn_and_capture_terminal_process(&orch, target, fence, "touch forbidden").await;

        assert_eq!(process.sandbox_mode, ProcessSandboxMode::ReadOnlyCheckout);
        let policy = process
            .sandbox_policy
            .expect("live checkout must carry a read-only policy");
        assert!(!policy.worktree_writable);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_spawn_runs_accepted_scopeless_dev_command_unconfined() {
        let db = crate::storage::migrated_test_db("term_policy_dev_unconfined.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        seed_terminal_fence(&orch, Fence::Ask).await;
        let checkout = tempdir().unwrap();
        std::fs::create_dir(checkout.path().join(".jj")).unwrap();
        std::fs::create_dir(checkout.path().join(".cairn")).unwrap();
        std::fs::write(
            checkout.path().join(".cairn/config.yaml"),
            "terminalCommands:\n  - name: Dev\n    command: bun dev:instance\n",
        )
        .unwrap();
        crate::config::settings::set_accepted_fence_command(
            &orch.config_dir,
            "p",
            "bun dev:instance",
            true,
        )
        .unwrap();
        let (mut target, fence) = test_terminal_target("policy-dev-unconfined");
        target.cwd = checkout.path().to_string_lossy().into_owned();

        let process =
            spawn_and_capture_terminal_process(&orch, target, fence, "bun dev:instance").await;

        assert_eq!(process.sandbox_mode, ProcessSandboxMode::Unconfined);
        assert!(process.sandbox_policy.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_spawn_confines_accepted_scoped_dev_command_with_expanded_glob() {
        let db = crate::storage::migrated_test_db("term_policy_dev_scoped.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        seed_terminal_fence(&orch, Fence::Ask).await;
        let checkout = tempdir().unwrap();
        std::fs::create_dir(checkout.path().join(".jj")).unwrap();
        std::fs::create_dir(checkout.path().join(".cairn")).unwrap();
        std::fs::write(
            checkout.path().join(".cairn/config.yaml"),
            "terminalCommands:\n  - name: Dev\n    command: bun dev:instance\n    write:\n      - ~/.cairn-dev-*\n",
        )
        .unwrap();
        crate::config::settings::set_accepted_fence_command(
            &orch.config_dir,
            "p",
            "bun dev:instance",
            true,
        )
        .unwrap();
        let (mut target, fence) = test_terminal_target("policy-dev-scoped");
        target.cwd = checkout.path().to_string_lossy().into_owned();

        let process =
            spawn_and_capture_terminal_process(&orch, target, fence, "bun dev:instance").await;

        assert_eq!(process.sandbox_mode, ProcessSandboxMode::Confined);
        let policy = process
            .sandbox_policy
            .expect("scoped accepted command must carry a policy");
        let expanded = dirs::home_dir().unwrap().join(".cairn-dev-*");
        assert!(policy
            .writable_regex
            .contains(&crate::services::sandbox::glob_to_regex(
                &expanded.to_string_lossy()
            )));
    }

    fn test_terminal_slot(fence: &LifetimeLeaseFence) -> PersistentCellState {
        let declaration = LifetimeLeaseDeclaration {
            lease_id: fence.lease_id.clone(),
            owner: fence.owner.clone(),
            owner_ref: None,
            name: "fast".into(),
            purpose: "test terminal".into(),
            repository: RepositoryLocator::ExistingCheckout {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/tmp".into(),
            },
            initial_base_commit: "base".into(),
            resource_reservation: ResourceReservation {
                memory_bytes: 1,
                disk_growth_bytes: 1,
                concurrency_units: 1,
                source: ResourceReservationSource::Declared,
            },
            owner_death_policy: LifetimeOwnerDeathPolicy {
                heartbeat_timeout_ms: 60_000,
                reclaim_grace_ms: 60_000,
            },
        };
        PersistentCellState {
            executor_id: String::new(),
            executor_display_name: None,
            project_id: "p".into(),
            cell_id: "cell".into(),
            path: "/cell".into(),
            workspace_name: "cell".into(),
            repository: "/tmp".into(),
            checkout_kind: CellCheckoutKind::ExistingCheckout,
            git_common_dir: None,
            authority_path: "/cell/.authority".into(),
            lifecycle: PersistentCellLifecycle::Running,
            lease_epoch: fence.lease_epoch,
            last_sealed_commit: Some("base".into()),
            last_used_unix_ms: 1,
            last_affinity_key: None,
            preparation_fingerprint: None,
            occupant: Some(CellOccupant::Lifetime(LifetimeLeaseState {
                declaration,
                incarnation_id: fence.incarnation_id.clone(),
                current_base_commit: "base".into(),
                phase: LifetimeLeasePhase::Active,
                last_heartbeat_unix_ms: 1,
                reclaim_deadline_unix_ms: 0,
                state_revision: 1,
                command_settled: true,
                processes: std::collections::BTreeMap::from([(
                    "main".into(),
                    LifetimeProcessState {
                        generation: 1,
                        spec: None,
                        status: LifetimeProcessStatus::Starting,
                    },
                )]),
                events: Vec::new(),
            })),
        }
    }

    #[test]
    fn persisted_terminal_binding_recovers_without_an_in_memory_session() {
        let (_, fence) = test_terminal_target("fast");
        let cell = test_terminal_slot(&fence);

        let binding = terminal_binding_from_cells(
            &[cell],
            &fence.lease_id,
            &fence.incarnation_id,
            fence.lease_epoch,
            Some("fast"),
            Some(1),
        )
        .expect("retained executor cell should recover the terminal binding");

        assert_eq!(binding.fence, fence);
        assert_eq!(binding.process_key, "main");
        assert_eq!(binding.process_generation, 1);
    }

    fn fence_from_cell(cell: &PersistentCellState) -> LifetimeLeaseFence {
        let lease = cell
            .occupant
            .as_ref()
            .and_then(CellOccupant::lifetime)
            .expect("terminal fixture cell has a lifetime occupant");
        LifetimeLeaseFence {
            lease_id: lease.declaration.lease_id.clone(),
            owner: lease.declaration.owner.clone(),
            incarnation_id: lease.incarnation_id.clone(),
            lease_epoch: cell.lease_epoch,
        }
    }

    /// A cell whose lifetime declaration mirrors `declaration`, so the fleet's
    /// acquire-route resolution treats a pre-registered fake lease as the same
    /// identity the runner is acquiring — letting a test drive a real Acquire.
    fn terminal_slot_for_declaration(
        declaration: LifetimeLeaseDeclaration,
        path: &std::path::Path,
    ) -> PersistentCellState {
        let base = declaration.initial_base_commit.clone();
        let mut cell = test_terminal_slot(&LifetimeLeaseFence {
            lease_id: declaration.lease_id.clone(),
            owner: declaration.owner.clone(),
            incarnation_id: "incarnation".into(),
            lease_epoch: 7,
        });
        cell.path = path.to_string_lossy().into_owned();
        cell.repository = path.to_string_lossy().into_owned();
        cell.last_sealed_commit = Some(base.clone());
        if let Some(CellOccupant::Lifetime(lease)) = cell.occupant.as_mut() {
            lease.declaration = declaration;
            lease.current_base_commit = base;
        }
        cell
    }

    fn attach_fake_terminal_executor(
        orch: &Orchestrator,
        fence: LifetimeLeaseFence,
        behavior: FakeTerminalStart,
    ) -> (
        tokio::task::JoinHandle<()>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Option<LifetimeProcessSpec>>>,
    ) {
        attach_fake_terminal_executor_with_cell(orch, test_terminal_slot(&fence), behavior)
    }

    fn attach_fake_terminal_executor_with_cell(
        orch: &Orchestrator,
        cell: PersistentCellState,
        behavior: FakeTerminalStart,
    ) -> (
        tokio::task::JoinHandle<()>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Option<LifetimeProcessSpec>>>,
    ) {
        let fence = fence_from_cell(&cell);
        let pool: Arc<Fleet> = orch.fleet.clone();
        let db = orch.db.local.clone();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let connection_generation = pool.attach_executor(sender);
        assert!(pool.set_executor_snapshot(
            "colocated",
            connection_generation,
            FleetSnapshot {
                cells: vec![cell.clone()],
                ..Default::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        let releases = Arc::new(AtomicUsize::new(0));
        let release_count = releases.clone();
        let ops = Arc::new(Mutex::new(Vec::<String>::new()));
        let ops_log = ops.clone();
        let start_process: Arc<Mutex<Option<LifetimeProcessSpec>>> = Arc::new(Mutex::new(None));
        let start_process_log = start_process.clone();
        let task = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                let ExecutorMessage::LifetimeLeaseRequest {
                    correlation_id,
                    operation,
                } = message
                else {
                    continue;
                };
                ops_log.lock().unwrap().push(
                    match &operation {
                        LifetimeLeaseOperation::Acquire { .. } => "Acquire",
                        LifetimeLeaseOperation::RefreshCheckout { .. } => "RefreshCheckout",
                        LifetimeLeaseOperation::StartProcess { .. } => "StartProcess",
                        LifetimeLeaseOperation::Release { .. } => "Release",
                        LifetimeLeaseOperation::StopProcess { .. } => "StopProcess",
                        _ => "Other",
                    }
                    .to_string(),
                );
                if let LifetimeLeaseOperation::StartProcess { process, .. } = &operation {
                    *start_process_log.lock().unwrap() = Some(process.clone());
                }
                let result = match operation {
                    LifetimeLeaseOperation::RefreshCheckout { .. }
                        if matches!(behavior, FakeTerminalStart::FailRefresh) =>
                    {
                        LifetimeLeaseResult::Failed {
                            kind: LifetimeLeaseFailureKind::Cleanup,
                            diagnostic: "injected refresh failure".into(),
                            cell_outcome: None,
                        }
                    }
                    LifetimeLeaseOperation::StartProcess { process_key, .. } => match behavior {
                        FakeTerminalStart::FailStart | FakeTerminalStart::FailRefresh => {
                            LifetimeLeaseResult::Failed {
                                kind: LifetimeLeaseFailureKind::Process,
                                diagnostic: "injected start failure".into(),
                                cell_outcome: None,
                            }
                        }
                        FakeTerminalStart::ImmediateExit => {
                            let mut started_slot = cell.clone();
                            let Some(CellOccupant::Lifetime(lease)) =
                                started_slot.occupant.as_mut()
                            else {
                                unreachable!("terminal fixture always has a lifetime occupant")
                            };
                            let process = lease
                                .processes
                                .remove("main")
                                .expect("terminal fixture process");
                            lease.processes.insert(process_key.clone(), process);
                            assert!(pool.set_executor_snapshot(
                                "colocated",
                                connection_generation,
                                FleetSnapshot {
                                    cells: vec![started_slot.clone()],
                                    ..Default::default()
                                },
                                ExecutorSubstrateReport::default(),
                            ));
                            let event = LifetimeProcessEvent {
                                lease_id: fence.lease_id.clone(),
                                incarnation_id: fence.incarnation_id.clone(),
                                lease_epoch: fence.lease_epoch,
                                process_key: process_key.clone(),
                                process_generation: 1,
                                event: LifetimeProcessEventKind::State {
                                    status: LifetimeProcessStatus::Exited {
                                        finished_at_unix_ms: 2,
                                        exit_code: Some(0),
                                        restartable: true,
                                        executor_lost: false,
                                    },
                                },
                            };
                            // Delivery before StartProcess responds is the race
                            // that creation must tolerate. A duplicate proves
                            // finalization and event emission remain idempotent.
                            pool.handle_executor_message(
                                "colocated",
                                connection_generation,
                                ExecutorMessage::LifetimeProcessEvent {
                                    event: event.clone(),
                                },
                            );
                            pool.handle_executor_message(
                                "colocated",
                                connection_generation,
                                ExecutorMessage::LifetimeProcessEvent { event },
                            );
                            LifetimeLeaseResult::State { cell: started_slot }
                        }
                        FakeTerminalStart::DeleteBindingBeforeResponse => {
                            db.execute(
                                "DELETE FROM job_terminals WHERE slug = ?1",
                                params!["fast"],
                            )
                            .await
                            .unwrap();
                            let mut started_slot = cell.clone();
                            let Some(CellOccupant::Lifetime(lease)) =
                                started_slot.occupant.as_mut()
                            else {
                                unreachable!("terminal fixture always has a lifetime occupant")
                            };
                            let process = lease
                                .processes
                                .remove("main")
                                .expect("terminal fixture process");
                            lease.processes.insert(process_key, process);
                            LifetimeLeaseResult::State { cell: started_slot }
                        }
                    },
                    LifetimeLeaseOperation::Release { .. } => {
                        release_count.fetch_add(1, AtomicOrdering::SeqCst);
                        LifetimeLeaseResult::Released {
                            lease_id: fence.lease_id.clone(),
                            lease_epoch: fence.lease_epoch,
                        }
                    }
                    _ => LifetimeLeaseResult::State { cell: cell.clone() },
                };
                pool.handle_executor_message(
                    "colocated",
                    connection_generation,
                    ExecutorMessage::LifetimeLeaseResponse {
                        correlation_id,
                        result,
                    },
                );
            }
        });
        (task, releases, ops, start_process)
    }

    async fn terminal_row_count(db: &LocalDb, slug: &str) -> i64 {
        db.query_one(
            "SELECT COUNT(*) FROM job_terminals WHERE slug = ?1",
            params![slug],
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn immediate_executor_exit_finalizes_the_preinserted_terminal_once() {
        let db = crate::storage::migrated_test_db("term_immediate_exit.db").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db);
        seed(&orch.db.local).await;
        let (target, fence) = test_terminal_target("fast");
        let (executor, _, _, _) =
            attach_fake_terminal_executor(&orch, fence, FakeTerminalStart::ImmediateExit);

        let session_id = spawn_terminal_session(
            &orch,
            CairnResource::NodeTerminal {
                project: "P".into(),
                number: 7,
                exec_seq: 2,
                node_id: "builder".into(),
                slug: "fast".into(),
            },
            target,
            String::new(),
            None,
            true,
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

        let (status, code, _) = read_terminal(&orch.db.local, &session_id).await;
        assert_eq!(status, "exited");
        assert_eq!(code, Some(0));
        assert_eq!(emitter.events_named("pty-exit").len(), 1);
        executor.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_failure_rolls_back_row_session_handler_and_retained_lease() {
        let db = crate::storage::migrated_test_db("term_start_rollback.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        let (target, fence) = test_terminal_target("fast");
        let lease_id = fence.lease_id.clone();
        let (executor, releases, _, _) =
            attach_fake_terminal_executor(&orch, fence, FakeTerminalStart::FailStart);

        let result = spawn_terminal_session(
            &orch,
            CairnResource::NodeTerminal {
                project: "P".into(),
                number: 7,
                exec_seq: 2,
                node_id: "builder".into(),
                slug: "fast".into(),
            },
            target,
            String::new(),
            None,
            true,
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(terminal_row_count(&orch.db.local, "fast").await, 0);
        assert!(orch.pty_state.sessions.lock().unwrap().is_empty());
        assert!(!orch
            .pty_state
            .lifetime_handlers
            .lock()
            .unwrap()
            .keys()
            .any(|(candidate, _)| candidate == &lease_id));
        assert_eq!(releases.load(AtomicOrdering::SeqCst), 1);
        executor.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_generation_binding_rolls_back_started_process_and_lease() {
        let db = crate::storage::migrated_test_db("term_persist_rollback.db").await;
        let orch = test_orchestrator(db);
        seed(&orch.db.local).await;
        let (target, fence) = test_terminal_target("fast");
        let (executor, releases, _, _) = attach_fake_terminal_executor(
            &orch,
            fence,
            FakeTerminalStart::DeleteBindingBeforeResponse,
        );

        let result = spawn_terminal_session(
            &orch,
            CairnResource::NodeTerminal {
                project: "P".into(),
                number: 7,
                exec_seq: 2,
                node_id: "builder".into(),
                slug: "fast".into(),
            },
            target,
            String::new(),
            None,
            true,
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await;
        assert!(result
            .unwrap_err()
            .contains("terminal generation binding disappeared"));
        assert_eq!(terminal_row_count(&orch.db.local, "fast").await, 0);
        assert!(orch.pty_state.sessions.lock().unwrap().is_empty());
        assert_eq!(releases.load(AtomicOrdering::SeqCst), 1);
        executor.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_residency_gate_refreshes_the_checkout_to_the_tip() {
        let db = crate::storage::migrated_test_db("term_residency_refresh_ok.db").await;
        let orch = test_orchestrator(db);
        let (_target, fence) = test_terminal_target("fast");
        let (executor, _releases, ops, _) =
            attach_fake_terminal_executor(&orch, fence.clone(), FakeTerminalStart::ImmediateExit);

        ensure_terminal_checkout_current(&orch, &fence, "new-tip")
            .await
            .expect("a healthy executor must accept the residency refresh");

        assert_eq!(
            *ops.lock().unwrap(),
            vec!["RefreshCheckout".to_string()],
            "the residency gate must issue exactly one RefreshCheckout to the tip"
        );
        executor.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_residency_gate_fails_closed_on_refresh_failure() {
        let db = crate::storage::migrated_test_db("term_residency_refresh_fail.db").await;
        let orch = test_orchestrator(db);
        let (_target, fence) = test_terminal_target("fast");
        let (executor, _releases, _ops, _) =
            attach_fake_terminal_executor(&orch, fence.clone(), FakeTerminalStart::FailRefresh);

        let error = ensure_terminal_checkout_current(&orch, &fence, "new-tip")
            .await
            .expect_err("a refresh failure must fail the residency gate closed");
        assert!(
            error.contains("refresh terminal checkout to new-tip"),
            "the failure must name the tip it could not reach: {error}"
        );
        executor.abort();
    }

    /// Point project `p` and job `j` at a real jj checkout so a NodeTerminal
    /// resolves to a managed job terminal on branch `main`.
    async fn seed_managed_job_terminal_checkout(orch: &Orchestrator, checkout: &std::path::Path) {
        seed(&orch.db.local).await;
        init_git_checkout(checkout);
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, checkout);
        crate::jj::ensure_project_store(&jj, &store, checkout).unwrap();
        let cp = checkout.to_string_lossy().into_owned();
        orch.db
            .local
            .execute(
                "UPDATE projects SET repo_path = ?1 WHERE id = 'p'",
                params![cp.as_str()],
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET worktree_path = ?1, branch = 'main' WHERE id = 'j'",
                params![cp.as_str()],
            )
            .await
            .unwrap();
    }

    fn node_terminal_resource(slug: &str) -> CairnResource {
        CairnResource::NodeTerminal {
            project: "P".into(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".into(),
            slug: slug.into(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn create_job_terminal_acquires_refreshes_then_starts_in_order() {
        let db = crate::storage::migrated_test_db("term_create_order.db").await;
        let orch = test_orchestrator(db);
        let checkout = tempdir().unwrap();
        seed_managed_job_terminal_checkout(&orch, checkout.path()).await;
        let resource = node_terminal_resource("direct");
        let target = resolve_terminal_resource_target(&orch.db.local, &resource)
            .await
            .unwrap();
        let (_, _, request) = terminal_lease_acquisition(&orch, &target).await.unwrap();
        let cell = terminal_slot_for_declaration(request.declaration.clone(), checkout.path());
        let (executor, _releases, ops, start_process) =
            attach_fake_terminal_executor_with_cell(&orch, cell, FakeTerminalStart::ImmediateExit);

        create_interactive_terminal_from_resource(
            &orch,
            resource,
            None,
            "t".into(),
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .expect("creating a job terminal at a current head should succeed");

        let recorded = ops.lock().unwrap().clone();
        assert!(
            recorded.len() >= 3,
            "expected acquire, refresh, and start: {recorded:?}"
        );
        assert_eq!(
            &recorded[..3],
            &[
                "Acquire".to_string(),
                "RefreshCheckout".to_string(),
                "StartProcess".to_string()
            ],
            "the shell must not spawn until the checkout is refreshed to the tip"
        );
        // The managed cell is a detached-HEAD checkout, so the spawned shell must
        // carry the job's branch via CAIRN_WORKTREE_BRANCH for branch-keyed
        // tooling (bun dev:instance, bun run changelog) to resolve without
        // --branch. Pinned at the executor boundary, mirroring the run path.
        let env = start_process
            .lock()
            .unwrap()
            .clone()
            .expect("StartProcess must carry a process specification")
            .env;
        // The env block appends over an inherited base (like PATH), so the last
        // occurrence is the effective value the executor applies.
        let branch = env
            .iter()
            .rev()
            .find(|(key, _)| key == "CAIRN_WORKTREE_BRANCH")
            .map(|(_, value)| value.as_str());
        assert_eq!(
            branch,
            Some("main"),
            "a job terminal's shell env must export the job's branch"
        );
        executor.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn create_job_terminal_fails_closed_without_spawning_on_refresh_failure() {
        let db = crate::storage::migrated_test_db("term_create_fail_closed.db").await;
        let orch = test_orchestrator(db);
        let checkout = tempdir().unwrap();
        seed_managed_job_terminal_checkout(&orch, checkout.path()).await;
        let resource = node_terminal_resource("direct");
        let target = resolve_terminal_resource_target(&orch.db.local, &resource)
            .await
            .unwrap();
        let (_, _, request) = terminal_lease_acquisition(&orch, &target).await.unwrap();
        let cell = terminal_slot_for_declaration(request.declaration.clone(), checkout.path());
        let (executor, _releases, ops, _) =
            attach_fake_terminal_executor_with_cell(&orch, cell, FakeTerminalStart::FailRefresh);

        let result = create_interactive_terminal_from_resource(
            &orch,
            resource,
            None,
            "t".into(),
            None,
            LifetimePtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await;

        assert!(
            result.is_err(),
            "a stale-head refresh failure must fail creation"
        );
        let recorded = ops.lock().unwrap().clone();
        assert!(
            !recorded.contains(&"StartProcess".to_string()),
            "no shell may spawn when the residency refresh fails: {recorded:?}"
        );
        assert_eq!(
            recorded,
            vec![
                "Acquire".to_string(),
                "RefreshCheckout".to_string(),
                "Release".to_string()
            ],
            "a failed refresh acquires, refreshes, then releases the lease"
        );
        assert_eq!(
            terminal_row_count(&orch.db.local, "direct").await,
            0,
            "no running terminal row may be inserted when creation fails closed"
        );
        executor.abort();
    }

    #[test]
    fn finalize_marks_exited_retains_tail_and_is_idempotent() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_a.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let buffer: Arc<Mutex<VecDeque<u8>>> =
            Arc::new(Mutex::new(b"all done\n".iter().copied().collect()));
        let uri = node_uri();
        finalize_terminal_session(
            &orch,
            "s1",
            Some(0),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );

        let (status, code, tail) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status, "exited");
        assert_eq!(code, Some(0));
        assert!(tail.unwrap_or_default().contains("all done"));

        // A second finalize with a different code must not re-transition the row.
        finalize_terminal_session(
            &orch,
            "s1",
            Some(99),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );
        let (status2, code2, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status2, "exited");
        assert_eq!(
            code2,
            Some(0),
            "idempotent finalize must not overwrite the recorded exit code"
        );
    }

    #[test]
    fn mark_terminal_exited_is_conditional_on_running() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_b.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let first = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(0),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(first.is_some(), "first mark transitions a running row");

        let second = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(5),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(
            second.is_none(),
            "an already-exited row must not transition again"
        );
    }

    /// Give job `j` a live agent session + run + active turn so a routed wake has
    /// a delivery target (mirrors the integration `seed_node`).
    async fn seed_live_turn(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO sessions(id, job_id, status, created_at, updated_at) VALUES('sess-live','j','active',1,1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode) VALUES('run-live','p','j','live','sess-live',1,1,'resume');
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at) VALUES('turn-live','sess-live','run-live','j',1,'running',1,1);
            UPDATE jobs SET current_session_id='sess-live', current_turn_id='turn-live' WHERE id='j';
            ",
        )
        .await
        .unwrap();
    }

    /// The output-wake subscribe must recover an exit that finalize already routed
    /// (and dropped) before the subscription existed. The caller's pre-persist
    /// `row` snapshot is stale (`None` here), so ONLY the post-persist status
    /// re-read can observe the exit. Removing that re-read makes this fail: the
    /// subscribe would register a watcher on the dead session and return
    /// `OutputPersisted`, leaving the one-shot subscription unconsumed.
    #[test]
    fn output_wake_reroutes_exit_observed_only_after_persist() {
        let db = block_on(crate::storage::migrated_test_db("term_output_reread.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));
        block_on(seed_live_turn(&orch.db.local));
        block_on(orch.db.local.execute(
            "UPDATE job_terminals SET status='exited', exit_code=3, exited_at=50, output_tail='boom' WHERE session_id='s1'",
            (),
        ))
        .unwrap();

        let uri = node_uri();
        let outcome = block_on(subscribe_terminal_output_wake_once(
            &orch,
            "j",
            "run-1",
            &uri,
            "ready",
            None,
            Some("s1"),
            "agent",
        ))
        .unwrap();
        assert!(
            matches!(outcome, TerminalWakeSubscriptionOutcome::ExitAlreadyQueued),
            "post-persist re-read must observe the already-exited terminal and route its exit"
        );

        let subs = block_on(crate::orchestrator::wakes::list_subscriptions_for_job(
            &orch.db.local,
            "j",
        ))
        .unwrap();
        assert!(
            !subs
                .iter()
                .any(|s| s.match_phrase.as_deref() == Some("ready")),
            "the routed exit must consume the newly persisted one-shot output subscription"
        );
    }

    /// The exit-only subscribe has the same recovery obligation: a fast exit that
    /// was routed-and-dropped before the subscription persisted must be recovered
    /// by the post-persist re-read, not left as a live `ExitSubscribed`.
    #[test]
    fn exit_wake_reroutes_exit_observed_only_after_persist() {
        let db = block_on(crate::storage::migrated_test_db("term_exit_reread.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));
        block_on(seed_live_turn(&orch.db.local));
        block_on(orch.db.local.execute(
            "UPDATE job_terminals SET status='exited', exit_code=3, exited_at=50, output_tail='boom' WHERE session_id='s1'",
            (),
        ))
        .unwrap();

        let uri = node_uri();
        let outcome = block_on(subscribe_terminal_exit_wake_once(
            &orch,
            "j",
            "run-1",
            &uri,
            None,
            Some("s1"),
            "agent",
        ))
        .unwrap();
        assert!(
            matches!(outcome, TerminalWakeSubscriptionOutcome::ExitAlreadyQueued),
            "post-persist re-read must observe the already-exited terminal and route its exit"
        );

        let subs = block_on(crate::orchestrator::wakes::list_subscriptions_for_job(
            &orch.db.local,
            "j",
        ))
        .unwrap();
        assert!(
            !subs.iter().any(|s| s.source_kind == "process"),
            "the routed exit must consume the newly persisted one-shot exit subscription"
        );
    }

    #[test]
    fn resolve_task_terminal_targets_child_job_and_scopes_slug_collisions() {
        let db = block_on(crate::storage::migrated_test_db("term_resolve_task.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));
        let resource = CairnResource::TaskTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "run-1".to_string(),
        };

        let target = block_on(resolve_terminal_resource_target(&orch.db.local, &resource)).unwrap();
        assert_eq!(target.job_id.as_deref(), Some("task-j"));
        assert_eq!(target.slug, "run-1");
        assert!(block_on(ensure_terminal_slug_available(&orch.db.local, &target)).is_err());

        let parent_resource = CairnResource::NodeTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "task-only".to_string(),
        };
        let parent_target = block_on(resolve_terminal_resource_target(
            &orch.db.local,
            &parent_resource,
        ))
        .unwrap();
        assert!(block_on(ensure_terminal_slug_available(
            &orch.db.local,
            &parent_target
        ))
        .is_ok());
    }

    /// Regression: synthetic Cairn messages (fence denials, restart notices) are
    /// emitted through `emit_terminal_data`, which must tee them into the
    /// persisted log too — otherwise a post-exit read, which prefers the log over
    /// `output_tail`, would drop exactly the diagnostic output that explains a
    /// fence denial.
    #[test]
    fn emit_terminal_data_tees_into_the_log() {
        use crate::services::testing::CapturingEmitter;
        use std::collections::VecDeque;

        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let emitter = CapturingEmitter::new();
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let log = Arc::new(Mutex::new(crate::scratch::TerminalLog::open(
            &job_id, "dev",
        )));

        let denial = "\r\nDenied by agent fence policy (fence: deny): touch /etc/x\r\n";
        emit_terminal_data(&emitter, "sess", denial, &buffer, &log);
        if let Ok(mut guard) = log.lock() {
            if let Some(l) = guard.as_mut() {
                l.flush();
            }
        }

        // Landed in the ring buffer (and hence `output_tail`)...
        let buffered: Vec<u8> = buffer.lock().unwrap().iter().copied().collect();
        assert!(String::from_utf8_lossy(&buffered).contains("Denied by agent fence policy"));
        // ...and in the persisted log, so a post-exit read still shows it.
        let logged = std::fs::read(crate::scratch::terminal_log_path(&job_id, "dev")).unwrap();
        assert!(
            String::from_utf8_lossy(&logged).contains("Denied by agent fence policy"),
            "synthetic fence message must persist to the log"
        );

        crate::scratch::remove_job_scratch_dir(&job_id);
    }
}
