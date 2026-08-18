//! Durable ownership and lifecycle evidence for one Codex provider turn.
//!
//! The current lease is the coordination primitive. Lifecycle rows are evidence:
//! callers must never derive ownership from the append-only history.

use cairn_db::turso::{params, Connection, Row};
use serde::{Deserialize, Serialize};

use crate::storage::{DbError, DbResult, LocalDb, RowExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogPhase {
    ProviderProgress,
    ToolOutstanding,
    PostToolContinuation,
}

/// Load every lease that may still require an owner decision.
///
/// Unlike [`list_reconcilable_watchdog_leases`], this intentionally applies no
/// wall-clock predicate. The reconciler uses it to recognize a backward clock
/// jump instead of treating the absence of deadline matches as healthy silence.
pub async fn list_open_watchdog_leases(db: &LocalDb) -> DbResult<Vec<WatchdogLease>> {
    db.query_all(
        format!(
            "SELECT {LEASE_COLUMNS} FROM codex_watchdog_leases
             WHERE state IN ('active', 'expired', 'recovery_claimed')
             ORDER BY phase_deadline_at, run_id"
        ),
        (),
        lease_from_row,
    )
    .await
}

/// Verify that one exact recovery claim still owns the lease.
///
/// Recovery callers must perform this durable check immediately before each
/// external side effect. Identity alone is insufficient because a timed-out
/// claimant can resume after a reconciler has reclaimed the same generation.
pub async fn owns_watchdog_recovery_claim(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    claimed_at: i64,
) -> DbResult<bool> {
    let identity = identity.clone();
    db.query_opt(
        "SELECT 1 FROM codex_watchdog_leases
         WHERE run_id = ?1 AND session_id = ?2 AND provider_turn_id = ?3
           AND generation = ?4 AND state = 'recovery_claimed' AND updated_at = ?5",
        params![
            identity.run_id,
            identity.session_id,
            identity.provider_turn_id,
            identity.generation,
            claimed_at,
        ],
        |_| Ok(()),
    )
    .await
    .map(|row| row.is_some())
}

impl WatchdogPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProviderProgress => "provider_progress",
            Self::ToolOutstanding => "tool_outstanding",
            Self::PostToolContinuation => "post_tool_continuation",
        }
    }

    fn parse(value: String) -> DbResult<Self> {
        match value.as_str() {
            "provider_progress" => Ok(Self::ProviderProgress),
            "tool_outstanding" => Ok(Self::ToolOutstanding),
            "post_tool_continuation" => Ok(Self::PostToolContinuation),
            _ => Err(DbError::Row(format!("unknown watchdog phase {value:?}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogLeaseState {
    Active,
    Expired,
    RecoveryClaimed,
    Disarmed,
    Terminalized,
    RecoveryFailed,
}

impl WatchdogLeaseState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expired => "expired",
            Self::RecoveryClaimed => "recovery_claimed",
            Self::Disarmed => "disarmed",
            Self::Terminalized => "terminalized",
            Self::RecoveryFailed => "recovery_failed",
        }
    }

    fn parse(value: String) -> DbResult<Self> {
        match value.as_str() {
            "active" => Ok(Self::Active),
            "expired" => Ok(Self::Expired),
            "recovery_claimed" => Ok(Self::RecoveryClaimed),
            "disarmed" => Ok(Self::Disarmed),
            "terminalized" => Ok(Self::Terminalized),
            "recovery_failed" => Ok(Self::RecoveryFailed),
            _ => Err(DbError::Row(format!(
                "unknown watchdog lease state {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogLifecycleKind {
    Armed,
    PhaseTransition,
    Heartbeat,
    Disarmed,
    Expired,
    WorkerPanicked,
    WorkerExitedUnexpectedly,
    RecoveryClaimed,
    SuccessorReserved,
    TerminalizationSucceeded,
    TerminalizationFailed,
    SuccessorStarted,
    SuccessorFailed,
}

impl WatchdogLifecycleKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Armed => "armed",
            Self::PhaseTransition => "phase_transition",
            Self::Heartbeat => "heartbeat",
            Self::Disarmed => "disarmed",
            Self::Expired => "expired",
            Self::WorkerPanicked => "worker_panicked",
            Self::WorkerExitedUnexpectedly => "worker_exited_unexpectedly",
            Self::RecoveryClaimed => "recovery_claimed",
            Self::SuccessorReserved => "successor_reserved",
            Self::TerminalizationSucceeded => "terminalization_succeeded",
            Self::TerminalizationFailed => "terminalization_failed",
            Self::SuccessorStarted => "successor_started",
            Self::SuccessorFailed => "successor_failed",
        }
    }

    fn parse(value: String) -> DbResult<Self> {
        match value.as_str() {
            "armed" => Ok(Self::Armed),
            "phase_transition" => Ok(Self::PhaseTransition),
            "heartbeat" => Ok(Self::Heartbeat),
            "disarmed" => Ok(Self::Disarmed),
            "expired" => Ok(Self::Expired),
            "worker_panicked" => Ok(Self::WorkerPanicked),
            "worker_exited_unexpectedly" => Ok(Self::WorkerExitedUnexpectedly),
            "recovery_claimed" => Ok(Self::RecoveryClaimed),
            "successor_reserved" => Ok(Self::SuccessorReserved),
            "terminalization_succeeded" => Ok(Self::TerminalizationSucceeded),
            "terminalization_failed" => Ok(Self::TerminalizationFailed),
            "successor_started" => Ok(Self::SuccessorStarted),
            "successor_failed" => Ok(Self::SuccessorFailed),
            _ => Err(DbError::Row(format!(
                "unknown watchdog lifecycle kind {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogIdentity {
    pub run_id: String,
    pub session_id: String,
    pub provider_turn_id: String,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogLease {
    pub identity: WatchdogIdentity,
    pub runner_boot_id: String,
    pub phase: WatchdogPhase,
    pub phase_deadline_at: i64,
    pub last_provider_progress_at: i64,
    pub last_worker_heartbeat_at: i64,
    pub armed_at: i64,
    pub updated_at: i64,
    pub state: WatchdogLeaseState,
    pub terminal_reason: Option<String>,
    pub successor_run_id: Option<String>,
    pub successor_turn_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewWatchdogLease {
    pub identity: WatchdogIdentity,
    pub runner_boot_id: String,
    pub phase: WatchdogPhase,
    pub phase_deadline_at: i64,
    pub now: i64,
}

#[derive(Debug, Clone)]
pub struct NewWatchdogLifecycle {
    pub id: String,
    pub identity: WatchdogIdentity,
    pub kind: WatchdogLifecycleKind,
    pub phase: Option<WatchdogPhase>,
    pub reason: Option<String>,
    pub details: Option<serde_json::Value>,
    pub successor_run_id: Option<String>,
    pub successor_turn_id: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogLifecycle {
    pub id: String,
    pub identity: WatchdogIdentity,
    pub kind: WatchdogLifecycleKind,
    pub phase: Option<WatchdogPhase>,
    pub reason: Option<String>,
    pub details_json: Option<String>,
    pub successor_run_id: Option<String>,
    pub successor_turn_id: Option<String>,
    pub created_at: i64,
}

const LEASE_COLUMNS: &str = "run_id, session_id, provider_turn_id, generation, runner_boot_id,
phase, phase_deadline_at, last_provider_progress_at, last_worker_heartbeat_at,
armed_at, updated_at, state, terminal_reason, successor_run_id, successor_turn_id";

fn lease_from_row(row: &Row) -> DbResult<WatchdogLease> {
    Ok(WatchdogLease {
        identity: WatchdogIdentity {
            run_id: row.text(0)?,
            session_id: row.text(1)?,
            provider_turn_id: row.text(2)?,
            generation: row.text(3)?,
        },
        runner_boot_id: row.text(4)?,
        phase: WatchdogPhase::parse(row.text(5)?)?,
        phase_deadline_at: row.i64(6)?,
        last_provider_progress_at: row.i64(7)?,
        last_worker_heartbeat_at: row.i64(8)?,
        armed_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        state: WatchdogLeaseState::parse(row.text(11)?)?,
        terminal_reason: row.opt_text(12)?,
        successor_run_id: row.opt_text(13)?,
        successor_turn_id: row.opt_text(14)?,
    })
}

pub async fn get_watchdog_lease(
    db: &LocalDb,
    run_id: &str,
    session_id: &str,
    provider_turn_id: &str,
) -> DbResult<Option<WatchdogLease>> {
    db.query_opt(
        format!(
            "SELECT {LEASE_COLUMNS} FROM codex_watchdog_leases
             WHERE run_id = ?1 AND session_id = ?2 AND provider_turn_id = ?3"
        ),
        params![
            run_id.to_string(),
            session_id.to_string(),
            provider_turn_id.to_string()
        ],
        lease_from_row,
    )
    .await
}

pub async fn list_reconcilable_watchdog_leases(
    db: &LocalDb,
    deadline_at_or_before: i64,
    heartbeat_before: i64,
) -> DbResult<Vec<WatchdogLease>> {
    db.query_all(
        format!(
            "SELECT {LEASE_COLUMNS} FROM codex_watchdog_leases
             WHERE state IN ('active', 'expired')
               AND phase_deadline_at <= ?1
               AND last_worker_heartbeat_at < ?2
             ORDER BY phase_deadline_at, run_id"
        ),
        params![deadline_at_or_before, heartbeat_before],
        lease_from_row,
    )
    .await
}

pub async fn arm_watchdog(
    db: &LocalDb,
    lease: NewWatchdogLease,
    event_id: String,
) -> DbResult<bool> {
    db.write(move |conn| {
        let lease = lease.clone();
        let event_id = event_id.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "INSERT INTO codex_watchdog_leases (
                   run_id, session_id, provider_turn_id, generation, runner_boot_id, phase,
                   phase_deadline_at, last_provider_progress_at, last_worker_heartbeat_at,
                   armed_at, updated_at, state
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8, ?8, 'active')
                 ON CONFLICT(run_id, session_id, provider_turn_id) DO UPDATE SET
                   generation = excluded.generation,
                   runner_boot_id = excluded.runner_boot_id,
                   phase = excluded.phase,
                   phase_deadline_at = excluded.phase_deadline_at,
                   last_provider_progress_at = excluded.last_provider_progress_at,
                   last_worker_heartbeat_at = excluded.last_worker_heartbeat_at,
                   armed_at = excluded.armed_at,
                   updated_at = excluded.updated_at,
                   state = 'active',
                   terminal_reason = NULL,
                   successor_run_id = NULL,
                   successor_turn_id = NULL
                 WHERE codex_watchdog_leases.state IN
                   ('disarmed', 'terminalized', 'recovery_failed')",
                    params![
                        lease.identity.run_id.as_str(),
                        lease.identity.session_id.as_str(),
                        lease.identity.provider_turn_id.as_str(),
                        lease.identity.generation.as_str(),
                        lease.runner_boot_id.as_str(),
                        lease.phase.as_str(),
                        lease.phase_deadline_at,
                        lease.now
                    ],
                )
                .await?;
            if changed == 1 {
                insert_lifecycle_conn(
                    conn,
                    NewWatchdogLifecycle {
                        id: event_id,
                        identity: lease.identity,
                        kind: WatchdogLifecycleKind::Armed,
                        phase: Some(lease.phase),
                        reason: None,
                        details: None,
                        successor_run_id: None,
                        successor_turn_id: None,
                        created_at: lease.now,
                    },
                )
                .await?;
            }
            Ok(changed == 1)
        })
    })
    .await
}

pub async fn heartbeat_watchdog(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    heartbeat_at: i64,
) -> DbResult<bool> {
    cas_update(
        db,
        identity,
        "UPDATE codex_watchdog_leases
         SET last_worker_heartbeat_at = ?1, updated_at = ?1
         WHERE run_id = ?2 AND session_id = ?3 AND provider_turn_id = ?4
           AND generation = ?5 AND state = 'active'",
        heartbeat_at,
        None,
    )
    .await
}

pub async fn refresh_watchdog_progress(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    phase: WatchdogPhase,
    deadline_at: i64,
    provider_progress_at: i64,
    updated_at: i64,
) -> DbResult<bool> {
    let identity = identity.clone();
    db.execute(
        "UPDATE codex_watchdog_leases
         SET phase = ?1, phase_deadline_at = ?2, last_provider_progress_at = ?3, updated_at = ?4
         WHERE run_id = ?5 AND session_id = ?6 AND provider_turn_id = ?7
           AND generation = ?8 AND state = 'active'",
        params![
            phase.as_str(),
            deadline_at,
            provider_progress_at,
            updated_at,
            identity.run_id,
            identity.session_id,
            identity.provider_turn_id,
            identity.generation
        ],
    )
    .await
    .map(|changed| changed == 1)
}

pub async fn expire_watchdog(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    now: i64,
) -> DbResult<bool> {
    cas_update(
        db,
        identity,
        "UPDATE codex_watchdog_leases SET state = 'expired', updated_at = ?1
         WHERE run_id = ?2 AND session_id = ?3 AND provider_turn_id = ?4
           AND generation = ?5 AND state = 'active' AND phase_deadline_at <= ?1",
        now,
        None,
    )
    .await
}

pub async fn claim_watchdog_recovery(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    now: i64,
) -> DbResult<bool> {
    cas_update(
        db,
        identity,
        "UPDATE codex_watchdog_leases SET state = 'recovery_claimed', updated_at = ?1
         WHERE run_id = ?2 AND session_id = ?3 AND provider_turn_id = ?4
           AND generation = ?5 AND phase_deadline_at <= ?1
           AND (state IN ('active', 'expired')
                OR (state = 'recovery_claimed' AND updated_at <= ?1 - 300))",
        now,
        None,
    )
    .await
}

pub async fn disarm_watchdog(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    reason: &str,
    now: i64,
) -> DbResult<bool> {
    cas_update(
        db,
        identity,
        "UPDATE codex_watchdog_leases
         SET state = 'disarmed', terminal_reason = ?6, updated_at = ?1
         WHERE run_id = ?2 AND session_id = ?3 AND provider_turn_id = ?4
           AND generation = ?5 AND state IN ('active', 'expired')",
        now,
        Some(reason),
    )
    .await
}

pub async fn finish_watchdog_recovery(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    claimed_at: i64,
    succeeded: bool,
    reason: &str,
    successor: Option<(&str, &str)>,
    now: i64,
) -> DbResult<bool> {
    let state = if succeeded {
        WatchdogLeaseState::Terminalized
    } else {
        WatchdogLeaseState::RecoveryFailed
    };
    let successor = successor.map(|(run, turn)| (run.to_string(), turn.to_string()));
    let identity = identity.clone();
    db.execute(
        "UPDATE codex_watchdog_leases
         SET state = ?7, terminal_reason = ?6, successor_run_id = ?8,
             successor_turn_id = ?9, updated_at = ?1
         WHERE run_id = ?2 AND session_id = ?3 AND provider_turn_id = ?4
           AND generation = ?5 AND state = 'recovery_claimed' AND updated_at = ?10",
        params![
            now,
            identity.run_id,
            identity.session_id,
            identity.provider_turn_id,
            identity.generation,
            reason,
            state.as_str(),
            successor.as_ref().map(|ids| ids.0.as_str()),
            successor.as_ref().map(|ids| ids.1.as_str()),
            claimed_at,
        ],
    )
    .await
    .map(|changed| changed == 1)
}

async fn cas_update(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    sql: &'static str,
    now: i64,
    reason: Option<&str>,
) -> DbResult<bool> {
    let identity = identity.clone();
    let reason = reason.map(str::to_string);
    let changed = if let Some(reason) = reason {
        db.execute(
            sql,
            params![
                now,
                identity.run_id,
                identity.session_id,
                identity.provider_turn_id,
                identity.generation,
                reason,
            ],
        )
        .await?
    } else {
        db.execute(
            sql,
            params![
                now,
                identity.run_id,
                identity.session_id,
                identity.provider_turn_id,
                identity.generation,
            ],
        )
        .await?
    };
    Ok(changed == 1)
}

pub async fn reserve_watchdog_successor(
    db: &LocalDb,
    identity: &WatchdogIdentity,
    job_id: &str,
    since: i64,
    limit: i64,
    now: i64,
) -> DbResult<bool> {
    let identity = identity.clone();
    let job_id = job_id.to_string();
    db.write(move |conn| {
        // Cloned per invocation because `write` may retry: the closure is
        // `FnMut`, so an `async move` body that consumed the captures directly
        // could only ever run once.
        let identity = identity.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM codex_watchdog_lifecycle lifecycle
                     JOIN runs ON runs.id = lifecycle.run_id
                     WHERE runs.job_id = ?1 AND lifecycle.kind = 'successor_reserved'
                       AND lifecycle.created_at >= ?2",
                    params![job_id.as_str(), since],
                )
                .await?;
            let count = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal("watchdog reservation count missing"))?
                .i64(0)?;
            if count >= limit {
                return Ok(false);
            }
            insert_lifecycle_conn(
                conn,
                NewWatchdogLifecycle {
                    id: uuid::Uuid::new_v4().to_string(),
                    identity,
                    kind: WatchdogLifecycleKind::SuccessorReserved,
                    phase: None,
                    reason: Some("automatic_recovery_budget".into()),
                    details: Some(serde_json::json!({ "job_id": job_id })),
                    successor_run_id: None,
                    successor_turn_id: None,
                    created_at: now,
                },
            )
            .await?;
            Ok(true)
        })
    })
    .await
}

pub async fn append_watchdog_lifecycle(db: &LocalDb, event: NewWatchdogLifecycle) -> DbResult<()> {
    db.write(move |conn| {
        let event = event.clone();
        Box::pin(async move { insert_lifecycle_conn(conn, event).await })
    })
    .await
}

async fn insert_lifecycle_conn(conn: &Connection, event: NewWatchdogLifecycle) -> DbResult<()> {
    let details_json = event
        .details
        .map(|details| serde_json::to_string(&details))
        .transpose()
        .map_err(|error| DbError::internal(format!("watchdog lifecycle details: {error}")))?;
    conn.execute(
        "INSERT INTO codex_watchdog_lifecycle (
           id, run_id, session_id, provider_turn_id, generation, kind, phase, reason,
           details_json, successor_run_id, successor_turn_id, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            event.id,
            event.identity.run_id,
            event.identity.session_id,
            event.identity.provider_turn_id,
            event.identity.generation,
            event.kind.as_str(),
            event.phase.map(WatchdogPhase::as_str),
            event.reason,
            details_json,
            event.successor_run_id,
            event.successor_turn_id,
            event.created_at
        ],
    )
    .await?;
    Ok(())
}

pub async fn list_watchdog_lifecycle(
    db: &LocalDb,
    identity: &WatchdogIdentity,
) -> DbResult<Vec<WatchdogLifecycle>> {
    let identity = identity.clone();
    db.query_all(
        "SELECT id, run_id, session_id, provider_turn_id, generation, kind, phase,
                reason, details_json, successor_run_id, successor_turn_id, created_at
         FROM codex_watchdog_lifecycle
         WHERE run_id = ?1 AND session_id = ?2 AND provider_turn_id = ?3 AND generation = ?4
         ORDER BY created_at, rowid",
        params![
            identity.run_id,
            identity.session_id,
            identity.provider_turn_id,
            identity.generation
        ],
        |row| {
            Ok(WatchdogLifecycle {
                id: row.text(0)?,
                identity: WatchdogIdentity {
                    run_id: row.text(1)?,
                    session_id: row.text(2)?,
                    provider_turn_id: row.text(3)?,
                    generation: row.text(4)?,
                },
                kind: WatchdogLifecycleKind::parse(row.text(5)?)?,
                phase: row.opt_text(6)?.map(WatchdogPhase::parse).transpose()?,
                reason: row.opt_text(7)?,
                details_json: row.opt_text(8)?,
                successor_run_id: row.opt_text(9)?,
                successor_turn_id: row.opt_text(10)?,
                created_at: row.i64(11)?,
            })
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn migrated_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("watchdog.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_identity(db: &LocalDb) -> WatchdogIdentity {
        // The ledger's FK contract is the relevant fixture; disable checks only
        // while creating the two canonical parent rows without the full job tree.
        db.execute_batch(
            "PRAGMA foreign_keys = OFF;
          INSERT INTO sessions (id, chat_id, backend, status, sequence, created_at, updated_at)
            VALUES ('session', 'chat', 'codex', 'open', 1, 1, 1);
          INSERT INTO runs (id, status, session_id, created_at, updated_at, start_mode)
            VALUES ('run', 'live', 'session', 1, 1, 'new');
          PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();
        WatchdogIdentity {
            run_id: "run".into(),
            session_id: "session".into(),
            provider_turn_id: "provider-turn".into(),
            generation: "generation-a".into(),
        }
    }

    #[tokio::test]
    async fn successor_reservation_is_persisted_and_bounded() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        db.execute_batch(
            "PRAGMA foreign_keys = OFF;
             UPDATE runs SET job_id = 'job' WHERE id = 'run';
             PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();

        for now in 100..103 {
            assert!(reserve_watchdog_successor(&db, &identity, "job", 0, 3, now)
                .await
                .unwrap());
        }
        assert!(
            !reserve_watchdog_successor(&db, &identity, "job", 0, 3, 103)
                .await
                .unwrap()
        );
        assert_eq!(
            list_watchdog_lifecycle(&db, &identity)
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved)
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn arm_read_and_history_are_typed() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        assert!(arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into()
        )
        .await
        .unwrap());

        let lease = get_watchdog_lease(&db, "run", "session", "provider-turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.identity, identity);
        assert_eq!(lease.state, WatchdogLeaseState::Active);
        assert_eq!(
            list_watchdog_lifecycle(&db, &identity).await.unwrap()[0].kind,
            WatchdogLifecycleKind::Armed
        );
    }

    #[tokio::test]
    async fn stale_generation_and_duplicate_claim_lose_cas() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into(),
        )
        .await
        .unwrap();

        let mut stale = identity.clone();
        stale.generation = "generation-b".into();
        assert!(!heartbeat_watchdog(&db, &stale, 150).await.unwrap());
        assert!(claim_watchdog_recovery(&db, &identity, 201).await.unwrap());
        assert!(!claim_watchdog_recovery(&db, &identity, 202).await.unwrap());
        assert!(!claim_watchdog_recovery(&db, &identity, 500).await.unwrap());
        assert!(claim_watchdog_recovery(&db, &identity, 501).await.unwrap());
        assert!(
            !finish_watchdog_recovery(&db, &identity, 201, false, "stale claimant", None, 502,)
                .await
                .unwrap()
        );
        assert!(!disarm_watchdog(&db, &identity, "late completion", 203)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn delayed_claimant_is_not_authorized_for_recovery_side_effects_after_reclaim() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into(),
        )
        .await
        .unwrap();

        let delayed_claim_epoch = 201;
        assert!(claim_watchdog_recovery(&db, &identity, delayed_claim_epoch)
            .await
            .unwrap());
        assert!(
            owns_watchdog_recovery_claim(&db, &identity, delayed_claim_epoch)
                .await
                .unwrap()
        );

        let reclaimer_epoch = 501;
        assert!(claim_watchdog_recovery(&db, &identity, reclaimer_epoch)
            .await
            .unwrap());

        let mut terminalizations = 0;
        if owns_watchdog_recovery_claim(&db, &identity, delayed_claim_epoch)
            .await
            .unwrap()
        {
            terminalizations += 1;
        }
        let mut successor_launches = 0;
        if owns_watchdog_recovery_claim(&db, &identity, delayed_claim_epoch)
            .await
            .unwrap()
        {
            successor_launches += 1;
        }

        assert_eq!(terminalizations, 0);
        assert_eq!(successor_launches, 0);
        assert!(
            owns_watchdog_recovery_claim(&db, &identity, reclaimer_epoch)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn terminal_lease_can_be_rearmed_with_a_new_generation() {
        let db = migrated_db().await;
        let first = seed_identity(&db).await;
        assert!(arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: first.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-first".into(),
        )
        .await
        .unwrap());
        assert!(disarm_watchdog(&db, &first, "completed", 150)
            .await
            .unwrap());

        let mut replacement = first.clone();
        replacement.generation = "generation-b".into();
        assert!(arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: replacement.clone(),
                runner_boot_id: "boot-b".into(),
                phase: WatchdogPhase::PostToolContinuation,
                phase_deadline_at: 300,
                now: 200,
            },
            "event-replacement".into(),
        )
        .await
        .unwrap());

        let lease = get_watchdog_lease(&db, "run", "session", "provider-turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.identity, replacement);
        assert_eq!(lease.state, WatchdogLeaseState::Active);
        assert_eq!(lease.phase, WatchdogPhase::PostToolContinuation);
        assert!(!heartbeat_watchdog(&db, &first, 250).await.unwrap());
        assert_eq!(list_watchdog_lifecycle(&db, &first).await.unwrap().len(), 1);
        assert_eq!(
            list_watchdog_lifecycle(&db, &replacement).await.unwrap()[0].kind,
            WatchdogLifecycleKind::Armed
        );
    }

    #[tokio::test]
    async fn reconciliation_read_requires_both_expired_deadline_and_stale_heartbeat() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity,
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::PostToolContinuation,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into(),
        )
        .await
        .unwrap();

        assert!(list_reconcilable_watchdog_leases(&db, 201, 100)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            list_reconcilable_watchdog_leases(&db, 201, 101)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    #[tokio::test]
    async fn open_lease_scan_includes_crashed_recovery_claims() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into(),
        )
        .await
        .unwrap();
        assert!(claim_watchdog_recovery(&db, &identity, 201).await.unwrap());

        assert_eq!(list_open_watchdog_leases(&db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn progress_refresh_extends_deadline_without_a_phase_change() {
        let db = migrated_db().await;
        let identity = seed_identity(&db).await;
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot-a".into(),
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: 200,
                now: 100,
            },
            "event-armed".into(),
        )
        .await
        .unwrap();

        assert!(refresh_watchdog_progress(
            &db,
            &identity,
            WatchdogPhase::ProviderProgress,
            260,
            160,
            160,
        )
        .await
        .unwrap());
        let lease = get_watchdog_lease(&db, "run", "session", "provider-turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.phase, WatchdogPhase::ProviderProgress);
        assert_eq!(lease.phase_deadline_at, 260);
        assert_eq!(lease.last_provider_progress_at, 160);
    }
}
