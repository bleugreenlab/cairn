//! Per-job stored facts on the `jobs` row, and the run queue's reading of
//! them.
//!
//! | column | value | written by |
//! |---|---|---|
//! | `head_turn` | what the current turn is doing ([`HeadTurn`]) | every turn-state, turn `run_id`/`end_reason` and `current_turn_id` writer |
//! | `activity_at` | when `head_turn` last changed | [`set_head_turn`] |
//! | `turn_end_attempt_state` | the state of the attempt `turn_end_check_attempt_id` names | every attempt-state and pointer writer |
//! | `open_cairn_waits` | waits resolving, or pending on anything but a person | every `agent_waits` state, kind and row writer |
//! | `hold_standing` | the job's active hold: queue, timer, other, or none | every active-hold writer |
//! | `open_check_requests` | unsettled check requests the job filed | check request accept and settle |
//!
//! The run queue's activity is [`QUEUE_ACTIVITY_SQL`], a `CASE` over those
//! columns and `status`/`current_turn_id` of the same row: a job's turn,
//! waits and timed hold count only while it is open (pending or running);
//! its turn-end attempt, check requests and a queue hold count whatever its
//! status, since a handed-off node's checks still wake it and a queue hold is
//! work the queue owes. `idx_jobs_queue_activity` is a partial index on
//! [`QUEUE_LISTED_SQL`], so the queue reads only active and wanting jobs, and
//! a status or pointer change re-evaluates the row with no setter of its own.
//!
//! Counts are deltas with no reconciler. Each writer derives its delta from
//! the rows its own statement changed (a compare-and-set's row count, its own
//! row read in the same transaction, or `RETURNING`), so a repeated settle or
//! a cancel after resolution changes nothing. A cascade delete is a writer
//! too: waits and holds cascade from runs, sessions and turns, and check
//! requests from observations. Production deletes those parents only with
//! their job (whose counts go with its row); the `source_structure` scan
//! fails on any other such delete. The columns' `CHECK (>= 0)` turns a
//! miscounted decrement into a failed write rather than a silent drift.

use crate::storage::{DbResult, TrackedConnection};
use crate::turso::params;

use super::StatusWritten;

/// What a run-queue entry is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Activity {
    /// Agent work is in flight: a turn running or due, or a wait on something
    /// Cairn is doing now. It holds its seat however far down the list it is.
    Active,
    /// Work is due but not started: it takes a free seat by its place.
    Wanting,
    /// Nothing it waits on is moving. It keeps its place and holds no seat.
    Dormant,
}

impl Activity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Wanting => "wanting",
            Self::Dormant => "dormant",
        }
    }

    pub fn from_stored(value: &str) -> Self {
        match value {
            "active" => Self::Active,
            "wanting" => Self::Wanting,
            _ => Self::Dormant,
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Dormant => 0,
            Self::Wanting => 1,
            Self::Active => 2,
        }
    }

    /// The greater of two: active over wanting over dormant.
    pub fn max(self, other: Self) -> Self {
        if other.rank() > self.rank() {
            other
        } else {
            self
        }
    }
}

macro_rules! queue_active_sql {
    () => {
        "(turn_end_attempt_state IN ('requested', 'claimed', 'runtime_started', 'deferred', 'submitted') \
         OR open_check_requests > 0 \
         OR (status IN ('pending', 'running') AND (head_turn IN ('running', 'resume_owed') \
             OR (status = 'running' AND current_turn_id IS NULL) \
             OR open_cairn_waits > 0 OR hold_standing = 'timer')))"
    };
}

/// A job row the run queue counts as active.
pub const QUEUE_ACTIVE_SQL: &str = queue_active_sql!();

/// A job row the run queue counts as active or wanting: the predicate of
/// `idx_jobs_queue_activity`, verbatim.
pub const QUEUE_LISTED_SQL: &str =
    concat!("(", queue_active_sql!(), " OR hold_standing = 'queue')");

/// A job row's run-queue activity.
pub const QUEUE_ACTIVITY_SQL: &str = concat!(
    "CASE WHEN ",
    queue_active_sql!(),
    " THEN 'active' WHEN hold_standing = 'queue' THEN 'wanting' ELSE 'dormant' END"
);

/// The `agent_waits` kinds a person answers. A pending wait of any other kind
/// is on something Cairn is doing (check lanes or requests, a run batch, a
/// terminal, a tool call, a `waitFor` duration), which its resolver ends.
pub const PERSON_WAIT_KINDS: &[&str] = &["operator_request"];

/// Turn-end attempt states in which its checks are still running.
pub const TURN_END_ATTEMPT_ACTIVE_STATES: &[&str] = &[
    "requested",
    "claimed",
    "runtime_started",
    "deferred",
    "submitted",
];

/// Check request states before settlement.
pub const CHECK_REQUEST_OPEN_STATES: &[&str] = &["accepted", "queued", "granted", "running"];

/// What a job's current turn is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadTurn {
    /// Pending or running.
    Running,
    /// A claimed `dependency_unblock` marker whose resume has not launched
    /// (no run, no `end_reason`): the parent's resume is owed.
    ResumeOwed,
    /// Anything else, or no current turn.
    Idle,
}

impl HeadTurn {
    /// A turn in `state`, by its state alone: for writers that know no
    /// marker can be involved.
    pub fn of_state(state: &str) -> Self {
        if matches!(state, "pending" | "running") {
            Self::Running
        } else {
            Self::Idle
        }
    }

    /// A turn with these columns.
    pub fn of(
        state: &str,
        start_reason: &str,
        run_id: Option<&str>,
        end_reason: Option<&str>,
    ) -> Self {
        match Self::of_state(state) {
            Self::Idle
                if state == "complete"
                    && start_reason == "dependency_unblock"
                    && run_id.is_none()
                    && end_reason.is_none() =>
            {
                Self::ResumeOwed
            }
            other => other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::ResumeOwed => "resume_owed",
            Self::Idle => "idle",
        }
    }
}

/// Whether a wait in `state` of `kind` counts in `open_cairn_waits`.
pub fn wait_counts(state: &str, kind: Option<&str>) -> bool {
    match state {
        "resolving" => true,
        "pending" => kind.is_some_and(|kind| !PERSON_WAIT_KINDS.contains(&kind)),
        _ => false,
    }
}

/// `open_cairn_waits`'s change when one wait moves from `before` to `after`,
/// each `(state, kind)`, or `None` for no row.
pub fn wait_delta(
    before: Option<(&str, Option<&str>)>,
    after: Option<(&str, Option<&str>)>,
) -> i64 {
    let counted = |wait: Option<(&str, Option<&str>)>| {
        i64::from(wait.is_some_and(|(state, kind)| wait_counts(state, kind)))
    };
    counted(after) - counted(before)
}

/// The standing of a job's active hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldStanding {
    /// No active hold.
    None,
    /// The run queue owes the work.
    Queue,
    /// A known reset and no gate: a timer Cairn is running.
    Timer,
    /// Any other active hold: no known reset, or a gate on it.
    Other,
}

impl HoldStanding {
    /// The standing of an active hold with these columns.
    pub fn of_active(
        boundary_source: &str,
        retry_not_before: Option<i64>,
        resume_gate: Option<&str>,
    ) -> Self {
        if boundary_source == "queue" {
            Self::Queue
        } else if retry_not_before.is_some() && resume_gate.is_none() {
            Self::Timer
        } else {
            Self::Other
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Queue => "queue",
            Self::Timer => "timer",
            Self::Other => "other",
        }
    }
}

/// Set `job_id`'s `head_turn` after the caller moved its pointer or changed
/// its current turn, stamping `activity_at` when it changes.
pub async fn set_head_turn(
    conn: &TrackedConnection,
    job_id: &str,
    head: HeadTurn,
    now: i64,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET head_turn = ?2, activity_at = ?3 WHERE id = ?1 AND head_turn <> ?2",
            params![job_id, head.as_str(), now],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// [`set_head_turn`] for the job whose current turn is `turn_id`, if any: for
/// a writer that changed one turn and does not know whether it is its job's
/// head. A turn that is no job's head changes nothing.
pub async fn set_head_turn_at(
    conn: &TrackedConnection,
    turn_id: &str,
    head: HeadTurn,
    now: i64,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET head_turn = ?2, activity_at = ?3
              WHERE current_turn_id = ?1 AND head_turn <> ?2",
            params![turn_id, head.as_str(), now],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Record that `job_id` has no pending or running turn left, for a writer
/// that just ended every one of them: a running head becomes idle, and any
/// other head (an owed resume, an idle one) is left as it is.
pub async fn set_no_turn_running(
    conn: &TrackedConnection,
    job_id: &str,
    now: i64,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET head_turn = 'idle', activity_at = ?2
              WHERE id = ?1 AND head_turn = 'running'",
            params![job_id, now],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Set `job_id`'s `turn_end_attempt_state`: the state of the attempt its
/// pointer now names, after the caller moved the pointer.
pub async fn set_turn_end_attempt_state(
    conn: &TrackedConnection,
    job_id: &str,
    state: Option<&str>,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET turn_end_attempt_state = ?2
              WHERE id = ?1 AND turn_end_attempt_state IS NOT ?2",
            params![job_id, state],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// [`set_turn_end_attempt_state`] for a writer that moved `attempt_id` to
/// `state` without moving the pointer: it changes the job only while the
/// pointer names that attempt.
pub async fn set_turn_end_attempt_state_if_pointed(
    conn: &TrackedConnection,
    job_id: &str,
    attempt_id: &str,
    state: &str,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET turn_end_attempt_state = ?3
              WHERE id = ?1 AND turn_end_check_attempt_id = ?2
                AND turn_end_attempt_state IS NOT ?3",
            params![job_id, attempt_id, state],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Add `delta` to `job_id`'s `open_cairn_waits`.
pub async fn add_open_cairn_waits(
    conn: &TrackedConnection,
    job_id: &str,
    delta: i64,
) -> DbResult<StatusWritten> {
    if delta == 0 {
        return Ok(StatusWritten::default());
    }
    let rows = conn
        .execute(
            "UPDATE jobs SET open_cairn_waits = open_cairn_waits + ?2 WHERE id = ?1",
            params![job_id, delta],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Set `job_id`'s `hold_standing`.
pub async fn set_hold_standing(
    conn: &TrackedConnection,
    job_id: &str,
    standing: HoldStanding,
) -> DbResult<StatusWritten> {
    let rows = conn
        .execute(
            "UPDATE jobs SET hold_standing = ?2 WHERE id = ?1 AND hold_standing <> ?2",
            params![job_id, standing.as_str()],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Add `delta` to `job_id`'s `open_check_requests`. A requester this database
/// does not hold changes nothing: the queue has only ever counted a request
/// beside its requester.
pub async fn add_open_check_requests(
    conn: &TrackedConnection,
    job_id: &str,
    delta: i64,
) -> DbResult<StatusWritten> {
    if delta == 0 {
        return Ok(StatusWritten::default());
    }
    let rows = conn
        .execute(
            "UPDATE jobs SET open_check_requests = open_check_requests + ?2 WHERE id = ?1",
            params![job_id, delta],
        )
        .await?;
    Ok(StatusWritten::from_rows(rows))
}

/// Every execution in this database some job is not dormant in, with the
/// greatest activity among its jobs. Reads only those jobs, through
/// `idx_jobs_queue_activity`.
pub async fn non_dormant_executions(
    conn: &TrackedConnection,
) -> DbResult<std::collections::HashMap<String, Activity>> {
    use crate::storage::RowExt;
    let sql = format!(
        "SELECT execution_id, {QUEUE_ACTIVITY_SQL} FROM jobs INDEXED BY idx_jobs_queue_activity
          WHERE {QUEUE_LISTED_SQL} AND execution_id IS NOT NULL"
    );
    let mut rows = conn.query(&sql, ()).await?;
    let mut out = std::collections::HashMap::<String, Activity>::new();
    while let Some(row) = rows.next().await? {
        let activity = Activity::from_stored(&row.text(1)?);
        let entry = out.entry(row.text(0)?).or_insert(Activity::Dormant);
        *entry = entry.max(activity);
    }
    Ok(out)
}

/// Give rows a test seeded with raw inserts their stored facts, by replaying
/// the migration's own backfill over the whole database. Fixtures only: a raw
/// insert is not a writer, and no production path derives a fact from its
/// source rows.
#[cfg(any(test, feature = "test-utils"))]
pub async fn seed_from_sources_for_test(db: &crate::storage::LocalDb) -> DbResult<()> {
    let migration = crate::storage::TURSO_MIGRATIONS
        .iter()
        .find(|migration| migration.name() == "job_stored_status")
        .expect("the stored-status migration is registered");
    let sql = migration.sql();
    let backfill = &sql[sql
        .find("UPDATE jobs SET head_turn")
        .expect("the migration carries its backfill")..];
    db.execute_script(
        "UPDATE jobs SET head_turn = 'idle', activity_at = NULL, turn_end_attempt_state = NULL,
                         open_cairn_waits = 0, hold_standing = 'none', open_check_requests = 0;",
    )
    .await?;
    db.execute_script(backfill).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_on_a_person_never_counts_and_any_resolving_wait_does() {
        assert!(wait_counts("resolving", Some("operator_request")));
        assert!(wait_counts("pending", Some("checks")));
        assert!(!wait_counts("pending", Some("operator_request")));
        assert!(!wait_counts("pending", None));
        assert!(!wait_counts("resolved", Some("checks")));
        assert_eq!(wait_delta(None, Some(("pending", Some("checks")))), 1);
        assert_eq!(
            wait_delta(
                Some(("pending", Some("operator_request"))),
                Some(("resolving", Some("operator_request")))
            ),
            1
        );
        assert_eq!(
            wait_delta(Some(("resolving", None)), Some(("resolved", None))),
            -1
        );
        assert_eq!(
            wait_delta(
                Some(("cancelled", Some("checks"))),
                Some(("cancelled", Some("checks")))
            ),
            0
        );
    }

    #[test]
    fn a_queue_hold_wants_and_a_timed_ungated_hold_is_a_timer() {
        assert_eq!(
            HoldStanding::of_active("queue", Some(1), None),
            HoldStanding::Queue
        );
        assert_eq!(
            HoldStanding::of_active("supply_reset", Some(1), None),
            HoldStanding::Timer
        );
        assert_eq!(
            HoldStanding::of_active("supply_reset", Some(1), Some("launch_failed")),
            HoldStanding::Other
        );
        assert_eq!(
            HoldStanding::of_active("unknown", None, None),
            HoldStanding::Other
        );
    }

    #[test]
    fn the_queue_predicates_name_the_same_states_as_the_classifiers() {
        for state in TURN_END_ATTEMPT_ACTIVE_STATES {
            assert!(QUEUE_ACTIVE_SQL.contains(&format!("'{state}'")));
        }
        assert!(QUEUE_LISTED_SQL.starts_with("((turn_end_attempt_state IN"));
        assert!(QUEUE_ACTIVITY_SQL.ends_with("ELSE 'dormant' END"));
    }
}
