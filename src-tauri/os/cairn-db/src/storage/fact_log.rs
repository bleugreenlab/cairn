//! The durable fact log: one row per minted route fact.
//!
//! A row is an index entry, not a copy: it carries registry-typed fields that
//! are bounded by the minter, and a pointer (`record_ref`, `record_id`) to the
//! canonical record that holds the content. The fact log worker consumes rows
//! in id order through two claims: `indexed_at` (the index lane, which wakes
//! threads and decides whether any route matches) and `route_state` (the route
//! lane, which runs matched routes one row at a time).
//!
//! Thread wakes do not go out per row: the index lane folds each matching row
//! into one pending `fact_log_wakes` row per (thread job, source), which the
//! worker delivers once its settle window closes.

use super::{DbError, DbResult, LocalDb, RowExt};
use crate::turso::params;

/// How minting decides that a fact is a redelivery rather than a new row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogDedupe {
    /// The identity is per event: any existing row with the same
    /// `(source, identity)` means this mint is a redelivery.
    Unique,
    /// The identity is a state: a mint is skipped only when the latest row for
    /// the same `(source, record_ref)` already holds this identity, so a state
    /// that changes and changes back is recorded each time it transitions.
    Transition,
}

/// The lane outcome a row carries. `pending` is the index lane's queue,
/// `queued` the route lane's.
pub mod route_state {
    pub const PENDING: &str = "pending";
    pub const UNMATCHED: &str = "unmatched";
    pub const ROUTE_GENERATED: &str = "route_generated";
    pub const STALE: &str = "stale";
    pub const QUEUED: &str = "queued";
    pub const RUNNING: &str = "running";
    pub const DONE: &str = "done";
    pub const FAILED: &str = "failed";
    pub const INTERRUPTED: &str = "interrupted";
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewFact {
    pub project_id: Option<String>,
    pub source: String,
    pub identity: String,
    pub record_ref: String,
    pub record_id: Option<String>,
    pub fields_json: String,
    pub summary: Option<String>,
    /// `active` or `away`, decided by the minter at decision time.
    pub presence: String,
    pub actor_job_id: Option<String>,
    pub origin_principal_json: Option<String>,
    pub origin_appearance_json: Option<String>,
    pub route_provenance: Option<String>,
    pub minted_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FactRow {
    pub id: i64,
    pub project_id: Option<String>,
    pub source: String,
    pub identity: String,
    pub record_ref: String,
    pub record_id: Option<String>,
    pub fields_json: String,
    pub summary: Option<String>,
    pub presence: String,
    pub actor_job_id: Option<String>,
    pub origin_principal_json: Option<String>,
    pub origin_appearance_json: Option<String>,
    pub route_provenance: Option<String>,
    pub minted_at: i64,
    pub indexed_at: Option<i64>,
    pub route_state: String,
    pub route_note: Option<String>,
    pub wakes_json: Option<String>,
}

const COLUMNS: &str = "id,project_id,source,identity,record_ref,record_id,fields_json,summary,presence,actor_job_id,origin_principal_json,origin_appearance_json,route_provenance,minted_at,indexed_at,route_state,route_note,wakes_json";

fn from_row(row: &crate::turso::Row) -> DbResult<FactRow> {
    Ok(FactRow {
        id: row.i64(0)?,
        project_id: row.opt_text(1)?,
        source: row.text(2)?,
        identity: row.text(3)?,
        record_ref: row.text(4)?,
        record_id: row.opt_text(5)?,
        fields_json: row.text(6)?,
        summary: row.opt_text(7)?,
        presence: row.text(8)?,
        actor_job_id: row.opt_text(9)?,
        origin_principal_json: row.opt_text(10)?,
        origin_appearance_json: row.opt_text(11)?,
        route_provenance: row.opt_text(12)?,
        minted_at: row.i64(13)?,
        indexed_at: row.opt_i64(14)?,
        route_state: row.text(15)?,
        route_note: row.opt_text(16)?,
        wakes_json: row.opt_text(17)?,
    })
}

/// Insert one fact, applying the source's dedupe rule in the same statement so
/// two minters racing on one redelivery cannot both land a row. Returns the
/// new row id, or `None` when the dedupe rule held the row back.
pub async fn insert_fact(db: &LocalDb, fact: NewFact, dedupe: LogDedupe) -> DbResult<Option<i64>> {
    let guard = match dedupe {
        LogDedupe::Unique => "NOT EXISTS (SELECT 1 FROM fact_log WHERE source=?2 AND identity=?3)",
        LogDedupe::Transition => "NOT EXISTS (SELECT 1 FROM fact_log WHERE id=(SELECT MAX(id) FROM fact_log WHERE source=?2 AND record_ref=?4) AND identity=?3)",
    };
    let sql = format!(
        "INSERT INTO fact_log (project_id,source,identity,record_ref,record_id,fields_json,summary,presence,actor_job_id,origin_principal_json,origin_appearance_json,route_provenance,minted_at) \
         SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13 WHERE {guard} RETURNING id"
    );
    db.write(move |conn| {
        let fact = fact.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &sql,
                    params![
                        fact.project_id,
                        fact.source,
                        fact.identity,
                        fact.record_ref,
                        fact.record_id,
                        fact.fields_json,
                        fact.summary,
                        fact.presence,
                        fact.actor_job_id,
                        fact.origin_principal_json,
                        fact.origin_appearance_json,
                        fact.route_provenance,
                        fact.minted_at
                    ],
                )
                .await?;
            let id = match rows.next().await? {
                Some(row) => Some(row.i64(0)?),
                None => None,
            };
            Ok(id)
        })
    })
    .await
}

/// Claim the oldest row the index lane has not seen. The claim is one
/// conditional UPDATE, so two processes sharing the database never both
/// index a row.
pub async fn claim_for_index(db: &LocalDb, now_ms: i64) -> DbResult<Option<FactRow>> {
    let sql = format!(
        "UPDATE fact_log SET indexed_at=?1 WHERE id=(SELECT MIN(id) FROM fact_log WHERE indexed_at IS NULL) AND indexed_at IS NULL RETURNING {COLUMNS}"
    );
    db.write(move |conn| {
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn.query(&sql, params![now_ms]).await?;
            match rows.next().await? {
                Some(row) => Ok(Some(from_row(&row)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// Record what the index lane decided: the route outcome it reached and the
/// thread wakes it delivered.
pub async fn set_index_outcome(
    db: &LocalDb,
    id: i64,
    state: &str,
    note: Option<&str>,
    wakes_json: Option<&str>,
) -> DbResult<()> {
    db.execute(
        "UPDATE fact_log SET route_state=?2, route_note=?3, wakes_json=?4 WHERE id=?1",
        params![
            id,
            state.to_string(),
            note.map(str::to_owned),
            wakes_json.map(str::to_owned)
        ],
    )
    .await?;
    Ok(())
}

/// Claim the oldest row queued for the route lane, marking it `running`. A
/// queued row minted before `stale_before_ms` is marked `stale` instead and the
/// next one is considered: a burst that piled up behind a stuck route must not
/// replay minutes-old notifications.
pub async fn claim_next_queued(db: &LocalDb, stale_before_ms: i64) -> DbResult<Option<FactRow>> {
    let sql = format!(
        "UPDATE fact_log SET route_state='running' WHERE id=(SELECT MIN(id) FROM fact_log WHERE route_state='queued') AND route_state='queued' RETURNING {COLUMNS}"
    );
    loop {
        let sql = sql.clone();
        let claimed = db
            .write(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&sql, ()).await?;
                    match rows.next().await? {
                        Some(row) => Ok(Some(from_row(&row)?)),
                        None => Ok(None),
                    }
                })
            })
            .await?;
        match claimed {
            Some(row) if row.minted_at < stale_before_ms => {
                finish(
                    db,
                    row.id,
                    route_state::STALE,
                    Some("older than max_dispatch_age when the route lane reached it"),
                )
                .await?;
            }
            other => return Ok(other),
        }
    }
}

pub async fn finish(db: &LocalDb, id: i64, state: &str, note: Option<&str>) -> DbResult<()> {
    db.execute(
        "UPDATE fact_log SET route_state=?2, route_note=?3 WHERE id=?1",
        params![id, state.to_string(), note.map(str::to_owned)],
    )
    .await?;
    Ok(())
}

/// What a restart found and settled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Recovery {
    pub interrupted: u64,
    pub stale: u64,
    /// Coalesced wakes that were mid-delivery and are never re-sent.
    pub interrupted_wakes: u64,
}

/// Settle rows a previous process left mid-flight. Both lanes are at-most-once:
/// a row that was `running` becomes `interrupted` and never re-fires, and so
/// does a row the index lane had claimed (`indexed_at` set) but not settled,
/// since its thread wakes may already be partly delivered and no claim query
/// would ever select it again. Unindexed rows older than `stale_before_ms`
/// become `stale` without being indexed. A coalesced wake claimed for delivery
/// but never settled becomes `interrupted` for the same reason; a pending wake
/// is untouched and goes out on the worker's next tick, count intact.
pub async fn recover_on_start(
    db: &LocalDb,
    now_ms: i64,
    stale_before_ms: i64,
) -> DbResult<Recovery> {
    let dispatching = db
        .execute(
            "UPDATE fact_log SET route_state='interrupted', route_note='the runner stopped while this row was dispatching' WHERE route_state='running'",
            (),
        )
        .await?;
    let indexing = db
        .execute(
            "UPDATE fact_log SET route_state='interrupted', route_note='the runner stopped while this row was being indexed' WHERE indexed_at IS NOT NULL AND route_state='pending'",
            (),
        )
        .await?;
    let interrupted = dispatching + indexing;
    let stale = db
        .execute(
            "UPDATE fact_log SET route_state='stale', indexed_at=?1, route_note='older than max_dispatch_age when the runner started' WHERE indexed_at IS NULL AND minted_at<?2",
            params![now_ms, stale_before_ms],
        )
        .await?;
    let interrupted_wakes = db
        .execute(
            "UPDATE fact_log_wakes SET delivered_action='interrupted' WHERE delivered_action='delivering'",
            (),
        )
        .await?;
    Ok(Recovery {
        interrupted,
        stale,
        interrupted_wakes,
    })
}

/// Which rows a read covers. A project view holds that project's rows only; the
/// workspace view holds every row, including the project-less ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactScope {
    Project(String),
    Workspace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactFilter {
    pub scope: FactScope,
    pub source: Option<String>,
    pub since_ms: Option<i64>,
    /// Inclusive row id bounds: the range a coalesced wake cites.
    pub from_id: Option<i64>,
    pub to_id: Option<i64>,
    pub limit: i64,
    pub offset: i64,
}

pub async fn list_facts(db: &LocalDb, filter: FactFilter) -> DbResult<Vec<FactRow>> {
    let (project, scoped) = match filter.scope {
        FactScope::Project(project) => (Some(project), 1_i64),
        FactScope::Workspace => (None, 0_i64),
    };
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM fact_log WHERE (?1=0 OR project_id=?2) AND (?3 IS NULL OR source=?3) AND (?4 IS NULL OR minted_at>=?4) AND (?7 IS NULL OR id>=?7) AND (?8 IS NULL OR id<=?8) ORDER BY id DESC LIMIT ?5 OFFSET ?6"
        ),
        params![
            scoped,
            project,
            filter.source,
            filter.since_ms,
            filter.limit,
            filter.offset,
            filter.from_id,
            filter.to_id
        ],
        from_row,
    )
    .await
}

pub async fn get_fact(db: &LocalDb, id: i64) -> DbResult<Option<FactRow>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM fact_log WHERE id=?1"),
        (id,),
        from_row,
    )
    .await
}

fn scope_params(scope: &FactScope) -> (i64, Option<String>) {
    match scope {
        FactScope::Project(project) => (1, Some(project.clone())),
        FactScope::Workspace => (0, None),
    }
}

/// The newest producer-minted row of each source in a scope: the route
/// Playground's sampled facts. Route-generated rows are excluded because a
/// route cannot fire on them, so they would be a misleading sample.
pub async fn latest_per_source(db: &LocalDb, scope: &FactScope) -> DbResult<Vec<FactRow>> {
    let (scoped, project) = scope_params(scope);
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM fact_log WHERE id IN (SELECT MAX(id) FROM fact_log WHERE route_provenance IS NULL AND (?1=0 OR project_id=?2) GROUP BY source) ORDER BY minted_at DESC, source"
        ),
        params![scoped, project],
        from_row,
    )
    .await
}

pub async fn latest_for_source(
    db: &LocalDb,
    scope: &FactScope,
    source: &str,
) -> DbResult<Option<FactRow>> {
    let (scoped, project) = scope_params(scope);
    db.query_opt(
        format!(
            "SELECT {COLUMNS} FROM fact_log WHERE route_provenance IS NULL AND (?1=0 OR project_id=?2) AND source=?3 ORDER BY id DESC LIMIT 1"
        ),
        params![scoped, project, source.to_string()],
        from_row,
    )
    .await
}

/// Which sources one prune pass covers. A source with its own retention is
/// pruned by name; the default retention covers every other source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruneSelector {
    Source(String),
    Except(Vec<String>),
}

/// The `source` predicate a selector stands for, and the source it binds as
/// `?3` (if any).
fn selector_sql(selector: &PruneSelector) -> (String, Vec<String>) {
    match selector {
        PruneSelector::Source(source) => ("source=?3".to_string(), vec![source.clone()]),
        PruneSelector::Except(sources) if sources.is_empty() => ("1=1".to_string(), Vec::new()),
        PruneSelector::Except(sources) => {
            let quoted = sources
                .iter()
                .map(|source| format!("'{}'", source.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            (format!("source NOT IN ({quoted})"), Vec::new())
        }
    }
}

async fn delete_batch(
    db: &LocalDb,
    sql: String,
    sources: &[String],
    older_than_ms: i64,
    batch: i64,
) -> DbResult<u64> {
    match sources.first() {
        Some(source) => {
            db.execute(sql, params![older_than_ms, batch, source.clone()])
                .await
        }
        None => db.execute(sql, params![older_than_ms, batch]).await,
    }
}

/// Delete up to `batch` rows minted before `older_than_ms`. A row the route
/// lane still owns (`queued`, `running`) is never pruned out from under it.
pub async fn prune(
    db: &LocalDb,
    selector: &PruneSelector,
    older_than_ms: i64,
    batch: i64,
) -> DbResult<u64> {
    let (source_sql, sources) = selector_sql(selector);
    let sql = format!(
        "DELETE FROM fact_log WHERE id IN (SELECT id FROM fact_log WHERE {source_sql} AND minted_at<?1 AND route_state NOT IN ('queued','running') ORDER BY id LIMIT ?2)"
    );
    delete_batch(db, sql, &sources, older_than_ms, batch).await
}

/// Delete up to `batch` settled coalesced wakes opened before
/// `older_than_ms`, under the same per-source retention as their rows. A
/// pending wake is never pruned: it still owes a delivery.
pub async fn prune_wakes(
    db: &LocalDb,
    selector: &PruneSelector,
    older_than_ms: i64,
    batch: i64,
) -> DbResult<u64> {
    let (source_sql, sources) = selector_sql(selector);
    let sql = format!(
        "DELETE FROM fact_log_wakes WHERE id IN (SELECT id FROM fact_log_wakes WHERE {source_sql} AND opened_at<?1 AND delivered_at IS NOT NULL ORDER BY id LIMIT ?2)"
    );
    delete_batch(db, sql, &sources, older_than_ms, batch).await
}

/// One coalesced thread wake: every row of one source that matched one
/// thread's standing triggers within one settle window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactWake {
    pub id: i64,
    pub job_id: String,
    pub source: String,
    /// The subscription the wake resolves to: the first matching clause's
    /// `source_ref`.
    pub clause_ref: String,
    /// The rows' shared project, or `None` when they are project-less or span
    /// projects; either way the workspace view is the one that holds them all.
    pub project_id: Option<String>,
    pub first_row_id: i64,
    pub last_row_id: i64,
    pub count: i64,
    pub opened_at: i64,
    pub deliver_after: i64,
    /// `None` while pending.
    pub delivered_at: Option<i64>,
    /// `delivering` while claimed, then the wake route's outcome
    /// (`delivered`, `suppressed`, `dropped`, `failed: …`) or `interrupted`.
    pub delivered_action: Option<String>,
}

const WAKE_COLUMNS: &str = "id,job_id,source,clause_ref,project_id,first_row_id,last_row_id,count,opened_at,deliver_after,delivered_at,delivered_action";

fn wake_from_row(row: &crate::turso::Row) -> DbResult<FactWake> {
    Ok(FactWake {
        id: row.i64(0)?,
        job_id: row.text(1)?,
        source: row.text(2)?,
        clause_ref: row.text(3)?,
        project_id: row.opt_text(4)?,
        first_row_id: row.i64(5)?,
        last_row_id: row.i64(6)?,
        count: row.i64(7)?,
        opened_at: row.i64(8)?,
        deliver_after: row.i64(9)?,
        delivered_at: row.opt_i64(10)?,
        delivered_action: row.opt_text(11)?,
    })
}

/// One matched row, to be folded into its thread's pending wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeMatch {
    pub job_id: String,
    pub source: String,
    pub clause_ref: String,
    pub project_id: Option<String>,
    pub row_id: i64,
    /// The row's `minted_at`. Windows are measured on mint time, so a backlog
    /// the index lane works through late coalesces the way it would have live.
    pub at_ms: i64,
}

/// How long a pending wake waits for more rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeWindow {
    /// Quiet time after the latest row before the wake is due (debounce).
    pub settle_ms: i64,
    /// The cap: a wake is due no later than this long after its first row,
    /// so a continuous stream still delivers once per window.
    pub max_window_ms: i64,
}

/// Fold one matched row into its (job, source) pending wake, opening one when
/// none is still open, and return the wake's id. A wake is open while its
/// `deliver_after` lies beyond the row's time; each joining row pushes
/// `deliver_after` to `at + settle` but never past `opened_at + max_window`.
pub async fn coalesce_wake(db: &LocalDb, matched: WakeMatch, window: WakeWindow) -> DbResult<i64> {
    db.write(move |conn| {
        let matched = matched.clone();
        Box::pin(async move {
            let open = {
                let mut rows = conn
                    .query(
                        "SELECT id FROM fact_log_wakes WHERE job_id=?1 AND source=?2 AND delivered_at IS NULL AND deliver_after>?3 ORDER BY id DESC LIMIT 1",
                        params![matched.job_id.clone(), matched.source.clone(), matched.at_ms],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Some(row.i64(0)?),
                    None => None,
                }
            };
            if let Some(id) = open {
                conn.execute(
                    "UPDATE fact_log_wakes SET count=count+1, first_row_id=MIN(first_row_id,?2), last_row_id=MAX(last_row_id,?2), deliver_after=MIN(MAX(deliver_after,?3+?4),opened_at+?5), project_id=CASE WHEN project_id=?6 THEN project_id ELSE NULL END WHERE id=?1",
                    params![
                        id,
                        matched.row_id,
                        matched.at_ms,
                        window.settle_ms,
                        window.max_window_ms,
                        matched.project_id.clone()
                    ],
                )
                .await?;
                return Ok(id);
            }
            let mut rows = conn
                .query(
                    "INSERT INTO fact_log_wakes (job_id,source,clause_ref,project_id,first_row_id,last_row_id,count,opened_at,deliver_after) VALUES (?1,?2,?3,?4,?5,?5,1,?6,?6+MIN(?7,?8)) RETURNING id",
                    params![
                        matched.job_id.clone(),
                        matched.source.clone(),
                        matched.clause_ref.clone(),
                        matched.project_id.clone(),
                        matched.row_id,
                        matched.at_ms,
                        window.settle_ms,
                        window.max_window_ms
                    ],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.i64(0)?),
                None => Err(DbError::internal("INSERT RETURNING produced no row")),
            }
        })
    })
    .await
}

/// Claim the oldest wake due by `due_by_ms` for delivery, stamping it
/// `delivering` at `now_ms`. One conditional UPDATE, so a wake is claimed once.
pub async fn claim_due_wake(
    db: &LocalDb,
    due_by_ms: i64,
    now_ms: i64,
) -> DbResult<Option<FactWake>> {
    let sql = format!(
        "UPDATE fact_log_wakes SET delivered_at=?2, delivered_action='delivering' WHERE id=(SELECT MIN(id) FROM fact_log_wakes WHERE delivered_at IS NULL AND deliver_after<=?1) AND delivered_at IS NULL RETURNING {WAKE_COLUMNS}"
    );
    db.write(move |conn| {
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn.query(&sql, params![due_by_ms, now_ms]).await?;
            match rows.next().await? {
                Some(row) => Ok(Some(wake_from_row(&row)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// Record a claimed wake's outcome.
pub async fn finish_wake(db: &LocalDb, id: i64, action: &str) -> DbResult<()> {
    db.execute(
        "UPDATE fact_log_wakes SET delivered_action=?2 WHERE id=?1",
        params![id, action.to_string()],
    )
    .await?;
    Ok(())
}

/// The wakes with these ids, in id order. Ids a prune removed are absent.
pub async fn get_wakes(db: &LocalDb, ids: &[i64]) -> DbResult<Vec<FactWake>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let list = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
    db.query_all(
        format!("SELECT {WAKE_COLUMNS} FROM fact_log_wakes WHERE id IN ({list}) ORDER BY id"),
        (),
        wake_from_row,
    )
    .await
}

pub async fn count_facts(db: &LocalDb) -> DbResult<i64> {
    db.query_opt_i64("SELECT COUNT(*) FROM fact_log", ())
        .await?
        .ok_or_else(|| DbError::internal("COUNT returned no row"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(source: &str, identity: &str, record_ref: &str, minted_at: i64) -> NewFact {
        NewFact {
            project_id: Some("p".into()),
            source: source.into(),
            identity: identity.into(),
            record_ref: record_ref.into(),
            record_id: None,
            fields_json: "{}".into(),
            summary: None,
            presence: "away".into(),
            actor_job_id: None,
            origin_principal_json: None,
            origin_appearance_json: None,
            route_provenance: None,
            minted_at,
        }
    }

    #[tokio::test]
    async fn the_fact_log_replaces_the_route_sample_table() {
        let db = crate::storage::migrated_test_db("fact-log-replaces-samples.db").await;
        let tables = |name: &'static str| {
            let db = &db;
            async move {
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    (name,),
                )
                .await
                .unwrap()
            }
        };
        assert_eq!(tables("fact_log").await, Some(1));
        assert_eq!(tables("route_fact_samples").await, Some(0));
    }

    #[tokio::test]
    async fn unique_dedupe_absorbs_a_redelivery() {
        let db = crate::storage::migrated_test_db("fact-log-unique.db").await;
        let first = insert_fact(
            &db,
            fact("github_comment", "c:1", "u", 1),
            LogDedupe::Unique,
        )
        .await
        .unwrap();
        let again = insert_fact(
            &db,
            fact("github_comment", "c:1", "u", 2),
            LogDedupe::Unique,
        )
        .await
        .unwrap();
        let other = insert_fact(
            &db,
            fact("github_comment", "c:2", "u", 3),
            LogDedupe::Unique,
        )
        .await
        .unwrap();
        assert!(first.is_some());
        assert_eq!(again, None);
        assert!(other > first);
        assert_eq!(count_facts(&db).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn transition_dedupe_records_each_change_of_state() {
        let db = crate::storage::migrated_test_db("fact-log-transition.db").await;
        let mint = |identity: &str, minted_at| {
            insert_fact(
                &db,
                fact("attention", identity, "cairn://p/cairn/1", minted_at),
                LogDedupe::Transition,
            )
        };
        assert!(mint("A", 1).await.unwrap().is_some());
        assert!(mint("A", 2).await.unwrap().is_none(), "a re-poll is no row");
        assert!(mint("B", 3).await.unwrap().is_some());
        assert!(
            mint("A", 4).await.unwrap().is_some(),
            "A again is a transition"
        );
        assert_eq!(count_facts(&db).await.unwrap(), 3);
        // Another issue's state is its own subject.
        assert!(insert_fact(
            &db,
            fact("attention", "A", "cairn://p/cairn/2", 5),
            LogDedupe::Transition
        )
        .await
        .unwrap()
        .is_some());
    }

    #[tokio::test]
    async fn lanes_claim_rows_in_order_and_exactly_once() {
        let db = crate::storage::migrated_test_db("fact-log-claims.db").await;
        for n in 0..3 {
            insert_fact(&db, fact("s", &format!("{n}"), "r", n), LogDedupe::Unique)
                .await
                .unwrap();
        }
        let first = claim_for_index(&db, 10).await.unwrap().unwrap();
        assert_eq!((first.identity.as_str(), first.indexed_at), ("0", Some(10)));
        set_index_outcome(&db, first.id, route_state::QUEUED, None, Some("[]"))
            .await
            .unwrap();
        let second = claim_for_index(&db, 11).await.unwrap().unwrap();
        set_index_outcome(&db, second.id, route_state::QUEUED, None, None)
            .await
            .unwrap();
        let third = claim_for_index(&db, 12).await.unwrap().unwrap();
        set_index_outcome(&db, third.id, route_state::UNMATCHED, None, None)
            .await
            .unwrap();
        assert_eq!(claim_for_index(&db, 13).await.unwrap(), None);

        // The first queued row is too old by the time the route lane reaches
        // it, so it goes stale and the second is claimed.
        let running = claim_next_queued(&db, 1).await.unwrap().unwrap();
        assert_eq!(running.id, second.id);
        assert_eq!(running.route_state, route_state::RUNNING);
        assert_eq!(
            get_fact(&db, first.id).await.unwrap().unwrap().route_state,
            route_state::STALE
        );
        assert_eq!(claim_next_queued(&db, 0).await.unwrap(), None);
        finish(&db, running.id, route_state::FAILED, Some("timeout"))
            .await
            .unwrap();
        let finished = get_fact(&db, running.id).await.unwrap().unwrap();
        assert_eq!(
            (
                finished.route_state.as_str(),
                finished.route_note.as_deref()
            ),
            (route_state::FAILED, Some("timeout"))
        );
    }

    #[tokio::test]
    async fn restart_interrupts_running_rows_and_stales_old_unindexed_ones() {
        let db = crate::storage::migrated_test_db("fact-log-recover.db").await;
        let running = insert_fact(&db, fact("s", "run", "r", 100), LogDedupe::Unique)
            .await
            .unwrap()
            .unwrap();
        set_index_outcome(&db, running, route_state::QUEUED, None, None)
            .await
            .unwrap();
        claim_next_queued(&db, 0).await.unwrap().unwrap();
        claim_for_index(&db, 100).await.unwrap().unwrap();
        // Claimed by the index lane, then the runner stopped before settling it.
        let indexing = insert_fact(&db, fact("s", "indexing", "r", 300), LogDedupe::Unique)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            claim_for_index(&db, 300).await.unwrap().unwrap().id,
            indexing
        );
        let old = insert_fact(&db, fact("s", "old", "r", 1), LogDedupe::Unique)
            .await
            .unwrap()
            .unwrap();
        let fresh = insert_fact(&db, fact("s", "fresh", "r", 500), LogDedupe::Unique)
            .await
            .unwrap()
            .unwrap();
        let recovery = recover_on_start(&db, 600, 50).await.unwrap();
        assert_eq!(
            recovery,
            Recovery {
                interrupted: 2,
                stale: 1,
                interrupted_wakes: 0
            }
        );
        let db = &db;
        let state = |id| async move { get_fact(db, id).await.unwrap().unwrap().route_state };
        assert_eq!(state(running).await, route_state::INTERRUPTED);
        assert_eq!(state(indexing).await, route_state::INTERRUPTED);
        assert_eq!(state(old).await, route_state::STALE);
        assert_eq!(state(fresh).await, route_state::PENDING);
    }

    #[tokio::test]
    async fn scoped_reads_and_samples() {
        let db = crate::storage::migrated_test_db("fact-log-scopes.db").await;
        let mut workspace = fact("attention", "w", "r", 1);
        workspace.project_id = None;
        insert_fact(&db, workspace, LogDedupe::Unique)
            .await
            .unwrap();
        insert_fact(&db, fact("attention", "p1", "r", 2), LogDedupe::Unique)
            .await
            .unwrap();
        insert_fact(&db, fact("file_changed", "f", "r", 3), LogDedupe::Unique)
            .await
            .unwrap();
        let mut generated = fact("file_changed", "g", "r", 4);
        generated.route_provenance = Some("route".into());
        insert_fact(&db, generated, LogDedupe::Unique)
            .await
            .unwrap();

        let filter = |scope, source: Option<&str>| FactFilter {
            scope,
            source: source.map(str::to_owned),
            since_ms: None,
            from_id: None,
            to_id: None,
            limit: 50,
            offset: 0,
        };
        let project = list_facts(&db, filter(FactScope::Project("p".into()), None))
            .await
            .unwrap();
        assert_eq!(
            project
                .iter()
                .map(|r| r.identity.as_str())
                .collect::<Vec<_>>(),
            vec!["g", "f", "p1"],
            "newest first, and the project-less row is not a project event"
        );
        let workspace_rows = list_facts(&db, filter(FactScope::Workspace, Some("attention")))
            .await
            .unwrap();
        assert_eq!(workspace_rows.len(), 2);
        let ranged = list_facts(
            &db,
            FactFilter {
                from_id: Some(project[2].id),
                to_id: Some(project[1].id),
                ..filter(FactScope::Workspace, None)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            ranged
                .iter()
                .map(|r| r.identity.as_str())
                .collect::<Vec<_>>(),
            vec!["f", "p1"],
            "from/to bound the ids inclusively"
        );

        let samples = latest_per_source(&db, &FactScope::Project("p".into()))
            .await
            .unwrap();
        let mut sampled = samples
            .iter()
            .map(|r| r.identity.as_str())
            .collect::<Vec<_>>();
        sampled.sort_unstable();
        assert_eq!(
            sampled,
            vec!["f", "p1"],
            "route-generated rows are not samples"
        );
        assert_eq!(
            latest_for_source(&db, &FactScope::Workspace, "attention")
                .await
                .unwrap()
                .unwrap()
                .identity,
            "p1"
        );
    }

    #[tokio::test]
    async fn prune_spares_rows_the_route_lane_owns() {
        let db = crate::storage::migrated_test_db("fact-log-prune.db").await;
        let queued = insert_fact(&db, fact("a", "q", "r", 1), LogDedupe::Unique)
            .await
            .unwrap()
            .unwrap();
        set_index_outcome(&db, queued, route_state::QUEUED, None, None)
            .await
            .unwrap();
        insert_fact(&db, fact("a", "old", "r", 2), LogDedupe::Unique)
            .await
            .unwrap();
        insert_fact(&db, fact("b", "old-b", "r", 2), LogDedupe::Unique)
            .await
            .unwrap();
        insert_fact(&db, fact("a", "new", "r", 100), LogDedupe::Unique)
            .await
            .unwrap();
        let removed = prune(&db, &PruneSelector::Source("a".into()), 50, 1000)
            .await
            .unwrap();
        assert_eq!(removed, 1, "only the settled old row of source a");
        let removed = prune(&db, &PruneSelector::Except(vec!["a".into()]), 50, 1000)
            .await
            .unwrap();
        assert_eq!(
            removed, 1,
            "the default retention covers every other source"
        );
        assert_eq!(count_facts(&db).await.unwrap(), 2);
    }

    fn matched(job: &str, source: &str, row_id: i64, at_ms: i64) -> WakeMatch {
        WakeMatch {
            job_id: job.into(),
            source: source.into(),
            clause_ref: format!("{{\"fact\":\"{source}\"}}"),
            project_id: Some("p".into()),
            row_id,
            at_ms,
        }
    }

    const WINDOW: WakeWindow = WakeWindow {
        settle_ms: 30,
        max_window_ms: 100,
    };

    #[tokio::test]
    async fn rows_within_the_settle_window_fold_into_one_wake_per_job_and_source() {
        let db = crate::storage::migrated_test_db("fact-log-coalesce.db").await;
        let first = coalesce_wake(&db, matched("j", "s", 1, 0), WINDOW)
            .await
            .unwrap();
        assert_eq!(
            coalesce_wake(&db, matched("j", "s", 2, 20), WINDOW)
                .await
                .unwrap(),
            first
        );
        let other_source = coalesce_wake(&db, matched("j", "t", 3, 20), WINDOW)
            .await
            .unwrap();
        let other_job = coalesce_wake(&db, matched("k", "s", 4, 20), WINDOW)
            .await
            .unwrap();
        assert_ne!(other_source, first);
        assert_ne!(other_job, first);
        let mut elsewhere = matched("j", "s", 5, 40);
        elsewhere.project_id = Some("q".into());
        coalesce_wake(&db, elsewhere, WINDOW).await.unwrap();

        let wake = &get_wakes(&db, &[first]).await.unwrap()[0];
        assert_eq!((wake.count, wake.first_row_id, wake.last_row_id), (3, 1, 5));
        assert_eq!(
            wake.deliver_after, 70,
            "debounced to the latest row + settle"
        );
        assert_eq!(
            wake.project_id, None,
            "rows spanning projects cite the workspace view"
        );

        assert_eq!(
            claim_due_wake(&db, 49, 1).await.unwrap(),
            None,
            "nothing is due before the earliest settle"
        );
        let due = claim_due_wake(&db, 70, 1).await.unwrap().unwrap();
        assert_eq!(due.id, first);
        assert_eq!(due.delivered_action.as_deref(), Some("delivering"));
        // A row after the window closed opens a fresh wake.
        let next = coalesce_wake(&db, matched("j", "s", 6, 75), WINDOW)
            .await
            .unwrap();
        assert_ne!(next, first);
    }

    #[tokio::test]
    async fn a_wake_is_due_no_later_than_its_max_window() {
        let db = crate::storage::migrated_test_db("fact-log-coalesce-cap.db").await;
        let mut ids = Vec::new();
        for (row, at) in (0..8).map(|n| (n + 1, n * 20)) {
            ids.push(
                coalesce_wake(&db, matched("j", "s", row, at), WINDOW)
                    .await
                    .unwrap(),
            );
        }
        let wakes = get_wakes(&db, &ids).await.unwrap();
        let wakes = wakes
            .iter()
            .map(|wake| (wake.count, wake.opened_at, wake.deliver_after))
            .collect::<Vec<_>>();
        assert_eq!(
            wakes,
            vec![(5, 0, 100), (3, 100, 170)],
            "rows at 0..80 fill the first window, capped at 100; 100 opens the next"
        );
    }

    #[tokio::test]
    async fn restart_interrupts_a_wake_caught_mid_delivery_and_keeps_pending_ones() {
        let db = crate::storage::migrated_test_db("fact-log-wake-recover.db").await;
        let claimed = coalesce_wake(&db, matched("j", "s", 1, 0), WINDOW)
            .await
            .unwrap();
        claim_due_wake(&db, 1000, 1000).await.unwrap().unwrap();
        let pending = coalesce_wake(&db, matched("j", "s", 2, 900), WINDOW)
            .await
            .unwrap();
        let recovery = recover_on_start(&db, 1000, 0).await.unwrap();
        assert_eq!(recovery.interrupted_wakes, 1);
        let wakes = get_wakes(&db, &[claimed, pending]).await.unwrap();
        assert_eq!(wakes[0].delivered_action.as_deref(), Some("interrupted"));
        assert_eq!(
            (wakes[1].delivered_at, wakes[1].count),
            (None, 1),
            "a pending wake survives a restart untouched"
        );
        assert_eq!(
            claim_due_wake(&db, 5000, 5000).await.unwrap().map(|w| w.id),
            Some(pending),
            "only the pending wake is still owed"
        );
        assert_eq!(claim_due_wake(&db, 5000, 5000).await.unwrap(), None);
    }
}
