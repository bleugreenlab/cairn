//! SQL for Tier A token-economics analytics.
//!
//! Everything hangs off the [`TOKEN_EVENTS_CTE`]: one row per top-level turn
//! event carrying its per-backend billable token total, broken out by
//! component, with project / model / role / backend joined in. The backend rule
//! mirrors `models::context_tokens`: Claude bills `assistant` events
//! (`input + cache_create + cache_read + output`); codex bills `result%` events
//! (`input + output`, since codex `input` already subsumes cache).
//!
//! These functions return raw component aggregates; pricing and role
//! normalization happen in the parent module so the price table stays in one
//! place.

use turso::{params, Connection, Value};

use super::tool_extract::{self, ToolInvocation};
use super::types::{Bucket, Scope, TimeRange, ToolBackfillSummary, TopTargetRow};
use crate::models::Event;
use crate::storage::{DbResult, LocalDb, RowExt};

// --- Tier A: token/cost rollup (incremental fold + read queries) ---
//
// The read paths aggregate the app-maintained `token_rollup` table -- one row
// per (UTC-hour x run x resolved dimensions) -- instead of rescanning the
// ~780k-row `events` table on every analytics page load. ONE hour-grain cache
// answers every time query: day/week/month floor an already-hour-floored epoch
// (exact, since an hour nests wholly inside its day/week/month), and the 24h
// window reads the rollup directly (`bucket_expr(Hour)` on an hour-floored
// column is the identity floor). `fold_token_rollup` keeps it fresh
// incrementally, watermark-gated per run, mirroring the tool-invocation rollup
// (migration 0067). The retained `*_live` event-scan queries further below are
// the `#[cfg(test)]` oracle the rollup is proven equal to -- no longer a
// production read path.

/// Backend resolved per event, mirroring `models::context_tokens`: backend
/// identity lives on `sessions.backend` (the vestigial `runs.backend` column was
/// dropped in migration 0081), defaulting to claude when no session resolves.
const BACKEND_EXPR: &str = "LOWER(COALESCE(s.backend, 'claude'))";

/// A billable top-level turn event: not a sub-agent event, and the backend's
/// billing event type (codex bills `result%`, everyone else `assistant`).
const BILLABLE_PREDICATE: &str = "e.parent_tool_use_id IS NULL
        AND (
            (LOWER(COALESCE(s.backend, 'claude')) = 'codex'
                AND e.event_type LIKE 'result%')
         OR (LOWER(COALESCE(s.backend, 'claude')) <> 'codex'
                AND e.event_type = 'assistant')
        )";

/// The backend-shaped billable token total for one event (codex subsumes cache
/// into input, so it never re-adds cache).
const BILLABLE_TOTAL: &str = "CASE
            WHEN LOWER(COALESCE(s.backend, 'claude')) = 'codex'
                THEN COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)
            ELSE COALESCE(e.input_tokens, 0) + COALESCE(e.cache_create_tokens, 0)
                 + COALESCE(e.cache_read_tokens, 0) + COALESCE(e.output_tokens, 0)
        END";

/// Runs whose newest rollup-relevant event (a billable token event for either
/// backend rule, or any event carrying a real `cost_usd`) is past the rollup
/// watermark, or which have never been folded.
const NEEDING_FOLD_SQL: &str = "SELECT e.run_id
    FROM events e
    LEFT JOIN token_rollup_runs w ON w.run_id = e.run_id
    WHERE (e.event_type = 'assistant' OR e.event_type LIKE 'result%' OR e.cost_usd IS NOT NULL)
    GROUP BY e.run_id
    HAVING MAX(e.created_at) > COALESCE(MAX(w.processed_through), -1)";

/// Scope/range predicate over the `token_rollup` alias `tr`. Bound params are
/// `?1` = project id, `?2` = start, `?3` = end; each NULL disables that filter.
/// The range filters the stored UTC-hour floor, so it matches the live per-event
/// scan exactly when the window start is hour-aligned -- which the analytics
/// range selector always is (see `AnalyticsSection`).
const ROLLUP_SCOPE_RANGE: &str = "(?1 IS NULL OR tr.project_id = ?1)
        AND (?2 IS NULL OR tr.bucket_start >= ?2)
        AND (?3 IS NULL OR tr.bucket_start < ?3)";

/// Sibling of [`ROLLUP_SCOPE_RANGE`] bound to params `?4`/`?5`/`?6` so the token
/// and cost scans can carry independent (identical) filters in one statement.
const ROLLUP_COST_SCOPE_RANGE: &str = "(?4 IS NULL OR tr.project_id = ?4)
        AND (?5 IS NULL OR tr.bucket_start >= ?5)
        AND (?6 IS NULL OR tr.bucket_start < ?6)";

/// Positional params for [`ROLLUP_SCOPE_RANGE`] (and the retained
/// `#[cfg(test)]` [`SCOPE_RANGE`] oracle, which has the same `?1..?3` shape).
fn scope_range_params(scope: &Scope, range: &TimeRange) -> Vec<Value> {
    vec![
        scope.project_id.clone().map_or(Value::Null, Value::Text),
        range.start.map_or(Value::Null, Value::Integer),
        range.end.map_or(Value::Null, Value::Integer),
    ]
}

/// `scope_range_params` repeated twice: `?1..?3` bind the token-side filter and
/// `?4..?6` bind the cost-side filter.
fn scope_range_params_doubled(scope: &Scope, range: &TimeRange) -> Vec<Value> {
    let mut params = scope_range_params(scope, range);
    params.extend(scope_range_params(scope, range));
    params
}

/// The per-run `INSERT ... SELECT` that folds one run's events into
/// `token_rollup`. Bound param `?1` is the run id. Token measures are gated on
/// the billable predicate; `exact_cost`/`exact_cost_count` range over every
/// event carrying a real `cost_usd` -- a broader population, since the metered
/// settlement event is not itself a billable token event. The synthetic `id`
/// is unique per grouped row because all of its components are functionally
/// determined by the run (fixed at `?1`) plus the grouped hour/session/model/
/// backend/agent dimensions.
fn rollup_insert_sql() -> String {
    format!(
        "INSERT INTO token_rollup (
            id, bucket_start, project_id, run_id, session_id, job_id, node_name, model,
            agent_config_id, backend, input_tokens, cache_read_tokens,
            cache_create_tokens, output_tokens, thinking_tokens, billable_tokens,
            token_event_count, exact_cost, exact_cost_count
        )
        SELECT
            CAST((e.created_at / 3600) * 3600 AS TEXT) || ':' || e.run_id || ':' ||
                COALESCE(e.session_id, '') || ':' || COALESCE(j.model, '') || ':' ||
                {backend} || ':' || COALESCE(j.agent_config_id, '') AS id,
            (e.created_at / 3600) * 3600 AS bucket_start,
            COALESCE(r.project_id, j.project_id) AS project_id,
            e.run_id AS run_id,
            e.session_id AS session_id,
            COALESCE(r.job_id, s.job_id) AS job_id,
            j.node_name AS node_name,
            j.model AS model,
            j.agent_config_id AS agent_config_id,
            {backend} AS backend,
            SUM(CASE WHEN {billable} THEN COALESCE(e.input_tokens, 0) ELSE 0 END),
            SUM(CASE WHEN {billable} THEN COALESCE(e.cache_read_tokens, 0) ELSE 0 END),
            SUM(CASE WHEN {billable} THEN COALESCE(e.cache_create_tokens, 0) ELSE 0 END),
            SUM(CASE WHEN {billable} THEN COALESCE(e.output_tokens, 0) ELSE 0 END),
            SUM(CASE WHEN {billable} THEN COALESCE(e.thinking_tokens, 0) ELSE 0 END),
            SUM(CASE WHEN {billable} THEN {billable_total} ELSE 0 END),
            SUM(CASE WHEN {billable} THEN 1 ELSE 0 END) AS token_event_count,
            COALESCE(SUM(e.cost_usd), 0.0) AS exact_cost,
            SUM(CASE WHEN e.cost_usd IS NOT NULL THEN 1 ELSE 0 END) AS exact_cost_count
        FROM events e
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = e.session_id
        LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
        WHERE e.run_id = ?1
          AND (({billable}) OR e.cost_usd IS NOT NULL)
        GROUP BY (e.created_at / 3600) * 3600,
                 COALESCE(r.project_id, j.project_id),
                 e.run_id,
                 e.session_id,
                 COALESCE(r.job_id, s.job_id),
                 j.node_name,
                 j.model,
                 j.agent_config_id,
                 {backend}",
        backend = BACKEND_EXPR,
        billable = BILLABLE_PREDICATE,
        billable_total = BILLABLE_TOTAL,
    )
}

/// Incrementally fold new events into `token_rollup`, watermark-gated per run.
///
/// Called at the top of every rollup-backed read query. `events` is append-only
/// and immutable, so the fold is forward-only: a run with new relevant events is
/// re-derived wholesale (delete-then-reinsert per run, idempotent), every other
/// run is skipped by its watermark.
///
/// Concurrency: an analytics page fires several rollup queries at once, so this
/// runs concurrently. The detect-delete-reinsert-advance cycle therefore happens
/// inside ONE serialized write transaction; a loser of the race re-evaluates an
/// empty needing-set against the now-advanced watermark and no-ops, instead of
/// double-folding and briefly exposing a partial rollup to a concurrent reader.
/// A read-only fast path skips the write transaction entirely when the watermark
/// is already current (the warm case).
pub(crate) async fn fold_token_rollup(db: &LocalDb) -> DbResult<()> {
    let needing = db
        .query_all(NEEDING_FOLD_SQL, (), |row| row.text(0))
        .await?;
    if needing.is_empty() {
        return Ok(());
    }
    let insert_sql = rollup_insert_sql();
    db.write(move |conn| {
        let insert_sql = insert_sql.clone();
        Box::pin(async move {
            // Re-detect inside the write txn so a concurrent fold that already
            // advanced the watermark collapses this call to a no-op.
            let mut runs: Vec<String> = Vec::new();
            {
                let mut rows = conn.query(NEEDING_FOLD_SQL, ()).await?;
                while let Some(row) = rows.next().await? {
                    runs.push(row.text(0)?);
                }
            }
            let now = chrono::Utc::now().timestamp();
            for run_id in &runs {
                conn.execute(
                    "DELETE FROM token_rollup WHERE run_id = ?1",
                    params![run_id.as_str()],
                )
                .await?;
                conn.execute(&insert_sql, params![run_id.as_str()]).await?;
                conn.execute(
                    "INSERT INTO token_rollup_runs(run_id, processed_through, updated_at)
                     VALUES (
                        ?1,
                        COALESCE((SELECT MAX(e.created_at) FROM events e
                                  WHERE e.run_id = ?1
                                    AND (e.event_type = 'assistant'
                                         OR e.event_type LIKE 'result%'
                                         OR e.cost_usd IS NOT NULL)), -1),
                        ?2)
                     ON CONFLICT(run_id) DO UPDATE SET
                        processed_through = excluded.processed_through,
                        updated_at = excluded.updated_at",
                    params![run_id.as_str(), now],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

// --- Tier A: per-event incremental maintenance (hot insert path) ---

/// The single-event upsert that maintains `token_rollup` as each event lands.
///
/// The batch [`fold_token_rollup`] re-derives a whole run with a GROUP BY SUM;
/// this applies only THIS event's contribution to its one matching rollup row
/// (`ON CONFLICT(id) DO UPDATE` increments every measure), so the streaming path
/// stays O(1) in the event instead of O(run). It reuses the exact same backend
/// resolution, billable predicate, and billable total as the fold, so summing
/// the per-event increments is provably equal to the batch fold (the retained
/// `*_live` event-scan queries are the oracle that equality is tested against).
/// Bound param `?1` is the event id; the event is already inserted in the same
/// transaction, so this SELECT sees it.
fn token_rollup_event_upsert_sql() -> String {
    format!(
        "INSERT INTO token_rollup (
            id, bucket_start, project_id, run_id, session_id, job_id, node_name, model,
            agent_config_id, backend, input_tokens, cache_read_tokens,
            cache_create_tokens, output_tokens, thinking_tokens, billable_tokens,
            token_event_count, exact_cost, exact_cost_count
        )
        SELECT
            CAST((e.created_at / 3600) * 3600 AS TEXT) || ':' || e.run_id || ':' ||
                COALESCE(e.session_id, '') || ':' || COALESCE(j.model, '') || ':' ||
                {backend} || ':' || COALESCE(j.agent_config_id, '') AS id,
            (e.created_at / 3600) * 3600 AS bucket_start,
            COALESCE(r.project_id, j.project_id) AS project_id,
            e.run_id,
            e.session_id,
            COALESCE(r.job_id, s.job_id) AS job_id,
            j.node_name,
            j.model,
            j.agent_config_id,
            {backend} AS backend,
            CASE WHEN {billable} THEN COALESCE(e.input_tokens, 0) ELSE 0 END,
            CASE WHEN {billable} THEN COALESCE(e.cache_read_tokens, 0) ELSE 0 END,
            CASE WHEN {billable} THEN COALESCE(e.cache_create_tokens, 0) ELSE 0 END,
            CASE WHEN {billable} THEN COALESCE(e.output_tokens, 0) ELSE 0 END,
            CASE WHEN {billable} THEN COALESCE(e.thinking_tokens, 0) ELSE 0 END,
            CASE WHEN {billable} THEN {billable_total} ELSE 0 END,
            CASE WHEN {billable} THEN 1 ELSE 0 END,
            COALESCE(e.cost_usd, 0.0),
            CASE WHEN e.cost_usd IS NOT NULL THEN 1 ELSE 0 END
        FROM events e
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = e.session_id
        LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
        WHERE e.id = ?1
          AND (({billable}) OR e.cost_usd IS NOT NULL)
        ON CONFLICT(id) DO UPDATE SET
            input_tokens = input_tokens + excluded.input_tokens,
            cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,
            cache_create_tokens = cache_create_tokens + excluded.cache_create_tokens,
            output_tokens = output_tokens + excluded.output_tokens,
            thinking_tokens = thinking_tokens + excluded.thinking_tokens,
            billable_tokens = billable_tokens + excluded.billable_tokens,
            token_event_count = token_event_count + excluded.token_event_count,
            exact_cost = exact_cost + excluded.exact_cost,
            exact_cost_count = exact_cost_count + excluded.exact_cost_count",
        backend = BACKEND_EXPR,
        billable = BILLABLE_PREDICATE,
        billable_total = BILLABLE_TOTAL,
    )
}

/// Maintain both analytics rollups for one freshly-inserted event, inside the
/// event-insert transaction (see
/// `transcripts::stream_store::insert_event_conn`).
///
/// Idempotency comes from the caller gating this on a NEW row actually landing
/// (`count > 0`): a duplicate-id re-insert never re-applies the increment.
/// Tier A applies this event's token/cost contribution; Tier B indexes an
/// assistant event's tool uses or links a `tool_result`'s error. Both are O(1)
/// in the event. The conn already has the event row written, so Tier A's SELECT
/// by id sees it.
pub(crate) async fn maintain_rollups_on_insert(
    conn: &Connection,
    event_id: &str,
    run_id: &str,
    created_at: i64,
    event_type: &str,
    data: &str,
    cost_usd: Option<f64>,
) -> DbResult<()> {
    // Tier A: only billable token events (assistant / result%) or events
    // carrying a real cost contribute. The SELECT self-filters, but skipping the
    // join for the common non-contributing event keeps the hot path cheap.
    if event_type == "assistant" || event_type.starts_with("result") || cost_usd.is_some() {
        conn.execute(&token_rollup_event_upsert_sql(), params![event_id])
            .await?;
    }
    // Tier B: an assistant event's tool uses, or a tool_result's error link.
    if event_type == "assistant" || event_type == "tool_result" {
        maintain_tool_rollup_event(conn, event_id, run_id, created_at, event_type, data).await?;
    }
    Ok(())
}

/// Tier B per-event maintenance: index a single assistant event's tool uses
/// (`is_error = 0`), or update the matching row's `is_error` from a single
/// `tool_result` event. Shares `tool_extract`'s extraction/classification with
/// the run-level backfill, so the live rows match what a full re-backfill would
/// produce; the backfill remains the idempotent safety net that reconciles any
/// out-of-order results.
async fn maintain_tool_rollup_event(
    conn: &Connection,
    event_id: &str,
    run_id: &str,
    created_at: i64,
    event_type: &str,
    data: &str,
) -> DbResult<()> {
    if event_type == "assistant" {
        for row in tool_extract::extract_event_invocations(event_id, run_id, created_at, data) {
            conn.execute(
                "INSERT OR REPLACE INTO tool_invocations
                    (id, event_id, tool_use_id, run_id, ts, verb, tool_name,
                     target_scheme, target_kind, target_path, is_error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    row.id(),
                    row.event_id.as_str(),
                    row.tool_use_id.as_str(),
                    row.run_id.as_str(),
                    row.ts,
                    row.verb.as_str(),
                    row.tool_name.as_str(),
                    row.target_scheme.as_str(),
                    row.target_kind.as_str(),
                    row.target_path.as_deref(),
                    row.is_error as i64
                ],
            )
            .await?;
        }
    } else if event_type == "tool_result" {
        if let Some((tool_use_id, is_error)) = tool_extract::tool_result_error(data) {
            conn.execute(
                "UPDATE tool_invocations SET is_error = ?3
                 WHERE run_id = ?1 AND tool_use_id = ?2",
                params![run_id, tool_use_id.as_str(), is_error as i64],
            )
            .await?;
        }
    }
    Ok(())
}

/// Retained event-scan CTE: the `#[cfg(test)]` oracle the rollup read queries are
/// proven equal to (no longer a production read path). One row per billable
/// top-level turn event, referenced as `te` by every `*_live` query.
#[cfg(test)]
const TOKEN_EVENTS_CTE: &str = "\
token_events AS (
    SELECT
        e.run_id AS run_id,
        e.session_id AS session_id,
        e.created_at AS ts,
        COALESCE(r.project_id, j.project_id) AS project_id,
        j.id AS job_id,
        j.model AS model,
        j.node_name AS node_name,
        j.agent_config_id AS agent_config_id,
        LOWER(COALESCE(s.backend, 'claude')) AS backend,
        CASE
            WHEN LOWER(COALESCE(s.backend, 'claude')) = 'codex'
                THEN COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)
            ELSE COALESCE(e.input_tokens, 0) + COALESCE(e.cache_create_tokens, 0)
                 + COALESCE(e.cache_read_tokens, 0) + COALESCE(e.output_tokens, 0)
        END AS billable_tokens,
        COALESCE(e.input_tokens, 0) AS input_tokens,
        COALESCE(e.cache_read_tokens, 0) AS cache_read_tokens,
        COALESCE(e.cache_create_tokens, 0) AS cache_create_tokens,
        COALESCE(e.output_tokens, 0) AS output_tokens,
        COALESCE(e.thinking_tokens, 0) AS thinking_tokens
    FROM events e
    LEFT JOIN runs r ON r.id = e.run_id
    LEFT JOIN sessions s ON s.id = e.session_id
    LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
    WHERE e.parent_tool_use_id IS NULL
      AND (
            (LOWER(COALESCE(s.backend, 'claude')) = 'codex'
                AND e.event_type LIKE 'result%')
        OR (LOWER(COALESCE(s.backend, 'claude')) <> 'codex'
                AND e.event_type = 'assistant')
      )
)";

/// Scope/range predicate over the `token_events` alias `te` (the `#[cfg(test)]`
/// oracle).
#[cfg(test)]
const SCOPE_RANGE: &str = "(?1 IS NULL OR te.project_id = ?1)
        AND (?2 IS NULL OR te.ts >= ?2)
        AND (?3 IS NULL OR te.ts < ?3)";

/// Scope/range predicate over the `cost_events` alias `ce` using `?4`/`?5`/`?6`
/// (the `#[cfg(test)]` oracle).
#[cfg(test)]
const COST_SCOPE_RANGE: &str = "(?4 IS NULL OR ce.project_id = ?4)
        AND (?5 IS NULL OR ce.ts >= ?5)
        AND (?6 IS NULL OR ce.ts < ?6)";

/// Per-session billable total bucketed by the session's first event time.
#[derive(Debug, Clone)]
pub(crate) struct SessionBucketRow {
    pub bucket_start: i64,
    pub avg_tokens: f64,
    pub session_count: i64,
}

/// Bucketing on `MIN(bucket_start)` equals bucketing on `MIN(ts)` because a UTC
/// hour nests wholly inside its day, week, and month, so the hour floor of the
/// earliest event shares the earliest event's day/week/month bucket.
pub(crate) async fn avg_tokens_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<SessionBucketRow>> {
    let bucket_expr = bucket.bucket_expr("session_day");
    let sql = format!(
        "WITH sessions_agg AS (
            SELECT tr.session_id AS session_id,
                   SUM(tr.billable_tokens) AS total,
                   MIN(tr.bucket_start) AS session_day
            FROM token_rollup tr
            WHERE tr.session_id IS NOT NULL AND tr.token_event_count > 0 AND {filter}
            GROUP BY tr.session_id
        )
        SELECT {bucket_expr} AS bucket_start,
               AVG(total) AS avg_tokens,
               COUNT(*) AS session_count
        FROM sessions_agg
        GROUP BY bucket_start
        ORDER BY bucket_start",
        filter = ROLLUP_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(SessionBucketRow {
            bucket_start: row.i64(0)?,
            avg_tokens: row.opt_f64(1)?.unwrap_or(0.0),
            session_count: row.i64(2)?,
        })
    })
    .await
}

/// Event-scan variant of [`avg_tokens_per_session`]: the `#[cfg(test)]` oracle
/// the rollup read is proven equal to (no longer a production path).
#[cfg(test)]
pub(crate) async fn avg_tokens_per_session_live(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<SessionBucketRow>> {
    let bucket_expr = bucket.bucket_expr("session_ts");
    let sql = format!(
        "WITH {cte},
        sessions_agg AS (
            SELECT te.session_id AS session_id,
                   SUM(te.billable_tokens) AS total,
                   MIN(te.ts) AS session_ts
            FROM token_events te
            WHERE te.session_id IS NOT NULL AND {filter}
            GROUP BY te.session_id
        )
        SELECT {bucket_expr} AS bucket_start,
               AVG(total) AS avg_tokens,
               COUNT(*) AS session_count
        FROM sessions_agg
        GROUP BY bucket_start
        ORDER BY bucket_start",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(SessionBucketRow {
            bucket_start: row.i64(0)?,
            avg_tokens: row.opt_f64(1)?.unwrap_or(0.0),
            session_count: row.i64(2)?,
        })
    })
    .await
}

/// One PR-producing job: billable tokens, lines changed, and the token/cost
/// components needed to derive its real metered cost (for cost-per-line).
#[derive(Debug, Clone)]
pub(crate) struct LocRow {
    pub job_id: String,
    pub ts: i64,
    pub billable: i64,
    pub lines: i64,
    pub model: Option<String>,
    pub node_name: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    /// Sum of real `events.cost_usd` for this job (0 when none reported).
    pub exact_cost: f64,
    /// Count of this job's events that carried a real `cost_usd`.
    pub exact_cost_count: i64,
}

pub(crate) async fn tokens_per_loc(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<LocRow>> {
    // Lines shipped prefer GitHub-synced PR stats (`merge_requests.additions /
    // deletions`), but those are only populated when GitHub integration is
    // active and the PR has been fetched. Most local PR-producing jobs have NULL
    // there, so fall back to the always-present local `file_changes` rollup —
    // otherwise the metric silently drops every PR without fetched GH stats.
    //
    // No day range on `token_rollup` here: the range applies to `mr.opened_at`,
    // and a job's whole billable total is attributed to its merge request.
    let sql = "WITH job_tokens AS (
            SELECT tr.job_id AS job_id,
                   SUM(tr.billable_tokens) AS billable,
                   SUM(tr.input_tokens) AS input,
                   SUM(tr.cache_read_tokens) AS cache_read,
                   SUM(tr.cache_create_tokens) AS cache_create,
                   SUM(tr.output_tokens) AS output,
                   MAX(tr.model) AS model,
                   MAX(tr.node_name) AS node_name,
                   MAX(tr.backend) AS backend
            FROM token_rollup tr
            WHERE tr.job_id IS NOT NULL AND tr.token_event_count > 0
              AND (?1 IS NULL OR tr.project_id = ?1)
            GROUP BY tr.job_id
        ),
        job_cost AS (
            SELECT tr.job_id AS job_id,
                   SUM(tr.exact_cost) AS exact_cost,
                   SUM(tr.exact_cost_count) AS exact_cost_count
            FROM token_rollup tr
            WHERE tr.job_id IS NOT NULL AND tr.exact_cost_count > 0
            GROUP BY tr.job_id
        ),
        job_lines AS (
            SELECT job_id,
                   SUM(COALESCE(additions, 0) + COALESCE(deletions, 0)) AS lines
            FROM file_changes
            GROUP BY job_id
        )
        SELECT jt.job_id,
               mr.opened_at AS ts,
               jt.billable,
               CASE
                   WHEN (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) > 0
                       THEN COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)
                   ELSE COALESCE(jl.lines, 0)
               END AS lines_changed,
               jt.model,
               jt.node_name,
               jt.backend,
               jt.input,
               jt.cache_read,
               jt.cache_create,
               jt.output,
               COALESCE(jc.exact_cost, 0.0),
               COALESCE(jc.exact_cost_count, 0)
        FROM job_tokens jt
        JOIN merge_requests mr ON mr.job_id = jt.job_id
        LEFT JOIN job_lines jl ON jl.job_id = jt.job_id
        LEFT JOIN job_cost jc ON jc.job_id = jt.job_id
        WHERE (CASE
                   WHEN (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) > 0
                       THEN COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)
                   ELSE COALESCE(jl.lines, 0)
               END) > 0
          AND (?2 IS NULL OR mr.opened_at >= ?2)
          AND (?3 IS NULL OR mr.opened_at < ?3)
        ORDER BY mr.opened_at";
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(LocRow {
            job_id: row.text(0)?,
            ts: row.i64(1)?,
            billable: row.i64(2)?,
            lines: row.i64(3)?,
            model: row.opt_text(4)?,
            node_name: row.opt_text(5)?,
            backend: row.text(6)?,
            input: row.i64(7)?,
            cache_read: row.i64(8)?,
            cache_create: row.i64(9)?,
            output: row.i64(10)?,
            exact_cost: row.opt_f64(11)?.unwrap_or(0.0),
            exact_cost_count: row.i64(12)?,
        })
    })
    .await
}

/// Event-scan oracle for [`tokens_per_loc`].
#[cfg(test)]
pub(crate) async fn tokens_per_loc_live(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<LocRow>> {
    let sql = format!(
        "WITH {cte},
        job_cost AS (
            SELECT COALESCE(r.job_id, s.job_id) AS job_id,
                   SUM(e.cost_usd) AS exact_cost,
                   COUNT(e.cost_usd) AS exact_cost_count
            FROM events e
            LEFT JOIN runs r ON r.id = e.run_id
            LEFT JOIN sessions s ON s.id = e.session_id
            WHERE e.cost_usd IS NOT NULL
            GROUP BY COALESCE(r.job_id, s.job_id)
        ),
        job_tokens AS (
            SELECT te.job_id AS job_id,
                   SUM(te.billable_tokens) AS billable,
                   SUM(te.input_tokens) AS input,
                   SUM(te.cache_read_tokens) AS cache_read,
                   SUM(te.cache_create_tokens) AS cache_create,
                   SUM(te.output_tokens) AS output,
                   MAX(te.model) AS model,
                   MAX(te.node_name) AS node_name,
                   MAX(te.backend) AS backend
            FROM token_events te
            WHERE te.job_id IS NOT NULL
              AND (?1 IS NULL OR te.project_id = ?1)
            GROUP BY te.job_id
        ),
        job_lines AS (
            SELECT job_id,
                   SUM(COALESCE(additions, 0) + COALESCE(deletions, 0)) AS lines
            FROM file_changes
            GROUP BY job_id
        )
        SELECT jt.job_id,
               mr.opened_at AS ts,
               jt.billable,
               CASE
                   WHEN (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) > 0
                       THEN COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)
                   ELSE COALESCE(jl.lines, 0)
               END AS lines_changed,
               jt.model,
               jt.node_name,
               jt.backend,
               jt.input,
               jt.cache_read,
               jt.cache_create,
               jt.output,
               COALESCE(jc.exact_cost, 0.0),
               COALESCE(jc.exact_cost_count, 0)
        FROM job_tokens jt
        JOIN merge_requests mr ON mr.job_id = jt.job_id
        LEFT JOIN job_lines jl ON jl.job_id = jt.job_id
        LEFT JOIN job_cost jc ON jc.job_id = jt.job_id
        WHERE (CASE
                   WHEN (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) > 0
                       THEN COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)
                   ELSE COALESCE(jl.lines, 0)
               END) > 0
          AND (?2 IS NULL OR mr.opened_at >= ?2)
          AND (?3 IS NULL OR mr.opened_at < ?3)
        ORDER BY mr.opened_at",
        cte = TOKEN_EVENTS_CTE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(LocRow {
            job_id: row.text(0)?,
            ts: row.i64(1)?,
            billable: row.i64(2)?,
            lines: row.i64(3)?,
            model: row.opt_text(4)?,
            node_name: row.opt_text(5)?,
            backend: row.text(6)?,
            input: row.i64(7)?,
            cache_read: row.i64(8)?,
            cache_create: row.i64(9)?,
            output: row.i64(10)?,
            exact_cost: row.opt_f64(11)?.unwrap_or(0.0),
            exact_cost_count: row.i64(12)?,
        })
    })
    .await
}

/// Token components for one (bucket, model, backend) group, plus any real
/// metered cost recorded on that group's events. For metered backends
/// (OpenRouter) the exact cost lands on the settlement event, which is a
/// different event than the billable token event but shares the same
/// (bucket, model, backend) group, so it is joined here rather than summed
/// inside the billable-token CTE.
#[derive(Debug, Clone)]
pub(crate) struct CostComponentRow {
    pub bucket_start: i64,
    pub model: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub billable: i64,
    /// Sum of real `events.cost_usd` for this group (0 when none reported).
    pub exact_cost: f64,
    /// Count of events in this group that carried a real `cost_usd`.
    pub exact_cost_count: i64,
}

pub(crate) async fn cost_components(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<CostComponentRow>> {
    let bucket_expr = bucket.bucket_expr("tr.bucket_start");
    let sql = format!(
        "WITH tok AS (
            SELECT {bucket_expr} AS bucket_start,
                   tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.input_tokens) AS input,
                   SUM(tr.cache_read_tokens) AS cache_read,
                   SUM(tr.cache_create_tokens) AS cache_create,
                   SUM(tr.output_tokens) AS output,
                   SUM(tr.billable_tokens) AS billable
            FROM token_rollup tr
            WHERE tr.token_event_count > 0 AND {filter}
            GROUP BY {bucket_expr}, tr.model, tr.backend
        ),
        cost AS (
            SELECT {bucket_expr} AS bucket_start,
                   tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.exact_cost) AS exact_cost,
                   SUM(tr.exact_cost_count) AS exact_count
            FROM token_rollup tr
            WHERE tr.exact_cost_count > 0 AND {cost_filter}
            GROUP BY {bucket_expr}, tr.model, tr.backend
        )
        SELECT tok.bucket_start,
               tok.model,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.billable,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON cost.bucket_start = tok.bucket_start
           AND IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND cost.backend = tok.backend
        ORDER BY tok.bucket_start",
        filter = ROLLUP_SCOPE_RANGE,
        cost_filter = ROLLUP_COST_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params_doubled(scope, range), |row| {
        Ok(CostComponentRow {
            bucket_start: row.i64(0)?,
            model: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            billable: row.i64(7)?,
            exact_cost: row.opt_f64(8)?.unwrap_or(0.0),
            exact_cost_count: row.i64(9)?,
        })
    })
    .await
}

/// Event-scan variant of [`cost_components`]: the `#[cfg(test)]` oracle the
/// rollup read is proven equal to (no longer a production path).
#[cfg(test)]
pub(crate) async fn cost_components_live(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<CostComponentRow>> {
    let tok_bucket = bucket.bucket_expr("te.ts");
    let cost_bucket = bucket.bucket_expr("e.created_at");
    let sql = format!(
        "WITH {cte},
        cost_events AS (
            SELECT {cost_bucket} AS bucket_start,
                   j.model AS model,
                   LOWER(COALESCE(s.backend, 'claude')) AS backend,
                   COALESCE(r.project_id, j.project_id) AS project_id,
                   e.created_at AS ts,
                   e.cost_usd AS cost_usd
            FROM events e
            LEFT JOIN runs r ON r.id = e.run_id
            LEFT JOIN sessions s ON s.id = e.session_id
            LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
            WHERE e.cost_usd IS NOT NULL
        ),
        tok AS (
            SELECT {tok_bucket} AS bucket_start,
                   te.model AS model,
                   te.backend AS backend,
                   SUM(te.input_tokens) AS input,
                   SUM(te.cache_read_tokens) AS cache_read,
                   SUM(te.cache_create_tokens) AS cache_create,
                   SUM(te.output_tokens) AS output,
                   SUM(te.billable_tokens) AS billable
            FROM token_events te
            WHERE {filter}
            GROUP BY bucket_start, te.model, te.backend
        ),
        cost AS (
            SELECT ce.bucket_start AS bucket_start,
                   ce.model AS model,
                   ce.backend AS backend,
                   SUM(ce.cost_usd) AS exact_cost,
                   COUNT(ce.cost_usd) AS exact_count
            FROM cost_events ce
            WHERE {cost_filter}
            GROUP BY ce.bucket_start, ce.model, ce.backend
        )
        SELECT tok.bucket_start,
               tok.model,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.billable,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON cost.bucket_start = tok.bucket_start
           AND IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND cost.backend = tok.backend
        ORDER BY tok.bucket_start",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
        cost_filter = COST_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params_doubled(scope, range), |row| {
        Ok(CostComponentRow {
            bucket_start: row.i64(0)?,
            model: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            billable: row.i64(7)?,
            exact_cost: row.opt_f64(8)?.unwrap_or(0.0),
            exact_cost_count: row.i64(9)?,
        })
    })
    .await
}

/// Per-model token components for a single backend, plus any real metered cost
/// recorded on that model's events and the count of distinct runs. The
/// backend-scoped sibling of [`CostComponentRow`]: grouped by model only (no
/// time bucket), filtered to one backend, for the provider usage card.
#[derive(Debug, Clone)]
pub(crate) struct ProviderModelComponentRow {
    pub model: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub billable: i64,
    pub runs: i64,
    /// Sum of real `events.cost_usd` for this model (0 when none reported).
    pub exact_cost: f64,
    /// Count of events for this model that carried a real `cost_usd`.
    pub exact_cost_count: i64,
}

/// Per-model billable token totals and real metered cost for one backend over a
/// scope/range. Mirrors [`cost_components`] (the exact-cost join lives on the
/// settlement event, a different event than the billable token event but the
/// same `(model, backend)` group) but collapses the time bucket and pins a
/// single backend via `?7`, reused across both CTE predicates.
pub(crate) async fn provider_model_components(
    db: &LocalDb,
    backend: &str,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProviderModelComponentRow>> {
    let sql = format!(
        "WITH tok AS (
            SELECT tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.input_tokens) AS input,
                   SUM(tr.cache_read_tokens) AS cache_read,
                   SUM(tr.cache_create_tokens) AS cache_create,
                   SUM(tr.output_tokens) AS output,
                   SUM(tr.billable_tokens) AS billable,
                   COUNT(DISTINCT tr.run_id) AS runs
            FROM token_rollup tr
            WHERE tr.token_event_count > 0 AND tr.backend = ?7 AND {filter}
            GROUP BY tr.model, tr.backend
        ),
        cost AS (
            SELECT tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.exact_cost) AS exact_cost,
                   SUM(tr.exact_cost_count) AS exact_count
            FROM token_rollup tr
            WHERE tr.exact_cost_count > 0 AND tr.backend = ?7 AND {cost_filter}
            GROUP BY tr.model, tr.backend
        )
        SELECT tok.model,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.billable,
               tok.runs,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND cost.backend = tok.backend",
        filter = ROLLUP_SCOPE_RANGE,
        cost_filter = ROLLUP_COST_SCOPE_RANGE,
    );
    let mut params = scope_range_params_doubled(scope, range);
    params.push(Value::Text(backend.to_string()));
    db.query_all(sql, params, |row| {
        Ok(ProviderModelComponentRow {
            model: row.opt_text(0)?,
            backend: row.text(1)?,
            input: row.i64(2)?,
            cache_read: row.i64(3)?,
            cache_create: row.i64(4)?,
            output: row.i64(5)?,
            billable: row.i64(6)?,
            runs: row.i64(7)?,
            exact_cost: row.opt_f64(8)?.unwrap_or(0.0),
            exact_cost_count: row.i64(9)?,
        })
    })
    .await
}

/// Event-scan oracle for [`provider_model_components`].
#[cfg(test)]
pub(crate) async fn provider_model_components_live(
    db: &LocalDb,
    backend: &str,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProviderModelComponentRow>> {
    let sql = format!(
        "WITH {cte},
        cost_events AS (
            SELECT j.model AS model,
                   LOWER(COALESCE(s.backend, 'claude')) AS backend,
                   COALESCE(r.project_id, j.project_id) AS project_id,
                   e.created_at AS ts,
                   e.cost_usd AS cost_usd
            FROM events e
            LEFT JOIN runs r ON r.id = e.run_id
            LEFT JOIN sessions s ON s.id = e.session_id
            LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
            WHERE e.cost_usd IS NOT NULL
        ),
        tok AS (
            SELECT te.model AS model,
                   te.backend AS backend,
                   SUM(te.input_tokens) AS input,
                   SUM(te.cache_read_tokens) AS cache_read,
                   SUM(te.cache_create_tokens) AS cache_create,
                   SUM(te.output_tokens) AS output,
                   SUM(te.billable_tokens) AS billable,
                   COUNT(DISTINCT te.run_id) AS runs
            FROM token_events te
            WHERE te.backend = ?7 AND {filter}
            GROUP BY te.model, te.backend
        ),
        cost AS (
            SELECT ce.model AS model,
                   ce.backend AS backend,
                   SUM(ce.cost_usd) AS exact_cost,
                   COUNT(ce.cost_usd) AS exact_count
            FROM cost_events ce
            WHERE ce.backend = ?7 AND {cost_filter}
            GROUP BY ce.model, ce.backend
        )
        SELECT tok.model,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.billable,
               tok.runs,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND cost.backend = tok.backend",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
        cost_filter = COST_SCOPE_RANGE,
    );
    let mut params = scope_range_params_doubled(scope, range);
    params.push(Value::Text(backend.to_string()));
    db.query_all(sql, params, |row| {
        Ok(ProviderModelComponentRow {
            model: row.opt_text(0)?,
            backend: row.text(1)?,
            input: row.i64(2)?,
            cache_read: row.i64(3)?,
            cache_create: row.i64(4)?,
            output: row.i64(5)?,
            billable: row.i64(6)?,
            runs: row.i64(7)?,
            exact_cost: row.opt_f64(8)?.unwrap_or(0.0),
            exact_cost_count: row.i64(9)?,
        })
    })
    .await
}

/// Token components for one (model, role, backend) group. Aggregated into
/// by-model and by-role views in the parent module; each run maps to exactly
/// one such group, so summing `runs` across groups never double-counts.
#[derive(Debug, Clone)]
pub(crate) struct GranularRow {
    pub model: Option<String>,
    pub agent_config_id: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub thinking: i64,
    pub billable: i64,
    pub runs: i64,
    /// Real metered cost (`events.cost_usd`) for this group: the sum of every
    /// metered event whose (model, agent, backend) group also has billable
    /// tokens, independent of per-row time co-location. 0 when none reported.
    pub exact_cost: f64,
    /// Count of events contributing to `exact_cost`.
    pub exact_cost_count: i64,
}

pub(crate) async fn economics_granular(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<GranularRow>> {
    // Cost attribution is grain-invariant by construction: the `tok` CTE keeps
    // only groups that HAVE billable tokens; the `cost` CTE sums each group's
    // metered `exact_cost` INDEPENDENTLY of time co-location (no
    // `token_event_count` gate); the LEFT JOIN gives each billable group ALL its
    // metered cost. So a settlement event that lands in a different hour than
    // its turn's billable tokens still attributes its cost -- and SUM(exact_cost)
    // over hour rows equals the same sum over day rows, so the re-grain can never
    // move economics cost. This mirrors `cost_by_project`, unifying cost
    // attribution into one pattern across the module.
    let sql = format!(
        "WITH tok AS (
            SELECT tr.model AS model,
                   tr.agent_config_id AS agent_config_id,
                   tr.backend AS backend,
                   SUM(tr.input_tokens) AS input,
                   SUM(tr.cache_read_tokens) AS cache_read,
                   SUM(tr.cache_create_tokens) AS cache_create,
                   SUM(tr.output_tokens) AS output,
                   SUM(tr.thinking_tokens) AS thinking,
                   SUM(tr.billable_tokens) AS billable,
                   COUNT(DISTINCT tr.run_id) AS runs
            FROM token_rollup tr
            WHERE tr.token_event_count > 0 AND {filter}
            GROUP BY tr.model, tr.agent_config_id, tr.backend
        ),
        cost AS (
            SELECT tr.model AS model,
                   tr.agent_config_id AS agent_config_id,
                   tr.backend AS backend,
                   SUM(tr.exact_cost) AS exact_cost,
                   SUM(tr.exact_cost_count) AS exact_count
            FROM token_rollup tr
            WHERE tr.exact_cost_count > 0 AND {cost_filter}
            GROUP BY tr.model, tr.agent_config_id, tr.backend
        )
        SELECT tok.model,
               tok.agent_config_id,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.thinking,
               tok.billable,
               tok.runs,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND IFNULL(cost.agent_config_id, '') = IFNULL(tok.agent_config_id, '')
           AND cost.backend = tok.backend",
        filter = ROLLUP_SCOPE_RANGE,
        cost_filter = ROLLUP_COST_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params_doubled(scope, range), |row| {
        Ok(GranularRow {
            model: row.opt_text(0)?,
            agent_config_id: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            thinking: row.i64(7)?,
            billable: row.i64(8)?,
            runs: row.i64(9)?,
            exact_cost: row.opt_f64(10)?.unwrap_or(0.0),
            exact_cost_count: row.i64(11)?,
        })
    })
    .await
}

/// Event-scan oracle for [`economics_granular`].
///
/// Both sides attribute cost by the SAME independent rule: sum every metered
/// event's `cost_usd` per `(model, agent_config_id, backend)` group, then keep
/// only groups that also carry billable tokens (the `FROM tok LEFT JOIN cost`
/// shape). There is no time co-location gate, so the attribution is invariant to
/// the rollup's hour-vs-day grain -- which is exactly why the rollup-vs-oracle
/// equality test cannot itself guard the co-location behavior (a dedicated
/// cross-hour economics test does).
#[cfg(test)]
pub(crate) async fn economics_granular_live(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<GranularRow>> {
    let sql = format!(
        "WITH {cte},
        cost_events AS (
            SELECT j.model AS model,
                   j.agent_config_id AS agent_config_id,
                   LOWER(COALESCE(s.backend, 'claude')) AS backend,
                   COALESCE(r.project_id, j.project_id) AS project_id,
                   e.created_at AS ts,
                   e.cost_usd AS cost_usd
            FROM events e
            LEFT JOIN runs r ON r.id = e.run_id
            LEFT JOIN sessions s ON s.id = e.session_id
            LEFT JOIN jobs j ON j.id = COALESCE(r.job_id, s.job_id)
            WHERE e.cost_usd IS NOT NULL
        ),
        cost AS (
            SELECT ce.model AS model,
                   ce.agent_config_id AS agent_config_id,
                   ce.backend AS backend,
                   SUM(ce.cost_usd) AS exact_cost,
                   COUNT(ce.cost_usd) AS exact_cost_count
            FROM cost_events ce
            WHERE {cost_filter}
            GROUP BY ce.model, ce.agent_config_id, ce.backend
        ),
        tok AS (
            SELECT te.model AS model,
                   te.agent_config_id AS agent_config_id,
                   te.backend AS backend,
                   SUM(te.input_tokens) AS input,
                   SUM(te.cache_read_tokens) AS cache_read,
                   SUM(te.cache_create_tokens) AS cache_create,
                   SUM(te.output_tokens) AS output,
                   SUM(te.thinking_tokens) AS thinking,
                   SUM(te.billable_tokens) AS billable,
                   COUNT(DISTINCT te.run_id) AS runs
            FROM token_events te
            WHERE {filter}
            GROUP BY te.model, te.agent_config_id, te.backend
        )
        SELECT tok.model,
               tok.agent_config_id,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.thinking,
               tok.billable,
               tok.runs,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_cost_count, 0)
        FROM tok
        LEFT JOIN cost
            ON IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND IFNULL(cost.agent_config_id, '') = IFNULL(tok.agent_config_id, '')
           AND cost.backend = tok.backend",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
        cost_filter = COST_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params_doubled(scope, range), |row| {
        Ok(GranularRow {
            model: row.opt_text(0)?,
            agent_config_id: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            thinking: row.i64(7)?,
            billable: row.i64(8)?,
            runs: row.i64(9)?,
            exact_cost: row.opt_f64(10)?.unwrap_or(0.0),
            exact_cost_count: row.i64(11)?,
        })
    })
    .await
}

// --- Tier B: tool-invocation rollup (backfill + read queries) ---

/// Joins `tool_invocations` to runs/jobs so scope filtering matches Tier A.
const TOOL_JOIN: &str = "FROM tool_invocations ti
        LEFT JOIN runs r ON r.id = ti.run_id
        LEFT JOIN jobs j ON j.id = r.job_id";

const TOOL_SCOPE_RANGE: &str = "(?1 IS NULL OR COALESCE(r.project_id, j.project_id) = ?1)
        AND (?2 IS NULL OR ti.ts >= ?2)
        AND (?3 IS NULL OR ti.ts < ?3)";

/// Run an idempotent incremental backfill of the tool-invocation rollup.
///
/// Off the event-insert hot path by design: it reconstructs each run's events
/// once (the same machinery behind `/chat`), extracts tool invocations, and
/// upserts them. A per-run watermark skips runs with no new events, so repeat
/// passes only touch active runs.
pub(crate) async fn backfill_tool_invocations(db: &LocalDb) -> DbResult<ToolBackfillSummary> {
    let runs = runs_needing_backfill(db).await?;
    let mut runs_processed = 0i64;
    let mut rows_written = 0i64;
    for run_id in runs {
        let events = load_run_events(db, &run_id).await?;
        let processed_through = events.iter().map(|e| e.created_at).max().unwrap_or(-1);
        let rows = tool_extract::extract_run_invocations(&events);
        rows_written += upsert_run_invocations(db, &run_id, &rows, processed_through).await? as i64;
        runs_processed += 1;
    }
    let total_indexed = db
        .query_opt_i64("SELECT COUNT(*) FROM tool_invocations", ())
        .await?
        .unwrap_or(0);
    Ok(ToolBackfillSummary {
        runs_processed,
        rows_written,
        total_indexed,
    })
}

/// Run ids whose newest assistant/tool_result event is past the rollup
/// watermark (or which have never been processed).
async fn runs_needing_backfill(db: &LocalDb) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT e.run_id
         FROM events e
         LEFT JOIN tool_invocation_runs w ON w.run_id = e.run_id
         WHERE e.event_type IN ('assistant', 'tool_result')
         GROUP BY e.run_id
         HAVING MAX(e.created_at) > COALESCE(MAX(w.processed_through), -1)",
        (),
        |row| row.text(0),
    )
    .await
}

async fn load_run_events(db: &LocalDb, run_id: &str) -> DbResult<Vec<Event>> {
    let run_id = run_id.to_string();
    let events = db
        .query_all(
            format!(
                "SELECT {cols} FROM events WHERE run_id = ?1 ORDER BY sequence ASC",
                cols = crate::runs::queries::EVENT_COLUMNS
            ),
            params![run_id],
            crate::runs::queries::event_from_row,
        )
        .await?;
    Ok(crate::archival::reconstruct_events(db, events).await)
}

async fn upsert_run_invocations(
    db: &LocalDb,
    run_id: &str,
    rows: &[ToolInvocation],
    processed_through: i64,
) -> DbResult<u64> {
    let rows = rows.to_vec();
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let rows = rows.clone();
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut count = 0u64;
            for row in &rows {
                conn.execute(
                    "INSERT OR REPLACE INTO tool_invocations
                        (id, event_id, tool_use_id, run_id, ts, verb, tool_name,
                         target_scheme, target_kind, target_path, is_error)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        row.id(),
                        row.event_id.as_str(),
                        row.tool_use_id.as_str(),
                        row.run_id.as_str(),
                        row.ts,
                        row.verb.as_str(),
                        row.tool_name.as_str(),
                        row.target_scheme.as_str(),
                        row.target_kind.as_str(),
                        row.target_path.as_deref(),
                        row.is_error as i64
                    ],
                )
                .await?;
                count += 1;
            }
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "INSERT INTO tool_invocation_runs(run_id, processed_through, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(run_id) DO UPDATE SET
                     processed_through = excluded.processed_through,
                     updated_at = excluded.updated_at",
                params![run_id.as_str(), processed_through, now],
            )
            .await?;
            Ok(count)
        })
    })
    .await
}

/// Activity + error counts per target shape (verb × scheme × kind).
pub(crate) struct ShapeCount {
    pub verb: String,
    pub scheme: String,
    pub kind: String,
    pub count: i64,
    pub error_count: i64,
}

pub(crate) async fn target_shapes(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ShapeCount>> {
    let sql = format!(
        "SELECT ti.verb, ti.target_scheme, ti.target_kind,
                COUNT(*) AS c, SUM(ti.is_error) AS errors
        {join}
        WHERE {filter}
        GROUP BY ti.verb, ti.target_scheme, ti.target_kind
        ORDER BY c DESC",
        join = TOOL_JOIN,
        filter = TOOL_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(ShapeCount {
            verb: row.text(0)?,
            scheme: row.text(1)?,
            kind: row.text(2)?,
            count: row.i64(3)?,
            error_count: row.opt_i64(4)?.unwrap_or(0),
        })
    })
    .await
}

pub(crate) async fn top_targets(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<TopTargetRow>> {
    let sql = format!(
        "SELECT ti.target_path, ti.target_scheme,
                COUNT(*) AS c, SUM(ti.is_error) AS errors
        {join}
        WHERE {filter} AND ti.target_path IS NOT NULL
        GROUP BY ti.target_path, ti.target_scheme
        ORDER BY c DESC
        LIMIT 25",
        join = TOOL_JOIN,
        filter = TOOL_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(TopTargetRow {
            target: row.text(0)?,
            scheme: row.text(1)?,
            count: row.i64(2)?,
            error_count: row.opt_i64(3)?.unwrap_or(0),
        })
    })
    .await
}

/// Average tool calls per session, bucketed by the session's first tool call.
pub(crate) struct ToolSessionBucketRow {
    pub bucket_start: i64,
    pub avg_calls: f64,
    pub session_count: i64,
}

pub(crate) async fn tool_calls_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolSessionBucketRow>> {
    let bucket_expr = bucket.bucket_expr("session_ts");
    let sql = format!(
        "WITH session_calls AS (
            SELECT r.session_id AS session_id,
                   COUNT(*) AS calls,
                   MIN(ti.ts) AS session_ts
            {join}
            WHERE r.session_id IS NOT NULL AND {filter}
            GROUP BY r.session_id
        )
        SELECT {bucket_expr} AS bucket_start,
               AVG(calls) AS avg_calls,
               COUNT(*) AS session_count
        FROM session_calls
        GROUP BY bucket_start
        ORDER BY bucket_start",
        join = TOOL_JOIN,
        filter = TOOL_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(ToolSessionBucketRow {
            bucket_start: row.i64(0)?,
            avg_calls: row.opt_f64(1)?.unwrap_or(0.0),
            session_count: row.i64(2)?,
        })
    })
    .await
}

/// Tool-call frequency by verb over time.
pub(crate) struct ToolMixRow {
    pub bucket_start: i64,
    pub verb: String,
    pub count: i64,
}

pub(crate) async fn tool_mix(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolMixRow>> {
    let bucket_expr = bucket.bucket_expr("ti.ts");
    let sql = format!(
        "SELECT {bucket_expr} AS bucket_start, ti.verb, COUNT(*) AS c
        {join}
        WHERE {filter}
        GROUP BY bucket_start, ti.verb
        ORDER BY bucket_start",
        join = TOOL_JOIN,
        filter = TOOL_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(ToolMixRow {
            bucket_start: row.i64(0)?,
            verb: row.text(1)?,
            count: row.i64(2)?,
        })
    })
    .await
}

// --- New charts: token composition, cost-by-project, tool error rate, heatmap ---

/// Summed token components (input / cache-read / cache-create / output /
/// thinking) for one time bucket. Powers the stacked token-composition chart.
#[derive(Debug, Clone)]
pub(crate) struct TokenComponentRow {
    pub bucket_start: i64,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub thinking: i64,
}

/// Token components per time bucket from the hour-grain rollup, served directly
/// at every granularity (`bucket_expr(Hour)` on the hour-floored `bucket_start`
/// is the identity floor).
pub(crate) async fn token_components(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TokenComponentRow>> {
    let bucket_expr = bucket.bucket_expr("tr.bucket_start");
    let sql = format!(
        "SELECT {bucket_expr} AS bucket_start,
                SUM(tr.input_tokens),
                SUM(tr.cache_read_tokens),
                SUM(tr.cache_create_tokens),
                SUM(tr.output_tokens),
                SUM(tr.thinking_tokens)
        FROM token_rollup tr
        WHERE tr.token_event_count > 0 AND {filter}
        GROUP BY {bucket_expr}
        ORDER BY {bucket_expr}",
        filter = ROLLUP_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(TokenComponentRow {
            bucket_start: row.i64(0)?,
            input: row.i64(1)?,
            cache_read: row.i64(2)?,
            cache_create: row.i64(3)?,
            output: row.i64(4)?,
            thinking: row.i64(5)?,
        })
    })
    .await
}

/// Event-scan variant of [`token_components`]: the `#[cfg(test)]` oracle the
/// rollup read is proven equal to (no longer a production path).
#[cfg(test)]
pub(crate) async fn token_components_live(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TokenComponentRow>> {
    let bucket_expr = bucket.bucket_expr("te.ts");
    let sql = format!(
        "WITH {cte}
        SELECT {bucket_expr} AS bucket_start,
                SUM(te.input_tokens),
                SUM(te.cache_read_tokens),
                SUM(te.cache_create_tokens),
                SUM(te.output_tokens),
                SUM(te.thinking_tokens)
        FROM token_events te
        WHERE {filter}
        GROUP BY bucket_start
        ORDER BY bucket_start",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(TokenComponentRow {
            bucket_start: row.i64(0)?,
            input: row.i64(1)?,
            cache_read: row.i64(2)?,
            cache_create: row.i64(3)?,
            output: row.i64(4)?,
            thinking: row.i64(5)?,
        })
    })
    .await
}

/// Per (project, model, backend) token components and real metered cost, with
/// the project's display name joined. Folded to per-project totals in the parent
/// module via the canonical exact-cost-preferred rule. The settlement cost lands
/// on a non-billable event in the same (project, model, backend) group, so it is
/// joined here exactly as [`cost_components`] does for its time buckets.
#[derive(Debug, Clone)]
pub(crate) struct ProjectCostComponentRow {
    pub project_id: String,
    pub project_name: String,
    pub model: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub billable: i64,
    pub exact_cost: f64,
    pub exact_cost_count: i64,
}

pub(crate) async fn cost_by_project(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProjectCostComponentRow>> {
    let sql = format!(
        "WITH tok AS (
            SELECT tr.project_id AS project_id,
                   tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.input_tokens) AS input,
                   SUM(tr.cache_read_tokens) AS cache_read,
                   SUM(tr.cache_create_tokens) AS cache_create,
                   SUM(tr.output_tokens) AS output,
                   SUM(tr.billable_tokens) AS billable
            FROM token_rollup tr
            WHERE tr.token_event_count > 0 AND tr.project_id IS NOT NULL AND {filter}
            GROUP BY tr.project_id, tr.model, tr.backend
        ),
        cost AS (
            SELECT tr.project_id AS project_id,
                   tr.model AS model,
                   tr.backend AS backend,
                   SUM(tr.exact_cost) AS exact_cost,
                   SUM(tr.exact_cost_count) AS exact_count
            FROM token_rollup tr
            WHERE tr.exact_cost_count > 0 AND tr.project_id IS NOT NULL AND {cost_filter}
            GROUP BY tr.project_id, tr.model, tr.backend
        )
        SELECT tok.project_id,
               COALESCE(p.name, tok.project_id) AS project_name,
               tok.model,
               tok.backend,
               tok.input,
               tok.cache_read,
               tok.cache_create,
               tok.output,
               tok.billable,
               COALESCE(cost.exact_cost, 0.0),
               COALESCE(cost.exact_count, 0)
        FROM tok
        LEFT JOIN cost
            ON cost.project_id = tok.project_id
           AND IFNULL(cost.model, '') = IFNULL(tok.model, '')
           AND cost.backend = tok.backend
        LEFT JOIN projects p ON p.id = tok.project_id
        ORDER BY tok.project_id",
        filter = ROLLUP_SCOPE_RANGE,
        cost_filter = ROLLUP_COST_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params_doubled(scope, range), |row| {
        Ok(ProjectCostComponentRow {
            project_id: row.text(0)?,
            project_name: row.text(1)?,
            model: row.opt_text(2)?,
            backend: row.text(3)?,
            input: row.i64(4)?,
            cache_read: row.i64(5)?,
            cache_create: row.i64(6)?,
            output: row.i64(7)?,
            billable: row.i64(8)?,
            exact_cost: row.opt_f64(9)?.unwrap_or(0.0),
            exact_cost_count: row.i64(10)?,
        })
    })
    .await
}

/// Tool-call volume and error count for one time bucket. The `tool_invocations`
/// table is at full `ts` resolution, so `bucket_expr("ti.ts")` serves every
/// granularity (including hourly) directly -- no `_live` sibling needed.
#[derive(Debug, Clone)]
pub(crate) struct ToolErrorBucketRow {
    pub bucket_start: i64,
    pub total: i64,
    pub errors: i64,
}

pub(crate) async fn tool_error_rate(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolErrorBucketRow>> {
    let bucket_expr = bucket.bucket_expr("ti.ts");
    let sql = format!(
        "SELECT {bucket_expr} AS bucket_start,
                COUNT(*) AS total,
                SUM(ti.is_error) AS errors
        {join}
        WHERE {filter}
        GROUP BY bucket_start
        ORDER BY bucket_start",
        join = TOOL_JOIN,
        filter = TOOL_SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(ToolErrorBucketRow {
            bucket_start: row.i64(0)?,
            total: row.i64(1)?,
            errors: row.opt_i64(2)?.unwrap_or(0),
        })
    })
    .await
}

/// Total billable tokens for one (day-of-week, hour) cell, in the user's local
/// time. `day_of_week` is SQLite's `%w` (0 = Sunday); the frontend reorders to
/// Mon-Sun.
#[derive(Debug, Clone)]
pub(crate) struct HeatmapBucketRow {
    pub day_of_week: i64,
    pub hour: i64,
    pub tokens: i64,
}

/// Billable tokens summed per local-time day-of-week and hour, from the
/// hour-grain `token_rollup`.
///
/// SQLite's `strftime` is UTC-only, so `tz_offset_seconds` shifts each bucket
/// into the user's local time BEFORE the weekday/hour is extracted (`?4` carries
/// the offset). Applying the offset in-SQL — rather than shifting hours on the
/// frontend — keeps a late-night hour that crosses the UTC day boundary on its
/// correct local weekday.
///
/// Precision caveat: rollup buckets are UTC-hour-floored, so adding the tz offset
/// then extracting the local hour is EXACT for whole-hour offsets but APPROXIMATE
/// (it smears activity into an adjacent local hour, and can misplace the weekday
/// for an hour straddling local midnight) only for sub-hour offsets like +5:30 —
/// a deliberate speed/precision trade against the retired full-`ts` event scan.
pub(crate) async fn usage_heatmap(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    tz_offset_seconds: i64,
) -> DbResult<Vec<HeatmapBucketRow>> {
    let sql = format!(
        "SELECT CAST(strftime('%w', tr.bucket_start + ?4, 'unixepoch') AS INTEGER) AS dow,
                CAST(strftime('%H', tr.bucket_start + ?4, 'unixepoch') AS INTEGER) AS hour,
                SUM(tr.billable_tokens) AS tokens
        FROM token_rollup tr
        WHERE tr.token_event_count > 0 AND {filter}
        GROUP BY dow, hour
        ORDER BY dow, hour",
        filter = ROLLUP_SCOPE_RANGE,
    );
    let mut params = scope_range_params(scope, range);
    params.push(Value::Integer(tz_offset_seconds));
    db.query_all(sql, params, |row| {
        Ok(HeatmapBucketRow {
            day_of_week: row.i64(0)?,
            hour: row.i64(1)?,
            tokens: row.i64(2)?,
        })
    })
    .await
}
