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

use turso::{params, Value};

use super::tool_extract::{self, ToolInvocation};
use super::types::{Bucket, Scope, TimeRange, ToolBackfillSummary, TopTargetRow};
use crate::models::Event;
use crate::storage::{DbResult, LocalDb, RowExt};

/// One row per billable top-level turn event, with per-backend billable broken
/// out. Referenced as `te` by every outer query.
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
        LOWER(COALESCE(r.backend, s.backend, 'claude')) AS backend,
        CASE
            WHEN LOWER(COALESCE(r.backend, s.backend, 'claude')) = 'codex'
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
            (LOWER(COALESCE(r.backend, s.backend, 'claude')) = 'codex'
                AND e.event_type LIKE 'result%')
        OR (LOWER(COALESCE(r.backend, s.backend, 'claude')) <> 'codex'
                AND e.event_type = 'assistant')
      )
)";

/// Scope/range predicate over the `token_events` alias `te`. Bound params are
/// `?1` = project id, `?2` = start, `?3` = end; each is NULL to disable that
/// filter.
const SCOPE_RANGE: &str = "(?1 IS NULL OR te.project_id = ?1)
        AND (?2 IS NULL OR te.ts >= ?2)
        AND (?3 IS NULL OR te.ts < ?3)";

/// Positional params for [`SCOPE_RANGE`].
fn scope_range_params(scope: &Scope, range: &TimeRange) -> Vec<Value> {
    vec![
        scope
            .project_id
            .clone()
            .map_or(Value::Null, Value::Text),
        range.start.map_or(Value::Null, Value::Integer),
        range.end.map_or(Value::Null, Value::Integer),
    ]
}

/// Per-session billable total bucketed by the session's first event time.
pub(crate) struct SessionBucketRow {
    pub bucket_start: i64,
    pub avg_tokens: f64,
    pub session_count: i64,
}

pub(crate) async fn avg_tokens_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<SessionBucketRow>> {
    let secs = bucket.seconds();
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
        SELECT (session_ts / {secs}) * {secs} AS bucket_start,
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

/// One PR-producing job: billable tokens and lines changed.
pub(crate) struct LocRow {
    pub job_id: String,
    pub ts: i64,
    pub billable: i64,
    pub lines: i64,
    pub model: Option<String>,
    pub node_name: Option<String>,
}

pub(crate) async fn tokens_per_loc(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<LocRow>> {
    let sql = format!(
        "WITH {cte},
        job_tokens AS (
            SELECT te.job_id AS job_id,
                   SUM(te.billable_tokens) AS billable,
                   MAX(te.model) AS model,
                   MAX(te.node_name) AS node_name
            FROM token_events te
            WHERE te.job_id IS NOT NULL
              AND (?1 IS NULL OR te.project_id = ?1)
            GROUP BY te.job_id
        )
        SELECT jt.job_id,
               mr.opened_at AS ts,
               jt.billable,
               (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) AS lines_changed,
               jt.model,
               jt.node_name
        FROM job_tokens jt
        JOIN merge_requests mr ON mr.job_id = jt.job_id
        WHERE (COALESCE(mr.additions, 0) + COALESCE(mr.deletions, 0)) > 0
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
        })
    })
    .await
}

/// Token components for one (bucket, model, backend) group.
pub(crate) struct CostComponentRow {
    pub bucket_start: i64,
    pub model: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub billable: i64,
}

pub(crate) async fn cost_components(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<CostComponentRow>> {
    let secs = bucket.seconds();
    let sql = format!(
        "WITH {cte}
        SELECT (te.ts / {secs}) * {secs} AS bucket_start,
               te.model,
               te.backend,
               SUM(te.input_tokens),
               SUM(te.cache_read_tokens),
               SUM(te.cache_create_tokens),
               SUM(te.output_tokens),
               SUM(te.billable_tokens)
        FROM token_events te
        WHERE {filter}
        GROUP BY bucket_start, te.model, te.backend
        ORDER BY bucket_start",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(CostComponentRow {
            bucket_start: row.i64(0)?,
            model: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            billable: row.i64(7)?,
        })
    })
    .await
}

/// Token components for one (model, role, backend) group. Aggregated into
/// by-model and by-role views in the parent module; each run maps to exactly
/// one such group, so summing `runs` across groups never double-counts.
pub(crate) struct GranularRow {
    pub model: Option<String>,
    pub node_name: Option<String>,
    pub backend: String,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub thinking: i64,
    pub billable: i64,
    pub runs: i64,
}

pub(crate) async fn economics_granular(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<GranularRow>> {
    let sql = format!(
        "WITH {cte}
        SELECT te.model,
               te.node_name,
               te.backend,
               SUM(te.input_tokens),
               SUM(te.cache_read_tokens),
               SUM(te.cache_create_tokens),
               SUM(te.output_tokens),
               SUM(te.thinking_tokens),
               SUM(te.billable_tokens),
               COUNT(DISTINCT te.run_id)
        FROM token_events te
        WHERE {filter}
        GROUP BY te.model, te.node_name, te.backend",
        cte = TOKEN_EVENTS_CTE,
        filter = SCOPE_RANGE,
    );
    db.query_all(sql, scope_range_params(scope, range), |row| {
        Ok(GranularRow {
            model: row.opt_text(0)?,
            node_name: row.opt_text(1)?,
            backend: row.text(2)?,
            input: row.i64(3)?,
            cache_read: row.i64(4)?,
            cache_create: row.i64(5)?,
            output: row.i64(6)?,
            thinking: row.i64(7)?,
            billable: row.i64(8)?,
            runs: row.i64(9)?,
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
    let secs = bucket.seconds();
    let sql = format!(
        "SELECT (ti.ts / {secs}) * {secs} AS bucket_start, ti.verb, COUNT(*) AS c
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
