//! Agent-economics analytics.
//!
//! Tier A surfaces token economics straight off event columns: average tokens
//! per session over time, token-to-line efficiency on PR-producing jobs, priced
//! cost trends, and per-model / per-role economics. The backend-shaped billable
//! rule lives in [`queries`] (the `token_events` CTE); pricing lives in
//! [`pricing`]; this module joins the two and normalizes roles.

pub mod pricing;
pub mod queries;
pub mod tool_extract;
pub mod types;

use std::collections::HashMap;

use crate::storage::{DbResult, LocalDb};

pub use types::{
    Bucket, CostPoint, EconomicsRow, EffectiveCostPoint, ModelRoleEconomics, Scope,
    TargetBreakdown, TargetShapeRow, TimeRange, TokensPerLocPoint, TokensPerSessionPoint,
    ToolBackfillSummary, ToolCallsPerSessionPoint, ToolMixPoint, TopTargetRow,
};

/// Average billable tokens per session, bucketed by the session's first event.
pub async fn avg_tokens_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TokensPerSessionPoint>> {
    let rows = queries::avg_tokens_per_session(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| TokensPerSessionPoint {
            bucket_start: r.bucket_start,
            avg_tokens: r.avg_tokens,
            session_count: r.session_count,
        })
        .collect())
}

/// Token-to-line efficiency for every job that produced a merge request.
pub async fn tokens_per_loc(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<TokensPerLocPoint>> {
    let rows = queries::tokens_per_loc(db, scope, range).await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let tokens_per_line = if r.lines > 0 {
                r.billable as f64 / r.lines as f64
            } else {
                0.0
            };
            TokensPerLocPoint {
                job_id: r.job_id,
                ts: r.ts,
                billable_tokens: r.billable,
                lines_changed: r.lines,
                tokens_per_line,
                model: r.model,
                role: normalize_role(r.node_name.as_deref()),
            }
        })
        .collect())
}

/// Priced cost over time, summed per bucket across each (model, backend) group.
pub async fn cost_timeseries(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<CostPoint>> {
    let rows = queries::cost_components(db, scope, range, bucket).await?;
    let mut buckets: HashMap<i64, (f64, i64)> = HashMap::new();
    for row in &rows {
        let entry = buckets.entry(row.bucket_start).or_insert((0.0, 0));
        entry.0 += group_cost(row);
        entry.1 += row.billable;
    }
    let mut points: Vec<CostPoint> = buckets
        .into_iter()
        .map(|(bucket_start, (cost_usd, billable_tokens))| CostPoint {
            bucket_start,
            cost_usd,
            billable_tokens,
        })
        .collect();
    points.sort_by_key(|p| p.bucket_start);
    Ok(points)
}

/// The cost of one (bucket, model, backend) group: the real metered cost when
/// any event in the group reported one, else the price-table estimate. The
/// price table returns ~$0 for many metered (OpenRouter) models, so a real cost
/// is always preferred when present.
fn group_cost(row: &queries::CostComponentRow) -> f64 {
    if row.exact_cost_count > 0 {
        row.exact_cost
    } else {
        pricing::cost_usd(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            row.output,
        )
    }
}

/// Real per-model cost for one backend: the metered dollar amount Cairn recorded
/// for that model, with billable tokens and run count.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderModelCost {
    pub model: String,
    pub cost_usd: f64,
    pub billable_tokens: i64,
    pub runs: i64,
}

/// Per-model cost for a single backend, preferring the real metered cost
/// (`events.cost_usd`) over the price-table estimate, sorted by cost descending.
///
/// This is the canonical answer for "what did we actually bill per model on this
/// backend" and powers the provider usage card (e.g. OpenRouter), where the
/// price table returns ~$0 for many metered models. Distinct from
/// [`model_role_economics`], the all-backend dashboard view computed purely from
/// the price table.
pub async fn provider_model_costs(
    db: &LocalDb,
    backend: &str,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProviderModelCost>> {
    let rows = queries::provider_model_components(db, backend, scope, range).await?;
    let mut costs: Vec<ProviderModelCost> = rows
        .into_iter()
        .map(|row| {
            let cost_usd = if row.exact_cost_count > 0 {
                row.exact_cost
            } else {
                pricing::cost_usd(
                    &row.backend,
                    row.model.as_deref(),
                    row.input,
                    row.cache_read,
                    row.cache_create,
                    row.output,
                )
            };
            let model = row
                .model
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "unknown".to_string());
            ProviderModelCost {
                model,
                cost_usd,
                billable_tokens: row.billable,
                runs: row.runs,
            }
        })
        .collect();
    costs.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(costs)
}

/// First-of-month (UTC) epoch seconds for the current wall-clock time. Mirrors
/// the SQLite `start of month` bucketing so the in-progress month is flagged
/// provisional.
fn current_month_start() -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let now = Utc::now();
    Utc.with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
        .single()
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

/// Effective (subscription-normalized) cost per (month, backend).
///
/// For each provider-month the workspace-wide list-price cost defines a ratio
/// against the configured flat subscription fee (`ratio = fee / list_cost`); the
/// scoped usage's list cost is then multiplied by that ratio to allocate the
/// provider's share of the fee. By construction the effective costs across a
/// provider-month sum to the fee when scoped to the whole workspace. Backends
/// with no configured fee (e.g. OpenRouter) are metered and pass through at real
/// cost with `ratio = 1`.
pub async fn effective_cost(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    fees: &HashMap<String, f64>,
) -> DbResult<Vec<EffectiveCostPoint>> {
    // The ratio denominator is always workspace-wide, even when display usage is
    // scoped to a project: a project shows its effective share of the shared fee.
    let workspace = Scope::new(None);
    let workspace_rows = queries::cost_components(db, &workspace, range, Bucket::Month).await?;
    let scoped_rows = queries::cost_components(db, scope, range, Bucket::Month).await?;

    let mut workspace_cost: HashMap<(i64, String), f64> = HashMap::new();
    for row in &workspace_rows {
        *workspace_cost
            .entry((row.bucket_start, row.backend.clone()))
            .or_default() += group_cost(row);
    }

    #[derive(Default)]
    struct ScopedAgg {
        cost: f64,
        billable: i64,
        /// Whether any event in this group reported a real per-generation cost.
        /// This — not the absence of a configured fee — is what makes a backend
        /// genuinely metered (pay-as-you-go, e.g. OpenRouter). Claude/Codex never
        /// report exact cost, so they are always subscription backends.
        has_exact: bool,
    }
    let mut scoped: HashMap<(i64, String), ScopedAgg> = HashMap::new();
    for row in &scoped_rows {
        let entry = scoped
            .entry((row.bucket_start, row.backend.clone()))
            .or_default();
        entry.cost += group_cost(row);
        entry.billable += row.billable;
        entry.has_exact |= row.exact_cost_count > 0;
    }

    let current_month = current_month_start();
    let mut points: Vec<EffectiveCostPoint> = scoped
        .into_iter()
        .map(|((month_start, backend), agg)| {
            let workspace_list = workspace_cost
                .get(&(month_start, backend.clone()))
                .copied()
                .unwrap_or(0.0);
            let fee = fees
                .get(&backend)
                .copied()
                .filter(|f| f.is_finite() && *f > 0.0);
            let provisional = month_start == current_month;
            if agg.has_exact {
                // Genuinely metered backend (OpenRouter): pass through real cost.
                // A real per-generation cost is authoritative even if a fee was
                // configured, so metering wins over normalization here.
                EffectiveCostPoint {
                    month_start,
                    backend,
                    metered: true,
                    provisional,
                    subscription_fee: 0.0,
                    list_cost: workspace_list,
                    scoped_cost: agg.cost,
                    ratio: Some(1.0),
                    effective_cost: agg.cost,
                    billable_tokens: agg.billable,
                }
            } else {
                // Subscription backend (Claude/Codex). With a fee, normalize;
                // without one, there is no effective cost yet — the card shows the
                // list-price estimate and prompts the user to set a fee. The
                // `subscription_fee == 0 && !metered` shape signals "no fee set".
                match fee {
                    Some(fee) if workspace_list > 0.0 => {
                        let ratio = fee / workspace_list;
                        EffectiveCostPoint {
                            month_start,
                            backend,
                            metered: false,
                            provisional,
                            subscription_fee: fee,
                            list_cost: workspace_list,
                            scoped_cost: agg.cost,
                            ratio: Some(ratio),
                            effective_cost: agg.cost * ratio,
                            billable_tokens: agg.billable,
                        }
                    }
                    // Fee paid but no priced usage: ratio undefined (unrecovered).
                    Some(fee) => EffectiveCostPoint {
                        month_start,
                        backend,
                        metered: false,
                        provisional,
                        subscription_fee: fee,
                        list_cost: 0.0,
                        scoped_cost: agg.cost,
                        ratio: None,
                        effective_cost: 0.0,
                        billable_tokens: agg.billable,
                    },
                    // No fee configured: show list-price estimate, no allocation.
                    None => EffectiveCostPoint {
                        month_start,
                        backend,
                        metered: false,
                        provisional,
                        subscription_fee: 0.0,
                        list_cost: workspace_list,
                        scoped_cost: agg.cost,
                        ratio: None,
                        effective_cost: 0.0,
                        billable_tokens: agg.billable,
                    },
                }
            }
        })
        .collect();
    points.sort_by(|a, b| {
        a.month_start
            .cmp(&b.month_start)
            .then_with(|| a.backend.cmp(&b.backend))
    });
    Ok(points)
}

/// Per-model and per-role economics (tokens, cost, runs, efficiency ratios).
pub async fn model_role_economics(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<ModelRoleEconomics> {
    let rows = queries::economics_granular(db, scope, range).await?;
    let mut by_model: HashMap<String, Acc> = HashMap::new();
    let mut by_role: HashMap<String, Acc> = HashMap::new();
    for row in &rows {
        let cost = pricing::cost_usd(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            row.output,
        );
        let model_key = row
            .model
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        by_model.entry(model_key).or_default().add(row, cost);
        // Role groups on the job's agent identity (`agent_config_id`), not its
        // free-form `node_name`. Delegated tasks and child issues set node_name
        // to the task title, which would otherwise pollute the role breakdown
        // with one row per task; the agent config id is the actual agent role.
        by_role
            .entry(normalize_role(row.agent_config_id.as_deref()))
            .or_default()
            .add(row, cost);
    }
    Ok(ModelRoleEconomics {
        by_model: finalize(by_model),
        by_role: finalize(by_role),
        price_source_date: pricing::PRICE_SOURCE_DATE.to_string(),
    })
}

/// Run the incremental tool-invocation backfill (Tier B). Off the hot path;
/// safe to invoke on demand from the dashboard.
pub async fn backfill_tool_invocations(db: &LocalDb) -> DbResult<ToolBackfillSummary> {
    queries::backfill_tool_invocations(db).await
}

/// Target-shape activity, per-shape error rate, and the most-targeted resources.
pub async fn target_breakdown(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<TargetBreakdown> {
    let raw = queries::target_shapes(db, scope, range).await?;
    let total: i64 = raw.iter().map(|s| s.count).sum();
    let shapes = raw
        .into_iter()
        .map(|s| TargetShapeRow {
            error_rate: if s.count > 0 {
                s.error_count as f64 / s.count as f64
            } else {
                0.0
            },
            verb: s.verb,
            scheme: s.scheme,
            kind: s.kind,
            count: s.count,
            error_count: s.error_count,
        })
        .collect();
    let top_targets = queries::top_targets(db, scope, range).await?;
    Ok(TargetBreakdown {
        shapes,
        top_targets,
        total,
    })
}

/// Average tool calls per session over time. Powered by the tool-invocation
/// rollup (Tier B), so it returns empty until the index is built.
pub async fn avg_tool_calls_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolCallsPerSessionPoint>> {
    let rows = queries::tool_calls_per_session(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolCallsPerSessionPoint {
            bucket_start: r.bucket_start,
            avg_calls: r.avg_calls,
            session_count: r.session_count,
        })
        .collect())
}

/// Tool-call frequency by verb over time.
pub async fn tool_mix(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolMixPoint>> {
    let rows = queries::tool_mix(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolMixPoint {
            bucket_start: r.bucket_start,
            verb: r.verb,
            count: r.count,
        })
        .collect())
}

/// Token/cost accumulator for one grouping key.
#[derive(Default)]
struct Acc {
    billable: i64,
    cost: f64,
    runs: i64,
    input: i64,
    cache_read: i64,
    cache_create: i64,
    output: i64,
    thinking: i64,
}

impl Acc {
    fn add(&mut self, row: &queries::GranularRow, cost: f64) {
        self.billable += row.billable;
        self.cost += cost;
        self.runs += row.runs;
        self.input += row.input;
        self.cache_read += row.cache_read;
        self.cache_create += row.cache_create;
        self.output += row.output;
        self.thinking += row.thinking;
    }
}

fn finalize(map: HashMap<String, Acc>) -> Vec<EconomicsRow> {
    let mut rows: Vec<EconomicsRow> = map
        .into_iter()
        .map(|(key, a)| {
            let total_input = a.input + a.cache_read + a.cache_create;
            let cache_hit_ratio = if total_input > 0 {
                a.cache_read as f64 / total_input as f64
            } else {
                0.0
            };
            let thinking_share = if a.output > 0 {
                a.thinking as f64 / a.output as f64
            } else {
                0.0
            };
            EconomicsRow {
                key,
                billable_tokens: a.billable,
                cost_usd: a.cost,
                runs: a.runs,
                input_tokens: a.input,
                cache_read_tokens: a.cache_read,
                cache_create_tokens: a.cache_create,
                output_tokens: a.output,
                thinking_tokens: a.thinking,
                cache_hit_ratio,
                thinking_share,
            }
        })
        .collect();
    rows.sort_by(|a, b| b.billable_tokens.cmp(&a.billable_tokens));
    rows
}

/// Normalize a raw `jobs.node_name` into a display role: strip a trailing
/// `-<digits>` instance suffix, replace separators with spaces, title-case
/// each word. Empty / missing names become `Other`.
fn normalize_role(node_name: Option<&str>) -> String {
    let raw = node_name.unwrap_or("").trim();
    if raw.is_empty() {
        return "Other".to_string();
    }
    let stripped = strip_instance_suffix(raw);
    let words: Vec<String> = stripped
        .split(|c: char| c == '-' || c == '_' || c.is_whitespace())
        .filter(|w| !w.is_empty())
        .map(title_word)
        .collect();
    if words.is_empty() {
        "Other".to_string()
    } else {
        words.join(" ")
    }
}

/// Drop a trailing `-<digits>` segment (e.g. `planner-0` -> `planner`).
fn strip_instance_suffix(name: &str) -> &str {
    if let Some(idx) = name.rfind('-') {
        let suffix = &name[idx + 1..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return &name[..idx];
        }
    }
    name
}

fn title_word(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn fixture_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("analytics-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        // A claude job (sonnet/Builder) and a codex job (gpt-5/Planner-0), each
        // with billable turn events plus one event the backend rule must skip:
        // claude skips cumulative `result%`, codex skips `assistant`.
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Proj', 'PRJ', '/tmp/prj', 1, 1);
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-claude', 'proj', 'merged', 'sonnet', 'builder', 'builder', 1, 1);
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-codex', 'proj', 'running', 'gpt-5', 'planner-0', 'planner-0', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-claude', 'job-claude', 'claude', 'open', 1, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-codex', 'job-codex', 'codex', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
             VALUES ('run-claude', 'proj', 'job-claude', 'exited', 'sess-claude', 'claude', 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
             VALUES ('run-codex', 'proj', 'job-codex', 'exited', 'sess-codex', 'codex', 1, 1);

            -- Claude: two assistant events count (135 + 20 = 155); result skipped.
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c1', 'run-claude', 'sess-claude', 1, 1, 'assistant', '{}',
                NULL, 90000, 10, 100, 20, 5, 3);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c2', 'run-claude', 'sess-claude', 2, 2, 'assistant', '{}',
                NULL, 90001, 12, 0, 0, 8, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c3', 'run-claude', 'sess-claude', 3, 3, 'result:success', '{}',
                NULL, 90002, 99999, 99999, 99999, 99999, 0);

            -- Codex: two result events count (110 + 220 = 330); assistant skipped.
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x1', 'run-codex', 'sess-codex', 1, 1, 'result:success', '{}',
                NULL, 90000, 100, 50, 0, 10, 0);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x2', 'run-codex', 'sess-codex', 2, 2, 'result:success', '{}',
                NULL, 90001, 200, 80, 0, 20, 0);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x3', 'run-codex', 'sess-codex', 3, 3, 'assistant', '{}',
                NULL, 90002, 88888, 0, 0, 88888, 0);

            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                target_branch, status, opened_at, updated_at, additions, deletions)
             VALUES ('mr1', 'job-claude', 'proj', NULL, 'PR', 'agent/x', 'main', 'merged',
                90100, 90100, 100, 55);
            ",
        )
        .await
        .unwrap();
        db
    }

    fn econ<'a>(rows: &'a [EconomicsRow], key: &str) -> &'a EconomicsRow {
        rows.iter()
            .find(|r| r.key == key)
            .unwrap_or_else(|| panic!("missing key {key}"))
    }

    #[tokio::test]
    async fn economics_applies_backend_billable_rule() {
        let db = fixture_db().await;
        let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
            .await
            .unwrap();

        // Claude assistant billable = input + cache_read + cache_create + output.
        assert_eq!(econ(&out.by_model, "sonnet").billable_tokens, 155);
        // Codex result billable = input + output (cache subsumed, never re-added).
        assert_eq!(econ(&out.by_model, "gpt-5").billable_tokens, 330);
        assert_eq!(econ(&out.by_role, "Builder").billable_tokens, 155);
        assert_eq!(econ(&out.by_role, "Planner").billable_tokens, 330);
        assert_eq!(econ(&out.by_model, "sonnet").runs, 1);
        assert_eq!(out.price_source_date, pricing::PRICE_SOURCE_DATE);
        // Both priced models produce a positive cost.
        assert!(econ(&out.by_model, "sonnet").cost_usd > 0.0);
        assert!(econ(&out.by_model, "gpt-5").cost_usd > 0.0);
    }

    #[tokio::test]
    async fn by_role_uses_agent_config_not_task_title() {
        let db = fixture_db().await;
        // A delegated task: node_name is a free-form task title, but the job's
        // agent_config_id names the real agent. The role breakdown must show the
        // agent ("Explore"), never the task title.
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-task', 'proj', 'running', 'sonnet', 'Trace the parser flow', 'explore', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-task', 'job-task', 'claude', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
             VALUES ('run-task', 'proj', 'job-task', 'exited', 'sess-task', 'claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('tk1', 'run-task', 'sess-task', 1, 1, 'assistant', '{}',
                NULL, 90000, 10, 0, 0, 5, 0);
            ",
        )
        .await
        .unwrap();

        let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
            .await
            .unwrap();
        assert!(out.by_role.iter().any(|r| r.key == "Explore"));
        assert!(!out.by_role.iter().any(|r| r.key == "Trace The Parser Flow"));
    }

    #[tokio::test]
    async fn tokens_per_loc_divides_billable_by_lines() {
        let db = fixture_db().await;
        let rows = tokens_per_loc(&db, &Scope::default(), &TimeRange::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.job_id, "job-claude");
        assert_eq!(r.billable_tokens, 155);
        assert_eq!(r.lines_changed, 155);
        assert!((r.tokens_per_line - 1.0).abs() < 1e-9);
        assert_eq!(r.role, "Builder");
    }

    #[tokio::test]
    async fn avg_tokens_per_session_buckets_by_day() {
        let db = fixture_db().await;
        let rows =
            avg_tokens_per_session(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1);
        // 90000s falls in day bucket starting at 86400.
        assert_eq!(rows[0].bucket_start, 86400);
        assert_eq!(rows[0].session_count, 2);
        // (155 + 330) / 2 = 242.5
        assert!((rows[0].avg_tokens - 242.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn scope_filter_restricts_to_project() {
        let db = fixture_db().await;
        let out = model_role_economics(
            &db,
            &Scope::new(Some("other-proj".to_string())),
            &TimeRange::default(),
        )
        .await
        .unwrap();
        assert!(out.by_model.is_empty());
    }

    #[tokio::test]
    async fn tool_backfill_extracts_and_breaks_down_shapes() {
        let db = fixture_db().await;
        // An assistant event with two tool uses (a cairn file read + a shell
        // run), plus a tool_result marking the read as an error.
        db.execute_script(
            "
            INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
             VALUES ('run-tools', 'proj', 'job-claude', 'exited', 'sess-claude', 'claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-a', 'run-tools', 'sess-claude', 1, 1, 'assistant',
                '{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{\"id\":\"tu1\",\"name\":\"mcp__cairn__read\",\"input\":{\"paths\":[\"file:src/lib.rs\"]}},{\"id\":\"tu2\",\"name\":\"run\",\"input\":{\"commands\":[{\"command\":\"cargo test\"}]}}]}',
                NULL, 90000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-r', 'run-tools', 'sess-claude', 2, 2, 'tool_result',
                '{\"eventType\":\"tool_result\",\"toolUseId\":\"tu1\",\"toolName\":\"read\",\"toolResult\":\"boom\",\"isError\":true}',
                NULL, 90001);
            ",
        )
        .await
        .unwrap();

        let summary = backfill_tool_invocations(&db).await.unwrap();
        assert_eq!(summary.rows_written, 2);
        assert_eq!(summary.total_indexed, 2);

        // A second pass is a no-op: the run watermark has advanced.
        let again = backfill_tool_invocations(&db).await.unwrap();
        assert_eq!(again.runs_processed, 0);

        let breakdown = target_breakdown(&db, &Scope::default(), &TimeRange::default())
            .await
            .unwrap();
        assert_eq!(breakdown.total, 2);
        let read_shape = breakdown
            .shapes
            .iter()
            .find(|s| s.verb == "read" && s.scheme == "file")
            .expect("read/file shape");
        assert_eq!(read_shape.count, 1);
        assert_eq!(read_shape.error_count, 1);
        assert!((read_shape.error_rate - 1.0).abs() < 1e-9);
        assert!(breakdown
            .shapes
            .iter()
            .any(|s| s.verb == "run" && s.scheme == "shell"));
        assert!(breakdown
            .top_targets
            .iter()
            .any(|t| t.target == "src/lib.rs"));
    }

    #[tokio::test]
    async fn tool_calls_per_session_averages_calls() {
        let db = fixture_db().await;
        // A run on sess-claude with two tool uses; backfill turns those into two
        // tool_invocations rows, so the session has 2 tool calls.
        db.execute_script(
            "
            INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
             VALUES ('run-tools', 'proj', 'job-claude', 'exited', 'sess-claude', 'claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-a', 'run-tools', 'sess-claude', 1, 1, 'assistant',
                '{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{\"id\":\"tu1\",\"name\":\"mcp__cairn__read\",\"input\":{\"paths\":[\"file:src/lib.rs\"]}},{\"id\":\"tu2\",\"name\":\"run\",\"input\":{\"commands\":[{\"command\":\"cargo test\"}]}}]}',
                NULL, 90000);
            ",
        )
        .await
        .unwrap();
        backfill_tool_invocations(&db).await.unwrap();

        let rows =
            avg_tool_calls_per_session(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session_count, 1);
        assert!((rows[0].avg_calls - 2.0).abs() < 1e-9);
        // 90000s falls in the day bucket starting at 86400.
        assert_eq!(rows[0].bucket_start, 86400);
    }

    #[test]
    fn role_normalization() {
        assert_eq!(normalize_role(Some("planner-0")), "Planner");
        assert_eq!(normalize_role(Some("builder")), "Builder");
        assert_eq!(normalize_role(Some("cairn-setup")), "Cairn Setup");
        assert_eq!(normalize_role(Some("pr-review-2")), "Pr Review");
        assert_eq!(normalize_role(None), "Other");
        assert_eq!(normalize_role(Some("  ")), "Other");
    }

    // --- Effective (subscription-normalized) cost ---

    /// 2025-01-01 and 2025-02-01 00:00:00 UTC, the month-bucket boundaries used
    /// by the effective-cost tests.
    const JAN_2025: i64 = 1_735_689_600;
    const FEB_2025: i64 = 1_738_368_000;

    async fn cost_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("eff-cost-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Proj', 'PRJ', '/tmp/prj', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    /// Seed a job/session/run triple keyed by `suffix` for one backend+model.
    async fn seed_run(db: &LocalDb, suffix: &str, backend: &str, model: &str) {
        db.execute_script(&format!(
            "INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
              VALUES ('job-{s}', 'proj', 'running', '{model}', 'builder', 'builder', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-{s}', 'job-{s}', '{backend}', 'open', 1, 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, session_id, backend, created_at, updated_at)
              VALUES ('run-{s}', 'proj', 'job-{s}', 'exited', 'sess-{s}', '{backend}', 1, 1);",
            s = suffix,
            model = model,
            backend = backend
        ))
        .await
        .unwrap();
    }

    /// Insert one event on `run-{suffix}` with the given type, timestamp, input
    /// and output tokens, and optional real `cost_usd`.
    #[allow(clippy::too_many_arguments)]
    async fn add_event(
        db: &LocalDb,
        id: &str,
        suffix: &str,
        etype: &str,
        ts: i64,
        input: i64,
        output: i64,
        cost: Option<f64>,
    ) {
        let cost_sql = cost.map_or_else(|| "NULL".to_string(), |c| c.to_string());
        db.execute_script(&format!(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens, cost_usd)
             VALUES ('{id}', 'run-{suffix}', NULL, 1, {ts}, '{etype}', '{{}}',
                NULL, {ts}, {input}, 0, 0, {output}, 0, {cost_sql});"
        ))
        .await
        .unwrap();
    }

    fn fees(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn point<'a>(
        points: &'a [EffectiveCostPoint],
        month_start: i64,
        backend: &str,
    ) -> &'a EffectiveCostPoint {
        points
            .iter()
            .find(|p| p.month_start == month_start && p.backend == backend)
            .unwrap_or_else(|| panic!("missing point for {backend} @ {month_start}"))
    }

    #[tokio::test]
    async fn effective_cost_ratio_allocates_the_fee() {
        let db = cost_db().await;
        // One claude assistant turn: 1M input + 1M output of sonnet =>
        // list cost (1e6*3 + 1e6*15)/1e6 = $18.
        seed_run(&db, "c", "claude", "sonnet").await;
        add_event(
            &db,
            "c1",
            "c",
            "assistant",
            JAN_2025 + 100,
            1_000_000,
            1_000_000,
            None,
        )
        .await;

        let points = effective_cost(
            &db,
            &Scope::default(),
            &TimeRange::default(),
            &fees(&[("claude", 200.0)]),
        )
        .await
        .unwrap();

        let p = point(&points, JAN_2025, "claude");
        assert!(!p.metered);
        assert!(
            (p.list_cost - 18.0).abs() < 1e-9,
            "list_cost {}",
            p.list_cost
        );
        assert!((p.subscription_fee - 200.0).abs() < 1e-9);
        let ratio = p.ratio.expect("ratio present");
        assert!((ratio - 200.0 / 18.0).abs() < 1e-9, "ratio {ratio}");
        // Workspace scope: effective cost allocates the whole flat fee.
        assert!(
            (p.effective_cost - 200.0).abs() < 1e-6,
            "effective {}",
            p.effective_cost
        );
        assert_eq!(p.billable_tokens, 2_000_000);
    }

    #[tokio::test]
    async fn effective_cost_sums_to_fee_across_models() {
        let db = cost_db().await;
        // Two claude models in the same month roll up to one (month, backend)
        // point whose effective cost is exactly the fee.
        seed_run(&db, "son", "claude", "sonnet").await;
        seed_run(&db, "op", "claude", "opus").await;
        add_event(
            &db,
            "s1",
            "son",
            "assistant",
            JAN_2025 + 10,
            1_000_000,
            1_000_000,
            None,
        )
        .await;
        add_event(
            &db,
            "o1",
            "op",
            "assistant",
            JAN_2025 + 20,
            1_000_000,
            1_000_000,
            None,
        )
        .await;

        let points = effective_cost(
            &db,
            &Scope::default(),
            &TimeRange::default(),
            &fees(&[("claude", 200.0)]),
        )
        .await
        .unwrap();
        let claude: Vec<_> = points.iter().filter(|p| p.backend == "claude").collect();
        assert_eq!(claude.len(), 1, "one claude point per month");
        assert!((claude[0].effective_cost - 200.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn effective_cost_zero_priced_usage_yields_no_ratio() {
        let db = cost_db().await;
        // An unpriced model: billable tokens exist but list cost is $0, so the
        // ratio is undefined rather than NaN/Inf.
        seed_run(&db, "m", "claude", "mystery-model").await;
        add_event(&db, "m1", "m", "assistant", JAN_2025 + 5, 1000, 1000, None).await;

        let points = effective_cost(
            &db,
            &Scope::default(),
            &TimeRange::default(),
            &fees(&[("claude", 200.0)]),
        )
        .await
        .unwrap();
        let p = point(&points, JAN_2025, "claude");
        assert!(p.ratio.is_none());
        assert_eq!(p.effective_cost, 0.0);
        assert!((p.subscription_fee - 200.0).abs() < 1e-9);
        assert!(p.effective_cost.is_finite());
    }

    #[tokio::test]
    async fn effective_cost_subscription_without_fee_is_not_metered() {
        let db = cost_db().await;
        // Claude is a subscription backend: with no configured fee and no real
        // per-generation cost, it must NOT be reported as metered. The card
        // shows the list-price estimate ($18) rather than a fabricated "actual".
        seed_run(&db, "c", "claude", "sonnet").await;
        add_event(
            &db,
            "c1",
            "c",
            "assistant",
            JAN_2025 + 100,
            1_000_000,
            1_000_000,
            None,
        )
        .await;

        let points = effective_cost(&db, &Scope::default(), &TimeRange::default(), &fees(&[]))
            .await
            .unwrap();
        let p = point(&points, JAN_2025, "claude");
        assert!(!p.metered, "claude is a subscription backend, not metered");
        assert_eq!(p.subscription_fee, 0.0);
        assert!(p.ratio.is_none());
        assert!(
            (p.scoped_cost - 18.0).abs() < 1e-9,
            "list estimate {}",
            p.scoped_cost
        );
    }

    #[tokio::test]
    async fn effective_cost_metered_backend_uses_real_cost() {
        let db = cost_db().await;
        // OpenRouter (no configured fee) bills its assistant event for tokens and
        // settles the real dollar cost on the result event. The real cost ($0.42)
        // must win over the price-table estimate ($0.018 for sonnet).
        seed_run(&db, "or", "openrouter", "sonnet").await;
        add_event(
            &db,
            "or1",
            "or",
            "assistant",
            JAN_2025 + 30,
            1000,
            1000,
            None,
        )
        .await;
        add_event(
            &db,
            "or2",
            "or",
            "result:success",
            JAN_2025 + 31,
            0,
            0,
            Some(0.42),
        )
        .await;

        let points = effective_cost(
            &db,
            &Scope::default(),
            &TimeRange::default(),
            &fees(&[("claude", 200.0)]),
        )
        .await
        .unwrap();
        let p = point(&points, JAN_2025, "openrouter");
        assert!(p.metered);
        assert_eq!(p.ratio, Some(1.0));
        assert!(
            (p.effective_cost - 0.42).abs() < 1e-9,
            "effective {}",
            p.effective_cost
        );
        assert!((p.scoped_cost - 0.42).abs() < 1e-9);
    }

    #[tokio::test]
    async fn effective_cost_buckets_by_calendar_month_and_flags_provisional() {
        use chrono::{Datelike, TimeZone, Utc};
        let db = cost_db().await;
        seed_run(&db, "c", "claude", "sonnet").await;
        // One event in January, one in February: two distinct month buckets.
        add_event(&db, "j", "c", "assistant", JAN_2025 + 100, 1000, 1000, None).await;
        add_event(&db, "f", "c", "assistant", FEB_2025 + 100, 1000, 1000, None).await;
        // One event in the current wall-clock month: must be flagged provisional.
        let now = Utc::now();
        let now_month = Utc
            .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp();
        add_event(
            &db,
            "n",
            "c",
            "assistant",
            now.timestamp(),
            1000,
            1000,
            None,
        )
        .await;

        let points = effective_cost(&db, &Scope::default(), &TimeRange::default(), &fees(&[]))
            .await
            .unwrap();

        assert!(!point(&points, JAN_2025, "claude").provisional);
        assert!(!point(&points, FEB_2025, "claude").provisional);
        assert!(point(&points, now_month, "claude").provisional);
        assert_ne!(JAN_2025, FEB_2025);
    }

    #[tokio::test]
    async fn cost_timeseries_prefers_exact_metered_cost() {
        let db = cost_db().await;
        seed_run(&db, "or", "openrouter", "sonnet").await;
        add_event(
            &db,
            "or1",
            "or",
            "assistant",
            JAN_2025 + 30,
            1000,
            1000,
            None,
        )
        .await;
        add_event(
            &db,
            "or2",
            "or",
            "result:success",
            JAN_2025 + 31,
            0,
            0,
            Some(0.42),
        )
        .await;

        let points = cost_timeseries(&db, &Scope::default(), &TimeRange::default(), Bucket::Month)
            .await
            .unwrap();
        let p = point_cost(&points, JAN_2025);
        // Real metered cost ($0.42), not the price-table estimate ($0.018).
        assert!((p.cost_usd - 0.42).abs() < 1e-9, "cost {}", p.cost_usd);
    }

    #[tokio::test]
    async fn provider_model_costs_uses_real_metered_cost_and_filters_backend() {
        let db = cost_db().await;
        // OpenRouter: bills tokens on the assistant event, settles the real dollar
        // cost ($0.42) on the result event.
        seed_run(&db, "or", "openrouter", "sonnet").await;
        add_event(
            &db,
            "or1",
            "or",
            "assistant",
            JAN_2025 + 30,
            1000,
            1000,
            None,
        )
        .await;
        add_event(
            &db,
            "or2",
            "or",
            "result:success",
            JAN_2025 + 31,
            0,
            0,
            Some(0.42),
        )
        .await;
        // A claude run in the same workspace must be excluded by the backend filter.
        seed_run(&db, "c", "claude", "opus").await;
        add_event(&db, "c1", "c", "assistant", JAN_2025 + 40, 1000, 1000, None).await;

        let rows =
            provider_model_costs(&db, "openrouter", &Scope::new(None), &TimeRange::default())
                .await
                .unwrap();
        assert_eq!(rows.len(), 1, "only the openrouter model is returned");
        assert_eq!(rows[0].model, "sonnet");
        // Real metered cost ($0.42), not the price-table estimate (~$0.018).
        assert!(
            (rows[0].cost_usd - 0.42).abs() < 1e-9,
            "cost {}",
            rows[0].cost_usd
        );
        assert_eq!(rows[0].billable_tokens, 2000);
        assert_eq!(rows[0].runs, 1);
    }

    #[tokio::test]
    async fn provider_model_costs_empty_without_usage() {
        let db = cost_db().await;
        let rows =
            provider_model_costs(&db, "openrouter", &Scope::new(None), &TimeRange::default())
                .await
                .unwrap();
        assert!(rows.is_empty());
    }

    fn point_cost(points: &[CostPoint], bucket_start: i64) -> &CostPoint {
        points
            .iter()
            .find(|p| p.bucket_start == bucket_start)
            .unwrap_or_else(|| panic!("missing cost point @ {bucket_start}"))
    }
}
