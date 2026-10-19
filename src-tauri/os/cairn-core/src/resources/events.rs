//! Read-only rendering of the durable fact log: `cairn://p/{project}/events`,
//! its workspace view `cairn://routes/events`, and one row of either.
//!
//! A row is an index entry, so it renders as a pointer: what was minted, the
//! record it points at (followed, never inlined), the threads it woke, and what
//! its routes did. Wakes render beside the route outcome so a row that matched
//! no route but woke a thread never reads as "nothing happened". A row that
//! woke a thread as part of a coalesced batch renders that batch's outcome.

use std::collections::HashMap;

use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_project_event_uri, build_project_route_history_entry_uri, build_route_history_entry_uri,
    build_workspace_event_uri,
};
use serde_json::Value;

use crate::orchestrator::Orchestrator;
use crate::routes::FactRegistry;
use crate::storage::{FactFilter, FactRow, FactScope, FactWake, RouteFiringRecord};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;
/// How many rows a `match=` listing examines, newest first, before it stops.
const MATCH_SCAN_ROWS: i64 = 10_000;
const MATCH_SCAN_PAGE: i64 = 500;

#[derive(Debug, PartialEq, Eq)]
struct ListQuery {
    source: Option<String>,
    since_ms: Option<i64>,
    from_id: Option<i64>,
    to_id: Option<i64>,
    limit: i64,
    offset: i64,
    /// `match=`, as the trigger clause a thread would declare.
    matcher: Option<crate::routes::TriggerClause>,
}

/// `match=field:value[,field:value...]` over `source`, as a trigger clause.
/// A value keeps everything after its first `:`, so `deliveryId:bug-report:7`
/// tests `deliveryId`. The clause is validated by the registry the way a
/// standing trigger is, and may name only stored fields: listing never
/// restores content from records.
fn parse_match(source: &str, value: &str) -> Result<crate::routes::TriggerClause, String> {
    let registry = FactRegistry::default();
    let mut clause = crate::routes::TriggerClause::new();
    clause.insert("fact".into(), Value::String(source.into()));
    for test in value.split(',') {
        let (field, expected) = test.split_once(':').ok_or_else(|| {
            format!(
                "Query parameter 'match' needs field:value pairs separated by commas, got '{test}'"
            )
        })?;
        let field = field.trim();
        if field.is_empty() || field == "fact" {
            return Err(format!(
                "Query parameter 'match' cannot test '{field}'; name the source with `source=`"
            ));
        }
        let base = field.strip_suffix("Prefix").unwrap_or(field);
        if registry
            .source(source)
            .is_some_and(|spec| spec.is_hydrated(base))
        {
            return Err(format!(
                "Query parameter 'match' tests stored fields only; '{base}' on {source} is restored from the record, so a listing cannot filter by it"
            ));
        }
        if clause
            .insert(field.to_string(), Value::String(expected.to_string()))
            .is_some()
        {
            return Err(format!("Query parameter 'match' names '{field}' twice"));
        }
    }
    registry
        .validate_clause(source, &clause)
        .map_err(|error| format!("Query parameter 'match': {error}"))?;
    Ok(clause)
}

/// Whether a row passes `clause`, by the matcher standing triggers use.
fn row_matches(clause: &crate::routes::TriggerClause, row: &FactRow) -> bool {
    let fields = serde_json::from_str(&row.fields_json).unwrap_or_default();
    crate::routes::matches(
        std::slice::from_ref(clause),
        &crate::routes::Fact {
            source: &row.source,
            fields: &fields,
            presence: crate::routes::log::presence_from_str(&row.presence),
        },
    )
}

fn row_id_param(key: &str, value: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .ok()
        .filter(|id| *id >= 0)
        .ok_or_else(|| format!("Query parameter '{key}' must be a non-negative row id"))
}

fn parse_list_query(params: &[QueryParam]) -> Result<ListQuery, String> {
    let mut query = ListQuery {
        source: None,
        since_ms: None,
        from_id: None,
        to_id: None,
        limit: DEFAULT_LIMIT,
        offset: 0,
        matcher: None,
    };
    let mut match_value: Option<&str> = None;
    let mut seen: Vec<&str> = Vec::new();
    for param in params {
        let key = param.key.as_str();
        if seen.contains(&key) {
            return Err(format!("Query parameter '{key}' must be specified once"));
        }
        seen.push(key);
        match key {
            "source" => {
                let sources = FactRegistry::default().source_names();
                if !sources.contains(&param.value.as_str()) {
                    return Err(format!(
                        "Unknown fact source '{}'. Valid sources: {}",
                        param.value,
                        sources.join(", ")
                    ));
                }
                query.source = Some(param.value.clone());
            }
            "since" => {
                let seconds = param.value.parse::<i64>().map_err(|_| {
                    "Query parameter 'since' must be an epoch timestamp in seconds".to_string()
                })?;
                query.since_ms = Some(seconds.saturating_mul(1000));
            }
            "match" => match_value = Some(param.value.as_str()),
            "from" => query.from_id = Some(row_id_param(key, &param.value)?),
            "to" => query.to_id = Some(row_id_param(key, &param.value)?),
            "limit" => {
                let limit = param.value.parse::<i64>().map_err(|_| {
                    "Query parameter 'limit' must be a positive integer".to_string()
                })?;
                if !(1..=MAX_LIMIT).contains(&limit) {
                    return Err(format!(
                        "Query parameter 'limit' must be between 1 and {MAX_LIMIT}"
                    ));
                }
                query.limit = limit;
            }
            "offset" => {
                query.offset = param
                    .value
                    .parse::<i64>()
                    .ok()
                    .filter(|offset| *offset >= 0)
                    .ok_or_else(|| {
                        "Query parameter 'offset' must be a non-negative integer".to_string()
                    })?;
            }
            other => {
                return Err(format!(
                    "Unsupported query parameter '{other}' for events (accepted: source, match, since, from, to, limit, offset)"
                ))
            }
        }
    }
    if let Some(value) = match_value {
        let source = query.source.as_deref().ok_or(
            "Query parameter 'match' needs `source=`: it tests that source's fields, the way a standing trigger does",
        )?;
        query.matcher = Some(parse_match(source, value)?);
    }
    if let (Some(from), Some(to)) = (query.from_id, query.to_id) {
        if from > to {
            return Err(format!(
                "Query parameter 'from' ({from}) must not exceed 'to' ({to})"
            ));
        }
    }
    Ok(query)
}

async fn project_id_for_key(orch: &Orchestrator, project: &str) -> Result<String, String> {
    orch.db
        .local
        .query_text(
            "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
            (project.to_string(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("Project not found: {project}"))
}

/// Project keys by id, read once per render.
async fn project_keys(orch: &Orchestrator) -> HashMap<String, String> {
    orch.db
        .local
        .query_all("SELECT id, key FROM projects", (), |row| {
            Ok((
                crate::storage::RowExt::text(row, 0)?,
                crate::storage::RowExt::text(row, 1)?,
            ))
        })
        .await
        .map(|rows| rows.into_iter().collect())
        .unwrap_or_default()
}

fn row_uri(keys: &HashMap<String, String>, row: &FactRow) -> String {
    match row.project_id.as_ref().and_then(|id| keys.get(id)) {
        Some(key) => build_project_event_uri(key, row.id),
        None => build_workspace_event_uri(row.id),
    }
}

fn firing_uri(keys: &HashMap<String, String>, firing: &RouteFiringRecord) -> String {
    match firing
        .scope_key
        .strip_prefix("project:")
        .and_then(|id| keys.get(id))
    {
        Some(key) => build_project_route_history_entry_uri(key, &firing.route_id, firing.seq),
        None => build_route_history_entry_uri(&firing.route_id, firing.seq),
    }
}

fn minted(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn fields_of(row: &FactRow) -> serde_json::Map<String, Value> {
    serde_json::from_str(&row.fields_json).unwrap_or_default()
}

/// What the row says, in one line: the producer's summary, and for a file
/// change the node that wrote it.
fn headline(row: &FactRow) -> String {
    let mut line = row.summary.clone().unwrap_or_else(|| row.identity.clone());
    if let Some(node) = fields_of(row).get("node").and_then(Value::as_str) {
        line.push_str(&format!(" by {node}"));
    }
    line
}

fn wakes_of(row: &FactRow) -> Vec<Value> {
    row.wakes_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default()
}

/// The coalesced wakes these rows' wake records point at, read once per render.
async fn coalesced_wakes(orch: &Orchestrator, rows: &[FactRow]) -> HashMap<i64, FactWake> {
    let mut ids: Vec<i64> = rows
        .iter()
        .flat_map(wakes_of)
        .filter_map(|wake| wake["wake"].as_i64())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    crate::storage::get_fact_wakes(&orch.db.local, &ids)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|wake| (wake.id, wake))
        .collect()
}

fn plural(count: i64, noun: &str) -> String {
    format!("{count} {noun}{}", if count == 1 { "" } else { "s" })
}

/// Where one wake record stands: delivered, still pending in a coalesced
/// batch, or some other outcome.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WakeStanding {
    Delivered,
    Pending,
    Other,
}

/// One wake record's standing and how it reads. A coalesced record takes its
/// batch's outcome, so a row that woke a thread as one of twenty reads as
/// having woken it.
fn wake_target(wake: &Value, batches: &HashMap<i64, FactWake>) -> (WakeStanding, String) {
    let target = wake["uri"]
        .as_str()
        .or_else(|| wake["job"].as_str())
        .unwrap_or("?");
    match wake["action"].as_str() {
        Some("delivered") | None => (WakeStanding::Delivered, target.to_string()),
        Some("coalesced") => {
            let Some(batch) = wake["wake"].as_i64().and_then(|id| batches.get(&id)) else {
                return (
                    WakeStanding::Other,
                    format!("{target} (coalesced; its wake record was pruned)"),
                );
            };
            match batch.delivered_action.as_deref() {
                None => (
                    WakeStanding::Pending,
                    format!(
                        "{target} (coalesced, pending: {} so far)",
                        plural(batch.count, "row")
                    ),
                ),
                Some("delivered") if batch.count == 1 => {
                    (WakeStanding::Delivered, target.to_string())
                }
                Some("delivered") => (
                    WakeStanding::Delivered,
                    format!("{target} (coalesced, {})", plural(batch.count, "row")),
                ),
                Some(action) => (
                    WakeStanding::Other,
                    format!(
                        "{target} (coalesced, {}, {action})",
                        plural(batch.count, "row")
                    ),
                ),
            }
        }
        Some(action) => (WakeStanding::Other, format!("{target} ({action})")),
    }
}

fn wakes_segment(row: &FactRow, batches: &HashMap<i64, FactWake>) -> String {
    let wakes = wakes_of(row);
    if wakes.is_empty() {
        return "woke no threads".to_string();
    }
    let targets: Vec<_> = wakes
        .iter()
        .map(|wake| wake_target(wake, batches))
        .collect();
    let count = |standing| targets.iter().filter(|(s, _)| *s == standing).count() as i64;
    let pending = count(WakeStanding::Pending);
    format!(
        "woke {}{}: {}",
        plural(count(WakeStanding::Delivered), "thread"),
        if pending > 0 {
            format!(", {pending} pending")
        } else {
            String::new()
        },
        targets
            .into_iter()
            .map(|(_, text)| text)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn routes_segment(
    keys: &HashMap<String, String>,
    row: &FactRow,
    firings: &[RouteFiringRecord],
) -> String {
    let state = match row.route_state.as_str() {
        "route_generated" => "not routed (route-generated)".to_string(),
        state => state.to_string(),
    };
    let mut segment = format!("routes: {state}");
    if let Some(note) = &row.route_note {
        segment.push_str(&format!(" ({note})"));
    }
    for firing in firings {
        let outcome = match (&firing.drop_reason, &firing.error) {
            (Some(reason), _) => format!("{} {reason}", firing.status),
            (None, Some(error)) => format!("{}: {error}", firing.status),
            _ => firing.status.clone(),
        };
        segment.push_str(&format!(
            "\n    [{} #{}]({}) {} → {}{}",
            firing.route_id,
            firing.seq,
            firing_uri(keys, firing),
            outcome,
            firing.sink_kind,
            firing
                .sink_ref
                .as_deref()
                .map(|sink| format!(" {sink}"))
                .unwrap_or_default()
        ));
    }
    segment
}

async fn firings_for(orch: &Orchestrator, row: &FactRow) -> Vec<RouteFiringRecord> {
    crate::storage::list_firings_for_fact(&orch.db.local, row.id)
        .await
        .unwrap_or_default()
}

/// `cairn://p/{project}/events` (`project` set) or `cairn://routes/events`.
pub(crate) async fn list(
    orch: &Orchestrator,
    project: Option<&str>,
    params: &[QueryParam],
) -> String {
    let query = match parse_list_query(params) {
        Ok(query) => query,
        Err(error) => return error,
    };
    let scope = match project {
        Some(project) => match project_id_for_key(orch, project).await {
            Ok(id) => FactScope::Project(id),
            Err(error) => return error,
        },
        None => FactScope::Workspace,
    };
    let filter = |limit, offset| FactFilter {
        scope: scope.clone(),
        source: query.source.clone(),
        since_ms: query.since_ms,
        from_id: query.from_id,
        to_id: query.to_id,
        limit,
        offset,
    };
    let mut scan_note = None;
    let rows = match &query.matcher {
        None => match crate::storage::list_facts(&orch.db.local, filter(query.limit, query.offset))
            .await
        {
            Ok(rows) => rows,
            Err(error) => return error.to_string(),
        },
        // The clause is evaluated in Rust by the trigger matcher, so the scan
        // pages through the source's rows; `offset` and `limit` count matches.
        Some(clause) => {
            let mut matched = Vec::new();
            let mut skipped = 0;
            let mut scanned = 0;
            'scan: while scanned < MATCH_SCAN_ROWS {
                let page = match crate::storage::list_facts(
                    &orch.db.local,
                    filter(MATCH_SCAN_PAGE, scanned),
                )
                .await
                {
                    Ok(page) => page,
                    Err(error) => return error.to_string(),
                };
                let exhausted = (page.len() as i64) < MATCH_SCAN_PAGE;
                scanned += page.len() as i64;
                for row in page {
                    if !row_matches(clause, &row) {
                        continue;
                    }
                    if skipped < query.offset {
                        skipped += 1;
                    } else {
                        matched.push(row);
                        if matched.len() as i64 == query.limit {
                            break 'scan;
                        }
                    }
                }
                if exhausted {
                    break;
                }
            }
            if scanned >= MATCH_SCAN_ROWS && (matched.len() as i64) < query.limit {
                scan_note = Some(format!(
                    "\nOnly the newest {MATCH_SCAN_ROWS} rows were examined; narrow with `since`, `from`, or `to` to look further back.\n"
                ));
            }
            matched
        }
    };
    let keys = project_keys(orch).await;
    let batches = coalesced_wakes(orch, &rows).await;
    let title = match project {
        Some(project) => format!("# Events — {project}"),
        None => "# Events — workspace".to_string(),
    };
    let mut out = format!(
        "{title}\n\nNewest first. Each row points at its record; read the record for content.\n"
    );
    if rows.is_empty() {
        out.push_str("\nNo events match.\n");
        return out;
    }
    for row in &rows {
        let firings = firings_for(orch, row).await;
        out.push_str(&format!(
            "\n[#{}]({}) {} {}  {}\n  {} · {}\n  record: {}\n",
            row.id,
            row_uri(&keys, row),
            minted(row.minted_at),
            row.source,
            headline(row),
            wakes_segment(row, &batches),
            routes_segment(&keys, row, &firings),
            row.record_ref,
        ));
    }
    if rows.len() as i64 == query.limit {
        out.push_str(&format!(
            "\nMore rows may follow: add `offset={}`.\n",
            query.offset + query.limit
        ));
    }
    if let Some(note) = scan_note {
        out.push_str(&note);
    }
    out
}

/// What a source's `record_id` is a key of.
fn record_id_label(source: &str) -> &'static str {
    match source {
        "attention" => "issues.id",
        "thread_stream" => "transcript events.rowid",
        "github_comment" => "GitHub comment id",
        "file_changed" => "file_changes.id",
        "post_created" => "posts.id",
        "post_commented" => "post_comments.id",
        "ingress" => "ingress_events.id",
        _ => "record id",
    }
}

/// `cairn://p/{project}/events/{id}` (`project` set) or `cairn://routes/events/{id}`.
pub(crate) async fn member(
    orch: &Orchestrator,
    project: Option<&str>,
    id: i64,
    params: &[QueryParam],
) -> String {
    let mut explain: Option<String> = None;
    for param in params {
        match param.key.as_str() {
            "explain" if explain.is_some() => {
                return "Query parameter 'explain' must be specified once".to_string()
            }
            "explain" => match cairn_common::uri::parse_uri(param.value.trim()) {
                Some(resource) => explain = Some(resource.to_uri()),
                None => {
                    return format!(
                        "Query parameter 'explain' must be a thread or node URI, got '{}'",
                        param.value
                    )
                }
            },
            other => {
                return format!(
                    "Unsupported query parameter '{other}' for an event (accepted: explain)"
                )
            }
        }
    }
    let not_found = || match project {
        Some(project) => format!("Event not found: {}", build_project_event_uri(project, id)),
        None => format!("Event not found: {}", build_workspace_event_uri(id)),
    };
    let row = match crate::storage::get_fact(&orch.db.local, id).await {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(error) => return error.to_string(),
    };
    if let Some(project) = project {
        match project_id_for_key(orch, project).await {
            Ok(project_id) if row.project_id.as_deref() == Some(project_id.as_str()) => {}
            Ok(_) => return not_found(),
            Err(error) => return error,
        }
    }
    let keys = project_keys(orch).await;
    let firings = firings_for(orch, &row).await;
    let fields = serde_json::to_string_pretty(&fields_of(&row)).unwrap_or_default();
    let wakes = wakes_of(&row);
    let batches = coalesced_wakes(orch, std::slice::from_ref(&row)).await;
    let mut out = format!(
        "# Event #{} — {}\n\n{}\n\n",
        row.id,
        row.source,
        headline(&row)
    );
    out.push_str(&format!("- canonical: {}\n", row_uri(&keys, &row)));
    out.push_str(&format!("- minted: {}\n", minted(row.minted_at)));
    out.push_str(&format!(
        "- project: {}\n",
        row.project_id
            .as_ref()
            .map(|id| keys.get(id).cloned().unwrap_or_else(|| id.clone()))
            .unwrap_or_else(|| "none (workspace fact)".to_string())
    ));
    out.push_str(&format!("- identity: {}\n", row.identity));
    out.push_str(&format!("- presence: {}\n", row.presence));
    if let Some(actor) = &row.actor_job_id {
        out.push_str(&format!("- actor job: {actor}\n"));
    }
    if let Some(provenance) = &row.route_provenance {
        out.push_str(&format!("- produced by route: {provenance}\n"));
    }
    out.push_str(&format!(
        "- origin: {}\n",
        if row.origin_principal_json.is_some() {
            "recorded"
        } else {
            "none"
        }
    ));
    out.push_str(&format!(
        "\n## Record\n\n- ref: {}\n- {}: {}\n\nContent is not stored here; follow the ref.\n",
        row.record_ref,
        record_id_label(&row.source),
        row.record_id.as_deref().unwrap_or("none")
    ));
    out.push_str(&format!("\n## Fields\n\n```json\n{fields}\n```\n"));
    out.push_str("\n## Wakes\n\n");
    if wakes.is_empty() {
        out.push_str("No thread was woken.\n");
    } else {
        for wake in &wakes {
            let action = wake["action"].as_str().unwrap_or("?");
            let outcome = match wake["wake"].as_i64().and_then(|id| batches.get(&id)) {
                Some(batch) => format!(
                    "coalesced into wake #{} ({}, ids {}\u{2013}{}): {}",
                    batch.id,
                    plural(batch.count, "row"),
                    batch.first_row_id,
                    batch.last_row_id,
                    batch.delivered_action.clone().unwrap_or_else(|| format!(
                        "pending until {}",
                        minted(batch.deliver_after)
                    ))
                ),
                None => action.to_string(),
            };
            let via = wake["via"]
                .as_str()
                .map(|field| format!("addressed via {field} · "))
                .unwrap_or_default();
            out.push_str(&format!(
                "- {} · job {} · {via}{outcome}\n",
                wake["uri"].as_str().unwrap_or("?"),
                wake["job"].as_str().unwrap_or("?"),
            ));
        }
    }
    if let Some(home) = explain {
        out.push_str(&explain_segment(orch, &row, &home).await);
    }
    out.push_str(&format!(
        "\n## Routes\n\n{}\n",
        routes_segment(&keys, &row, &firings)
    ));
    out
}

/// Why this row did or did not wake the thread at `home`, clause by clause.
async fn explain_segment(orch: &Orchestrator, row: &FactRow, home: &str) -> String {
    let mut out = format!(
        "\n## Standing triggers of {home}\n\nEvaluated now, against the thread's current triggers. What was actually delivered is under Wakes, and can differ if the triggers changed since or the wake was muted.\n\n"
    );
    let addressed = match crate::routes::addressed_via(orch, row, home).await {
        Ok(addressed) => addressed,
        Err(error) => {
            out.push_str(&format!(
                "Could not resolve the row's addressees: {error}\n"
            ));
            None
        }
    };
    if let Some(field) = addressed {
        out.push_str(&format!(
            "- addressed via {field}: the row names {home}, which wakes it whatever its standing triggers say (unless it is the row's own write or outside its project)\n"
        ));
    }
    match crate::routes::explain_thread_triggers(orch, row, home).await {
        Err(error) => out.push_str(&format!("Could not evaluate: {error}\n")),
        Ok(verdicts) if verdicts.is_empty() && addressed.is_some() => {}
        Ok(verdicts) if verdicts.is_empty() => out.push_str(&format!(
            "{home} has no standing trigger on {}.\n",
            row.source
        )),
        Ok(verdicts) => {
            for verdict in verdicts {
                if verdict.reasons.is_empty() {
                    out.push_str(&format!("- would wake: `{}`\n", verdict.clause_ref));
                    continue;
                }
                out.push_str(&format!("- would not wake: `{}`\n", verdict.clause_ref));
                for reason in verdict.reasons {
                    out.push_str(&format!("  - {reason}\n"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> Vec<QueryParam> {
        pairs
            .iter()
            .map(|(key, value)| QueryParam {
                key: (*key).into(),
                value: (*value).into(),
            })
            .collect()
    }

    #[test]
    fn list_query_accepts_exactly_its_declared_keys() {
        assert_eq!(
            parse_list_query(&params(&[
                ("source", "file_changed"),
                ("since", "10"),
                ("from", "3"),
                ("to", "9"),
                ("limit", "5"),
                ("offset", "2")
            ]))
            .unwrap(),
            ListQuery {
                source: Some("file_changed".into()),
                since_ms: Some(10_000),
                from_id: Some(3),
                to_id: Some(9),
                limit: 5,
                offset: 2,
                matcher: None,
            }
        );
        assert!(parse_list_query(&params(&[("from", "9"), ("to", "3")]))
            .unwrap_err()
            .contains("must not exceed"));
        assert!(parse_list_query(&params(&[("from", "x")])).is_err());
        assert!(parse_list_query(&params(&[("grep", "x")]))
            .unwrap_err()
            .contains("Unsupported query parameter 'grep'"));
        assert!(parse_list_query(&params(&[("limit", "1"), ("limit", "2")]))
            .unwrap_err()
            .contains("once"));
        let unknown = parse_list_query(&params(&[("source", "tool_called")])).unwrap_err();
        assert!(
            unknown.contains("file_changed") && unknown.contains("thread_stream"),
            "{unknown}"
        );
        assert!(parse_list_query(&params(&[("limit", "201")])).is_err());
    }

    #[test]
    fn match_is_a_trigger_clause_over_stored_fields_of_the_named_source() {
        let clause = parse_list_query(&params(&[
            ("source", "ingress"),
            ("match", "endpoint:bugs,deliveryId:bug-report:7"),
        ]))
        .unwrap()
        .matcher
        .unwrap();
        assert_eq!(
            serde_json::to_value(&clause).unwrap(),
            serde_json::json!({"fact": "ingress", "endpoint": "bugs", "deliveryId": "bug-report:7"})
        );
        let refused = |pairs: &[(&str, &str)]| parse_list_query(&params(pairs)).unwrap_err();
        assert!(refused(&[("match", "endpoint:bugs")]).contains("needs `source=`"));
        assert!(
            refused(&[("source", "ingress"), ("match", "body:x")]).contains("stored fields only")
        );
        assert!(refused(&[("source", "ingress"), ("match", "mystery:x")]).contains("unknown field"));
        assert!(refused(&[("source", "ingress"), ("match", "endpoint")]).contains("field:value"));
        assert!(refused(&[("source", "ingress"), ("match", "fact:attention")]).contains("source="));
    }

    /// A listing filtered by `match=` and a thread whose standing trigger is
    /// the same clause select exactly the same rows.
    #[tokio::test]
    async fn match_and_a_standing_trigger_agree_on_which_rows_they_select() {
        use crate::ingress::consume::{consume, EndpointEvent};
        use crate::routes::fixture::{orchestrator, standing_triggers};

        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        for name in ["bugs", "ci"] {
            crate::ingress::upsert_endpoint(
                &db,
                &crate::ingress::EndpointRecord {
                    id: format!("src_{name}"),
                    channel_id: "ch-1".into(),
                    project_id: Some("p".into()),
                    name: name.into(),
                    provider: "first-party".into(),
                    topic: Some("cairn.bugs".into()),
                    status: "active".into(),
                    granted_at: None,
                    created_at: 1,
                    revoked_at: None,
                },
            )
            .await
            .unwrap();
        }
        let trigger = r#"[{"fact":"ingress","endpoint":"bugs","type":"bug.reported"}]"#;
        standing_triggers(&db, "watcher", trigger).await;
        let fixtures = [
            ("bugs", "bug.reported"),
            ("bugs", "bug.closed"),
            ("ci", "bug.reported"),
            ("bugs", "bug.reported"),
        ];
        for (n, (endpoint, event_type)) in fixtures.iter().enumerate() {
            consume(
                &orch,
                EndpointEvent {
                    relay_event_id: n as i64 + 1,
                    endpoint_id: format!("src_{endpoint}"),
                    delivery_id: None,
                    event_type: (*event_type).into(),
                    verification: Some("first-party".into()),
                    sender: None,
                    received_at: "2026-10-18T20:00:00.000Z".into(),
                    body: Ok(serde_json::json!({})),
                },
            )
            .await;
        }
        crate::routes::drain_fact_log(&orch).await;

        let woken: Vec<i64> = db
            .query_all(
                "SELECT id FROM fact_log WHERE source = 'ingress' AND wakes_json LIKE '%watcher%' ORDER BY id DESC",
                (),
                |row| crate::storage::RowExt::i64(row, 0),
            )
            .await
            .unwrap();
        let listed = list(
            &orch,
            Some("cairn"),
            &params(&[
                ("source", "ingress"),
                ("match", "endpoint:bugs,type:bug.reported"),
            ]),
        )
        .await;
        let listed_ids: Vec<i64> = listed
            .lines()
            .filter_map(|line| line.strip_prefix("[#"))
            .filter_map(|rest| rest.split(']').next()?.parse().ok())
            .collect();
        assert_eq!(woken.len(), 2, "{listed}");
        assert_eq!(listed_ids, woken, "{listed}");
    }
}
