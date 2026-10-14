//! Read-only rendering of the durable fact log: `cairn://p/{project}/events`,
//! its workspace view `cairn://routes/events`, and one row of either.
//!
//! A row is an index entry, so it renders as a pointer: what was minted, the
//! record it points at (followed, never inlined), the threads it woke, and what
//! its routes did. Wakes render beside the route outcome so a row that matched
//! no route but woke a thread never reads as "nothing happened".

use std::collections::HashMap;

use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_project_event_uri, build_project_route_history_entry_uri, build_route_history_entry_uri,
    build_workspace_event_uri,
};
use serde_json::Value;

use crate::orchestrator::Orchestrator;
use crate::routes::FactRegistry;
use crate::storage::{FactFilter, FactRow, FactScope, RouteFiringRecord};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

#[derive(Debug, PartialEq, Eq)]
struct ListQuery {
    source: Option<String>,
    since_ms: Option<i64>,
    limit: i64,
    offset: i64,
}

fn parse_list_query(params: &[QueryParam]) -> Result<ListQuery, String> {
    let mut query = ListQuery {
        source: None,
        since_ms: None,
        limit: DEFAULT_LIMIT,
        offset: 0,
    };
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
                    "Unsupported query parameter '{other}' for events (accepted: source, since, limit, offset)"
                ))
            }
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

fn wake_target(wake: &Value) -> String {
    let target = wake["uri"]
        .as_str()
        .or_else(|| wake["job"].as_str())
        .unwrap_or("?");
    match wake["action"].as_str() {
        Some("delivered") | None => target.to_string(),
        Some(action) => format!("{target} ({action})"),
    }
}

fn wakes_segment(row: &FactRow) -> String {
    let wakes = wakes_of(row);
    if wakes.is_empty() {
        return "woke no threads".to_string();
    }
    let delivered = wakes
        .iter()
        .filter(|wake| wake["action"].as_str() == Some("delivered"))
        .count();
    format!(
        "woke {delivered} thread{}: {}",
        if delivered == 1 { "" } else { "s" },
        wakes.iter().map(wake_target).collect::<Vec<_>>().join(", ")
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
    let rows = match crate::storage::list_facts(
        &orch.db.local,
        FactFilter {
            scope,
            source: query.source.clone(),
            since_ms: query.since_ms,
            limit: query.limit,
            offset: query.offset,
        },
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => return error.to_string(),
    };
    let keys = project_keys(orch).await;
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
            wakes_segment(row),
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
    out
}

/// What a source's `record_id` is a key of.
fn record_id_label(source: &str) -> &'static str {
    match source {
        "attention" => "issues.id",
        "thread_stream" => "transcript events.rowid",
        "github_comment" => "GitHub comment id",
        "file_changed" => "file_changes.id",
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
            out.push_str(&format!(
                "- {} · job {} · {}\n",
                wake["uri"].as_str().unwrap_or("?"),
                wake["job"].as_str().unwrap_or("?"),
                wake["action"].as_str().unwrap_or("?")
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
    match crate::routes::explain_thread_triggers(orch, row, home).await {
        Err(error) => out.push_str(&format!("Could not evaluate: {error}\n")),
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
                ("limit", "5"),
                ("offset", "2")
            ]))
            .unwrap(),
            ListQuery {
                source: Some("file_changed".into()),
                since_ms: Some(10_000),
                limit: 5,
                offset: 2
            }
        );
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
}
