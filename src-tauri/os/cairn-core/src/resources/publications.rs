//! Read views of the publication ledger (`cairn://p/{project}/publish`): the
//! publications newest first, one publication's grid of targets by legs, and
//! one leg's log. The ledger is private to this install and read-only here;
//! appending a start is the write dispatch's.
use super::paging::RecordPage;
use crate::orchestrator::Orchestrator;
use crate::publications::{leg_log_path, Publication, PublicationLeg};
use cairn_common::query::QueryParam;
use cairn_uri::{
    build_project_publication_leg_log_uri, build_project_publication_uri, build_project_publish_uri,
};

const NOTICE: &str = "Private to this install. Every publication is a shadow in this phase: its build legs leave evidence (asset hashes, a manifest-shape comparison with the live release) and never upload. Append {action:\"start\"} to start one.";

fn page_bounds(params: &[QueryParam]) -> Result<(usize, usize), String> {
    let mut offset = None;
    let mut limit = None;
    for param in params {
        let slot = match param.key.as_str() {
            "offset" => &mut offset,
            "limit" => &mut limit,
            other => {
                return Err(format!(
                    "Unsupported publication ledger query parameter '{other}'"
                ))
            }
        };
        let value = param
            .value
            .parse::<usize>()
            .map_err(|_| format!("{} must be a nonnegative integer", param.key))?;
        if slot.replace(value).is_some() {
            return Err(format!(
                "Duplicate publication ledger query parameter '{}'",
                param.key
            ));
        }
    }
    let limit = limit.unwrap_or(25);
    if !(1..=199).contains(&limit) {
        return Err("Publication ledger limit must be between 1 and 199".into());
    }
    Ok((offset.unwrap_or(0), limit))
}

fn when(ms: Option<i64>) -> String {
    ms.and_then(chrono::DateTime::from_timestamp_millis)
        .map(|at| at.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| "-".into())
}

/// A table cell: one line, no pipe that would split it.
fn cell(text: &str) -> String {
    let flat = text.replace(['\n', '\r'], " ").replace('|', "\\|");
    if flat.is_empty() {
        "-".into()
    } else {
        flat
    }
}

fn json_cell(value: Option<&serde_json::Value>) -> String {
    value.map_or_else(|| "-".into(), |value| cell(&value.to_string()))
}

fn short(commit: Option<&str>) -> String {
    commit.map_or_else(|| "-".into(), |commit| commit.chars().take(12).collect())
}

fn headline(project: &str, row: &Publication) -> String {
    format!(
        "[{}]({}) · {} {} · {} · {} · commit {} · protocol {} · {} · {}",
        row.id,
        build_project_publication_uri(project, &row.id),
        row.mode,
        row.kind,
        row.trigger,
        row.state,
        short(row.commit.as_deref()),
        row.protocol.map_or_else(|| "-".into(), |p| format!("v{p}")),
        when(Some(row.created_at)),
        row.summary.as_deref().unwrap_or(""),
    )
}

async fn project_id(orch: &Orchestrator, project: &str) -> Result<String, String> {
    crate::project_services::project_id_for_key(orch, project).await
}

pub(super) async fn list_page(
    orch: &Orchestrator,
    project: &str,
    params: &[QueryParam],
) -> Result<RecordPage, String> {
    let (offset, limit) = page_bounds(params)?;
    let project_id = project_id(orch, project).await?;
    let mut rows = orch
        .db
        .local
        .list_publications(&project_id, limit + 1, offset)
        .await
        .map_err(|error| error.to_string())?;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let shown = rows.len();
    let mut body = format!("# Publications\n\n{NOTICE}\n\n");
    for row in &rows {
        body.push_str(&format!("- {}\n", headline(project, row)));
    }
    if shown == 0 {
        body.push_str("No publications recorded on this page.\n");
    }
    Ok(RecordPage {
        body,
        noun: "publications",
        offset,
        limit,
        shown,
        total: None,
        more,
        prelude_lines: None,
    })
}

pub(super) async fn read_collection(
    orch: &Orchestrator,
    project: &str,
    params: &[QueryParam],
) -> String {
    match list_page(orch, project, params).await {
        Ok(page) => page.body,
        Err(error) => format!(
            "Unable to read {}: {error}",
            build_project_publish_uri(project)
        ),
    }
}

fn asset_cell(leg: &PublicationLeg) -> String {
    if leg.assets.is_empty() {
        return "-".into();
    }
    leg.assets
        .iter()
        .map(|asset| {
            format!(
                "{} {}",
                asset.name,
                &asset.sha256[..asset.sha256.len().min(12)]
            )
        })
        .collect::<Vec<_>>()
        .join("<br>")
}

/// One publication: its identity, then a row per (target, leg) with every
/// ledger column.
pub(crate) fn render_publication(
    project: &str,
    row: &Publication,
    legs: &[PublicationLeg],
) -> String {
    let mut out = format!(
        "# Publication {}\n\n{NOTICE}\n\n- kind: {}\n- mode: {}\n- trigger: {}\n- requested by: {}\n- state: {}\n- commit: {}\n- protocol: {}\n- forced targets: {}\n- started: {}\n- ended: {}\n- summary: {}\n\n",
        row.id,
        row.kind,
        row.mode,
        row.trigger,
        row.requested_by.as_deref().unwrap_or("-"),
        row.state,
        row.commit.as_deref().unwrap_or("-"),
        row.protocol.map_or_else(|| "-".into(), |p| format!("v{p}")),
        if row.force_targets.is_empty() { "-".into() } else { row.force_targets.join(", ") },
        when(Some(row.created_at)),
        when(row.ended_at),
        row.summary.as_deref().unwrap_or("-"),
    );
    out.push_str("| target | leg | builder | builder kind | state | reason | started | ended | assets (sha256) | location | signer | verifier | build jobs | log |\n");
    out.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    for leg in legs {
        let log = leg.log_ref.clone().unwrap_or_else(|| {
            build_project_publication_leg_log_uri(project, &row.id, &leg.target, &leg.leg)
        });
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            cell(&leg.target),
            leg.leg,
            cell(leg.builder.as_deref().unwrap_or("")),
            leg.builder_kind,
            leg.state,
            cell(leg.reason.as_deref().unwrap_or("")),
            when(leg.started_at),
            when(leg.ended_at),
            asset_cell(leg),
            cell(leg.artifact_location.as_deref().unwrap_or("")),
            json_cell(leg.signer.as_ref()),
            json_cell(leg.verifier.as_ref()),
            json_cell(leg.build_jobs.as_ref()),
            log,
        ));
    }
    if legs.is_empty() {
        out.push_str("\nNo legs laid out yet.\n");
    }
    if let Some(plan) = &row.plan {
        out.push_str("\n## Plan document\n\n```json\n");
        out.push_str(
            &serde_json::to_string_pretty(plan)
                .unwrap_or_default()
                .replace("```", "` ``"),
        );
        out.push_str("\n```\n");
    }
    out
}

pub(super) async fn read_publication(
    orch: &Orchestrator,
    project: &str,
    id: &str,
    params: &[QueryParam],
) -> String {
    if let Some(param) = params.first() {
        return format!(
            "Query parameter `{}` is not supported on a publication",
            param.key
        );
    }
    let uri = build_project_publication_uri(project, id);
    let project_id = match project_id(orch, project).await {
        Ok(id) => id,
        Err(error) => return error,
    };
    match orch.db.local.get_publication(id).await {
        Ok(Some(row)) if row.project_id == project_id => {
            match orch.db.local.publication_legs(id).await {
                Ok(legs) => render_publication(project, &row, &legs),
                Err(error) => format!("Unable to read {uri}: {error}"),
            }
        }
        Ok(_) => format!("No publication is recorded at {uri}."),
        Err(error) => format!("Unable to read {uri}: {error}"),
    }
}

/// The lines of a log after `since` (a 1-based line number), at most `limit`.
pub(crate) fn window(text: &str, since: usize, limit: usize) -> (Vec<(usize, &str)>, bool) {
    let mut lines = text
        .lines()
        .enumerate()
        .map(|(index, line)| (index + 1, line))
        .skip(since);
    let shown: Vec<_> = lines.by_ref().take(limit).collect();
    (shown, lines.next().is_some())
}

pub(super) async fn read_leg_log(
    orch: &Orchestrator,
    project: &str,
    id: &str,
    target: &str,
    leg: &str,
    params: &[QueryParam],
) -> String {
    let mut since = 0usize;
    let mut limit = 2_000usize;
    for param in params {
        let value = param.value.parse::<usize>();
        match (param.key.as_str(), value) {
            ("since", Ok(value)) => since = value,
            ("limit", Ok(value)) if value > 0 => limit = value.min(20_000),
            ("since" | "limit", _) => {
                return format!(
                    "`{}` must be a positive integer, not `{}`",
                    param.key, param.value
                )
            }
            (other, _) => {
                return format!("Query parameter `{other}` is not supported on a leg log")
            }
        }
    }
    let uri = build_project_publication_leg_log_uri(project, id, target, leg);
    let project_id = match project_id(orch, project).await {
        Ok(id) => id,
        Err(error) => return error,
    };
    match orch.db.local.get_publication(id).await {
        Ok(Some(row)) if row.project_id == project_id => {}
        Ok(_) => {
            return format!(
                "No publication is recorded at {}.",
                build_project_publication_uri(project, id)
            )
        }
        Err(error) => return format!("Unable to read {uri}: {error}"),
    }
    let path = leg_log_path(&orch.config_dir, id, target, leg);
    let text = match tokio::fs::read_to_string(&path).await {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return format!("{uri} has no log yet: the leg has not been placed, or never ran.")
        }
        Err(error) => return format!("Unable to read {uri}: {error}"),
    };
    let (lines, more) = window(&text, since, limit);
    let mut out = format!("# {target} {leg} log of publication {id}\n\n```text\n");
    for (_, line) in &lines {
        out.push_str(&line.replace("```", "` ``"));
        out.push('\n');
    }
    out.push_str("```\n");
    if more {
        let next = lines.last().map_or(since, |(number, _)| *number);
        out.push_str(&format!("\ncontinue: {uri}?since={next}&limit={limit}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::storage::publications::LegAsset;
    use serde_json::json;

    fn params(query: &str) -> Vec<QueryParam> {
        cairn_common::query::parse_query_params(query).unwrap()
    }

    #[test]
    fn pagination_is_bounded_and_typed() {
        assert_eq!(page_bounds(&[]).unwrap(), (0, 25));
        assert_eq!(page_bounds(&params("offset=3&limit=1")).unwrap(), (3, 1));
        for query in [
            "limit=0",
            "limit=200",
            "limit=x",
            "offset=1&offset=2",
            "state=queued",
        ] {
            assert!(page_bounds(&params(query)).is_err(), "{query}");
        }
    }

    #[test]
    fn a_log_window_continues_where_it_stopped() {
        let text = "a\nb\nc\nd\n";
        let (lines, more) = window(text, 1, 2);
        assert_eq!(lines, [(2, "b"), (3, "c")]);
        assert!(more);
        let (lines, more) = window(text, 3, 2);
        assert_eq!(lines, [(4, "d")]);
        assert!(!more);
    }

    #[test]
    fn a_publication_renders_every_ledger_column_one_line_per_leg() {
        let row = Publication {
            id: "pub-1".into(),
            project_id: "p".into(),
            kind: "executor".into(),
            commit: Some("a".repeat(40)),
            protocol: Some(171),
            trigger: "manual".into(),
            requested_by: Some("operator".into()),
            force_targets: vec![],
            tool: None,
            plan: Some(json!({"protocolVersion": 171})),
            mode: "shadow".into(),
            state: "settled".into(),
            summary: Some("done".into()),
            created_at: 1_794_493_559_000,
            ended_at: Some(1_794_493_659_000),
        };
        let leg = PublicationLeg {
            publication_id: "pub-1".into(),
            target: "x86_64-unknown-linux-gnu".into(),
            leg: "build".into(),
            seq: 1,
            builder: Some("fedora".into()),
            builder_kind: "fleet".into(),
            state: "deferred".into(),
            reason: Some("deferred: no eligible builder | x\nsecond line".into()),
            started_at: Some(1_794_493_560_000),
            ended_at: Some(1_794_493_600_000),
            assets: vec![LegAsset {
                name: "cairn-exec".into(),
                sha256: "f".repeat(64),
                size: 3,
            }],
            artifact_location: None,
            signer: None,
            verifier: Some(json!({"check": "manifest-shape", "result": "pass"})),
            build_jobs: Some(json!({"jobs": 8})),
            log_ref: None,
            request_id: None,
        };
        let rendered = render_publication("cairn", &row, &[leg]);
        let table_row = rendered
            .lines()
            .find(|line| line.starts_with("| x86_64-unknown-linux-gnu"))
            .expect("one line per leg");
        for expected in [
            "fedora",
            "fleet",
            "deferred",
            "no eligible builder \\| x second line",
            "cairn-exec ffffffffffff",
            "manifest-shape",
            "\"jobs\":8",
            "cairn://p/cairn/publish/pub-1/x86_64-unknown-linux-gnu/build/log",
        ] {
            assert!(table_row.contains(expected), "{expected}: {table_row}");
        }
        assert_eq!(table_row.matches(" | ").count(), 13, "fourteen columns");
        assert!(rendered.contains("protocol: v171") && rendered.contains("## Plan document"));
    }
}
