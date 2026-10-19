//! `cairn://p/{project}/tenets` and `cairn://p/{project}/tenets/{number}`.
//!
//! A verdict never renders without who gave it and when: the answerer label
//! and the answer time sit on the same line as the verdict, on every surface,
//! because an unverified desktop answer and a signed-in one release a gate the
//! same way today and a reader must be able to tell them apart.

use cairn_common::query::QueryParam;
use cairn_common::uri::{build_project_tenets_uri, build_thread_uri};

use crate::storage::LocalDb;
use crate::tenets::{TenetFilter, TenetStatus, TenetView, Verdict};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

fn value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
        .filter(|value| !value.is_empty())
}

fn wants_json(params: &[QueryParam]) -> Result<bool, String> {
    match value(params, "format") {
        None => Ok(false),
        Some("json") => Ok(true),
        Some(other) => Err(format!("tenets format must be json, not {other}")),
    }
}

async fn project_id(db: &LocalDb, project_key: &str) -> Result<String, String> {
    crate::mcp::handlers::run_context::project_id_by_key(db, project_key).await
}

/// The verdict line, with its answerer and time always beside it.
pub(crate) fn verdict_line(view: &TenetView) -> Option<String> {
    let row = &view.row;
    let decided = match (row.status, row.verdict) {
        (TenetStatus::Proposed, _) => return Some("Awaiting the operator's verdict".into()),
        (TenetStatus::Withdrawn, _) => {
            return Some("Withdrawn by its thread before a verdict".into())
        }
        (_, Some(Verdict::Yes)) => "Approved: yes",
        (_, Some(Verdict::YourCall)) => "Approved: your call (the thread's judgment, trusted)",
        (_, Some(Verdict::No)) => "Declined: no",
        (_, None) => "Answered",
    };
    let by = view.answerer_label.as_deref().unwrap_or("unattributed");
    let at = row
        .answered_at
        .map(crate::clock::age)
        .unwrap_or_else(|| "at an unrecorded time".into());
    let superseded = match (row.status, view.superseded_by) {
        (TenetStatus::Superseded, Some(successor)) => format!("; since superseded by #{successor}"),
        (TenetStatus::Superseded, None) => "; since superseded".to_string(),
        _ => String::new(),
    };
    Some(format!("{decided} — {by}, {at}{superseded}"))
}

fn thread_link(view: &TenetView) -> String {
    match &view.proposing_thread_name {
        Some(name) => format!(
            "[{}]({})",
            view.proposing_thread,
            build_thread_uri(&view.project_key, name)
        ),
        None => format!("{} (thread no longer exists)", view.proposing_thread),
    }
}

fn summary(view: &TenetView) -> String {
    let row = &view.row;
    let mut out = format!(
        "## [#{}]({}) · {}\n\n{}\n\n",
        row.number,
        view.uri,
        row.status.as_str(),
        row.statement
    );
    if let Some(line) = verdict_line(view) {
        out.push_str(&format!("- {line}\n"));
    }
    if let Some(comment) = &row.comment {
        out.push_str(&format!("- Comment: {comment}\n"));
    }
    out.push_str(&format!("- Thread: {}\n", thread_link(view)));
    if let Some(slug) = &row.slug {
        out.push_str(&format!("- Slug: `{slug}`\n"));
    }
    out
}

/// `cairn://p/{project}/tenets`.
pub(crate) async fn read_collection(
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    match collection(db, project_key, params).await {
        Ok(text) => text,
        Err(error) => error,
    }
}

async fn collection(
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    for param in params {
        if !matches!(
            param.key.as_str(),
            "status" | "thread" | "slug" | "search" | "limit" | "format"
        ) {
            return Err(format!("Unsupported tenets query parameter: {}", param.key));
        }
    }
    let json = wants_json(params)?;
    let limit = match value(params, "limit") {
        None => DEFAULT_LIMIT,
        Some(limit) => limit
            .parse::<usize>()
            .ok()
            .filter(|limit| *limit > 0)
            .ok_or_else(|| "tenets limit must be a positive integer".to_string())?
            .min(MAX_LIMIT),
    };
    let (statuses, oldest_first) = crate::tenets::status_filter(value(params, "status"))?;
    let project_id = project_id(db, project_key).await?;
    let thread_id = match value(params, "thread") {
        None => None,
        Some(name) => Some(crate::tenets::thread_named(db, &project_id, name).await?),
    };
    let filter = TenetFilter {
        statuses,
        thread_id,
        slug: value(params, "slug").map(str::to_string),
        search: value(params, "search").map(str::to_string),
        oldest_first,
        limit: Some(limit),
    };
    let views = crate::tenets::list(db, &project_id, &filter).await?;
    if json {
        return serde_json::to_string(&views).map_err(|error| error.to_string());
    }
    let policy = crate::tenets::load_policy(db, &project_id).await?;
    let pending = crate::tenets::pending_count(db, &project_id).await?;
    let collection_uri = build_project_tenets_uri(project_key);
    let mut out = format!(
        "# Tenets\n\n{} · {}. Inbox: [{collection_uri}?status=pending]({collection_uri}?status=pending)\n\n",
        awaiting(pending),
        policy.label(),
    );
    if views.is_empty() {
        out.push_str("No tenets match.\n");
        return Ok(out);
    }
    out.push_str(&views.iter().map(summary).collect::<Vec<_>>().join("\n"));
    Ok(out)
}

/// "N tenets awaiting a verdict", in the project's own words.
pub(crate) fn awaiting(pending: i64) -> String {
    match pending {
        1 => "1 tenet awaiting a verdict".to_string(),
        n => format!("{n} tenets awaiting a verdict"),
    }
}

/// `cairn://p/{project}/tenets/{key}`. A slug read renders the canonical
/// number.
pub(crate) async fn read_entity(
    db: &LocalDb,
    project_key: &str,
    key: &str,
    params: &[QueryParam],
) -> String {
    match entity(db, project_key, key, params).await {
        Ok(text) => text,
        Err(error) => error,
    }
}

async fn entity(
    db: &LocalDb,
    project_key: &str,
    key: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    for param in params {
        if param.key != "format" {
            return Err(format!("Unsupported tenet query parameter: {}", param.key));
        }
    }
    let json = wants_json(params)?;
    let project_id = project_id(db, project_key).await?;
    let number = crate::tenets::resolve_number(db, &project_id, key)
        .await?
        .ok_or_else(|| format!("No tenet `{key}` in project {project_key}."))?;
    let view = crate::tenets::get(db, &project_id, number)
        .await?
        .ok_or_else(|| format!("No tenet #{number} in project {project_key}."))?;
    if json {
        return serde_json::to_string(&view).map_err(|error| error.to_string());
    }
    Ok(render_entity(&view))
}

/// How a tenet's `supersedes` reads. The replacement is in effect only once
/// the successor is approved; a declined or withdrawn successor replaced
/// nothing, and its predecessor stands.
fn supersedes_line(status: TenetStatus, previous: &str) -> String {
    match status {
        TenetStatus::Approved | TenetStatus::Superseded => format!("Supersedes: {previous}"),
        TenetStatus::Proposed => {
            format!("Would supersede {previous} (takes effect only if this tenet is approved)")
        }
        TenetStatus::Declined => {
            format!(
                "Proposed to supersede {previous}; not in effect, because this tenet was declined"
            )
        }
        TenetStatus::Withdrawn => {
            format!(
                "Proposed to supersede {previous}; not in effect, because this tenet was withdrawn"
            )
        }
    }
}

fn render_entity(view: &TenetView) -> String {
    let row = &view.row;
    let mut out = format!(
        "# Tenet #{} · {}\n\n{}\n\n",
        row.number,
        row.status.as_str(),
        row.statement,
    );
    out.push_str("## Verdict\n\n");
    if let Some(line) = verdict_line(view) {
        out.push_str(&format!("- {line}\n"));
    }
    if let Some(comment) = &row.comment {
        out.push_str(&format!("- Comment: {comment}\n"));
    }
    out.push_str(&format!("- {}\n\n", view.policy_label));

    out.push_str("## Provenance\n\n");
    out.push_str(&format!("- Canonical: {}\n", view.uri));
    out.push_str(&format!("- Proposed by thread: {}\n", thread_link(view)));
    out.push_str(&format!("- Origin: {}\n", row.origin.as_str()));
    if let Some(slug) = &row.slug {
        out.push_str(&format!("- Slug: `{slug}` (an alias of #{})\n", row.number));
    }
    out.push_str(&format!(
        "- Proposed: {}\n",
        crate::clock::age(row.created_at)
    ));
    if view.thread_your_calls > 0 {
        out.push_str(&format!(
            "- This thread's tenets answered \"your call\": {}\n",
            view.thread_your_calls
        ));
    }
    let tenets = build_project_tenets_uri(&view.project_key);
    if let Some(previous) = row.supersedes {
        out.push_str(&format!(
            "- {}\n",
            supersedes_line(row.status, &format!("[#{previous}]({tenets}/{previous})"))
        ));
    }
    if let Some(successor) = view.superseded_by {
        out.push_str(&format!(
            "- Superseded by: [#{successor}]({tenets}/{successor})\n"
        ));
    }
    if let Some(legacy) = &row.legacy {
        out.push_str("\n## Migrated ruling\n\n");
        for field in ["rationale", "provenance"] {
            match legacy.get(field) {
                Some(serde_json::Value::String(text)) => {
                    out.push_str(&format!("- {field}: {text}\n"))
                }
                Some(serde_json::Value::Null) | None => {}
                Some(other) => out.push_str(&format!("- {field}: {other}\n")),
            }
        }
    }

    out.push_str("\n## Gates\n\n");
    if view.gates.is_empty() {
        out.push_str("Gates no execution.\n");
    }
    for gate in &view.gates {
        let state = if gate.foreclosed {
            "foreclosed: won't start"
        } else if gate.released {
            "released"
        } else if gate.satisfied {
            "this verdict satisfied its gate; the execution still waits on its other gates"
        } else {
            "held on this verdict"
        };
        let target = match (&gate.issue, gate.exec_seq) {
            (Some(issue), Some(seq)) => format!("{issue} (execution {seq})"),
            (Some(issue), None) => issue.clone(),
            _ => format!("execution {}", gate.execution_id),
        };
        out.push_str(&format!("- {target} — {state}\n"));
    }

    out.push_str("\n## Evidence\n\n");
    if view.evidence.is_empty() {
        out.push_str("None recorded. Evidence is gathered automatically; agents never write it.\n");
    } else {
        let count = |sign: &str| {
            view.evidence
                .iter()
                .filter(|evidence| evidence.sign == sign)
                .count()
        };
        out.push_str(&format!(
            "{} supports · {} contradicts\n\n",
            count("supports"),
            count("contradicts")
        ));
    }
    for evidence in &view.evidence {
        out.push_str(&format!(
            "- {} {} ({}, {})\n",
            evidence.sign,
            evidence.source_uri,
            evidence.measurement(),
            crate::tenets::evidence::utc_date(evidence.evaluated_at, "%Y-%m-%d")
        ));
    }
    out
}
