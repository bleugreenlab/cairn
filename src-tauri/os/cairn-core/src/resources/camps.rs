//! `cairn://p/{project}/camps`, `/camps/{slug}` and `/camps/{slug}/participants`.
//!
//! Everything a camp page shows except its title is generated from the rows
//! that own it: the lead from the camp row, levels from the subscription rows,
//! the roll-call from the issues joined to it, history from the journal.

use std::collections::HashMap;

use cairn_common::query::QueryParam;
use cairn_db::storage::camps::{
    camp_by_slug_conn, camp_history_conn, camp_issues_conn, camp_participants_conn,
    list_camps_conn, CampEvent, CampIssue,
};
use cairn_uri::{
    build_issue_uri, build_project_camp_participants_uri, build_project_camp_uri,
    build_project_camps_uri, build_thread_uri,
};
use serde_json::json;

use crate::models::{Camp, CampParticipant, CampSubject};
use crate::storage::{LocalDb, RowExt};

const HISTORY_LIMIT: i64 = 20;

fn wants_json(params: &[QueryParam]) -> Result<bool, String> {
    for param in params {
        if param.key != "format" {
            return Err(format!("Unsupported camps query parameter: {}", param.key));
        }
    }
    match params.iter().find(|param| param.key == "format") {
        None => Ok(false),
        Some(param) if param.value == "json" => Ok(true),
        Some(param) => Err(format!("camps format must be json, not {}", param.value)),
    }
}

/// The project row a camps read stands on.
async fn project(db: &LocalDb, project_key: &str) -> Result<(String, String), String> {
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(db, project_key).await?;
    Ok((project_id, cairn_uri::canonical_project(project_key)))
}

/// Thread id to name, for rendering a stable subject under its current name.
async fn thread_names(db: &LocalDb, project_id: &str) -> Result<HashMap<String, String>, String> {
    db.query_all(
        "SELECT id, name FROM threads WHERE project_id = ?1",
        (project_id.to_string(),),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map(|rows| rows.into_iter().collect())
    .map_err(|error| format!("Failed to load the project's threads: {error}"))
}

fn subject_label(
    subject: &CampSubject,
    project_key: &str,
    names: &HashMap<String, String>,
) -> String {
    match subject {
        CampSubject::Thread { thread_id } => match names.get(thread_id) {
            Some(name) => format!("[{name}]({})", build_thread_uri(project_key, name)),
            None => format!("thread {thread_id} (no longer exists)"),
        },
        CampSubject::Human {
            issuer, subject, ..
        } => format!("{subject} ({issuer})"),
    }
}

fn subject_name(subject: &CampSubject, names: &HashMap<String, String>) -> String {
    match subject {
        CampSubject::Thread { thread_id } => names
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| thread_id.clone()),
        CampSubject::Human { subject, .. } => subject.clone(),
    }
}

struct CampView {
    camp: Camp,
    participants: Vec<CampParticipant>,
    issues: Vec<CampIssue>,
    history: Vec<CampEvent>,
}

async fn camp_view(db: &LocalDb, project_id: &str, slug: &str) -> Result<Option<CampView>, String> {
    let (project_id, wanted) = (project_id.to_string(), slug.to_string());
    db.read(move |conn| {
        let (project_id, slug) = (project_id.clone(), wanted.clone());
        Box::pin(async move {
            let Some(camp) = camp_by_slug_conn(conn, &project_id, &slug).await? else {
                return Ok(None);
            };
            let participants = camp_participants_conn(conn, &camp).await?;
            let issues = camp_issues_conn(conn, &camp.id).await?;
            let history = camp_history_conn(conn, &camp.id, HISTORY_LIMIT).await?;
            Ok(Some(CampView {
                camp,
                participants,
                issues,
                history,
            }))
        })
    })
    .await
    .map_err(|error| format!("Failed to load camp `{slug}`: {error}"))
}

fn missing_camp(project_key: &str, slug: &str) -> String {
    if slug == crate::models::BASE_CAMP_SLUG {
        format!(
            "Base camp is not established for project {project_key} yet. It is created with the project and backfilled at boot; the boot log names why it failed. Restarting Cairn retries it."
        )
    } else {
        format!(
            "No camp `{slug}` in project {project_key}. List the project's camps at {}.",
            build_project_camps_uri(project_key)
        )
    }
}

/// `cairn://p/{project}/camps`.
pub(crate) async fn read_camps(db: &LocalDb, project_key: &str, params: &[QueryParam]) -> String {
    match camps(db, project_key, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn camps(db: &LocalDb, project_key: &str, params: &[QueryParam]) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let names = thread_names(db, &project_id).await?;
    let listed = {
        let project_id = project_id.clone();
        db.read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let camps = list_camps_conn(conn, &project_id).await?;
                let mut rows = Vec::with_capacity(camps.len());
                for camp in camps {
                    let participants = camp_participants_conn(conn, &camp).await?.len();
                    let issues = camp_issues_conn(conn, &camp.id).await?.len();
                    rows.push((camp, participants, issues));
                }
                Ok(rows)
            })
        })
        .await
        .map_err(|error| format!("Failed to list camps: {error}"))?
    };
    if json {
        let camps: Vec<_> = listed
            .iter()
            .map(|(camp, participants, issues)| {
                json!({
                    "uri": build_project_camp_uri(&project_key, &camp.slug),
                    "slug": camp.slug,
                    "title": camp.title,
                    "kind": camp.kind,
                    "status": camp.status,
                    "lead": names.get(&camp.lead_thread_id).map(|name| build_thread_uri(&project_key, name)),
                    "participants": participants,
                    "issues": issues,
                })
            })
            .collect();
        return Ok(serde_json::to_string_pretty(&json!({ "camps": camps })).unwrap_or_default());
    }
    let mut out = format!("# Camps — {project_key}\n\n");
    if !listed.iter().any(|(camp, _, _)| camp.is_base()) {
        out.push_str(&format!(
            "{}\n\n",
            missing_camp(&project_key, crate::models::BASE_CAMP_SLUG)
        ));
    }
    for (camp, participants, issues) in &listed {
        let lead = subject_name(
            &CampSubject::Thread {
                thread_id: camp.lead_thread_id.clone(),
            },
            &names,
        );
        out.push_str(&format!(
            "- [{}]({}) — {} · lead {lead} · {participants} participant{} · {issues} issue{} · {}\n",
            camp.slug,
            build_project_camp_uri(&project_key, &camp.slug),
            camp.title,
            if *participants == 1 { "" } else { "s" },
            if *issues == 1 { "" } else { "s" },
            camp.status.as_str(),
        ));
    }
    if listed.is_empty() {
        out.push_str("No camps.\n");
    }
    Ok(out)
}

fn participant_line(
    participant: &CampParticipant,
    project_key: &str,
    names: &HashMap<String, String>,
) -> String {
    let mut line = format!(
        "- {} — {}",
        subject_label(&participant.subject, project_key, names),
        participant.role.as_str()
    );
    if participant.lead {
        if let Some(level) = participant.level {
            line.push_str(&format!(" (also a {} member)", level.as_str()));
        }
    }
    if let Some(reason) = &participant.reason {
        line.push_str(&format!(": {reason}"));
    }
    line.push_str(&format!(
        " · since {}",
        crate::clock::age(participant.since)
    ));
    line
}

fn participant_json(
    participant: &CampParticipant,
    project_key: &str,
    names: &HashMap<String, String>,
) -> serde_json::Value {
    let thread = participant
        .subject
        .thread_id()
        .and_then(|id| names.get(id))
        .map(|name| build_thread_uri(project_key, name));
    json!({
        "subject": participant.subject,
        "thread": thread,
        "role": participant.role,
        "lead": participant.lead,
        "level": participant.level,
        "reason": participant.reason,
        "since": participant.since,
    })
}

fn history_line(event: &CampEvent, project_key: &str, names: &HashMap<String, String>) -> String {
    let subject = event.subject_key.as_deref().map(|key| {
        let subject = if CampSubject::key_is_thread(key) {
            CampSubject::Thread {
                thread_id: key.to_string(),
            }
        } else {
            return key.to_string();
        };
        subject_label(&subject, project_key, names)
    });
    let what = match (
        event.kind.as_str(),
        event.old_value.as_deref(),
        event.new_value.as_deref(),
    ) {
        ("pitched", _, _) => format!("pitched, led by {}", subject.unwrap_or_default()),
        ("joined", _, Some(level)) => format!("{} joined at {level}", subject.unwrap_or_default()),
        ("role_changed", Some(old), Some(new)) => {
            format!("{} moved from {old} to {new}", subject.unwrap_or_default())
        }
        ("left", _, _) => format!("{} left", subject.unwrap_or_default()),
        ("retitled", _, Some(title)) => format!("retitled to \"{title}\""),
        (kind, _, _) => kind.replace('_', " "),
    };
    let reason = event
        .reason
        .as_deref()
        .map(|reason| format!(" — {reason}"))
        .unwrap_or_default();
    let by = match &event.actor {
        None => " (system)".to_string(),
        Some(cairn_common::identity::PrincipalRef::Agent { node, .. }) => format!(" by {node}"),
        Some(_) => String::new(),
    };
    format!(
        "- {}: {what}{reason}{by}",
        crate::clock::age(event.created_at)
    )
}

/// `cairn://p/{project}/camps/{slug}`.
pub(crate) async fn read_camp(
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    match camp(db, project_key, slug, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn camp(
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let Some(view) = camp_view(db, &project_id, slug).await? else {
        return Err(missing_camp(&project_key, slug));
    };
    let names = thread_names(db, &project_id).await?;
    let CampView {
        camp,
        participants,
        issues,
        history,
    } = view;
    if json {
        let value = json!({
            "uri": build_project_camp_uri(&project_key, &camp.slug),
            "camp": camp,
            "participants": participants.iter().map(|p| participant_json(p, &project_key, &names)).collect::<Vec<_>>(),
            "issues": issues.iter().map(|issue| json!({
                "uri": build_issue_uri(&project_key, issue.number as i32),
                "title": issue.title,
                "status": issue.status,
            })).collect::<Vec<_>>(),
        });
        return Ok(serde_json::to_string_pretty(&value).unwrap_or_default());
    }
    let lead = subject_label(
        &CampSubject::Thread {
            thread_id: camp.lead_thread_id.clone(),
        },
        &project_key,
        &names,
    );
    let mut out = format!("# Camp: {} (`{}`)\n\n", camp.title, camp.slug);
    out.push_str(&format!(
        "- Kind: {}\n- Status: {}{}\n- Lead: {lead}\n- Pitched: {}\n\n",
        camp.kind.as_str(),
        camp.status.as_str(),
        camp.struck_reason
            .as_deref()
            .map(|reason| format!(" ({reason})"))
            .unwrap_or_default(),
        crate::clock::age(camp.created_at),
    ));
    out.push_str(&format!("## Participants ({})\n\n", participants.len()));
    for participant in participants.iter().take(12) {
        out.push_str(&participant_line(participant, &project_key, &names));
        out.push('\n');
    }
    if participants.len() > 12 {
        out.push_str(&format!(
            "- … {} more at {}\n",
            participants.len() - 12,
            build_project_camp_participants_uri(&project_key, &camp.slug)
        ));
    }
    out.push_str(&format!("\n## Issues ({})\n\n", issues.len()));
    for issue in &issues {
        out.push_str(&format!(
            "- [#{}]({}) {} · {}\n",
            issue.number,
            build_issue_uri(&project_key, issue.number as i32),
            issue.title,
            issue.status
        ));
    }
    if issues.is_empty() {
        out.push_str("No issues joined.\n");
    }
    out.push_str("\n## History\n\n");
    for event in &history {
        out.push_str(&history_line(event, &project_key, &names));
        out.push('\n');
    }
    Ok(out)
}

/// `cairn://p/{project}/camps/{slug}/participants`.
pub(crate) async fn read_participants(
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    match participants(db, project_key, slug, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn participants(
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let Some(view) = camp_view(db, &project_id, slug).await? else {
        return Err(missing_camp(&project_key, slug));
    };
    let names = thread_names(db, &project_id).await?;
    if json {
        let value = json!({
            "camp": build_project_camp_uri(&project_key, slug),
            "participants": view.participants.iter().map(|p| participant_json(p, &project_key, &names)).collect::<Vec<_>>(),
        });
        return Ok(serde_json::to_string_pretty(&value).unwrap_or_default());
    }
    let mut out = format!(
        "# Participants — camp `{}` ({})\n\n",
        view.camp.slug,
        view.participants.len()
    );
    for participant in &view.participants {
        out.push_str(&participant_line(participant, &project_key, &names));
        out.push('\n');
    }
    Ok(out)
}
