//! `cairn://p/{project}/camps`, `/camps/{slug}`, `/camps/{slug}/participants`
//! and `/camps/{slug}/posts`.
//!
//! Every rendering here is a view of [`crate::camps::view`], the one camp
//! projection the desktop page and the thread seed also read, so a count or a
//! history line reads the same wherever it appears.

use cairn_common::query::QueryParam;
use cairn_uri::{
    build_project_camp_participants_uri, build_project_camp_posts_uri, build_project_camp_uri,
    build_project_camps_uri,
};
use serde_json::json;

use crate::camps::view::{self, CampSummary, CensusEntry};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// How many participants a camp read lists before pointing at the census.
const CENSUS_INLINE: usize = 12;

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

fn now() -> i64 {
    chrono::Utc::now().timestamp()
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

fn plural(count: usize, word: &str) -> String {
    format!("{count} {word}{}", if count == 1 { "" } else { "s" })
}

fn summary_line(camp: &CampSummary) -> String {
    format!(
        "- [{}]({}) — {} · lead {} · {}{}{}\n",
        camp.slug,
        camp.uri,
        camp.title,
        camp.lead.label,
        plural(camp.participants, "participant"),
        if camp.state.is_empty() {
            String::new()
        } else {
            format!(" · {}", camp.state)
        },
        if camp.status == crate::models::CampStatus::Struck {
            " · struck"
        } else {
            ""
        },
    )
}

/// `cairn://p/{project}/camps`.
pub(crate) async fn read_camps(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    match camps(orch, db, project_key, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn camps(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let listed =
        view::list_summaries(db, Some(&orch.db.local), &project_id, &project_key, now()).await?;
    if json {
        return Ok(serde_json::to_string_pretty(&json!({ "camps": listed })).unwrap_or_default());
    }
    let mut out = format!("# Camps — {project_key}\n\n");
    if !listed
        .iter()
        .any(|camp| camp.kind == crate::models::CampKind::Base)
    {
        out.push_str(&format!(
            "{}\n\n",
            missing_camp(&project_key, crate::models::BASE_CAMP_SLUG)
        ));
    }
    for camp in &listed {
        out.push_str(&summary_line(camp));
    }
    if listed.is_empty() {
        out.push_str("No camps.\n");
    }
    Ok(out)
}

fn census_line(entry: &CensusEntry) -> String {
    let who = match &entry.thread {
        Some(uri) => format!("[{}]({uri})", entry.display.label),
        None => entry.display.inline(),
    };
    let mut line = format!("- {who} — {}", entry.role.as_str());
    if entry.lead {
        if let Some(level) = entry.level {
            line.push_str(&format!(" (also a {} member)", level.as_str()));
        }
    }
    if let Some(reason) = &entry.reason {
        line.push_str(&format!(": {reason}"));
    }
    if let Some(by) = &entry.assigned_by {
        line.push_str(&format!(" · set by {}", by.label));
    }
    line.push_str(&format!(" · since {}", crate::clock::age(entry.since)));
    line
}

/// `cairn://p/{project}/camps/{slug}`.
pub(crate) async fn read_camp(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    match camp(orch, db, project_key, slug, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn camp(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let Some(camp) = view::camp_view(
        db,
        Some(&orch.db.local),
        &project_id,
        &project_key,
        slug,
        now(),
    )
    .await?
    else {
        return Err(missing_camp(&project_key, slug));
    };
    if json {
        return Ok(serde_json::to_string_pretty(&camp).unwrap_or_default());
    }
    let summary = &camp.summary;
    let lead = match &summary.lead_thread {
        Some(uri) => format!("[{}]({uri})", summary.lead.label),
        None => summary.lead.label.clone(),
    };
    let mut out = format!("# Camp: {} (`{}`)\n\n", summary.title, summary.slug);
    out.push_str(&format!(
        "- Kind: {}\n- Status: {}{}\n- Lead: {lead}\n- Pitched: {}\n{}\n",
        summary.kind.as_str(),
        summary.status.as_str(),
        summary
            .struck_reason
            .as_deref()
            .map(|reason| format!(" ({reason})"))
            .unwrap_or_default(),
        crate::clock::age(summary.created_at),
        if summary.state.is_empty() {
            String::new()
        } else {
            format!("- State: {}\n", summary.state)
        },
    ));
    if summary.kind == crate::models::CampKind::Base {
        out.push_str(&format!("## Roadmap ({})\n\n", camp.roadmap.len()));
        for other in &camp.roadmap {
            out.push_str(&summary_line(other));
        }
        if camp.roadmap.is_empty() {
            out.push_str("No camps pitched.\n");
        }
        out.push('\n');
    }
    out.push_str(&format!("## Participants ({})\n\n", camp.census.len()));
    for entry in camp.census.iter().take(CENSUS_INLINE) {
        out.push_str(&census_line(entry));
        out.push('\n');
    }
    if camp.census.len() > CENSUS_INLINE {
        out.push_str(&format!(
            "- … {} more at {}\n",
            camp.census.len() - CENSUS_INLINE,
            build_project_camp_participants_uri(&project_key, &summary.slug)
        ));
    }
    out.push_str(&format!("\n## Issues ({})\n\n", camp.issues.len()));
    for issue in &camp.issues {
        out.push_str(&format!(
            "- [#{}]({}) {} · {}\n",
            issue.number, issue.uri, issue.title, issue.status
        ));
    }
    if camp.issues.is_empty() {
        out.push_str("No issues joined.\n");
    }
    out.push_str(&format!(
        "\n## Posts ({})\n\nRead and write them at {}.\n",
        summary.posts,
        build_project_camp_posts_uri(&project_key, &summary.slug)
    ));
    if !camp.tenets.is_empty() {
        out.push_str(&format!("\n## Linked tenets ({})\n\n", camp.tenets.len()));
        for tenet in &camp.tenets {
            out.push_str(&format!(
                "- [{}]({}) {} · {}\n",
                tenet.number, tenet.uri, tenet.statement, tenet.status
            ));
        }
    }
    out.push_str("\n## History\n\n");
    for entry in &camp.history {
        out.push_str(&format!(
            "- {}: {}\n",
            crate::clock::age(entry.at),
            entry.text
        ));
    }
    Ok(out)
}

/// `cairn://p/{project}/camps/{slug}/participants`.
pub(crate) async fn read_participants(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    match participants(orch, db, project_key, slug, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn participants(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let Some(camp) = view::camp_view(
        db,
        Some(&orch.db.local),
        &project_id,
        &project_key,
        slug,
        now(),
    )
    .await?
    else {
        return Err(missing_camp(&project_key, slug));
    };
    if json {
        let value = json!({
            "camp": build_project_camp_uri(&project_key, slug),
            "participants": camp.census,
        });
        return Ok(serde_json::to_string_pretty(&value).unwrap_or_default());
    }
    let mut out = format!(
        "# Participants — camp `{}` ({})\n\n",
        camp.summary.slug,
        camp.census.len()
    );
    for entry in &camp.census {
        out.push_str(&census_line(entry));
        out.push('\n');
    }
    Ok(out)
}

/// `cairn://p/{project}/camps/{slug}/posts`: the camp's posts, rendered as
/// every posts surface renders them.
pub(crate) async fn read_camp_posts(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    let (project_id, project_key) = match project(db, project_key).await {
        Ok(project) => project,
        Err(error) => return error,
    };
    let camp = match db.camp_by_slug(&project_id, slug).await {
        Ok(Some(camp)) => camp,
        Ok(None) => return missing_camp(&project_key, slug),
        Err(error) => return format!("Failed to load camp `{slug}`: {error}"),
    };
    super::posts::read_posts(
        &orch.db.local,
        Some(orch),
        crate::storage::PostScope::Camp(&camp.id),
        params,
    )
    .await
}
