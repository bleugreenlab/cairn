//! `cairn://p/{project}/camps`, `/camps/{slug}`, `/camps/{slug}/members` and
//! `/camps/{slug}/posts`.
//!
//! Every rendering here is a view of [`crate::camps::view`], the one camp
//! projection the desktop page and the thread seed also read, so a member
//! count or a last post reads the same wherever it appears.

use cairn_common::query::QueryParam;
use cairn_uri::{
    build_project_camp_members_uri, build_project_camp_posts_uri, build_project_camp_uri,
    build_project_camps_uri,
};
use serde_json::json;

use crate::camps::view::{self, CampSummary, MemberEntry};
use crate::models::{CampKind, CampStatus};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// How many members a camp read lists before pointing at the member list.
const MEMBERS_INLINE: usize = 12;
/// How many lines of conversation a camp read shows.
const CONVERSATION_INLINE: usize = 20;

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
        "- [{}]({}) — {} · {}{}{}\n",
        camp.slug,
        camp.uri,
        camp.title,
        plural(camp.members, "member"),
        camp.last_post
            .as_ref()
            .map(|post| format!(" · last post {}", crate::clock::age(post.at)))
            .unwrap_or_default(),
        if camp.status == CampStatus::Archived {
            " · archived"
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
    let listed = view::list_summaries(db, Some(&orch.db.local), &project_id, &project_key).await?;
    if json {
        return Ok(serde_json::to_string_pretty(&json!({ "camps": listed })).unwrap_or_default());
    }
    let mut out = format!("# Camps — {project_key}\n\n");
    if !listed.iter().any(|camp| camp.kind == CampKind::Base) {
        out.push_str(&format!(
            "{}\n\n",
            missing_camp(&project_key, crate::models::BASE_CAMP_SLUG)
        ));
    }
    for camp in &listed {
        out.push_str(&summary_line(camp));
    }
    if listed.is_empty() {
        out.push_str("No camps yet.\n");
    }
    Ok(out)
}

fn member_line(entry: &MemberEntry) -> String {
    let who = match &entry.thread {
        Some(uri) => format!("[{}]({uri})", entry.display.label),
        None => entry.display.label.clone(),
    };
    format!("- {who} · since {}", crate::clock::age(entry.since))
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
    let local = &orch.db.local;
    let Some(camp) = view::camp_view(db, Some(local), &project_id, &project_key, slug).await?
    else {
        return Err(missing_camp(&project_key, slug));
    };
    if json {
        return Ok(serde_json::to_string_pretty(&camp).unwrap_or_default());
    }
    let summary = &camp.summary;
    let base = summary.kind == CampKind::Base;
    let mut out = format!("# Camp: {} (`{}`)\n\n", summary.title, summary.slug);
    out.push_str(&format!(
        "- Kind: {}\n- Status: {}\n- Created: {}\n\n",
        if base { "base camp" } else { "camp" },
        summary.status.as_str(),
        crate::clock::age(summary.created_at),
    ));
    out.push_str(&format!("## Members ({})\n\n", summary.members));
    if base {
        out.push_str(&format!(
            "Every open thread of the project ({}).\n",
            summary.members
        ));
    } else {
        for entry in camp.members.iter().take(MEMBERS_INLINE) {
            out.push_str(&member_line(entry));
            out.push('\n');
        }
        if camp.members.len() > MEMBERS_INLINE {
            out.push_str(&format!(
                "- … {} more at {}\n",
                camp.members.len() - MEMBERS_INLINE,
                build_project_camp_members_uri(&project_key, &summary.slug)
            ));
        }
    }

    // The conversation: the newest posts and notices, interleaved by time.
    let camp_row = db
        .camp_by_slug(&project_id, slug)
        .await
        .map_err(|error| format!("Failed to load camp `{slug}`: {error}"))?;
    let mut posts = match &camp_row {
        Some(row) => local
            .list_posts(
                crate::storage::PostScope::Camp(&row.id),
                None,
                CONVERSATION_INLINE,
            )
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    };
    crate::identity::display::resolve_author_displays(local, &mut posts).await;
    let mut lines: Vec<(i64, String)> = posts
        .iter()
        .map(|post| {
            let author = post
                .author_display
                .as_ref()
                .map(|display| display.label.clone())
                .unwrap_or_else(|| "someone".to_string());
            (
                post.created_at,
                format!(
                    "- {} · {author}: {} — cairn://posts/{}",
                    crate::clock::age(post.created_at),
                    view::excerpt(post),
                    post.id
                ),
            )
        })
        .collect();
    lines.extend(camp.notices.iter().map(|notice| {
        (
            notice.at,
            format!("- {} · {}", crate::clock::age(notice.at), notice.text),
        )
    }));
    lines.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
    out.push_str(&format!("\n## Conversation ({} posts)\n\n", summary.posts));
    for (_, line) in lines.iter().take(CONVERSATION_INLINE) {
        out.push_str(line);
        out.push('\n');
    }
    if lines.is_empty() {
        out.push_str("Nothing posted yet.\n");
    }
    out.push_str(&format!(
        "\nRead and write the posts at {}.\n",
        build_project_camp_posts_uri(&project_key, &summary.slug)
    ));
    Ok(out)
}

/// `cairn://p/{project}/camps/{slug}/members`.
pub(crate) async fn read_members(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> String {
    match members(orch, db, project_key, slug, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn members(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    slug: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let json = wants_json(params)?;
    let (project_id, project_key) = project(db, project_key).await?;
    let Some(camp) =
        view::camp_view(db, Some(&orch.db.local), &project_id, &project_key, slug).await?
    else {
        return Err(missing_camp(&project_key, slug));
    };
    if json {
        let value = json!({
            "camp": build_project_camp_uri(&project_key, slug),
            "members": camp.members,
        });
        return Ok(serde_json::to_string_pretty(&value).unwrap_or_default());
    }
    let mut out = format!(
        "# Members — camp `{}` ({})\n\n",
        camp.summary.slug,
        camp.members.len()
    );
    if camp.summary.kind == CampKind::Base {
        out.push_str("Every open thread of the project is in base camp.\n\n");
    }
    for entry in &camp.members {
        out.push_str(&member_line(entry));
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
