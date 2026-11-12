//! One projection of a camp, which every camp surface renders.
//!
//! The markdown read (`cairn://p/{project}/camps/{slug}`), its `format=json`,
//! the thread seed's camp line and the desktop's Camps tab and camp page all
//! take their values from here, so a member count or a last post cannot read
//! one way on one surface and another way on the next.
//!
//! Everything here is generated from the rows that own it: the camp row, its
//! members, the posts addressed to it, and the notices minted to it. Nothing
//! authored is quoted except titles and post excerpts.

use std::collections::HashMap;

use cairn_common::identity::display::{PrincipalAliases, PrincipalDisplay};
use cairn_db::storage::camps::{camp_by_slug_conn, camp_members_conn, list_camps_conn};
use cairn_uri::{build_project_camp_uri, build_thread_uri};
use serde::Serialize;

use crate::models::{Camp, CampKind, CampMember, CampStatus, CampSubject, Post};
use crate::storage::{LocalDb, RowExt};

/// How many notices a camp view carries, newest first.
pub const NOTICE_LIMIT: usize = 50;
/// How many characters of a post a last-post excerpt keeps.
const EXCERPT_CHARS: usize = 120;

/// A camp's newest post, as a list row shows it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastPost {
    pub id: i64,
    pub author: PrincipalDisplay,
    pub excerpt: String,
    pub at: i64,
}

/// One camp in brief: what the camp list and the seed show.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampSummary {
    pub uri: String,
    pub slug: String,
    pub title: String,
    pub kind: CampKind,
    pub status: CampStatus,
    pub archived_at: Option<i64>,
    /// How many members the camp has now. For base camp, every open thread.
    pub members: usize,
    /// A camp's members by name, longest standing first. Empty for base camp,
    /// whose members are every open thread and are not listed.
    pub member_labels: Vec<PrincipalDisplay>,
    pub last_post: Option<LastPost>,
    pub posts: i64,
    /// Whether the operator is a member. Never true for base camp, which is
    /// a channel of threads.
    pub operator_member: bool,
    /// Posts by others the operator has not read, for a camp the operator is
    /// a member of; zero otherwise.
    pub unread: i64,
    /// The newest of the camp's own changes and its last post.
    pub last_activity_at: i64,
    pub created_at: i64,
}

/// One member as a camp page lists it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberEntry {
    pub subject_key: String,
    pub subject: CampSubject,
    pub display: PrincipalDisplay,
    /// The thread's canonical URI, for a thread member.
    pub thread: Option<String>,
    pub since: i64,
}

/// One notice minted to the camp: a line in its conversation that no one
/// wrote (a rebuild, a member added, the camp archived).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NoticeLine {
    pub kind: String,
    pub text: String,
    pub at: i64,
    pub detail_uri: Option<String>,
}

/// One camp in full.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampView {
    pub summary: CampSummary,
    pub members: Vec<MemberEntry>,
    /// Newest first, bounded by [`NOTICE_LIMIT`], for as long as the fact log
    /// keeps them.
    pub notices: Vec<NoticeLine>,
}

/// `5m ago`, `3h ago`, `2d ago`: the coarse age a one-line row shows.
pub fn coarse_ago(at: i64, now: i64) -> String {
    let seconds = now.saturating_sub(at).max(0);
    match seconds {
        s if s < 3_600 => format!("{}m ago", s / 60),
        s if s < 86_400 => format!("{}h ago", s / 3_600),
        s => format!("{}d ago", s / 86_400),
    }
}

/// A post's first line, title first, cut to [`EXCERPT_CHARS`].
pub(crate) fn excerpt(post: &Post) -> String {
    let text = post
        .title
        .as_deref()
        .filter(|title| !title.trim().is_empty())
        .unwrap_or(&post.content);
    let line = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let line = line.trim();
    if line.chars().count() > EXCERPT_CHARS {
        let cut: String = line.chars().take(EXCERPT_CHARS).collect();
        format!("{}…", cut.trim_end())
    } else {
        line.to_string()
    }
}

/// The rows a projection reads from the camp's own database, in one read.
struct CampRows {
    camp: Camp,
    members: Vec<CampMember>,
}

/// What a projection needs beside the camp's own rows.
struct Names {
    project_key: String,
    threads: HashMap<String, String>,
    aliases: PrincipalAliases,
    /// Camp id to post count.
    posts: HashMap<String, i64>,
    last_posts: HashMap<String, Post>,
    unread: HashMap<String, i64>,
}

impl Names {
    async fn load(
        db: &LocalDb,
        local: Option<&LocalDb>,
        project_id: &str,
        project_key: &str,
    ) -> Result<Self, String> {
        let threads = db
            .query_all(
                "SELECT id, name FROM threads WHERE project_id = ?1",
                (project_id.to_string(),),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| format!("Failed to load the project's threads: {error}"))?
            .into_iter()
            .collect();
        // Posts and read cursors live in the install's private database. A
        // caller without it gets summaries with no posts rather than a guess.
        let (aliases, posts, last_posts, unread) = match local {
            Some(local) => (
                crate::identity::display::principal_aliases(local).await,
                local
                    .camp_post_stats(project_id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(camp, (count, _))| (camp, count))
                    .collect(),
                local.last_camp_posts(project_id).await.unwrap_or_default(),
                local
                    .camp_unread_counts(crate::models::OPERATOR_MEMBER_KEY, project_id)
                    .await
                    .unwrap_or_default(),
            ),
            None => Default::default(),
        };
        Ok(Self {
            project_key: cairn_uri::canonical_project(project_key),
            threads,
            aliases,
            posts,
            last_posts,
            unread,
        })
    }

    fn thread_uri(&self, thread_id: &str) -> Option<String> {
        self.threads
            .get(thread_id)
            .map(|name| build_thread_uri(&self.project_key, name))
    }

    /// How a member reads: a thread by its current name, the operator as such.
    fn member(&self, subject: &CampSubject) -> PrincipalDisplay {
        match subject {
            CampSubject::Thread { thread_id } => match self.threads.get(thread_id) {
                Some(name) => PrincipalDisplay {
                    label: name.clone(),
                    detail: build_thread_uri(&self.project_key, name),
                },
                None => PrincipalDisplay {
                    label: format!("thread {thread_id} (no longer exists)"),
                    detail: thread_id.clone(),
                },
            },
            CampSubject::Operator => PrincipalDisplay {
                label: "operator".to_string(),
                detail: "the operator".to_string(),
            },
        }
    }
}

async fn camp_rows_conn(
    conn: &cairn_db::storage::TrackedConnection,
    camp: Camp,
) -> cairn_db::storage::DbResult<CampRows> {
    let members = camp_members_conn(conn, &camp).await?;
    Ok(CampRows { camp, members })
}

fn summary(rows: &CampRows, names: &Names) -> CampSummary {
    let camp = &rows.camp;
    let last_post = names.last_posts.get(&camp.id).map(|post| LastPost {
        id: post.id,
        author: names
            .aliases
            .display(Some(&post.author), Some(&post.appearance)),
        excerpt: excerpt(post),
        at: post.created_at,
    });
    let operator_member = !camp.is_base()
        && rows
            .members
            .iter()
            .any(|member| member.subject == CampSubject::Operator);
    let last_activity_at = last_post
        .as_ref()
        .map_or(camp.updated_at, |post| post.at.max(camp.updated_at));
    CampSummary {
        uri: build_project_camp_uri(&names.project_key, &camp.slug),
        slug: camp.slug.clone(),
        title: camp.title.clone(),
        kind: camp.kind,
        status: camp.status,
        archived_at: camp.archived_at,
        members: rows.members.len(),
        member_labels: if camp.is_base() {
            Vec::new()
        } else {
            rows.members
                .iter()
                .map(|member| names.member(&member.subject))
                .collect()
        },
        last_post,
        posts: names.posts.get(&camp.id).copied().unwrap_or(0),
        operator_member,
        unread: if operator_member {
            names.unread.get(&camp.id).copied().unwrap_or(0)
        } else {
            0
        },
        last_activity_at,
        created_at: camp.created_at,
    }
}

fn members(rows: &CampRows, names: &Names) -> Vec<MemberEntry> {
    rows.members
        .iter()
        .map(|member| MemberEntry {
            subject_key: member.subject_key.clone(),
            subject: member.subject.clone(),
            display: names.member(&member.subject),
            thread: member
                .subject
                .thread_id()
                .and_then(|id| names.thread_uri(id)),
            since: member.since,
        })
        .collect()
}

/// The notices minted to `camp_uri`, newest first. Notices are facts in the
/// install's private fact log; a caller without it gets none.
async fn notices(local: Option<&LocalDb>, project_id: &str, camp_uri: &str) -> Vec<NoticeLine> {
    let Some(local) = local else {
        return Vec::new();
    };
    // The camp is matched in the query, before the limit, so a busy camp's
    // notices never crowd a quiet one's out. `fields_json` is the compact
    // serialization of the notice's fields, so the camp field appears in it
    // verbatim; the exact match below rejects any other placement.
    let needle = serde_json::json!({ "camp": camp_uri }).to_string();
    let needle = needle
        .trim_start_matches('{')
        .trim_end_matches('}')
        .to_string();
    let rows = local
        .query_all(
            "SELECT fields_json, summary, minted_at FROM fact_log
             WHERE source = ?1 AND project_id = ?2 AND instr(fields_json, ?3) > 0
             ORDER BY id DESC LIMIT ?4",
            (
                super::CAMP_NOTICE.to_string(),
                project_id.to_string(),
                needle,
                NOTICE_LIMIT as i64,
            ),
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.i64(2)?)),
        )
        .await
        .unwrap_or_default();
    rows.into_iter()
        .filter_map(|(fields_json, summary, minted_at)| {
            let fields: serde_json::Value = serde_json::from_str(&fields_json).ok()?;
            let field = |name: &str| fields.get(name).and_then(|value| value.as_str());
            (field("camp") == Some(camp_uri)).then(|| NoticeLine {
                kind: field("kind").unwrap_or("notice").to_string(),
                text: field("text")
                    .map(str::to_string)
                    .or(summary)
                    .unwrap_or_default(),
                at: minted_at / 1_000,
                detail_uri: field("detailUri").map(str::to_string),
            })
        })
        .collect()
}

/// Every camp of a project in brief: base first, then active camps by title,
/// then archived ones.
///
/// `db` is the database that owns the project; `local` is the install's
/// private database, which holds posts, read cursors and the principal alias
/// registries.
pub async fn list_summaries(
    db: &LocalDb,
    local: Option<&LocalDb>,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<CampSummary>, String> {
    let names = Names::load(db, local, project_id, project_key).await?;
    let rows = {
        let project_id = project_id.to_string();
        db.read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = Vec::new();
                for camp in list_camps_conn(conn, &project_id).await? {
                    rows.push(camp_rows_conn(conn, camp).await?);
                }
                Ok(rows)
            })
        })
        .await
        .map_err(|error| format!("Failed to list camps: {error}"))?
    };
    Ok(rows.iter().map(|rows| summary(rows, &names)).collect())
}

/// One camp in full, or `None` when the project has no camp `slug`.
pub async fn camp_view(
    db: &LocalDb,
    local: Option<&LocalDb>,
    project_id: &str,
    project_key: &str,
    slug: &str,
) -> Result<Option<CampView>, String> {
    let names = Names::load(db, local, project_id, project_key).await?;
    let loaded = {
        let (project_id, wanted) = (project_id.to_string(), slug.to_string());
        db.read(move |conn| {
            let (project_id, slug) = (project_id.clone(), wanted.clone());
            Box::pin(async move {
                let Some(camp) = camp_by_slug_conn(conn, &project_id, &slug).await? else {
                    return Ok(None);
                };
                Ok(Some(camp_rows_conn(conn, camp).await?))
            })
        })
        .await
        .map_err(|error| format!("Failed to load camp `{slug}`: {error}"))?
    };
    let Some(rows) = loaded else {
        return Ok(None);
    };
    let summary = summary(&rows, &names);
    let notices = notices(local, project_id, &summary.uri).await;
    Ok(Some(CampView {
        members: members(&rows, &names),
        notices,
        summary,
    }))
}

#[cfg(test)]
mod tests;
