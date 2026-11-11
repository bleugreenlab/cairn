//! One projection of a camp, which every camp surface renders.
//!
//! The markdown read (`cairn://p/{project}/camps/{slug}`), its `format=json`,
//! the thread seed's camp line and the desktop's camp page all take their
//! values from here, so a count, a state line or a history line cannot read
//! one way on one surface and another way on the next.
//!
//! Everything here is generated from the rows that own it: the camp row, the
//! census, the issues joined to the camp, the journal, the posts addressed to
//! it, and the tenets that cite it. Nothing authored is quoted except titles.
//! A camp's authored description and open questions arrive with Camps part 2
//! (cairn/6392); until then they are typed and empty, so a surface can say
//! plainly that nothing has been authored rather than invent text.

use std::collections::{HashMap, HashSet};

use cairn_common::identity::display::{PrincipalAliases, PrincipalDisplay};
use cairn_common::identity::PrincipalRef;
use cairn_db::storage::camps::{
    camp_by_slug_conn, camp_history_conn, camp_issues_conn, camp_participants_conn,
    list_camps_conn, CampEvent, CampIssue,
};
use cairn_uri::{build_issue_uri, build_project_camp_uri, build_thread_uri};
use serde::Serialize;

use crate::models::{
    Camp, CampEventKind, CampKind, CampLevel, CampParticipant, CampRole, CampStatus, CampSubject,
};
use crate::storage::{LocalDb, RowExt};

/// How many journal rows a camp view carries.
pub const HISTORY_LIMIT: i64 = 20;
/// How many linked tenets a camp view carries.
const TENET_LIMIT: usize = 10;

/// A camp's joined issues counted by status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueCounts {
    pub backlog: usize,
    pub active: usize,
    pub waiting: usize,
    pub complete: usize,
    pub failed: usize,
    pub merged: usize,
    pub closed: usize,
}

impl IssueCounts {
    pub fn from_statuses<'a>(statuses: impl IntoIterator<Item = &'a str>) -> Self {
        let mut counts = Self::default();
        for status in statuses {
            counts.add(status);
        }
        counts
    }

    fn add(&mut self, status: &str) {
        match status {
            "active" => self.active += 1,
            "waiting" => self.waiting += 1,
            "complete" => self.complete += 1,
            "failed" => self.failed += 1,
            "merged" => self.merged += 1,
            "closed" => self.closed += 1,
            _ => self.backlog += 1,
        }
    }

    /// Issues still being worked: not yet merged, closed, complete or failed.
    pub fn open(&self) -> usize {
        self.backlog + self.active + self.waiting
    }

    pub fn total(&self) -> usize {
        self.open() + self.complete + self.failed + self.merged + self.closed
    }
}

/// One camp in brief: what a camp list, the roadmap and the seed show.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampSummary {
    pub uri: String,
    pub slug: String,
    pub title: String,
    pub kind: CampKind,
    pub status: CampStatus,
    pub struck_reason: Option<String>,
    pub lead: PrincipalDisplay,
    /// The lead thread's canonical URI, when the thread still exists.
    pub lead_thread: Option<String>,
    pub participants: usize,
    pub issues: IssueCounts,
    /// Every joined issue's number, so a surface can join live PR, check and
    /// gate state onto the roll-up without another read.
    pub issue_numbers: Vec<i64>,
    pub posts: i64,
    pub last_post_at: Option<i64>,
    /// The newest of the camp's journal, its joined issues' updates and its
    /// posts.
    pub last_activity_at: i64,
    pub created_at: i64,
    /// [`state_line`], as of when this summary was built.
    pub state: String,
}

/// A camp's authored description. Camps part 2 (cairn/6392) writes it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampDescription {
    pub text: String,
    pub version: i64,
}

/// An open question asked in a camp. Camps part 2 (cairn/6392) writes them.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampQuestion {
    pub text: String,
    pub asked_at: i64,
}

/// One participant as the census shows it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CensusEntry {
    pub subject_key: String,
    pub subject: CampSubject,
    pub display: PrincipalDisplay,
    /// The thread's canonical URI, for a thread that still exists.
    pub thread: Option<String>,
    pub role: CampRole,
    pub lead: bool,
    pub level: Option<CampLevel>,
    pub reason: Option<String>,
    pub assigned_by: Option<PrincipalDisplay>,
    pub since: i64,
}

/// One issue joined to the camp.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampIssueRow {
    pub number: i64,
    pub title: String,
    pub status: String,
    pub updated_at: i64,
    pub uri: String,
}

/// A tenet whose statement or rationale cites the camp or one of its issues.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkedTenet {
    pub number: i64,
    pub statement: String,
    pub status: String,
    pub uri: String,
}

/// One journal row, rendered.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub kind: String,
    pub text: String,
    pub at: i64,
}

/// One camp in full.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampView {
    pub summary: CampSummary,
    pub description: Option<CampDescription>,
    pub questions: Vec<CampQuestion>,
    pub census: Vec<CensusEntry>,
    pub issues: Vec<CampIssueRow>,
    pub tenets: Vec<LinkedTenet>,
    pub history: Vec<HistoryEntry>,
    /// Base camp only: every pitched camp of the project, as the roadmap.
    pub roadmap: Vec<CampSummary>,
}

/// `5m ago`, `3h ago`, `2d ago`: the coarse age a one-line state shows.
fn coarse_ago(at: i64, now: i64) -> String {
    let seconds = now.saturating_sub(at).max(0);
    match seconds {
        s if s < 3_600 => format!("{}m ago", s / 60),
        s if s < 86_400 => format!("{}h ago", s / 3_600),
        s => format!("{}d ago", s / 86_400),
    }
}

/// The bounded, generated line that says where a camp stands, e.g.
/// `3 open (1 active, 2 waiting) · 4 merged · last post 2h ago`. It never
/// quotes authored text, and is empty for a camp with no issues and no posts:
/// a surface shows nothing rather than a sentence saying there is nothing.
pub fn state_line(issues: &IssueCounts, last_post_at: Option<i64>, now: i64) -> String {
    let mut parts = Vec::new();
    if issues.total() > 0 {
        let open: Vec<String> = [
            (issues.active, "active"),
            (issues.waiting, "waiting"),
            (issues.backlog, "backlog"),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, word)| format!("{count} {word}"))
        .collect();
        parts.push(if open.is_empty() {
            "0 open".to_string()
        } else {
            format!("{} open ({})", issues.open(), open.join(", "))
        });
        for (count, word) in [
            (issues.merged, "merged"),
            (issues.complete, "complete"),
            (issues.failed, "failed"),
            (issues.closed, "closed"),
        ] {
            if count > 0 {
                parts.push(format!("{count} {word}"));
            }
        }
    }
    if let Some(at) = last_post_at {
        parts.push(format!("last post {}", coarse_ago(at, now)));
    }
    parts.join(" · ")
}

/// The reason every base-camp seat carries by default. It restates what base
/// camp is, so surfaces show it as no reason at all.
fn authored_reason(reason: Option<&str>) -> Option<&str> {
    reason.filter(|reason| *reason != cairn_db::storage::camps::BASE_MEMBERSHIP_REASON)
}

/// The rows a projection reads from the camp's own database, in one read.
struct CampRows {
    camp: Camp,
    participants: Vec<CampParticipant>,
    issues: Vec<CampIssue>,
    newest_event_at: Option<i64>,
}

async fn camp_rows_conn(
    conn: &cairn_db::storage::TrackedConnection,
    camp: Camp,
) -> cairn_db::storage::DbResult<CampRows> {
    let participants = camp_participants_conn(conn, &camp).await?;
    let issues = camp_issues_conn(conn, &camp.id).await?;
    let newest_event_at = camp_history_conn(conn, &camp.id, 1)
        .await?
        .first()
        .map(|event| event.created_at);
    Ok(CampRows {
        camp,
        participants,
        issues,
        newest_event_at,
    })
}

/// What a projection needs beside the camp's own rows.
struct Names {
    project_key: String,
    threads: HashMap<String, String>,
    aliases: PrincipalAliases,
    /// Camp id to (post count, newest post).
    posts: HashMap<String, (i64, i64)>,
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
        // Posts live in the install's private database. A caller without it
        // (the thread seed, which reads the project's database only) gets
        // summaries with no post counts rather than a guess.
        let (aliases, posts) = match local {
            Some(local) => (
                crate::identity::display::principal_aliases(local).await,
                local.camp_post_stats(project_id).await.unwrap_or_default(),
            ),
            None => (PrincipalAliases::default(), HashMap::new()),
        };
        Ok(Self {
            project_key: cairn_uri::canonical_project(project_key),
            threads,
            aliases,
            posts,
        })
    }

    fn thread_uri(&self, thread_id: &str) -> Option<String> {
        self.threads
            .get(thread_id)
            .map(|name| build_thread_uri(&self.project_key, name))
    }

    /// How a subject reads: a thread by its current name, a human by the
    /// shared principal display.
    fn subject(&self, subject: &CampSubject, principal: Option<&PrincipalRef>) -> PrincipalDisplay {
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
            CampSubject::Human { .. } => self.aliases.display(principal, None),
        }
    }

    fn thread(&self, thread_id: &str) -> PrincipalDisplay {
        self.subject(
            &CampSubject::Thread {
                thread_id: thread_id.to_string(),
            },
            None,
        )
    }
}

fn summary(rows: &CampRows, names: &Names, now: i64) -> CampSummary {
    let camp = &rows.camp;
    let mut issues = IssueCounts::default();
    for issue in &rows.issues {
        issues.add(&issue.status);
    }
    let (posts, last_post_at) = match names.posts.get(&camp.id) {
        Some((count, at)) => (*count, Some(*at)),
        None => (0, None),
    };
    let last_activity_at = [
        Some(camp.created_at),
        rows.newest_event_at,
        rows.issues.iter().map(|issue| issue.updated_at).max(),
        last_post_at,
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(camp.created_at);
    CampSummary {
        uri: build_project_camp_uri(&names.project_key, &camp.slug),
        slug: camp.slug.clone(),
        title: camp.title.clone(),
        kind: camp.kind,
        status: camp.status,
        struck_reason: camp.struck_reason.clone(),
        lead: names.thread(&camp.lead_thread_id),
        lead_thread: names.thread_uri(&camp.lead_thread_id),
        participants: rows.participants.len(),
        state: state_line(&issues, last_post_at, now),
        issues,
        issue_numbers: rows.issues.iter().map(|issue| issue.number).collect(),
        posts,
        last_post_at,
        last_activity_at,
        created_at: camp.created_at,
    }
}

/// The text of one journal row. Every kind the journal admits is named; an
/// unknown stored kind reads as its words, never its underscores.
fn history_text(event: &CampEvent, names: &Names) -> String {
    let subject = || match event.subject_key.as_deref() {
        Some(key) if CampSubject::key_is_thread(key) => names.thread(key).label,
        Some(key) => key.to_string(),
        None => "someone".to_string(),
    };
    let thread = |id: Option<&str>| match id {
        Some(id) => names.thread(id).label,
        None => "a thread".to_string(),
    };
    let new = event.new_value.as_deref().unwrap_or_default();
    let old = event.old_value.as_deref().unwrap_or_default();
    let what = match CampEventKind::parse(&event.kind) {
        CampEventKind::Pitched => format!("pitched, led by {}", subject()),
        CampEventKind::Joined => format!("{} joined at {new}", subject()),
        CampEventKind::RoleChanged => format!("{} moved from {old} to {new}", subject()),
        CampEventKind::Left => format!("{} left", subject()),
        CampEventKind::LeadOffered => {
            format!("lead offered to {}", thread(event.new_value.as_deref()))
        }
        CampEventKind::LeadChanged => format!(
            "lead passed from {} to {}",
            thread(event.old_value.as_deref()),
            thread(event.new_value.as_deref())
        ),
        CampEventKind::Retitled => format!("retitled to \"{new}\""),
        CampEventKind::Struck => "struck".to_string(),
        CampEventKind::IssueJoined => format!("issue #{new} joined"),
        CampEventKind::IssueLeft => format!("issue #{old} left"),
        CampEventKind::Other(kind) => kind.replace('_', " "),
    };
    let reason = authored_reason(event.reason.as_deref())
        .map(|reason| format!(" — {reason}"))
        .unwrap_or_default();
    let by = match &event.actor {
        None => String::new(),
        Some(actor) => format!(" by {}", names.aliases.display(Some(actor), None).label),
    };
    format!("{what}{reason}{by}")
}

fn census(rows: &CampRows, names: &Names) -> Vec<CensusEntry> {
    rows.participants
        .iter()
        .map(|participant| CensusEntry {
            subject_key: participant.subject_key.clone(),
            subject: participant.subject.clone(),
            display: names.subject(&participant.subject, participant.principal.as_ref()),
            thread: participant
                .subject
                .thread_id()
                .and_then(|id| names.thread_uri(id)),
            role: participant.role,
            lead: participant.lead,
            level: participant.level,
            reason: authored_reason(participant.reason.as_deref()).map(str::to_string),
            assigned_by: participant
                .assigned_by
                .as_ref()
                .map(|principal| names.aliases.display(Some(principal), None)),
            since: participant.since,
        })
        .collect()
}

/// The project's tenets whose statement or rationale cites the camp or one of
/// its joined issues, by structural URI match.
async fn linked_tenets(
    db: &LocalDb,
    project_id: &str,
    names: &Names,
    camp: &Camp,
    issues: &[CampIssue],
) -> Vec<LinkedTenet> {
    let mut wanted: HashSet<String> = issues
        .iter()
        .map(|issue| build_issue_uri(&names.project_key, issue.number as i32))
        .collect();
    wanted.insert(build_project_camp_uri(&names.project_key, &camp.slug));
    let tenets = db
        .query_all(
            "SELECT number, statement, COALESCE(rationale, ''), status FROM tenets
             WHERE project_id = ?1 ORDER BY number DESC",
            (project_id.to_string(),),
            |row| Ok((row.i64(0)?, row.text(1)?, row.text(2)?, row.text(3)?)),
        )
        .await
        .unwrap_or_default();
    tenets
        .into_iter()
        .filter(|(_, statement, rationale, _)| {
            cairn_uri::scan_uris(&format!("{statement}\n{rationale}"))
                .iter()
                .any(|found| wanted.contains(&found.resource.to_uri()))
        })
        .take(TENET_LIMIT)
        .map(|(number, statement, _, status)| LinkedTenet {
            uri: cairn_uri::build_project_tenet_uri(&names.project_key, &number.to_string()),
            number,
            statement,
            status,
        })
        .collect()
}

/// Every camp of a project in brief, base first, then pitched before struck.
///
/// `db` is the database that owns the project; `local` is the install's
/// private database, which holds posts and the principal alias registries.
pub async fn list_summaries(
    db: &LocalDb,
    local: Option<&LocalDb>,
    project_id: &str,
    project_key: &str,
    now: i64,
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
    Ok(rows.iter().map(|rows| summary(rows, &names, now)).collect())
}

/// One camp in full, or `None` when the project has no camp `slug`.
pub async fn camp_view(
    db: &LocalDb,
    local: Option<&LocalDb>,
    project_id: &str,
    project_key: &str,
    slug: &str,
    now: i64,
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
                let history = camp_history_conn(conn, &camp.id, HISTORY_LIMIT).await?;
                let roadmap = if camp.is_base() {
                    let mut roadmap = Vec::new();
                    for other in list_camps_conn(conn, &project_id).await? {
                        if !other.is_base() && !other.is_struck() {
                            roadmap.push(camp_rows_conn(conn, other).await?);
                        }
                    }
                    roadmap
                } else {
                    Vec::new()
                };
                Ok(Some((camp_rows_conn(conn, camp).await?, history, roadmap)))
            })
        })
        .await
        .map_err(|error| format!("Failed to load camp `{slug}`: {error}"))?
    };
    let Some((rows, history, roadmap)) = loaded else {
        return Ok(None);
    };
    let tenets = linked_tenets(db, project_id, &names, &rows.camp, &rows.issues).await;
    Ok(Some(CampView {
        summary: summary(&rows, &names, now),
        description: None,
        questions: Vec::new(),
        census: census(&rows, &names),
        issues: rows
            .issues
            .iter()
            .map(|issue| CampIssueRow {
                number: issue.number,
                title: issue.title.clone(),
                status: issue.status.clone(),
                updated_at: issue.updated_at,
                uri: build_issue_uri(&names.project_key, issue.number as i32),
            })
            .collect(),
        tenets,
        history: history
            .iter()
            .map(|event| HistoryEntry {
                kind: event.kind.clone(),
                text: history_text(event, &names),
                at: event.created_at,
            })
            .collect(),
        roadmap: roadmap
            .iter()
            .map(|rows| summary(rows, &names, now))
            .collect(),
    }))
}

#[cfg(test)]
mod tests;
