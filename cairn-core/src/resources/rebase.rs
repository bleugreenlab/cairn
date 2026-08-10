//! `cairn:~/rebase` — the active conflict resolution session for a node's branch.
//!
//! When a base advance cannot replay a branch, the rebase is rolled back and the
//! branch is left untouched. That is the safety property; the cost it used to
//! carry was an information-free tree. This resource pays that cost back: it
//! names what arrived, shows both sides of the merge, lists the incoming change's
//! COMPLETE file set flagged conflicting versus clean-on-retry, states whether
//! markers are actually present in the checkout, and offers the one sanctioned
//! next action.
//!
//! Both sides are recomputed from immutable commits rather than stored, so the
//! session never serves an aging patch. When an object is unavailable the read
//! says so and keeps the stored identity and inventory — it never substitutes the
//! rolled-back current tree, which answers a different question and would read as
//! an authority.
//!
//! It also PERFORMS the merge rather than describing it. Two separate patches
//! leave the agent to reconstruct the result by hand, which in practice meant
//! shelling out to `git merge-file` over temporary files; `?view=merged` computes
//! it line-wise from the same immutable commits and hands back either the
//! three-way projection or the complete merged file. That same computation is
//! what lets this page say, before a replay is requested, which conflicting files
//! still carry incoming work the whole-file restore would discard — and it is the
//! computation the replay request itself refuses on.
//!
//! Every probe here reads the branch's LIVE tip alongside the session's frozen
//! `ours`. The frozen coordinate is one of three immutable facts and must stay
//! that way, which is exactly why it cannot answer "has this been resolved yet":
//! a resolution is a commit it has never heard of.

use std::collections::{BTreeMap, BTreeSet};

use cairn_common::query::QueryParam;

use super::common::{connect_and_find_node_job, find_query_value, node_branch};
use crate::orchestrator::conflict_session::{
    assess_session_tip, load_active_session, load_latest_session, ConflictSession, MarkerState,
    SessionFile, TipAssessment, Unproven,
};
use crate::orchestrator::replay_base::{load_base_candidates, resolve_base, BaseSource};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Hard ceiling on patch lines served in one read, regardless of `limit`. A
/// merge side can be enormous; a resource that streams all of it into a context
/// window is not more useful than one that pages.
const MAX_PATCH_LINES: usize = 400;
const DEFAULT_PATCH_LINES: usize = 200;
/// Hard ceiling on inventory rows in the summary. The counts stay exact.
const MAX_FILE_ROWS: usize = 100;

/// How many conflicting paths the merged view renders in its unscoped form.
/// Each costs jj reads; a session with more than this is asking for the scoped
/// form anyway.
const MAX_MERGED_SECTIONS: usize = 10;
/// Lines of context kept around a conflict region in the unscoped merged view.
const MERGED_CONTEXT_LINES: usize = 3;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum RebaseView {
    Summary,
    BaseOurs,
    BaseTheirs,
    /// The three-way projection: base, ours, and theirs merged line-wise, with
    /// everything non-overlapping already resolved.
    ///
    /// One view, not two. A diff3-style merge result IS the three-way projection
    /// and the auto-merge candidate at the same time; building them separately
    /// would be two implementations of one idea.
    Merged,
}

#[derive(Debug)]
struct RebaseRequest<'a> {
    view: RebaseView,
    file: Option<&'a str>,
    offset: usize,
    limit: usize,
}

fn parse_rebase_request<'a>(params: &'a [QueryParam]) -> Result<RebaseRequest<'a>, String> {
    if let Some(param) = params
        .iter()
        .find(|param| !matches!(param.key.as_str(), "view" | "file" | "offset" | "limit"))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for node rebase. Expected view, file, offset, or limit.",
            param.key
        ));
    }
    let view = match find_query_value(params, "view") {
        None => RebaseView::Summary,
        Some("base-ours") => RebaseView::BaseOurs,
        Some("base-theirs") => RebaseView::BaseTheirs,
        Some("merged") => RebaseView::Merged,
        Some(value) => {
            return Err(format!(
                "Invalid node rebase view '{value}'. Expected merged, base-ours, or base-theirs."
            ));
        }
    };
    let file = find_query_value(params, "file").filter(|value| !value.is_empty());
    if file.is_some() && view == RebaseView::Summary {
        return Err(
            "file=PATH is only valid with view=merged, view=base-ours, or view=base-theirs"
                .to_string(),
        );
    }
    let parse_number = |key: &str| -> Result<Option<usize>, String> {
        match find_query_value(params, key).filter(|value| !value.is_empty()) {
            None => Ok(None),
            Some(value) => value
                .parse::<usize>()
                .map(Some)
                .map_err(|_| format!("Invalid {key} '{value}' for node rebase; expected a number")),
        }
    };
    let offset = parse_number("offset")?.unwrap_or(0);
    // The cap is applied here rather than trusted from the caller, so no request
    // can ask the server for an unbounded read.
    let limit = parse_number("limit")?
        .unwrap_or(DEFAULT_PATCH_LINES)
        .clamp(1, MAX_PATCH_LINES);
    Ok(RebaseRequest {
        view,
        file,
        offset,
        limit,
    })
}

pub(super) async fn read_node_rebase(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    params: &[QueryParam],
) -> String {
    let request = match parse_rebase_request(params) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let (conn, job) = match connect_and_find_node_job(&db, project, number, exec_seq, node_id).await
    {
        Ok(found) => found,
        Err(error) => return error,
    };
    let branch = match node_branch(&conn, &job.id).await {
        Ok(Some(branch)) => branch,
        Ok(None) => {
            return "This node has no branch, so it cannot have a rebase session.".to_string()
        }
        Err(error) => return error,
    };
    let session = match load_active_session(&db, &branch).await {
        Ok(Some(session)) => session,
        Ok(None) => return render_closed_session(orch, &db, &conn, &job.id, &branch).await,
        Err(error) => return error,
    };

    match request.view {
        RebaseView::Summary => render_summary(orch, project, number, exec_seq, node_id, &session),
        RebaseView::Merged => {
            render_merged(orch, project, number, exec_seq, node_id, &session, &request)
        }
        RebaseView::BaseOurs | RebaseView::BaseTheirs => {
            render_side(orch, project, number, exec_seq, node_id, &session, &request)
        }
    }
}

/// The page for a branch with no OPEN session.
///
/// A bare "nothing to do here" is the right answer for a branch that never
/// conflicted, and the wrong one for a branch that just watched a replay land:
/// the arc from queued through replaying to published would end by vanishing,
/// which is what sent agents back to re-read a page that had already forgotten.
/// So the most recent session is loaded whatever its state, and a closed one
/// reports its outcome.
///
/// It is also the wrong answer for a branch whose base advanced without anything
/// replaying it. Absence of a session is absence of a CONFLICT, not absence of a
/// problem: an advance that was deferred, dropped, or never attempted leaves a
/// branch silently behind its base, still green locally and unmergeable
/// remotely. So this asks the store where the branch actually stands before
/// telling anyone there is nothing to do.
async fn render_closed_session(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    branch: &str,
) -> String {
    let latest = match load_latest_session(db, branch).await {
        Ok(latest) => latest,
        Err(error) => return error,
    };
    let Some(session) = latest.filter(|session| !session.is_open()) else {
        return render_no_session(orch, conn, job_id, branch).await;
    };

    let closed_at = crate::clock::stamp(session.updated_at)
        .map(|stamp| format!(" at {stamp}"))
        .unwrap_or_default();
    let outcome = match session.resolution_state.as_deref() {
        Some("superseded") => format!(
            "It was **superseded**{closed_at}: the base advanced again before this one was \
             replayed, so its coordinates describe a merge nobody will perform. If a newer \
             conflict is open, this page shows it instead; if none is, the newer advance replayed \
             cleanly."
        ),
        _ => format!(
            "It was **resolved**{closed_at}: your branch was replayed onto `{}` and published. \
             Its ancestry now carries that base, so the PR is no longer conflicting on this \
             account.",
            session.destination_commit
        ),
    };
    // A closed session says where the branch was left, never where it now
    // stands. The base can have moved again since — and if that advance was
    // deferred or never fired, nothing reopened a session to say so. Reporting
    // an old resolution as "nothing needs doing" is the same false clearance
    // this page gives a branch that never conflicted at all, so it is answered
    // the same way: from the store.
    let standing = load_base_standing(orch, conn, job_id, branch).await;
    let now = match &standing {
        Ok(standing) if !standing.carries_base => format!(
            "\n⚠️ **Since then, the base has moved again.** `{}` is now at `{}`, which this \
             branch's ancestry does not contain, and no session opened for it — so that advance \
             was deferred or never ran. The outcome above is history, not this branch's current \
             standing.\n{}\n{}\n",
            standing.base_branch,
            standing.destination,
            standing.superseded_note(),
            replay_next_action(&standing.base_branch),
        ),
        Ok(standing) => format!(
            "\nNothing needs doing here: the branch carries its base's current tip. A later base \
             advance that cannot replay cleanly opens a fresh session at this same \
             address.\n{}",
            standing.superseded_note()
        ),
        Err(why) => format!(
            "\nWhere this branch now stands relative to its base could not be established, so \
             treat the outcome above as history rather than a current all-clear.\n\n{why}\n"
        ),
    };
    format!(
        "# Rebase session for `{branch}`\n\nNo OPEN session — the last one has closed.\n\n## \
         Outcome\n\n{outcome}\n\n{}{now}",
        coordinates_block(&session),
    )
}

/// The one sanctioned way to move a branch's ancestry, rendered for a page that
/// has no session to quote a fingerprint from.
fn replay_next_action(base_branch: &str) -> String {
    format!(
        "## Next action\n\n```\nwrite({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",\
         payload:{{action:\"replay\"}}}}]}})\n```\n\nThis asks the store to replay this branch \
         onto `{base_branch}`. A clean replay publishes the branch; one that conflicts opens a \
         session at this address with both sides and the merged view. Never rebase, reset, or \
         force-push by hand."
    )
}

/// Where a branch with no session stands relative to the base it will be
/// replayed onto.
struct BaseStanding {
    base_branch: String,
    destination: String,
    /// True when the branch already has the base's current tip in its ancestry.
    carries_base: bool,
    /// The recorded base this standing had to move past, present exactly when
    /// that name no longer exists in the store.
    superseded: Option<String>,
    source: BaseSource,
}

impl BaseStanding {
    /// Said out loud whenever the branch named here is NOT the one the job
    /// recorded. Silently measuring against a different base would make every
    /// other number on the page unreadable.
    fn superseded_note(&self) -> String {
        let Some(superseded) = self.superseded.as_deref() else {
            return String::new();
        };
        format!(
            "\n> The base `{superseded}` this branch recorded no longer exists in the store — the \
             usual cause is that its parent merged and the branch was deleted with it. Its \
             standing here is measured against `{}`, {}. Requesting a replay corrects the \
             record, whether or not it has anything left to move.\n",
            self.base_branch,
            self.source.describe()
        )
    }
}

/// Ask the store where this branch stands relative to the base it will be
/// replayed onto.
///
/// The failure is a SENTENCE, not an absence. "Base branch, repository, or store
/// did not resolve" was one message for three unrelated situations, and the
/// commonest of them — the recorded base was deleted when the parent merged —
/// is both specifically diagnosable and specifically actionable, so each one now
/// says which it is.
async fn load_base_standing(
    orch: &Orchestrator,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    branch: &str,
) -> Result<BaseStanding, String> {
    let mut rows = conn
        .query(
            "SELECT p.repo_path
             FROM jobs j JOIN projects p ON j.project_id = p.id
             WHERE j.id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
        .map_err(|error| format!("This node's project row could not be read: {error}"))?;
    let repo_path = rows
        .next()
        .await
        .map_err(|error| format!("This node's project row could not be read: {error}"))?
        .and_then(|row| row.opt_text(0).ok().flatten())
        .filter(|repo_path| !repo_path.is_empty())
        .ok_or_else(|| {
            "Cairn holds no repository path for this node's project, so the branch store could \
             not be asked anything about it."
                .to_string()
        })?;

    let candidates = load_base_candidates(conn, job_id)
        .await
        .map_err(|error| format!("This node's base branch could not be read: {error}"))?;
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repo_path));
    let resolved =
        resolve_base(&jj, &store, branch, &candidates).map_err(|error| error.to_string())?;
    let carries_base = crate::jj::branch_carries_commit(&jj, &store, branch, &resolved.commit);
    Ok(BaseStanding {
        base_branch: resolved.branch,
        destination: resolved.commit,
        carries_base,
        superseded: resolved.superseded,
        source: resolved.source,
    })
}

/// The page for a branch that has never had a conflict session at all.
///
/// The old text — "this branch has never hit a conflicting base advance,
/// nothing needs doing here" — was true about conflicts and wrong about the
/// branch whenever an advance had been owed and never ran. Those two branches
/// are indistinguishable from the session table alone and completely distinct in
/// the store, so the store is what decides which page this is.
async fn render_no_session(
    orch: &Orchestrator,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    branch: &str,
) -> String {
    let standing = match load_base_standing(orch, conn, job_id, branch).await {
        Ok(standing) => standing,
        Err(why) => {
            return format!(
                "# Rebase session\n\nNo conflict resolution session for `{branch}`, and where this \
                 branch stands relative to its base could not be established.\n\n{why}\n\nThat is \
                 a fact about this read, not a clean bill of health for the branch."
            )
        }
    };
    let superseded = standing.superseded_note();
    let BaseStanding {
        base_branch,
        destination,
        carries_base,
        ..
    } = standing;

    if carries_base {
        return format!(
            "# Rebase session\n\nNo conflict resolution session for `{branch}`, and none is \
             needed: the branch already carries `{base_branch}` at `{destination}` in its \
             ancestry. Nothing needs doing here.\n{superseded}"
        );
    }

    format!(
        "# Rebase session for `{branch}`\n\nNo conflict session has ever opened on this branch — \
         **and its base has moved out from under it anyway.** `{base_branch}` is now at \
         `{destination}`, which this branch's ancestry does not contain.\n\nThat combination is \
         the quiet failure, not a clean state. A session is the artifact of a rebase that was \
         ATTEMPTED and hit a conflict; no session means no attempt reached that point — the \
         advance was deferred while a run batch was in flight, or never fired at all. Nothing \
         about it is visible from your checkout: local checks stay green, `git diff \
         {base_branch}...HEAD` stays correct, and only the pull request shows it, as \
         `Mergeable: CONFLICTING`.\n\nEditing files cannot fix it. Your branch's ANCESTRY is what \
         is stale, and slot refs are downstream exports of the runner's store, so nothing you \
         commit moves it.\n{superseded}\n{next}\n",
        next = replay_next_action(&base_branch),
    )
}

fn incoming_line(session: &ConflictSession) -> String {
    let what = match (
        session.incoming.pr_number,
        session.incoming.issue.as_deref(),
    ) {
        (Some(pr), Some(issue)) => format!("PR #{pr} ({issue})"),
        (Some(pr), None) => format!("PR #{pr}"),
        (None, Some(issue)) => issue.to_string(),
        (None, None) => "an external advance".to_string(),
    };
    let onto = if session.incoming.base_branch.is_empty() {
        session.target_branch.clone()
    } else {
        session.incoming.base_branch.clone()
    };
    format!("{what} landed on `{onto}`")
}

fn marker_line(session: &ConflictSession) -> String {
    match session.marker_state {
        MarkerState::Materialized => {
            let bearing: Vec<&str> = session
                .files
                .iter()
                .filter(|file| file.marker_disposition.as_deref() == Some("markers"))
                .map(|file| file.path.as_str())
                .collect();
            if bearing.is_empty() {
                "**Markers:** materialized, and none were needed — the three-way merge resolved \
                 every conflicting file on its own. Review the merged content, then commit it."
                    .to_string()
            } else {
                format!(
                    "**Markers:** present in your checkout, in {}. Resolve them with ordinary \
                     file writes and commit; a file still bearing markers cannot be committed.",
                    bearing.join(", ")
                )
            }
        }
        MarkerState::Pending => "**Markers:** requested but NOT yet confirmed present. Do not go \
             looking for them yet; the durable worker retries and this line changes when the \
             executor confirms."
            .to_string(),
        MarkerState::Failed => format!(
            "**Markers:** could not be projected into your checkout ({}). Read `?view=merged` \
             below instead — it computes the same merge and hands you the complete merged file.",
            session
                .marker_diagnostic
                .as_deref()
                .unwrap_or("no diagnostic recorded")
        ),
        MarkerState::NotMaterialized => "**Markers:** not materialized. Read `?view=merged` below \
             — it computes the merge for you rather than leaving you to reconstruct it from two \
             separate patches."
            .to_string(),
    }
}

fn coordinates_block(session: &ConflictSession) -> String {
    let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "unavailable".to_string());
    format!(
        "| side | commit |\n|---|---|\n| base (fork point) | `{}` |\n| ours (your branch) | \
         `{}` |\n| theirs (incoming) | `{}` |\n",
        field(&session.base),
        field(&session.ours),
        field(&session.theirs)
    )
}

fn file_table(title: &str, files: &[&SessionFile]) -> String {
    if files.is_empty() {
        return String::new();
    }
    let shown = files.len().min(MAX_FILE_ROWS);
    let mut out = format!(
        "\n## {title} ({})\n\n| file | change |\n|---|---|\n",
        files.len()
    );
    for file in files.iter().take(shown) {
        out.push_str(&format!("| `{}` | {} |\n", file.path, file.status));
    }
    if files.len() > shown {
        out.push_str(&format!(
            "\n… and {} more.\n",
            files.len().saturating_sub(shown)
        ));
    }
    out
}

fn render_summary(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
) -> String {
    let base = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/rebase");
    let conflicting: Vec<&SessionFile> = session.conflicting().collect();
    let clean: Vec<&SessionFile> = session.clean_on_retry().collect();

    // A session written by a different build may mean something else by these
    // columns. Say so rather than interpreting it with this build's rules.
    let version_note = if session.version_is_current() {
        String::new()
    } else {
        format!(
            "\n⚠️ This session was recorded by a different build (diagnostic version {}, this build \
             reads {}). Its identity and file list are shown as stored; treat the interpretation \
             below with suspicion, and prefer requesting a fresh replay.\n",
            session.diagnostic_version,
            crate::orchestrator::conflict_session::CONFLICT_DIAGNOSTIC_VERSION
        )
    };

    let (condition, replay_payload) = if session.is_base_drift() {
        (
            "**Condition: base drift.** Every conflicting file is already byte-identical between your \
             branch and the new base, so there is nothing to merge — what is stale is your branch's \
             ANCESTRY. Editing files will not clear this, and you cannot repair it from your checkout: \
             slot refs are downstream exports of the runner's private jj store. Request the replay \
             below.",
            "action:\"replay\"",
        )
    } else {
        (
            "**Condition: content conflict.** The two sides genuinely disagree. Read both versions of \
             each conflicting file, write the merged result with ordinary edits, and commit it on your \
             branch. Then request the replay below so the branch actually moves onto the base — \
             resolving the content does not by itself change your branch's ancestry.",
            "action:\"replay\",resolution:\"take-committed-tip\"",
        )
    };

    // One store probe answers three questions: has the agent committed anything
    // since, does that commit already contain both sides, and would the
    // whole-file restore drop anything. Computed once so the sections below can
    // never disagree with each other or with the guard on the replay itself.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    let assessment = assess_session_tip(&jj, &store, session);

    format!(
        "# Rebase session for `{branch}`\n{version_note}\n{incoming}, and the automatic replay of your branch \
         onto it recorded a conflict, so it was rolled back. Your branch is untouched and on its \
         own content; nothing was lost.\n\n{condition}\n{status}\n{markers}\n\n## Three-way \
         coordinates\n\n{coords}\nThese are immutable commits. Read any file as of either side \
         with `?branch=<commit>`, or read the merge:\n\n- `{base}?view=merged` — the two sides \
         merged line-wise, with everything non-overlapping already resolved. Add `file=PATH` for \
         the complete merged file to commit.\n- `{base}?view=base-ours` — what your branch \
         did\n- `{base}?view=base-theirs` — what arrived\n\nAll three accept `file=PATH`, \
         `offset=N`, and `limit=N`.\n{resolution_section}{conflicting_table}{dropped_section}{collision_section}{clean_table}\n## Next \
         action\n\n```\nwrite({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{{replay_payload}}}}}]}})\n```\n\nThis \
         asks the store to replay your branch onto `{dest}`. It is the only way the branch's \
         ancestry moves — never rebase, reset, or force-push by hand. A clean replay publishes the \
         branch and closes this session; a conflicting one refreshes this page.\n\nSession \
         fingerprint `{fingerprint}`, recorded {recorded}.\n",
        branch = session.bookmark,
        incoming = incoming_line(session),
        markers = marker_line(session),
        status = status_line(session),
        coords = coordinates_block(session),
        resolution_section = resolution_section(assessment.as_ref(), &base),
        dropped_section = dropped_work_section(assessment.as_ref(), &base),
        collision_section = collision_section(&jj, &store, session),
        conflicting_table = file_table("Conflicting files, yours to merge", &conflicting),
        clean_table = file_table(
            "Also arriving with this change, cleanly on replay",
            &clean
        ),
        dest = session.destination_commit,
        fingerprint = session.fingerprint(),
        recorded = crate::clock::stamp(session.updated_at)
            .unwrap_or_else(|| "at an unrecorded time".to_string()),
        replay_payload = replay_payload,
    )
}

/// The advisory notice that rides on every merged rendering.
///
/// This view is the one most likely to be mistaken for an authority: it looks
/// like a merge result because it is one, but the store's replay is judged by
/// jj's merge and not by this one. Said where the content is, not in a footnote.
const MERGED_ADVISORY: &str = "This merge is ADVISORY: computed line-wise in this process, while \
     the store's replay is the authority on what actually lands. It is here so you can see the \
     merge before requesting one, not so you can skip requesting it.";

/// The warning that belongs beside rendered conflict markers.
const MERGED_MARKER_WARNING: &str = "⚠️ The text below contains literal conflict markers. Cairn \
     REFUSES a commit containing them unless you supply `conflict_markers_reason`, so pasting this \
     form into a file and committing is correctly rejected. Resolve each region first, or read \
     `file=PATH` for the marker-free completion candidate.";

/// The three-way projection and the auto-merge candidate, which are the same
/// artifact seen at two scopes.
///
/// Unscoped: one section per conflicting path, showing the overlapping regions
/// with context — the "show base, ours, and theirs together" view. Scoped to a
/// file: the COMPLETE merged content, paged, which is what gets committed.
///
/// What it renders depends on whether the branch has moved. An unmoved tip has
/// nothing resolved yet, so the markers are the point. A moved tip is showing a
/// resolution in progress, so the candidate (conflicts already taken from the
/// committed tip) plus the diff from the tip to it answers the more useful
/// question: what does my resolution still not include.
fn render_merged(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
    request: &RebaseRequest<'_>,
) -> String {
    let uri = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/rebase");
    let (Some(base_rev), Some(theirs_rev)) = (session.base.as_deref(), session.theirs.as_deref())
    else {
        return format!(
            "# Merged view\n\nThis merge cannot be computed: the session did not record both \
             outer coordinates for it. The stored identity and file inventory are still intact — \
             read `{uri}` for them."
        );
    };
    if session.is_base_drift() {
        return format!(
            "# Merged view\n\nThis session is **base drift**: every conflicting file is already \
             byte-identical between your branch and the new base, so there is no content to \
             merge and this view would show you an unchanged file. What is stale is your branch's \
             ANCESTRY. Read `{uri}` and request the replay."
        );
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    // The LIVE tip, not the session's frozen `ours`: a resolution the agent has
    // already committed is exactly what this view needs to account for.
    let ours_rev = crate::jj::bookmark_commit(&jj, &store, &session.bookmark)
        .unwrap_or_else(|| session.ours.clone().unwrap_or_default());
    let tip_moved = session.ours.as_deref() != Some(ours_rev.as_str());

    match request.file {
        Some(file) => render_merged_file(
            &jj, &store, base_rev, &ours_rev, theirs_rev, file, tip_moved, request, &uri,
        ),
        None => render_merged_overview(
            &jj, &store, base_rev, &ours_rev, theirs_rev, session, tip_moved, &uri,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn render_merged_file(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    base_rev: &str,
    ours_rev: &str,
    theirs_rev: &str,
    file: &str,
    tip_moved: bool,
    request: &RebaseRequest<'_>,
    uri: &str,
) -> String {
    let paths = vec![file.to_string()];
    let Some((_, sides)) =
        crate::jj::read_sides_for_paths(jj, store, base_rev, ours_rev, theirs_rev, &paths)
            .into_iter()
            .next()
    else {
        return format!("# Merged — `{file}`\n\nNothing to merge for this path.");
    };
    let sides = match sides {
        Ok(sides) => sides,
        Err(error) => {
            return format!(
                "# Merged — `{file}`\n\nThis merge is unavailable: {error}\n\nThe conflict's \
                 identity and complete file inventory are still recorded — read `{uri}` for them."
            )
        }
    };
    let (Some(base), Some(ours), Some(theirs)) = (
        sides.base.mergeable(),
        sides.ours.mergeable(),
        sides.theirs.mergeable(),
    ) else {
        return format!(
            "# Merged — `{file}`\n\nOne side of this path is not UTF-8 text, so it cannot be \
             merged line-wise and this view will not guess at it. Read the two sides as patches \
             instead: `{uri}?view=base-ours&file={file}` and `{uri}?view=base-theirs&file={file}`."
        );
    };

    // An unmoved tip has nothing resolved yet, so the markers ARE the answer. A
    // moved tip is mid-resolution, so the candidate plus the diff from the tip
    // to it is the more useful one: it is directly committable and it names
    // exactly what the resolution still lacks.
    let (heading, body, note) = if tip_moved {
        let candidate = crate::jj::completion_candidate(base, ours, theirs);
        let note = if candidate == ours || candidate.trim_end() == ours.trim_end() {
            format!(
                "Your committed tip already contains both sides of this file. Nothing further to \
                 commit here — the replay's whole-file restore of `{file}` is exactly right."
            )
        } else {
            let dropped = crate::jj::create_patch_body(ours, &candidate);
            format!(
                "Your committed tip is missing part of the incoming change. This is what it still \
                 lacks, and committing the file below supplies it:\n\n```diff\n{dropped}\n```"
            )
        };
        (
            "the completion candidate",
            candidate,
            format!("{note}\n\n{MERGED_ADVISORY}"),
        )
    } else {
        let preview = crate::jj::merge_preview(base, ours, theirs);
        let note = match preview.regions() {
            0 => format!(
                "The two sides did not overlap in this file: this is the complete merged content, \
                 ready to commit as-is.\n\n{MERGED_ADVISORY}"
            ),
            regions => format!(
                "{regions} conflict region(s) remain — resolve each one, then commit the whole \
                 file.\n\n{MERGED_MARKER_WARNING}\n\n{MERGED_ADVISORY}"
            ),
        };
        ("the merged file", preview.text().to_string(), note)
    };

    let lines: Vec<&str> = body.lines().collect();
    let total = lines.len();
    if total == 0 {
        return format!("# Merged — `{file}`\n\n{note}\n\nThe merged file is empty.");
    }
    let start = request.offset.min(total);
    let end = start.saturating_add(request.limit).min(total);
    let mut out = format!(
        "# Merged — `{file}`\n\nThis is {heading}: the COMPLETE file, not a patch. Commit it \
         whole.\n\n{note}\n\n```\n{}\n```\n",
        lines[start..end].join("\n")
    );
    if end < total {
        out.push_str(&format!(
            "\n[lines {}–{} of {} — continue: {uri}?view=merged&file={file}&offset={}&limit={}]\n",
            start + 1,
            end,
            total,
            end,
            request.limit
        ));
        out.push_str(
            "\nThe file is only complete once you have read to the end; committing a prefix of it \
             would truncate the file.\n",
        );
    } else {
        out.push_str(&format!("\n[lines {}–{} of {}]\n", start + 1, end, total));
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn render_merged_overview(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    base_rev: &str,
    ours_rev: &str,
    theirs_rev: &str,
    session: &ConflictSession,
    tip_moved: bool,
    uri: &str,
) -> String {
    let all: Vec<String> = session
        .conflicting()
        .map(|file| file.path.clone())
        .collect();
    if all.is_empty() {
        return format!(
            "# Merged view\n\nThis session records no conflicting files, so there is nothing to \
             merge. Read `{uri}` for what it does record."
        );
    }
    let shown: Vec<String> = all.iter().take(MAX_MERGED_SECTIONS).cloned().collect();
    let elided = all.len().saturating_sub(shown.len());

    let mut out = format!(
        "# Merged view\n\nThe two sides merged line-wise: everything they did NOT both touch is \
         already resolved here, and what remains is the overlap. {MERGED_ADVISORY}\n\nFor the \
         complete merged content of one file — which is what you commit — read \
         `{uri}?view=merged&file=PATH`.\n"
    );
    let mut any_markers = false;

    for (path, sides) in
        crate::jj::read_sides_for_paths(jj, store, base_rev, ours_rev, theirs_rev, &shown)
    {
        out.push_str(&format!("\n## `{path}`\n\n"));
        let sides = match sides {
            Ok(sides) => sides,
            Err(error) => {
                out.push_str(&format!("Unavailable: {error}\n"));
                continue;
            }
        };
        let (Some(base), Some(ours), Some(theirs)) = (
            sides.base.mergeable(),
            sides.ours.mergeable(),
            sides.theirs.mergeable(),
        ) else {
            out.push_str(
                "One side is not UTF-8 text, so this path cannot be merged line-wise. Read it as \
                 two patches instead.\n",
            );
            continue;
        };
        let preview = crate::jj::merge_preview(base, ours, theirs);
        match preview.regions() {
            0 if tip_moved => out.push_str(
                "Clean — your committed tip already carries both sides. Nothing to do here.\n",
            ),
            0 => out.push_str(
                "Clean — the two sides did not overlap in this file. It merges on its own.\n",
            ),
            regions => {
                any_markers = true;
                out.push_str(&format!(
                    "{regions} conflict region(s):\n\n```\n{}\n```\n",
                    conflict_excerpt(preview.text())
                ));
            }
        }
    }
    if elided > 0 {
        out.push_str(&format!(
            "\n… and {elided} further conflicting file(s). Read them individually with \
             `{uri}?view=merged&file=PATH`.\n"
        ));
    }
    if any_markers {
        out.push_str(&format!("\n{MERGED_MARKER_WARNING}\n"));
    }
    out
}

/// The conflict regions of a merged file with a little surrounding context,
/// so the overview shows the overlap rather than the whole file.
fn conflict_excerpt(merged: &str) -> String {
    let lines: Vec<&str> = merged.lines().collect();
    let mut keep = vec![false; lines.len()];
    let mut inside = false;
    for (index, line) in lines.iter().enumerate() {
        if line.starts_with("<<<<<<<") {
            inside = true;
        }
        if inside {
            let from = index.saturating_sub(MERGED_CONTEXT_LINES);
            let to = (index + MERGED_CONTEXT_LINES + 1).min(lines.len());
            keep[from..to].iter_mut().for_each(|slot| *slot = true);
        }
        if line.starts_with(">>>>>>>") {
            inside = false;
        }
    }

    let mut out: Vec<String> = Vec::new();
    let mut gap = false;
    for (index, line) in lines.iter().enumerate() {
        if keep[index] {
            if gap {
                out.push("…".to_string());
                gap = false;
            }
            out.push((*line).to_string());
        } else {
            gap = !out.is_empty();
        }
    }
    // A gap still open at the end means the file continues past the excerpt.
    // Without this the reader cannot tell an excerpt that ends from a file that
    // ends, which is the difference between "nothing more to resolve" and
    // "there is more and you have not seen it".
    if gap {
        out.push("…".to_string());
    }
    out.join("\n")
}

/// Two files, one on each side, added into the same directory under the same
/// zero-padded numeric prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NumberCollision {
    directory: String,
    incoming: String,
    ours: String,
    /// One past the highest prefix in that directory across both sides, padded
    /// to the width the colliding name uses.
    suggested: String,
}

/// The leading zero-padded number of a name like `0148_channel_thread.sql`, with
/// the width it was written at.
fn numeric_prefix(name: &str) -> Option<(u64, usize)> {
    let digits: String = name.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() || !name[digits.len()..].starts_with('_') {
        return None;
    }
    digits.parse().ok().map(|value| (value, digits.len()))
}

fn split_directory(path: &str) -> (&str, &str) {
    match path.rsplit_once('/') {
        Some((directory, name)) => (directory, name),
        None => ("", path),
    }
}

/// Detect numeric-prefix collisions between files each side ADDED.
///
/// Structural rather than migration-aware on purpose: nothing here knows what a
/// migration is. Any convention that numbers files in a directory — migrations,
/// ordered fixtures, numbered docs — gets the same detection, and the check
/// section below names `migrations` on its own because the colliding path
/// matches that check's globs. The two compose without either hardcoding the
/// other's knowledge.
///
/// `occupied` maps a directory to every name already in it on the incoming side,
/// which is what makes the suggestion a real next value rather than a guess.
fn detect_number_collisions(
    ours_added: &[String],
    incoming_added: &[String],
    occupied: &BTreeMap<String, Vec<String>>,
) -> Vec<NumberCollision> {
    let mut collisions = Vec::new();
    for ours in ours_added {
        let (directory, our_name) = split_directory(ours);
        let Some((value, width)) = numeric_prefix(our_name) else {
            continue;
        };
        for incoming in incoming_added {
            let (incoming_directory, incoming_name) = split_directory(incoming);
            if incoming_directory != directory || incoming_name == our_name {
                continue;
            }
            if numeric_prefix(incoming_name).map(|(value, _)| value) != Some(value) {
                continue;
            }
            // The next free number has to clear BOTH sides: the incoming
            // directory listing (which already includes the incoming addition)
            // and this side's own additions, which that listing has never seen.
            let highest = occupied
                .get(directory)
                .into_iter()
                .flatten()
                .map(String::as_str)
                .chain(ours_added.iter().map(|path| split_directory(path).1))
                .filter(|name| split_directory(name).0.is_empty())
                .filter_map(numeric_prefix)
                .map(|(value, _)| value)
                .max()
                .unwrap_or(value);
            collisions.push(NumberCollision {
                directory: directory.to_string(),
                incoming: incoming_name.to_string(),
                ours: our_name.to_string(),
                suggested: format!("{:0width$}", highest + 1, width = width),
            });
        }
    }
    collisions
}

/// Render the collision section, probing the store for what each side added.
fn collision_section(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    session: &ConflictSession,
) -> String {
    let (Some(base), Some(ours), Some(theirs)) = (
        session.base.as_deref(),
        session.ours.as_deref(),
        session.theirs.as_deref(),
    ) else {
        return String::new();
    };
    let ours_added: Vec<String> = crate::jj::diff_name_status(jj, store, base, ours)
        .into_iter()
        .filter(|(status, _)| status == "A")
        .map(|(_, path)| path)
        .collect();
    if ours_added.is_empty() {
        return String::new();
    }
    let incoming_added: Vec<String> = session
        .files
        .iter()
        .filter(|file| file.status == "A")
        .map(|file| file.path.clone())
        .collect();

    // Only directories that could actually collide are listed, so the ordinary
    // session pays no subprocess for this section at all.
    let mut occupied: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let candidate_directories: BTreeSet<&str> = ours_added
        .iter()
        .filter(|path| numeric_prefix(split_directory(path).1).is_some())
        .map(|path| split_directory(path).0)
        .collect();
    for directory in candidate_directories {
        let listed = jj
            .run(
                store,
                &[
                    "file",
                    "list",
                    "--ignore-working-copy",
                    "-r",
                    theirs,
                    "--",
                    directory,
                ],
                "jj file list (numbered collision probe)",
            )
            .unwrap_or_default();
        occupied.insert(
            directory.to_string(),
            listed
                .lines()
                .map(|line| split_directory(line.trim()).1.to_string())
                .filter(|name| !name.is_empty())
                .collect(),
        );
    }

    let collisions = detect_number_collisions(&ours_added, &incoming_added, &occupied);
    if collisions.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n## Numbered-file collisions\n\n");
    for collision in &collisions {
        out.push_str(&format!(
            "`{}/` — incoming claims `{}`; your branch adds `{}`. Renumber yours to **{}** and \
             update wherever it is registered.\n",
            collision.directory, collision.incoming, collision.ours, collision.suggested
        ));
    }
    out.push_str(
        "\nBoth files will exist after the replay, so this is not something the merge resolves for \
         you: two files claiming one number is a conflict of meaning, not of text.\n",
    );
    out
}

/// Whether a replay is outstanding, and how far it has got.
///
/// Omitted when nothing has been requested: the Next action block below already
/// says what to do, and a line saying "no replay requested" would be noise on
/// every read of the ordinary case.
fn status_line(session: &ConflictSession) -> String {
    let Some(progress) = session.replay_progress() else {
        return String::new();
    };
    let requested = crate::clock::stamp(progress.requested_at)
        .map(|stamp| format!(", requested {stamp}"))
        .unwrap_or_default();
    if progress.running {
        format!(
            "\n**Status:** replay IN PROGRESS onto `{}`{requested}. It runs under the store lock; \
             nothing runs in your slot.\n",
            session.destination_commit
        )
    } else {
        format!(
            "\n**Status:** replay QUEUED onto `{}`{requested}. The durable reconcile worker picks \
             it up; nothing runs in your slot, and re-requesting does not make it sooner.\n",
            session.destination_commit
        )
    }
}

/// Whether the branch's committed tip has already absorbed both sides.
///
/// The reviewer's case: a file that already contains both changes, a node diff
/// showing no conflicts, and GitHub still reporting the PR as conflicting. Those
/// are all true at once, and naming the distinction is the whole point of this
/// section — content resolution and ancestry replay are separate facts, and only
/// the second is what GitHub is looking at.
fn resolution_section(assessment: Option<&TipAssessment>, base: &str) -> String {
    let Some(assessment) = assessment.filter(|assessment| assessment.moved) else {
        return String::new();
    };
    let mut out = String::from("\n## Resolution assessment\n\n");
    if assessment.every_path_is_resolved() {
        out.push_str(&format!(
            "Your committed tip `{}` contains BOTH sides of every conflicting file. **Content \
             appears resolved; ancestry replay remains.**\n\nGitHub will keep reporting this PR \
             as conflicting until the replay lands, and that is not a second problem to solve: \
             your branch's ancestry is still rooted at the old base, and nothing you do in your \
             checkout can move it. Request the replay below.\n",
            assessment.tip
        ));
        return out;
    }
    let unresolved: Vec<&str> = assessment
        .paths
        .iter()
        .filter(|path| path.verdict != crate::jj::RestoreVerdict::Lossless)
        .map(|path| path.path.as_str())
        .collect();
    out.push_str(&format!(
        "Your committed tip `{}` has moved since the conflict was recorded, but it does not yet \
         contain both sides of every conflicting file.\n\n",
        assessment.tip
    ));
    for path in &unresolved {
        out.push_str(&format!(
            "- `{path}` — read `{base}?view=merged&file={path}` for the complete merged file.\n"
        ));
    }
    if assessment.truncated > 0 {
        out.push_str(&format!(
            "\n… and {} further conflicting file(s) were not assessed.\n",
            assessment.truncated
        ));
    }
    out
}

/// Incoming hunks living inside conflicting files but outside the conflicting
/// region — the work `take-committed-tip` discards without saying so.
///
/// Stated here, before the request is composed, because the guard on the request
/// itself refuses rather than warns and this is what makes that refusal
/// predictable instead of surprising.
fn dropped_work_section(assessment: Option<&TipAssessment>, base: &str) -> String {
    let Some(assessment) = assessment else {
        return String::new();
    };
    // The SAME predicate the replay guard refuses on, so this page and that
    // refusal can never name different files.
    let unproven = assessment.unproven();
    if unproven.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n## Clean hunks inside conflicting files\n\n");
    for (path, reason) in &unproven {
        match reason {
            Unproven::Drops(dropped) => out.push_str(&format!(
                "`{path}` carries {} incoming hunk(s) ({} line(s)) outside the conflicting region. \
                 `take-committed-tip` keeps your whole file, so these arrive only if your commit \
                 includes them. Read `{base}?view=merged&file={path}`.\n\n",
                dropped.hunks, dropped.added_lines
            )),
            Unproven::Unjudged(why) => out.push_str(&format!(
                "`{path}` cannot be checked this way — {why}. The replay would restore your \
                 committed version of it whole, so decide that deliberately.\n\n"
            )),
        }
    }
    out.push_str(
        "A replay requested while any of these stands is REFUSED rather than proceeding on an \
         unverified restore; the refusal names the same files, and carries the escape hatch for a \
         drop you actually intend.\n",
    );
    if assessment.truncated > 0 {
        out.push_str(&format!(
            "\nThis page checked the first {} conflicting file(s) only; {} more were not looked at \
             here. The replay request checks ALL of them.\n",
            assessment.paths.len(),
            assessment.truncated
        ));
    }
    out
}

fn render_side(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
    request: &RebaseRequest<'_>,
) -> String {
    let (label, to) = match request.view {
        RebaseView::BaseTheirs => ("base→theirs (what arrived)", session.theirs.as_deref()),
        _ => ("base→ours (what your branch did)", session.ours.as_deref()),
    };
    let view_key = match request.view {
        RebaseView::BaseTheirs => "base-theirs",
        _ => "base-ours",
    };
    let uri = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/rebase");

    let (Some(from), Some(to)) = (session.base.as_deref(), to) else {
        return format!(
            "# {label}\n\nThis side cannot be rendered: the session did not record both \
             coordinates for it. The stored identity and file inventory are still intact — read \
             `{uri}` for them. Nothing here has been substituted from your current tree, which \
             describes a different range."
        );
    };

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    let patch = match crate::jj::merge_side_patch(&jj, &store, from, to, request.file) {
        Ok(patch) => patch,
        Err(error) => {
            return format!(
                "# {label}\n\nThis side is unavailable: {error}\n\nThe conflict's identity and \
                 complete file inventory are still recorded — read `{uri}` for them. This read \
                 deliberately does NOT fall back to your current tree: that describes a different \
                 range and would answer a question you did not ask."
            );
        }
    };

    let lines: Vec<&str> = patch.lines().collect();
    let total = lines.len();
    if total == 0 {
        return format!("# {label}\n\n`{from}` → `{to}` — no changes on this side.");
    }
    let start = request.offset.min(total);
    let end = start.saturating_add(request.limit).min(total);
    let body = lines[start..end].join("\n");

    let scope = request
        .file
        .map(|file| format!(" scoped to `{file}`"))
        .unwrap_or_default();
    let mut out = format!("# {label}\n\n`{from}` → `{to}`{scope}\n\n```diff\n{body}\n```\n");
    if end < total {
        let file_param = request
            .file
            .map(|file| format!("&file={file}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "\n[lines {}–{} of {} — continue: {uri}?view={view_key}&offset={}&limit={}{file_param}]\n",
            start + 1,
            end,
            total,
            end,
            request.limit
        ));
    } else {
        out.push_str(&format!("\n[lines {}–{} of {}]\n", start + 1, end, total));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standing(superseded: Option<&str>) -> BaseStanding {
        BaseStanding {
            base_branch: "main".into(),
            destination: "deadbeef".into(),
            carries_base: false,
            superseded: superseded.map(str::to_string),
            source: BaseSource::PullRequestTarget,
        }
    }

    /// Measuring a branch against a base it never recorded is only honest if the
    /// page says so — otherwise every number on it answers a question the reader
    /// did not ask.
    #[test]
    fn a_superseded_base_is_named_wherever_the_standing_is_reported() {
        let note = standing(Some("agent/CAIRN-1-planner-0")).superseded_note();
        assert!(
            note.contains("agent/CAIRN-1-planner-0")
                && note.contains("no longer exists")
                && note.contains("parent merged"),
            "the note names the vanished base and why it vanished: {note}"
        );
        assert!(
            note.contains("`main`") && note.contains("pull request targets"),
            "and the base the standing is actually measured against: {note}"
        );
        assert!(
            standing(None).superseded_note().is_empty(),
            "a branch whose recorded base is alive has nothing to explain"
        );
    }

    fn params(pairs: &[(&str, &str)]) -> Vec<QueryParam> {
        pairs
            .iter()
            .map(|(key, value)| QueryParam {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    /// An unknown key is a typo or a wrong mental model, and either way silently
    /// ignoring it serves a page the caller did not ask for.
    #[test]
    fn an_unknown_query_key_is_refused_by_name() {
        let typo = params(&[("vue", "base-ours")]);
        let error = parse_rebase_request(&typo).unwrap_err();
        assert!(error.contains("vue"), "{error}");
        assert!(error.contains("view"), "names what is accepted: {error}");
    }

    #[test]
    fn an_unknown_view_is_refused() {
        let bad = params(&[("view", "both")]);
        assert!(parse_rebase_request(&bad).is_err());
    }

    /// The views page independently. Scoping to a file only means something
    /// against a view, so asking for it on the summary is a mistake worth naming
    /// rather than quietly dropping.
    #[test]
    fn file_scoping_requires_a_view() {
        let bare_file = params(&[("file", "a.rs")]);
        assert!(parse_rebase_request(&bare_file).is_err());

        let scoped = params(&[("view", "base-theirs"), ("file", "a.rs")]);
        let request = parse_rebase_request(&scoped).unwrap();
        assert_eq!(request.view, RebaseView::BaseTheirs);
        assert_eq!(request.file, Some("a.rs"));
    }

    /// `merged` is the one view that means something at BOTH scopes: unscoped it
    /// is the three-way projection across the session, scoped it is the complete
    /// file to commit. So unlike the two side patches, `file=` is genuinely
    /// optional for it.
    #[test]
    fn the_merged_view_is_useful_with_or_without_a_file() {
        let bare = params(&[("view", "merged")]);
        let overview = parse_rebase_request(&bare).unwrap();
        assert_eq!(overview.view, RebaseView::Merged);
        assert_eq!(overview.file, None);

        let with_file = params(&[("view", "merged"), ("file", "src/a.rs")]);
        let scoped = parse_rebase_request(&with_file).unwrap();
        assert_eq!(scoped.view, RebaseView::Merged);
        assert_eq!(scoped.file, Some("src/a.rs"));
    }

    /// The merged view pages by the same server-owned window as the sides. It
    /// serves a COMPLETE file, so a caller that silently got a truncated one
    /// would commit a truncated file.
    #[test]
    fn the_merged_view_clamps_its_window_like_the_sides_do() {
        let huge = params(&[("view", "merged"), ("limit", "100000")]);
        assert_eq!(parse_rebase_request(&huge).unwrap().limit, MAX_PATCH_LINES);

        let unscoped = params(&[("view", "merged")]);
        let bare = parse_rebase_request(&unscoped).unwrap();
        assert_eq!(bare.limit, DEFAULT_PATCH_LINES);
        assert_eq!(bare.offset, 0);
    }

    fn owned(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|path| (*path).to_string()).collect()
    }

    /// The live case this section was written from: the incoming change claimed
    /// a migration number this branch had already used.
    #[test]
    fn two_files_claiming_one_number_in_one_directory_collide() {
        let occupied = BTreeMap::from([(
            "migrations".to_string(),
            owned(&["0147_a.sql", "0148_b.sql", "0149_theirs.sql"]),
        )]);
        let collisions = detect_number_collisions(
            &owned(&["migrations/0149_ours.sql"]),
            &owned(&["migrations/0149_theirs.sql"]),
            &occupied,
        );
        assert_eq!(collisions.len(), 1, "{collisions:?}");
        assert_eq!(
            collisions[0].suggested, "0150",
            "one past the highest taken"
        );
        assert_eq!(collisions[0].directory, "migrations");
    }

    /// Both sides adding the SAME file is a merge, not a collision — the whole
    /// point of a numbering convention is that one number names one thing.
    #[test]
    fn the_same_file_added_on_both_sides_is_not_a_collision() {
        let collisions = detect_number_collisions(
            &owned(&["migrations/0149_same.sql"]),
            &owned(&["migrations/0149_same.sql"]),
            &BTreeMap::new(),
        );
        assert!(collisions.is_empty(), "{collisions:?}");
    }

    /// Numbering is per-directory. Two directories that each number from 0001
    /// are not in conflict with each other.
    #[test]
    fn the_same_number_in_different_directories_does_not_collide() {
        let collisions = detect_number_collisions(
            &owned(&["a/0001_ours.sql"]),
            &owned(&["b/0001_theirs.sql"]),
            &BTreeMap::new(),
        );
        assert!(collisions.is_empty(), "{collisions:?}");
    }

    /// The suggestion is padded to the width the colliding name already uses, so
    /// it sorts beside its siblings instead of ahead of or behind them.
    #[test]
    fn the_suggested_number_keeps_the_padding_width_in_use() {
        let occupied = BTreeMap::from([("m".to_string(), owned(&["007_theirs.sql"]))]);
        let collisions = detect_number_collisions(
            &owned(&["m/007_ours.sql"]),
            &owned(&["m/007_theirs.sql"]),
            &occupied,
        );
        assert_eq!(collisions[0].suggested, "008");
    }

    /// The suggestion has to clear this branch's OWN additions too: the incoming
    /// directory listing has never seen them, so trusting it alone would suggest
    /// a number this branch is already using.
    #[test]
    fn the_suggestion_clears_this_branch_s_own_additions() {
        let occupied = BTreeMap::from([("m".to_string(), owned(&["0149_theirs.sql"]))]);
        let collisions = detect_number_collisions(
            &owned(&["m/0149_ours.sql", "m/0150_also_ours.sql"]),
            &owned(&["m/0149_theirs.sql"]),
            &occupied,
        );
        assert_eq!(
            collisions[0].suggested, "0151",
            "0150 is taken by this branch"
        );
    }

    /// An unnumbered file is not participating in a numbering convention.
    #[test]
    fn files_without_a_numeric_prefix_are_ignored() {
        assert_eq!(numeric_prefix("0148_a.sql"), Some((148, 4)));
        assert_eq!(numeric_prefix("schema.sql"), None);
        assert_eq!(numeric_prefix("0148.sql"), None, "needs the separator");
        assert!(detect_number_collisions(
            &owned(&["m/schema.sql"]),
            &owned(&["m/0149_theirs.sql"]),
            &BTreeMap::new()
        )
        .is_empty());
    }

    /// The overview shows the overlap, not the file. A long clean stretch
    /// between two regions is elided, and the regions themselves are intact.
    #[test]
    fn the_overview_excerpt_keeps_regions_and_elides_the_rest() {
        let merged = format!(
            "a\nb\nc\n{}\nmine\n{}\nbase\n{}\ntheirs\n{}\nx\ny\nz\n{}\n",
            "<".repeat(7),
            "|".repeat(7),
            "=".repeat(7),
            ">".repeat(7),
            (0..40)
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        );
        let excerpt = conflict_excerpt(&merged);
        assert!(excerpt.contains("mine") && excerpt.contains("theirs"));
        assert!(excerpt.contains("…"), "the clean tail is elided: {excerpt}");
        assert!(
            !excerpt.contains("\n39"),
            "a long clean stretch is not reproduced: {excerpt}"
        );
    }

    /// The cap is the server's, not the caller's. A request for a million lines
    /// is answered with the ceiling rather than honored.
    #[test]
    fn the_server_caps_the_page_size_whatever_is_asked_for() {
        let huge = params(&[("view", "base-ours"), ("limit", "100000")]);
        assert_eq!(parse_rebase_request(&huge).unwrap().limit, MAX_PATCH_LINES);

        let zero = params(&[("view", "base-ours"), ("limit", "0")]);
        assert_eq!(
            parse_rebase_request(&zero).unwrap().limit,
            1,
            "a zero-line page is not a page"
        );

        let bare = params(&[("view", "base-ours")]);
        let request = parse_rebase_request(&bare).unwrap();
        assert_eq!(request.limit, DEFAULT_PATCH_LINES);
        assert_eq!(request.offset, 0);
    }

    #[test]
    fn a_non_numeric_window_is_refused_rather_than_defaulted() {
        let bad = params(&[("view", "base-ours"), ("offset", "soon")]);
        assert!(parse_rebase_request(&bad).is_err());
    }
}
