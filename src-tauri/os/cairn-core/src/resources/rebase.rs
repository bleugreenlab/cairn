//! `cairn:~/rebase` — the active conflict resolution session for a node's branch.
//!
//! When a base advance cannot replay a branch, the new base does not land and the
//! branch remains on its pre-advance content. Replay may still rewrite the store
//! commit identity, so the resource always publishes current coordinates. The
//! preserved content is the safety property; the cost it used to
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
use crate::orchestrator::base_advance::{
    inflight_run_batch_on_branch, mutation_boundary_hold, InflightRunBatch,
};
use crate::orchestrator::conflict_session::{
    assess_session_tip, load_active_session, load_latest_session, load_reconcile_pass,
    ConflictSession, MarkerState, ReplayPass, ReplayState, SessionFile, TipAssessment, Unproven,
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
    // Read before anything else on the page: while the name is conflicted, no
    // replay can run, so everything below it describes a branch that cannot
    // move until this is settled.
    let name_section = conflicted_name_section(orch, &conn, &job.id, &branch).await;
    let session = match load_active_session(&db, &branch).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return format!(
                "{name_section}{}",
                render_closed_session(orch, &db, &conn, &job.id, &branch).await
            )
        }
        Err(error) => return error,
    };

    let rendered = match request.view {
        RebaseView::Summary => {
            let standing = load_live_standing(orch, &conn, &job.id, &branch, &session).await;
            let hold = inflight_run_batch_on_branch(&db, &job.id, &branch).await;
            render_summary(
                orch, project, number, exec_seq, node_id, &session, &standing, &hold,
            )
        }
        RebaseView::Merged => {
            render_merged(orch, project, number, exec_seq, node_id, &session, &request)
        }
        RebaseView::BaseOurs | RebaseView::BaseTheirs => {
            render_side(orch, project, number, exec_seq, node_id, &session, &request)
        }
    };
    if !matches!(request.view, RebaseView::Summary) {
        return rendered;
    }
    let rendered = format!("{name_section}{rendered}");
    let obligations =
        match crate::mcp::handlers::run_context::project_id_by_key(&orch.db.local, project).await {
            Ok(project_id) => {
                crate::orchestrator::contended_identifiers::outstanding_obligation_labels(
                    &orch.db.local,
                    &project_id,
                    &branch,
                )
                .await
                .unwrap_or_default()
            }
            Err(_) => Vec::new(),
        };
    if obligations.is_empty() {
        rendered
    } else {
        let rows = obligations
            .iter()
            .map(|identifier| format!("- `{identifier}`"))
            .collect::<Vec<_>>()
            .join("\n");
        let example = obligations.first().expect("non-empty");
        format!(
            "# Outstanding contended identifier obligation\n\nThis branch is held by the in-app merge gate until Cairn applies or automatically discharges the following durable recovery floor(s):\n\n{rows}\n\nIf an obligation itself is wrong, release it explicitly and say why; the actor and reason are retained:\n\n```\nwrite({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{action:\"discharge-contended-identifier\",identifier:\"{example}\",reason:\"…\"}}}}]}})\n```\n\n{rendered}"
        )
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
        return render_no_session(orch, db, conn, job_id, branch).await;
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
    // the same way: from the store, and from what the reconcile record says
    // happened to that advance.
    let standing = load_base_standing(orch, conn, job_id, branch).await;
    let now = match &standing {
        Ok(standing) if !standing.carries_base => format!(
            "\n⚠️ **Since then, the base has moved again.** `{}` is now at `{}`, which this \
             branch's ancestry does not contain, and no session opened for it. The outcome above \
             is history, not this branch's current standing.\n\n## Where the advance \
             stands\n\n{}\n{}\n{}\n",
            standing.base_branch,
            standing.destination,
            load_advance_state(db, job_id, branch, standing).await,
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
    /// The store the standing was read from, as reconcile intents record it.
    store_path: String,
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
    let repo_path = load_repo_path(conn, job_id).await?;
    let candidates = load_base_candidates(conn, job_id)
        .await
        .map_err(|error| format!("This node's base branch could not be read: {error}"))?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repo_path));
    let resolved =
        resolve_base(&jj, &store, branch, &candidates).map_err(|error| error.to_string())?;
    let carries_base = crate::jj::branch_carries_commit(&jj, &store, branch, &resolved.commit);
    Ok(BaseStanding {
        base_branch: resolved.branch,
        destination: resolved.commit,
        store_path: store.to_string_lossy().into_owned(),
        carries_base,
        superseded: resolved.superseded,
        source: resolved.source,
    })
}

/// What happened to the advance onto the base's current head, for a branch
/// that does not carry it and has no open session.
///
/// Both reads are made here so the renderer stays pure: the reconcile row for
/// this branch at that destination says whether a pass deferred it, attempted
/// it, or never reached it, and the live suspension rows say whether a run
/// batch on this branch — this node's or a child task's sharing it — holds
/// the mutation boundary now.
async fn load_advance_state(
    db: &crate::storage::LocalDb,
    job_id: &str,
    branch: &str,
    standing: &BaseStanding,
) -> String {
    let pass = load_reconcile_pass(db, branch, &standing.store_path, &standing.destination).await;
    let hold = inflight_run_batch_on_branch(db, job_id, branch).await;
    advance_state(&standing.destination, &pass, &hold)
}

/// The advance's state in words, from the reconcile record and the live hold.
///
/// "Deferred or never ran" was two incompatible readings offered as one, while
/// the record knew which: a deferral writes its row, names the batch that held
/// it, and stays queued for the sweep. So each state the record can be in is
/// said as itself, and one it cannot vouch for is said to be unknown.
fn advance_state(
    destination: &str,
    pass: &Result<Option<ReplayPass>, String>,
    hold: &Result<Option<InflightRunBatch>, String>,
) -> String {
    let held_now = match hold {
        Ok(Some(batch)) => format!(
            " A {} is in flight on this branch now, so an automatic pass that reaches the branch \
             before it ends will defer at the mutation boundary.",
            batch.describe()
        ),
        Ok(None) => String::new(),
        Err(error) => {
            format!(" Whether a run batch on this branch is in flight is unknown: {error}.")
        }
    };
    let pass = match pass {
        Err(error) => {
            return format!(
                "Whether any base-advance pass has reached this branch at `{destination}` is \
                 **unknown**: its reconcile record could not be read ({error}).{held_now}"
            )
        }
        Ok(None) => {
            return format!(
                "**Never ran.** No base-advance pass has reached this branch at `{destination}`: \
                 none deferred it and none attempted it.{held_now}"
            )
        }
        Ok(Some(pass)) => pass,
    };
    let at = crate::clock::stamp(pass.updated_at)
        .map(|stamp| format!(" at {stamp}"))
        .unwrap_or_default();
    let quoted = pass
        .diagnostic
        .as_deref()
        .map(|text| format!("\n\n> {}\n\n", text.trim().replace('\n', "\n> ")))
        .unwrap_or_else(|| " ".to_string());
    let intent = pass.intent_status.as_deref();
    let ref_lock = pass
        .failure_kind
        .as_deref()
        .is_some_and(crate::jj::reconcile_failure_is_ref_lock);
    match (intent, pass.outcome_kind.as_deref()) {
        (Some("running"), _) => format!(
            "**Running.** A base-advance pass onto `{destination}` is running now, under the store \
             lock.{held_now}"
        ),
        (Some("pending"), _) if ref_lock => format!(
            "**Retrying.** The last pass onto `{destination}`{at} could not take the ref store's \
             lock. That is the machine's shared Git ref store, not your branch; the reconcile \
             worker retries it automatically.{quoted}"
        ),
        (Some("pending"), Some("deferred")) => format!(
            "**Deferred.** A base-advance pass onto `{destination}` reached this branch{at} and \
             stopped at the mutation boundary without attempting the rebase. It recorded:\
             {quoted}{}",
            mutation_boundary_hold(hold)
        ),
        (_, Some("failed")) => format!(
            "**Attempted and failed** ({}){at}. The diagnostic says what the pass changed.\
             {quoted}A transient failure is retried automatically; one that needs you also \
             arrives as a message.",
            pass.failure_kind.as_deref().unwrap_or("unknown")
        ),
        (Some("pending"), None) => format!(
            "**Queued.** A pass onto `{destination}` is queued for the durable reconcile worker \
             and has not reached this branch yet.{held_now}"
        ),
        (intent, outcome) => format!(
            "**Unknown.** The last pass onto `{destination}` recorded `{}`{at}, and its reconcile \
             work is {}, yet this branch's ancestry does not carry `{destination}`. The record \
             does not explain that, so this page will not guess.{held_now}",
            outcome.unwrap_or("nothing"),
            intent.unwrap_or("missing"),
        ),
    }
}

/// The page's section for a branch whose name the store holds conflicted, or
/// nothing when it is not. Reads only: the page never settles the name, it
/// says whether the next replay or branch read will, and when not, why and
/// with which commands a maintainer can.
async fn conflicted_name_section(
    orch: &Orchestrator,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    branch: &str,
) -> String {
    let Ok(repo_path) = load_repo_path(conn, job_id).await else {
        return String::new();
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repo_path));
    if !crate::jj::is_jj_dir(&store) {
        return String::new();
    }
    // The base names the column that says which version sits on the newer
    // base; without it the table still renders, just without that column.
    let base_branch = match load_base_candidates(conn, job_id).await {
        Ok(candidates) => match resolve_base(&jj, &store, branch, &candidates) {
            Ok(resolved) => Some(resolved.branch),
            Err(_) => candidates.recorded.or(candidates.project_default),
        },
        Err(_) => None,
    };
    match crate::jj::assess_conflicted_bookmark_name(&jj, &store, branch, base_branch.as_deref()) {
        Ok(Some(assessment)) => render_conflicted_name(&assessment),
        Ok(None) => String::new(),
        Err(error) => {
            log::warn!(
                "rebase page: could not read whether `{branch}`'s name is conflicted: {error}"
            );
            String::new()
        }
    }
}

fn render_conflicted_name(assessment: &crate::jj::ConflictedNameAssessment) -> String {
    let crate::jj::ConflictedNameAssessment {
        conflict,
        bases,
        outlook,
    } = assessment;
    let cell = |text: &str| text.replace('|', "\\|");
    let mut out = format!(
        "# Branch name is conflicted\n\nThe store records {} versions of `{}`, so no read, write, \
         or replay can use the name until they are combined. Nothing has been discarded: every \
         version is still readable by its commit id.\n\n| | commit | change | base | description \
         |\n|---|---|---|---|---|\n",
        conflict.added.len(),
        conflict.branch,
    );
    for target in &conflict.removed {
        out.push_str(&format!(
            "| moved away from | `{}` | `{}` | | {} |\n",
            target.commit,
            target.change,
            cell(&target.description)
        ));
    }
    for (index, target) in conflict.added.iter().enumerate() {
        let base = match bases.get(index) {
            Some(Some(base)) => format!(
                "`{}`{}{}",
                base.commit,
                base.pull_request
                    .map(|pr| format!(" (#{pr})"))
                    .unwrap_or_default(),
                if base.newest { ", newest" } else { "" }
            ),
            Some(None) => "unknown".to_string(),
            None => String::new(),
        };
        out.push_str(&format!(
            "| version | `{}` | `{}` | {base} | {} |\n",
            target.commit,
            target.change,
            cell(&target.description)
        ));
    }
    match outlook {
        crate::jj::ConflictedNameOutlook::Settleable { how, commands } => {
            out.push_str(&format!(
                "\n**Expected to settle automatically:** the next replay or branch read will try \
                 to {how}. That has not been attempted yet."
            ));
            if commands.is_empty() {
                out.push_str(" Nothing needs doing by hand.\n\n");
            } else {
                out.push_str(&format!(
                    " If that replay conflicts it is rolled back and refused, and this page will \
                     say so; a maintainer can then combine the versions with:\n\n```sh\n{}\n```\n\n",
                    commands.join("\n")
                ));
            }
        }
        crate::jj::ConflictedNameOutlook::Refused(refusal) => out.push_str(&format!(
            "\n**Not settleable automatically:** {}. Choosing one version would silently drop \
             the other's commits, so the store will not. A maintainer can combine them with one \
             of these orderings, run against the store:\n\n```sh\n{}\n```\n\n",
            refusal.reason,
            refusal.commands.join("\n")
        )),
    }
    out
}

async fn load_repo_path(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<String, String> {
    let mut rows = conn
        .query(
            "SELECT p.repo_path
             FROM jobs j JOIN projects p ON j.project_id = p.id
             WHERE j.id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
        .map_err(|error| format!("This node's project row could not be read: {error}"))?;
    rows.next()
        .await
        .map_err(|error| format!("This node's project row could not be read: {error}"))?
        .and_then(|row| row.opt_text(0).ok().flatten())
        .filter(|repo_path| !repo_path.is_empty())
        .ok_or_else(|| {
            "Cairn holds no repository path for this node's project, so the branch store could \
             not be asked anything about it."
                .to_string()
        })
}

/// Where a branch with an open session stands NOW, read live from the store.
///
/// The session's coordinates are frozen at the advance that recorded it. The
/// node diff measures from the live fork point, and the base may have moved
/// again since; without these three live facts beside the frozen ones, the page
/// and the diff disagree with nothing saying which is stale.
struct LiveStanding {
    base_branch: String,
    /// The base's head now.
    head: String,
    /// The branch tip's merge-base with that head: the base the node diff uses.
    fork_point: Option<String>,
    /// Whether the session's `ours` is still in the branch's ancestry.
    ours_in_ancestry: Option<bool>,
    /// Each distinct incoming side the tip's resolved files were resolved
    /// against, with the files resolved against it.
    resolved_against: BTreeMap<String, Vec<String>>,
}

async fn load_live_standing(
    orch: &Orchestrator,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    branch: &str,
    session: &ConflictSession,
) -> Result<LiveStanding, String> {
    let base = load_base_standing(orch, conn, job_id, branch).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    let tip = crate::jj::bookmark_commit(&jj, &store, branch);
    let fork_point = tip.as_deref().and_then(|tip| {
        crate::jj::revset_commit(
            &jj,
            &store,
            &format!("heads(::{tip} & ::{})", base.destination),
        )
    });
    let ours_in_ancestry = session
        .ours
        .as_deref()
        .map(|ours| crate::jj::branch_carries_commit(&jj, &store, branch, ours));
    let basis = tip
        .as_deref()
        .and_then(|tip| session.resolution_basis_at(&jj, &store, tip).ok())
        .unwrap_or_else(|| session.resolution_basis.clone());
    let mut resolved_against: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (path, side) in basis {
        resolved_against.entry(side.theirs).or_default().push(path);
    }
    Ok(LiveStanding {
        base_branch: base.base_branch,
        head: base.destination,
        fork_point,
        ours_in_ancestry,
        resolved_against,
    })
}

/// The page's statement of what is stale: the session's recorded destination
/// beside the base's live head and the branch's live fork point, a branch
/// rewritten since recording, and what the committed tip was resolved against.
fn standing_section(session: &ConflictSession, standing: &Result<LiveStanding, String>) -> String {
    let standing = match standing {
        Ok(standing) => standing,
        Err(why) => {
            return format!(
                "\n## Where the branch stands now\n\nThe base's live head and this branch's fork \
                 point could not be read ({why}), so the coordinates below may describe an older \
                 merge than a replay would perform.\n"
            )
        }
    };
    let recorded = session.destination_commit.as_str();
    let fork = standing.fork_point.as_deref().unwrap_or("unavailable");
    let mut out = format!(
        "\n## Where the branch stands now\n\n| | commit |\n|---|---|\n| this session was recorded \
         against | `{recorded}` |\n| `{base}` now | `{head}` |\n| your branch's fork point with \
         `{base}` (the node diff's base) | `{fork}` |\n",
        base = standing.base_branch,
        head = standing.head,
    );
    if recorded != standing.head {
        out.push_str(&format!(
            "\n⚠️ This session was recorded against `{recorded}`; `{base}` is now `{head}`; your \
             branch's fork point is `{fork}`. The three-way coordinates below describe the \
             recorded merge. A replay lands on `{head}`, not `{recorded}`.\n",
            base = standing.base_branch,
            head = standing.head,
        ));
    }
    if standing.ours_in_ancestry == Some(false) {
        out.push_str(
            "\n⚠️ The branch was rewritten since this session was recorded: the recorded `ours` is \
             no longer in its ancestry, so the coordinates below name commits the branch no \
             longer carries.\n",
        );
    }
    if !standing.resolved_against.is_empty() {
        out.push_str("\nYour committed tip's resolutions:\n\n");
        for (theirs, paths) in &standing.resolved_against {
            let paths: Vec<String> = paths.iter().map(|path| format!("`{path}`")).collect();
            out.push_str(&format!(
                "- resolved against `{theirs}`: {}\n",
                paths.join(", ")
            ));
        }
        out.push_str(&format!(
            "\nA take-committed-tip replay carries each forward onto `{head}`: a file the base has \
             not touched since lands as you committed it, a file it changed elsewhere has those \
             changes merged in, and a file where it changed the region you resolved is refused \
             and shown here again.\n",
            head = standing.head,
        ));
    }
    out
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
    db: &crate::storage::LocalDb,
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
    let advance = if standing.carries_base {
        String::new()
    } else {
        load_advance_state(db, job_id, branch, &standing).await
    };
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
         ATTEMPTED and hit a conflict; no session means no attempt reached that point.\n\n## \
         Where the advance stands\n\n{advance}\n\nNothing about it is visible from your \
         checkout: local checks stay green, `git diff {base_branch}...HEAD` stays correct, and \
         only the pull request shows it, as `Mergeable: CONFLICTING`.\n\nEditing files cannot \
         fix it. Your branch's ANCESTRY is what \
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

#[allow(clippy::too_many_arguments)]
fn render_summary(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
    standing: &Result<LiveStanding, String>,
    hold: &Result<Option<InflightRunBatch>, String>,
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

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);

    let (condition, replay_payload) = if session.is_base_drift() {
        (
            "**Condition: base drift.** Every conflicting file is already byte-identical between your \
             branch and the new base, so there is nothing to merge — what is stale is your branch's \
             ANCESTRY. Editing files will not clear this, and you cannot repair it from your checkout: \
             slot refs are downstream exports of the runner's private jj store. Request the replay \
             below."
                .to_string(),
            "action:\"replay\"",
        )
    } else if session.is_unparseable() {
        (
            format!(
                "**Condition: unparseable merge.** jj merged the two sides without a recorded \
                 conflict, and the result does not parse, so the replay was rolled back.{} The \
                 merged view below IS that unparseable text: do not commit it as is. Read both \
                 sides of each file, write it so it holds both sides' changes and parses (after two \
                 appends to one list, usually: one closing bracket, every entry inside it), commit \
                 it on your branch, then request the replay below.",
                unparseable_failures(&jj, &store, session)
            ),
            "action:\"replay\",resolution:\"take-committed-tip\"",
        )
    } else {
        (
            "**Condition: content conflict.** The two sides genuinely disagree. Read both versions of \
             each conflicting file, write the merged result with ordinary edits, and commit it on your \
             branch. Then request the replay below so the branch actually moves onto the base — \
             resolving the content does not by itself change your branch's ancestry."
                .to_string(),
            "action:\"replay\",resolution:\"take-committed-tip\"",
        )
    };
    let merged_warning = if session.is_unparseable() {
        " **For this session the merged file is the defect: it does not parse. Do not commit it \
         as is.**"
    } else {
        ""
    };

    // One store probe answers three questions: has the agent committed anything
    // since, does that commit already contain both sides, and would the
    // whole-file restore drop anything. Computed once so the sections below can
    // never disagree with each other or with the guard on the replay itself.
    let assessment = assess_session_tip(&jj, &store, session);

    format!(
        "# Rebase session for `{branch}`\n{version_note}\n{incoming}, and the automatic replay of your branch \
         onto it recorded a conflict and did not land the new base. Your branch remains on its \
         pre-advance content, but replay machinery may rewrite its store commit identity; the \
         coordinates below are current.\n\n{condition}\n{status}\n{markers}\n{standing}\n## Three-way \
         coordinates\n\n{coords}\nThese are immutable commits. Read any file as of either side \
         with `?branch=<commit>`, or read the merge:\n\n- `{base}?view=merged` — the two sides \
         merged line-wise, with everything non-overlapping already resolved. Add `file=PATH` for \
         the complete merged file to commit.{merged_warning}\n- `{base}?view=base-ours` — what your branch \
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
        status = status_line(session, hold),
        standing = standing_section(session, standing),
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
        merged_warning = merged_warning,
    )
}

/// The parser's first error in each conflicting file's line merge, recomputed
/// from the session's three coordinates: a session stores its condition, not
/// the failures that set it.
fn unparseable_failures(
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
    let paths: Vec<String> = session
        .conflicting()
        .map(|file| file.path.clone())
        .collect();
    let failures: Vec<cairn_vcs::parse_gate::ParseFailure> =
        crate::jj::read_sides_for_paths(jj, store, base, ours, theirs, &paths)
            .into_iter()
            .filter_map(|(path, sides)| {
                let sides = sides.ok()?;
                let merged = match crate::jj::merge_preview(
                    sides.base.mergeable()?,
                    sides.ours.mergeable()?,
                    sides.theirs.mergeable()?,
                ) {
                    crate::jj::MergePreview::Clean(text) => text,
                    crate::jj::MergePreview::Conflicted { .. } => return None,
                };
                cairn_vcs::parse_gate::parse_failure(&path, &merged)
            })
            .collect();
    if failures.is_empty() {
        return " The parser's errors could not be recomputed from the coordinates below; read \
                the merged view of each conflicting file."
            .to_string();
    }
    format!(
        " The parser's first error in each file's line merge:\n\n{}\n",
        crate::jj::render_failures(&failures)
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

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    // The LIVE tip, not the session's frozen `ours`: a resolution the agent has
    // already committed is exactly what this view needs to account for.
    let ours_rev = crate::jj::bookmark_commit(&jj, &store, &session.bookmark)
        .unwrap_or_else(|| session.ours.clone().unwrap_or_default());
    let tip_moved = session.ours.as_deref() != Some(ours_rev.as_str());

    let view = match request.file {
        Some(file) => render_merged_file(
            &jj, &store, base_rev, &ours_rev, theirs_rev, file, tip_moved, request, &uri,
        ),
        None => render_merged_overview(
            &jj, &store, base_rev, &ours_rev, theirs_rev, session, tip_moved, &uri,
        ),
    };
    if session.is_unparseable() {
        // The line merge of an unparseable session reports no conflict, so
        // nothing below looks wrong; it is the very text that was refused.
        format!(
            "> ⚠️ This session is an **unparseable merge**: the line merge below reports no \
             conflict and does not parse. It is the defect, not a resolution: do not commit it \
             as is. `{uri}` names the parser's first error in each file.\n\n{view}"
        )
    } else {
        view
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
fn status_line(
    session: &ConflictSession,
    hold: &Result<Option<InflightRunBatch>, String>,
) -> String {
    let Some(progress) = session.replay_progress() else {
        return String::new();
    };
    let requested = crate::clock::stamp(progress.requested_at)
        .map(|stamp| format!(", requested {stamp}"))
        .unwrap_or_default();
    let destination = progress.destination.as_str();
    let when = |at: i64| {
        crate::clock::stamp(at)
            .map(|stamp| format!(" at {stamp}"))
            .unwrap_or_default()
    };
    match progress.state {
        ReplayState::Retrying { diagnosis, at } => format!(
            "\n**Status:** replay onto `{destination}`{requested} is RETRYING. Its last \
             attempt{} could not take the ref store's lock: {diagnosis}. That is the machine's \
             shared Git ref store, not your branch — nothing needs repair, and the reconcile \
             worker retries it automatically. Re-requesting does not make it sooner.\n",
            when(at)
        ),
        // A failure says nothing about where the branch now stands: a clean
        // rebase may have landed before its publication failed. Only the
        // diagnostic does, so it is quoted rather than summarized.
        ReplayState::Failed {
            kind,
            diagnostic,
            at,
        } => {
            let quoted = diagnostic
                .as_deref()
                .map(|text| format!("\n\n```\n{}\n```\n", text.trim()))
                .unwrap_or_default();
            if kind == crate::jj::ROLLBACK_FAILED {
                format!(
                    "\n**Status:** replay onto `{destination}`{requested} FAILED{} and its \
                     rollback did NOT complete, so the store may not be where the pass found it. \
                     Do not publish or force-push this branch; report the diagnostic below.{quoted}",
                    when(at)
                )
            } else {
                format!(
                    "\n**Status:** replay onto `{destination}`{requested} was attempted{} and \
                     FAILED ({kind}); the diagnostic below says what the pass changed.{quoted} A \
                     transient failure is retried automatically; one that needs you also arrives \
                     as a message.\n",
                    when(at)
                )
            }
        }
        ReplayState::Deferred { diagnostic, at } => {
            let recorded = diagnostic
                .as_deref()
                .map(|text| format!(" It recorded:\n\n> {}\n\n", text.trim()))
                .unwrap_or_else(|| " ".to_string());
            format!(
                "\n**Status:** replay onto `{destination}`{requested} is DEFERRED. Its last pass{} \
                 stopped at the mutation boundary and attempted nothing.{recorded}{}\n",
                when(at),
                mutation_boundary_hold(hold)
            )
        }
        ReplayState::Processed { outcome, at } => format!(
            "\n**Status:** replay onto `{destination}`{requested} was processed: {} {}. If this \
             page has not caught up with that pass, re-read it.\n",
            outcome.replace('_', " "),
            when(at).trim_start()
        ),
        ReplayState::NotProcessed { intent_status } => format!(
            "\n**Status:** a replay was requested onto `{destination}`{requested}, but no pass \
             has processed it and none is queued to (its reconcile work is {intent_status}). \
             Request the replay again.\n"
        ),
        ReplayState::Running => format!(
            "\n**Status:** replay IN PROGRESS onto `{destination}`{requested}. It runs under the \
             store lock; nothing runs in your slot.\n"
        ),
        ReplayState::Queued => format!(
            "\n**Status:** replay QUEUED onto `{destination}`{requested}. The durable reconcile \
             worker picks it up; nothing runs in your slot, and re-requesting does not make it \
             sooner.\n"
        ),
        ReplayState::Superseded => format!(
            "\n**Status:** a replay was requested onto `{destination}`{requested}, but the \
             reconcile work this session was recorded under has since been SUPERSEDED by a newer \
             base, so this line no longer tracks that request. Its outcome is either a fresh \
             session on this page or a published branch; if neither has appeared, request the \
             replay again.\n"
        ),
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

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
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
            store_path: "/store".into(),
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
        let note = standing(Some("agent/cairn-1-planner-0")).superseded_note();
        assert!(
            note.contains("agent/cairn-1-planner-0")
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

    fn live(head: &str, ours_in_ancestry: bool) -> Result<LiveStanding, String> {
        Ok(LiveStanding {
            base_branch: "main".to_string(),
            head: head.to_string(),
            fork_point: Some("fork0000".to_string()),
            ours_in_ancestry: Some(ours_in_ancestry),
            resolved_against: [("a337fa4d".to_string(), vec!["migrations.rs".to_string()])]
                .into_iter()
                .collect(),
        })
    }

    fn recorded_session() -> ConflictSession {
        ConflictSession {
            intent_id: "intent".to_string(),
            bookmark: "agent/test".to_string(),
            project_id: "project".to_string(),
            store_path: "/store".to_string(),
            target_branch: "main".to_string(),
            destination_commit: "a337fa4d".to_string(),
            diagnostic_version: crate::orchestrator::conflict_session::CONFLICT_DIAGNOSTIC_VERSION,
            condition: "content_conflict".to_string(),
            base: Some("fork0000".to_string()),
            ours: Some("c9bcf33c".to_string()),
            theirs: Some("a337fa4d".to_string()),
            conflicted_tip: None,
            incoming: Default::default(),
            marker_state: MarkerState::NotMaterialized,
            marker_diagnostic: None,
            resolution_state: Some("open".to_string()),
            updated_at: 0,
            replay_requested_at: None,
            item_status: None,
            intent_status: None,
            files: Vec::new(),
            resolution_basis: Default::default(),
            replay_requested_tip: None,
            replay_requested_destination: None,
            replay_pass: None,
        }
    }

    /// The specimen's page: recorded against a337fa4d while main is at
    /// b5461a3b. It names both, and the fork point the node diff uses, instead
    /// of reading as a current account.
    #[test]
    fn a_session_recorded_against_an_older_base_says_so_beside_the_live_coordinates() {
        let section = standing_section(&recorded_session(), &live("b5461a3b", true));
        assert!(
            section.contains("`a337fa4d`") && section.contains("`b5461a3b`"),
            "{section}"
        );
        assert!(section.contains("fork point is `fork0000`"), "{section}");
        assert!(
            section.contains("A replay lands on `b5461a3b`"),
            "{section}"
        );
        // The tip moved off `ours`, so it was resolved against this session's
        // own incoming side.
        assert!(
            section.contains("resolved against `a337fa4d`: `migrations.rs`")
                && section.contains("carries each forward onto `b5461a3b`"),
            "{section}"
        );
        assert!(!section.contains("rewritten"), "{section}");

        let current = standing_section(&recorded_session(), &live("a337fa4d", false));
        assert!(
            !current.contains("A replay lands on"),
            "nothing stale: {current}"
        );
        assert!(
            current.contains("rewritten since this session was recorded"),
            "{current}"
        );
    }

    /// A replay whose last pass lost the ref-store lock says so, names the
    /// lock's state, and tells the agent there is nothing to repair; one no
    /// pass will process says to ask again instead of claiming to be queued.
    #[test]
    fn the_status_line_reports_the_last_pass_instead_of_queued() {
        let mut session = recorded_session();
        session.replay_requested_at = Some(100);
        session.replay_pass = Some(crate::orchestrator::conflict_session::ReplayPass {
            item_status: Some("pending".to_string()),
            intent_status: Some("pending".to_string()),
            outcome_kind: Some("failed".to_string()),
            failure_kind: Some(crate::jj::REF_LOCK_CONTENTION.to_string()),
            diagnostic: Some(
                "jj rebase failed: The lock for the packed-ref file could not be obtained\n\
                 packed-refs lock: contention — held by live process 77 (git) after 30s"
                    .to_string(),
            ),
            updated_at: 130,
        });
        let line = status_line(&session, &Ok(None));
        assert!(line.contains("RETRYING"), "{line}");
        assert!(
            line.contains("held by live process 77 (git)"),
            "names the lock's state: {line}"
        );
        assert!(line.contains("nothing needs repair"), "{line}");
        assert!(!line.contains("QUEUED"), "{line}");

        session.replay_pass = Some(crate::orchestrator::conflict_session::ReplayPass {
            intent_status: Some("completed".to_string()),
            updated_at: 50,
            ..Default::default()
        });
        let line = status_line(&session, &Ok(None));
        assert!(line.contains("no pass has processed it"), "{line}");
        assert!(line.contains("Request the replay again"), "{line}");
        assert!(!line.contains("QUEUED"), "{line}");
    }

    /// A failure never promises an unchanged branch: a publication can fail
    /// after its rebase landed. And a failure whose rollback did not complete
    /// — even one caused by a lost lock — is a warning, never a quiet retry.
    #[test]
    fn a_failed_pass_claims_nothing_about_the_branch_and_an_incomplete_rollback_warns() {
        let failed = |kind: &str, diagnostic: &str| {
            let mut session = recorded_session();
            session.replay_requested_at = Some(100);
            session.replay_pass = Some(crate::orchestrator::conflict_session::ReplayPass {
                item_status: Some("pending".to_string()),
                intent_status: Some("pending".to_string()),
                outcome_kind: Some("failed".to_string()),
                failure_kind: Some(kind.to_string()),
                diagnostic: Some(diagnostic.to_string()),
                updated_at: 130,
            });
            status_line(&session, &Ok(None))
        };
        let push = failed(
            "transient_process",
            "origin push failed: connection reset. GitHub still holds the previous branch head",
        );
        assert!(push.contains("FAILED (transient_process)"), "{push}");
        assert!(
            !push.contains("unchanged"),
            "a published rebase may have landed: {push}"
        );
        assert!(
            push.contains("origin push failed"),
            "the diagnostic is quoted: {push}"
        );

        let unsafe_rollback = failed(
            crate::jj::ROLLBACK_FAILED,
            "jj rebase moved the bookmark, and exporting it failed: The lock for the packed-ref \
             file could not be obtained\nrollback incomplete: rolling the rebase back also failed",
        );
        assert!(
            unsafe_rollback.contains("rollback did NOT complete"),
            "{unsafe_rollback}"
        );
        assert!(
            unsafe_rollback.contains("Do not publish"),
            "{unsafe_rollback}"
        );
        assert!(!unsafe_rollback.contains("RETRYING"), "{unsafe_rollback}");
        assert!(
            !unsafe_rollback.contains("nothing needs repair"),
            "{unsafe_rollback}"
        );
    }

    fn deferred_pass() -> crate::orchestrator::conflict_session::ReplayPass {
        crate::orchestrator::conflict_session::ReplayPass {
            item_status: Some("pending".to_string()),
            intent_status: Some("pending".to_string()),
            outcome_kind: Some("deferred".to_string()),
            failure_kind: None,
            diagnostic: Some(
                "deferred at the mutation boundary: run batch `ba0e0085` was in flight on this \
                 branch, so its rebase was not attempted."
                    .to_string(),
            ),
            updated_at: 130,
        }
    }

    fn held() -> Result<Option<InflightRunBatch>, String> {
        Ok(Some(InflightRunBatch {
            job_id: "job-builder".to_string(),
            holder: Some("cairn://p/cairn/5069/1/builder".to_string()),
            request_id: "ba0e0085".to_string(),
            label: Some("rust-tests on main".to_string()),
            parked_at_ms: 130_000,
        }))
    }

    /// The 5069 specimen: main moved, no session opened, and the page said
    /// "deferred or never ran" while the record knew it was deferred by the
    /// builder's own run batch. Each recorded state is now said as itself.
    #[test]
    fn an_advance_owed_by_a_branch_with_no_session_is_stated_from_the_record() {
        let deferred = advance_state("279ddbcf", &Ok(Some(deferred_pass())), &held());
        assert!(deferred.contains("**Deferred.**"), "{deferred}");
        assert!(
            deferred.contains("run batch `ba0e0085` (\"rust-tests on main\")")
                && deferred.contains("on cairn://p/cairn/5069/1/builder")
                && deferred.contains("finishes or is stopped")
                && deferred.contains("durable sweep"),
            "names the holding batch and what ends the hold: {deferred}"
        );
        assert!(!deferred.contains("never ran"), "{deferred}");

        let released = advance_state("279ddbcf", &Ok(Some(deferred_pass())), &Ok(None));
        assert!(
            released.contains("**Deferred.**")
                && released.contains("nothing holds the boundary any more"),
            "a hold whose batch has ended says the sweep takes it from here: {released}"
        );

        let never = advance_state("279ddbcf", &Ok(None), &Ok(None));
        assert!(never.contains("**Never ran.**"), "{never}");
        assert!(!never.contains("Deferred"), "{never}");

        let unread = advance_state("279ddbcf", &Err("no such table".to_string()), &held());
        assert!(
            unread.contains("**unknown**") && unread.contains("no such table"),
            "{unread}"
        );
        assert!(
            unread.contains("`ba0e0085`"),
            "the live hold is still reported: {unread}"
        );

        let mut contradictory = deferred_pass();
        contradictory.intent_status = Some("completed".to_string());
        contradictory.outcome_kind = Some("rebased_clean".to_string());
        let contradictory = advance_state("279ddbcf", &Ok(Some(contradictory)), &Ok(None));
        assert!(contradictory.contains("**Unknown.**"), "{contradictory}");
    }

    /// A replay request whose pass deferred reads DEFERRED with the batch, not
    /// QUEUED, which read as imminent while the branch could not move.
    #[test]
    fn a_deferred_replay_request_names_the_batch_instead_of_reading_queued() {
        let mut session = recorded_session();
        session.replay_requested_at = Some(100);
        session.replay_pass = Some(deferred_pass());
        let line = status_line(&session, &held());
        assert!(line.contains("DEFERRED"), "{line}");
        assert!(!line.contains("QUEUED"), "{line}");
        assert!(
            line.contains("run batch `ba0e0085`") && line.contains("finishes or is stopped"),
            "{line}"
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
    fn conflicted(added: &[(&str, &str, &str)]) -> crate::jj::ConflictedBookmark {
        let target =
            |(commit, change, description): &(&str, &str, &str)| crate::jj::BookmarkTarget {
                commit: commit.to_string(),
                change: change.to_string(),
                description: description.to_string(),
            };
        crate::jj::ConflictedBookmark {
            branch: "agent/cairn-4966-builder-0".into(),
            removed: vec![target(&("5dc58aff", "uvxqtrvw", "flatten"))],
            added: added.iter().map(target).collect(),
        }
    }

    #[test]
    fn a_settleable_conflicted_name_says_the_next_replay_settles_it() {
        let conflict = conflicted(&[
            ("c4e36dd2", "uvxqtrvw", "flatten"),
            ("f1c676bc", "sxznpoqz", "docs | protocol"),
        ]);
        let page = render_conflicted_name(&crate::jj::ConflictedNameAssessment {
            conflict,
            bases: vec![
                Some(crate::jj::VersionBase {
                    commit: "ad06afa2".into(),
                    description: "Newer thing (#4167)".into(),
                    pull_request: Some(4167),
                    newest: true,
                }),
                None,
            ],
            outlook: crate::jj::ConflictedNameOutlook::Settleable {
                how: "replay 1 commit(s) of `f1c676bc` onto `c4e36dd2`".into(),
                commands: vec!["jj -R '/store' --ignore-working-copy rebase -r '5dc58aff..f1c676bc' -d c4e36dd2".into()],
            },
        });

        assert!(page.starts_with("# Branch name is conflicted"), "{page}");
        for line in [
            "| moved away from | `5dc58aff` | `uvxqtrvw` | | flatten |",
            "| version | `c4e36dd2` | `uvxqtrvw` | `ad06afa2` (#4167), newest | flatten |",
            "| version | `f1c676bc` | `sxznpoqz` | unknown | docs \\| protocol |",
        ] {
            assert!(page.contains(line), "{page}");
        }
        // A replay not yet attempted is never promised.
        assert!(
            page.contains(
                "**Expected to settle automatically:** the next replay or branch read will try \
                 to replay 1 commit(s)"
            ),
            "{page}"
        );
        assert!(page.contains("has not been attempted yet"), "{page}");
        assert!(!page.contains("Nothing needs doing"), "{page}");
        assert!(
            page.contains(
                "```sh\njj -R '/store' --ignore-working-copy rebase -r '5dc58aff..f1c676bc' -d c4e36dd2\n```"
            ),
            "{page}"
        );
    }

    #[test]
    fn a_refused_conflicted_name_shows_the_reason_and_the_commands() {
        let conflict = conflicted(&[("aaaa1111", "kkkk", "one"), ("bbbb2222", "llll", "two")]);
        let refusal = crate::jj::RepairRefusal {
            branch: conflict.branch.clone(),
            conflict: conflict.clone(),
            reason: "neither version is a rewrite of the commit both moved away from".into(),
            commands: vec![
                "# Keep aaaa1111 as the base and replay bbbb2222's own commits onto it (tried in memory: replays cleanly)".into(),
                "jj -R '/store' --ignore-working-copy rebase -r '(aaaa1111..bbbb2222) ~ ::(bookmarks(~exact:\"agent/cairn-4966-builder-0\") | remote_bookmarks(~exact:\"agent/cairn-4966-builder-0\"))' -d aaaa1111".into(),
            ],
        };
        let page = render_conflicted_name(&crate::jj::ConflictedNameAssessment {
            conflict,
            bases: Vec::new(),
            outlook: crate::jj::ConflictedNameOutlook::Refused(Box::new(refusal.clone())),
        });

        assert!(
            page.contains("**Not settleable automatically:** neither version is a rewrite"),
            "{page}"
        );
        assert!(
            page.contains(&format!("```sh\n{}\n```", refusal.commands.join("\n"))),
            "{page}"
        );
    }

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
