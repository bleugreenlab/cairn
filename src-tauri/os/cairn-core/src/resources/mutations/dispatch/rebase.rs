//! The store-side actions on `cairn:~/rebase`.
//!
//! An agent's slot is a plain git worktree whose refs are downstream exports of
//! the runner's private jj store, so nothing done there can move a branch's
//! ancestry. This is the sanctioned request: it enqueues durable reconcile work
//! and returns. No jj runs in the agent's slot, and there is deliberately no
//! git/jj escape hatch beside it — the shared store stays the single authority on
//! ancestry.
//!
//! `dismiss-acceptance` is the one repair for an `unknown` row of the
//! acceptance journal (`orchestrator/base_advance/preparation.rs`), which
//! otherwise refuses every writer of the branch forever. It is the owning
//! node's to send, names the row, the branch's literal head and a reason, and
//! retires the row without accepting anything: the journal's own helper does
//! every check under the branch's reservation and the store lock, and nothing
//! moves, exports, publishes or wakes.
//!
//! Two more actions concern the branch's catch-up mode (see
//! `orchestrator::catch_up`). `catch-up-mode` sets or clears it, and is the
//! operator's to approve: [`rebase_authority`] names the authority ahead of any
//! side effect, the write handler raises the operator's card, and the dispatch
//! re-derives the same request and consumes the grant just before it acts.
//! `mark-reviewed` moves the owner's review mark forward and is direct: the
//! operator decides which paths need review, the owner says what it has read.
//!
//! `rebuild` recovers a flagged branch onto its current base from coordinates
//! its owner attests: the last clean source commit, the original cut and why
//! it is one, and the flagged tip observed. It refuses while a run batch can
//! still publish to the branch, and while the branch is held behind its
//! parent's open conflict session. That hold is read from the parent's live
//! session through `preparation::held_by_parent_conflict`, the one source of
//! the rule; rebuild keeps no hold of its own. Accepting a rebuild retires only
//! the obligations the rebuilt head discharges (`acceptance`).

use std::path::Path;

use super::super::{
    build_failure, payload_non_empty_str, target_resource_for_request, ResourceMutationResult,
};
use crate::authorization::prepare::Prepared;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::base_advance::preparation::{AcceptanceDebt, AcceptanceDismissal};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityDecision;
use cairn_uri::CairnResource;

/// What a `cairn:~/rebase` patch asks for, judged from its payload alone.
#[derive(Debug, PartialEq, Eq)]
enum RebaseAction<'a> {
    Replay {
        fingerprint: Option<&'a str>,
        take_committed_tip: bool,
        drop_incoming_reason: Option<&'a str>,
    },
    FastForwardFromOrigin {
        base: bool,
    },
    /// The bounded managed retry of a recovery fence's targeted repair
    /// (`orchestrator/branch_recovery.rs`), brought forward.
    Repair,
    DismissAcceptance(Dismissal<'a>),
}

/// The coordinates a dismissal must name. The journal's helper re-checks all
/// of them under the lock; this only refuses what can never be right.
#[derive(Debug, PartialEq, Eq)]
struct Dismissal<'a> {
    acceptance_id: &'a str,
    expected_head: &'a str,
    reason: &'a str,
}

const DISMISSAL_KEYS: [&str; 3] = ["acceptance_id", "expected_head", "reason"];
const REPLAY_AND_FAST_FORWARD_KEYS: [&str; 4] = [
    "fingerprint",
    "resolution",
    "drop_incoming_reason",
    "branch",
];
const MAX_DISMISSAL_REASON_CHARS: usize = 2000;
const DISMISSAL_PURPOSE: &str = "dismiss an unknown acceptance";

fn present_key<'k>(payload: &serde_json::Value, keys: &[&'k str]) -> Option<&'k str> {
    keys.iter()
        .copied()
        .find(|key| payload.get(key).is_some_and(|value| !value.is_null()))
}

/// A full commit id in the spelling the journal stores: SHA-1 or SHA-256 hex,
/// lower case. A prefix could name a different commit tomorrow.
fn is_full_commit_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_rebase_action(payload: &serde_json::Value) -> Result<RebaseAction<'_>, String> {
    let action =
        payload_non_empty_str(payload, "action", &[]).ok_or("payload.action is required")?;
    if action == "dismiss-acceptance" {
        return parse_dismissal(payload).map(RebaseAction::DismissAcceptance);
    }
    if !matches!(action, "replay" | "fast-forward-from-origin" | "repair") {
        return Err(format!(
            "payload.action must be 'replay', 'fast-forward-from-origin', 'repair', 'rebuild', 'catch-up-mode', 'mark-reviewed' or 'dismiss-acceptance', got '{action}'"
        ));
    }
    if let Some(key) = present_key(payload, &DISMISSAL_KEYS) {
        return Err(format!(
            "payload.{key} applies only to action:'dismiss-acceptance'"
        ));
    }
    if action == "repair" {
        // A repair retries the fence's recorded repair and nothing else.
        if let Some(key) = present_key(payload, &REPLAY_AND_FAST_FORWARD_KEYS) {
            return Err(format!(
                "payload.{key} does not apply to action:'repair', which only retries the \
                 branch's recorded recovery repair"
            ));
        }
        return Ok(RebaseAction::Repair);
    }
    let fast_forward_branch = payload_non_empty_str(payload, "branch", &[]);
    if fast_forward_branch.is_some() && action != "fast-forward-from-origin" {
        return Err("payload.branch applies only to fast-forward-from-origin".to_string());
    }
    if fast_forward_branch.is_some_and(|value| value != "base") {
        return Err("payload.branch must be 'base' when provided: fast-forward-from-origin acts on this node's branch, or with branch:'base' on the branch it is cut from".to_string());
    }
    let fingerprint = payload_non_empty_str(payload, "fingerprint", &[]);
    let resolution = payload_non_empty_str(payload, "resolution", &[]);
    if resolution.is_some_and(|value| value != "take-committed-tip") {
        return Err("payload.resolution must be 'take-committed-tip' when provided".to_string());
    }
    // The documented escape from the loss guard. It only means anything
    // alongside the resolution whose whole-file restore does the dropping, so a
    // request carrying it without one is confused about what it is asking for
    // and is told so rather than obeyed.
    let drop_incoming_reason = payload_non_empty_str(payload, "drop_incoming_reason", &[]);
    if drop_incoming_reason.is_some() && resolution.is_none() {
        return Err("payload.drop_incoming_reason only applies to \
                    resolution:'take-committed-tip', which is what discards incoming hunks; a \
                    plain replay drops nothing"
            .to_string());
    }
    Ok(if action == "fast-forward-from-origin" {
        RebaseAction::FastForwardFromOrigin {
            base: fast_forward_branch.is_some(),
        }
    } else {
        RebaseAction::Replay {
            fingerprint,
            take_committed_tip: resolution.is_some(),
            drop_incoming_reason,
        }
    })
}

fn parse_dismissal(payload: &serde_json::Value) -> Result<Dismissal<'_>, String> {
    // A dismissal replays, resolves and fast-forwards nothing, so a key from
    // those actions means the caller is asking for two things at once.
    if let Some(key) = present_key(payload, &REPLAY_AND_FAST_FORWARD_KEYS) {
        return Err(format!(
            "payload.{key} does not apply to action:'dismiss-acceptance', which replays, \
             resolves and fast-forwards nothing; send the dismissal on its own with \
             acceptance_id, expected_head and reason"
        ));
    }
    let required = |key: &str| {
        payload_non_empty_str(payload, key, &[])
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "payload.{key} is required for action:'dismiss-acceptance' (acceptance_id, \
                     expected_head and reason, as cairn:~/rebase lists them)"
                )
            })
    };
    let acceptance_id = required("acceptance_id")?;
    let expected_head = required("expected_head")?;
    let reason = required("reason")?.trim();
    if !is_full_commit_id(expected_head) {
        return Err(format!(
            "payload.expected_head must be the branch's full current commit id (40 or 64 \
             lower-case hex characters) exactly as cairn:~/rebase shows it; '{expected_head}' \
             is not one, and a prefix or a branch name is refused"
        ));
    }
    if reason.chars().count() > MAX_DISMISSAL_REASON_CHARS {
        return Err(format!(
            "payload.reason is longer than {MAX_DISMISSAL_REASON_CHARS} characters; say why \
             retiring the row is correct in a sentence or two"
        ));
    }
    Ok(Dismissal {
        acceptance_id,
        expected_head,
        reason,
    })
}

/// Only the node that owns the branch may dismiss its debt: the agent closest
/// to the work, or a task it delegated (which runs under the same job). A
/// foreign node, or a request from no run at all, is refused; there is no
/// separate operator path here.
async fn authorize_branch_owner(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    owner_job_id: &str,
) -> Result<(), String> {
    let (ctx, _) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .map_err(|_| {
            "Unauthorized: dismissing an acceptance is the repair of the node that owns the \
             branch, and this request comes from no agent run. Ask that node to send it."
                .to_string()
        })?;
    if ctx.job_id != owner_job_id {
        return Err(
            "Unauthorized: only the node that owns this branch, or a task it delegated, may \
             dismiss its acceptances; this request comes from a different node. Nothing was \
             changed."
                .to_string(),
        );
    }
    Ok(())
}

async fn dismiss_acceptance(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    project: &str,
    branch: &str,
    dismissal: &Dismissal<'_>,
    dry_run: bool,
) -> Result<String, String> {
    let Dismissal {
        acceptance_id,
        expected_head,
        reason,
    } = dismissal;
    let project_id =
        crate::mcp::handlers::run_context::project_id_by_key(&orch.db.local, project).await?;
    let repo = crate::orchestrator::base_advance::load_project_repo_path(&orch.db, &project_id)
        .await?
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            "Cairn holds no repository path for this project, so no store journal can owe an \
             acceptance here"
                .to_string()
        })?;
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo));
    if dry_run {
        return Ok(format!(
            "Would dismiss unknown acceptance `{acceptance_id}` of `{branch}` if the branch's \
             head is still `{expected_head}` when read under its reservation and the store \
             lock. Nothing would move, export or publish."
        ));
    }
    let reservation = orch
        .reserve_branches(
            &store,
            &[branch],
            DISMISSAL_PURPOSE,
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    let guard = crate::mcp::vcs::acquire_store_lock(
        orch,
        Some(&store),
        crate::orchestrator::StoreLockClass::Foreground,
        DISMISSAL_PURPOSE,
        crate::mcp::vcs::STORE_LOCK_TIMEOUT,
    )
    .await?
    .ok_or_else(|| "the store lock was not taken".to_string())?;
    let outcome = crate::orchestrator::base_advance::preparation::dismiss_unknown_acceptance(
        db,
        &guard,
        &reservation,
        &project_id,
        &store,
        branch,
        acceptance_id,
        expected_head,
        reason,
    )
    .await?;
    drop(guard);
    drop(reservation);
    Ok(match outcome {
        AcceptanceDismissal::Dismissed => format!(
            "Dismissed acceptance `{acceptance_id}` of `{branch}` at its current head \
             `{expected_head}`. The row stays in the journal as `dismissed` with your reason. \
             Its drop claims were never proven and are discarded, not accepted. Nothing moved, \
             exported or published. The branch's next writer is admitted unless it still owes \
             another acceptance; a later publication must still account for every commit only \
             origin holds, and may still refuse."
        ),
        AcceptanceDismissal::AlreadyDismissed => format!(
            "Acceptance `{acceptance_id}` of `{branch}` was already dismissed at \
             `{expected_head}` with this reason; nothing changed."
        ),
    })
}

/// The `cairn:~/rebase` page's account of what the branch still owes the
/// acceptance journal, and the repair for an `unknown` row. `head` is the
/// branch's literal head from the page's pinned view, `None` when it cannot be
/// read; the page then shows no expected head rather than a stale or guessed
/// one. Empty when nothing is owed.
pub(crate) fn acceptance_debt_section(
    debt: &Result<Vec<AcceptanceDebt>, String>,
    head: Option<&str>,
) -> String {
    let rows = match debt {
        Ok(rows) if rows.is_empty() => return String::new(),
        Ok(rows) => rows,
        Err(error) => {
            return format!(
                "### Acceptance debt\n\nWhether this branch owes an earlier install's \
                 acceptance is unknown: the journal could not be read ({error}). Writers of \
                 this branch refuse until it can be.\n"
            )
        }
    };
    let mut out = String::from(
        "### Acceptance debt\n\nThis branch owes the acceptance of an earlier install, and \
         every writer of it refuses until that is settled.\n\n",
    );
    match head {
        Some(head) => out.push_str(&format!("Current head: `{head}`\n\n")),
        None => out.push_str(
            "Current head: unknown right now (the store could not be read at one settled \
             view).\n\n",
        ),
    }
    let mut any_unknown = false;
    for row in rows {
        let install = format!("install from `{}` to `{}`", row.head_before, row.head_after);
        match row.status.as_str() {
            "pending" => out.push_str(&format!(
                "- `{}` **pending**, {install}. It settles at this branch's next guarded \
                 write against the literal head: the install's resulting head accepts it, its \
                 starting head abandons it, anything else makes it unknown. A pending row \
                 cannot be dismissed.\n",
                row.id
            )),
            "unknown" => {
                any_unknown = true;
                out.push_str(&format!(
                    "- `{}` **unknown**, {install}. No head names this install, so it needs \
                     an explicit repair.\n",
                    row.id
                ));
                match head {
                    Some(head) => out.push_str(&format!(
                        "  Dismiss it (this node or its tasks only): \
                         `write({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{action:\"dismiss-acceptance\",acceptance_id:\"{}\",expected_head:\"{head}\",reason:\"...\"}}}}]}})`\n",
                        row.id
                    )),
                    None => out.push_str(
                        "  Its dismissal needs the branch's exact current head, which cannot \
                         be read right now; re-read this page before dismissing.\n",
                    ),
                }
            }
            other => out.push_str(&format!("- `{}` **{other}**, {install}.\n", row.id)),
        }
    }
    if any_unknown {
        out.push_str(
            "\n⚠️ Dismissing retires the row; it never accepts it. The row's drop claims were \
             never proven and are discarded, and nothing moves, exports or publishes. A later \
             publication must still account for every commit only origin holds, and may still \
             refuse. Dismiss only once the current head is the work you mean to keep.\n",
        );
    }
    out
}

/// A parsed `catch-up-mode` payload.
struct CatchUpSetting {
    mode: String,
    review_paths: Vec<String>,
    reviewed_through: Option<String>,
}

fn parse_catch_up_setting(payload: &serde_json::Value) -> Result<CatchUpSetting, String> {
    let mode = payload_non_empty_str(payload, "mode", &[]).ok_or_else(|| {
        "catch-up-mode needs payload.mode: 'merge' (catch up by one merge commit) or 'replay' \
         (end the mode)"
            .to_string()
    })?;
    if !matches!(mode, "merge" | "replay") {
        return Err(format!(
            "payload.mode must be 'merge' or 'replay', got '{mode}'"
        ));
    }
    let review_paths = match payload.get("review_paths") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                    "payload.review_paths is a list of path globs (strings)".to_string()
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("payload.review_paths is a list of path globs".to_string()),
    };
    let reviewed_through =
        payload_non_empty_str(payload, "reviewed_through", &[]).map(str::to_string);
    if mode == "replay" && (!review_paths.is_empty() || reviewed_through.is_some()) {
        return Err(
            "review_paths and reviewed_through apply only to mode:'merge'; mode:'replay' ends the \
             mode"
                .to_string(),
        );
    }
    crate::orchestrator::catch_up::validate_review_paths(&review_paths)?;
    Ok(CatchUpSetting {
        mode: mode.to_string(),
        review_paths,
        reviewed_through,
    })
}

/// The authority a `catch-up-mode` write crosses, for `authorization::prepare`:
/// `None` unless the change is that action on a node's rebase page.
pub(crate) async fn rebase_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<Prepared> {
    if item.mode != ChangeMode::Patch {
        return None;
    }
    let payload = item.payload.as_ref()?;
    if payload_non_empty_str(payload, "action", &[]) != Some("catch-up-mode") {
        return None;
    }
    let CairnResource::NodeRebase {
        project,
        number,
        exec_seq,
        node_id,
    } = target_resource_for_request(orch, request, item)
        .await
        .ok()?
    else {
        return None;
    };
    Some(catch_up_authority(orch, &project, number, exec_seq, &node_id, payload).await)
}

/// Prepare and dispatch both derive the request here, so they name the same
/// one. A branch that can never take the mode is refused here, which
/// preparation turns into a refusal ahead of any card: no approval is spent on
/// a request that could not succeed. Any read error fails closed as `Invalid`.
async fn catch_up_authority(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    payload: &serde_json::Value,
) -> Prepared {
    let prepared = async {
        let setting = parse_catch_up_setting(payload)?;
        let db = orch.db.for_project(project).await;
        let rows =
            crate::resources::NodeRebaseRows::read(&db, project, number, exec_seq, node_id).await?;
        let branch = rows
            .branch
            .clone()
            .ok_or_else(|| "This node has no branch to set a catch-up mode on.".to_string())?;
        let project_id = rows
            .project_id()
            .ok_or_else(|| "this node's project could not be read".to_string())?;
        if branch == project_default_branch(&db, project_id).await? {
            return Ok(Prepared::Refused(format!(
                "Refused: `{branch}` is the project's default branch, which never catches up with \
                 anything. Nothing changed."
            )));
        }
        Ok::<_, String>(Prepared::Authority(
            crate::authorization::normalize::branch_catch_up_request(
                project_id,
                &branch,
                &setting.mode,
                &setting.review_paths,
                setting.reviewed_through.as_deref(),
            ),
        ))
    }
    .await;
    prepared.unwrap_or_else(Prepared::Invalid)
}

/// Who is writing: the run's node home, or `operator` for a write no run makes.
/// A run may write only for its own node.
async fn writer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    owning_job: &str,
) -> Result<String, String> {
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Ok("operator".to_string());
    }
    let (context, _) =
        crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if context.job_id != owning_job {
        return Err(
            "Refused: only this branch's owning node or the operator may change its rebase state. \
             Nothing changed."
                .to_string(),
        );
    }
    Ok(
        crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
            .await
            .unwrap_or_else(|_| format!("run {}", context.run_id)),
    )
}

/// The commit `revision` names, proven to lie in both the branch's and the
/// default branch's history.
fn shared_commit(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    branch: &str,
    default_branch: &str,
    revision: &str,
) -> Result<String, String> {
    let commit = crate::jj::revset_commit_checked(jj, store, revision)?
        .ok_or_else(|| format!("`{revision}` does not name one commit in the store"))?;
    let branch_revset = format!("bookmarks(exact:{branch:?})");
    let default_revset = format!("bookmarks(exact:{default_branch:?})");
    if !crate::jj::revset_descends_from(jj, store, &branch_revset, &commit)
        || !crate::jj::revset_descends_from(jj, store, &default_revset, &commit)
    {
        return Err(format!(
            "{commit} is not in the history of both `{branch}` and `{default_branch}`; a review \
             mark is a base commit this branch already contains"
        ));
    }
    Ok(commit)
}

async fn project_default_branch(
    db: &crate::storage::LocalDb,
    project_id: &str,
) -> Result<String, String> {
    db.query_opt_text(
        "SELECT default_branch FROM projects WHERE id = ?1",
        (project_id.to_string(),),
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "the project's default branch could not be read".to_string())
}

#[allow(clippy::too_many_arguments)]
async fn set_catch_up_mode(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    db: &crate::storage::LocalDb,
    rows: &crate::resources::NodeRebaseRows,
    branch: &str,
    resource: (&str, i32, i32, &str),
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let setting = parse_catch_up_setting(payload)?;
    // Branches cut from this one are unaffected by its mode, finished or live:
    // every consumer keys on the merge-mode bookmark itself, and a child is
    // reconciled under its own name, so it still rebases onto this branch's
    // tip, merge commit or not.
    let (project, number, exec_seq, node_id) = resource;
    let authority =
        match catch_up_authority(orch, project, number, exec_seq, node_id, payload).await {
            Prepared::Authority(authority) => authority,
            Prepared::Invalid(error) | Prepared::Refused(error) => return Err(error),
        };
    let project_id = rows
        .project_id()
        .ok_or_else(|| "this node's project could not be read".to_string())?
        .to_string();
    let store = rows.store_path(orch).await?;
    let default_branch = project_default_branch(db, &project_id).await?;
    if dry_run {
        return Ok(format!("Would {}", authority.summary));
    }
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(format!(
            "Denied: a branch's catch-up mode is the operator's to approve, and this request comes \
             from no run that could ask. Ask an agent on this node to request it; the operator's \
             approval of that card sets it. Asked: {}.",
            authority.summary
        ));
    };
    let (decision, event_id) = crate::authorization::authorize_recorded(&actor, &authority).await?;
    match decision {
        AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. } => {}
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            return Err(crate::authorization::refusal_message(&authority, reason))
        }
    }
    let asker = actor
        .principal
        .node_uri
        .clone()
        .or_else(|| actor.principal.run_id.clone())
        .unwrap_or_else(|| "an unnamed run".to_string());
    let who = match event_id {
        Some(event) => format!("operator (approved {event}, asked by {asker})"),
        None => format!("operator (asked by {asker})"),
    };

    if setting.mode == "replay" {
        let cleared = crate::orchestrator::catch_up::clear(
            db,
            &project_id,
            &store,
            branch,
            &who,
            "returned to catching up by replay",
        )
        .await?;
        return Ok(if cleared {
            format!("`{branch}` catches up by replay again; its merge catch-up mode is cleared.")
        } else {
            format!("`{branch}` had no catch-up mode; it catches up by replay.")
        });
    }

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (read_store, read_branch, read_default, through) = (
        store.clone(),
        branch.to_string(),
        default_branch.clone(),
        setting.reviewed_through.clone(),
    );
    let reviewed_through = tokio::task::spawn_blocking(move || -> Result<String, String> {
        match through {
            Some(revision) => {
                shared_commit(&jj, &read_store, &read_branch, &read_default, &revision)
            }
            None => {
                let tip = crate::jj::bookmark_commit_checked(&jj, &read_store, &read_branch)?
                    .ok_or_else(|| format!("`{read_branch}` does not resolve in the store"))?;
                let default_tip =
                    crate::jj::bookmark_commit_checked(&jj, &read_store, &read_default)?
                        .ok_or_else(|| format!("`{read_default}` does not resolve in the store"))?;
                let bases = crate::jj::merge_bases(&jj, &read_store, &tip, &default_tip)?;
                match bases.as_slice() {
                    [base] => Ok(base.clone()),
                    _ => Err(format!(
                        "`{read_branch}` and `{read_default}` share {} merge bases ({}), so there \
                         is no one default review mark; pass reviewed_through explicitly",
                        bases.len(),
                        bases.join(", ")
                    )),
                }
            }
        }
    })
    .await
    .map_err(|error| format!("review mark worker failed: {error}"))??;
    let mode = crate::orchestrator::catch_up::set(
        db,
        crate::orchestrator::catch_up::SetCatchUpMode {
            project_id: &project_id,
            store: &store,
            bookmark: branch,
            job_id: &rows.job_id,
            actor: &who,
            review_paths: &setting.review_paths,
            reviewed_through: &reviewed_through,
        },
    )
    .await?;
    Ok(format!(
        "`{branch}` now catches up by merge (set by {who}). Review-required paths: {}. Reviewed \
         through {}. It is never replayed; the next base advance merges the base into it.",
        if mode.review_paths.is_empty() {
            "none".to_string()
        } else {
            mode.review_paths.join(", ")
        },
        mode.reviewed_through
    ))
}

async fn mark_reviewed(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    db: &crate::storage::LocalDb,
    rows: &crate::resources::NodeRebaseRows,
    branch: &str,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let through = payload_non_empty_str(payload, "through", &[])
        .ok_or_else(|| {
            "mark-reviewed needs payload.through: the base commit you reviewed through".to_string()
        })?
        .to_string();
    let project_id = rows
        .project_id()
        .ok_or_else(|| "this node's project could not be read".to_string())?
        .to_string();
    let store = rows.store_path(orch).await?;
    let mode = crate::orchestrator::catch_up::mode_for(db, &project_id, &store, branch)
        .await?
        .ok_or_else(|| {
            format!("`{branch}` has no catch-up mode, so there is no review mark to move")
        })?;
    let who = writer(orch, request, &rows.job_id).await?;
    let default_branch = project_default_branch(db, &project_id).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (read_store, read_branch, previous) = (
        store.clone(),
        branch.to_string(),
        mode.reviewed_through.clone(),
    );
    let commit = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let commit = shared_commit(&jj, &read_store, &read_branch, &default_branch, &through)?;
        if !crate::jj::revset_descends_from(&jj, &read_store, &commit, &previous) {
            return Err(format!(
                "{commit} is not at or after the current review mark {previous}; a review mark \
                 only moves forward"
            ));
        }
        Ok(commit)
    })
    .await
    .map_err(|error| format!("review mark worker failed: {error}"))??;
    if dry_run {
        return Ok(format!("Would mark `{branch}` reviewed through {commit}"));
    }
    crate::orchestrator::catch_up::mark_reviewed(db, &mode, &commit, &who).await?;
    Ok(format!(
        "`{branch}` is marked reviewed through {commit} by {who} (was {}).",
        mode.reviewed_through
    ))
}

mod acceptance;

/// The payload keys only `rebuild` reads.
const REBUILD_KEYS: [&str; 4] = [
    "source_commit",
    "source_base",
    "source_base_reason",
    "expected_tip",
];

async fn require_rebuild_branch_owner(
    db: &crate::storage::LocalDb,
    job_id: &str,
    branch: &str,
) -> Result<(), String> {
    match rebuild_branch_owner_refusal(db, job_id, branch).await? {
        Some(reason) => Err(reason.to_string()),
        None => Ok(()),
    }
}

async fn rebuild_branch_owner_refusal(
    db: &crate::storage::LocalDb,
    job_id: &str,
    branch: &str,
) -> Result<Option<&'static str>, String> {
    let job = job_id.to_string();
    let default = db.query_opt_text(
        "SELECT p.default_branch FROM projects p JOIN jobs j ON j.project_id = p.id WHERE j.id = ?1",
        (job.clone(),),
    ).await.map_err(|e| e.to_string())?.ok_or_else(|| "rebuild project is missing".to_string())?;
    if branch == default {
        return Ok(Some(
            "rebuild refuses to replace the project's default branch",
        ));
    }
    let parent_branch = db
        .query_opt_text(
            "SELECT p.branch FROM jobs j JOIN jobs p ON p.id = j.parent_job_id WHERE j.id = ?1",
            (job.clone(),),
        )
        .await
        .map_err(|e| e.to_string())?;
    if parent_branch.as_deref() == Some(branch) {
        return Ok(Some("rebuild refused: this node inherits the upstream branch; address its owning node instead"));
    }
    let execution = db
        .query_opt_text(
            "SELECT execution_id FROM jobs WHERE id = ?1",
            (job.clone(),),
        )
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "rebuild owner has no execution".to_string())?;
    let node = db
        .query_opt_text("SELECT recipe_node_id FROM jobs WHERE id = ?1", (job,))
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "rebuild owner has no recipe node".to_string())?;
    let snapshot = crate::jobs::queries::load_execution_snapshot(db, &execution)
        .await
        .map_err(|e| e.to_string())?;
    let node = snapshot
        .recipe
        .nodes
        .iter()
        .find(|candidate| candidate.id == node)
        .ok_or_else(|| "rebuild owning recipe node is absent from its execution".to_string())?;
    let db_node = crate::execution::dag::recipe_node_to_db(node, &snapshot.recipe.id);
    if !crate::execution::step_behavior::resolve_node_behavior(&db_node).mints_branch {
        return Ok(Some("rebuild refused: this node did not mint an isolated branch; address its owning node instead"));
    }
    Ok(None)
}

fn rebuild_coordinates(payload: &serde_json::Value) -> Result<(&str, &str, &str, &str), String> {
    let source = payload_non_empty_str(payload, "source_commit", &[]).ok_or_else(|| {
        "rebuild requires source_commit: the full last clean branch commit ID".to_string()
    })?;
    let expected = payload_non_empty_str(payload, "expected_tip", &[]).ok_or_else(|| {
        "rebuild requires expected_tip: the full flagged branch tip ID".to_string()
    })?;
    for key in [
        "branch",
        "resolution",
        "fingerprint",
        "drop_incoming_reason",
        "mode",
        "through",
        "review_paths",
        "reviewed_through",
        "acceptance_id",
        "expected_head",
        "reason",
    ] {
        if payload.get(key).is_some() {
            return Err(format!("payload.{key} does not apply to rebuild"));
        }
    }
    let source_base = payload_non_empty_str(payload, "source_base", &[])
        .ok_or_else(|| "rebuild requires source_base: owner-attested full original branch cut ID, excluding inherited parent changes".to_string())?;
    let reason = payload_non_empty_str(payload, "source_base_reason", &[])
        .ok_or_else(|| "rebuild requires source_base_reason explaining why this is the original cut, excluding inherited parent changes".to_string())?;
    Ok((source, source_base, reason, expected))
}

#[allow(clippy::too_many_arguments)]
async fn rebuild(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let (source, source_base, reason, expected) = rebuild_coordinates(payload)?;
    let (source, source_base, reason, expected) = (
        source.to_string(),
        source_base.to_string(),
        reason.to_string(),
        expected.to_string(),
    );
    let db = orch.db.for_project(project).await;
    let rows =
        crate::resources::NodeRebaseRows::read(&db, project, number, exec_seq, node_id).await?;
    writer(orch, request, &rows.job_id).await?;
    let branch = rows
        .branch
        .clone()
        .ok_or_else(|| "This node has no branch to rebuild".to_string())?;
    require_rebuild_branch_owner(&db, &rows.job_id, &branch).await?;
    let base = crate::resources::node_base_branch(orch, &rows, &branch).await?;
    let store = rows.store_path(orch).await?;
    let _guard = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            format!("explicit rebuild: {branch}"),
        )
        .await;
    crate::orchestrator::branch_recovery::refuse_if_fenced(
        &db,
        &store,
        &[&branch, &base],
        "rebuild flagged branch",
    )
    .await?;
    if let Some(batch) =
        crate::orchestrator::base_advance::inflight_run_batch_on_branch(&db, &rows.job_id, &branch)
            .await?
    {
        return Err(format!("Rebuild refused while {} can still publish to this branch; finish or stop that batch and retry.", batch.describe()));
    }
    // Read under the store lock, where a parent's session is opened or
    // settled, so the answer cannot change before the native call below.
    if let Some(hold) = crate::orchestrator::base_advance::preparation::held_by_parent_conflict(
        &db, &store, &branch, &base,
    )
    .await?
    {
        return Err(format!(
            "Rebuild refused: `{}` is held behind its parent `{}`, whose conflict session {} with \
             `{}` at {} is open. Nothing was rebuilt; resolve the parent's session first, then \
             retry.",
            hold.branch, hold.parent, hold.session, hold.parent_base, hold.parent_destination
        ));
    }
    let snapshot = acceptance::Snapshot::capture(&db, &store, &branch, &rows.job_id).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (worker_jj, worker_store, worker_branch, worker_base) =
        (jj.clone(), store.clone(), branch.clone(), base.clone());
    let outcome = tokio::task::spawn_blocking(move || {
        crate::jj::rebuild_flagged_branch(
            &worker_jj,
            &worker_store,
            &worker_branch,
            &worker_base,
            &source,
            &source_base,
            &reason,
            &expected,
            dry_run,
        )
    })
    .await
    .map_err(|error| format!("rebuild worker failed: {error}"))??;
    match outcome {
        crate::jj::RebuildOutcome::Preview { summary } => Ok(summary),
        crate::jj::RebuildOutcome::Accepted {
            previous_tip,
            rebuilt_tip,
            base_commit,
            summary,
            export_error,
        } => {
            let retained = snapshot.accept(&db, &jj, &store, &branch, &base, &previous_tip, &rebuilt_tip, &base_commit).await
                .map_err(|error| format!("Rebuild remains committed at {rebuilt_tip}, but metadata acceptance failed: {error}. Retry this same rebuild request; it will not create another native head."))?;
            let summary = if retained == 0 {
                summary
            } else {
                format!("{summary} Retained {retained} newer or unproven replay generations; those obligations were not superseded.")
            };
            match export_error {
                Some(error) => Err(format!("{summary} Metadata accepted, but Git export failed: {error}. Retry this same rebuild request to retry export only.")),
                None => Ok(summary),
            }
        }
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::NodeRebase {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if payload_non_empty_str(payload, "action", &[]) == Some("rebuild") {
                return rebuild(
                    orch, request, project, *number, *exec_seq, node_id, payload, dry_run,
                )
                .await
                .map(Some)
                .map_err(|error| build_failure(index, item, error));
            }
            if let Some(key) = present_key(payload, &REBUILD_KEYS) {
                return Err(build_failure(
                    index,
                    item,
                    format!("payload.{key} applies only to action:'rebuild'"),
                ));
            }
            if matches!(
                payload_non_empty_str(payload, "action", &[]),
                Some("catch-up-mode" | "mark-reviewed")
            ) {
                let db = orch.db.for_project(project).await;
                let rows = crate::resources::NodeRebaseRows::read(
                    &db, project, *number, *exec_seq, node_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let branch = rows.branch.clone().ok_or_else(|| {
                    build_failure(index, item, "This node has no branch.".to_string())
                })?;
                let outcome =
                    if payload_non_empty_str(payload, "action", &[]) == Some("catch-up-mode") {
                        set_catch_up_mode(
                            orch,
                            request,
                            &db,
                            &rows,
                            &branch,
                            (project.as_str(), *number, *exec_seq, node_id.as_str()),
                            payload,
                            dry_run,
                        )
                        .await
                    } else {
                        mark_reviewed(orch, request, &db, &rows, &branch, payload, dry_run).await
                    };
                return outcome
                    .map(Some)
                    .map_err(|error| build_failure(index, item, error));
            }
            let action =
                parse_rebase_action(payload).map_err(|error| build_failure(index, item, error))?;

            let db = orch.db.for_project(project).await;
            // Read, then release: the origin fetch and the replay below both
            // wait on the store lock, which this same write batch may hold, so
            // nothing here may keep a read open across them (cairn/5502).
            let rows =
                crate::resources::NodeRebaseRows::read(&db, project, *number, *exec_seq, node_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
            let no_branch = if matches!(action, RebaseAction::DismissAcceptance(_)) {
                "This node has no branch, so it owes no acceptance."
            } else {
                "This node has no branch to replay."
            };
            let branch = rows
                .branch
                .clone()
                .ok_or_else(|| build_failure(index, item, no_branch.to_string()))?;

            match action {
                RebaseAction::DismissAcceptance(dismissal) => {
                    authorize_branch_owner(orch, request, &rows.job_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    dismiss_acceptance(orch, &db, project, &branch, &dismissal, dry_run)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
                RebaseAction::Repair if dry_run => {
                    format!("Would retry the targeted recovery repair of `{branch}` now")
                }
                RebaseAction::Repair => {
                    let store = rows
                        .store_path(orch)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    crate::orchestrator::branch_recovery::repair_now(orch, &db, &store, &branch)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
                RebaseAction::FastForwardFromOrigin { base } => {
                    let target = if base {
                        crate::resources::node_base_branch(orch, &rows, &branch)
                            .await
                            .map_err(|error| build_failure(index, item, error))?
                    } else {
                        branch.clone()
                    };
                    if dry_run {
                        format!(
                            "Would take origin's copy of `{target}` if it is a pure fast-forward"
                        )
                    } else {
                        let project_id = crate::mcp::handlers::run_context::project_id_by_key(
                            &orch.db.local,
                            project,
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                        crate::orchestrator::base_branch_sync::fast_forward_now(
                            orch,
                            &project_id,
                            &target,
                        )
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                    }
                }
                RebaseAction::Replay { .. } if dry_run => {
                    format!("Would request a store-side replay of `{branch}`")
                }
                RebaseAction::Replay {
                    fingerprint,
                    take_committed_tip,
                    drop_incoming_reason,
                } => crate::orchestrator::base_advance::request_branch_replay(
                    orch,
                    &db,
                    &rows.job_id,
                    &branch,
                    fingerprint,
                    take_committed_tip,
                    drop_incoming_reason,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn rebuild_payload_requires_explicit_coordinates_and_rejects_replay_overrides() {
        use serde_json::json;
        assert!(rebuild_coordinates(&json!({}))
            .unwrap_err()
            .contains("source_commit"));
        assert!(rebuild_coordinates(&json!({"source_commit":"source"}))
            .unwrap_err()
            .contains("expected_tip"));
        assert!(
            rebuild_coordinates(&json!({"source_commit":"source", "expected_tip":"tip"}))
                .unwrap_err()
                .contains("source_base")
        );
        assert!(rebuild_coordinates(
            &json!({"source_commit":"source", "source_base":"cut", "expected_tip":"tip"})
        )
        .unwrap_err()
        .contains("source_base_reason"));
        for key in [
            "branch",
            "resolution",
            "fingerprint",
            "drop_incoming_reason",
            "mode",
            "through",
            "review_paths",
            "reviewed_through",
        ] {
            let mut payload = json!({"source_commit":"source", "source_base":"cut", "source_base_reason":"original cut", "expected_tip":"tip"});
            payload[key] = json!("override");
            assert!(rebuild_coordinates(&payload).unwrap_err().contains(key));
        }
        assert_eq!(
            rebuild_coordinates(&json!({"source_commit":"source", "source_base":"cut", "source_base_reason":"original cut", "expected_tip":"tip"})).unwrap(),
            ("source", "cut", "original cut", "tip")
        );
    }

    #[tokio::test]
    async fn rebuild_dispatch_denies_inherited_and_default_branches_before_store_access() {
        let db = Arc::new(crate::storage::migrated_test_db("rebuild-inherited.db").await);
        // Child workflows have node addresses despite their delegation parent;
        // ordinary child jobs resolve only through task addresses.
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1','default','Project','proj','/repo','main',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1','proj-1',1,'Issue','active',1,1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1','recipe-default','issue-1','proj-1','running',1,1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('parent','exec-1','builder','issue-1','proj-1','running','agent/parent','main','builder',1,1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, parent_job_id, agent_config_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('child','exec-1','reviewer','issue-1','proj-1','parent','workflow','running','agent/parent','main','reviewer',1,1);"
        ).await.unwrap();
        let mut snapshot: crate::models::ExecutionSnapshot = serde_json::from_str(
            &crate::test_support::shipping_execution_snapshot_json("issue-1", "proj-1"),
        )
        .unwrap();
        snapshot.recipe.id = "recipe-default".into();
        let node = &mut snapshot.recipe.nodes[0];
        node.id = "reviewer".into();
        node.agent_config.as_mut().unwrap().git_config = Some(crate::models::AgentGitConfig {
            branch_mode: crate::models::BranchMode::Inherit,
            ..Default::default()
        });
        db.execute(
            "UPDATE executions SET snapshot=?1 WHERE id='exec-1'",
            (serde_json::to_string(&snapshot).unwrap(),),
        )
        .await
        .unwrap();
        let rows = crate::resources::NodeRebaseRows::read(&db, "proj", 1, 1, "reviewer")
            .await
            .unwrap();
        assert_eq!(rows.job_id, "child");
        assert_eq!(rows.branch.as_deref(), Some("agent/parent"));
        let root = tempfile::tempdir().unwrap();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.path().join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        let item = ChangeItem {
            target: "cairn://p/proj/1/1/reviewer/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({
                "action":"rebuild",
                "source_base_reason":"original cut",
                "source_base":"123456789abcdef0123456789abcdef012345678",
                "source_commit":"0123456789abcdef0123456789abcdef01234567",
                "expected_tip":"abcdef0123456789abcdef0123456789abcdef01"
            })),
        };
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        };
        let resource = CairnResource::NodeRebase {
            project: "proj".into(),
            number: 1,
            exec_seq: 1,
            node_id: "reviewer".into(),
        };
        let error = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("inherits the upstream branch"),
            "{error:?}"
        );
        db.execute_script(
            "UPDATE jobs SET branch='main', base_branch='agent/parent' WHERE id='child';",
        )
        .await
        .unwrap();
        let error = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("default branch"), "{error:?}");

        // A recipe inherit node has no delegation parent to trigger the earlier
        // shortcut; its frozen branch policy must independently deny rebuild.
        db.execute_script(
            "UPDATE jobs SET parent_job_id=NULL, branch='agent/parent', base_branch='main' WHERE id='child';",
        )
        .await
        .unwrap();
        let error = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("did not mint an isolated branch"),
            "{error:?}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn rebuild_team_overlay_routes_standing_preview_and_acceptance_to_the_local_store() {
        use crate::jj::tests::{git, git_stdout, init_project, rebase_recording_conflict};
        use crate::storage::{LocalDb, MigrationRunner, TEAM_MIGRATIONS};

        let bin = crate::jj::tests::resolve_jj().expect("provisioned real jj required");
        let root = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let path = repo.path();
        init_project(path);
        let cut = git_stdout(path, &["rev-parse", "HEAD"]);
        let branch = "agent/team-rebuild";
        git(path, &["checkout", "-q", "-b", branch]);
        std::fs::write(path.join("shared.rs"), "team branch work\n").unwrap();
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", "team branch delta"]);
        let source = git_stdout(path, &["rev-parse", "HEAD"]);
        git(path, &["checkout", "-q", "-b", "bad", &cut]);
        std::fs::write(path.join("shared.rs"), "obsolete parent\n").unwrap();
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", "obsolete parent"]);
        git(path, &["checkout", "-q", "main"]);
        std::fs::write(path.join("incoming.txt"), "current base\n").unwrap();
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", "current base"]);
        let base = git_stdout(path, &["rev-parse", "HEAD"]);
        let jj = crate::jj::environment(&bin, root.path());
        let store = crate::jj::project_store_dir(root.path(), path);
        crate::jj::ensure_project_store(&jj, &store, path).unwrap();
        rebase_recording_conflict(&jj, &store, branch, "bad");
        let flagged = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();

        let private =
            Arc::new(crate::storage::migrated_test_db("rebuild-overlay-private.db").await);
        let team = Arc::new(LocalDb::open(root.path().join("team.db")).await.unwrap());
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&team)
            .await
            .unwrap();
        let project_id = "teamrepair~00000000-0000-4000-8000-000000000001";
        let issue_id = "teamrepair~00000000-0000-4000-8000-000000000002";
        let execution_id = "teamrepair~00000000-0000-4000-8000-000000000003";
        let job_id = "teamrepair~00000000-0000-4000-8000-000000000004";
        let creator = root.path().join("creator-machine-repository");
        let creator_store = crate::jj::project_store_dir(root.path(), &creator);
        let creator_sql = creator.to_string_lossy().replace('\'', "''");
        team.execute_script(format!(
            "INSERT INTO teams(id,name,created_at,updated_at) VALUES ('teamrepair','Repair team',1,1);
             INSERT INTO projects(id,team_id,name,key,repo_path,default_branch,created_at,updated_at)
             VALUES ('{project_id}','teamrepair','Team repair','team-repair','{creator_sql}','main',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
             VALUES ('{issue_id}','{project_id}',1,'Team repair','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
             VALUES ('{execution_id}','recipe','{issue_id}','{project_id}','running',1,1);
             INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,branch,base_branch,uri_segment,created_at,updated_at)
             VALUES ('{job_id}','{execution_id}','builder','{issue_id}','{project_id}','running','{branch}','main','builder',1,1);"
        )).await.unwrap();
        team.execute(
            "UPDATE executions SET snapshot=?1 WHERE id=?2",
            (
                crate::test_support::shipping_execution_snapshot_json(issue_id, project_id),
                execution_id,
            ),
        )
        .await
        .unwrap();
        private
            .execute_script(
                "INSERT INTO teams(id,name,sync_url,replica_path,created_at)
             VALUES ('teamrepair','Repair team','http://sync.invalid','/unused-replica',1);
             INSERT INTO project_routes(project_key,team_id,created_at)
             VALUES ('team-repair','teamrepair',1);",
            )
            .await
            .unwrap();
        crate::projects::crud::set_local_repo_path(&private, "team-repair", path)
            .await
            .unwrap();
        let dbs = Arc::new(DbState::new(
            private.clone(),
            Arc::new(SearchIndex::open_or_create(root.path().join("overlay-search")).unwrap()),
        ));
        dbs.register_team_db_for_test("teamrepair".into(), team.clone())
            .await;
        dbs.set_route("team-repair", Some("teamrepair".into()))
            .await;
        let orch = Orchestrator::builder(
            dbs,
            Arc::new(TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .jj_binary_path(&bin)
        .build();
        let routed = orch.db.for_project("team-repair").await;
        assert!(Arc::ptr_eq(&routed, &team));
        assert!(private
            .query_opt_text("SELECT id FROM projects WHERE id=?1", (project_id,))
            .await
            .unwrap()
            .is_none());
        let rows = crate::resources::NodeRebaseRows::read(&routed, "team-repair", 1, 1, "builder")
            .await
            .unwrap();
        assert_eq!(rows.project_id(), Some(project_id));
        assert_eq!(rows.store_path(&orch).await.unwrap(), store);
        assert_ne!(store, creator_store);
        assert_eq!(
            crate::resources::node_base_branch(&orch, &rows, branch)
                .await
                .unwrap(),
            "main"
        );

        let item = ChangeItem {
            target: "cairn://p/team-repair/1/1/builder/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({"action":"rebuild",
                "source_commit":source, "source_base":cut, "source_base_reason":"original team mint",
                "expected_tip":flagged})),
        };
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        };
        let resource = CairnResource::NodeRebase {
            project: "team-repair".into(),
            number: 1,
            exec_seq: 1,
            node_id: "builder".into(),
        };
        let preview = dispatch(&orch, &request, 0, &item, true, &resource)
            .await
            .unwrap()
            .unwrap();
        assert!(preview.contains(&base), "{preview}");
        assert_eq!(
            crate::jj::bookmark_commit(&jj, &store, branch).as_deref(),
            Some(flagged.as_str())
        );
        let accepted = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap()
            .unwrap();
        assert!(accepted.contains("Rebuilt"), "{accepted}");
        let rebuilt = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
        assert_ne!(rebuilt, flagged);
        crate::jj::require_clean_tree(&jj, &store, &rebuilt, "team overlay rebuild").unwrap();
        assert_eq!(
            git_stdout(path, &["show", &format!("{rebuilt}:incoming.txt")]),
            "current base"
        );
        assert_eq!(
            git_stdout(path, &["show", &format!("{rebuilt}:shared.rs")]),
            "team branch work"
        );
        assert!(
            !creator_store.exists(),
            "the synced creator path must never select or initialize a store"
        );

        private
            .execute(
                "UPDATE project_routes SET local_repo_path=NULL WHERE project_key='team-repair'",
                (),
            )
            .await
            .unwrap();
        let error = rows.store_path(&orch).await.unwrap_err();
        assert!(
            error.contains("no local repository path on this machine"),
            "{error}"
        );
        assert!(crate::resources::node_base_branch(&orch, &rows, branch)
            .await
            .unwrap_err()
            .contains("no local repository path"));
        let error = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("no local repository path"),
            "{error:?}"
        );
        assert_eq!(
            crate::jj::bookmark_commit(&jj, &store, branch).as_deref(),
            Some(rebuilt.as_str())
        );
        assert!(!creator_store.exists());

        // Local projects retain their own repo path without a private overlay.
        private.execute_script(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('local-project','default','Local','local-repair','/local/repository',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
             VALUES ('local-issue','local-project',1,'Local','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
             VALUES ('local-exec','recipe','local-issue','local-project','running',1,1);
             INSERT INTO jobs(id,execution_id,issue_id,project_id,status,branch,base_branch,uri_segment,created_at,updated_at)
             VALUES ('local-job','local-exec','local-issue','local-project','running','agent/local','main','builder',1,1);"
        ).await.unwrap();
        let local =
            crate::resources::NodeRebaseRows::read(&private, "local-repair", 1, 1, "builder")
                .await
                .unwrap();
        assert_eq!(
            local.store_path(&orch).await.unwrap(),
            crate::jj::project_store_dir(root.path(), std::path::Path::new("/local/repository"))
        );
    }

    async fn catch_up_fixture(name: &str) -> (Arc<crate::storage::LocalDb>, Orchestrator) {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        // `agent/one` is an issue parent: every job ever cut from it is still
        // in `jobs`, as 4113's 295 finished children are.
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('job-1', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', 'agent/one', 'main', 'builder', 1, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('done', 'exec-1', 'issue-1', 'proj-1', 'complete', 'agent/done', 'agent/one', 'done', 1, 1),
                      ('gone', 'exec-1', 'issue-1', 'proj-1', 'cancelled', 'agent/gone', 'agent/one', 'gone', 1, 1),
                      ('broke', 'exec-1', 'issue-1', 'proj-1', 'failed', 'agent/broke', 'agent/one', 'broke', 1, 1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();
        (db, orch)
    }

    fn run_less_request() -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        }
    }

    fn catch_up_item() -> ChangeItem {
        ChangeItem {
            target: "cairn://p/proj/1/1/builder/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({"action": "catch-up-mode", "mode": "merge"})),
        }
    }

    fn builder_rebase() -> CairnResource {
        CairnResource::NodeRebase {
            project: "proj".into(),
            number: 1,
            exec_seq: 1,
            node_id: "builder".into(),
        }
    }

    /// 4113's case: a branch other jobs were cut from asks for the mode. It is
    /// a question for the operator, never a refusal, whether those children
    /// are long finished or still running.
    #[tokio::test]
    async fn catch_up_mode_is_open_to_a_branch_other_work_was_cut_from() {
        let (db, orch) = catch_up_fixture("catch-up-with-children.db").await;
        let (request, item, resource) = (run_less_request(), catch_up_item(), builder_rebase());

        for live in [false, true] {
            if live {
                db.execute_script(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
                       VALUES ('live', 'exec-1', 'issue-1', 'proj-1', 'running', 'agent/live', 'agent/one', 'live', 1, 1);",
                )
                .await
                .unwrap();
            }
            match rebase_authority(&orch, &request, &item).await {
                Some(Prepared::Authority(authority)) => {
                    assert!(
                        authority.summary.contains("agent/one"),
                        "{}",
                        authority.summary
                    )
                }
                other => {
                    panic!("preparation names the operator's authority (live={live}): {other:?}")
                }
            }
            let preview = dispatch(&orch, &request, 0, &item, true, &resource)
                .await
                .unwrap()
                .unwrap();
            assert!(
                preview.starts_with("Would let `agent/one` catch up"),
                "live={live}: {preview}"
            );
        }

        // Accepted still means subject to authority: with no run to ask for it,
        // nothing is set.
        let error = dispatch(&orch, &request, 0, &item, false, &resource)
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("catch-up mode is the operator's to approve"),
            "{error:?}"
        );
        assert!(crate::orchestrator::catch_up::mode_for(
            &db,
            "proj-1",
            &crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new("/repo")),
            "agent/one"
        )
        .await
        .unwrap()
        .is_none());
    }

    /// The default branch can never take the mode, so it is refused while the
    /// batch is prepared, before any approval card could be raised for it.
    #[tokio::test]
    async fn catch_up_mode_refuses_the_default_branch_before_any_card() {
        let (db, orch) = catch_up_fixture("catch-up-default.db").await;
        db.execute_script("UPDATE jobs SET branch = 'main' WHERE id = 'job-1';")
            .await
            .unwrap();
        let (request, item, resource) = (run_less_request(), catch_up_item(), builder_rebase());
        match rebase_authority(&orch, &request, &item).await {
            Some(Prepared::Refused(reason)) => {
                assert!(reason.contains("default branch"), "{reason}")
            }
            other => panic!("the default branch is refused in preparation: {other:?}"),
        }
        let error = dispatch(&orch, &request, 0, &item, true, &resource)
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("default branch"), "{error:?}");
    }

    /// The 10-29 holder (cairn/5502): a replay request whose store lock is
    /// already held, as it is when the same write batch carries file items.
    /// The request waits on the lock; while it does, it must hold no database
    /// connection, because a transaction parked there for the whole wait blocks
    /// every checkpoint and nothing can cut it.
    #[tokio::test]
    async fn a_replay_waiting_on_the_store_lock_holds_no_connection() {
        let db = Arc::new(crate::storage::migrated_test_db("replay-holds-nothing.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
               VALUES ('job-1', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', 'agent/one', 'main', 'builder', 1, 1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = crate::orchestrator::Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new("/repo"));
        let held = orch
            .hold_jj_store_lock_on_another_task(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                "the write batch's file items",
            )
            .await;

        let item = ChangeItem {
            target: "cairn:~/rebase".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({ "action": "replay" })),
        };
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "write".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            answer_bound_ms: None,
            origin: None,
        };
        let resource = CairnResource::NodeRebase {
            project: "proj".into(),
            number: 1,
            exec_seq: 1,
            node_id: "builder".into(),
        };
        let replay = dispatch(&orch, &request, 0, &item, false, &resource);
        let mut replay = std::pin::pin!(replay);

        // Give the request every chance to reach the lock and park there.
        let parked = tokio::time::timeout(Duration::from_millis(500), replay.as_mut()).await;
        assert!(
            parked.is_err(),
            "the replay must still be waiting on the held store lock"
        );
        assert_eq!(
            db.live_connections(),
            0,
            "a replay waiting on the store lock is holding a database connection: {:?}",
            db.overdue_holders(Duration::ZERO, Duration::ZERO)
        );

        held.release().await;
        // Whatever the replay concludes against a store that does not exist,
        // it concludes, and leaves nothing open.
        let _ = tokio::time::timeout(Duration::from_secs(30), replay.as_mut())
            .await
            .expect("the replay finishes once the lock is released");
        assert_eq!(db.live_connections(), 0);
    }

    const HEAD: &str = "0123456789abcdef0123456789abcdef01234567";

    fn dismissal(extra: serde_json::Value) -> serde_json::Value {
        let mut payload = serde_json::json!({
            "action": "dismiss-acceptance",
            "acceptance_id": "acceptance-1",
            "expected_head": HEAD,
            "reason": "  the install was superseded by the owner's own rewrite  ",
        });
        for (key, value) in extra.as_object().unwrap() {
            payload[key] = value.clone();
        }
        payload
    }

    #[test]
    fn a_dismissal_names_the_row_the_full_head_and_a_trimmed_reason() {
        assert_eq!(
            parse_rebase_action(&dismissal(serde_json::json!({}))).unwrap(),
            RebaseAction::DismissAcceptance(Dismissal {
                acceptance_id: "acceptance-1",
                expected_head: HEAD,
                reason: "the install was superseded by the owner's own rewrite",
            })
        );
        let sha256 = "a".repeat(64);
        assert!(
            parse_rebase_action(&dismissal(serde_json::json!({"expected_head": sha256}))).is_ok()
        );
    }

    #[test]
    fn a_dismissal_missing_any_coordinate_is_refused_by_name() {
        for key in DISMISSAL_KEYS {
            let mut payload = dismissal(serde_json::json!({}));
            payload.as_object_mut().unwrap().remove(key);
            let error = parse_rebase_action(&payload).unwrap_err();
            assert!(
                error.contains(&format!("payload.{key} is required")),
                "{error}"
            );
            for blank in ["", "   "] {
                let error =
                    parse_rebase_action(&dismissal(serde_json::json!({ key: blank }))).unwrap_err();
                assert!(
                    error.contains(&format!("payload.{key}")),
                    "{key}={blank:?}: {error}"
                );
            }
        }
    }

    #[test]
    fn a_dismissal_needs_the_literal_full_head_never_a_prefix_or_a_name() {
        let heads: Vec<String> = vec![
            HEAD[..12].to_string(),
            "agent/prep".to_string(),
            "main".to_string(),
            HEAD.to_uppercase(),
            format!("{HEAD}0"),
            format!(" {}", &HEAD[1..]),
        ];
        for head in heads {
            let error =
                parse_rebase_action(&dismissal(serde_json::json!({ "expected_head": head })))
                    .unwrap_err();
            assert!(error.contains("full current commit id"), "{head}: {error}");
        }
    }

    #[test]
    fn a_dismissal_carries_no_replay_or_fast_forward_key() {
        for (key, value) in [
            ("fingerprint", "fp"),
            ("resolution", "take-committed-tip"),
            ("drop_incoming_reason", "moot"),
            ("branch", "base"),
        ] {
            let error =
                parse_rebase_action(&dismissal(serde_json::json!({ key: value }))).unwrap_err();
            assert!(
                error.contains(&format!(
                    "payload.{key} does not apply to action:'dismiss-acceptance'"
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn a_replay_or_fast_forward_carries_no_dismissal_key() {
        for action in ["replay", "fast-forward-from-origin"] {
            for key in DISMISSAL_KEYS {
                let error = parse_rebase_action(&serde_json::json!({
                    "action": action,
                    key: "x",
                }))
                .unwrap_err();
                assert_eq!(
                    error,
                    format!("payload.{key} applies only to action:'dismiss-acceptance'")
                );
            }
        }
    }

    #[test]
    fn an_overlong_reason_is_refused() {
        let reason = "x".repeat(MAX_DISMISSAL_REASON_CHARS + 1);
        let error =
            parse_rebase_action(&dismissal(serde_json::json!({ "reason": reason }))).unwrap_err();
        assert!(error.contains("payload.reason is longer than"), "{error}");
    }

    #[test]
    fn the_replay_and_fast_forward_shapes_are_unchanged() {
        assert_eq!(
            parse_rebase_action(&serde_json::json!({
                "action": "replay",
                "fingerprint": "fp",
                "resolution": "take-committed-tip",
                "drop_incoming_reason": "moot",
            }))
            .unwrap(),
            RebaseAction::Replay {
                fingerprint: Some("fp"),
                take_committed_tip: true,
                drop_incoming_reason: Some("moot"),
            }
        );
        assert_eq!(
            parse_rebase_action(
                &serde_json::json!({"action": "fast-forward-from-origin", "branch": "base"})
            )
            .unwrap(),
            RebaseAction::FastForwardFromOrigin { base: true }
        );
        for (payload, expected) in [
            (serde_json::json!({"action": "undo"}), "payload.action must be 'replay', 'fast-forward-from-origin', 'repair', 'rebuild', 'catch-up-mode', 'mark-reviewed' or 'dismiss-acceptance', got 'undo'"),
            (serde_json::json!({"action": "replay", "branch": "base"}), "payload.branch applies only to fast-forward-from-origin"),
            (serde_json::json!({"action": "replay", "resolution": "ours"}), "payload.resolution must be 'take-committed-tip' when provided"),
        ] {
            assert_eq!(parse_rebase_action(&payload).unwrap_err(), expected);
        }
        assert!(parse_rebase_action(
            &serde_json::json!({"action": "replay", "drop_incoming_reason": "moot"})
        )
        .unwrap_err()
        .contains("a plain replay drops nothing"));
    }

    #[test]
    fn a_repair_carries_no_other_action_key() {
        assert_eq!(
            parse_rebase_action(&serde_json::json!({"action": "repair"})).unwrap(),
            RebaseAction::Repair
        );
        for key in REPLAY_AND_FAST_FORWARD_KEYS {
            let error = parse_rebase_action(&serde_json::json!({"action": "repair", key: "base"}))
                .unwrap_err();
            assert!(
                error.contains(&format!("payload.{key} does not apply to action:'repair'")),
                "{error}"
            );
        }
        for key in DISMISSAL_KEYS {
            assert_eq!(
                parse_rebase_action(&serde_json::json!({"action": "repair", key: "x"}))
                    .unwrap_err(),
                format!("payload.{key} applies only to action:'dismiss-acceptance'")
            );
        }
    }

    fn debt(id: &str, status: &str) -> AcceptanceDebt {
        AcceptanceDebt {
            id: id.to_string(),
            status: status.to_string(),
            head_before: "b".repeat(40),
            head_after: "c".repeat(40),
            bracket_id: None,
            bracket_bookmark: None,
            bracket_git_ref: None,
        }
    }

    #[test]
    fn the_page_shows_no_debt_section_when_nothing_is_owed() {
        assert_eq!(acceptance_debt_section(&Ok(Vec::new()), Some(HEAD)), "");
    }

    #[test]
    fn an_unreadable_journal_is_unknown_debt_never_none() {
        let section = acceptance_debt_section(&Err("database is locked".to_string()), Some(HEAD));
        assert!(
            section.contains("is unknown") && section.contains("database is locked"),
            "{section}"
        );
        assert!(!section.contains("dismiss-acceptance"), "{section}");
    }

    #[test]
    fn an_unknown_row_offers_the_exact_dismissal_and_warns_it_discards_claims() {
        let section = acceptance_debt_section(&Ok(vec![debt("acc-u", "unknown")]), Some(HEAD));
        assert!(
            section.contains(&format!(
                "payload:{{action:\"dismiss-acceptance\",acceptance_id:\"acc-u\",expected_head:\"{HEAD}\",reason:\"...\"}}"
            )),
            "{section}"
        );
        assert!(
            section.contains(&format!("Current head: `{HEAD}`")),
            "{section}"
        );
        assert!(section.contains(&"b".repeat(40)) && section.contains(&"c".repeat(40)));
        assert!(
            section.contains("never accepts it")
                && section.contains("discarded")
                && section.contains("may still refuse"),
            "{section}"
        );
    }

    #[test]
    fn an_unreadable_head_offers_no_guessed_expected_head() {
        let section = acceptance_debt_section(&Ok(vec![debt("acc-u", "unknown")]), None);
        assert!(!section.contains("expected_head:"), "{section}");
        assert!(section.contains("re-read this page"), "{section}");
    }

    #[test]
    fn a_pending_row_settles_and_is_never_offered_a_dismissal() {
        let section = acceptance_debt_section(&Ok(vec![debt("acc-p", "pending")]), Some(HEAD));
        assert!(section.contains("`acc-p` **pending**"), "{section}");
        assert!(section.contains("cannot be dismissed"), "{section}");
        assert!(!section.contains("dismiss-acceptance"), "{section}");
    }
}
