//! Landing a pull request on its project's default branch.
//!
//! Every default-branch landing runs one loop, and the store lock is held only
//! where something must not move:
//!
//! 1. **Prospect**, under the lock, then undone: the merge result as it would
//!    land on the live default tip. A branch that changed a declared identifier
//!    site (`landing.identifiers`) is replayed onto the tip and bound there
//!    (`orchestrator::landing::bind`); any other branch is folded prospectively
//!    as before.
//! 2. **Validate**, off the lock: every file the landing changes still parses,
//!    and every bound identifier holds its invariant (no NEXT, the next numbers
//!    and no others, at most one new protocol version).
//! 3. **Land if unchanged**, under the lock: if either tip moved, go round
//!    again rather than hold the lock; otherwise redo the replay and bind for
//!    real, prove the tree is the one validated, and land it, by the store fold
//!    or through GitHub with the validated head as `sha`.
//!
//! A GitHub landing with nothing to bind skips the loop: it is judged on its
//! branch head and merged as it always was, so an ordinary pull request pays no
//! fetch and no prospective build.
//!
//! Holding the lock from the recheck through the landing is what serializes
//! binding: a second landing that bound against the same tip finds it moved
//! and binds again against the first one's result.
//!
//! What this does not serialize is a merge made outside Cairn (GitHub's web
//! UI) between a GitHub-route landing's recheck and GitHub performing its
//! merge. Such a merge can land a number this landing also bound; the backstop
//! is `check:migrations` and `check:executor-protocol` on the default branch,
//! which refuse a duplicate number and an unbound NEXT.

use std::path::{Path, PathBuf};

use crate::config::project_settings::IdentifierRule;
use crate::github::api;
use crate::github::credentials::get_owner_repo;
use crate::orchestrator::landing::{self, bind};
use crate::orchestrator::Orchestrator;
use crate::security::broker::github::installation_authority;
use crate::storage::LocalDb;

use super::context::MergeMrContext;
use super::store_merge::{
    land_validated_default, live_default_landing_tips, prospective_default_landing,
    publish_landing_source, ProspectiveMerge,
};

/// How many times the loop builds and validates a merge result because a tip
/// moved under it, before it gives the merge back to its caller.
const MAX_LANDING_ATTEMPTS: usize = 4;

/// What landed, and what it allocated.
#[derive(Debug)]
pub(super) struct DefaultLanding {
    /// The commit the default branch now holds: the store fold's commit, or
    /// the merge commit GitHub reported (absent only if GitHub's answer did
    /// not say).
    pub landed_commit: Option<String>,
    /// The identifiers the landing bound.
    pub plan: bind::BindPlan,
}

/// One merge result as it would land, and what it took to build it.
struct Prospect {
    merge: ProspectiveMerge,
    /// Whether the landing replays and binds the branch.
    binds: bool,
    rules: Vec<(String, IdentifierRule)>,
    plan: bind::BindPlan,
}

struct Store {
    jj: crate::jj::JjEnv,
    path: PathBuf,
}

fn store_of(orch: &Orchestrator, merge_context: &MergeMrContext) -> Store {
    Store {
        jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        path: crate::jj::project_store_dir(
            &orch.config_dir,
            Path::new(&merge_context.mr.repo_path),
        ),
    }
}

/// Land `merge_context`'s branch on the default branch. `method` is the
/// landing method as the route resolves it; `via_github` selects the GitHub
/// route. `verified_head` is a pin bump's verified head, which must be exactly
/// what lands.
pub(super) async fn land_on_default(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    via_github: bool,
    verified_head: Option<&str>,
) -> Result<DefaultLanding, String> {
    let source = merge_context.source_branch.as_str();
    let target = merge_context.target_branch.as_str();
    let store = store_of(orch, merge_context);
    if via_github {
        if let Some(landed) =
            github_without_prospect(orch, merge_context, method, &store, verified_head).await?
        {
            return Ok(landed);
        }
    }
    for attempt in 0..MAX_LANDING_ATTEMPTS {
        let prospect = {
            let _guard = orch
                .acquire_jj_store_lock(
                    &store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("landing prospect for {source}"),
                )
                .await;
            build_prospect(orch, merge_context, method, &store).await?
        };
        if let Some(verified) = verified_head {
            if prospect.binds || prospect.merge.source_tip != verified {
                return Err(format!(
                    "Refusing to merge `{source}` as a pin bump: the head that would land is not \
                     the verified head {verified} (the branch moved, or it changes an identifier \
                     the landing binds, which a pin bump never does). Merge again so the new head \
                     is verified."
                ));
            }
        }

        let validated = {
            let (jj, path) = (store.jj.clone(), store.path.clone());
            let (head, tip) = (
                prospect.merge.commit_id.clone(),
                prospect.merge.target_tip.clone(),
            );
            let (target, source) = (target.to_string(), source.to_string());
            let rules = if prospect.binds {
                prospect.rules.clone()
            } else {
                Vec::new()
            };
            tokio::task::spawn_blocking(move || {
                bind::validate_landing(&jj, &path, &head, &tip, &target, &source, &rules)
            })
            .await
            .map_err(|error| format!("landing validation did not finish: {error}"))?
        };
        validated?;

        let landed = {
            let guard = orch
                .acquire_jj_store_lock(
                    &store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("landing {source} on {target}"),
                )
                .await;
            let _phase = guard.phase("recheck, bind and land");
            land_if_unchanged(orch, merge_context, method, via_github, &store, &prospect).await?
        };
        match landed {
            Some(landed_commit) => {
                return Ok(DefaultLanding {
                    landed_commit,
                    plan: prospect.plan,
                })
            }
            None => log::info!(
                "landing[{job_id}]: `{source}` or `{target}` moved while the landing was \
                 validated (attempt {}); building and validating the new merge result",
                attempt + 1
            ),
        }
    }
    Err(format!(
        "Refusing to merge `{source}` into `{target}`: `{target}` or the branch kept moving while \
         the landing was validated, {MAX_LANDING_ATTEMPTS} times. `{target}` was not advanced; merge \
         again."
    ))
}

/// A GitHub landing with nothing to bind needs no prospective merge result,
/// and building one would fetch origin for no question: it is judged on its
/// branch head and merged through GitHub, bound to a pin bump's verified head
/// when there is one, exactly as before the binder existed. `None` when the
/// landing needs the full loop. Whether the branch touched an identifier site
/// is its own change since its fork, which the store's last-seen default tip
/// answers as well as a fetched one.
async fn github_without_prospect(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    store: &Store,
    verified_head: Option<&str>,
) -> Result<Option<DefaultLanding>, String> {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    let (Some(source_tip), Some(target_tip)) = (
        crate::jj::bookmark_commit(&store.jj, &store.path, source),
        crate::jj::bookmark_commit(&store.jj, &store.path, target),
    ) else {
        return Ok(None);
    };
    let rules = landing::identifier_rules(&landing::landing_settings_at(
        &store.jj,
        &store.path,
        &target_tip,
    ))?;
    if bind::touches_identifiers(&store.jj, &store.path, &source_tip, &target_tip, &rules)? {
        return Ok(None);
    }
    let refusal = {
        let (jj, path) = (store.jj.clone(), store.path.clone());
        let (target, source) = (target.to_string(), source.to_string());
        tokio::task::spawn_blocking(move || {
            crate::jj::branch_parse_refusal(&jj, &path, &target, &source)
        })
        .await
        .ok()
        .flatten()
    };
    if let Some(refusal) = refusal {
        return Err(refusal);
    }
    let landed_commit = match verified_head {
        Some(head) => github_merge(orch, merge_context, method, head).await?,
        None => github_merge_unbound(orch, merge_context, method).await?,
    };
    Ok(Some(DefaultLanding {
        landed_commit,
        plan: bind::BindPlan::default(),
    }))
}

/// Replay `source` onto `target_tip` unless it already descends from it, then
/// bind it there. The caller holds the store lock.
fn replay_and_bind(
    store: &Store,
    merge_context: &MergeMrContext,
    target_tip: &str,
    rules: &[(String, IdentifierRule)],
) -> Result<bind::BoundLanding, String> {
    let source = merge_context.source_branch.as_str();
    let descends = crate::jj::revset_commit(
        &store.jj,
        &store.path,
        &format!("{target_tip} & ::bookmarks(exact:{source:?})"),
    )
    .is_some();
    if !descends {
        crate::orchestrator::base_advance::replay_for_landing(
            &store.jj,
            &store.path,
            source,
            target_tip,
            &merge_context.target_branch,
        )?;
    }
    bind::bind_onto(&store.jj, &store.path, source, target_tip, rules)
}

/// The merge result as it would land now, built in the store and undone. The
/// caller holds the store lock.
async fn build_prospect(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    store: &Store,
) -> Result<Prospect, String> {
    let (source_tip, target_tip) = live_default_landing_tips(orch, merge_context).await?;
    let rules = landing::identifier_rules(&landing::landing_settings_at(
        &store.jj,
        &store.path,
        &target_tip,
    ))?;
    let binds =
        bind::touches_identifiers(&store.jj, &store.path, &source_tip, &target_tip, &rules)?;
    if !binds {
        return Ok(Prospect {
            merge: prospective_default_landing(orch, merge_context, method).await?,
            binds,
            rules,
            plan: bind::BindPlan::default(),
        });
    }

    let operation = crate::jj::operation_id(&store.jj, &store.path)?;
    let built = (|| -> Result<Prospect, String> {
        let bound = replay_and_bind(store, merge_context, &target_tip, &rules)?;
        let tree_hash = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head)?;
        let patch = store.jj.run(
            &store.path,
            &[
                "diff",
                "--ignore-working-copy",
                "--git",
                "--from",
                &target_tip,
                "--to",
                &bound.head,
            ],
            "jj diff bound landing",
        )?;
        Ok(Prospect {
            merge: ProspectiveMerge {
                source_tip: source_tip.clone(),
                target_tip: target_tip.clone(),
                tree_entries: crate::jj::tree_entries(&store.jj, &store.path, &bound.head)?,
                commit_id: bound.head,
                tree_hash,
                changed_files: crate::jj::parse_git_diff(&patch),
            },
            binds,
            rules: rules.clone(),
            plan: bound.plan,
        })
    })();
    let restored = crate::jj::restore_operation(&store.jj, &store.path, &operation);
    match (built, restored) {
        (Ok(prospect), Ok(())) => Ok(prospect),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!(
            "failed to restore the prospective landing operation: {error}"
        )),
        (Err(error), Err(restore)) => Err(format!(
            "{error} (also failed to restore the prospective landing operation: {restore})"
        )),
    }
}

/// Land the validated prospect if neither tip moved since it was built: `Some`
/// with what landed, `None` when it went stale. The caller holds the store
/// lock from the recheck through the landing.
async fn land_if_unchanged(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    via_github: bool,
    store: &Store,
    prospect: &Prospect,
) -> Result<Option<Option<String>>, String> {
    let live = live_default_landing_tips(orch, merge_context).await?;
    if live
        != (
            prospect.merge.source_tip.clone(),
            prospect.merge.target_tip.clone(),
        )
    {
        return Ok(None);
    }
    let operation = crate::jj::operation_id(&store.jj, &store.path)?;
    let rollback = |error: String| -> String {
        match crate::jj::restore_operation(&store.jj, &store.path, &operation) {
            Ok(()) => error,
            Err(restore) => format!("{error} (also failed to roll the landing back: {restore})"),
        }
    };
    let head = if prospect.binds {
        let bound = replay_and_bind(
            store,
            merge_context,
            &prospect.merge.target_tip,
            &prospect.rules,
        )
        .map_err(&rollback)?;
        let tree = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head)
            .map_err(&rollback)?;
        if tree != prospect.merge.tree_hash {
            log::warn!(
                "landing: binding `{}` again produced tree {tree}, not the judged {}; judging again",
                merge_context.source_branch,
                prospect.merge.tree_hash
            );
            crate::jj::restore_operation(&store.jj, &store.path, &operation).map_err(|error| {
                format!("failed to roll back a landing whose tree changed: {error}")
            })?;
            return Ok(None);
        }
        bound.head
    } else {
        prospect.merge.source_tip.clone()
    };

    if !via_github {
        let commit = land_validated_default(orch, merge_context, method, &prospect.merge.tree_hash)
            .await
            .map_err(&rollback)?;
        return Ok(Some(Some(commit)));
    }

    if prospect.binds {
        publish_landing_source(
            &store.jj,
            &store.path,
            &merge_context.target_branch,
            &merge_context.source_branch,
        )
        .map_err(|error| {
            rollback(format!(
                "Refusing to merge `{}`: its bound head could not be pushed to origin ({error}). \
                 Nothing was merged; merge again.",
                merge_context.source_branch
            ))
        })?;
    }
    // From here origin holds the bound head. A refused GitHub merge leaves the
    // branch bound, and the next landing binds it again against the tip it
    // then finds: a no-op when nothing landed in between.
    github_merge(orch, merge_context, method, &head)
        .await
        .map(Some)
}

/// Merge the pull request through GitHub, bound to `head`: GitHub refuses if
/// the pull request's head is anything else, so it squashes exactly the tree
/// that was judged.
async fn github_merge(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    head: &str,
) -> Result<Option<String>, String> {
    github_merge_at(orch, merge_context, method, Some(head)).await
}

/// [`github_merge`] for a landing nothing was judged at, so no head binds it.
async fn github_merge_unbound(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
) -> Result<Option<String>, String> {
    github_merge_at(orch, merge_context, method, None).await
}

async fn github_merge_at(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    head: Option<&str>,
) -> Result<Option<String>, String> {
    let pr_number = merge_context
        .mr
        .github_pr_number
        .ok_or_else(|| "GitHub merge requires a PR number".to_string())?;
    let (owner, repo) = get_owner_repo(&merge_context.mr.repo_path)?;
    let auth = installation_authority(&orch.db.local, &owner).await?;
    let started = std::time::Instant::now();
    let merge_commit = api::merge_pr(
        &*orch.services.http,
        &auth,
        &owner,
        &repo,
        pr_number,
        method,
        head,
    )
    .await
    .map_err(|error| {
        super::merge::map_github_merge_error(
            &merge_context.source_branch,
            &merge_context.target_branch,
            error,
        )
    })?;
    // Origin moved the moment GitHub answered. Record it before anything else
    // so a default-branch read between now and the store import says it is
    // behind instead of passing the pre-merge tree off as current.
    if let Some(merge_commit) = merge_commit.as_deref() {
        orch.base_branch_sync.record_tip(
            &merge_context.project_id,
            &merge_context.target_branch,
            merge_commit,
            crate::orchestrator::base_branch_sync::Source::Merge,
        );
    }
    log::info!("landing: GitHub merge_pr took {:?}", started.elapsed());
    Ok(merge_commit)
}

/// Record what a landing bound: one `landing_allocations` row per value, and a
/// comment on the issue saying what the branch landed as. Written only after
/// the landing succeeded; a failure is logged and never undoes the landing,
/// whose commit already says what it bound.
pub(super) async fn record_allocations(
    orch: &Orchestrator,
    db: &LocalDb,
    merge_context: &MergeMrContext,
    landed: &DefaultLanding,
) {
    if landed.plan.allocations.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp();
    for allocation in &landed.plan.allocations {
        let written = orch
            .db
            .local
            .execute(
                "INSERT INTO landing_allocations
                   (id, project_id, kind, value, name, provisional, issue_id, branch,
                    landed_commit, allocated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                cairn_db::turso::params![
                    uuid::Uuid::new_v4().to_string(),
                    merge_context.project_id.as_str(),
                    allocation.kind.as_str(),
                    i64::try_from(allocation.value).unwrap_or(i64::MAX),
                    allocation.name.as_str(),
                    allocation.provisional.as_str(),
                    merge_context.issue_id.as_deref(),
                    merge_context.source_branch.as_str(),
                    landed.landed_commit.as_deref(),
                    now,
                ],
            )
            .await;
        if let Err(error) = written {
            log::warn!(
                "landing: `{}` landed as {} {}, but the allocation was not recorded: {error}",
                merge_context.source_branch,
                allocation.kind,
                allocation.name
            );
        }
    }
    let Some(issue_id) = merge_context.issue_id.clone() else {
        return;
    };
    let content = format!("Landed as {}.", landed.plan.summary());
    let commented = db
        .write(move |conn| {
            let (issue_id, content) = (issue_id.clone(), content.clone());
            Box::pin(async move {
                crate::issues::comments::insert_conn(
                    conn,
                    &cairn_common::ids::mint_child(&issue_id),
                    &issue_id,
                    &content,
                    "agent",
                    now,
                )
                .await
                .map(|_| ())
            })
        })
        .await;
    match commented {
        Ok(()) => {
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "comments", "action": "insert"}),
            );
        }
        Err(error) => log::warn!(
            "landing: `{}` landed as {}, but the issue comment was not written: {error}",
            merge_context.source_branch,
            landed.plan.summary()
        ),
    }
}
