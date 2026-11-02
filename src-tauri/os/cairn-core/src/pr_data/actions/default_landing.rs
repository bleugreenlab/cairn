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
use super::human_approval::{
    self, approval_strategy, gate_checks, Ceremony, Execution, LandedShape, LandingEvidence,
    LandingWorld, Observation, SystemClock,
};
use super::merge::MergeAttemptJournal;
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
    /// Empty commits dropped from the branch before it was published, as
    /// `<short id> "<subject>"`.
    pub dropped: Vec<String>,
}

/// What one landing attempt put on the default branch.
struct Landed {
    commit: Option<String>,
    dropped: Vec<String>,
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

        validate_prospect(&store, merge_context, &prospect).await?;

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
            Some(Landed { commit, dropped }) => {
                return Ok(DefaultLanding {
                    landed_commit: commit,
                    plan: prospect.plan,
                    dropped,
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

/// Validate a prospect off the store lock: every changed file parses, and
/// every bound identifier holds its invariant.
async fn validate_prospect(
    store: &Store,
    merge_context: &MergeMrContext,
    prospect: &Prospect,
) -> Result<(), String> {
    let (jj, path) = (store.jj.clone(), store.path.clone());
    let (head, tip) = (
        prospect.merge.commit_id.clone(),
        prospect.merge.target_tip.clone(),
    );
    let (target, source) = (
        merge_context.target_branch.clone(),
        merge_context.source_branch.clone(),
    );
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
        dropped: Vec::new(),
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
) -> Result<Option<Landed>, String> {
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
        return Ok(Some(Landed {
            commit: Some(commit),
            dropped: Vec::new(),
        }));
    }

    let (head, dropped) = if prospect.binds {
        // Publishing drops any empty commit the replay left, which can move the
        // tip to another commit with the same tree. GitHub is bound to the
        // head origin now holds.
        let published = publish_landing_source(
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
        (published.head, published.dropped)
    } else {
        (head, Vec::new())
    };
    // From here origin holds the bound head. A refused GitHub merge leaves the
    // branch bound, and the next landing binds it again against the tip it
    // then finds: a no-op when nothing landed in between.
    let commit = github_merge(orch, merge_context, method, &head).await?;
    Ok(Some(Landed { commit, dropped }))
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

// ─── The approval-gated landing ────────────────────────────────────────────────────────

/// What an approval-gated landing needs beyond an ordinary one.
pub(super) struct ApprovalGate<'a> {
    pub db: &'a LocalDb,
    pub enrollment_id: &'a str,
    pub description: Option<String>,
    pub attempt: MergeAttemptJournal<'a>,
}

/// Land on the default branch only on the selected phone's approval of exactly
/// this act. The full prospect loop always runs (never the unbound GitHub
/// shortcut), but once: an approval binds one act, so a moved tip ends the
/// attempt rather than retrying it. Returns a notice when the landing could
/// not yet be proven to the approvals service.
pub(super) async fn land_on_default_with_approval(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    via_github: bool,
    gate: ApprovalGate<'_>,
) -> Result<(DefaultLanding, Option<String>), String> {
    let journal = human_approval::AttemptJournal::new(gate.db, gate.attempt, 1);
    let credential = match crate::api::runner_credential() {
        Ok(credential) => credential,
        Err(error) => {
            journal
                .append(
                    cairn_common::approvals::ApprovalOutcome::PolicyUnavailable,
                    super::merge::JournalEntry {
                        detail: Some(error.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            return Err(format!(
                "Refusing to merge into the project base branch: the approval phone cannot be \
                 asked because this runner has no approvals credential ({error})."
            ));
        }
    };
    let client = crate::approvals_client::ApprovalsClient::new(
        &*orch.services.http,
        orch.approval_keys.clone(),
        orch.runtime_admission_api_config(),
        credential,
    );
    let mut world = match GatedLanding::new(orch, job_id, merge_context, method, via_github) {
        Ok(world) => world,
        Err(refusal) => {
            journal
                .append(
                    cairn_common::approvals::ApprovalOutcome::Failed,
                    super::merge::JournalEntry {
                        detail: Some(refusal.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            return Err(refusal);
        }
    };
    let ceremony = Ceremony {
        client: &client,
        journal: &journal,
        clock: &SystemClock,
        enrollment_id: gate.enrollment_id,
        project_id: &merge_context.project_id,
        attempt_id: gate.attempt.attempt_id,
        job_id,
        title: Some(merge_context.title.clone()),
        description: gate.description,
    };
    let approved = ceremony.run(&mut world).await?;
    let notice = (!approved.proven).then(|| {
        "The merge landed, but whether it is exactly the approved merge is not yet proven; the \
         approval stays unsettled until Cairn reconciles it."
            .to_string()
    });
    Ok((approved.landing, notice))
}

/// The default-branch landing as the approval ceremony drives it.
struct GatedLanding<'a> {
    orch: &'a Orchestrator,
    job_id: &'a str,
    merge_context: &'a MergeMrContext,
    method: &'a str,
    via_github: bool,
    store: Store,
    owner: String,
    repo: String,
    prospect: Option<Prospect>,
}

impl<'a> GatedLanding<'a> {
    fn new(
        orch: &'a Orchestrator,
        job_id: &'a str,
        merge_context: &'a MergeMrContext,
        method: &'a str,
        via_github: bool,
    ) -> Result<Self, String> {
        let (owner, repo) = get_owner_repo(&merge_context.mr.repo_path).map_err(|error| {
            format!(
                "Refusing to merge into the project base branch: approval needs the project's \
                 GitHub repository, and it could not be resolved ({error})."
            )
        })?;
        Ok(Self {
            orch,
            job_id,
            merge_context,
            method,
            via_github,
            store: store_of(orch, merge_context),
            owner,
            repo,
            prospect: None,
        })
    }

    fn target_revision(&self) -> String {
        if self.merge_context.mr.is_local {
            self.merge_context.target_branch.clone()
        } else {
            format!("{}@origin", self.merge_context.target_branch)
        }
    }
}

fn jj_log(store: &Store, revset: &str, template: &str, label: &str) -> Result<String, String> {
    store.jj.run(
        &store.path,
        &[
            "log",
            "-r",
            revset,
            "--no-graph",
            "-T",
            template,
            "--ignore-working-copy",
        ],
        label,
    )
}

fn store_commit_shape(store: &Store, commit: &str) -> Result<LandedShape, String> {
    let parents = jj_log(
        store,
        commit,
        "parents.map(|c| c.commit_id()).join(\" \")",
        "jj log parents",
    )?;
    Ok(LandedShape {
        parents: parents.split_whitespace().map(str::to_string).collect(),
        tree: crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, commit)?,
    })
}

/// A GitHub merge error the provider answered definitively: it did not merge.
fn github_refused_definitively(error: &str) -> bool {
    error.contains("GitHub refused")
        || (error.contains("Failed to merge PR: 4") && !error.contains("already in progress"))
}

impl LandingWorld for GatedLanding<'_> {
    type Landing = DefaultLanding;

    fn route(&self) -> &'static str {
        if self.via_github {
            "github"
        } else {
            "store"
        }
    }

    async fn observe(&mut self) -> Result<Observation, String> {
        let source = self.merge_context.source_branch.as_str();
        let prospect = {
            let _guard = self
                .orch
                .acquire_jj_store_lock(
                    &self.store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("approval prospect for {source}"),
                )
                .await;
            build_prospect(self.orch, self.merge_context, self.method, &self.store).await?
        };
        validate_prospect(&self.store, self.merge_context, &prospect).await?;
        let strategy = approval_strategy(self.method, self.via_github)?;
        // Everything below runs with no store lock held.
        let auth = installation_authority(&self.orch.db.local, &self.owner).await?;
        let repository =
            api::fetch_repository(&*self.orch.services.http, &auth, &self.owner, &self.repo)
                .await
                .map_err(|error| format!("the repository could not be read: {error}"))?;
        let (source_tip, target_tip) = (
            prospect.merge.source_tip.clone(),
            prospect.merge.target_tip.clone(),
        );
        let source_message = jj_log(
            &self.store,
            &source_tip,
            "description",
            "jj log description",
        )?;
        let count = jj_log(
            &self.store,
            &format!("{target_tip}..{source_tip}"),
            "commit_id ++ \"\\n\"",
            "jj log source range",
        )?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
        let source_commit_count = u32::try_from(count)
            .ok()
            .filter(|count| *count >= 1)
            .ok_or_else(|| {
                format!("`{source}` has no commits to merge that the base does not already hold")
            })?;
        let merge_base_oid = crate::jj::revset_commit_checked(
            &self.store.jj,
            &self.store.path,
            &format!("heads(::{target_tip} & ::{source_tip})"),
        )?;
        let statuses =
            crate::execution::checks_status::node_check_statuses(self.orch, self.job_id).await;
        let observation = Observation {
            repository_id: repository.id.to_string(),
            repository_name: repository.full_name,
            target_ref: format!("refs/heads/{}", self.merge_context.target_branch),
            target_oid: target_tip,
            source_ref: format!("refs/heads/{source}"),
            source_oid: source_tip.clone(),
            source_message,
            source_commit_count,
            merge_base_oid,
            // Only an unbound GitHub landing merges a commit that exists now;
            // a fold or a bound head is minted at landing, with fresh dates.
            candidate_oid: (self.via_github && !prospect.binds).then(|| source_tip.clone()),
            result_tree_oid: prospect.merge.tree_hash.clone(),
            strategy,
            // `after_github_merge` deletes the merged branch on GitHub; the
            // store fold leaves the branch to worktree teardown.
            deletes_branch: self.via_github,
            checks: gate_checks(statuses.as_deref(), &source_tip),
        };
        self.prospect = Some(prospect);
        Ok(observation)
    }

    async fn execute(&mut self, approved: &Observation) -> Execution<DefaultLanding> {
        let Some(prospect) = self.prospect.take() else {
            return Execution::Refused("the landing was never observed".to_string());
        };
        let (source, target) = (
            self.merge_context.source_branch.as_str(),
            self.merge_context.target_branch.as_str(),
        );
        let store = &self.store;
        // ── The final comparison, local only, under the store lock ──
        let head = {
            let guard = self
                .orch
                .acquire_jj_store_lock(
                    &store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("approved landing {source} on {target}"),
                )
                .await;
            let _phase = guard.phase("compare approved state, bind and land");
            let source_now = crate::jj::bookmark_commit(&store.jj, &store.path, source);
            let target_now =
                crate::jj::revset_commit(&store.jj, &store.path, &self.target_revision());
            if source_now.as_deref() != Some(approved.source_oid.as_str())
                || target_now.as_deref() != Some(approved.target_oid.as_str())
            {
                return Execution::BoundStateChanged {
                    observed: serde_json::json!({
                        "sourceOid": source_now,
                        "targetOid": target_now,
                    }),
                };
            }
            let operation = match crate::jj::operation_id(&store.jj, &store.path) {
                Ok(operation) => operation,
                Err(error) => return Execution::Refused(error),
            };
            let restore = || crate::jj::restore_operation(&store.jj, &store.path, &operation);
            let head = if prospect.binds {
                let bound = match replay_and_bind(
                    store,
                    self.merge_context,
                    &approved.target_oid,
                    &prospect.rules,
                ) {
                    Ok(bound) => bound,
                    Err(error) => {
                        let _ = restore();
                        return Execution::Refused(error);
                    }
                };
                let tree = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head);
                if tree.as_deref() != Ok(approved.result_tree_oid.as_str()) {
                    let _ = restore();
                    return Execution::BoundStateChanged {
                        observed: serde_json::json!({ "resultTreeOid": tree.ok() }),
                    };
                }
                bound.head
            } else {
                approved.source_oid.clone()
            };
            if !self.via_github {
                // The store fold is the route's mutation, and it runs under
                // the lock exactly as an ungated landing's does.
                return match land_validated_default(
                    self.orch,
                    self.merge_context,
                    self.method,
                    &approved.result_tree_oid,
                )
                .await
                {
                    Ok(commit) => Execution::Landed {
                        commit: commit.clone(),
                        approved_head: head,
                        landing: DefaultLanding {
                            landed_commit: Some(commit),
                            plan: prospect.plan,
                            dropped: Vec::new(),
                        },
                    },
                    Err(error) => {
                        let _ = restore();
                        let unchanged = crate::jj::revset_commit(
                            &store.jj,
                            &store.path,
                            &self.target_revision(),
                        )
                        .as_deref()
                            == Some(approved.target_oid.as_str())
                            && crate::jj::bookmark_commit(&store.jj, &store.path, target)
                                .as_deref()
                                == Some(approved.target_oid.as_str());
                        if unchanged {
                            Execution::Refused(error)
                        } else {
                            Execution::Unknown {
                                reason: error,
                                approved_head: Some(head),
                            }
                        }
                    }
                };
            }
            head
        };
        // ── GitHub: no lock held; GitHub's `sha` binds the head ──
        // Publishing drops any empty commit the replay left, which can move the
        // tip to another commit with the approved tree; GitHub is bound to the
        // head origin then holds.
        let (head, dropped) = if prospect.binds {
            match publish_landing_source(&store.jj, &store.path, target, source) {
                Ok(published) => (published.head, published.dropped),
                Err(error) => {
                    return Execution::Refused(format!(
                        "Refusing to merge `{source}`: its bound head could not be pushed to \
                         origin ({error}). Nothing was merged; merge again."
                    ))
                }
            }
        } else {
            (head, Vec::new())
        };
        match github_merge(self.orch, self.merge_context, self.method, &head).await {
            Ok(Some(commit)) => Execution::Landed {
                commit: commit.clone(),
                approved_head: head,
                landing: DefaultLanding {
                    landed_commit: Some(commit),
                    plan: prospect.plan,
                    dropped,
                },
            },
            Ok(None) => Execution::Unknown {
                reason: "GitHub did not name the merge commit".to_string(),
                approved_head: Some(head),
            },
            Err(error) if github_refused_definitively(&error) => Execution::Refused(error),
            Err(error) => Execution::Unknown {
                reason: error,
                approved_head: Some(head),
            },
        }
    }

    async fn landed_shape(&self, commit: &str) -> Result<LandedShape, String> {
        if !self.via_github {
            return store_commit_shape(&self.store, commit);
        }
        let auth = installation_authority(&self.orch.db.local, &self.owner).await?;
        let shape = api::fetch_commit_shape(
            &*self.orch.services.http,
            &auth,
            &self.owner,
            &self.repo,
            commit,
        )
        .await?;
        Ok(LandedShape {
            parents: shape.parents,
            tree: shape.tree,
        })
    }
}

/// Reconcile every approved merge whose outcome was never settled: prove it
/// landed, find definitive evidence it did not, or leave it `indeterminate`.
/// Runs at startup and on a slow timer; an attempt still running in this
/// process is left to the call driving it.
pub async fn reconcile_approved_merges(orch: &Orchestrator) {
    let db = &orch.db.local;
    let attempts = match human_approval::unsettled_attempts(db).await {
        Ok(attempts) => attempts,
        Err(error) => {
            log::warn!("approval reconciliation: {error}");
            return;
        }
    };
    if attempts.is_empty() {
        return;
    }
    let credential = match crate::api::runner_credential() {
        Ok(credential) => credential,
        Err(error) => {
            log::warn!(
                "approval reconciliation: {} unsettled approved merge(s) wait: {error}",
                attempts.len()
            );
            return;
        }
    };
    let client = crate::approvals_client::ApprovalsClient::new(
        &*orch.services.http,
        orch.approval_keys.clone(),
        orch.runtime_admission_api_config(),
        credential,
    );
    for attempt in attempts {
        let evidence = landing_evidence(orch, &attempt).await;
        match human_approval::settle_attempt(db, &client, &attempt, &evidence).await {
            Ok(Some(outcome)) => log::info!(
                "approval reconciliation: merge attempt {} settled as {outcome:?}",
                attempt.attempt_id
            ),
            Ok(None) => {}
            Err(error) => log::warn!(
                "approval reconciliation: merge attempt {}: {error}",
                attempt.attempt_id
            ),
        }
    }
}

async fn landing_evidence(
    orch: &Orchestrator,
    attempt: &human_approval::UnsettledAttempt,
) -> LandingEvidence {
    if attempt.mutation_issued == Some(false) {
        return LandingEvidence::DefinitelyNotLanded(
            "the attempt recorded that no mutation was issued".to_string(),
        );
    }
    let context =
        match super::context::resolve_merge_mr_context_for_job(&orch.db.local, &attempt.job_id)
            .await
        {
            Ok(context) => context,
            Err(error) => return LandingEvidence::Unknown(error),
        };
    if attempt.route == "github" {
        let Some(pr_number) = context.mr.github_pr_number else {
            return LandingEvidence::Unknown("the pull request has no number".to_string());
        };
        let Ok((owner, repo)) = get_owner_repo(&context.mr.repo_path) else {
            return LandingEvidence::Unknown("the repository is unresolvable".to_string());
        };
        let auth = match installation_authority(&orch.db.local, &owner).await {
            Ok(auth) => auth,
            Err(error) => return LandingEvidence::Unknown(error),
        };
        let http = &*orch.services.http;
        let state = match api::fetch_pr_merge_state(http, &auth, &owner, &repo, pr_number).await {
            Ok(state) => state,
            Err(error) => return LandingEvidence::Unknown(error),
        };
        let pr = human_approval::PullRequestState {
            merged: state.merged,
            closed: state.state == "closed",
            head: Some(state.head.sha),
            merge_commit: state.merge_commit_sha,
        };
        let commit = match human_approval::github_attribution(&pr, attempt.bound_head()) {
            Ok(commit) => commit,
            Err(evidence) => return evidence,
        };
        return match api::fetch_commit_shape(http, &auth, &owner, &repo, &commit).await {
            Ok(shape) => LandingEvidence::Landed {
                commit,
                shape: LandedShape {
                    parents: shape.parents,
                    tree: shape.tree,
                },
            },
            Err(error) => LandingEvidence::Unknown(error),
        };
    }
    // The store fold runs in this process under the store lock and its jj
    // operation is atomic, so once no call is driving it the target either
    // advanced with this attempt's fold or did not.
    let store = store_of(orch, &context);
    let Some(tip) = crate::jj::bookmark_commit(&store.jj, &store.path, &context.target_branch)
    else {
        return LandingEvidence::Unknown("the target bookmark is unreadable".to_string());
    };
    human_approval::store_evidence(&tip, &attempt.intent, |commit| {
        store_commit_shape(&store, commit)
    })
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
