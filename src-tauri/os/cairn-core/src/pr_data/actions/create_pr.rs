//! Sync an edited `create-pr` artifact's prose into the PR it already produced.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

use super::context::{db_error, query_mr_context_for_create_pr_artifact_job};

/// Update the local merge-request prose cache after a create-pr artifact edit
/// has become the PR's source of truth.
async fn update_merge_request_title_body(
    db: &LocalDb,
    mr_id: &str,
    title: &str,
    body: Option<&str>,
    now: i64,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let title = title.to_string();
    let body = body.map(ToOwned::to_owned);
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let title = title.clone();
        let body = body.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET title = ?1, body = ?2, updated_at = ?3
                 WHERE id = ?4",
                params![title.as_str(), body.as_deref(), now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request title/body", e))
}

fn gh_pr_edit_args<'a>(pr_number: &'a str, title: &'a str, body: &'a str) -> [&'a str; 7] {
    ["pr", "edit", pr_number, "--title", title, "--body", body]
}

async fn edit_github_pr_title_body(
    repo_path: &str,
    pr_number: i32,
    title: &str,
    body: Option<&str>,
) -> Result<(), String> {
    let pr_number = pr_number.to_string();
    let body = body.unwrap_or("");
    let output = crate::env::gh()
        .args(gh_pr_edit_args(&pr_number, title, body))
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr edit: {e}"))?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "Failed to update GitHub PR #{pr_number}: {}",
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// The branch a merge-request row publishes from.
async fn source_branch_for_merge_request(db: &LocalDb, mr_id: &str) -> Result<String, String> {
    db.query_text(
        "SELECT source_branch FROM merge_requests WHERE id = ?1",
        (mr_id.to_string(),),
    )
    .await
    .map_err(|error| format!("Failed to resolve merge request source branch: {error}"))?
    .filter(|branch| !branch.trim().is_empty())
    .ok_or_else(|| format!("merge request {mr_id} has no source branch to publish"))
}

/// Resolve the branch a merge request publishes from and push it through the
/// recovering wrapper every managed push uses. Returns the branch published;
/// the error names the branch.
async fn publish_merge_request_branch(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Result<String, String> {
    let branch = source_branch_for_merge_request(&orch.db.local, mr_id).await?;
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(repo_path));
    crate::orchestrator::base_advance::publish_managed_branch(orch, &store, &branch)
        .await
        .map_err(|error| format!("could not publish `{branch}`: {error}"))?;
    Ok(branch)
}

/// Publish the branch behind an already-open PR before its prose is re-synced.
///
/// Re-writing a `create-pr` artifact is the sanctioned trigger for publishing a
/// builder's branch, and the affordance docs tell agents so. Without this the
/// artifact edit moved only the PR's title and body: a builder that pushed
/// review fixes and re-saved its artifact left the PR presenting the pre-fix
/// tree as its reviewable, mergeable version, and a coordinator merging on first
/// sight of the artifact ships code the review already invalidated.
///
/// Fails closed. This is external state other people and agents act on, so
/// "could not publish" must not be reported as a completed sync.
async fn publish_branch_for_artifact_sync(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Result<(), String> {
    publish_merge_request_branch(orch, repo_path, mr_id)
        .await
        .map(|_| ())
        .map_err(|error| {
            format!(
                "Failed to publish before syncing the create-pr artifact: {error}. The pull \
                 request still presents its previous head, so the artifact was not synced."
            )
        })
}

/// What a failed `action:"publish"` leaves to do. A convergence refusal means
/// origin holds history this branch does not share, which Cairn will not
/// overwrite on its own; anything else is a cause the reply has just named.
fn publish_failure_remedy(error: &str) -> &'static str {
    if error.contains("stale publication recovery refused") {
        "GitHub holds a copy of this branch whose history this branch does not share, so Cairn \
         will not overwrite it. A person needs to decide which history the pull request should \
         carry."
    } else {
        "The pull request still shows its previous head. Patch this artifact with \
         action:\"publish\" again once the cause above is cleared."
    }
}

/// How a node's pull request was resolved. A resolved pull request's create-pr
/// artifact is sealed: it is the record of what merged (or closed), so nothing
/// writes a new version of it or pushes its prose or branch again. A reopened
/// pull request stays sealed; its follow-up is a new execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sealed {
    Merged,
    Closed,
}

impl Sealed {
    /// The seal a row's recorded status and last-fetched GitHub state imply.
    /// Either side saying merged is a merge; GitHub can merge a pull request
    /// the row recorded closed.
    fn of(status: Option<&str>, github_state: Option<&str>) -> Option<Self> {
        let github = github_state.map(str::to_ascii_lowercase);
        if status == Some("merged") || github.as_deref() == Some("merged") {
            Some(Self::Merged)
        } else if status == Some("closed") || github.as_deref() == Some("closed") {
            Some(Self::Closed)
        } else {
            None
        }
    }

    fn from_state(state: &crate::models::PrState) -> Option<Self> {
        match state {
            crate::models::PrState::Merged => Some(Self::Merged),
            crate::models::PrState::Closed => Some(Self::Closed),
            crate::models::PrState::Open | crate::models::PrState::Unpublished => None,
        }
    }
}

/// The merge request's recorded lifecycle, read before anything is written,
/// pushed or edited.
async fn recorded_seal(db: &LocalDb, mr_id: &str) -> Result<Option<Sealed>, String> {
    db.query_opt(
        "SELECT status, github_state FROM merge_requests WHERE id = ?1",
        (mr_id.to_string(),),
        |row| Ok((row.opt_text(0)?, row.opt_text(1)?)),
    )
    .await
    .map_err(|error| db_error("Failed to read the merge request's state", error))
    .map(|row| row.and_then(|(status, github)| Sealed::of(status.as_deref(), github.as_deref())))
}

/// `cairn://p/KEY/N` for a node URI `cairn://p/KEY/N/EXEC/NODE`.
fn issue_uri_of(node_uri: &str) -> Option<String> {
    let rest = node_uri.strip_prefix("cairn://p/")?;
    let mut parts = rest.splitn(3, '/');
    let (key, number) = (parts.next()?, parts.next()?);
    (!key.is_empty() && !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()))
        .then(|| format!("cairn://p/{key}/{number}"))
}

/// What a gated caller was about to do, which its refusal says did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatedAct {
    /// Store a new version of the create-pr artifact.
    NewVersion,
    /// Push the pull request's branch.
    Publish,
    /// Edit the pull request's title and body from a version already stored.
    ProseSync,
}

impl GatedAct {
    fn consequence(self) -> &'static str {
        match self {
            Self::NewVersion => "Nothing was written.",
            Self::Publish => "The branch was not published.",
            Self::ProseSync => "The pull request was not edited.",
        }
    }
}

/// The one refusal of a write to a sealed create-pr artifact. It names the
/// route a follow-up takes, because a second pull request from the node whose
/// pull request already resolved is exactly what the seal exists to stop.
pub(crate) fn sealed_refusal(
    node_uri: Option<&str>,
    pr_number: Option<i32>,
    sealed: Sealed,
    act: GatedAct,
) -> String {
    let subject = match pr_number {
        Some(number) => format!("PR #{number}"),
        None => "change".to_string(),
    };
    let outcome = match sealed {
        Sealed::Merged => "merged",
        Sealed::Closed => "closed without merging",
    };
    let issue = node_uri.and_then(|uri| Some((uri, issue_uri_of(uri)?)));
    let (executions, continuity) = match issue {
        Some((node, issue)) => (format!("{issue}/executions"), node.to_string()),
        None => (
            "cairn://p/PROJECT/NUMBER/executions".to_string(),
            "<this node's URI>".to_string(),
        ),
    };
    let consequence = act.consequence();
    format!(
        "This node's {subject} {outcome}. Follow-up work goes in a new execution on this issue \
         (`{executions}` with `continuity`), or a new issue. {consequence}\n\
         e.g. write({{changes:[{{target:\"{executions}\",mode:\"append\",payload:{{continuity:\"{continuity}\"}}}}]}})"
    )
}

/// The one gate on every write of a create-pr artifact: a new version, a
/// publish of its branch, or a sync of its prose into the pull request.
///
/// The recorded state answers first and costs no network call. A pull request
/// the record calls open is then read live from GitHub when `live` is set,
/// unconditionally: `github_fetched_at` dates the last fetch that changed
/// something, not the last look, so no stored timestamp can say the record is
/// fresh. A failed read refuses, because a stale "open" is how a merged pull
/// request's record came to be rewritten. `live` is unset only where network
/// I/O must not run (under a store lock); the deferred sync re-gates live.
///
/// A job with no merge request yet is writable: its first pull request has not
/// opened. `act` is what the caller was about to do, which a refusal says did
/// not happen.
pub(crate) async fn ensure_pr_artifact_writable(
    orch: &Orchestrator,
    job_id: &str,
    live: bool,
    act: GatedAct,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let owned_job_id = job_id.to_string();
    let Some(mr) = db
        .read(|conn| {
            let job_id = owned_job_id.clone();
            Box::pin(
                async move { query_mr_context_for_create_pr_artifact_job(conn, &job_id).await },
            )
        })
        .await
        .map_err(|e| db_error("Failed to resolve merge request", e))?
    else {
        return Ok(());
    };
    if let Some(sealed) = recorded_seal(&db, &mr.mr_id).await? {
        return Err(refusal_for_job(&db, job_id, mr.github_pr_number, sealed, act).await);
    }
    let Some(pr_number) = mr.github_pr_number.filter(|_| live && !mr.is_local) else {
        return Ok(());
    };
    // The live read goes through the GitHub App even though the prose edit runs
    // through `gh`. A missing installation is fixed in GitHub's settings, where
    // retrying does nothing, so it is told apart before the read is attempted.
    let owner = crate::github::credentials::get_owner_repo_for_pr(&mr.pr_url, &mr.repo_path)
        .map(|(owner, _)| owner)
        .map_err(|error| unconfirmed_refusal(pr_number, Unconfirmed::UnknownOwner(error), act))?;
    if let Err(detail) =
        crate::security::broker::github::installation_authority(&orch.db.local, &owner).await
    {
        return Err(unconfirmed_refusal(
            pr_number,
            Unconfirmed::NotInstalled { owner, detail },
            act,
        ));
    }
    // The snapshot's own observation of a merge or close records it, so the
    // refusal below and the row agree.
    let snapshot = crate::pr_data::actions::refresh::pr_snapshot_for_job(
        orch,
        &mr.job_id,
        Some(std::time::Instant::now()),
    )
    .await
    .map_err(|error| unconfirmed_refusal(pr_number, Unconfirmed::Unanswered(error), act))?;
    match Sealed::from_state(&snapshot.cache.state) {
        Some(sealed) => Err(refusal_for_job(&db, job_id, Some(pr_number), sealed, act).await),
        None => Ok(()),
    }
}

/// Why a pull request the record calls open could not be confirmed open. Each
/// cause carries its own remedy, because the gate fails closed on all of them.
enum Unconfirmed {
    /// The pull request's URL named no repository owner.
    UnknownOwner(String),
    /// No GitHub App installation covers the owner, so there is no credential
    /// to read the pull request with.
    NotInstalled { owner: String, detail: String },
    /// The read was attempted and GitHub did not answer it.
    Unanswered(String),
}

fn unconfirmed_refusal(pr_number: i32, cause: Unconfirmed, act: GatedAct) -> String {
    let consequence = act.consequence();
    let (cause, remedy) = match cause {
        Unconfirmed::UnknownOwner(error) => (
            format!("its repository owner could not be resolved ({error})"),
            "check the pull request URL bound to this node's merge request, then retry."
                .to_string(),
        ),
        Unconfirmed::NotInstalled { owner, detail } => (
            format!("no GitHub App installation for {owner} ({detail})"),
            format!(
                "install Cairn's GitHub App on {owner} (connect the app first if none is), then \
                 retry. The edit runs through `gh`, but whether the pull request is still open \
                 is read through the app."
            ),
        ),
        Unconfirmed::Unanswered(error) => (
            format!("GitHub did not answer the read ({error})"),
            "retry once GitHub answers. The write is refused rather than risk rewriting a pull \
             request that may have merged."
                .to_string(),
        ),
    };
    format!(
        "Could not confirm PR #{pr_number} is still open: {cause}. {consequence}\nRemedy: {remedy}"
    )
}

/// [`sealed_refusal`] addressed from the node `job_id` belongs to.
async fn refusal_for_job(
    db: &LocalDb,
    job_id: &str,
    pr_number: Option<i32>,
    sealed: Sealed,
    act: GatedAct,
) -> String {
    let node_uri = crate::messages::delivery::node_uri_for_job(db, job_id).await;
    sealed_refusal(node_uri.as_deref(), pr_number, sealed, act)
}

/// What a live re-read after the push establishes about the pull request.
struct RefreshedAfterPublish<'a> {
    summary: String,
    /// The refresh's own comparison of the PR head with the branch head. It is
    /// `Unknown` for a pull request no longer open, and whenever the comparison
    /// could not run.
    heads: &'a crate::pr_data::publication::HeadComparison,
}

/// The reply to a successful push. It claims the pull request holds the
/// branch only when the refresh's comparison found the heads `Equal`; otherwise
/// it says what the push did and keeps the divergence, or the uncertainty, the
/// refresh found. The branch can move again between the push and the re-read
/// (a seal, a base advance), so the pushed commit is named from origin, not
/// from the bookmark.
fn publish_outcome_message(
    branch: &str,
    origin_tip: Option<&str>,
    refreshed: Result<RefreshedAfterPublish<'_>, String>,
) -> String {
    use crate::pr_data::publication::HeadComparison;

    let short = |commit: &str| commit.chars().take(12).collect::<String>();
    let pushed = match origin_tip {
        Some(tip) => format!("Pushed `{branch}`; origin holds {}", short(tip)),
        None => format!("Pushed `{branch}`; origin's tip could not be read back"),
    };
    match refreshed {
        Err(error) => format!(
            "{pushed}, but re-reading the pull request failed: {error}. Whether it shows the \
             pushed head is unknown; patch with action:\"refresh\" to re-read it."
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Divergent(divergence),
            ..
        }) => format!(
            "{pushed}. {} The branch may have moved again during the push, or GitHub has not \
             caught up yet; read the artifact again before acting on the pull request's signals.",
            divergence.note(branch)
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Unknown,
            summary,
        }) => format!(
            "{pushed}. The re-read of {summary} could not compare its head with the branch, so \
             whether it shows the pushed head is unknown."
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Equal(head),
            summary,
        }) => format!(
            "{pushed}, and {summary} holds {}, the branch's current head.",
            short(head)
        ),
    }
}

/// `action:"publish"` on a PR artifact: publish the branch behind the job's
/// OPEN pull request at its current head, then re-read the pull request and
/// report what it verifiably holds.
///
/// This is the lever an agent reaches for when the artifact warns that the pull
/// request holds an older head than the branch, instead of committing again to
/// force a push. The publish self-heals a stale or untracked origin copy of the
/// branch that shares its history, and refuses one that does not.
///
/// Refused without pushing unless the pull request is bound and open, both as
/// recorded and as GitHub reports it live ([`ensure_pr_artifact_writable`]):
/// publishing the branch of a merged or closed pull request could recreate a
/// remote branch removed on purpose.
///
/// A commit barrier first: commits earlier batches sealed on the branch but
/// could not publish are republished, so the push carries them.
pub(crate) async fn publish_pr_branch_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let barrier = crate::mcp::handlers::run::held::job_commit_barrier(orch, job_id).await;
    publish_pr_branch_past_barrier(orch, job_id)
        .await
        .map(|reply| barrier.prefix(reply))
        .map_err(|error| barrier.prefix(error))
}

async fn publish_pr_branch_past_barrier(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let mr = crate::pr_data::actions::try_resolve_mr_context_for_job(&orch.db.local, job_id)
        .await?
        .ok_or_else(|| {
            format!("job {job_id} has no pull request whose branch could be published")
        })?;
    if mr.is_local {
        return Err(
            "this change was opened without a GitHub remote, so there is nothing to publish; it \
             merges through Cairn's store"
                .to_string(),
        );
    }
    if mr.github_pr_number.is_none() {
        return Err(
            "no pull request is bound to this change yet, so there is no pull request head to \
             advance; patch with action:\"refresh\" to bind one that exists"
                .to_string(),
        );
    }
    ensure_pr_artifact_writable(orch, job_id, true, GatedAct::Publish).await?;

    let branch = publish_merge_request_branch(orch, &mr.repo_path, &mr.mr_id)
        .await
        .map_err(|error| format!("{error}. {}", publish_failure_remedy(&error)))?;

    let origin_tip = crate::pr_data::publication::origin_branch_tip(
        &*orch.services.git,
        std::path::Path::new(&mr.repo_path),
        &branch,
    )
    .ok()
    .flatten();
    orch.invalidate_pr_refresh(&mr.job_id, "branch-published");
    let refreshed = crate::pr_data::actions::refresh::pr_snapshot_for_job(
        orch,
        &mr.job_id,
        Some(std::time::Instant::now()),
    )
    .await;
    Ok(publish_outcome_message(
        &branch,
        origin_tip.as_deref(),
        refreshed
            .as_deref()
            .map(|snapshot| RefreshedAfterPublish {
                summary: crate::pr_data::publication::refreshed_summary(&snapshot.cache),
                heads: &snapshot.heads,
            })
            .map_err(|error| error.to_string()),
    ))
}

/// Sync an edited `create-pr` artifact into the already-open PR it produced:
/// publish the branch, then update the prose.
///
/// A builder can rewrite its `create-pr` artifact after a PR node/action has
/// opened the live PR. In that case the artifact is the source of truth the
/// reviewer should see — both its *tree* and its prose — so the branch is
/// published to origin first and only then are GitHub and the local
/// `merge_requests` cache updated.
pub(crate) async fn sync_create_pr_artifact_for_job(
    orch: &Orchestrator,
    job_id: &str,
    title: &str,
    body: Option<&str>,
) -> Result<bool, String> {
    sync_pr_prose(orch, job_id, title, body, None).await
}

/// Appended to a create-pr artifact write's summary when its pull-request
/// sync waits for the write to release its store lock. It stays in the report
/// recorded at the seal, so an interrupted write is replayed as pending, never
/// as synced; completion replaces it with what actually happened.
pub(crate) const DEFERRED_PR_SYNC_PENDING: &str =
    "pull request sync pending until this write releases its store lock (if this write is \
     interrupted, patch the artifact with action:\"publish\" or save it again to sync)";

/// A create-pr artifact version stored by a write that held its project's
/// store lock. Publishing the branch and editing the pull request are network
/// I/O that must not run under that lock, so the write runs this after it
/// releases the lock and its branch reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeferredPrSync {
    pub(crate) job_id: String,
    pub(crate) output_name: String,
    pub(crate) artifact_id: String,
    pub(crate) version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeferredPrSyncOutcome {
    /// The pull request carries the artifact's latest version.
    Synced,
    /// The job has no pull request yet; there is nothing to sync.
    NoPullRequest,
    /// A newer version replaced this one before its sync ran; that version's
    /// own write syncs it, and this older one must not overwrite it.
    Superseded { by_version: i32 },
}

/// How many times [`complete_deferred_pr_sync`] re-syncs when a newer artifact
/// version lands while it is editing the pull request.
const DEFERRED_PR_SYNC_ATTEMPTS: usize = 3;

async fn latest_artifact_version(
    orch: &Orchestrator,
    job_id: &str,
    output_name: &str,
) -> Result<(String, i32, serde_json::Value), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let (job_id, output_name) = (job_id.to_string(), output_name.to_string());
    let row = db
        .read(|conn| {
            let (job_id, output_name) = (job_id.clone(), output_name.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, version, data FROM artifacts
                          WHERE job_id = ?1 AND output_name = ?2
                          ORDER BY version DESC LIMIT 1",
                        params![job_id.as_str(), output_name.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.i64(1)? as i32, row.text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| db_error("Failed to load the create-pr artifact", e))?
        .ok_or_else(|| format!("job {job_id} has no `{output_name}` artifact to sync"))?;
    let data = serde_json::from_str(&row.2)
        .map_err(|error| format!("the stored create-pr artifact does not parse: {error}"))?;
    Ok((row.0, row.1, data))
}

/// Sync a deferred create-pr artifact into its pull request. Runs with no store
/// lock or branch reservation held. `published_branch` is a branch the write
/// already pushed after releasing the lock; it is not pushed again.
///
/// The pull request always receives the artifact's latest version, and only
/// while the deferred one is still the latest when the sync starts: a newer
/// version is that version's own write to sync. A version that lands during the
/// edit is followed, a bounded number of times, so an older edit never has the
/// last word.
pub(crate) async fn complete_deferred_pr_sync(
    orch: &Orchestrator,
    deferral: &DeferredPrSync,
    published_branch: Option<&str>,
) -> Result<DeferredPrSyncOutcome, String> {
    let mut synced_id = deferral.artifact_id.clone();
    for attempt in 0..DEFERRED_PR_SYNC_ATTEMPTS {
        let (id, version, data) =
            latest_artifact_version(orch, &deferral.job_id, &deferral.output_name).await?;
        if attempt == 0 && id != deferral.artifact_id {
            return Ok(DeferredPrSyncOutcome::Superseded {
                by_version: version,
            });
        }
        if attempt > 0 && id == synced_id {
            return Ok(DeferredPrSyncOutcome::Synced);
        }
        // The version was stored under a store lock, where only the recorded
        // state could be checked. GitHub answers before its prose is pushed;
        // a merge seen here restores the record and keeps this version as a
        // post-merge draft.
        if attempt == 0 {
            ensure_pr_artifact_writable(orch, &deferral.job_id, true, GatedAct::ProseSync).await?;
        }
        let title = data
            .get("title")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("create-pr artifact version {version} has no title"))?;
        let body = data.get("body").and_then(serde_json::Value::as_str);
        if !sync_pr_prose(orch, &deferral.job_id, title, body, published_branch).await? {
            return Ok(DeferredPrSyncOutcome::NoPullRequest);
        }
        synced_id = id;
    }
    let (id, version, _) =
        latest_artifact_version(orch, &deferral.job_id, &deferral.output_name).await?;
    if id == synced_id {
        Ok(DeferredPrSyncOutcome::Synced)
    } else {
        Err(format!(
            "create-pr artifact versions kept landing while the pull request was synced (now \
             version {version}); save the artifact again to sync it"
        ))
    }
}

async fn sync_pr_prose(
    orch: &Orchestrator,
    job_id: &str,
    title: &str,
    body: Option<&str>,
    published_branch: Option<&str>,
) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let Some(mr_context) = orch
        .db
        .local
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(
                async move { query_mr_context_for_create_pr_artifact_job(conn, &job_id).await },
            )
        })
        .await
        .map_err(|e| db_error("Failed to resolve merge request", e))?
    else {
        return Ok(false);
    };
    // Every caller has passed the gate already; the recorded seal is checked
    // again here because a merge can be recorded between the gate and this
    // edit, and an edit of a merged pull request rewrites its record.
    if let Some(sealed) = recorded_seal(&orch.db.local, &mr_context.mr_id).await? {
        return Err(refusal_for_job(
            &orch.db.local,
            &job_id,
            mr_context.github_pr_number,
            sealed,
            GatedAct::ProseSync,
        )
        .await);
    }

    // Only a bound remote PR has a head to advance; a local-only change has no
    // origin to publish to and nothing external reading it.
    if let Some(pr_number) = mr_context.github_pr_number {
        let already_published = match published_branch {
            Some(published) => {
                source_branch_for_merge_request(&orch.db.local, &mr_context.mr_id).await?
                    == published
            }
            None => false,
        };
        if !already_published {
            publish_branch_for_artifact_sync(orch, &mr_context.repo_path, &mr_context.mr_id)
                .await?;
        }
        edit_pull_request_prose(
            orch,
            &mr_context.repo_path,
            &mr_context.mr_id,
            pr_number,
            title,
            body,
        )
        .await?;
    }

    let now = chrono::Utc::now().timestamp();
    update_merge_request_title_body(&orch.db.local, &mr_context.mr_id, title, body, now).await?;
    orch.invalidate_pr_refresh(&mr_context.job_id, "pull-request-title-or-body");
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::merge_request_db_change_ids(
            "update",
            Some(&mr_context.mr_id),
            Some(&mr_context.job_id),
            mr_context.issue_id.as_deref(),
            &mr_context.project_id,
        ),
    );
    Ok(true)
}

/// Edit a bound pull request's title and body on GitHub.
async fn edit_pull_request_prose(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
    pr_number: i32,
    title: &str,
    body: Option<&str>,
) -> Result<(), String> {
    #[cfg(feature = "test-utils")]
    let scripted = prose_edit_seam::intercept(orch, repo_path, mr_id, pr_number, title, body).await;
    #[cfg(not(feature = "test-utils"))]
    let scripted: Option<Result<(), String>> = {
        let _ = (orch, mr_id);
        None
    };
    match scripted {
        Some(result) => result,
        None => edit_github_pr_title_body(repo_path, pr_number, title, body).await,
    }
}

/// Put a resolved pull request's merged prose back on GitHub and in its row,
/// after Cairn itself pushed a post-merge draft over it. The seal's gate never
/// sees this edit: it restores the record the gate protects.
pub(super) async fn restore_pull_request_prose(
    orch: &Orchestrator,
    db: &LocalDb,
    repo_path: &str,
    mr_id: &str,
    pr_number: i32,
    title: &str,
    body: Option<&str>,
) -> Result<(), String> {
    edit_pull_request_prose(orch, repo_path, mr_id, pr_number, title, body).await?;
    update_merge_request_title_body(db, mr_id, title, body, chrono::Utc::now().timestamp()).await
}

/// Lets an integration test stand in for `gh pr edit` on one repository and
/// observe the locks held at the moment the pull request would be edited.
#[cfg(feature = "test-utils")]
pub mod prose_edit_seam {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex};

    /// What was held when a pull-request edit ran.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ProseEditProbe {
        pub pr_number: i32,
        pub title: String,
        pub body: Option<String>,
        pub store_lock_holder: Option<String>,
        pub branch_reservation_holder: Option<String>,
    }

    type Script = Arc<dyn Fn(&ProseEditProbe) -> Result<(), String> + Send + Sync>;
    type Installed = (Script, Arc<Mutex<Vec<ProseEditProbe>>>);
    static SEAMS: LazyLock<Mutex<HashMap<String, Installed>>> = LazyLock::new(Default::default);

    /// The installed stand-in; dropping it removes it.
    pub struct ProseEdits {
        repo_path: String,
        probes: Arc<Mutex<Vec<ProseEditProbe>>>,
    }

    impl ProseEdits {
        pub fn probes(&self) -> Vec<ProseEditProbe> {
            self.probes.lock().unwrap().clone()
        }
    }

    impl Drop for ProseEdits {
        fn drop(&mut self) {
            SEAMS.lock().unwrap().remove(&self.repo_path);
        }
    }

    /// Answer every pull-request edit for the checkout at `repo_path` with
    /// `script` instead of running `gh`.
    pub fn install(
        repo_path: &std::path::Path,
        script: impl Fn(&ProseEditProbe) -> Result<(), String> + Send + Sync + 'static,
    ) -> ProseEdits {
        let repo_path = repo_path.to_string_lossy().into_owned();
        let probes = Arc::new(Mutex::new(Vec::new()));
        SEAMS
            .lock()
            .unwrap()
            .insert(repo_path.clone(), (Arc::new(script), probes.clone()));
        ProseEdits { repo_path, probes }
    }

    pub(super) async fn intercept(
        orch: &crate::orchestrator::Orchestrator,
        repo_path: &str,
        mr_id: &str,
        pr_number: i32,
        title: &str,
        body: Option<&str>,
    ) -> Option<Result<(), String>> {
        let (script, probes) = SEAMS.lock().unwrap().get(repo_path).cloned()?;
        let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(repo_path));
        let branch = super::source_branch_for_merge_request(&orch.db.local, mr_id)
            .await
            .unwrap_or_default();
        let probe = ProseEditProbe {
            pr_number,
            title: title.to_string(),
            body: body.map(str::to_string),
            store_lock_holder: orch
                .store_lock_status(&store)
                .and_then(|status| status.holder.map(|(operation, _)| operation)),
            branch_reservation_holder: orch.branch_reservation_holder(&store, &branch),
        };
        probes.lock().unwrap().push(probe.clone());
        Some(script(&probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_local_open_merge_request, seed_pr_node_merge_request_for_artifact_job,
        seed_remote_open_merge_request, test_orchestrator,
    };
    use crate::services::testing::MockGitClient;
    use crate::storage::RowExt;

    #[test]
    fn gh_pr_edit_args_include_title_and_body() {
        assert_eq!(
            gh_pr_edit_args("1538", "New title", "New body"),
            [
                "pr",
                "edit",
                "1538",
                "--title",
                "New title",
                "--body",
                "New body"
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_updates_local_merge_request_cache() {
        let db = migrated_db().await;
        seed_local_open_merge_request(&db, "owner-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "owner-job",
            "New archive semantics",
            Some("Updated PR body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-local'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New archive semantics");
        assert_eq!(row.1, "Updated PR body");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_follows_pr_node_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "builder-job",
            "New PR-node title",
            Some("New PR-node body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-pr-node'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New PR-node title");
        assert_eq!(row.1, "New PR-node body");
    }

    /// Re-saving the artifact is the sanctioned trigger for publishing a
    /// builder's branch, so a sync that cannot publish must NOT report success.
    /// Reporting `Ok` here is how two pull requests came to present their
    /// pre-review trees as the reviewable version while their branches carried
    /// the fixes.
    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_fails_closed_when_the_branch_cannot_be_published() {
        let db = migrated_db().await;
        let repo = tempfile::tempdir().unwrap();
        seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let error = sync_create_pr_artifact_for_job(&orch, "owner-job", "Title", Some("Body"))
            .await
            .expect_err("an unpublishable branch must not report a completed sync");

        assert!(
            error.contains("Failed to publish") && error.contains("agent/proj-4-builder-0"),
            "the failure must name the branch it could not publish: {error}"
        );
        assert!(
            error.contains("was not synced"),
            "the failure must say the artifact did not land: {error}"
        );
    }

    /// A merged or closed pull request's branch is never published: the refusal
    /// comes from the recorded state, before any push is attempted (reaching
    /// the push here would fail with "could not publish", since the fixture has
    /// no store). It is the seal's one refusal, route included.
    #[tokio::test(flavor = "current_thread")]
    async fn publish_refuses_a_merged_or_closed_pull_request_without_pushing() {
        for (column, value, outcome) in [
            ("status", "closed", "PR #2833 closed without merging"),
            ("status", "merged", "PR #2833 merged"),
            ("github_state", "MERGED", "PR #2833 merged"),
            ("github_state", "CLOSED", "PR #2833 closed without merging"),
        ] {
            let db = migrated_db().await;
            let repo = tempfile::tempdir().unwrap();
            seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
            db.execute(
                format!("UPDATE merge_requests SET {column} = ?1 WHERE id = 'mr-remote'"),
                params![value],
            )
            .await
            .unwrap();
            let orch = test_orchestrator(db, MockGitClient::new());

            let error = publish_pr_branch_for_job(&orch, "owner-job")
                .await
                .expect_err("a merged or closed pull request must not be published");
            assert!(
                error.contains(outcome)
                    && error.contains("continuity")
                    && error.contains("The branch was not published."),
                "{column}={value}: {error}"
            );
            assert!(
                !error.contains("could not publish"),
                "{column}={value}: the refusal must precede any push: {error}"
            );
        }
    }

    /// Without a bound pull request there is no head to advance.
    #[tokio::test(flavor = "current_thread")]
    async fn publish_refuses_an_unbound_change_without_pushing() {
        let db = migrated_db().await;
        let repo = tempfile::tempdir().unwrap();
        seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
        db.execute(
            "UPDATE merge_requests SET github_pr_number = NULL WHERE id = 'mr-remote'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let error = publish_pr_branch_for_job(&orch, "owner-job")
            .await
            .expect_err("an unbound change has no pull request head to advance");
        assert!(error.contains("no pull request is bound"), "{error}");
    }

    fn reply_for(origin_tip: &str, heads: &crate::pr_data::publication::HeadComparison) -> String {
        publish_outcome_message(
            "agent/proj-1-builder-0",
            Some(origin_tip),
            Ok(RefreshedAfterPublish {
                summary: "PR #7 (state OPEN, +1 -0)".to_string(),
                heads,
            }),
        )
    }

    /// The branch advanced from A to B after A was pushed, and the refreshed
    /// snapshot still reports the pull request at A. The reply names what origin
    /// holds, keeps the divergence warning, and does not claim the pull request
    /// holds the branch.
    #[test]
    fn a_branch_that_moved_after_the_push_is_not_reported_as_published() {
        let a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let heads = crate::pr_data::publication::classify_heads(a, Some(b.to_string()), |_| true);
        let message = reply_for(a, &heads);
        assert!(message.contains("origin holds aaaaaaaaaaaa"), "{message}");
        assert!(message.contains("bbbbbbbbbbbb"), "{message}");
        assert!(
            message.contains("describes the version the pull request holds"),
            "{message}"
        );
        assert!(!message.contains("the branch's current head"), "{message}");
    }

    /// The refresh's comparison could not run: branch metadata exists, but
    /// GitHub reported an empty head or the store could not read the bookmark.
    /// Built through the production classifier, the reply says unknown and
    /// never claims convergence.
    #[test]
    fn an_uncomparable_refresh_reports_unknown_not_convergence() {
        let tip = "cccccccccccccccccccccccccccccccccccccccc";
        for heads in [
            crate::pr_data::publication::classify_heads("", Some(tip.to_string()), |_| true),
            crate::pr_data::publication::classify_heads(tip, None, |_| true),
        ] {
            let message = reply_for(tip, &heads);
            assert!(message.contains("unknown"), "{heads:?}: {message}");
            assert!(
                !message.contains("the branch's current head"),
                "{heads:?}: {message}"
            );
        }
    }

    /// Convergence is claimed only from an `Equal` comparison, and a failed
    /// re-read claims nothing about the PR head.
    #[test]
    fn publish_claims_convergence_only_from_an_equal_comparison() {
        let tip = "cccccccccccccccccccccccccccccccccccccccc";
        let equal = crate::pr_data::publication::classify_heads(
            tip,
            Some(tip.to_string()),
            |_| unreachable!(),
        );
        let agreed = reply_for(tip, &equal);
        assert!(
            agreed.contains("holds cccccccccccc, the branch's current head"),
            "{agreed}"
        );

        let failed = publish_outcome_message("b", None, Err("offline".to_string()));
        assert!(failed.contains("could not be read back"), "{failed}");
        assert!(
            failed.contains("offline") && failed.contains("unknown"),
            "{failed}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_noops_without_merge_request() {
        let db = migrated_db().await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(&orch, "missing-job", "Title", Some("Body"))
            .await
            .unwrap();

        assert!(!synced);
    }
}
