//! Synchronous `when:write` project-check runner.
//!
//! A committing `write`/`run` that seals a source-touching commit calls
//! [`run_write_checks_after_seal`] right after the seal. It loads the project's
//! `checks` contract, computes the node's changed files, selects the affected
//! `when:write` checks, and runs each one to completion against the sealed
//! commit — streaming its output live into the originating tool's transcript and
//! returning a compact inline pass/fail line. A cache hit returns the stored
//! verdict without re-running.
//!
//! ## Contract source: live project config, not the worktree
//!
//! The `checks` contract is read from the project's MAIN-CHECKOUT
//! `.cairn/config.yaml` (located by `project_id`), which is exactly the file the
//! Settings UI edits. It is NOT read from the agent worktree's own committed
//! copy: that copy is snapshotted when the branch is cut, so a project-level
//! checks edit made while a session is in flight would never reach it. Sourcing
//! the contract live means a Settings edit takes effect on the very next commit
//! of an in-flight session with no restart. The live project config wins
//! outright over a branch's own committed `.cairn/config.yaml`; the worktree copy
//! is a fallback only when the project repo path cannot be resolved. Everything
//! else — the changed-file set, impact-glob matching, the cache tree hash, and
//! the check commands' working directory — still targets the sealed worktree
//! commit. See [`load_live_project_checks`].
//!
//! ## Scope
//!
//! Only the `when:write` cadence runs here; `when:review` runs at turn-end
//! ([`crate::execution::checks_turn_end`]). A check passes iff its
//! command exits `0` — output parsing ([`crate::execution::check_parsers`]) is
//! pure enrichment (failing test names + excerpt) and never changes a verdict;
//! a spawn error or sandbox denial is a clear failure, never a silent pass.
//! Placeholder selectors narrow to the delta since the check's last PASSING
//! baseline and fall back to the cumulative branch diff on any uncertainty (see
//! `baseline_delta_changed_files`). Checks are invoked through the `run` verb's
//! process machinery directly (not `run_one`), so a sandbox-blocked syscall
//! surfaces as a failed exit rather than an interactive fence prompt.
//!
//! ## Concurrency and isolation
//!
//! The affected cache-MISS checks run SEQUENTIALLY, in deterministic plan order,
//! inside one pure-verdict cell lease materialized at the just-sealed commit.
//! Cache hits are resolved before admission, so an all-hit cadence acquires no slot.
//! The committing run's affinity key prefers its warm slot while each check retains
//! its own cache identity, result, timing, output stream, and provenance. Checks may
//! not mutate the checked-out tree; mutation is a typed substrate failure and no
//! check path amends the agent's commit.
//!
//! ## Cache key
//!
//! Each check's verdict is keyed by [`check_result_key`]: the impact-filtered
//! sealed-tree content, configured command, platform, and cached toolchain
//! identity. A commit that changed none of a check's inputs — a doc-only commit
//! landing after a source commit — hits the cache even though the whole-tree hash moved, so the
//! check is not re-run. A check with no `impact` globs keeps whole-tree keying
//! ([`crate::jj::sealed_tree_hash`]). The row also stores that whole-tree hash and
//! re-stamps it on every evaluation (run OR hit), so the `/checks` listing — which
//! looks rows up by whole-tree hash — still surfaces every applicable check at the
//! current tree. If the sealed tree can't be read, an impact-scoped check falls
//! back to whole-tree keying: conservative (re-runs on any change), never a false
//! reuse.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

// Isolated checks carry their configured fleet constraints into the immutable request.
use crate::config::project_settings::{load_checks, CheckCommand, CheckPolicy, CheckWhen};
use crate::execution::cache::{
    get_check_result, list_latest_check_results_for_project, store_check_result,
    CheckResultCacheEntry, CheckResultCacheWrite,
};
use crate::execution::check_admission::CheckAdmissionController;
use crate::fleet::{
    CellOutcome, CellPriority, CellRequest, CommandResourceIdentity, MutationPolicy,
    PureVerdictBatchItem,
};
use crate::mcp::git::GitAuthor;
use cairn_common::executor_protocol::{
    PlacementConstraints, ProcessBatchExecution, ProcessBatchItem, RepositoryLocator,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecMode {
    Isolated,
    Shared,
}

const SLOT_CHECK_DEV_DEBUG_ENV: (&str, &str) = ("CARGO_PROFILE_DEV_DEBUG", "line-tables-only");

fn slot_check_env(mut env: Vec<(String, String)>) -> Vec<(String, String)> {
    env.retain(|(key, _)| key != SLOT_CHECK_DEV_DEBUG_ENV.0);
    env.push((
        SLOT_CHECK_DEV_DEBUG_ENV.0.to_string(),
        SLOT_CHECK_DEV_DEBUG_ENV.1.to_string(),
    ));
    env
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    admission: &CheckAdmissionController,
    diagnostic_orch: Option<&Orchestrator>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    run_planned_checks_with_board(
        db,
        project_id,
        tree_hash,
        job_id,
        plans,
        tool_use_id,
        mode,
        admission,
        diagnostic_orch,
        None,
        execute,
        notify,
    )
    .await
}

type CheckStatusNotify =
    Arc<dyn Fn(Vec<CheckStatusEntry>, Option<String>, Option<String>) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct CheckStatusBoard {
    entries: Arc<std::sync::Mutex<Vec<CheckStatusEntry>>>,
    phase: Arc<std::sync::Mutex<(Option<String>, Option<String>)>>,
    notify: CheckStatusNotify,
}

impl CheckStatusBoard {
    fn new(plans: &[(CheckPlan, String)], notify: CheckStatusNotify) -> Self {
        Self {
            entries: Arc::new(std::sync::Mutex::new(
                plans
                    .iter()
                    .enumerate()
                    .map(|(index, (plan, _))| CheckStatusEntry {
                        index,
                        name: plan.name.clone(),
                        state: "pending".into(),
                        annotation: None,
                    })
                    .collect(),
            )),
            phase: Arc::new(std::sync::Mutex::new((None, None))),
            notify,
        }
    }

    fn emit(&self) {
        let entries = self.entries.lock().unwrap().clone();
        let (phase, detail) = self.phase.lock().unwrap().clone();
        (self.notify)(entries, phase, detail);
    }

    fn emit_initial(&self) {
        self.emit();
    }

    pub(crate) fn transition(&self, index: usize, state: &str, annotation: Option<String>) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get_mut(index) {
                entry.state = state.to_string();
                entry.annotation = annotation;
            }
        }
        if state == "running" {
            *self.phase.lock().unwrap() = (Some("running".into()), None);
        } else if self
            .entries
            .lock()
            .unwrap()
            .iter()
            .all(|entry| entry.state == "passed" || entry.state == "failed")
        {
            *self.phase.lock().unwrap() = (None, None);
        }
        self.emit();
    }

    fn set_phase(&self, phase: Option<&str>, detail: Option<String>) {
        *self.phase.lock().unwrap() = (phase.map(str::to_string), detail);
        self.emit();
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCheckCacheContext {
    pub project_id: String,
    pub job_id: String,
    pub tree_hash: String,
    pub input_hash: String,
    cacheable: bool,
    entry: Option<crate::execution::cache::CheckResultCacheEntry>,
}

impl ManualCheckCacheContext {
    pub fn require_cacheable(&self) -> Result<(), String> {
        self.cacheable.then_some(()).ok_or_else(|| {
            "impact-relevant working-copy changes cannot be stored as sealed-tree evidence"
                .to_string()
        })
    }
}

/// Resolve the canonical check-result identity for a manual command launched in
/// a managed worktree. Impact-relevant loose edits make the request a cache miss;
/// unrelated dirt may still reuse the sealed tree's evidence.
pub async fn manual_check_cache_context(
    orch: &Orchestrator,
    worktree: &Path,
    check_name: &str,
) -> Result<ManualCheckCacheContext, String> {
    let canonical = std::fs::canonicalize(worktree).unwrap_or_else(|_| worktree.to_path_buf());
    let mut owner = None;
    for db in orch.db.all_dbs().await {
        let needle = canonical.to_string_lossy().to_string();
        let found = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT id, project_id FROM jobs WHERE worktree_path = ?1 ORDER BY created_at DESC LIMIT 1",
                (needle.as_str(),),
            ).await?;
            rows.next().await?.map(|row| Ok((row.text(0)?, row.text(1)?))).transpose()
        })).await.map_err(|e| e.to_string())?;
        if let Some((job_id, project_id)) = found {
            owner = Some((db, job_id, project_id));
            break;
        }
    }
    let (db, job_id, project_id) =
        owner.ok_or_else(|| "current directory is not a managed Cairn worktree".to_string())?;
    let checks = load_live_project_checks(orch, &project_id, &canonical)
        .await
        .ok_or_else(|| "project has no configured checks".to_string())?;
    let check = checks
        .get(check_name)
        .ok_or_else(|| format!("configured check {check_name:?} was not found"))?;
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let dirty = crate::jj::working_copy_dirty_paths(&jj, &canonical)?;
    let relevant_dirty = match check.impact.as_ref() {
        None => !dirty.is_empty(),
        Some(globs) => crate::execution::selection::paths_match_impact(globs, &dirty),
    };
    let tree_hash = crate::jj::sealed_tree_hash(&jj, &canonical)?;
    let entries = crate::jj::sealed_tree_entries(&jj, &canonical).ok();
    let input_hash = check_result_key(
        check,
        entries.as_deref(),
        &tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
    );
    let entry = if relevant_dirty {
        None
    } else {
        crate::execution::cache::get_check_result(db, &project_id, check_name, &input_hash)?
    };
    Ok(ManualCheckCacheContext {
        project_id,
        job_id,
        tree_hash,
        input_hash,
        cacheable: !relevant_dirty,
        entry,
    })
}

fn format_fixed_batch_summary(summary: &str, commit: &str, paths: &[String]) -> String {
    let short = commit.get(..12).unwrap_or(commit);
    let verdicts = summary.strip_prefix("Checks: ").unwrap_or(summary);
    format!(
        "Checks: ✓ write-check fixes (fixed, {short}; {} file{}) · {verdicts}",
        paths.len(),
        if paths.len() == 1 { "" } else { "s" }
    )
}

fn delta_patch_excerpt(repo: &Path, delta: &crate::fleet::MutationDelta) -> String {
    std::process::Command::new("git")
        .args(["diff", "--binary", &delta.base_commit, &delta.delta_commit])
        .current_dir(repo)
        .output()
        .ok()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .chars()
                .take(4_000)
                .collect()
        })
        .unwrap_or_else(|| "(delta patch unavailable)".into())
}

fn split_write_check_batch_outcome(
    outcome: CellOutcome,
    expected: usize,
) -> (
    Vec<Result<CheckExecResult, CheckExecutionFailure>>,
    Option<crate::fleet::MutationDelta>,
) {
    match outcome {
        CellOutcome::Completed {
            output,
            metadata,
            mutation_delta,
            sandbox_denials: _,
            tracked_modifications: _,
            ..
        } => {
            let decoded = serde_json::from_str::<
                Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
            >(&output);
            match decoded {
                Ok(items) if items.len() == expected => {
                    let results = items
                        .into_iter()
                        .map(|item| {
                            let mut output = item.body;
                            append_sandbox_denial_evidence(&mut output, &item.sandbox_denials);
                            append_tracked_modification_evidence(
                                &mut output,
                                item.tracked_modifications.as_ref(),
                            );
                            let mut provenance = metadata.clone();
                            provenance.started_at_unix_ms = item.started_at_unix_ms;
                            provenance.finished_at_unix_ms = item.finished_at_unix_ms;
                            provenance.duration_ms = Some(item.duration_ms);
                            provenance.peak_rss_bytes = item.peak_rss_bytes;
                            provenance.disk_delta_bytes = item.disk_delta_bytes;
                            Ok(CheckExecResult {
                                exit_code: item.exit_code,
                                output,
                                timed_out: item.timed_out,
                                duration_ms: Some(
                                    i64::try_from(item.duration_ms).unwrap_or(i64::MAX),
                                ),
                                provenance: Some(provenance),
                                publication: None,
                            })
                        })
                        .collect();
                    (results, mutation_delta.map(|delta| *delta))
                }
                Ok(items) => batch_failure_results(
                    expected,
                    format!(
                        "executor returned {} item outcomes for {expected} checks",
                        items.len()
                    ),
                ),
                Err(error) => batch_failure_results(
                    expected,
                    format!("decode typed write-check batch outcomes: {error}"),
                ),
            }
        }
        other => {
            let error = check_result_from_cell_outcome(other, None)
                .err()
                .unwrap_or_else(|| {
                    CheckExecutionFailure::Substrate("write-check batch produced no result".into())
                });
            let text = match error {
                CheckExecutionFailure::Process(text) | CheckExecutionFailure::Substrate(text) => {
                    text
                }
            };
            batch_failure_results(expected, text)
        }
    }
}

fn batch_failure_results(
    expected: usize,
    error: String,
) -> (
    Vec<Result<CheckExecResult, CheckExecutionFailure>>,
    Option<crate::fleet::MutationDelta>,
) {
    (
        (0..expected)
            .map(|_| Err(CheckExecutionFailure::Substrate(error.clone())))
            .collect(),
        None,
    )
}

/// Stable cost identity for a configured check. Unlike [`check_result_key`],
/// this deliberately excludes the tree: verdict validity is tree-sensitive,
/// while the resources consumed by the same configured command transcend trees.
pub(crate) fn check_resource_identity(name: &str, check: &CheckCommand) -> CommandResourceIdentity {
    use sha2::{Digest, Sha256};

    fn field(hasher: &mut Sha256, value: &str) {
        hasher.update(value.len().to_be_bytes());
        hasher.update(value.as_bytes());
    }
    fn option(hasher: &mut Sha256, value: Option<&str>) {
        field(hasher, value.unwrap_or("<none>"));
    }
    fn strings(hasher: &mut Sha256, values: Option<&[String]>) {
        let mut values = values.unwrap_or_default().to_vec();
        values.sort();
        field(hasher, &values.len().to_string());
        for value in values {
            field(hasher, &value);
        }
    }

    let mut hasher = Sha256::new();
    field(&mut hasher, "check-resource-v1");
    field(&mut hasher, name);
    field(&mut hasher, &check.command);
    strings(&mut hasher, check.impact.as_deref());
    field(&mut hasher, check.policy.as_str());
    field(&mut hasher, check.when.as_str());
    field(&mut hasher, check.resource_class.as_str());
    option(
        &mut hasher,
        check.timeout.map(|value| value.to_string()).as_deref(),
    );
    if let Some(constraints) = check.constraints.as_ref() {
        field(&mut hasher, "constraints");
        option(&mut hasher, constraints.executor_id.as_deref());
        option(&mut hasher, constraints.device_id.as_deref());
        option(&mut hasher, constraints.os.as_deref());
        option(&mut hasher, constraints.arch.as_deref());
        strings(&mut hasher, Some(&constraints.required_toolchains));
    } else {
        field(&mut hasher, "no-constraints");
    }
    CommandResourceIdentity {
        version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
        key: format!("{:x}", hasher.finalize()),
    }
}

pub(crate) async fn resolve_check_repository(
    orch: &Orchestrator,
    project_id: &str,
    job_id: &str,
    workspace: &Path,
) -> Result<(String, std::path::PathBuf), String> {
    let context = crate::execution::jobs::workspace_identity::resolve_managed_workspace_context(
        orch.db.local.clone(),
        job_id.to_string(),
    )
    .await?
    .ok_or_else(|| format!("check dispatch job {job_id} has no managed workspace assignment"))?;
    if context.identity.project_id != project_id {
        return Err(format!(
            "check dispatch project mismatch: request names {project_id}, managed workspace names {}",
            context.identity.project_id
        ));
    }
    let store = crate::jj::project_store_dir(&orch.config_dir, &context.identity.project_root);
    let target = crate::execution::jobs::workspace_identity::resolve_managed_workspace_git_target(
        &orch.config_dir,
        &context,
        &store,
        "check dispatch",
    )?;
    let requested_workspace = std::fs::canonicalize(workspace).map_err(|error| {
        format!(
            "canonicalize check dispatch workspace {}: {error}",
            workspace.display()
        )
    })?;
    if requested_workspace != target.workspace {
        return Err(format!(
            "check dispatch workspace mismatch: request resolves to {}, managed assignment resolves to {}",
            requested_workspace.display(),
            target.workspace.display()
        ));
    }
    Ok((
        target.repository.to_string_lossy().into_owned(),
        target.store_dir,
    ))
}

struct TemporaryCheckRef {
    orch: Orchestrator,
    repository: std::path::PathBuf,
    store_dir: std::path::PathBuf,
    reference: String,
    commit: String,
    armed: bool,
}

impl std::fmt::Debug for TemporaryCheckRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TemporaryCheckRef")
            .field("repository", &self.repository)
            .field("store_dir", &self.store_dir)
            .field("reference", &self.reference)
            .field("commit", &self.commit)
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

impl TemporaryCheckRef {
    fn delete_locked(&mut self) -> Result<(), String> {
        git_check_output(
            &self.repository,
            &["update-ref", "-d", &self.reference, &self.commit],
            "delete sealed check commit reference",
        )?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for TemporaryCheckRef {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let orch = self.orch.clone();
        let repository = self.repository.clone();
        let store_dir = self.store_dir.clone();
        let reference = self.reference.clone();
        let commit = self.commit.clone();
        let cleanup = async move {
            loop {
                let _guard = orch
                    .acquire_jj_store_lock(
                        &store_dir,
                        "remove abandoned sealed check commit reference",
                    )
                    .await;
                match git_check_output(
                    &repository,
                    &["update-ref", "-d", &reference, &commit],
                    "delete abandoned sealed check commit reference",
                ) {
                    Ok(_) => return,
                    Err(error) => log::warn!(
                        "temporary check ref cleanup will retry: reference={}, commit={}, error={}",
                        reference,
                        commit,
                        error
                    ),
                }
                drop(_guard);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(cleanup);
        } else {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("temporary check ref cleanup runtime");
                runtime.block_on(cleanup);
            });
        }
    }
}

fn git_check_output(repository: &Path, args: &[&str], context: &str) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn publish_check_commit_ref(
    orch: &Orchestrator,
    repository: &Path,
    store_dir: &Path,
    commit: &str,
    request_id: &str,
) -> Result<TemporaryCheckRef, String> {
    publish_check_commit_ref_with_verifier(
        orch,
        repository,
        store_dir,
        commit,
        request_id,
        |repository, reference| {
            git_check_output(
                repository,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                "verify sealed check commit reference",
            )
        },
    )
    .await
}

async fn publish_check_commit_ref_with_verifier<F>(
    orch: &Orchestrator,
    repository: &Path,
    store_dir: &Path,
    commit: &str,
    request_id: &str,
    verify: F,
) -> Result<TemporaryCheckRef, String>
where
    F: FnOnce(&Path, &str) -> Result<String, String>,
{
    let _guard = orch
        .acquire_jj_store_lock_with_timeout(
            store_dir,
            "publish sealed check commit",
            Some(std::time::Duration::from_secs(600)),
        )
        .await
        .map_err(|_| "timed out acquiring the managed store lock for check dispatch".to_string())?;
    git_check_output(
        repository,
        &["cat-file", "-e", &format!("{commit}^{{commit}}")],
        "verify sealed check commit in the managed Git backend",
    )?;
    let reference = format!("refs/cairn/checks/{request_id}");
    let absent = "0".repeat(commit.len());
    git_check_output(
        repository,
        &["update-ref", &reference, commit, &absent],
        "publish sealed check commit reference",
    )?;
    // Arm the cleanup obligation immediately after publication. Every later
    // error path either deletes under this held store lock or transfers the
    // obligation to Drop's lock-aware retry task.
    let mut temporary_ref = TemporaryCheckRef {
        orch: orch.clone(),
        repository: repository.to_path_buf(),
        store_dir: store_dir.to_path_buf(),
        reference,
        commit: commit.to_string(),
        armed: true,
    };
    let resolved = match verify(repository, &temporary_ref.reference) {
        Ok(resolved) => resolved,
        Err(error) => {
            return match temporary_ref.delete_locked() {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "{error}; temporary check ref cleanup failed and was scheduled for retry: {cleanup_error}"
                )),
            };
        }
    };
    if resolved != commit {
        let error =
            format!("sealed check commit reference resolved to {resolved}, expected {commit}");
        return match temporary_ref.delete_locked() {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(format!(
                "{error}; temporary check ref cleanup failed and was scheduled for retry: {cleanup_error}"
            )),
        };
    }
    Ok(temporary_ref)
}

async fn cleanup_check_commit_ref(
    orch: &Orchestrator,
    store_dir: &Path,
    temporary_ref: &mut TemporaryCheckRef,
) -> Result<(), String> {
    let _guard = orch
        .acquire_jj_store_lock_with_timeout(
            store_dir,
            "remove sealed check commit reference",
            Some(std::time::Duration::from_secs(600)),
        )
        .await
        .map_err(|_| "timed out acquiring the managed store lock for check cleanup".to_string())?;
    temporary_ref.delete_locked()
}

fn merge_batch_constraints(
    items: &[PlannedCheckBatchItem],
) -> Result<Option<PlacementConstraints>, String> {
    fn merge_scalar(
        current: &mut Option<String>,
        incoming: &Option<String>,
        field: &str,
    ) -> Result<(), String> {
        let Some(incoming) = incoming else {
            return Ok(());
        };
        match current {
            Some(current) if current != incoming => Err(format!(
                "conflicting review check placement constraint {field}: {current:?} vs {incoming:?}"
            )),
            Some(_) => Ok(()),
            None => {
                *current = Some(incoming.clone());
                Ok(())
            }
        }
    }

    let mut merged = PlacementConstraints::default();
    let mut toolchains = BTreeSet::new();
    for constraints in items.iter().filter_map(|item| item.constraints.as_ref()) {
        merge_scalar(
            &mut merged.executor_id,
            &constraints.executor_id,
            "executorId",
        )?;
        merge_scalar(&mut merged.device_id, &constraints.device_id, "deviceId")?;
        merge_scalar(&mut merged.os, &constraints.os, "os")?;
        merge_scalar(&mut merged.arch, &constraints.arch, "arch")?;
        toolchains.extend(constraints.required_toolchains.iter().cloned());
    }
    merged.required_toolchains = toolchains.into_iter().collect();
    Ok((!merged.is_empty()).then_some(merged))
}

pub(crate) async fn submit_planned_check_batch(
    orch: &Orchestrator,
    mut batch: PlannedCheckBatchRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    if batch.mutation_policy == MutationPolicy::PureVerdict {
        let mut groups = partition_check_items_by_constraints(std::mem::take(&mut batch.items));
        if groups.len() > 1 {
            let mut combined = PlannedCheckBatchOutcome {
                results: HashMap::new(),
                request: None,
                delta: None,
                store_dir: Some(batch.store_dir.clone()),
            };
            for (_, items) in groups {
                let outcome = submit_single_planned_check_batch(
                    orch,
                    PlannedCheckBatchRequest {
                        items,
                        ..batch.clone()
                    },
                )
                .await?;
                combined.results.extend(outcome.results);
            }
            return Ok(combined);
        }
        batch.items = groups.pop().map(|(_, items)| items).unwrap_or_default();
    }
    submit_single_planned_check_batch(orch, batch).await
}

fn partition_check_items_by_constraints(
    items: Vec<PlannedCheckBatchItem>,
) -> Vec<(Option<PlacementConstraints>, Vec<PlannedCheckBatchItem>)> {
    let mut groups: Vec<(Option<PlacementConstraints>, Vec<PlannedCheckBatchItem>)> = Vec::new();
    for item in items {
        let constraints = item.constraints.clone().filter(|value| !value.is_empty());
        if let Some((_, items)) = groups
            .iter_mut()
            .find(|(candidate, _)| candidate == &constraints)
        {
            items.push(item);
        } else {
            groups.push((constraints, vec![item]));
        }
    }
    groups
}

async fn submit_single_planned_check_batch(
    orch: &Orchestrator,
    batch: PlannedCheckBatchRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    // Configuration conflicts are deterministic caller errors and must surface
    // before any transient infrastructure preflight can obscure them.
    let constraints = merge_batch_constraints(&batch.items)?;
    if let Some(failure) =
        active_build_service_failure(&orch.build_service_diagnostic_snapshot("sccache"))
    {
        return Ok(PlannedCheckBatchOutcome::failed(
            batch.items.iter().map(|item| item.index).collect(),
            failure,
        ));
    }
    let timeout_ms = batch
        .items
        .iter()
        .fold(0_u32, |sum, item| sum.saturating_add(item.timeout_ms));
    let fleet_config = crate::config::settings::load_fleet(&orch.config_dir);
    let request_id = uuid::Uuid::new_v4().to_string();
    let attempt_id = uuid::Uuid::new_v4().to_string();
    let mut temporary_ref = match publish_check_commit_ref(
        orch,
        Path::new(&batch.repository),
        &batch.store_dir,
        &batch.sealed_commit,
        &request_id,
    )
    .await
    {
        Ok(reference) => reference,
        Err(error) => {
            return Ok(PlannedCheckBatchOutcome::failed(
                batch.items.iter().map(|item| item.index).collect(),
                error,
            ))
        }
    };
    let command = batch
        .items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>()
        .join(" · ");
    let request = CellRequest {
        request_id,
        attempt_id,
        project_id: batch.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: batch.project_id.clone(),
            repository_id: batch.project_id.clone(),
            absolute_path: batch.repository,
        },
        base_commit: batch.sealed_commit,
        command_class: cairn_common::executor_protocol::CellCommandClass::classify(&command),
        command,
        owner: Some(batch.owner.clone()),
        cwd: String::new(),
        env: batch.env,
        priority: batch.priority,
        deadline_unix_ms: unix_time_ms_for_checks()
            + fleet_config
                .acquisition_deadline_seconds
                .saturating_mul(1_000),
        timeout_ms,
        mutation_policy: batch.mutation_policy,
        requesting_job_id: Some(batch.requesting_job_id),
        affinity_key: batch.affinity_key,
        constraints,
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
    };
    let indexed: Vec<_> = batch.items.iter().map(|item| item.index).collect();
    let items = batch
        .items
        .into_iter()
        .map(|item| PureVerdictBatchItem {
            result_identity: crate::execution::cache::CheckResultIdentity::new(
                &batch.project_id,
                &item.name,
                &item.input_hash,
            ),
            process: ProcessBatchItem {
                header: item.name,
                stream_id: item.stream_id,
                execution: ProcessBatchExecution::Direct,
                program: "bash".into(),
                args: vec!["-c".into(), item.command],
                env: item.env,
                stdin: None,
                timeout_ms: item.timeout_ms,
                command_resource_identity: Some(CommandResourceIdentity {
                    version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
                    key: item.resource_identity_key,
                }),
            },
        })
        .collect();
    let mutation_policy = request.mutation_policy.clone();
    let submitted = if mutation_policy == MutationPolicy::PureVerdict {
        orch.fleet
            .submit_pure_verdict_batch(orch, request.clone(), items, batch.run_context)
            .await
    } else {
        let processes = items.into_iter().map(|item| item.process).collect();
        if let Some(board) = &batch.status_board {
            board.set_phase(Some("queued"), Some("waiting for build slot".into()));
        }
        let outcome = orch
            .fleet
            .submit_write_check_batch(
                orch,
                request.clone(),
                processes,
                batch.run_context,
                batch.status_board.clone(),
            )
            .await;
        let (item_outcomes, delta) = split_write_check_batch_outcome(outcome, indexed.len());
        let results = indexed.iter().copied().zip(item_outcomes).collect();
        if let Err(error) =
            cleanup_check_commit_ref(orch, &batch.store_dir, &mut temporary_ref).await
        {
            return Ok(PlannedCheckBatchOutcome::failed(indexed, error));
        }
        return Ok(PlannedCheckBatchOutcome {
            results,
            request: Some(request),
            delta,
            store_dir: Some(batch.store_dir),
        });
    };
    if let Err(error) = cleanup_check_commit_ref(orch, &batch.store_dir, &mut temporary_ref).await {
        return Ok(PlannedCheckBatchOutcome::failed(indexed, error));
    }
    let results = indexed
        .into_iter()
        .zip(submitted)
        .map(|(index, submitted)| {
            let result = match submitted {
                Ok(submitted) => {
                    check_result_from_cell_outcome(submitted.outcome, Some(submitted.publication))
                }
                Err(outcome) => check_result_from_cell_outcome(outcome, None),
            };
            (index, result)
        })
        .collect();
    Ok(PlannedCheckBatchOutcome {
        results,
        request: Some(request),
        delta: None,
        store_dir: Some(batch.store_dir),
    })
}

#[derive(Clone)]
pub(crate) struct PlannedCheckBatchItem {
    pub index: usize,
    pub name: String,
    pub input_hash: String,
    pub resource_identity_key: String,
    pub command: String,
    pub stream_id: String,
    pub env: Vec<(String, String)>,
    pub timeout_ms: u32,
    pub constraints: Option<PlacementConstraints>,
}

#[derive(Clone)]
pub(crate) struct PlannedCheckBatchRequest {
    pub project_id: String,
    pub repository: String,
    pub store_dir: std::path::PathBuf,
    pub sealed_commit: String,
    pub requesting_job_id: String,
    pub owner: cairn_common::executor_protocol::CellOwnerRef,
    pub affinity_key: Option<String>,
    pub priority: CellPriority,
    pub env: Vec<(String, String)>,
    pub items: Vec<PlannedCheckBatchItem>,
    pub run_context: Option<RunContext>,
    pub mutation_policy: MutationPolicy,
    pub status_board: Option<CheckStatusBoard>,
}

pub(crate) struct PlannedCheckBatchOutcome {
    pub results: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
    pub request: Option<CellRequest>,
    pub delta: Option<crate::fleet::MutationDelta>,
    pub store_dir: Option<std::path::PathBuf>,
}

impl PlannedCheckBatchOutcome {
    fn failed(indices: Vec<usize>, error: String) -> Self {
        Self {
            results: indices
                .into_iter()
                .map(|index| {
                    (
                        index,
                        Err(CheckExecutionFailure::Substrate(format!(
                            "Cairn check infrastructure failure: {error}"
                        ))),
                    )
                })
                .collect(),
            request: None,
            delta: None,
            store_dir: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckExecutionFailure {
    Process(String),
    Substrate(String),
}

impl From<String> for CheckExecutionFailure {
    fn from(error: String) -> Self {
        Self::Process(error)
    }
}
use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, parse_check_output,
    ParsedCheckResult, MAX_FAILURE_NAMES,
};
use crate::execution::selection::{plan_checks, CheckPlan};
use crate::jj::{logical_changed_files, logical_tree_hash, tree_entries, GraphFileChange, JjEnv};
use crate::mcp::handlers::run::{CheckExecResult, CheckStatusEntry, CheckStatusPayload};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Default per-check time cap for the mid-turn `when:write` cadence. Its checks
/// are light (change-scoped test runs, a formatter, small consistency guards),
/// so 10 minutes is ample. A check may raise its own via the schema `timeout`.
const DEFAULT_WRITE_CHECK_TIMEOUT_MS: u32 = 600_000;
/// Default per-check time cap for the turn-end `when:review` cadence. Sized to
/// comfortably cover a COLD, uncached full Rust compile + ~1900 tests on this
/// hardware: observed *successful* `rust-full` runs already reach ~9.3 min, so
/// the prior hard 10-min ceiling guillotined healthy-but-slow suites (dozens of
/// rows killed at ~600s in this project's cache). An uncached cold build
/// (sccache down, CAIRN-2621) runs longer still, so 30 min gives ~3x headroom
/// over the slowest observed green. A check may override via the schema
/// `timeout` field.
pub(crate) const DEFAULT_REVIEW_CHECK_TIMEOUT_MS: u32 = 1_800_000;
/// Hard ceiling on a check's configured `timeout` (seconds → ms): a guardrail so
/// a config typo cannot wedge a check for hours. 60 minutes.
const MAX_CHECK_TIMEOUT_MS: u32 = 3_600_000;

/// Resolve one check's effective timeout in ms: its schema `timeout` (SECONDS,
/// clamped to [`MAX_CHECK_TIMEOUT_MS`]) when set, else the cadence default.
pub(crate) fn resolve_check_timeout_ms(check: Option<&CheckCommand>, default_ms: u32) -> u32 {
    match check.and_then(|c| c.timeout) {
        Some(secs) => secs.saturating_mul(1000).min(MAX_CHECK_TIMEOUT_MS),
        None => default_ms,
    }
}

/// Terminal classification refining a FAILING check's binary `passed = false`
/// verdict, so a timeout or a spawn failure renders AS itself instead of an
/// opaque `exit -1`. Persisted (snake_case) in `check_result_cache.failure_kind`;
/// `None`/absent means an ordinary failure (non-zero exit) or a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFailureKind {
    /// Killed at its timeout budget.
    TimedOut,
    /// The process could not be spawned (e.g. its cwd vanished mid-run).
    SpawnError,
    /// Died by signal mid-run without hitting the budget (crash / OOM kill).
    Killed,
    /// Positive evidence identifies a host, toolchain, or shared-service failure.
    Infrastructure,
    /// A recognized test runner exited abnormally without assertion failures.
    RunnerError,
}

impl CheckFailureKind {
    pub(crate) fn is_infrastructure(self) -> bool {
        matches!(
            self,
            CheckFailureKind::SpawnError
                | CheckFailureKind::Infrastructure
                | CheckFailureKind::RunnerError
        )
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CheckFailureKind::TimedOut => "timed_out",
            CheckFailureKind::SpawnError => "spawn_error",
            CheckFailureKind::Killed => "killed",
            CheckFailureKind::Infrastructure => "infrastructure",
            CheckFailureKind::RunnerError => "runner_error",
        }
    }

    /// Parse a persisted `failure_kind` string back into the enum; `None` for a
    /// pass, an ordinary failure, or a legacy `NULL`. (Not `FromStr`: the
    /// absent/unknown case is an ordinary `None`, not a parse error.)
    pub(crate) fn from_stored(s: &str) -> Option<Self> {
        match s {
            "timed_out" => Some(CheckFailureKind::TimedOut),
            "spawn_error" => Some(CheckFailureKind::SpawnError),
            "killed" => Some(CheckFailureKind::Killed),
            "infrastructure" => Some(CheckFailureKind::Infrastructure),
            "runner_error" => Some(CheckFailureKind::RunnerError),
            _ => None,
        }
    }

    /// The human verdict fragment, given the run duration (used for the timeout
    /// budget it was killed at).
    pub(crate) fn describe(self, duration_ms: i64) -> String {
        match self {
            CheckFailureKind::TimedOut => {
                format!("timed out after {}", format_timeout_budget(duration_ms))
            }
            CheckFailureKind::SpawnError => "failed to spawn".to_string(),
            CheckFailureKind::Killed => "killed (signal)".to_string(),
            CheckFailureKind::Infrastructure => "infrastructure/toolchain failure".to_string(),
            CheckFailureKind::RunnerError => "test runner failed".to_string(),
        }
    }
}

/// Format a timeout budget compactly: whole minutes at or above a minute, else
/// seconds. `600_000` → `10m`, `1_800_000` → `30m`, `45_000` → `45s`.
fn format_timeout_budget(duration_ms: i64) -> String {
    if duration_ms >= 60_000 {
        format!("{}m", (duration_ms as f64 / 60_000.0).round() as i64)
    } else {
        format!("{}s", (duration_ms as f64 / 1000.0).round() as i64)
    }
}

/// Chars of combined check output retained in the cache row's `output_tail`.
const OUTPUT_TAIL_CHARS: usize = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailureClassification {
    kind: CheckFailureKind,
    reason: String,
    evidence_line: Option<usize>,
}

fn classify_check_failure(
    exit_code: Option<i32>,
    timed_out: bool,
    spawn_error: bool,
    parsed: Option<&ParsedCheckResult>,
    output: &str,
) -> Option<FailureClassification> {
    if exit_code == Some(0) {
        return None;
    }
    if spawn_error {
        return Some(FailureClassification {
            kind: CheckFailureKind::SpawnError,
            reason: "Cairn: check process failed to spawn".to_string(),
            evidence_line: None,
        });
    }
    if timed_out {
        return Some(FailureClassification {
            kind: CheckFailureKind::TimedOut,
            reason: "Cairn: check exceeded its timeout budget".to_string(),
            evidence_line: None,
        });
    }
    if exit_code.is_none() {
        return Some(FailureClassification {
            kind: CheckFailureKind::Killed,
            reason: "Cairn: check process died without an exit code".to_string(),
            evidence_line: None,
        });
    }
    if parsed.is_some_and(|result| result.failed > 0) {
        return None;
    }

    let lines: Vec<&str> = output.lines().collect();
    let transport = lines.iter().position(|line| {
        let line = line.to_ascii_lowercase();
        line.contains("failed to send data to or receive data from server")
            || line.contains("failed client/server communication")
            || line.contains("failed to fill whole buffer")
            || line.contains("server looks like it shut down unexpectedly")
    });
    let abnormal_254 = (exit_code == Some(254))
        .then(|| {
            lines.iter().position(|line| {
                let line = line.to_ascii_lowercase();
                (line.contains("rustc") || line.contains("sccache"))
                    && (line.contains("exit status: 254")
                        || line.contains("exited with status 254"))
                    && (line.contains("process didn't exit successfully")
                        || line.contains("failed to execute compile")
                        || line.contains("compiler process"))
            })
        })
        .flatten();
    let missing_generated = lines.iter().position(|line| {
        let line = line.replace('\\', "/").to_ascii_lowercase();
        line.contains("target/")
            && line.contains("/build/")
            && line.contains("/out/")
            && (line.contains("couldn't read")
                || line.contains("failed to read")
                || line.contains("no such file or directory"))
    });
    if let Some(evidence_line) = transport.or(abnormal_254).or(missing_generated) {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason:
                "Cairn: infrastructure/toolchain failure matched reviewed abnormal-build evidence"
                    .to_string(),
            evidence_line: Some(evidence_line),
        });
    }

    if let Some(result) = parsed.filter(|result| result.parser == "vitest" && result.failed == 0) {
        let reason = if result.passed == 0 {
            "Cairn: Vitest failed before reporting any test assertions".to_string()
        } else {
            format!(
                "Cairn: Vitest runner failed after {} tests passed with no assertion failures",
                result.passed
            )
        };
        return Some(FailureClassification {
            kind: CheckFailureKind::RunnerError,
            reason,
            evidence_line: None,
        });
    }
    None
}

fn classified_output_excerpt(
    output: &str,
    classification: Option<&FailureClassification>,
) -> String {
    let Some(classification) = classification else {
        return tail(output, OUTPUT_TAIL_CHARS);
    };
    let lines: Vec<&str> = output.lines().collect();
    let context = classification.evidence_line.map(|index| {
        let start = index.saturating_sub(2);
        let end = (index + 3).min(lines.len());
        lines[start..end].join("\n")
    });
    let mut prefix = classification.reason.clone();
    if let Some(context) = context.filter(|context| !context.is_empty()) {
        prefix.push_str("\nEvidence:\n");
        prefix.push_str(&context);
    }
    prefix.push_str("\n\nFinal output:\n");
    let remaining = OUTPUT_TAIL_CHARS.saturating_sub(prefix.chars().count());
    prefix.push_str(&tail(output, remaining));
    prefix.chars().take(OUTPUT_TAIL_CHARS).collect()
}

/// Cancel any in-flight `when:review` check suite for `job_id` when a commit
/// seals mid-turn. The branch just advanced, so that suite — launched at the
/// previous turn-end against the now-superseded tree — is validating a tree
/// nobody will look at again, while its bounded concurrent full Rust compiles
/// (each in its own COW clone) saturate CPU and I/O. That starves this commit's
/// own `when:write` checks (which fire right after the seal via
/// [`run_write_checks_after_seal`]) and the agent's next manual `bun run
/// test:rust`. Cancelling frees those resources for the checks that validate the
/// NEW code; the review cadence relaunches a fresh suite for the advanced tree
/// at the next turn-end via the normal `spawn_turn_end_checks` path.
///
/// This reuses the CAIRN-2648 [`Orchestrator::cancel_turn_end_checks`] lever, so
/// it is best-effort and idempotent: a no-op when no review suite is in flight
/// for the job, hence safe on every commit. Keyed by the committing run's own
/// `job_id` — the dominant path is the builder committing its own fix, which
/// cancels exactly its suite. A sub-agent/task that commits into the builder's
/// inherited worktree under a *different* job id would not hit the builder's
/// suite; that edge case is deliberately left as-is.
pub(crate) fn cancel_stale_review_on_branch_advance(orch: &Orchestrator, job_id: &str) {
    orch.cancel_turn_end_checks(job_id);
}

/// Run the affected `when:write` checks after a source-touching commit has been
/// sealed, streaming their output live and returning a compact inline pass/fail
/// summary to append to the originating tool result.
///
/// Returns `None` whenever nothing applied: no run context (so no streaming
/// target), no `checks` contract, no resolvable changed-file set, an empty
/// change set, or no `when:write` check whose impact the change set matches (a
/// doc-only / non-source commit). A cache hit returns the stored verdict without
/// re-running.
pub(crate) async fn run_write_checks_after_seal(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
) -> Option<String> {
    let _guard = run_context.map(|context| WriteChecksInFlightGuard::new(orch, &context.job_id));
    run_write_checks_after_seal_inner(orch, run_context, cwd, tool_use_id, true).await
}

struct WriteChecksInFlightGuard<'a> {
    orch: &'a Orchestrator,
    job_id: String,
}

impl<'a> WriteChecksInFlightGuard<'a> {
    fn new(orch: &'a Orchestrator, job_id: &str) -> Self {
        orch.begin_write_checks(job_id);
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
        Self {
            orch,
            job_id: job_id.to_string(),
        }
    }
}

impl Drop for WriteChecksInFlightGuard<'_> {
    fn drop(&mut self) {
        self.orch.end_write_checks(&self.job_id);
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
    }
}

async fn run_write_checks_after_seal_inner(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
    may_fold: bool,
) -> Option<String> {
    // No run context ⇒ no run id to stream against and no job to anchor the diff.
    let run_context = run_context?;
    let repo_root = Path::new(cwd);

    // 1. Load the LIVE checks contract from the project's main checkout — the same
    // `.cairn/config.yaml` the Settings UI edits. The worktree's own committed
    // copy was snapshotted when the branch was cut, so a project-level edit made
    // mid-session would never reach an in-flight agent if we read it. Sourcing the
    // contract from the live project config makes a Settings edit take effect on
    // the very next commit, no restart. (Changed files, glob matching, the tree
    // hash, and the check commands themselves still run against the sealed
    // worktree commit at `repo_root` — only the contract is project-sourced.)
    let checks = load_live_project_checks(orch, &run_context.project_id, repo_root).await?;
    if checks.is_empty() {
        return None;
    }

    // 2–5. VCS inspection, cargo-metadata planning, and the synchronous cache
    // bridge all wait on subprocesses or joined threads. Gather the DB anchors
    // asynchronously, then keep the complete synchronous planning unit off Tokio
    // runtime workers.
    let (_base_branch, base_commit) =
        load_node_vcs_anchors(&orch.db.local, &run_context.job_id).await;
    let logical = crate::mcp::handlers::branch::resolve_current_for_read(
        orch,
        &crate::mcp::types::McpCallbackRequest {
            run_id: Some(run_context.run_id.clone()),
            cwd: cwd.to_string(),
            ..Default::default()
        },
    )
    .await
    .ok()?;
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let planning_jj = jj.clone();
    let planning_repo = logical.repository_path;
    let planning_head = logical.commit_id;
    let planning_base = base_commit.unwrap_or(logical.default_commit_id);
    let planning_checks = checks.clone();
    let planning_db = orch.db.local.clone();
    let planning_project_id = run_context.project_id.clone();
    let planned = tokio::task::spawn_blocking(move || {
        let changed =
            logical_changed_files(&planning_jj, &planning_repo, &planning_base, &planning_head)?;
        if changed.is_empty() {
            return None;
        }
        let plans = applicable_write_checks(&planning_checks, &changed, &planning_repo);
        if plans.is_empty() {
            return None;
        }
        let tree_hash = logical_tree_hash(&planning_jj, &planning_repo, &planning_head).ok()?;
        let sealed_commit = planning_head.clone();
        let entries = if plans.iter().any(|plan| {
            planning_checks
                .get(&plan.name)
                .is_some_and(|check| check.impact.is_some())
        }) {
            tree_entries(&planning_jj, &planning_repo, &planning_head).ok()
        } else {
            None
        };
        let latest_by_check: HashMap<String, CheckResultCacheEntry> =
            list_latest_check_results_for_project(planning_db.clone(), &planning_project_id)
                .unwrap_or_default()
                .into_iter()
                .map(|row| (row.check_name.clone(), row))
                .collect();
        let keyed: Vec<(CheckPlan, String)> = plans
            .into_iter()
            .map(|plan| {
                let check = planning_checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition");
                let input_hash = check_result_key(
                    check,
                    entries.as_deref(),
                    &tree_hash,
                    &check_platform_identity(),
                    check_toolchain_identity(),
                );
                let should_reselect = get_check_result(
                    planning_db.clone(),
                    &planning_project_id,
                    &plan.name,
                    &input_hash,
                )
                .ok()
                .flatten()
                .is_none();
                let selected_plan = if should_reselect {
                    let selected_changed = selected_changed_files_for_miss(
                        latest_by_check.get(&plan.name),
                        entries.as_deref(),
                        check.impact.as_ref(),
                        &changed,
                        &planning_jj,
                        &planning_repo,
                    );
                    replan_one_check(&plan.name, check, &selected_changed, &planning_repo)
                        .unwrap_or(plan)
                } else {
                    plan
                };
                (selected_plan, input_hash)
            })
            .collect();
        Some((keyed, tree_hash, sealed_commit))
    })
    .await
    .ok()??;
    let (keyed, tree_hash, sealed_commit) = planned;

    log::info!(
        "when:write checks: cache-filtering {} planned check(s) before one sequential pure-verdict slot lease",
        keyed.len()
    );

    // The live status-line emitter. `run_planned_checks` calls this with a full
    // checklist snapshot on every state transition; we forward each snapshot to
    // the frontend as a `check-status` event keyed by the committing call id.
    // Follows the `db-change` emit idiom below.
    let emitter = orch.services.emitter.clone();
    let notify_run_id = run_context.run_id.clone();
    let notify_tool_use_id = tool_use_id.to_string();
    // Per-check effective timeout, aligned to plan index (the `execute` closure
    // is indexed the same way). A check's schema `timeout` overrides the write
    // cadence default.
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_WRITE_CHECK_TIMEOUT_MS)
        })
        .collect();
    let miss_indices: Vec<usize> = keyed
        .iter()
        .enumerate()
        .filter_map(|(index, (plan, input_hash))| {
            get_check_result(
                orch.db.local.clone(),
                &run_context.project_id,
                &plan.name,
                input_hash,
            )
            .ok()
            .flatten()
            .is_none()
            .then_some(index)
        })
        .collect();
    let status_notify: CheckStatusNotify = Arc::new(move |checks, phase, phase_detail| {
        let _ = emitter.emit(
            "check-status",
            serde_json::to_value(CheckStatusPayload {
                run_id: notify_run_id.clone(),
                tool_use_id: notify_tool_use_id.clone(),
                checks,
                phase,
                phase_detail,
            })
            .unwrap_or(serde_json::Value::Null),
        );
    });
    let status_board = CheckStatusBoard::new(&keyed, status_notify);
    status_board.set_phase(
        (!miss_indices.is_empty()).then_some("dispatching"),
        (!miss_indices.is_empty()).then(|| "preparing check request".to_string()),
    );
    for (index, (plan, input_hash)) in keyed.iter().enumerate() {
        if miss_indices.contains(&index) {
            continue;
        }
        if let Ok(Some(hit)) = get_check_result(
            orch.db.local.clone(),
            &run_context.project_id,
            &plan.name,
            input_hash,
        ) {
            status_board.transition(
                index,
                if hit.passed { "passed" } else { "failed" },
                Some("cached".into()),
            );
        }
    }

    let batch_outcome = if miss_indices.is_empty() {
        None
    } else {
        let slot_env = slot_check_env(jj.shell_env());
        let items = miss_indices
            .iter()
            .map(|index| PlannedCheckBatchItem {
                index: *index,
                name: keyed[*index].0.name.clone(),
                input_hash: keyed[*index].1.clone(),
                resource_identity_key: check_resource_identity(
                    &keyed[*index].0.name,
                    checks
                        .get(&keyed[*index].0.name)
                        .expect("planned check must retain its configured definition"),
                )
                .key,
                command: keyed[*index].0.command.clone(),
                stream_id: crate::mcp::handlers::run::check_stream_id(tool_use_id, *index),
                env: slot_env.clone(),
                timeout_ms: timeouts[*index],
                constraints: checks
                    .get(&keyed[*index].0.name)
                    .and_then(|check| check.constraints.clone()),
            })
            .collect();
        status_board.set_phase(Some("provisioning"), Some("resolving build slot".into()));
        let repository = resolve_check_repository(
            orch,
            &run_context.project_id,
            &run_context.job_id,
            repo_root,
        )
        .await;
        let submitted = match repository {
            Ok((repository, store_dir)) => {
                submit_planned_check_batch(
                    orch,
                    PlannedCheckBatchRequest {
                        project_id: run_context.project_id.clone(),
                        repository,
                        store_dir,
                        sealed_commit,
                        requesting_job_id: run_context.job_id.clone(),
                        owner: cairn_common::executor_protocol::CellOwnerRef {
                            project_id: run_context.project_id.clone(),
                            project_key: Some(run_context.project_key.clone()),
                            issue_number: run_context.issue_number,
                            job_id: Some(run_context.job_id.clone()),
                            execution_seq: run_context.exec_seq,
                            node_kind: run_context.job_name.clone(),
                        },
                        affinity_key: Some(run_context.job_id.clone()),
                        priority: CellPriority::WriteCheck,
                        env: slot_env,
                        items,
                        run_context: Some(run_context.clone()),
                        mutation_policy: MutationPolicy::AllowDelta,
                        status_board: Some(status_board.clone()),
                    },
                )
                .await
            }
            Err(error) => Ok(PlannedCheckBatchOutcome::failed(
                miss_indices.clone(),
                error,
            )),
        };
        Some(match submitted {
            Ok(results) => results,
            Err(error) => PlannedCheckBatchOutcome::failed(miss_indices.clone(), error),
        })
    };
    if let Some(delta) = batch_outcome
        .as_ref()
        .and_then(|outcome| outcome.delta.as_ref())
    {
        if !may_fold {
            let patch = delta_patch_excerpt(repo_root, delta);
            return Some(format!(
                "Checks: ✗ write-check batch (non-convergent: verification mutated again)\n```diff\n{patch}\n```"
            ));
        }
        let request = batch_outcome.as_ref()?.request.as_ref()?;
        let store_dir = batch_outcome.as_ref()?.store_dir.as_ref()?;
        let author = GitAuthor::new("Cairn checks", "checks@cairn.local");
        let branch =
            match crate::execution::jobs::workspace_identity::resolve_managed_workspace_context(
                orch.db.local.clone(),
                run_context.job_id.clone(),
            )
            .await
            {
                Ok(Some(context)) => context.identity.branch,
                Ok(None) => {
                    return Some(
                        "Checks: ✗ write-check fold (managed logical branch is absent)".into(),
                    )
                }
                Err(error) => {
                    return Some(format!(
                        "Checks: ✗ write-check fold (resolve managed logical branch: {error})"
                    ))
                }
            };
        let published = crate::mcp::handlers::run::publish_and_seal_slot_delta(
            orch,
            store_dir,
            request,
            delta,
            &branch,
            "fix: apply write-check changes",
            Some(&author),
        )
        .await;
        let published = match published {
            Ok(published) => published,
            Err(error) => {
                let patch = delta_patch_excerpt(repo_root, delta);
                return Some(format!(
                    "Checks: ✗ write-check fold ({error})\n```diff\n{patch}\n```"
                ));
            }
        };
        log::info!(
            "write checks published fix commit {} for {} path(s), patch_bytes={}",
            published.commit,
            published.paths.len(),
            published.patch.len()
        );
        let _ = orch.services.emitter.emit(
            "worktree-changed",
            serde_json::json!({"path": cwd, "source": "write-check-fix"}),
        );
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "invalidate"}),
        );
        let rerun = Box::pin(run_write_checks_after_seal_inner(
            orch,
            Some(run_context),
            cwd,
            tool_use_id,
            false,
        ))
        .await
        .unwrap_or_else(|| "Checks: ✗ write-check verification produced no verdict".into());
        return Some(format_fixed_batch_summary(
            &rerun,
            &published.commit,
            &published.paths,
        ));
    }
    let batched_results = batch_outcome.map_or_else(HashMap::new, |outcome| outcome.results);
    let batched_results = Arc::new(std::sync::Mutex::new(batched_results));
    let results = run_planned_checks_with_board(
        orch.db.local.clone(),
        &run_context.project_id,
        &tree_hash,
        run_context.job_id.as_str(),
        &keyed,
        tool_use_id,
        CheckExecMode::Shared,
        &orch.check_admission,
        Some(orch),
        Some(status_board),
        move |index, _command, _stream_id| {
            let batched_results = batched_results.clone();
            async move {
                batched_results
                .lock()
                .unwrap()
                .remove(&index)
                .unwrap_or_else(|| {
                    Err(CheckExecutionFailure::Substrate(format!(
                        "Cairn check infrastructure failure: missing batched outcome for plan index {index}"
                    )))
                })
            }
        },
        move |_checks| {},
    )
    .await;

    // Nudge any open Checks settings view (and other `check_result_cache`
    // consumers) to re-read the freshly stored verdicts. The turn-end cadence
    // emits the same signal; the write cadence must too, or per-commit results
    // never surface live in the settings editor.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    if results.is_empty() {
        return None;
    }
    Some(format!("Checks: {}", format_check_summary(&results)))
}

/// Resolve the project's LIVE `checks` contract for the runner.
///
/// The contract source is the project's main-checkout `.cairn/config.yaml`,
/// located by `project_id` (this resolves team replicas through the route table,
/// not just `projects.repo_path`). That is the same file the Settings UI writes,
/// so a project-level edit is visible here on the next commit without restarting
/// the session. This runs in the host orchestrator process (the MCP callback
/// handler), not the fenced agent subprocess, so reading the main checkout is
/// not a fence crossing — the same host-side read `projects::remote` already does.
///
/// Precedence: the live project config wins outright. The worktree's own
/// committed `.cairn/config.yaml` is consulted ONLY as a fallback when the
/// project repo path cannot be resolved (e.g. a team project with no local
/// clone), so an unresolved project never silently drops every check.
pub(crate) async fn load_live_project_checks(
    orch: &Orchestrator,
    project_id: &str,
    worktree_root: &Path,
) -> Option<HashMap<String, CheckCommand>> {
    let project_repo = crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, project_id)
        .await
        .ok()
        .and_then(|(path, _key)| path);
    let worktree_root = worktree_root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        checks_from_source(project_repo.as_deref().map(Path::new), &worktree_root)
    })
    .await
    .ok()
    .flatten()
}

/// Pick the live `checks` contract given the optionally-resolved project repo
/// path and the worktree fallback. Pure (filesystem reads only, no orchestrator)
/// so the project-wins / worktree-fallback precedence is unit-testable. Both
/// reads use the non-migrating [`load_checks`] so neither path triggers a config
/// migration commit from inside an agent run.
fn checks_from_source(
    project_repo: Option<&Path>,
    worktree_root: &Path,
) -> Option<HashMap<String, CheckCommand>> {
    match project_repo {
        Some(path) => load_checks(path),
        None => load_checks(worktree_root),
    }
}

/// The subset of planned checks that both apply to the change set AND run at the
/// TURN-END cadence. `when:review` (including the `idle` legacy alias) runs at
/// every turn-end; `when:write` never runs here (it is the mid-turn cadence). An
/// impact-scoped check that no changed file matches has `applies == false`. Pure,
/// so the cadence gate is unit-tested.
pub(crate) fn applicable_turn_end_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, changed, repo_root)
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| match check.when {
                    CheckWhen::Review => true,
                    CheckWhen::Write => false,
                })
        })
        .collect()
}

/// Applicable hard requirements for synchronous combined-tree verification.
/// Advisory review checks remain part of the turn-end feedback cadence, but a
/// manual child-PR merge must not launch them.
fn applicable_combined_tree_gate_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    applicable_turn_end_checks(checks, changed, repo_root)
        .into_iter()
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| check.policy == CheckPolicy::Gate)
        })
        .collect()
}

fn applicable_write_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, changed, repo_root)
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| check.when == CheckWhen::Write)
        })
        .collect()
}

fn replan_one_check(
    name: &str,
    check: &CheckCommand,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Option<CheckPlan> {
    let mut one = HashMap::new();
    one.insert(name.to_string(), check.clone());
    plan_checks(&one, changed, repo_root)
        .into_iter()
        .next()
        .filter(|plan| plan.applies)
}

/// Changed-file selector for a cache miss. The planner stays pure: this runner
/// reads cache rows and tree objects, then hands `plan_checks` either the narrowed
/// delta or the conservative cumulative branch diff as ordinary data.
fn selected_changed_files_for_miss(
    latest: Option<&CheckResultCacheEntry>,
    current_entries: Option<&[(String, String)]>,
    impact: Option<&Vec<String>>,
    cumulative: &[GraphFileChange],
    jj: &JjEnv,
    repo_root: &Path,
) -> Vec<GraphFileChange> {
    let Some(latest) = latest.filter(|row| row.passed) else {
        return cumulative.to_vec();
    };
    let Some(current_entries) = current_entries else {
        return cumulative.to_vec();
    };
    let baseline_entries = match tree_entries(jj, repo_root, &latest.tree_hash) {
        Ok(entries) => entries,
        Err(e) => {
            log::warn!(
                "when:write checks: failed to read cached baseline tree {} for {}: {e}; \
                 using cumulative selection",
                latest.tree_hash,
                latest.check_name
            );
            return cumulative.to_vec();
        }
    };
    baseline_delta_changed_files(
        Some(latest),
        Some(&baseline_entries),
        Some(current_entries),
        impact,
        cumulative,
    )
}

/// Pure decision rule for choosing a placeholder-selection change set. A passing
/// baseline means the cached verdict covered the baseline tree's impact-matched
/// subset, so the next run only has to select tests/targets reachable from the
/// paths whose matching tree entries changed since then. The baseline row is
/// project-global: it may have been re-stamped by another branch, but comparing
/// tree objects under the same impact globs is still sound. If the other branch's
/// tree differs in extra matching paths, the delta over-includes; it cannot hide a
/// current change from a passing baseline.
fn baseline_delta_changed_files(
    latest: Option<&CheckResultCacheEntry>,
    baseline_entries: Option<&[(String, String)]>,
    current_entries: Option<&[(String, String)]>,
    impact: Option<&Vec<String>>,
    cumulative: &[GraphFileChange],
) -> Vec<GraphFileChange> {
    if !latest.is_some_and(|row| row.passed) {
        return cumulative.to_vec();
    }
    let (Some(baseline), Some(current)) = (baseline_entries, current_entries) else {
        return cumulative.to_vec();
    };
    match diff_tree_entries_for_impact(baseline, current, impact) {
        Some(delta) if !delta.is_empty() => delta,
        _ => cumulative.to_vec(),
    }
}

fn diff_tree_entries_for_impact(
    baseline: &[(String, String)],
    current: &[(String, String)],
    impact: Option<&Vec<String>>,
) -> Option<Vec<GraphFileChange>> {
    let matcher = match impact {
        Some(globs) => Some(crate::execution::selection::build_glob_set(globs).ok()?),
        None => None,
    };
    let is_match = |path: &str| {
        matcher
            .as_ref()
            .map(|set| set.is_match(path))
            .unwrap_or(true)
    };
    let baseline: BTreeMap<&str, &str> = baseline
        .iter()
        .filter(|(path, _)| is_match(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    let current: BTreeMap<&str, &str> = current
        .iter()
        .filter(|(path, _)| is_match(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    let paths: BTreeSet<&str> = baseline.keys().chain(current.keys()).copied().collect();
    let mut changes = Vec::new();
    for path in paths {
        let before = baseline.get(path);
        let after = current.get(path);
        if before == after {
            continue;
        }
        changes.push(GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: match (before, after) {
                (None, Some(_)) => "added",
                (Some(_), None) => "deleted",
                (Some(_), Some(_)) => "modified",
                (None, None) => unreachable!(),
            }
            .to_string(),
            additions: 0,
            deletions: 0,
        });
    }
    Some(changes)
}

/// The outcome of one planned check: its exit-code-driven verdict, the parsed
/// per-test detail (enrichment, may be absent), and the retained combined-output
/// tail used as the excerpt fallback. Carried out of [`run_planned_checks`] so
/// the inline summary can render WHAT failed, not just the exit code.
pub(crate) struct CheckOutcome {
    pub(crate) name: String,
    pub(crate) passed: bool,
    pub(crate) exit_code: Option<i32>,
    /// Terminal classification for a FAILING check (timeout / spawn error /
    /// signal kill), so a summary renders the real failure, not a bare exit
    /// code. `None` for a pass or an ordinary non-zero exit.
    pub(crate) failure_kind: Option<CheckFailureKind>,
    /// Structured per-test result, when the runner's output could be parsed.
    pub(crate) parsed: Option<ParsedCheckResult>,
    /// Retained combined-output tail — the excerpt source when the parse carries
    /// no per-failure messages (nextest) or there is no parse at all.
    pub(crate) output_tail: String,
    /// Whether this verdict was REUSED from the cache rather than run for this
    /// commit. The summary annotates cache hits so a reused verdict is
    /// distinguishable from a fresh run at a glance.
    pub(crate) cached: bool,
    /// Wall-clock duration of the run that produced this verdict, in ms. On a
    /// cache hit this is the stored duration of the original run. Surfaced for
    /// non-test-runner checks (typecheck, api, …) where a test count is not
    /// meaningful.
    pub(crate) duration_ms: i64,
}

/// Versioned identity of the host tools that can affect project-check outcomes.
/// Probes run at most once per runner process; cache lookups never shell out.
static CHECK_TOOLCHAIN_IDENTITY: OnceLock<String> = OnceLock::new();
// v4 includes the slot-check line-tables-only Cargo profile used by every
// executed check, invalidating verdicts produced under the old codegen profile.
const CHECK_RESULT_KEY_VERSION: &str = "check-result-v4";

pub(crate) fn check_platform_identity() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

pub fn check_toolchain_identity() -> &'static str {
    CHECK_TOOLCHAIN_IDENTITY
        .get_or_init(|| {
            fn version(program: &str, args: &[&str]) -> String {
                std::process::Command::new(program)
                    .args(args)
                    .output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
                    .filter(|output| !output.is_empty())
                    .unwrap_or_else(|| "unavailable".to_string())
            }

            format!(
                "rustc={};bun={}",
                version("rustc", &["--version", "--verbose"]),
                version("bun", &["--version"])
            )
        })
        .as_str()
}

/// Canonical identity for one project-check result. This is deliberately the
/// single composition seam shared by cache lookups today and in-flight request
/// coalescing later: `(impact-filtered tree, command, platform, toolchain)`.
///
/// A check with no impact globs uses the whole sealed tree. When tree entries
/// cannot be read, an impact-scoped check also falls back to the whole-tree hash,
/// conservatively over-invalidating rather than falsely reusing a verdict.
/// Length-prefixing every component makes the tuple encoding unambiguous, and the
/// version prefix guarantees existing input-only rows invalidate by miss.
fn check_command_identity(
    check: &CheckCommand,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
) -> CommandResourceIdentity {
    use sha2::{Digest, Sha256};

    fn field(hasher: &mut Sha256, value: &str) {
        hasher.update(value.len().to_be_bytes());
        hasher.update(value.as_bytes());
    }
    fn option(hasher: &mut Sha256, value: Option<&str>) {
        match value {
            Some(value) => {
                field(hasher, "some");
                field(hasher, value);
            }
            None => field(hasher, "none"),
        }
    }
    fn strings(hasher: &mut Sha256, values: Option<&[String]>) {
        let mut values = values.unwrap_or_default().to_vec();
        values.sort();
        field(hasher, &values.len().to_string());
        for value in values {
            field(hasher, &value);
        }
    }

    let filtered_tree = match check.impact.as_ref() {
        None => tree_hash.to_string(),
        Some(globs) => match entries {
            Some(entries) => crate::execution::selection::check_input_hash(entries, globs),
            None => tree_hash.to_string(),
        },
    };
    let mut hasher = Sha256::new();
    field(&mut hasher, CHECK_RESULT_KEY_VERSION);
    field(&mut hasher, &filtered_tree);
    field(&mut hasher, &check.command);
    strings(&mut hasher, check.impact.as_deref());
    field(&mut hasher, check.policy.as_str());
    field(&mut hasher, check.when.as_str());
    field(&mut hasher, check.resource_class.as_str());
    option(
        &mut hasher,
        check.timeout.map(|value| value.to_string()).as_deref(),
    );
    if let Some(constraints) = check.constraints.as_ref() {
        field(&mut hasher, "constraints");
        option(&mut hasher, constraints.executor_id.as_deref());
        option(&mut hasher, constraints.device_id.as_deref());
        option(&mut hasher, constraints.os.as_deref());
        option(&mut hasher, constraints.arch.as_deref());
        strings(&mut hasher, Some(&constraints.required_toolchains));
    } else {
        field(&mut hasher, "no-constraints");
    }
    field(&mut hasher, platform);
    // This fingerprint is runner-local and is sound while executors are colocated.
    // Executor-resolved keying for heterogeneous fleets remains deliberately deferred.
    field(&mut hasher, toolchain);
    CommandResourceIdentity {
        version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
        key: format!("{:x}", hasher.finalize()),
    }
}

pub(crate) fn check_result_key(
    check: &CheckCommand,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
) -> String {
    check_command_identity(check, entries, tree_hash, platform, toolchain).key
}

/// Execute the planned checks against the sealed tree, consulting the cache
/// first. Each plan is paired with its per-check input hash (the cache key);
/// `tree_hash` is the whole-tree pointer re-stamped onto every evaluated row so
/// the `/checks` listing still surfaces the check at the current tree. Generic
/// over the spawn closure so the cache hit/miss behavior is unit-testable without
/// spawning a real process. Returns one [`CheckOutcome`] per check in plan order.
///
/// ## Ordering and isolation
///
/// Two phases. Phase 1 resolves cache HITS sequentially (a cheap re-stamp +
/// transition; a hit runs nothing). Phase 2 executes the MISSES, whose ordering
/// depends on `mode`:
///
/// - `Isolated`: each miss runs against its OWN copy-on-write clone of the sealed
///   worktree (resolved by the caller's `execute` closure). The futures are polled
///   concurrently, but every process spawn first crosses the runner-wide fair
///   admission controller. A formatter's writes land in its
///   private clone and are copied back only after every check finishes, so no check
///   ever observes another's half-written tree — every check validates exactly the
///   sealed tree.
/// - `Shared`: the fallback when a cheap clone is unavailable. All misses share
///   the one sealed checkout, so they MUST run SEQUENTIALLY, in plan order — a
///   mutating check's edits have to settle before the next check observes the
///   worktree, or a read-only check (e.g. `migrations` reading a Rust file) could
///   see a formatter's partial write.
///
/// One `run_miss` future serves both paths so the fallback is not a code fork.
/// Outcomes are reassembled into plan order regardless of completion order, so a
/// concurrent miss finishing first never reorders the summary. The
/// snapshot/`transition` machinery is a `std::sync::Mutex` with no guard held
/// across an await, and the per-check output streams are namespaced
/// `{toolUseId}:check-{index}`, so concurrent transitions and streams are safe.
///
/// ## Live status snapshots
///
/// `notify` receives a FULL checklist snapshot on every state transition (never a
/// delta), so a frontend consumer stays stateless — the latest snapshot wins. The
/// planned set (all `pending`) is emitted immediately; each entry then moves to
/// `running` when its command starts and to `passed`/`failed` (annotated exactly
/// as the final summary via [`summary_annotation`]) when it finishes. A cache hit
/// jumps straight from `pending` to its final state with no `running` phase.
///
/// A miss parses the runner's output into structured per-test results
/// ([`parse_check_output`]) and persists them in the cache row's
/// `target_results_json`; a hit rehydrates that column. Parsing is pure
/// enrichment — `passed` / `exit_code` stay exit-code-driven either way, so a
/// parser miss can never turn a failing exit into a pass.
// Each parameter is a distinct scalar/closure the runner genuinely needs (cache
// identity, plan set, spawn closure, live-status notifier); grouping them into a
// struct would only add indirection here.
#[allow(clippy::too_many_arguments)]
async fn run_planned_checks_with_board<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    admission: &CheckAdmissionController,
    diagnostic_orch: Option<&Orchestrator>,
    status_board: Option<CheckStatusBoard>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    // Checklist snapshot, seeded all-`pending` from the plan list. Each check
    // transitions ITS OWN entry and re-emits the whole snapshot, so the live line
    // is self-healing (latest snapshot wins). A std Mutex keeps the transition
    // helper a plain `Fn`; it is only ever locked to mutate + clone and released
    // before the (synchronous) emit, so no guard is held across an await.
    let board = status_board.unwrap_or_else(|| {
        CheckStatusBoard::new(plans, Arc::new(move |checks, _, _| notify(checks)))
    });
    board.emit_initial();

    // Phase 1: resolve cache HITS sequentially, and collect the MISS indices to
    // execute. `outcomes` is index-addressed so misses can complete out of order
    // (concurrent `Isolated` mode) and still reassemble into plan order.
    let mut outcomes: Vec<Option<CheckOutcome>> = (0..plans.len()).map(|_| None).collect();
    let mut misses: Vec<usize> = Vec::new();
    for (index, (plan, input_hash)) in plans.iter().enumerate() {
        // Cache hit ⇒ reuse the stored verdict and rehydrate the structured
        // detail; run nothing. The lookup is keyed by the per-check INPUT hash, so
        // a commit that changed none of this check's impact-matched files hits
        // even though the whole-tree hash moved.
        let Ok(Some(entry)) = get_check_result(db.clone(), project_id, &plan.name, input_hash)
        else {
            misses.push(index);
            continue;
        };
        // Re-stamp the row onto the current whole tree so the `/checks` listing
        // (keyed by whole-tree hash) still surfaces this check at the current
        // tree — without re-running it.
        let _ = store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: project_id.to_string(),
                tree_hash: tree_hash.to_string(),
                input_hash: input_hash.clone(),
                check_name: plan.name.clone(),
                exit_code: entry.exit_code,
                passed: entry.passed,
                output_tail: entry.output_tail.clone(),
                duration_ms: entry.duration_ms,
                target_results_json: entry.target_results_json.clone(),
                job_id: Some(job_id.to_string()),
                cached: Some(true),
                failure_kind: entry.failure_kind.clone(),
                executor_id: entry.executor_id.clone(),
                executor_device_id: entry.executor_device_id.clone(),
                executor_connection_generation: entry.executor_connection_generation,
                executor_cell_id: entry.executor_cell_id.clone(),
                executor_lease_epoch: entry.executor_lease_epoch,
                executor_started_at_unix_ms: entry.executor_started_at_unix_ms,
                executor_finished_at_unix_ms: entry.executor_finished_at_unix_ms,
                toolchain_fingerprint: entry.toolchain_fingerprint.clone(),
            },
        );
        if let Some(orch) = diagnostic_orch {
            orch.fleet.record_cached_completion(
                project_id,
                job_id,
                entry.executor_id.as_deref(),
                &plan.command,
                if tool_use_id.starts_with("turn-checks:") {
                    CellPriority::ReviewCheck
                } else {
                    CellPriority::WriteCheck
                },
                entry.passed,
            );
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "build_slots", "action": "update"}),
            );
        }
        // Rehydrate the structured per-test detail persisted at run time.
        let parsed = entry
            .target_results_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
        let outcome = CheckOutcome {
            name: plan.name.clone(),
            passed: entry.passed,
            exit_code: Some(entry.exit_code),
            failure_kind: entry
                .failure_kind
                .as_deref()
                .and_then(CheckFailureKind::from_stored),
            parsed,
            output_tail: entry.output_tail,
            cached: true,
            duration_ms: entry.duration_ms,
        };
        // A cache hit jumps straight from pending to its final state.
        board.transition(
            index,
            if outcome.passed { "passed" } else { "failed" },
            summary_annotation(&outcome),
        );
        outcomes[index] = Some(outcome);
    }

    // One miss: transition running → run (streaming) → record → transition final,
    // yielding `(index, outcome)` so the caller can reassemble into plan order.
    // Borrows shared state by reference so the returned future is not tied to a
    // moved closure capture (mirrors how `orch`/`run_context` flow through
    // `execute`), letting concurrent mode hold many of these futures at once.
    let run_miss = |index: usize| {
        let db = &db;
        let execute = &execute;
        let board = &board;
        async move {
            let (plan, input_hash) = &plans[index];
            // Slot scheduling owns its own priority-aware admission. Acquiring the
            // legacy FIFO permit before entering that queue would let a review
            // request hold host capacity while blocking a later interactive/write
            // request. Local checks retain the existing controller unchanged.
            let permit = if plan.requires_runner_local_admission {
                Some(
                    admission
                        .acquire(plan.resource_class)
                        .await
                        .expect("check admission semaphore must remain open"),
                )
            } else {
                None
            };
            board.transition(index, "running", None);
            let stream_id = crate::mcp::handlers::run::check_stream_id(tool_use_id, index);
            let started = Instant::now();
            let (
                exit_code,
                output,
                timed_out,
                spawn_error,
                substrate_failure,
                authoritative_duration_ms,
                provenance,
                publication,
            ) = match execute(index, plan.command.clone(), stream_id).await {
                Ok(CheckExecResult {
                    exit_code,
                    output,
                    timed_out,
                    duration_ms,
                    provenance,
                    publication,
                }) => (
                    exit_code,
                    output,
                    timed_out,
                    false,
                    false,
                    duration_ms,
                    provenance,
                    publication,
                ),
                Err(error) => match error.into() {
                    CheckExecutionFailure::Process(err) => {
                        (None, err, false, true, false, None, None, None)
                    }
                    CheckExecutionFailure::Substrate(err) => {
                        (None, err, false, false, true, None, None, None)
                    }
                },
            };
            let duration_ms =
                authoritative_duration_ms.unwrap_or_else(|| started.elapsed().as_millis() as i64);
            let passed = exit_code == Some(0);

            // Parse before classifying: positive assertion failures outrank any
            // incidental infrastructure warning in the combined output.
            let parsed = parse_check_output(&plan.command, &output);
            let classification = if substrate_failure {
                Some(FailureClassification {
                    kind: CheckFailureKind::Infrastructure,
                    reason: "Cairn: check execution substrate failed before producing a verdict"
                        .to_string(),
                    evidence_line: None,
                })
            } else {
                classify_check_failure(exit_code, timed_out, spawn_error, parsed.as_ref(), &output)
            };
            let failure_kind = classification.as_ref().map(|c| c.kind);
            let target_results_json = parsed.as_ref().and_then(|p| serde_json::to_string(p).ok());
            let mut output_tail = classified_output_excerpt(&output, classification.as_ref());

            if failure_kind == Some(CheckFailureKind::Infrastructure) {
                let admission_snapshot = admission.snapshot();
                let resources = crate::pressure::platform::read_process_resources();
                let host = crate::pressure::platform::read_host_resources();
                let process_tree = crate::pressure::process_tree::sample_ps_rows()
                    .map(|rows| {
                        let mut rows = crate::pressure::process_tree::select_process_tree(
                            &rows,
                            std::process::id(),
                        );
                        rows.sort_by_key(|row| std::cmp::Reverse(row.rss_kb));
                        rows.into_iter()
                            .take(16)
                            .map(|row| {
                                serde_json::json!({
                                    "pid": row.pid,
                                    "parentPid": row.ppid,
                                    "cpuPercent": row.cpu_percent,
                                    "rssKb": row.rss_kb,
                                    "command": row.command,
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let build_service =
                    diagnostic_orch.map(|orch| orch.build_service_diagnostic_snapshot("sccache"));
                if let Some(snapshot) = &build_service {
                    let evidence_head: String = output_tail.chars().take(1_500).collect();
                    let diagnostic = format!("Cairn diagnostic: {}", snapshot.compact_summary());
                    let reserved = evidence_head.chars().count() + diagnostic.chars().count() + 4;
                    let final_tail = tail(&output_tail, OUTPUT_TAIL_CHARS.saturating_sub(reserved));
                    output_tail = format!("{evidence_head}\n\n{diagnostic}\n\n{final_tail}");
                }
                log::warn!(
                    "check infrastructure failure: {}",
                    serde_json::json!({
                        "check": plan.name,
                        "jobId": job_id,
                        "suiteId": tool_use_id,
                        "cadence": if tool_use_id.starts_with("turn-checks:") { "review" } else { "write" },
                        "resourceClass": plan.resource_class.as_str(),
                        "queueWaitMs": permit.as_ref().map_or(0, |permit| permit.waited().as_millis()),
                        "durationMs": duration_ms,
                        "exitCode": exit_code,
                        "timedOut": timed_out,
                        "admission": admission_snapshot,
                        "runnerRssBytes": resources.rss_bytes,
                        "runnerPhysicalFootprintBytes": resources.phys_footprint_bytes,
                        "hostTotalMemoryBytes": host.total_memory_bytes,
                        "hostAvailableMemoryBytes": host.available_memory_bytes,
                        "hostLoadAverage": host.load_average,
                        "processTree": process_tree,
                        "buildService": build_service,
                    })
                );
            }

            let publication_role = match publication {
                Some(publication) => Some(publication.acquire().await),
                None => None,
            };
            let should_publish = !matches!(
                publication_role,
                Some(crate::fleet::PublicationRole::Published)
            );
            if should_publish {
                let _ = store_check_result(
                    db.clone(),
                    CheckResultCacheWrite {
                        project_id: project_id.to_string(),
                        tree_hash: tree_hash.to_string(),
                        input_hash: input_hash.clone(),
                        check_name: plan.name.clone(),
                        exit_code: exit_code.unwrap_or(-1),
                        passed,
                        output_tail: output_tail.clone(),
                        duration_ms,
                        target_results_json,
                        job_id: Some(job_id.to_string()),
                        cached: Some(false),
                        failure_kind: failure_kind.map(|kind| kind.as_str().to_string()),
                        executor_id: provenance.as_ref().map(|meta| meta.executor_id.clone()),
                        executor_device_id: provenance
                            .as_ref()
                            .map(|meta| meta.executor_device_id.clone()),
                        executor_connection_generation: provenance
                            .as_ref()
                            .map(|meta| meta.executor_connection_generation as i64),
                        executor_cell_id: provenance.as_ref().map(|meta| meta.cell_id.clone()),
                        executor_lease_epoch: provenance
                            .as_ref()
                            .map(|meta| meta.lease_epoch as i64),
                        executor_started_at_unix_ms: provenance
                            .as_ref()
                            .map(|meta| meta.started_at_unix_ms as i64),
                        executor_finished_at_unix_ms: provenance
                            .as_ref()
                            .map(|meta| meta.finished_at_unix_ms as i64),
                        toolchain_fingerprint: Some(check_toolchain_identity().to_string()),
                    },
                );
                if let Some(crate::fleet::PublicationRole::Publisher(guard)) = publication_role {
                    guard.published();
                }
            }

            let outcome = CheckOutcome {
                name: plan.name.clone(),
                passed,
                exit_code,
                failure_kind,
                parsed,
                output_tail,
                cached: false,
                duration_ms,
            };
            board.transition(
                index,
                if passed { "passed" } else { "failed" },
                summary_annotation(&outcome),
            );
            drop(permit);
            (index, outcome)
        }
    };

    // Phase 2: execute misses concurrently when the caller owns independent
    // execution substrates, or sequentially when they share one mutable checkout.
    match mode {
        CheckExecMode::Isolated => {
            // Poll every independently placed miss. Admission is owned either by the
            // external substrate or by the shared host controller.
            let done: Vec<(usize, CheckOutcome)> =
                futures_util::future::join_all(misses.iter().map(|&index| run_miss(index))).await;
            for (index, outcome) in done {
                outcomes[index] = Some(outcome);
            }
        }
        CheckExecMode::Shared => {
            for &index in &misses {
                let (index, outcome) = run_miss(index).await;
                outcomes[index] = Some(outcome);
            }
        }
    }

    outcomes
        .into_iter()
        .map(|o| o.expect("every plan resolved to a hit or a miss outcome"))
        .collect()
}

/// Render the inline pass/fail summary appended to the originating tool result.
/// The first line is the compact per-check status
/// (`\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)`); each failing check
/// then gets a detail block naming the failing tests and a bounded output
/// excerpt, so the agent learns WHAT broke without re-running the suite. Pure, so
/// it is unit-tested directly.
fn format_check_summary(results: &[CheckOutcome]) -> String {
    let header = results
        .iter()
        .map(|o| {
            let mark = if o.passed { '\u{2713}' } else { '\u{2717}' };
            match summary_annotation(o) {
                Some(ann) => format!("{mark} {} ({ann})", o.name),
                None => format!("{mark} {}", o.name),
            }
        })
        .collect::<Vec<_>>()
        .join(" \u{b7} ");

    let mut out = header;
    for o in results.iter().filter(|o| !o.passed) {
        if let Some(detail) = format_check_detail(o) {
            out.push_str("\n\n");
            out.push_str(&detail);
        }
    }
    out
}

/// The parenthetical annotation for one check's status line, or `None` when there
/// is nothing worth adding beyond the bare `\u{2713}`/`\u{2717} <name>`. This is
/// the trust-carrying part of the summary: it turns three indistinguishable
/// greens (a real N-test pass, a zero-selection vacuous pass, and a reused cache
/// hit) into three visibly different lines.
///
/// - Passing TEST-RUNNER check: `12 tests`, or `no tests matched the change`
///   when the selector executed zero tests (a `related` run that matched nothing).
/// - Passing non-test check (tsc/api/dead-code): `4.1s` on a fresh run (duration
///   is the only meaningful signal; a test count would be a lie).
/// - Failing TEST-RUNNER check: `2 of 40 failed, exit 101`.
/// - Failing non-test check: `exit 101`, or `failed to run` on a spawn error.
/// - A cache hit appends `cached` so a reused verdict never masquerades as fresh.
///
/// Pure, so it is unit-tested directly.
fn summary_annotation(o: &CheckOutcome) -> Option<String> {
    let test_parse = o.parsed.as_ref().filter(|p| p.is_test_runner());
    let mut parts: Vec<String> = Vec::new();
    if o.passed {
        match test_parse {
            Some(p) if p.tests_run() == 0 => parts.push("no tests matched the change".to_string()),
            Some(p) => parts.push(format!("{} tests", p.tests_run())),
            // Non-test check: duration is the only honest signal, and only on a
            // fresh run (a cache hit's stored duration would be misleading).
            None if !o.cached && o.duration_ms > 0 => {
                parts.push(format_check_duration(o.duration_ms))
            }
            None => {}
        }
    } else if let Some(kind) = o.failure_kind {
        // A classified death renders as itself, never a zero-failure assertion
        // count the agent would chase into tests that never failed.
        if kind == CheckFailureKind::RunnerError {
            let passed = o.parsed.as_ref().map(|parsed| parsed.passed).unwrap_or(0);
            if passed == 0 {
                parts.push("test runner failed before reporting tests".to_string());
            } else {
                parts.push(format!(
                    "test runner failed after {passed} tests passed with no assertion failures"
                ));
            }
        } else {
            parts.push(kind.describe(o.duration_ms));
        }
    } else {
        match test_parse {
            Some(p) => {
                let exit = o
                    .exit_code
                    .map(|c| format!(", exit {c}"))
                    .unwrap_or_default();
                parts.push(format!("{} of {} failed{exit}", p.failed, p.tests_run()));
            }
            None => match o.exit_code {
                Some(code) => parts.push(format!("exit {code}")),
                None => parts.push("failed to run".to_string()),
            },
        }
    }
    if o.cached {
        parts.push("cached".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

/// Render a check duration compactly: `4.1s` at or above a second, `850ms` below.
fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// One failing check's detail block: a `\u{2717} <name> \u{2014} N failed: ...`
/// line (when structured names are available) over a fenced, bounded excerpt.
/// `None` when there is nothing to add beyond the header status (no structured
/// names and no output to excerpt). Pure.
/// For a timed-out check, the `N still running at kill: a, b, c +M more` line
/// naming the nextest tests that were mid-flight when the budget expired. `None`
/// when the check did not time out or no running tests could be parsed — the
/// agent's first question is "what was it doing when it died?".
fn timeout_running_names(o: &CheckOutcome) -> Option<String> {
    if o.failure_kind != Some(CheckFailureKind::TimedOut) {
        return None;
    }
    let running = extract_running_tests(&o.output_tail);
    if running.is_empty() {
        return None;
    }
    let shown: Vec<&str> = running
        .iter()
        .take(MAX_FAILURE_NAMES)
        .map(String::as_str)
        .collect();
    let more = running.len().saturating_sub(shown.len());
    let listed = if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    };
    Some(format!("{} still running at kill: {listed}", running.len()))
}

fn format_check_detail(o: &CheckOutcome) -> Option<String> {
    // A timeout has no failing tests to name, but its still-running tests are
    // exactly the detail the agent needs; fall back to parsed failures otherwise.
    let names =
        timeout_running_names(o).or_else(|| o.parsed.as_ref().and_then(format_failure_names));
    let excerpt = format_failure_excerpt(o.parsed.as_ref(), &o.output_tail);
    let head = match names {
        Some(n) => format!("\u{2717} {} \u{2014} {n}", o.name),
        None if excerpt.trim().is_empty() => return None,
        None => format!("\u{2717} {}:", o.name),
    };
    let mut block = head;
    if !excerpt.trim().is_empty() {
        block.push_str("\n```\n");
        block.push_str(excerpt.trim_end());
        block.push_str("\n```");
    }
    Some(block)
}

/// Last `max_chars` characters of `s`, on a char boundary.
fn tail(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    s.chars().skip(count - max_chars).collect()
}

/// The node job's `(base_branch, base_commit)` VCS anchors — the inputs
/// [`node_changed_files`] needs to diff `fork..@`. Mirrors the projection in
/// `resources/files.rs`; empty/missing values fall through to `None`.
async fn load_node_vcs_anchors(db: &LocalDb, job_id: &str) -> (Option<String>, Option<String>) {
    let job_id = job_id.to_string();
    let row = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT base_branch, base_commit FROM jobs WHERE id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match row {
        Ok(Some((branch, commit))) => (
            branch.filter(|s| !s.is_empty()),
            commit.filter(|s| !s.is_empty()),
        ),
        _ => (None, None),
    }
}

async fn submit_review_check(
    orch: &Orchestrator,
    result_identity: crate::execution::cache::CheckResultIdentity,
    request: CellRequest,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    let build_service = orch.build_service_diagnostic_snapshot("sccache");
    if let Some(failure) = active_build_service_failure(&build_service) {
        return Err(CheckExecutionFailure::Substrate(failure));
    }
    let submitted = match orch
        .fleet
        .submit_pure_verdict(orch, result_identity, request)
        .await
    {
        Ok(submitted) => submitted,
        Err(outcome) => {
            return Err(check_result_from_cell_outcome(outcome, None).unwrap_err());
        }
    };
    check_result_from_cell_outcome(submitted.outcome, Some(submitted.publication))
}

fn active_build_service_failure(
    snapshot: &crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot,
) -> Option<String> {
    snapshot.current_failure().map(|failure| {
        format!("Cairn check infrastructure failure: sccache port conflict: {failure}")
    })
}

fn append_sandbox_denial_evidence(
    output: &mut String,
    denials: &[cairn_common::executor_protocol::SandboxDenialEvidence],
) {
    for evidence in denials {
        if !output.is_empty() {
            output.push_str("\n\n");
        }
        let scope = match &evidence.denial {
            cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                format!("path={path}")
            }
            cairn_common::executor_protocol::SandboxDenial::Command => "scope=command".to_string(),
        };
        output.push_str(&format!(
            "Cairn sandbox denial evidence: operation={}, {scope}, command={}, stream={}",
            evidence.operation.as_deref().unwrap_or("unknown"),
            evidence.command,
            evidence.stream_id,
        ));
    }
}

fn append_tracked_modification_evidence(
    output: &mut String,
    evidence: Option<&cairn_common::executor_protocol::TrackedModificationEvidence>,
) {
    let Some(evidence) = evidence else { return };
    if !output.is_empty() {
        output.push_str("\n\n");
    }

    output.push_str(&format!(
        "note: check modified tracked paths: {} ({} files, +{} -{}); changes were discarded",
        evidence.paths.join(", "),
        evidence.files_changed,
        evidence.lines_added,
        evidence.lines_deleted,
    ));
}

fn check_result_from_cell_outcome(
    outcome: CellOutcome,
    publication: Option<crate::fleet::PublicationCoordination>,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    match outcome {
        CellOutcome::Completed {
            exit_code,
            mut output,
            timed_out,
            metadata,
            mutation_delta: None,
            sandbox_denials,
            tracked_modifications,
            ..
        } => {
            let duration_ms = metadata
                .duration_ms
                .map(|duration| i64::try_from(duration).unwrap_or(i64::MAX));
            append_sandbox_denial_evidence(&mut output, &sandbox_denials);
            append_tracked_modification_evidence(&mut output, tracked_modifications.as_ref());
            Ok(CheckExecResult {
                exit_code,
                output,
                timed_out,
                duration_ms,
                provenance: Some(metadata),
                publication,
            })
        }
        CellOutcome::Completed {
            mutation_delta: Some(delta),
            ..
        } => Err(CheckExecutionFailure::Substrate(format!(
            "Cairn check infrastructure failure: cell produced mutation delta {} based on {}",
            delta.delta_commit, delta.base_commit
        ))),
        CellOutcome::FailedAfterExecution { diagnostic, .. } => {
            Err(CheckExecutionFailure::Substrate(format!(
                "Cairn check infrastructure failure: slot result publication failed: {diagnostic}"
            )))
        }
        CellOutcome::Unavailable {
            reason:
                cairn_common::executor_protocol::CellUnavailableReason::Deadline {
                    host_pressure,
                    substrate: Some(substrate),
                },
            diagnostic,
        } => {
            let now = unix_time_ms_for_checks();
            let mut facts = vec![format!(
                "substrate={:?}, lastProgressAge={}ms",
                substrate.state,
                now.saturating_sub(substrate.last_progress_unix_ms)
            )];
            if let Some(depth) = substrate.queue_depth {
                facts.push(format!("queueDepth={depth}"));
            }
            if let Some(position) = substrate.queue_position {
                facts.push(format!("queuePosition={position}"));
            }
            if let Some(active) = substrate.active_cell_count {
                facts.push(format!("activeSlots={active}"));
            }
            if let Some(started) = substrate.oldest_running_started_at_unix_ms {
                facts.push(format!(
                    "oldestRunningAge={}ms",
                    now.saturating_sub(started)
                ));
            }
            if let Some(pressure) = host_pressure {
                facts.push(format!("hostPressure={pressure:?}"));
            }
            Err(CheckExecutionFailure::Substrate(format!(
                "Cairn check infrastructure failure: build-slot deadline ({}) — {diagnostic}",
                facts.join(", ")
            )))
        }
        CellOutcome::Unavailable { reason, diagnostic } => Err(CheckExecutionFailure::Substrate(
            format!("Cairn check infrastructure failure: {reason:?}: {diagnostic}"),
        )),
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            ..
        } => Err(CheckExecutionFailure::Substrate(format!(
            "Cairn check infrastructure storage failure ({stage:?}/{kind:?}): {diagnostic}"
        ))),
        CellOutcome::Cancelled { .. } => Err(CheckExecutionFailure::Substrate(
            "Cairn check infrastructure failure: cell request was cancelled".to_string(),
        )),
    }
}

#[derive(Debug)]
pub(crate) enum ReviewTreeGateResult {
    Green,
    CheckFailed { name: String, detail: String },
    InfrastructureFailure(String),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_review_tree(
    orch: &Orchestrator,
    project_id: &str,
    repository: &str,
    planning_root: &Path,
    commit_id: &str,
    tree_hash: &str,
    tree_entries: &[(String, String)],
    changed: &[GraphFileChange],
    requesting_job_id: &str,
    priority: CellPriority,
) -> ReviewTreeGateResult {
    if changed.is_empty() {
        return ReviewTreeGateResult::Green;
    }
    let Some(checks) = load_live_project_checks(orch, project_id, planning_root).await else {
        return ReviewTreeGateResult::Green;
    };
    let mut plans = applicable_combined_tree_gate_checks(&checks, changed, planning_root);
    if plans.is_empty() {
        return ReviewTreeGateResult::Green;
    }

    let platform = check_platform_identity();
    let toolchain = check_toolchain_identity();
    let mut keyed = Vec::with_capacity(plans.len());
    for mut plan in plans.drain(..) {
        plan.requires_runner_local_admission = false;
        let configured = checks
            .get(&plan.name)
            .expect("planned check must retain its configured definition");
        let key = check_result_key(
            configured,
            Some(tree_entries),
            tree_hash,
            &platform,
            toolchain,
        );
        keyed.push((plan, key));
    }

    let changed_path =
        std::env::temp_dir().join(format!("cairn-merge-checks-{}.txt", uuid::Uuid::new_v4()));
    let changed_body = changed
        .iter()
        .map(|change| change.path.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if let Err(error) = std::fs::write(&changed_path, changed_body) {
        return ReviewTreeGateResult::InfrastructureFailure(format!(
            "failed to prepare changed-file input: {error}"
        ));
    }
    let env = vec![(
        "CAIRN_CHECK_CHANGED_FILES".to_string(),
        changed_path.to_string_lossy().into_owned(),
    )];
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let command_identity_keys: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            check_resource_identity(
                &plan.name,
                checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition"),
            )
            .key
        })
        .collect();
    let constraints: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            checks
                .get(&plan.name)
                .and_then(|check| check.constraints.clone())
        })
        .collect();
    let fleet_config = crate::config::settings::load_fleet(&orch.config_dir);
    let deadline = fleet_config
        .acquisition_deadline_seconds
        .saturating_mul(1_000);
    let project = project_id.to_string();
    let repository = repository.to_string();
    let commit = commit_id.to_string();
    let job = requesting_job_id.to_string();
    let resolved_owner =
        crate::execution::checks_turn_end::resolve_job_coords(&orch.db.local, requesting_job_id)
            .await
            .ok()
            .flatten()
            .map(|coords| cairn_common::executor_protocol::CellOwnerRef {
                project_id: coords.project_id,
                project_key: Some(coords.project_key),
                issue_number: Some(coords.number),
                job_id: Some(requesting_job_id.to_string()),
                execution_seq: Some(coords.exec_seq),
                node_kind: Some(coords.node_segment),
            });
    let keyed_ref = &keyed;
    let outcomes = run_planned_checks(
        orch.db.local.clone(),
        project_id,
        tree_hash,
        requesting_job_id,
        &keyed,
        &format!("merge-gate:{requesting_job_id}"),
        CheckExecMode::Isolated,
        &orch.check_admission,
        Some(orch),
        move |index, command, _| {
            let project = project.clone();
            let repository = repository.clone();
            let commit = commit.clone();
            let job = job.clone();
            let owner = resolved_owner.clone();
            let env = env.clone();
            let timeout_ms = timeouts[index];
            let constraints = constraints[index].clone();
            let command_resource_identity = CommandResourceIdentity {
                version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
                key: command_identity_keys[index].clone(),
            };
            async move {
                let request = CellRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    attempt_id: uuid::Uuid::new_v4().to_string(),
                    project_id: project.clone(),
                    repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
                        project_id: project.clone(),
                        repository_id: project.clone(),
                        absolute_path: repository,
                    },
                    base_commit: commit,
                    command_class: cairn_common::executor_protocol::CellCommandClass::classify(
                        &command,
                    ),
                    command,
                    owner,
                    cwd: String::new(),
                    env,
                    priority,
                    deadline_unix_ms: unix_time_ms_for_checks() + deadline,
                    timeout_ms,
                    mutation_policy: MutationPolicy::PureVerdict,
                    requesting_job_id: Some(job),
                    affinity_key: None,
                    constraints,
                    command_resource_identity: Some(command_resource_identity),
                    resource_reservation: Default::default(),
                    learned_estimate: None,
                };
                submit_review_check(
                    orch,
                    crate::execution::cache::CheckResultIdentity::new(
                        &project,
                        &keyed_ref[index].0.name,
                        &keyed_ref[index].1,
                    ),
                    request,
                )
                .await
            }
        },
        |_| {},
    )
    .await;
    let _ = std::fs::remove_file(changed_path);

    review_tree_gate_result(outcomes)
}

fn review_tree_gate_result(outcomes: Vec<CheckOutcome>) -> ReviewTreeGateResult {
    for outcome in outcomes {
        if outcome.passed {
            continue;
        }
        if outcome
            .failure_kind
            .is_some_and(CheckFailureKind::is_infrastructure)
        {
            return ReviewTreeGateResult::InfrastructureFailure(format!(
                "check '{}': {}",
                outcome.name, outcome.output_tail
            ));
        }
        return ReviewTreeGateResult::CheckFailed {
            name: outcome.name,
            detail: outcome.output_tail,
        };
    }
    ReviewTreeGateResult::Green
}

fn unix_time_ms_for_checks() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_check_env_enforces_line_tables_only_without_mutating_agent_env() {
        let agent_env = vec![
            ("PATH".to_string(), "/tools".to_string()),
            (SLOT_CHECK_DEV_DEBUG_ENV.0.to_string(), "full".to_string()),
        ];
        let slot_env = slot_check_env(agent_env.clone());

        assert_eq!(
            slot_env
                .iter()
                .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
                .map(|(_, value)| value.as_str()),
            Some(SLOT_CHECK_DEV_DEBUG_ENV.1)
        );
        assert_eq!(
            agent_env
                .iter()
                .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
                .map(|(_, value)| value.as_str()),
            Some("full")
        );
    }

    #[test]
    fn dirty_manual_cache_context_rejects_store() {
        let context = ManualCheckCacheContext {
            project_id: "project".into(),
            job_id: "job".into(),
            tree_hash: "tree".into(),
            input_hash: "input".into(),
            cacheable: false,
            entry: None,
        };
        assert!(context.require_cacheable().is_err());
        assert!(ManualCheckCacheContext {
            cacheable: true,
            ..context
        }
        .require_cacheable()
        .is_ok());
    }
    use crate::config::project_settings::{CheckPolicy, CheckResourceClass};
    use crate::db::DbState;
    use crate::execution::selection::CheckScope;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn admission() -> CheckAdmissionController {
        CheckAdmissionController::new(2)
    }

    async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
        let local = LocalDb::open(config_dir.join("checks.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.to_path_buf(),
        )
        .build()
    }

    #[tokio::test]
    async fn write_check_guard_cleans_up_on_drop_and_counts_overlap() {
        let temp = TempDir::new().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let first = WriteChecksInFlightGuard::new(&orch, "job-a");
        let second = WriteChecksInFlightGuard::new(&orch, "job-a");
        assert!(orch.write_checks_in_flight("job-a"));

        drop(first);
        assert!(
            orch.write_checks_in_flight("job-a"),
            "one dropped future must not clear an overlapping write batch",
        );
        drop(second);
        assert!(
            !orch.write_checks_in_flight("job-a"),
            "dropping the final future must clear sidebar state",
        );
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git should run in cache-key fixture");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn git_tree_entries(repo: &Path, revision: &str) -> Vec<(String, String)> {
        git(repo, &["ls-tree", "-r", revision])
            .lines()
            .map(|line| {
                let (metadata, path) = line.split_once('\t').expect("ls-tree row has a path");
                let blob = metadata
                    .split_whitespace()
                    .nth(2)
                    .expect("ls-tree row has an object id");
                (path.to_string(), blob.to_string())
            })
            .collect()
    }

    fn check_ref_exists(repo: &Path, reference: &str) -> bool {
        Command::new("git")
            .args(["show-ref", "--verify", "--quiet", reference])
            .current_dir(repo)
            .status()
            .unwrap()
            .success()
    }

    fn committed_git_repo(temp: &TempDir) -> (std::path::PathBuf, String) {
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        std::fs::write(repo.join("checked.txt"), "sealed").unwrap();
        git(&repo, &["add", "checked.txt"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Cairn Test",
                "-c",
                "user.email=test@cairn.local",
                "commit",
                "-q",
                "-m",
                "sealed",
            ],
        );
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        (repo, commit)
    }

    #[tokio::test]
    async fn post_publication_verification_failure_removes_temporary_check_ref() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (repo, commit) = committed_git_repo(&temp);
        let store = temp.path().join("store");
        let request_id = "verification-failure";
        let reference = format!("refs/cairn/checks/{request_id}");

        let error = publish_check_commit_ref_with_verifier(
            &orch,
            &repo,
            &store,
            &commit,
            request_id,
            |_repo, _reference| Err("injected post-publication verification failure".into()),
        )
        .await
        .unwrap_err();

        assert!(error.contains("injected post-publication verification failure"));
        assert!(!check_ref_exists(&repo, &reference));
    }

    #[tokio::test]
    async fn dropped_temporary_check_ref_retries_cleanup_under_store_lock() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (repo, commit) = committed_git_repo(&temp);
        let store = temp.path().join("store");
        let reference = "refs/cairn/checks/cancelled".to_string();
        git(&repo, &["update-ref", &reference, &commit]);
        let held = orch
            .acquire_jj_store_lock(&store, "hold cancellation cleanup test")
            .await;

        drop(TemporaryCheckRef {
            orch: orch.clone(),
            repository: repo.clone(),
            store_dir: store,
            reference: reference.clone(),
            commit,
            armed: true,
        });
        tokio::task::yield_now().await;
        assert!(
            check_ref_exists(&repo, &reference),
            "cleanup must not bypass the held store lock"
        );
        drop(held);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while check_ref_exists(&repo, &reference) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation cleanup removes the ref after acquiring the store lock");
    }

    #[test]
    fn result_key_is_impact_filtered_and_includes_definition_platform_and_toolchain() {
        let mut definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        let base = vec![
            ("src/lib.rs".to_string(), "blob-a".to_string()),
            ("docs/readme.md".to_string(), "docs-a".to_string()),
        ];
        let key = check_result_key(
            &definition,
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
        );

        let mut changed_definition = definition.clone();
        changed_definition.command = "cargo test --all".to_string();
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.policy = CheckPolicy::Gate;
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.resource_class = CheckResourceClass::Exclusive;
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.timeout = Some(42);
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.constraints =
            Some(cairn_common::executor_protocol::PlacementConstraints {
                executor_id: Some("executor-a".to_string()),
                device_id: Some("device-a".to_string()),
                os: Some("macos".to_string()),
                arch: Some("aarch64".to_string()),
                required_toolchains: vec!["rust".to_string()],
            });
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        assert_ne!(
            key,
            check_result_key(
                &definition,
                Some(&base),
                "whole-tree-a",
                "linux-x86_64",
                "rustc=1;bun=1"
            )
        );
        assert_ne!(
            key,
            check_result_key(
                &definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=2;bun=1"
            )
        );

        let inside_changed = vec![
            ("src/lib.rs".to_string(), "blob-b".to_string()),
            ("docs/readme.md".to_string(), "docs-a".to_string()),
        ];
        assert_ne!(
            key,
            check_result_key(
                &definition,
                Some(&inside_changed),
                "whole-tree-b",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        let outside_changed = vec![
            ("src/lib.rs".to_string(), "blob-a".to_string()),
            ("docs/readme.md".to_string(), "docs-b".to_string()),
        ];
        assert_eq!(
            key,
            check_result_key(
                &definition,
                Some(&outside_changed),
                "whole-tree-b",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );

        definition.impact = Some(vec!["docs/**".to_string()]);
        assert_ne!(
            key,
            check_result_key(
                &definition,
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
    }

    #[test]
    fn resource_identity_is_stable_across_tree_states() {
        let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        assert_ne!(
            check_result_key(&definition, None, "tree-one", "platform", "toolchain"),
            check_result_key(&definition, None, "tree-two", "platform", "toolchain"),
        );
        assert_eq!(
            check_resource_identity("rust-full", &definition),
            check_resource_identity("rust-full", &definition),
        );
        assert_ne!(
            check_resource_identity("rust-full", &definition),
            check_resource_identity("rust-fmt", &definition),
        );
    }

    #[test]
    fn repository_rust_checks_select_all_cargo_control_inputs() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("cairn-core manifest is nested under the repository root");
        let checks = load_checks(repo_root).expect("repository check configuration must parse");
        for name in ["rust-lint", "rust-full"] {
            let check = checks.get(name).expect("configured Rust review check");
            assert_eq!(check.when, CheckWhen::Review);
            let impacts = check.impact.as_ref().expect("Rust check has impacts");
            for path in [
                "src-tauri/.cargo/config.toml",
                "src-tauri/Cargo.toml",
                "src-tauri/os/cairn-core/Cargo.toml",
                "src-tauri/Cargo.lock",
                "src-tauri/os/cairn-core/build.rs",
            ] {
                let planned = plan_checks(
                    &HashMap::from([(name.to_string(), check.clone())]),
                    &[change(path)],
                    repo_root,
                );
                assert!(
                    planned.iter().any(|plan| plan.name == name && plan.applies),
                    "{name} must apply when {path} changes; impacts={impacts:?}"
                );
            }
        }
    }

    #[test]
    fn result_key_uses_tree_content_not_commit_history() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path();
        git(repo, &["init", "--quiet"]);
        git(repo, &["config", "user.name", "Cairn Test"]);
        git(repo, &["config", "user.email", "cairn@example.test"]);
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
        std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
        git(repo, &["add", "."]);
        git(repo, &["commit", "--quiet", "-m", "first message"]);

        let first_commit = git(repo, &["rev-parse", "HEAD"]);
        let tree = git(repo, &["rev-parse", "HEAD^{tree}"]);
        let second_commit = {
            let mut child = Command::new("git")
                .args(["commit-tree", tree.as_str(), "-p", first_commit.as_str()])
                .current_dir(repo)
                .env("GIT_AUTHOR_NAME", "Cairn Test")
                .env("GIT_AUTHOR_EMAIL", "cairn@example.test")
                .env("GIT_COMMITTER_NAME", "Cairn Test")
                .env("GIT_COMMITTER_EMAIL", "cairn@example.test")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"different message and parent\n")
                .unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };
        assert_ne!(first_commit, second_commit);

        let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        let first_entries = git_tree_entries(repo, &first_commit);
        let second_entries = git_tree_entries(repo, &second_commit);
        assert_eq!(
            check_result_key(
                &definition,
                Some(&first_entries),
                &tree,
                "test-platform",
                "test-toolchain",
            ),
            check_result_key(
                &definition,
                Some(&second_entries),
                &tree,
                "test-platform",
                "test-toolchain",
            ),
            "identical impact-filtered trees must hit despite different commit history"
        );
    }

    fn change(path: &str) -> GraphFileChange {
        GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: "modified".to_string(),
            additions: 1,
            deletions: 0,
        }
    }

    fn check(command: &str, impact: Option<&[&str]>, when: CheckWhen) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: impact.map(|globs| globs.iter().map(|s| s.to_string()).collect()),
            policy: CheckPolicy::Advisory,
            when,
            resource_class: CheckResourceClass::Shared,
            timeout: None,
            constraints: None,
        }
    }

    fn cache_entry(check_name: &str, tree_hash: &str, passed: bool) -> CheckResultCacheEntry {
        CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: tree_hash.to_string(),
            input_hash: format!("input-{tree_hash}"),
            check_name: check_name.to_string(),
            exit_code: if passed { 0 } else { 1 },
            passed,
            output_tail: String::new(),
            duration_ms: 1,
            ran_at: 1,
            target_results_json: None,
            job_id: None,
            cached: None,
            failure_kind: None,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
        }
    }

    /// The repo's `checks` shape: two `when:write` checks (frontend, typecheck)
    /// scoped to the frontend trees, one `when:review` check (rust) scoped to
    /// src-tauri.
    fn repo_checks() -> HashMap<String, CheckCommand> {
        let mut checks = HashMap::new();
        checks.insert(
            "frontend".to_string(),
            check(
                "bunx vitest related {changedFiles}",
                Some(&["src/**", "packages/ui/**"]),
                CheckWhen::Write,
            ),
        );
        checks.insert(
            "typecheck".to_string(),
            check(
                "bunx tsc --noEmit",
                Some(&["src/**", "packages/ui/**"]),
                CheckWhen::Write,
            ),
        );
        checks.insert(
            "rust".to_string(),
            check(
                "bun run test:rust",
                Some(&["src-tauri/**"]),
                CheckWhen::Review,
            ),
        );
        checks
    }

    // --- the write-cadence gate -------------------------------------------

    #[test]
    fn gate_selects_write_checks_for_a_src_change() {
        let plans =
            applicable_write_checks(&repo_checks(), &[change("src/App.tsx")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["frontend", "typecheck"]);
    }

    #[test]
    fn gate_is_empty_for_a_doc_only_change() {
        let plans =
            applicable_write_checks(&repo_checks(), &[change("docs/x.md")], Path::new("/repo"));
        assert!(plans.is_empty(), "a doc-only commit triggers no checks");
    }

    #[test]
    fn gate_excludes_review_checks_for_a_rust_change() {
        let plans = applicable_write_checks(
            &repo_checks(),
            &[change("src-tauri/os/cairn-core/src/lib.rs")],
            Path::new("/repo"),
        );
        // rust matches the change but is when:review; frontend/typecheck do not
        // match the src-tauri impact ⇒ nothing applies at the write cadence.
        assert!(
            !plans.iter().any(|p| p.name == "rust"),
            "a when:review check never runs on write"
        );
        assert!(plans.is_empty());
    }

    // --- contract source: live project config wins over the worktree -------

    /// Write a minimal `.cairn/config.yaml` declaring one `when:write` check.
    fn write_checks_config(dir: &Path, check_name: &str) {
        let path = crate::config::project_settings::get_project_config_path(dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "checks:\n  {check_name}:\n    command: run-it\n    impact:\n      - src/**\n    when: write\n"
            ),
        )
        .unwrap();
    }

    #[test]
    fn checks_from_source_prefers_project_over_worktree() {
        let project = tempfile::TempDir::new().unwrap();
        let worktree = tempfile::TempDir::new().unwrap();
        // Project (live) config and the worktree's committed config disagree:
        // the project edited its checks after this branch was cut.
        write_checks_config(project.path(), "frontend");
        write_checks_config(worktree.path(), "typecheck");

        let checks = checks_from_source(Some(project.path()), worktree.path()).unwrap();
        assert!(
            checks.contains_key("frontend"),
            "the live project contract must win"
        );
        assert!(
            !checks.contains_key("typecheck"),
            "the stale worktree contract must not leak in"
        );
    }

    #[test]
    fn checks_from_source_falls_back_to_worktree_when_project_unresolved() {
        let worktree = tempfile::TempDir::new().unwrap();
        write_checks_config(worktree.path(), "typecheck");
        // No resolvable project repo path (e.g. a team project with no local
        // clone): fall back to the worktree's own committed contract rather than
        // silently dropping every check.
        let checks = checks_from_source(None, worktree.path()).unwrap();
        assert!(checks.contains_key("typecheck"));
    }

    #[test]
    fn checks_from_source_none_when_neither_declares_checks() {
        let project = tempfile::TempDir::new().unwrap();
        let worktree = tempfile::TempDir::new().unwrap();
        assert!(checks_from_source(Some(project.path()), worktree.path()).is_none());
    }

    // --- passing-baseline delta selection ---------------------------------

    #[test]
    fn tree_entry_delta_reports_added_changed_removed_under_impact_globs() {
        let baseline = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
            ("src/removed.ts".to_string(), "r1".to_string()),
            ("docs/ignored.md".to_string(), "d1".to_string()),
        ];
        let current = vec![
            ("src/a.ts".to_string(), "a2".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
            ("src/added.ts".to_string(), "n1".to_string()),
            ("docs/ignored.md".to_string(), "d2".to_string()),
        ];
        let impact = vec!["src/**".to_string()];

        let delta = diff_tree_entries_for_impact(&baseline, &current, Some(&impact)).unwrap();
        let observed: Vec<(&str, &str)> = delta
            .iter()
            .map(|change| (change.path.as_str(), change.status.as_str()))
            .collect();
        assert_eq!(
            observed,
            vec![
                ("src/a.ts", "modified"),
                ("src/added.ts", "added"),
                ("src/removed.ts", "deleted"),
            ]
        );
    }

    #[test]
    fn baseline_decision_uses_delta_only_from_passing_baseline() {
        let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
        let current = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/a.ts"), change("src/b.ts")];
        let passing = cache_entry("frontend", "tree-a", true);
        let failing = cache_entry("frontend", "tree-a", false);

        let narrowed = baseline_delta_changed_files(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(
            narrowed.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
            vec!["src/b.ts"]
        );

        let from_fail = baseline_delta_changed_files(
            Some(&failing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(from_fail, cumulative);

        let from_missing = baseline_delta_changed_files(
            None,
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(from_missing, cumulative);
    }

    #[test]
    fn baseline_decision_falls_back_to_cumulative_on_empty_or_uncertain_delta() {
        let entries = vec![("src/a.ts".to_string(), "a1".to_string())];
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/a.ts")];
        let passing = cache_entry("frontend", "tree-a", true);

        let empty_delta = baseline_delta_changed_files(
            Some(&passing),
            Some(&entries),
            Some(&entries),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(empty_delta, cumulative);

        let unreadable_current = baseline_delta_changed_files(
            Some(&passing),
            Some(&entries),
            None,
            Some(&impact),
            &cumulative,
        );
        assert_eq!(unreadable_current, cumulative);
    }

    #[test]
    fn passing_baseline_delta_replans_changed_files_selector_to_new_file_only() {
        // Commit A touched f1 and passed, so the cached baseline tree contains f1.
        let baseline = vec![("src/f1.ts".to_string(), "f1-a".to_string())];
        // Commit B touches f2. The cumulative branch diff still contains f1 and f2,
        // but a passing baseline makes the safe selector just the tree delta: f2.
        let current = vec![
            ("src/f1.ts".to_string(), "f1-a".to_string()),
            ("src/f2.ts".to_string(), "f2-b".to_string()),
        ];
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/f1.ts"), change("src/f2.ts")];
        let passing = cache_entry("frontend", "tree-a", true);
        let selected = baseline_delta_changed_files(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        let check = check(
            "bunx vitest related --reporter=json {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );

        let plan = replan_one_check("frontend", &check, &selected, Path::new("/repo")).unwrap();
        assert_eq!(
            plan.command,
            "bunx vitest related --reporter=json src/f2.ts"
        );
        assert_eq!(plan.scope, CheckScope::Partial);
    }

    // --- the turn-end-cadence gate ----------------------------------------

    /// A checks map with one check per cadence, all scoped to `src/**`.
    fn cadence_checks() -> HashMap<String, CheckCommand> {
        let mut checks = HashMap::new();
        checks.insert(
            "w".to_string(),
            check("run-w", Some(&["src/**"]), CheckWhen::Write),
        );
        checks.insert(
            "r".to_string(),
            check("run-r", Some(&["src/**"]), CheckWhen::Review),
        );
        checks
    }

    #[test]
    fn turn_end_gate_runs_review_not_write() {
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["r"],
            "review runs at every turn-end; write never runs here"
        );
    }

    #[test]
    fn turn_end_selection_keeps_advisory_and_gate_review_checks() {
        let mut checks = cadence_checks();
        let mut gate = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
        gate.policy = CheckPolicy::Gate;
        checks.insert("g".to_string(), gate);

        let plans =
            applicable_turn_end_checks(&checks, &[change("src/App.tsx")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(names, vec!["g", "r"]);
    }

    #[test]
    fn combined_tree_selection_excludes_default_advisory_review_checks() {
        let default_advisory: CheckCommand =
            serde_yaml::from_str("command: run-advisory\nimpact:\n  - src/**\nwhen: review\n")
                .unwrap();
        assert_eq!(default_advisory.policy, CheckPolicy::Advisory);
        let checks = HashMap::from([("advisory".to_string(), default_advisory)]);

        let plans = applicable_combined_tree_gate_checks(
            &checks,
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn combined_tree_selection_keeps_only_applicable_review_gates() {
        let mut applicable = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
        applicable.policy = CheckPolicy::Gate;
        let mut impact_mismatch = check("run-docs", Some(&["docs/**"]), CheckWhen::Review);
        impact_mismatch.policy = CheckPolicy::Gate;
        let mut write = check("run-write", Some(&["src/**"]), CheckWhen::Write);
        write.policy = CheckPolicy::Gate;
        let checks = HashMap::from([
            ("applicable".to_string(), applicable),
            ("impact-mismatch".to_string(), impact_mismatch),
            ("write".to_string(), write),
        ]);

        let plans = applicable_combined_tree_gate_checks(
            &checks,
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(names, vec!["applicable"]);
    }

    #[test]
    fn turn_end_gate_runs_an_idle_aliased_check() {
        // `when: idle` in a project config deserializes to CheckWhen::Review, so
        // an un-migrated check still runs at turn-end (the alias path).
        let aliased: CheckWhen = serde_yaml::from_str("idle").unwrap();
        assert_eq!(aliased, CheckWhen::Review);
        let mut checks = HashMap::new();
        checks.insert(
            "legacy".to_string(),
            check("run", Some(&["src/**"]), aliased),
        );
        let plans =
            applicable_turn_end_checks(&checks, &[change("src/App.tsx")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["legacy"]);
    }

    #[test]
    fn turn_end_gate_excludes_a_non_matching_impact() {
        // A doc-only change matches no impact glob, so nothing applies.
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &[change("docs/x.md")],
            Path::new("/repo"),
        );
        assert!(plans.is_empty());
    }

    // --- summary formatting -----------------------------------------------

    /// A bare outcome with no structured detail and no output tail, so the
    /// summary renders only the header status line.
    fn outcome(name: &str, passed: bool, exit_code: Option<i32>) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code,
            failure_kind: None,
            parsed: None,
            output_tail: String::new(),
            cached: false,
            duration_ms: 0,
        }
    }

    /// A test-runner parse with explicit pass/fail counts, so the summary's
    /// count-bearing annotations are exercised without a real runner.
    fn runner_parse(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
        ParsedCheckResult {
            parser: parser.to_string(),
            passed,
            failed,
            skipped: 0,
            failures: (0..failed)
                .map(|i| crate::execution::check_parsers::CheckFailure {
                    name: format!("t{i}"),
                    message: None,
                })
                .collect(),
        }
    }

    #[test]
    fn summary_renders_pass_and_fail() {
        // No structured detail / output ⇒ header line only.
        let s = format_check_summary(&[
            outcome("frontend", true, Some(0)),
            outcome("typecheck", false, Some(1)),
        ]);
        assert_eq!(s, "\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)");
    }

    #[test]
    fn summary_renders_spawn_failure_without_exit_code() {
        let s = format_check_summary(&[outcome("frontend", false, None)]);
        assert_eq!(s, "\u{2717} frontend (failed to run)");
    }

    #[test]
    fn summary_appends_failing_test_names_and_excerpt() {
        let parsed = crate::execution::check_parsers::parse_check_output(
            "bunx tsc --noEmit",
            "a.ts(1,7): error TS2322: Type 'string' is not assignable to type 'number'.",
        );
        let results = vec![CheckOutcome {
            name: "typecheck".to_string(),
            passed: false,
            exit_code: Some(1),
            failure_kind: None,
            parsed,
            output_tail: "raw output tail".to_string(),
            cached: false,
            duration_ms: 0,
        }];
        let s = format_check_summary(&results);
        // Header status line first.
        assert!(s.starts_with("\u{2717} typecheck (exit 1)"));
        // Then a detail block naming the failing test and quoting the error.
        assert!(s.contains("\u{2717} typecheck \u{2014} 1 failed: a.ts(1,7)"));
        assert!(s.contains("TS2322: Type 'string' is not assignable"));
    }

    /// Build a passing/failing outcome carrying a runner parse, for the
    /// count-bearing annotation tests.
    fn parsed_outcome(
        name: &str,
        passed: bool,
        exit_code: Option<i32>,
        parsed: ParsedCheckResult,
        cached: bool,
    ) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code,
            failure_kind: None,
            parsed: Some(parsed),
            output_tail: String::new(),
            cached,
            duration_ms: 0,
        }
    }

    #[test]
    fn summary_shows_test_count_on_a_passing_runner_check() {
        let o = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 12, 0),
            false,
        );
        assert_eq!(format_check_summary(&[o]), "\u{2713} frontend (12 tests)");
    }

    #[test]
    fn summary_flags_a_zero_selection_pass_honestly() {
        // A `related` selector that matched nothing exits 0 but validated nothing:
        // the annotation must say so rather than render a bare green.
        let o = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 0, 0),
            false,
        );
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2713} frontend (no tests matched the change)"
        );
    }

    #[test]
    fn summary_shows_pass_of_total_on_a_failing_runner_check() {
        let o = parsed_outcome(
            "rust",
            false,
            Some(101),
            runner_parse("nextest", 38, 2),
            false,
        );
        let s = format_check_summary(&[o]);
        assert!(
            s.starts_with("\u{2717} rust (2 of 40 failed, exit 101)"),
            "got: {s}"
        );
    }

    #[test]
    fn summary_shows_duration_on_a_passing_unparsed_check() {
        // typecheck / api have no test-runner parse; a fresh pass shows duration.
        let mut o = outcome("typecheck", true, Some(0));
        o.duration_ms = 4100;
        assert_eq!(format_check_summary(&[o]), "\u{2713} typecheck (4.1s)");
    }

    #[test]
    fn summary_annotates_a_cache_hit() {
        // A reused verdict is distinguishable from a fresh run. Duration is
        // suppressed for a cache hit (it belonged to the original run).
        let mut o = outcome("typecheck", true, Some(0));
        o.cached = true;
        o.duration_ms = 4100;
        assert_eq!(format_check_summary(&[o]), "\u{2713} typecheck (cached)");

        // A cached test-runner pass keeps its count AND flags the reuse.
        let cached_runner = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 7, 0),
            true,
        );
        assert_eq!(
            format_check_summary(&[cached_runner]),
            "\u{2713} frontend (7 tests, cached)"
        );
    }

    #[test]
    fn fixed_summary_attributes_a_combined_delta_once_at_batch_level() {
        let summary = "Checks: ✓ rust-fmt (2.1s) · ✓ migrations (cached)";
        let rendered = format_fixed_batch_summary(
            summary,
            "1234567890abcdef",
            &["src/lib.rs".into(), "src/main.rs".into()],
        );
        assert_eq!(
            rendered,
            "Checks: ✓ write-check fixes (fixed, 1234567890ab; 2 files) · ✓ rust-fmt (2.1s) · ✓ migrations (cached)"
        );
        assert!(!rendered.contains("migrations (fixed"));
    }

    // --- timeout budgets + failure classification -------------------------

    #[test]
    fn sandbox_denial_evidence_is_legible_without_overwriting_command_output() {
        let mut output = "1,412 tests passed".to_string();
        append_sandbox_denial_evidence(
            &mut output,
            &[cairn_common::executor_protocol::SandboxDenialEvidence {
                denial: cairn_common::executor_protocol::SandboxDenial::Path(
                    "/tmp/tool-cache".into(),
                ),
                operation: Some("file-write-create".into()),
                command: "bunx vitest run".into(),
                stream_id: "turn-checks:1".into(),
            }],
        );

        assert!(output.starts_with("1,412 tests passed"));
        assert!(output.contains("operation=file-write-create"));
        assert!(output.contains("path=/tmp/tool-cache"));
        assert!(output.contains("command=bunx vitest run"));
        assert!(output.contains("stream=turn-checks:1"));
    }

    #[test]
    fn tracked_modification_evidence_is_legible_without_overwriting_command_output() {
        let mut output = "cargo check failed with code 101".to_string();
        append_tracked_modification_evidence(
            &mut output,
            Some(
                &cairn_common::executor_protocol::TrackedModificationEvidence {
                    paths: vec!["Cargo.lock".into(), "src/generated.rs".into()],
                    files_changed: 2,
                    lines_added: 4,
                    lines_deleted: 1,
                },
            ),
        );

        assert!(output.starts_with("cargo check failed with code 101"));
        assert!(output.contains("check modified tracked paths: Cargo.lock, src/generated.rs"));
        assert!(output.contains("2 files, +4 -1"));
        assert!(output.ends_with("changes were discarded"));
    }

    #[test]
    fn timeout_budget_formats_minutes_and_seconds() {
        assert_eq!(format_timeout_budget(600_000), "10m");
        assert_eq!(format_timeout_budget(1_800_000), "30m");
        assert_eq!(format_timeout_budget(45_000), "45s");
        assert_eq!(format_timeout_budget(0), "0s");
    }

    #[test]
    fn resolve_timeout_prefers_schema_then_default_then_cap() {
        let default_ms = DEFAULT_REVIEW_CHECK_TIMEOUT_MS;
        // No check / no schema timeout ⇒ the cadence default.
        assert_eq!(resolve_check_timeout_ms(None, default_ms), default_ms);
        let mut c = check("run", None, CheckWhen::Review);
        assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), default_ms);
        // A schema timeout (SECONDS) wins, converted to ms.
        c.timeout = Some(120);
        assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), 120_000);
        // An absurd value is clamped to the hard 60-minute ceiling.
        c.timeout = Some(10_000);
        assert_eq!(
            resolve_check_timeout_ms(Some(&c), default_ms),
            MAX_CHECK_TIMEOUT_MS
        );
    }

    #[test]
    fn defaults_give_the_heavy_review_cadence_more_headroom() {
        // The whole point: review's default must sit well above the 10-min wall
        // the write cadence keeps, or a healthy-but-slow suite is guillotined
        // again (dozens of `rust-full` rows were killed at ~600s). Bind to locals
        // so the guards aren't flagged as constant-value assertions.
        let (write, review) = (
            DEFAULT_WRITE_CHECK_TIMEOUT_MS,
            DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
        );
        assert_eq!(write, 600_000);
        assert!(
            review >= 1_800_000,
            "review default must cover a cold, uncached full Rust build"
        );
        assert!(
            review > write,
            "review default must exceed the tighter write default"
        );
    }

    #[tokio::test]
    async fn merge_gate_classifies_all_substrate_failures_as_infrastructure() {
        use cairn_common::executor_protocol::{
            CellUnavailableReason::*, ObjectInfrastructureStage,
        };
        let unavailable = vec![
            Deadline {
                host_pressure: None,
                substrate: None,
            },
            Provisioning,
            Checkout,
            Spawn,
            Preparation,
            ExecutorUnavailable,
            NoMatchingExecutor,
            ObjectInfrastructure(ObjectInfrastructureStage::FetchInterrupted),
            ObjectInfrastructure(ObjectInfrastructureStage::IntegrityFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::IncompleteClosure),
            ObjectInfrastructure(ObjectInfrastructureStage::InstallFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::UploadFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::ExpiredReceipt),
            ObjectInfrastructure(ObjectInfrastructureStage::StaleReceipt),
        ];
        let mut failures = unavailable
            .into_iter()
            .map(|reason| CellOutcome::Unavailable {
                reason,
                diagnostic: "unavailable".to_string(),
            })
            .collect::<Vec<_>>();
        failures.push(CellOutcome::FailedAfterExecution {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            diagnostic: "publication failed".to_string(),
        });
        failures.push(CellOutcome::Cancelled {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
        });
        failures.push(CellOutcome::Completed {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            exit_code: Some(0),
            output: String::new(),
            timed_out: false,
            metadata: cairn_common::executor_protocol::CellExecutionMeta {
                executor_id: "executor".to_string(),
                executor_device_id: "device".to_string(),
                executor_connection_generation: 1,
                cell_id: "slot".to_string(),
                lease_epoch: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
            },
            mutation_delta: Some(Box::new(cairn_common::executor_protocol::MutationDelta {
                base_commit: "base".to_string(),
                delta_commit: "delta".to_string(),
                upload_receipt: None,
            })),
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
        });

        for (index, outcome) in failures.into_iter().enumerate() {
            let failure = check_result_from_cell_outcome(outcome, None)
                .expect_err("substrate outcome must not become a command verdict");
            let db = cache_db().await;
            let results = run_planned_checks(
                db,
                "project-a",
                &format!("tree-{index}"),
                "job-a",
                &[(plan("rust", "cargo test"), format!("input-{index}"))],
                "tool",
                CheckExecMode::Shared,
                &admission(),
                None,
                move |_, _, _| {
                    let failure = failure.clone();
                    async move { Err::<CheckExecResult, _>(failure) }
                },
                |_| {},
            )
            .await;
            assert!(matches!(
                review_tree_gate_result(results),
                ReviewTreeGateResult::InfrastructureFailure(_)
            ));
        }
    }

    #[tokio::test]
    async fn completed_executor_provenance_is_persisted_with_toolchain_claim() {
        let db = cache_db().await;
        let provenance = cairn_common::executor_protocol::CellExecutionMeta {
            executor_id: "executor-a".to_string(),
            executor_device_id: "device-a".to_string(),
            executor_connection_generation: 3,
            cell_id: "slot-a".to_string(),
            lease_epoch: 4,
            started_at_unix_ms: 100,
            finished_at_unix_ms: 200,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            disk_delta_bytes: None,
            measurement_quality: None,
        };
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-provenance",
            "job-a",
            &[(plan("rust", "cargo test"), "input-provenance".to_string())],
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_, _, _| {
                let provenance = provenance.clone();
                async move {
                    Ok::<_, String>(CheckExecResult {
                        exit_code: Some(0),
                        output: "ok".to_string(),
                        timed_out: false,
                        duration_ms: Some(12_345),
                        provenance: Some(provenance),
                        publication: None,
                    })
                }
            },
            |_| {},
        )
        .await;
        assert!(results[0].passed);
        assert_eq!(results[0].duration_ms, 12_345);
        let row = get_check_result(db, "project-a", "rust", "input-provenance")
            .unwrap()
            .expect("completed verdict is reusable");
        assert_eq!(row.executor_id.as_deref(), Some("executor-a"));
        assert_eq!(row.executor_device_id.as_deref(), Some("device-a"));
        assert_eq!(row.executor_connection_generation, Some(3));
        assert_eq!(row.executor_cell_id.as_deref(), Some("slot-a"));
        assert_eq!(row.executor_lease_epoch, Some(4));
        assert_eq!(row.executor_started_at_unix_ms, Some(100));
        assert_eq!(row.executor_finished_at_unix_ms, Some(200));
        assert_eq!(row.duration_ms, 12_345);
        assert_eq!(
            row.toolchain_fingerprint.as_deref(),
            Some(check_toolchain_identity())
        );
    }

    #[test]
    fn merge_gate_treats_command_timeout_as_named_check_failure() {
        let result = review_tree_gate_result(vec![CheckOutcome {
            name: "rust-full".to_string(),
            passed: false,
            exit_code: None,
            failure_kind: Some(CheckFailureKind::TimedOut),
            parsed: None,
            output_tail: "timed out after 30m".to_string(),
            cached: false,
            duration_ms: 1_800_000,
        }]);
        assert!(matches!(
            result,
            ReviewTreeGateResult::CheckFailed { ref name, ref detail }
                if name == "rust-full" && detail.contains("timed out")
        ));
    }

    #[test]
    fn infrastructure_predicate_covers_operator_owned_failures() {
        assert!(CheckFailureKind::SpawnError.is_infrastructure());
        assert!(CheckFailureKind::Infrastructure.is_infrastructure());
        assert!(CheckFailureKind::RunnerError.is_infrastructure());
        assert!(!CheckFailureKind::TimedOut.is_infrastructure());
        assert!(!CheckFailureKind::Killed.is_infrastructure());
    }

    #[test]
    fn failure_kind_describe_names_each_death() {
        assert_eq!(
            CheckFailureKind::TimedOut.describe(600_000),
            "timed out after 10m"
        );
        assert_eq!(CheckFailureKind::SpawnError.describe(6), "failed to spawn");
        assert_eq!(CheckFailureKind::Killed.describe(67_000), "killed (signal)");
    }

    #[test]
    fn failure_kind_round_trips_through_its_string() {
        for kind in [
            CheckFailureKind::TimedOut,
            CheckFailureKind::SpawnError,
            CheckFailureKind::Killed,
        ] {
            assert_eq!(CheckFailureKind::from_stored(kind.as_str()), Some(kind));
        }
        assert_eq!(CheckFailureKind::from_stored("nonsense"), None);
    }

    /// String-level guard on the acceptance requirement: a timed-out check must
    /// render "timed out after …", never a bare exit code, so the wording cannot
    /// silently regress to a generic failure that sends an agent debugging tests
    /// that never failed.
    #[test]
    fn summary_renders_a_timeout_as_a_timeout_not_an_exit_code() {
        let mut o = outcome("rust-full", false, None);
        o.failure_kind = Some(CheckFailureKind::TimedOut);
        o.duration_ms = 1_800_000;
        let s = format_check_summary(&[o]);
        assert!(s.contains("timed out after 30m"), "got: {s}");
        assert!(!s.contains("failed to run"), "got: {s}");
        assert!(!s.contains("exit"), "got: {s}");
    }

    #[test]
    fn summary_renders_a_spawn_error_legibly() {
        let mut o = outcome("rust-lint", false, None);
        o.failure_kind = Some(CheckFailureKind::SpawnError);
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2717} rust-lint (failed to spawn)"
        );
    }

    // --- cache hit / miss at the runner seam ------------------------------

    async fn cache_db() -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db("when-write-runner-test.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);",
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    fn plan(name: &str, command: &str) -> CheckPlan {
        CheckPlan {
            name: name.to_string(),
            applies: true,
            command: command.to_string(),
            scope: CheckScope::Full,
            resource_class: CheckResourceClass::Shared,
            requires_runner_local_admission: true,
        }
    }

    /// A fake successful (or non-zero-exit) check run for the `run_planned_checks`
    /// harness: a completed process that did not time out. Timeout / spawn / signal
    /// cases build [`CheckExecResult`] / `Err` explicitly.
    fn exec_ok(
        exit_code: Option<i32>,
        output: impl Into<String>,
    ) -> Result<CheckExecResult, String> {
        Ok(CheckExecResult {
            exit_code,
            output: output.into(),
            timed_out: false,
            duration_ms: None,
            provenance: None,
            publication: None,
        })
    }

    #[tokio::test]
    async fn cache_hit_skips_execution() {
        let db = cache_db().await;
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-frontend".to_string(),
                check_name: "frontend".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: "cached".to_string(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
                executor_id: None,
                executor_device_id: None,
                executor_connection_generation: None,
                executor_cell_id: None,
                executor_lease_epoch: None,
                executor_started_at_unix_ms: None,
                executor_finished_at_unix_ms: None,
                toolchain_fingerprint: None,
            },
        )
        .unwrap();

        let plans = vec![(
            plan("frontend", "bunx vitest run"),
            "ih-frontend".to_string(),
        )];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let controller = CheckAdmissionController::new(1);
        let _blocker = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-a",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &controller,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a cache hit must not re-run the check"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "frontend");
        assert!(results[0].passed);
        assert_eq!(results[0].exit_code, Some(0));
    }

    #[tokio::test]
    async fn cancelling_queued_miss_spawns_nothing_and_stores_nothing() {
        let db = cache_db().await;
        let controller = Arc::new(CheckAdmissionController::new(1));
        let blocker = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let task = {
            let db = db.clone();
            let controller = controller.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                let plans = vec![(plan("queued", "run-queued"), "ih-queued".to_string())];
                run_planned_checks(
                    db,
                    "project-a",
                    "tree-cancelled",
                    "job-a",
                    &plans,
                    "tool",
                    CheckExecMode::Isolated,
                    controller.as_ref(),
                    None,
                    move |_index, _command, _stream_id| {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            exec_ok(Some(0), "should not run")
                        }
                    },
                    |_| {},
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(controller.snapshot().queued_requests, 1);
        task.abort();
        let _ = task.await;
        drop(blocker);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            crate::execution::cache::list_check_results(db, "project-a", "tree-cancelled")
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cache_miss_runs_then_stores() {
        let db = cache_db().await;
        let plans = vec![(plan("frontend", "bunx vitest run"), "ih-b".to_string())];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-b",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(1), "vitest failed")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a miss runs the check once"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "frontend");
        assert!(!results[0].passed);
        assert_eq!(results[0].exit_code, Some(1));

        assert!(
            get_check_result(db.clone(), "project-a", "frontend", "ih-b")
                .unwrap()
                .is_none(),
            "a failed verdict is stored for visibility but is not reusable"
        );
        let stored =
            crate::execution::cache::list_check_results(db, "project-a", "tree-b").unwrap();
        assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
        let stored = &stored[0];
        assert_eq!(stored.input_hash, "ih-b");
        assert_eq!(stored.check_name, "frontend");
        assert_eq!(stored.exit_code, 1);
        assert!(!stored.passed);
        assert_eq!(stored.failure_kind, None);
        assert_eq!(stored.output_tail, "vitest failed");
        assert_eq!(stored.job_id.as_deref(), Some("job-a"));
        assert_eq!(stored.cached, Some(false));
        assert_eq!(stored.executor_id, None);
        assert_eq!(stored.executor_device_id, None);
        assert_eq!(stored.executor_connection_generation, None);
        assert_eq!(stored.executor_cell_id, None);
        assert_eq!(stored.executor_lease_epoch, None);
        assert_eq!(stored.executor_started_at_unix_ms, None);
        assert_eq!(stored.executor_finished_at_unix_ms, None);
        assert_eq!(
            stored.toolchain_fingerprint.as_deref(),
            Some(check_toolchain_identity())
        );
    }

    #[tokio::test]
    async fn cache_miss_persists_structured_results() {
        let db = cache_db().await;
        let plans = vec![(
            plan("rust", "bun run test:rust:nextest"),
            "ih-structured".to_string(),
        )];
        let nextest_output = "     Summary [   0.1s] 3 tests run: 1 passed, 2 failed, 0 skipped\n\
            \x20       FAIL [   0.0s] (1/3) mycrate mod::test_a\n\
            \x20       FAIL [   0.0s] (2/3) mycrate mod::test_b"
            .to_string();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-structured",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let out = nextest_output.clone();
                async move { exec_ok(Some(100), out) }
            },
            |_| {},
        )
        .await;

        // The outcome carries the parsed per-test detail.
        let parsed = results[0].parsed.as_ref().expect("nextest output parses");
        assert_eq!(parsed.parser, "nextest");
        assert_eq!(parsed.failed, 2);
        assert_eq!(parsed.failures.len(), 2);

        // The failed verdict is not reusable, but its structured evidence remains
        // visible on the tree-scoped result row for diagnostics and baseline work.
        assert!(
            get_check_result(db.clone(), "project-a", "rust", "ih-structured")
                .unwrap()
                .is_none(),
            "a failed verdict is stored for visibility but is not reusable"
        );
        let stored =
            crate::execution::cache::list_check_results(db, "project-a", "tree-structured")
                .unwrap();
        assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
        let stored = &stored[0];
        assert_eq!(stored.input_hash, "ih-structured");
        assert_eq!(stored.check_name, "rust");
        assert_eq!(stored.exit_code, 100);
        assert!(!stored.passed);
        assert_eq!(stored.failure_kind, None);
        assert_eq!(stored.job_id.as_deref(), Some("job-a"));
        assert_eq!(stored.cached, Some(false));
        assert_eq!(stored.executor_id, None);
        assert_eq!(stored.executor_device_id, None);
        assert_eq!(stored.executor_connection_generation, None);
        assert_eq!(stored.executor_cell_id, None);
        assert_eq!(stored.executor_lease_epoch, None);
        assert_eq!(stored.executor_started_at_unix_ms, None);
        assert_eq!(stored.executor_finished_at_unix_ms, None);
        assert_eq!(
            stored.toolchain_fingerprint.as_deref(),
            Some(check_toolchain_identity())
        );
        let json = stored
            .target_results_json
            .as_deref()
            .expect("structured results persisted");
        assert!(json.contains("\"parser\":\"nextest\""));
        assert!(json.contains("mycrate mod::test_a"));
        assert!(json.contains("mycrate mod::test_b"));
    }

    /// The repro this whole change fixes. A src-tauri commit runs the rust check
    /// and caches its verdict keyed by the src-tauri input hash. A following
    /// doc-only commit moves the WHOLE-tree hash but leaves that input hash
    /// unchanged, so the verdict is a cache HIT — rust does not re-run — and the
    /// row is re-stamped onto the doc commit's tree so the `/checks` listing still
    /// surfaces it.
    #[tokio::test]
    async fn doc_only_commit_reuses_impact_scoped_verdict() {
        let db = cache_db().await;
        let calls = Arc::new(AtomicUsize::new(0));

        // Commit 1 touches src-tauri: rust runs and caches its verdict for input
        // hash IH1 at whole-tree tree-1.
        let plans = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
        let counted = calls.clone();
        let r1 = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-1",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].name, "rust");
        assert!(r1[0].passed);
        assert_eq!(r1[0].exit_code, Some(0));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Commit 2 is doc-only: the whole tree changes to tree-2, but the rust
        // input hash is UNCHANGED (still IH1), so the verdict is a cache hit and
        // the check does not re-run.
        let plans2 = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
        let counted = calls.clone();
        let r2 = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-2",
            "job-a",
            &plans2,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].name, "rust");
        assert!(r2[0].passed);
        assert_eq!(r2[0].exit_code, Some(0));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a doc-only commit must not re-run the rust check"
        );

        // The reused verdict was re-stamped onto the doc commit's tree, so the
        // tree-keyed `/checks` listing surfaces rust at the current tree.
        let stamped = get_check_result(db.clone(), "project-a", "rust", "IH1")
            .unwrap()
            .expect("the verdict is still cached");
        assert_eq!(stamped.tree_hash, "tree-2");
        let rows = crate::execution::cache::list_check_results(db, "project-a", "tree-2").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].check_name, "rust");
    }

    /// The three abnormal deaths a check can suffer are each classified,
    /// persisted durably, and rendered as themselves — the core of this change.
    #[tokio::test]
    async fn miss_classifies_and_persists_timeout_spawn_and_signal() {
        let db = cache_db().await;
        let plans = vec![
            (plan("slow", "run-slow"), "ih-slow".to_string()),
            (plan("nogo", "run-nogo"), "ih-nogo".to_string()),
            (plan("crash", "run-crash"), "ih-crash".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-cls",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |index, _command, _stream_id| async move {
                match index {
                    // Killed at its budget, with a nextest SLOW line naming the
                    // test in flight at the kill.
                    0 => Ok(CheckExecResult {
                        exit_code: None,
                        output: "     SLOW [>  60.000s] mycrate mod::hangs\nstill going"
                            .to_string(),
                        timed_out: true,
                        duration_ms: None,
                        provenance: None,
                        publication: None,
                    }),
                    // The process could not be spawned.
                    1 => Err("Failed to spawn command: No such file or directory".to_string()),
                    // Died by signal mid-run (no exit code, not a timeout).
                    _ => Ok(CheckExecResult {
                        exit_code: None,
                        output: "segfault".to_string(),
                        timed_out: false,
                        duration_ms: None,
                        provenance: None,
                        publication: None,
                    }),
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(results[0].failure_kind, Some(CheckFailureKind::TimedOut));
        assert_eq!(results[1].failure_kind, Some(CheckFailureKind::SpawnError));
        assert_eq!(results[2].failure_kind, Some(CheckFailureKind::Killed));
        assert!(results.iter().all(|o| !o.passed));

        // The classification is persisted, so every downstream surface can render
        // the real death rather than re-deriving it from exit -1.
        let stored =
            crate::execution::cache::list_check_results(db.clone(), "project-a", "tree-cls")
                .unwrap();
        let kind_of = |name: &str| {
            stored
                .iter()
                .find(|row| row.check_name == name)
                .and_then(|row| row.failure_kind.as_deref())
        };
        assert_eq!(kind_of("slow"), Some("timed_out"));
        assert_eq!(kind_of("nogo"), Some("spawn_error"));
        assert_eq!(kind_of("crash"), Some("killed"));
        assert!(get_check_result(db.clone(), "project-a", "slow", "ih-slow")
            .unwrap()
            .is_none());

        // The timeout summary names the timeout AND the still-running test; the
        // spawn error names itself.
        let summary = format_check_summary(&results);
        assert!(summary.contains("timed out after"), "got: {summary}");
        assert!(summary.contains("mycrate mod::hangs"), "got: {summary}");
        assert!(summary.contains("failed to spawn"), "got: {summary}");
    }

    // --- live status snapshots + sequential ordering ----------------------

    fn find<'a>(snap: &'a [CheckStatusEntry], name: &str) -> &'a CheckStatusEntry {
        snap.iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("no `{name}` entry in snapshot"))
    }

    /// The notify callback receives a full checklist snapshot on every
    /// transition: the planned set is all-pending, a cache hit jumps straight to
    /// its final state (annotated `cached`, never `running`), and a miss passes
    /// through `running` before its annotated final state.
    #[tokio::test]
    async fn notify_emits_planned_running_and_final_snapshots() {
        let db = cache_db().await;
        // frontend is already cached (passing); typecheck is a fresh miss.
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-frontend".to_string(),
                check_name: "frontend".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: String::new(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
                executor_id: None,
                executor_device_id: None,
                executor_connection_generation: None,
                executor_cell_id: None,
                executor_lease_epoch: None,
                executor_started_at_unix_ms: None,
                executor_finished_at_unix_ms: None,
                toolchain_fingerprint: None,
            },
        )
        .unwrap();

        let plans = vec![
            (plan("frontend", "run-frontend"), "ih-frontend".to_string()),
            (
                plan("typecheck", "run-typecheck"),
                "ih-typecheck".to_string(),
            ),
        ];
        let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = snapshots.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-a",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            // typecheck misses and fails with a bare exit code.
            move |_index, _command, _stream_id| async move { exec_ok(Some(1), "boom") },
            move |checks| captured.lock().unwrap().push(checks),
        )
        .await;

        let snaps = snapshots.lock().unwrap();
        assert!(
            snaps.len() >= 4,
            "planned + hit + running + final, got {}",
            snaps.len()
        );

        // First snapshot is the planned set: everything pending, unannotated.
        let planned = &snaps[0];
        assert!(planned
            .iter()
            .all(|e| e.state == "pending" && e.annotation.is_none()));

        // frontend was a cache hit: it reaches `passed` annotated `cached` and is
        // NEVER seen in a `running` state (no run phase for a hit).
        assert!(
            snaps.iter().all(|s| find(s, "frontend").state != "running"),
            "a cache hit must never pass through `running`"
        );
        let frontend_final = find(snaps.last().unwrap(), "frontend");
        assert_eq!(frontend_final.state, "passed");
        assert_eq!(frontend_final.annotation.as_deref(), Some("cached"));

        // typecheck (a miss) passes through `running` (unannotated) then `failed`
        // with the same annotation the final summary uses.
        assert!(
            snaps.iter().any(|s| {
                let e = find(s, "typecheck");
                e.state == "running" && e.annotation.is_none()
            }),
            "a miss must surface a `running` snapshot"
        );
        let typecheck_final = find(snaps.last().unwrap(), "typecheck");
        assert_eq!(typecheck_final.state, "failed");
        assert_eq!(typecheck_final.annotation.as_deref(), Some("exit 1"));
    }

    /// Outcomes — and the summary built from them — come back in plan order.
    #[tokio::test]
    async fn checks_return_and_summarize_in_plan_order() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-p",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| async move { exec_ok(Some(0), String::new()) },
            |_| {},
        )
        .await;

        let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"], "outcomes must be in plan order");
        // The summary follows the same plan order (each name carries a duration
        // annotation, so match on relative position rather than exact text).
        let summary = format_check_summary(&results);
        let pos = |name: &str| summary.find(name).expect("name present in summary");
        assert!(
            pos("a") < pos("b") && pos("b") < pos("c"),
            "the summary must reflect plan order: {summary}"
        );
    }

    /// `Shared` is the canonical write-check mode and must never overlap two check
    /// commands in the one checkout: a mutating check (a formatter / `--fix` lint)
    /// has to settle before the next check observes the shared sealed worktree.
    /// Even when each executor yields at an await, the concurrent-invocation
    /// high-water mark stays 1.
    #[tokio::test]
    async fn shared_mode_checks_stay_sequential() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
        ];
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let hw = high_water.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-c",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "shared-mode checks must not overlap; exactly one may run at a time"
        );
    }

    #[tokio::test]
    async fn shared_mode_exposes_earlier_mutations_across_cache_hit_and_failure() {
        let db = cache_db().await;
        let plans = vec![
            (plan("formatter", "format"), "ih-format".to_string()),
            (plan("cached", "cached"), "ih-cached".to_string()),
            (plan("reader", "read"), "ih-read".to_string()),
        ];
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "older-tree".to_string(),
                input_hash: "ih-cached".to_string(),
                check_name: "cached".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: String::new(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
                executor_id: None,
                executor_device_id: None,
                executor_connection_generation: None,
                executor_cell_id: None,
                executor_lease_epoch: None,
                executor_started_at_unix_ms: None,
                executor_finished_at_unix_ms: None,
                toolchain_fingerprint: None,
            },
        )
        .unwrap();

        let checkout = Arc::new(tempfile::tempdir().unwrap());
        let marker = checkout.path().join("formatted.txt");
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let marker_for_run = marker.clone();
        let observed_for_run = observed.clone();
        let results = run_planned_checks(
            db,
            "project-a",
            "tree-shared",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            &admission(),
            None,
            move |index, _command, _stream_id| {
                let marker = marker_for_run.clone();
                let observed = observed_for_run.clone();
                async move {
                    observed.lock().unwrap().push(index);
                    match index {
                        0 => {
                            std::fs::write(&marker, "settled formatter output").unwrap();
                            exec_ok(Some(1), "formatter reported a failure")
                        }
                        2 => {
                            let contents = std::fs::read_to_string(&marker)
                                .expect("later check sees the earlier in-place mutation");
                            assert_eq!(contents, "settled formatter output");
                            exec_ok(Some(0), contents)
                        }
                        _ => panic!("the cached plan must not execute"),
                    }
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(*observed.lock().unwrap(), vec![0, 2]);
        assert!(
            !results[0].passed,
            "a failed mutating check is still recorded"
        );
        assert!(results[1].cached, "the middle plan remains a cache hit");
        assert!(results[2].passed);
        assert_eq!(
            std::fs::read_to_string(marker).unwrap(),
            "settled formatter output",
            "the mutation remains in the real checkout for the canonical fold"
        );
    }

    /// In `Isolated` mode the misses run CONCURRENTLY. Each executor signals
    /// entry into a 2-party barrier and then awaits the other; both must be
    /// in-flight at once for the barrier to release. Sequential execution would
    /// park the first executor forever and trip the timeout, so a pass proves
    /// genuine overlap.
    #[tokio::test]
    async fn isolated_checks_run_concurrently() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
        ];
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b = barrier.clone();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_planned_checks(
                db.clone(),
                "project-a",
                "tree-iso",
                "job-a",
                &plans,
                "tool",
                CheckExecMode::Isolated,
                &admission(),
                None,
                move |_index, _command, _stream_id| {
                    let b = b.clone();
                    async move {
                        b.wait().await;
                        exec_ok(Some(0), String::new())
                    }
                },
                |_| {},
            ),
        )
        .await;

        let results = outcome.expect(
            "isolated checks must run concurrently; the rendezvous timed out (ran sequentially?)",
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|o| o.passed));
    }

    /// Concurrent misses may complete out of plan order (here the later-in-plan
    /// checks finish FIRST), but the runner reassembles outcomes into plan order.
    #[tokio::test]
    async fn isolated_results_reassemble_into_plan_order() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-rev",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            &admission(),
            None,
            // index 0 sleeps longest, so completion order reverses plan order.
            move |index, _command, _stream_id| async move {
                let delay = (3 - index as u64) * 20;
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                exec_ok(Some(0), String::new())
            },
            |_| {},
        )
        .await;

        let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["a", "b", "c"],
            "results must reassemble into plan order despite reversed completion"
        );
    }

    /// Concurrent transitions still emit FULL snapshots, and the last snapshot has
    /// every entry in a final (passed/failed) state.
    #[tokio::test]
    async fn isolated_concurrent_transitions_end_all_final() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
            (plan("z", "cmd-z"), "ih-z".to_string()),
        ];
        let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = snapshots.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-snap",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            &admission(),
            None,
            // z fails, the others pass — mixed final states under concurrency.
            move |index, _command, _stream_id| async move {
                let code = if index == 2 { 1 } else { 0 };
                exec_ok(Some(code), String::new())
            },
            move |checks| captured.lock().unwrap().push(checks),
        )
        .await;

        let snaps = snapshots.lock().unwrap();
        assert!(
            snaps.iter().all(|s| s.len() == 3),
            "every snapshot carries the full checklist"
        );
        let last = snaps.last().expect("at least the planned snapshot emitted");
        assert!(
            last.iter()
                .all(|e| e.state == "passed" || e.state == "failed"),
            "the final snapshot must have every entry final: {last:?}"
        );
        assert_eq!(find(last, "z").state, "failed");
    }

    /// Bounded `Isolated` mode caps in-flight misses: with `Some(2)` over 3 misses
    /// the concurrent high-water mark never exceeds 2, yet still reaches 2 (genuine
    /// overlap, not accidental sequencing).
    #[tokio::test]
    async fn bounded_isolated_never_exceeds_cap() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let hw = high_water.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-bounded",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            &admission(),
            None,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(results.len(), 3);
        assert_eq!(
            high_water.load(Ordering::SeqCst),
            2,
            "global admission must reach but never exceed capacity 2"
        );
    }

    /// A one-permit global controller makes isolated shared checks sequential.
    #[tokio::test]
    async fn global_admission_capacity_one_is_sequential() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let hw = high_water.clone();
        let controller = CheckAdmissionController::new(1);
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-seq1",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            &controller,
            None,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "global capacity one must serialize isolated misses"
        );
    }

    #[tokio::test]
    async fn independent_suites_share_one_global_controller() {
        let db = cache_db().await;
        let controller = Arc::new(CheckAdmissionController::new(1));
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let run_suite = |name: &'static str, tree: &'static str, job: &'static str| {
            let db = db.clone();
            let controller = controller.clone();
            let active = active.clone();
            let high_water = high_water.clone();
            async move {
                let plans = vec![(plan(name, "fixture"), format!("input-{name}"))];
                run_planned_checks(
                    db,
                    "project-a",
                    tree,
                    job,
                    &plans,
                    "tool",
                    CheckExecMode::Isolated,
                    controller.as_ref(),
                    None,
                    move |_index, _command, _stream_id| {
                        let active = active.clone();
                        let high_water = high_water.clone();
                        async move {
                            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                            high_water.fetch_max(now, Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            exec_ok(Some(0), "ok")
                        }
                    },
                    |_| {},
                )
                .await
            }
        };
        let (first, second) = tokio::join!(
            run_suite("first", "tree-first", "job-first"),
            run_suite("second", "tree-second", "job-second")
        );
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(high_water.load(Ordering::SeqCst), 1);
    }

    fn parsed(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
        ParsedCheckResult {
            parser: parser.to_string(),
            passed,
            failed,
            skipped: 0,
            failures: Vec::new(),
        }
    }

    #[test]
    fn active_build_service_failure_preflights_without_relabeling_spawn_outcomes() {
        let config_fingerprint = "active-config".to_string();
        let snapshot = crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot {
            name: "sccache".to_string(),
            configured: true,
            enabled: true,
            supervised_child: false,
            config_fingerprint: Some(config_fingerprint.clone()),
            state_dir: None,
            error_log_tail: Some("historical log entry".to_string()),
            runtime: crate::orchestrator::build_services::BuildServiceRuntimeDiagnostic {
                current_failure: Some(
                    "sccache: error: Address already in use (os error 48)".to_string(),
                ),
                failure_config: Some(config_fingerprint),
                ..Default::default()
            },
        };
        assert_eq!(
            active_build_service_failure(&snapshot).as_deref(),
            Some("Cairn check infrastructure failure: sccache port conflict: sccache: error: Address already in use (os error 48)")
        );

        let mut disabled = snapshot.clone();
        disabled.enabled = false;
        assert_eq!(active_build_service_failure(&disabled), None);

        let mut replaced = snapshot.clone();
        replaced.config_fingerprint = Some("replacement-config".to_string());
        assert_eq!(active_build_service_failure(&replaced), None);

        let outcome = CellOutcome::Unavailable {
            reason: cairn_common::executor_protocol::CellUnavailableReason::Spawn,
            diagnostic: "sandbox denial cannot be adjudicated without runner context".to_string(),
        };
        let failure = check_result_from_cell_outcome(outcome, None).unwrap_err();
        assert_eq!(
            failure,
            CheckExecutionFailure::Substrate(
                "Cairn check infrastructure failure: Spawn: sandbox denial cannot be adjudicated without runner context".to_string()
            )
        );
    }

    #[test]
    fn failure_classifier_requires_positive_evidence_and_preserves_precedence() {
        let warning = "sccache: warning: The server looks like it shut down unexpectedly";
        assert_eq!(
            classify_check_failure(Some(0), false, false, None, warning),
            None
        );
        assert_eq!(
            classify_check_failure(Some(254), false, false, None, "script exited 254"),
            None
        );

        let abnormal = "error: process didn't exit successfully: sccache rustc --crate-name bytes (exit status: 254)";
        assert_eq!(
            classify_check_failure(Some(254), false, false, None, abnormal)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure)
        );
        for signature in [
            "Failed to send data to or receive data from server",
            "failed client/server communication",
            "failed to fill whole buffer",
            "server looks like it shut down unexpectedly",
        ] {
            assert_eq!(
                classify_check_failure(Some(1), false, false, None, signature)
                    .map(|classification| classification.kind),
                Some(CheckFailureKind::Infrastructure),
                "signature: {signature}"
            );
        }
        let missing =
            "couldn't read target/debug/build/tree/out/generated.txt: No such file or directory";
        assert_eq!(
            classify_check_failure(Some(1), false, false, None, missing)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure)
        );
        assert_eq!(
            classify_check_failure(
                Some(1),
                false,
                false,
                Some(&parsed("vitest", 2, 1)),
                warning
            ),
            None,
            "real assertion failures outrank incidental infrastructure text"
        );
    }

    #[test]
    fn runner_error_is_vitest_only_and_reports_progress() {
        let vitest = parsed("vitest", 12, 0);
        let classification =
            classify_check_failure(Some(1), false, false, Some(&vitest), "report complete")
                .expect("abnormal Vitest exit");
        assert_eq!(classification.kind, CheckFailureKind::RunnerError);
        assert!(classification.reason.contains("12 tests passed"));
        assert_eq!(
            classify_check_failure(
                Some(101),
                false,
                false,
                Some(&parsed("nextest", 0, 0)),
                "compile error"
            ),
            None
        );
    }

    #[test]
    fn review_batch_constraints_union_toolchains_and_reject_scalar_conflicts() {
        let item = |index, constraints| PlannedCheckBatchItem {
            index,
            name: format!("check-{index}"),
            input_hash: format!("hash-{index}"),
            resource_identity_key: format!("resource-{index}"),
            command: "true".into(),
            stream_id: format!("stream-{index}"),
            env: Vec::new(),
            timeout_ms: 1,
            constraints: Some(constraints),
        };
        let merged = merge_batch_constraints(&[
            item(
                0,
                PlacementConstraints {
                    os: Some("linux".into()),
                    required_toolchains: vec!["rust".into()],
                    ..PlacementConstraints::default()
                },
            ),
            item(
                1,
                PlacementConstraints {
                    os: Some("linux".into()),
                    required_toolchains: vec!["bun".into(), "rust".into()],
                    ..PlacementConstraints::default()
                },
            ),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(merged.os.as_deref(), Some("linux"));
        assert_eq!(merged.required_toolchains, vec!["bun", "rust"]);

        let conflict = merge_batch_constraints(&[
            item(
                0,
                PlacementConstraints {
                    arch: Some("arm64".into()),
                    ..PlacementConstraints::default()
                },
            ),
            item(
                1,
                PlacementConstraints {
                    arch: Some("x86_64".into()),
                    ..PlacementConstraints::default()
                },
            ),
        ])
        .unwrap_err();
        assert!(conflict.contains("conflicting review check placement constraint arch"));
    }

    #[test]
    fn pure_verdict_batches_keep_unconstrained_and_constrained_checks_separate() {
        let item = |index, constraints| PlannedCheckBatchItem {
            index,
            name: format!("check-{index}"),
            input_hash: format!("hash-{index}"),
            resource_identity_key: format!("resource-{index}"),
            command: "true".into(),
            stream_id: format!("stream-{index}"),
            env: Vec::new(),
            timeout_ms: 1,
            constraints,
        };
        let groups = partition_check_items_by_constraints(vec![
            item(0, None),
            item(
                1,
                Some(PlacementConstraints {
                    os: Some("linux".into()),
                    ..Default::default()
                }),
            ),
            item(2, None),
        ]);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .1
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            groups[1]
                .1
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn classified_excerpt_keeps_early_evidence_and_final_tail_bounded() {
        let output = format!(
            "failed to fill whole buffer\n{}\nFINAL-MARKER",
            "noise\n".repeat(2000)
        );
        let classification = classify_check_failure(Some(1), false, false, None, &output);
        let excerpt = classified_output_excerpt(&output, classification.as_ref());
        assert!(excerpt.contains("failed to fill whole buffer"));
        assert!(excerpt.contains("FINAL-MARKER"));
        assert!(excerpt.chars().count() <= OUTPUT_TAIL_CHARS);
    }

    #[test]
    fn build_slot_deadline_formats_truthful_capacity_and_stall_evidence() {
        let now = unix_time_ms_for_checks();
        let outcome = |state, last_progress_unix_ms| CellOutcome::Unavailable {
            reason: cairn_common::executor_protocol::CellUnavailableReason::Deadline {
                host_pressure: None,
                substrate: Some(cairn_common::executor_protocol::ExecutorSubstrateEvidence {
                    state,
                    since_unix_ms: last_progress_unix_ms,
                    last_progress_unix_ms,
                    diagnostic: None,
                    queue_depth: Some(4),
                    queue_position: Some(3),
                    active_cell_count: Some(2),
                    oldest_running_started_at_unix_ms: Some(now.saturating_sub(500)),
                }),
            },
            diagnostic: "acquisition deadline elapsed".into(),
        };

        let fresh = check_result_from_cell_outcome(
            outcome(
                cairn_common::executor_protocol::ExecutorSubstrateState::CapacityBusy,
                now,
            ),
            None,
        )
        .unwrap_err();
        let CheckExecutionFailure::Substrate(fresh) = fresh else {
            panic!("deadline must be a substrate failure");
        };
        assert!(fresh.contains("substrate=CapacityBusy"));
        assert!(!fresh.contains("ConnectedStalled"));

        let stale = check_result_from_cell_outcome(
            outcome(
                cairn_common::executor_protocol::ExecutorSubstrateState::ConnectedStalled,
                now.saturating_sub(
                    cairn_common::executor_protocol::EXECUTOR_PROGRESS_FRESHNESS_MS + 1,
                ),
            ),
            None,
        )
        .unwrap_err();
        let CheckExecutionFailure::Substrate(stale) = stale else {
            panic!("deadline must be a substrate failure");
        };
        assert!(stale.contains("substrate=ConnectedStalled"));
        assert!(stale.contains("lastProgressAge="));
        assert!(stale.contains("queueDepth=4"));
        assert!(stale.contains("queuePosition=3"));
        assert!(stale.contains("activeSlots=2"));
    }
}
