use super::*;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCheckCacheContext {
    pub project_id: String,
    pub project_key: String,
    pub job_id: String,
    pub commit_sha: String,
    pub tree_hash: String,
    pub input_hash: String,
    pub(super) cacheable: bool,
    pub(super) entry: Option<crate::execution::cache::CheckResultCacheEntry>,
}

impl ManualCheckCacheContext {
    pub fn require_cacheable(&self) -> Result<(), String> {
        self.cacheable.then_some(()).ok_or_else(|| {
            "the run coordinate cannot be stored as sealed-tree evidence".to_string()
        })
    }
}

pub(super) const UNCACHED_RETRY_PREFIX: &str = "export CAIRN_BUILD_SERVICE_UNFIT=1\n";

pub(super) fn uncached_retry_command(command: &str) -> String {
    format!("{UNCACHED_RETRY_PREFIX}{command}")
}

pub(super) fn require_snapshot_command(
    expected: &str,
    submitted: &str,
) -> Result<(), CheckExecutionFailure> {
    if submitted == expected || submitted == uncached_retry_command(expected) {
        Ok(())
    } else {
        Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Result,
            "manual check command diverged from its immutable contract snapshot",
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CheckContractSnapshot {
    pub(crate) context: ManualCheckCacheContext,
    pub(super) sealed_coordinate: crate::execution::cache::SealedCheckCoordinate,
    pub(super) repository_path: String,
    /// What the sealed head changed since its fork point with the run's
    /// attribution target ([`manual_attribution_target`]). The key above names
    /// it for a base-relative check, and the transport is built from it.
    pub(super) change: BaseRelativeChange,
    /// The commit whose `.cairn/config.yaml` declared the requested check. An
    /// explicitly requested branch supplies both the definition and the content,
    /// so a manual check on one branch can never run another branch's definition.
    pub(super) defined_by_commit: String,
    pub(super) configured_check: CheckCommand,
    /// Where this run may be placed and whose answer counts, after the
    /// request's override was applied to `configured_check`. The configured
    /// check itself stays untouched: it is what the cache key is computed from.
    pub(super) placement: crate::execution::check_placement::EffectiveCheckPlacement,
    pub(super) plan: CheckPlan,
    pub(super) timeout_ms: u64,
    /// How a manual run captured the commit it judges. `None` for the main
    /// cadence and the pinBump lanes, which are handed an exact commit.
    pub(super) capture: Option<CheckCapture>,
}

/// How a manual check's judged commit was captured, reported in its result so
/// a verdict always names the exact head and base it is about.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckCapture {
    /// `managed` when the named branch was prepared against its base first,
    /// `exact` when the revision was captured as it stands.
    pub authority: String,
    pub revision: String,
    pub head_before: Option<String>,
    /// The judged commit. For an exact capture it is also the head before.
    pub head_after: String,
    /// The Git tree object id of `head_after`, for a managed capture. Distinct
    /// from the logical tree hash a check result reports as `treeHash`.
    pub git_tree: Option<String>,
    pub base_branch: Option<String>,
    pub base_commit: Option<String>,
    pub advanced: bool,
    pub attempts: Option<u32>,
    /// Incomplete follow-up of an accepted install, such as a failed export.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub provenance: String,
}

impl CheckCapture {
    pub(crate) fn of(captured: &CapturedRevision) -> Self {
        let prepared = captured.prepared.as_ref();
        Self {
            authority: if prepared.is_some() {
                "managed"
            } else {
                "exact"
            }
            .to_string(),
            revision: captured.rev().to_string(),
            head_before: prepared.map(|prepared| prepared.head_before.clone()),
            head_after: captured.commit_id.clone(),
            git_tree: prepared.map(|prepared| prepared.git_tree.clone()),
            base_branch: prepared.map(|prepared| prepared.base_branch.clone()),
            base_commit: prepared.map(|prepared| prepared.base_commit.clone()),
            advanced: prepared.is_some_and(|prepared| prepared.advanced),
            attempts: prepared.map(|prepared| prepared.attempts),
            warnings: prepared
                .map(|prepared| prepared.warnings.clone())
                .unwrap_or_default(),
            provenance: captured.provenance(),
        }
    }
}

/// Why a check requested from a thread session cannot resolve its own coordinate.
///
/// A manual check records a verdict against a sealed commit, and the commit it
/// seals is the one the requesting run's job points at. A thread session points
/// at none of its own: it runs on the project's base branch and owns no branch,
/// by design (see [`crate::threads`]). So this is a structural refusal, not a
/// lookup failure — and the reader can act on it, because naming a coordinate
/// explicitly supplies the sealed commit the job cannot.
pub(super) fn thread_session_coordinate_refusal(check_name: &str) -> String {
    format!(
        "cairn check run records {check_name:?} against the sealed commit of the branch whose \
         work it is, and this session belongs to a thread: a thread runs on the project's base \
         branch and owns no branch of its own. Name the revision explicitly to record a verdict \
         from here — `cairn check run {check_name} --branch <revision>` — or run the suite's \
         command in a shell for an answer that is yours alone and records nothing."
    )
}

/// The durable coordinates a manual run resolves from its run row.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ManualRunRow {
    pub(super) job_id: String,
    pub(super) project_id: String,
    pub(super) project_key: String,
    pub(super) repository_path: String,
    /// The branch the job's work merges into: its parent issue's branch for a
    /// stacked child, the project default otherwise. The same expression the
    /// rendered diff and the merge gate resolve a node's base from.
    pub(super) integration_target: String,
    /// The branch the job minted for itself, when it has one.
    pub(super) branch: Option<String>,
}

/// Read a manual run's coordinates from `db`, which must be the run's OWNING
/// database. A team job lives wholly in its team replica, so asking the private
/// database for it finds nothing.
pub(super) async fn load_manual_run_row(
    db: &LocalDb,
    run_id: &str,
) -> Result<ManualRunRow, String> {
    let run_id = run_id.to_string();
    db.read(move |conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT r.job_id, r.project_id, p.key, p.repo_path,
                            COALESCE(j.base_branch, p.default_branch, 'main'),
                            j.branch
                     FROM runs r
                     JOIN projects p ON p.id = r.project_id
                     JOIN jobs j ON j.id = r.job_id
                     WHERE r.id = ?1
                     LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row(format!(
                    "run {run_id} has no resolvable job coordinate"
                ))
            })?;
            Ok(ManualRunRow {
                job_id: row.text(0)?,
                project_id: row.text(1)?,
                project_key: row.text(2)?,
                repository_path: row.text(3)?,
                integration_target: row.text(4)?,
                branch: row.opt_text(5)?.filter(|branch| !branch.trim().is_empty()),
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The revision a manual check judges, as the check-run handler classified it
/// before any observation existed.
#[derive(Debug, Clone)]
pub(crate) enum ManualCheckRevision {
    /// The requesting job's own head. When the job owns a managed branch, the
    /// run prepares it against its declared base first, exactly once.
    OwnHead,
    /// The requesting job's logical head exactly as it stands. A lookup that
    /// only reads (the cache context) never prepares anything.
    OwnHeadLiteral,
    /// An exact revision, already captured at its commit.
    Captured(Box<CapturedRevision>),
    /// A managed branch named with `--branch`, the requester's or another
    /// owner's, prepared once when the run starts.
    Managed(Box<TypedRevision>),
}

/// The commit a manual check judges and what its change is measured against,
/// captured once and carried unchanged through contract loading, keying,
/// execution, and recording.
pub(super) struct ManualCheckCoordinate {
    pub(super) commit: String,
    pub(super) attribution_label: String,
    /// The exact commit to measure from, when the capture fixed one.
    pub(super) attribution_commit: Option<String>,
    pub(super) capture: Option<CheckCapture>,
}

/// Preparation refusals remain typed until the request's durable terminal reply.
#[derive(Debug)]
pub(crate) enum ManualCheckError {
    Capture(crate::mcp::handlers::branch::RevisionCaptureError),
    Other(String),
}

impl From<String> for ManualCheckError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

impl std::fmt::Display for ManualCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capture(error) => write!(f, "{error}"),
            Self::Other(error) => write!(f, "{error}"),
        }
    }
}

pub(super) async fn capture_manual_check_coordinate(
    orch: &Orchestrator,
    row: &ManualRunRow,
    revision: ManualCheckRevision,
    check_name: &str,
) -> Result<ManualCheckCoordinate, ManualCheckError> {
    let captured = match revision {
        ManualCheckRevision::Captured(captured) => *captured,
        ManualCheckRevision::Managed(revision) => {
            capture_managed_for_check(orch, *revision).await?
        }
        // A job with no branch of its own works directly on its base, which it
        // does not manage: its head is taken as it stands.
        ManualCheckRevision::OwnHead => match row.branch.as_deref() {
            Some(branch) => {
                let revision = crate::mcp::handlers::branch::resolve_own_branch_revision(
                    orch,
                    &row.project_id,
                    branch,
                    &row.job_id,
                )
                .await?;
                capture_managed_for_check(orch, revision).await?
            }
            None => {
                return own_logical_head(orch, row, check_name)
                    .await
                    .map_err(Into::into)
            }
        },
        ManualCheckRevision::OwnHeadLiteral => {
            return own_logical_head(orch, row, check_name)
                .await
                .map_err(Into::into);
        }
    };
    let (attribution_label, attribution_commit) = captured.attribution();
    Ok(ManualCheckCoordinate {
        commit: captured.commit_id.clone(),
        attribution_label,
        attribution_commit,
        capture: Some(CheckCapture::of(&captured)),
    })
}

/// The job's logical head as it stands, measured against its integration
/// target: a job with no managed branch of its own, or a pure lookup.
pub(super) async fn own_logical_head(
    orch: &Orchestrator,
    row: &ManualRunRow,
    check_name: &str,
) -> Result<ManualCheckCoordinate, String> {
    let commit = crate::execution::cache::resolve_job_logical_head(orch, &row.job_id)
        .await
        .map_err(|error| match error {
            crate::execution::cache::JobLogicalHeadError::Branchless {
                thread_owned: true, ..
            } => thread_session_coordinate_refusal(check_name),
            other => other.to_string(),
        })?;
    Ok(ManualCheckCoordinate {
        commit,
        attribution_label: row.integration_target.clone(),
        attribution_commit: None,
        capture: None,
    })
}

pub(super) async fn capture_managed_for_check(
    orch: &Orchestrator,
    revision: TypedRevision,
) -> Result<CapturedRevision, ManualCheckError> {
    let store = manual_check_store(orch, &revision.project_path).await?;
    crate::mcp::handlers::branch::capture_revision(
        orch,
        revision,
        store,
        crate::mcp::handlers::branch::CapturePurpose::Check,
    )
    .await
    .map_err(ManualCheckError::Capture)
}

/// The managed store a manual check resolves and reads in, initialized if absent.
pub(super) async fn manual_check_store(
    orch: &Orchestrator,
    project_path: &Path,
) -> Result<PathBuf, String> {
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let project_repo = project_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let jj = crate::jj::environment(&jj_binary_path, &config_dir);
        crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
    })
    .await
    .map_err(|error| format!("manual check coordinate repository task failed: {error}"))?
}

/// Resolve every mutable input to a manual check exactly once. The returned owned
/// values are the contract carried through cache lookup, execution, and recording;
/// callers must not consult the live project config again during this operation.
pub(super) async fn resolve_manual_check_contract_snapshot(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
    revision: ManualCheckRevision,
    placement_override: Option<&crate::execution::check_placement::CheckPlacementOverride>,
) -> Result<CheckContractSnapshot, ManualCheckError> {
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let row = load_manual_run_row(&db, run_id).await?;
    // Capture the coordinate FIRST: the captured commit supplies the content to
    // check AND the definition to check it with, and nothing after this point
    // resolves the requested name again.
    let coordinate = capture_manual_check_coordinate(orch, &row, revision, check_name).await?;
    let ManualRunRow {
        job_id,
        project_id,
        project_key,
        repository_path,
        ..
    } = row;
    let mut snapshot = resolve_check_contract_at_commit(
        orch,
        db,
        CheckRunTarget {
            job_id,
            project_id,
            project_key,
            repository_path,
            attribution_target: coordinate.attribution_label,
            attribution_commit: coordinate.attribution_commit,
        },
        coordinate.commit,
        check_name,
        placement_override,
    )
    .await?;
    snapshot.capture = coordinate.capture;
    Ok(snapshot)
}

/// What a manual check's one snapshot fixed, for boundary tests: the judged
/// commit and every stage's view of it.
#[cfg(test)]
pub(crate) struct ManualCaptureEvidence {
    pub(crate) commit: String,
    pub(crate) defined_by: String,
    pub(crate) tree_hash: String,
    pub(crate) input_hash: String,
    pub(crate) change_head: String,
    pub(crate) change_base: String,
    pub(crate) changed: Vec<String>,
    pub(crate) capture: Option<CheckCapture>,
}

#[cfg(test)]
pub(crate) async fn manual_capture_evidence(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
    revision: ManualCheckRevision,
) -> Result<ManualCaptureEvidence, String> {
    let snapshot = resolve_manual_check_contract_snapshot(orch, run_id, check_name, revision, None)
        .await
        .map_err(|error| error.to_string())?;
    Ok(ManualCaptureEvidence {
        commit: snapshot.context.commit_sha.clone(),
        defined_by: snapshot.defined_by_commit.clone(),
        tree_hash: snapshot.context.tree_hash.clone(),
        input_hash: snapshot.context.input_hash.clone(),
        change_head: snapshot.change.head.clone(),
        change_base: snapshot.change.base.clone(),
        changed: snapshot
            .change
            .changed
            .iter()
            .map(|change| change.path.clone())
            .collect(),
        capture: snapshot.capture.clone(),
    })
}

/// The forms `--branch` takes, named in each refusal that would otherwise leave
/// the caller guessing which one it got wrong.
pub(super) const MANUAL_CHECK_BRANCH_FORMS: &str = "--branch takes a branch name (`main`, \
     `agent/cairn-123-builder-0`), a commit id, or a node URI \
     (`cairn://p/PROJECT/ISSUE/EXEC/NODE`), the same forms `read ?branch=` and `run branch:` take";

/// Classify the revision a manual check names with `--branch`, in the project
/// of the run that asked.
///
/// The check-run handler calls this before it mints an observation, so a
/// revision that names nothing is refused before anything is queued. An exact
/// revision is captured here, at its commit; a managed branch is only
/// classified, because preparing it can take the whole preparation budget and
/// belongs to the run, which prepares it exactly once.
pub(crate) async fn classify_manual_check_revision(
    orch: &Orchestrator,
    run_id: &str,
    branch: &str,
) -> Result<ManualCheckRevision, String> {
    use crate::mcp::handlers::branch::{
        capture_revision, resolve_typed_revision, CapturePurpose, RevisionAuthority,
        RevisionCaptureError, RevisionScope,
    };
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let row = load_manual_run_row(&db, run_id).await?;
    let revision = resolve_typed_revision(orch, RevisionScope::Project(&row.project_id), branch)
        .await
        .map_err(|error| manual_check_branch_refusal(&error))?;
    if matches!(revision.authority, RevisionAuthority::Managed { .. }) {
        return Ok(ManualCheckRevision::Managed(Box::new(revision)));
    }
    let store = manual_check_store(orch, &revision.project_path).await?;
    capture_revision(orch, revision, store, CapturePurpose::Check)
        .await
        .map(|captured| ManualCheckRevision::Captured(Box::new(captured)))
        .map_err(|error| match error {
            RevisionCaptureError::Resolution(error) => manual_check_branch_refusal(&error),
            other => other.to_string(),
        })
}

pub(super) fn manual_check_branch_refusal(
    error: &crate::mcp::handlers::branch::BranchRefResolutionError,
) -> String {
    use crate::mcp::handlers::branch::BranchRefResolutionError;
    match error {
        BranchRefResolutionError::Invalid { .. }
        | BranchRefResolutionError::Unresolvable { .. }
        | BranchRefResolutionError::Ambiguous { .. } => {
            format!("{error}. {MANUAL_CHECK_BRANCH_FORMS}.")
        }
        // A conflicted or lost branch was named correctly; its refusal carries
        // its own way forward, and a list of forms would only bury it.
        BranchRefResolutionError::Conflicted { .. }
        | BranchRefResolutionError::OwnBranchLost { .. } => error.to_string(),
    }
}

/// Who a configured check run belongs to and what it is measured against.
///
/// `job_id` is the identity the observation records. For a manual run it is the
/// requesting run's job; for the main cadence it is the synthetic
/// `main:{project_id}` owner, which names no job row.
pub(crate) struct CheckRunTarget {
    pub(crate) job_id: String,
    pub(crate) project_id: String,
    pub(crate) project_key: String,
    pub(crate) repository_path: String,
    /// The branch whose fork point with the evaluated commit is this run's
    /// base: a managed revision's declared integration target, the project
    /// default for an exact one, the job's target for its own literal head.
    pub(crate) attribution_target: String,
    /// The exact commit of `attribution_target` to measure from, when the
    /// capture fixed one. `None` resolves the target by name.
    pub(crate) attribution_commit: Option<String>,
}

/// Resolve every mutable input to one configured check at one already-resolved
/// commit: the definition that commit declares, its placement, its whole-suite
/// plan, and its input key. This is the run-independent half of the trusted
/// manual producer, shared with the main cadence so both record the same shape.
pub(crate) async fn resolve_check_contract_at_commit(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    target: CheckRunTarget,
    commit: String,
    check_name: &str,
    placement_override: Option<&crate::execution::check_placement::CheckPlacementOverride>,
) -> Result<CheckContractSnapshot, String> {
    let CheckRunTarget {
        job_id,
        project_id,
        project_key,
        repository_path,
        attribution_target,
        attribution_commit,
    } = target;
    let repository = PathBuf::from(&repository_path);
    let CommitChecksContract {
        contract:
            ChecksContract {
                checks,
                extra_inputs,
                unsupported,
                budgets: _,
                cell_groups: _,
            },
        defined_by_commit,
    } = load_checks_contract_at_commit_detailed(&repository, &commit)
        .await
        .map_err(|error| error.manual_diagnostic(&commit))?;
    assert_eq!(
        defined_by_commit, commit,
        "a manual check must be defined by the commit it evaluates"
    );
    let check = checks
        .get(check_name)
        .ok_or_else(|| match unsupported.get(check_name) {
            Some(unsupported) => format!("check {check_name:?} {}", unsupported.notice),
            None => format!("configured check {check_name:?} was not found"),
        })?;
    // Refused here, before anything is planned: a request this check cannot
    // answer must not cost a tree read, let alone a lane.
    let placement = crate::execution::check_placement::resolve_check_placement(
        check_name,
        check,
        placement_override,
        |name| orch.fleet.executor_os_named(name),
    )?;
    let jj = immutable_read_jj(
        &crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        &repository,
        &[&commit],
    )?;
    let tree_hash = crate::jj::logical_tree_hash(&jj, &repository, &commit)?;
    let entries = crate::jj::tree_entries(&jj, &repository, &commit).ok();
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let change = resolve_base_relative_change(
        &orch.jj_binary_path,
        &orch.config_dir,
        &repository,
        &commit,
        &attribution_target,
        attribution_commit.as_deref().unwrap_or(&attribution_target),
        check_name,
        Some((db.clone(), project_id.clone())),
    )
    .await
    .map_err(|error| {
        format!(
            "Manual-check planning infrastructure failure: could not compute the base-relative \
             inputs {check_name} runs against: {error}"
        )
    })?;
    let (mut plan, input_hash) = manual_check_plan(
        &checks,
        &inputs,
        entries.as_deref(),
        &tree_hash,
        check_name,
        &repository,
        change.basis(),
    )?;
    // Reuse answers only what this request may count: the narrowed platforms,
    // and for a named machine, that machine's own observations.
    plan.verdict_platforms = placement.verdict_platforms.clone();
    plan.verdict_executor_ids = placement
        .executor_name
        .as_deref()
        .map(|name| orch.fleet.executor_ids_named(name));
    let configured_check = check.clone();
    let timeout_ms =
        resolve_check_timeout_ms(Some(&configured_check), DEFAULT_REVIEW_CHECK_TIMEOUT_MS);
    let entry =
        crate::execution::cache::get_check_result(db, &project_id, check_name, &input_hash)?;
    let sealed_coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        &commit,
        &defined_by_commit,
        &tree_hash,
        &input_hash,
    );
    Ok(CheckContractSnapshot {
        context: ManualCheckCacheContext {
            project_id,
            project_key,
            job_id,
            commit_sha: commit,
            tree_hash,
            input_hash,
            cacheable: true,
            entry,
        },
        sealed_coordinate,
        repository_path,
        change,
        defined_by_commit,
        configured_check,
        placement,
        plan,
        timeout_ms: timeout_ms.into(),
        capture: None,
    })
}

/// The base-relative inputs a check of `head` ships to whichever cell runs it.
///
/// The base is the fork point of the sealed `head` itself with `target`: the
/// merge base, so a target that advanced after the fork contributes neither
/// attribution nor comparison content, and computed from the head being checked
/// rather than whatever a branch bookmark points at now. Attribution and the
/// protocol bundle read that one immutable commit. Every resolution failure is
/// an error: substituting another base would produce a verdict about a
/// different change. The runner computes this because the script cannot: a
/// remote cell's checkout is a detached head with no base ref to ask.
///
/// With `baseline_store` (the store and project the run records into), a check
/// judged against its base's findings also carries that base's recorded set
/// ([`crate::execution::lint_baseline`]).
#[cfg(test)]
pub(super) async fn base_relative_transport(
    jj_binary_path: &str,
    config_dir: &Path,
    project_repository: &Path,
    head: &str,
    target: &str,
    check_name: &str,
    baseline_store: Option<(Arc<LocalDb>, String)>,
) -> Result<Vec<(String, String)>, String> {
    let change = resolve_base_relative_change(
        jj_binary_path,
        config_dir,
        project_repository,
        head,
        target,
        target,
        check_name,
        baseline_store,
    )
    .await?;
    let check_name = check_name.to_string();
    tokio::task::spawn_blocking(move || base_relative_transport_for(&change, &check_name))
        .await
        .map_err(|error| format!("manual check transport task failed: {error}"))?
}

/// The change a run of `head` is measured against: everything it changed since
/// its fork point with an integration target. Resolved once, before the run is
/// keyed, so the key a base-relative check is looked up under and the transport
/// its command reads come from the same diff.
#[derive(Clone)]
pub(crate) struct BaseRelativeChange {
    pub(super) jj: JjEnv,
    /// The logical repository `base` and `head` resolve in.
    pub(super) repository: PathBuf,
    pub(super) base: String,
    pub(super) head: String,
    pub(super) target: String,
    pub(super) changed: Vec<GraphFileChange>,
    /// The selected base path/blob pairs, read only for a check Cairn ships base content
    /// to, whose key names the base blobs that content came from.
    pub(super) base_entries: Option<Vec<(String, Option<String>)>>,
    /// The base finding set a base-subtracting check is shipped, read once so
    /// its key and its transport name the same set.
    pub(super) base_findings: Option<crate::execution::lint_baseline::LintBaselineTransport>,
}

impl std::fmt::Debug for BaseRelativeChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseRelativeChange")
            .field("repository", &self.repository)
            .field("base", &self.base)
            .field("head", &self.head)
            .field("target", &self.target)
            .field("changed", &self.changed.len())
            .finish_non_exhaustive()
    }
}

impl BaseRelativeChange {
    pub(super) fn basis(&self) -> ChangeBasis<'_> {
        ChangeBasis::Against {
            changed: &self.changed,
            base_entries: self.base_entries.as_deref(),
            base_findings: self.base_findings.as_ref(),
        }
    }
}

/// `jj` pinned for a read addressed only by the immutable `commits` in
/// `repository`. An unpinned command (even `jj git root`, or a diff between two
/// commit ids) loads the store at its head and merges a split operation log on
/// the way, which a read must never do. A plain Git checkout has no operation
/// log, so its reads run unpinned. A commit no operation indexes is an error,
/// never a fall back to the moving head.
pub(crate) fn immutable_read_jj(
    jj: &JjEnv,
    repository: &Path,
    commits: &[&str],
) -> Result<JjEnv, String> {
    if !crate::jj::is_jj_dir(repository) {
        return Ok(jj.clone());
    }
    jj.pin_immutable_read(repository, commits).map_err(|error| {
        format!(
            "the captured commits {} cannot be read without reconciling the store: {error}",
            commits.join(", ")
        )
    })
}

/// `target` labels the integration target; `target_revision` is what the fork
/// point is computed from: the same name, or the exact commit a capture fixed so
/// a target that moves after capture cannot change this run's base.
///
/// With `baseline_store` (the store and project the run records into), a check
/// judged against its base's findings reads that base's recorded set here,
/// before the run is keyed ([`crate::execution::lint_baseline`]).
// Each argument is a distinct, already-resolved fact about this one run.
#[allow(clippy::too_many_arguments)]
pub(super) async fn resolve_base_relative_change(
    jj_binary_path: &str,
    config_dir: &Path,
    project_repository: &Path,
    head: &str,
    target: &str,
    target_revision: &str,
    check_name: &str,
    baseline_store: Option<(Arc<LocalDb>, String)>,
) -> Result<BaseRelativeChange, String> {
    let jj = crate::jj::environment(jj_binary_path, config_dir);
    let repository = {
        let jj = jj.clone();
        let config_dir = config_dir.to_path_buf();
        let project_repo = project_repository.to_path_buf();
        tokio::task::spawn_blocking(move || {
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("coordinate repository task failed: {error}"))??
    };
    let base = cairn_vcs::merge_base(&repository, head, target_revision)
        .await
        .map_err(|error| {
            format!(
                "the fork point of {head} with integration target {target:?} at \
                 {target_revision} is unresolvable: {error}"
            )
        })?
        .base;
    let jj = immutable_read_jj(&jj, &repository, &[head, base.as_str()])?;
    let head = head.to_string();
    let names_base_blobs = needs_executor_base_content(check_name);
    let target = target.to_string();
    let object_repository = project_repository.to_path_buf();
    let check_name = check_name.to_string();
    tokio::task::spawn_blocking(move || {
        let changed = logical_changed_files(&jj, &repository, &base, &head).map_err(|error| {
            format!("the changed files of {base}..{head} could not be read: {error}")
        })?;
        let base_entries = names_base_blobs
            .then(|| executor_base_content_entries(&jj, &repository, &base, &head, &changed))
            .transpose()
            .map_err(|error| format!("the tree of base {base} could not be read: {error}"))?;
        let base_findings = baseline_store.and_then(|(db, project_id)| {
            crate::execution::lint_baseline::lint_baseline_for(
                &crate::execution::lint_baseline::LintBaselineSource {
                    jj: &jj,
                    object_repository: &object_repository,
                    repository: &repository,
                    base: &base,
                    head: &head,
                    base_label: Some(&target),
                },
                db,
                &project_id,
                [check_name.as_str()],
            )
        });
        Ok(BaseRelativeChange {
            jj,
            repository,
            base,
            head,
            target,
            changed,
            base_entries,
            base_findings,
        })
    })
    .await
    .map_err(|error| format!("manual check change task failed: {error}"))?
}

/// The transport a run of one check ships for `change`: its changed files, and
/// the executor-protocol base bundle or the lint baseline when the check is
/// one that receives it. The baseline is the one `change` was keyed with.
///
/// Synchronous: it reads the object database.
pub(super) fn base_relative_transport_for(
    change: &BaseRelativeChange,
    check_name: &str,
) -> Result<Vec<(String, String)>, String> {
    let BaseRelativeChange {
        jj,
        repository,
        base,
        head,
        changed,
        base_findings,
        ..
    } = change;
    let base_content = needs_executor_base_content(check_name)
        .then(|| executor_base_content_bundle(jj, repository, base, head, changed))
        .transpose()
        .map_err(|error| format!("executor protocol base content from {base}: {error}"))?;
    let mut env = check_transport_env(changed, base_content);
    env.extend(
        base_findings
            .as_ref()
            .map(crate::execution::lint_baseline::lint_baseline_env),
    );
    Ok(env)
}

/// The one item a manual run submits, less the per-attempt command and stream.
/// Built once, so every attempt carries the transport computed for this run.
// Each argument is a distinct, already-resolved fact about this one run;
// bundling them would only rename the parameter list.
#[allow(clippy::too_many_arguments)]
pub(super) fn manual_check_batch_item(
    check_name: &str,
    input_hash: &str,
    check: &CheckCommand,
    placement: &crate::execution::check_placement::EffectiveCheckPlacement,
    resource_class: CheckResourceClass,
    timeout_ms: u64,
    infra_scopes: InfraScopes,
    transport_env: Vec<(String, String)>,
) -> PlannedCheckBatchItem {
    PlannedCheckBatchItem {
        recording_plan: None,
        index: 0,
        name: check_name.to_string(),
        input_hash: input_hash.to_string(),
        resource_identity: CheckResourceIdentityInput::Configured {
            name: check_name.to_string(),
            check: check.clone(),
        },
        command: String::new(),
        stream_id: String::new(),
        env: transport_env,
        verdict_environment_names: Vec::new(),
        timeout_ms,
        executor: placement.selector.clone(),
        verdict_platforms: placement.verdict_platforms.clone(),
        resource_class,
        infra_scopes,
        avoid_executor_ids: Vec::new(),
    }
}

/// Resolve manual check evidence from an authenticated run's durable coordinate.
/// The logical head is sealed by construction, so process cwd and loose host files
/// cannot influence cache ownership or the computed tree.
///
/// A lookup only reads: it keys the job's logical head as it stands and never
/// prepares the branch against its base.
pub async fn manual_check_cache_context(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
) -> Result<ManualCheckCacheContext, String> {
    Ok(resolve_manual_check_contract_snapshot(
        orch,
        run_id,
        check_name,
        ManualCheckRevision::OwnHeadLiteral,
        None,
    )
    .await
    .map_err(|error| error.to_string())?
    .context)
}

/// Result of a trusted, agent-initiated configured check run.
///
/// Every identity in this response was derived by the runner from the authenticated
/// run coordinate and the project's live checks contract. Callers never supply a
/// command, content hash, environment fingerprint, or verdict.
///
/// The observation fields report what the run RECORDED, carried out of the
/// recorder itself. They are never re-derived from a second lookup: the key a
/// requesting machine computes does not describe a verdict another machine
/// produced, so a lookup-derived reply calls a spilled check unrecorded while its
/// row sits in the database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualConfiguredCheckResult {
    pub check_name: String,
    pub commit_sha: String,
    pub tree_hash: String,
    pub input_hash: String,
    /// The environment identity the recorded observation is keyed by. Empty when
    /// a legacy remote executor did not report one, and when this run recorded
    /// nothing at all.
    pub environment_fingerprint: String,
    /// The observation this run recorded, or `None` when it recorded none: a
    /// suppressed check never ran, and a failed write leaves a real verdict
    /// standing without a durable row behind it.
    #[serde(skip)]
    pub observation_id: Option<String>,
    /// Canonical, resolvable citation for the durable observation.
    pub observation_ref: Option<String>,
    /// Host-clock label for the source observation's actual run instant.
    pub ran_at: Option<String>,
    /// Whether the recorded observation may answer a later run of this check on
    /// these inputs without executing it.
    pub reusable: bool,
    /// Why this verdict was not recorded as reusable evidence, when the producer
    /// knows the concrete cause.
    pub not_recorded: Option<String>,
    /// `fresh` when this run produced the verdict, `cached` when a stored
    /// observation answered it without running anything, and `unmet` when the
    /// verdict came from a placement this request did not demand.
    pub disposition: String,
    pub passed: bool,
    /// Whether the successful exit is a verdict under the shared lane
    /// classification, including an exit-code-only validator.
    pub verified: bool,
    pub unverified_reason: Option<String>,
    /// `fast tier: N ran, D deferred` when the run was the rust-tests fast
    /// tier, so a reader never takes its green for a whole-suite one.
    pub tier: Option<String>,
    pub exit_code: Option<i32>,
    /// Set when the run produced NO VERDICT about the tree.
    pub no_verdict: Option<ManualCheckNoVerdict>,
    #[serde(skip)]
    pub(crate) left_running: Option<crate::fleet::LeftRunningAttempt>,
    #[serde(skip)]
    pub(crate) consumer_guard: Option<Arc<crate::fleet::CheckConsumerGuard>>,
    pub output_tail: String,
    /// The machine whose run is the recorded verdict: this run's, or for a
    /// reused verdict the source observation's. A mis-placed lane is visible
    /// here without reading its log.
    pub executor: Option<CheckMachine>,
    /// The platform the recorded verdict counts from.
    pub verdict_platform: Option<String>,
    /// The versions of the check's runtime instruments the recorded verdict was
    /// produced under (`bun 1.4.4`), named beside its machine because they are
    /// part of which verdict it is. `None` when the check declares no
    /// instrument or no observation was recorded.
    pub runtime: Option<String>,
    /// Why the batch pin this request inherited was not applied, when it
    /// contradicted the suite's configuration.
    pub placement_note: Option<String>,
    /// Why the reported verdict does not answer the placement this request
    /// demanded, when it does not. Such a verdict is not this request's answer.
    pub placement_unmet: Option<String>,
    /// How the judged commit was captured: the head before and after any
    /// preparation and the exact base it contains.
    pub capture: Option<CheckCapture>,
}

/// A machine as a check result names it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckMachine {
    /// The public name `cairn://executors` lists.
    pub name: String,
    pub os: String,
}

/// Why a manual run has no verdict to report.
///
/// An infrastructure failure and a suppression are facts about Cairn, not results
/// about the caller's change. Each has to render as itself: reporting either as a
/// red — or as a complaint about recording — hides the only thing the reader can
/// act on.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCheckNoVerdict {
    /// `infrastructure` when Cairn's own machinery failed around the check,
    /// `suppressed` when Cairn declined to run it after repeated failures,
    /// `empty` when its selection matched no tests, and otherwise the stored
    /// failure kind of a run that died without a verdict (`killed`,
    /// `environment`, `unclassified`, `cancelled`).
    pub kind: String,
    /// The consecutive infrastructure failures behind a `suppressed` decision.
    pub after_failures: Option<i64>,
    /// The named cause, composed for the reader where the failure happened.
    pub cause: String,
}

/// [`ManualCheckNoVerdict::kind`] when Cairn declined to run the check.
pub(crate) const NO_VERDICT_SUPPRESSED: &str = "suppressed";

pub(crate) const NO_VERDICT_LEFT_RUNNING: &str = "awaiting_adoption";

/// Build the manual reply from the outcome the run returned.
///
/// Everything here comes from the evaluation itself — its verdict, and the
/// observation its recorder wrote. A second lookup could only re-derive a key the
/// recorder may not have used, which is exactly how a verdict that ran on another
/// machine came back as "not recorded".
///
/// `command` is the configured command the run executed. Whether its green is
/// verified is the `/checks` board's own answer
/// ([`verdict_evidence_with_expectation`] over the lane's [`row_existence`]): a
/// green with no structured result is verified only for a command that is not a
/// test runner (a build, a type check, a validator script), and an unknown
/// command (a request recorded before its command was kept) is held to the test
/// runner's bar.
///
/// [`verdict_evidence_with_expectation`]: crate::execution::checks_status::verdict_evidence_with_expectation
/// [`row_existence`]: crate::execution::checks_status::row_existence
pub(super) fn manual_configured_check_result(
    check_name: &str,
    project_key: &str,
    commit_sha: String,
    tree_hash: String,
    input_hash: String,
    command: Option<&str>,
    outcome: CheckOutcome,
) -> ManualConfiguredCheckResult {
    use crate::execution::checks_status::{
        row_existence, verdict_evidence_with_expectation, LaneExistence, VerdictEvidence,
    };

    let no_verdict = manual_check_no_verdict(&outcome);
    let facts = outcome
        .parsed
        .as_ref()
        .map(ParsedCheckResult::verdict_facts);
    let existence = row_existence(
        outcome.passed,
        outcome.failure_kind.map(CheckFailureKind::as_str),
        outcome.suppressed_after,
        facts.as_ref(),
    );
    let evidence = verdict_evidence_with_expectation(
        outcome.passed,
        facts.as_ref(),
        command.map(crate::execution::check_parsers::is_test_runner_command),
    );
    let verified = matches!(evidence, VerdictEvidence::Verified)
        && matches!(existence, LaneExistence::Complete);
    let unverified_reason = (outcome.passed && !verified).then(|| match evidence {
        VerdictEvidence::Unverified(reason) => reason.to_string(),
        _ => existence.describe(),
    });
    let tier = outcome
        .parsed
        .as_ref()
        .and_then(ParsedCheckResult::fast_tier_label);
    let not_recorded = outcome.not_recorded.clone();
    let disposition = if outcome.cached { "cached" } else { "fresh" }.to_string();
    let (observation_id, observation_ref, ran_at, environment_fingerprint, reusable) =
        match outcome.recorded {
            Some(recorded) => (
                Some(recorded.id),
                Some(format!(
                    "cairn://p/{project_key}/check-observations/{}",
                    recorded.public_handle
                )),
                crate::clock::stamp_millis(recorded.ran_at),
                recorded.environment_fingerprint,
                recorded.reusable,
            ),
            None => (None, None, None, String::new(), false),
        };
    ManualConfiguredCheckResult {
        check_name: check_name.to_string(),
        commit_sha,
        tree_hash,
        input_hash,
        environment_fingerprint,
        observation_id,
        observation_ref,
        ran_at,
        reusable,
        not_recorded,
        disposition,
        passed: outcome.passed,
        verified,
        unverified_reason,
        tier,
        exit_code: outcome.exit_code,
        no_verdict,
        left_running: outcome.left_running,
        consumer_guard: outcome.consumer_guard,
        output_tail: outcome.output_tail,
        executor: None,
        verdict_platform: None,
        runtime: None,
        placement_note: None,
        placement_unmet: None,
        capture: None,
    }
}

/// Name a failed run that produced no verdict, by the classification the wake
/// body, the turn-end checks section, and the merge gate share
/// ([`row_existence`]). A run killed by a signal, one whose selection matched
/// nothing, and one that died in Cairn's machinery all end without judging the
/// change, so none of them may come back as a red.
///
/// A successful exit is never one: whether its green is verified is reported
/// beside it as `verified` and `unverified_reason`.
///
/// [`row_existence`]: crate::execution::checks_status::row_existence
pub(super) fn manual_check_no_verdict(outcome: &CheckOutcome) -> Option<ManualCheckNoVerdict> {
    use crate::execution::checks_status::{row_existence, LaneExistence};
    if outcome.left_running.is_some() {
        return Some(ManualCheckNoVerdict {
            kind: NO_VERDICT_LEFT_RUNNING.to_string(),
            after_failures: None,
            cause: outcome.output_tail.clone(),
        });
    }
    if outcome.passed {
        return None;
    }
    let existence = row_existence(
        false,
        outcome.failure_kind.map(CheckFailureKind::as_str),
        outcome.suppressed_after,
        outcome
            .parsed
            .as_ref()
            .map(ParsedCheckResult::verdict_facts)
            .as_ref(),
    );
    let kind = match existence {
        LaneExistence::Complete => return None,
        LaneExistence::Breaker { .. } => NO_VERDICT_SUPPRESSED.to_string(),
        LaneExistence::Died { kind } => match CheckFailureKind::from_stored(&kind) {
            Some(failure) if failure.is_infrastructure() => "infrastructure".to_string(),
            _ => kind,
        },
        other => other.label().to_string(),
    };
    Some(ManualCheckNoVerdict {
        kind,
        after_failures: outcome.suppressed_after,
        cause: outcome.output_tail.clone(),
    })
}

/// One step of a manual check's progress, as its requester is shown it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualCheckProgressUpdate {
    /// `parked`, `queued`, `running`, or the check's own board state.
    pub state: String,
    pub diagnostic: String,
    /// The machine running it, once one has claimed it.
    pub executor: Option<String>,
    /// When the current state began.
    pub since_unix_ms: Option<u64>,
}

pub(super) type ManualCheckProgress = Arc<dyn Fn(ManualCheckProgressUpdate) + Send + Sync>;

/// What a manual check's requester is shown for one board update.
///
/// The board's own diagnostic passes through. Replacing a running check's
/// "running on fedora since 09:48:09Z" with "running configured check" would
/// drop the one fact the requester is polling for.
pub(super) fn manual_progress_update(
    checks: &[CheckStatusEntry],
    phase: CheckPhaseStatus,
) -> ManualCheckProgressUpdate {
    let state = match phase.phase.as_deref() {
        Some("queued") => "parked".to_string(),
        Some("provisioning") => "queued".to_string(),
        Some("running") => "running".to_string(),
        _ => checks
            .first()
            .map_or_else(|| "running".to_string(), |check| check.state.clone()),
    };
    let diagnostic = phase.diagnostic.unwrap_or_else(|| match state.as_str() {
        "parked" => "parked waiting for check capacity".to_string(),
        state => format!("{state} configured check"),
    });
    ManualCheckProgressUpdate {
        state,
        diagnostic,
        executor: phase.executor,
        since_unix_ms: phase.since_unix_ms,
    }
}

/// One manual check request: the configured suite, the revision its run
/// names (captured at most once, by the run), and the durable request handle
/// its answer is recorded under, when the request has one.
pub(crate) struct ManualCheckRequest<'a> {
    pub check_name: &'a str,
    pub revision: ManualCheckRevision,
    pub placement: Option<crate::execution::check_placement::CheckPlacementOverride>,
    pub retry: bool,
    pub manual_handle: Option<&'a str>,
}

/// Execute one named configured suite at the revision a run's check names,
/// reporting progress as it goes.
///
/// This is the trusted manual producer. It deliberately accepts no raw command or
/// caller-asserted result metadata: the durable run resolves ownership, the
/// one capture of `revision` fixes the commit, the contract that commit declares
/// supplies the command, and the normal planned-check path computes identities,
/// performs conservative reuse, executes misses, parses results, and records
/// immutable observations.
pub(crate) async fn run_manual_configured_check_with_progress(
    orch: &Orchestrator,
    run_id: &str,
    request: ManualCheckRequest<'_>,
    progress: ManualCheckProgress,
) -> Result<ManualConfiguredCheckResult, ManualCheckError> {
    let ManualCheckRequest {
        check_name,
        revision,
        placement,
        retry,
        manual_handle,
    } = request;
    let contract = resolve_manual_check_contract_snapshot(
        orch,
        run_id,
        check_name,
        revision,
        placement.as_ref(),
    )
    .await?;
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &contract.context.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = contract.context.job_id.clone();
    if let Some(handle) = manual_handle {
        let recovery = ManualCheckRecoveryContext {
            project_key: contract.context.project_key.clone(),
            command: Some(contract.plan.command.clone()),
            placement: contract.placement.clone(),
            capture: contract.capture.clone(),
            runtime_instruments: contract
                .plan
                .verdict_runtime
                .iter()
                .map(|(name, _)| name.clone())
                .collect(),
        };
        let context = serde_json::to_string(&recovery).map_err(|error| error.to_string())?;
        let updated = db
            .execute(
                "UPDATE manual_check_handles SET context_json=?1,commit_sha=?4,tree_hash=?5,input_hash=?6,disposition='fresh'
                 WHERE project_id=?2 AND handle=?3",
                cairn_db::turso::params![
                    context, contract.context.project_id.as_str(), handle,
                    contract.context.commit_sha.as_str(), contract.context.tree_hash.as_str(),
                    contract.context.input_hash.as_str(),
                ],
            )
            .await
            .map_err(|error| format!("could not persist manual check request context: {error}"))?;
        if updated != 1 {
            return Err(ManualCheckError::Other(
                "manual check request has no durable handle".to_string(),
            ));
        }
    }
    execute_check_contract(
        orch,
        db,
        contract,
        CheckRunProducer {
            adopt: None,
            tool_use_id: match manual_handle {
                Some(handle) => format!("manual-check:{run_id}:handle={handle}:{check_name}"),
                None => format!("manual-check:{run_id}:{check_name}"),
            },
            node_kind: "manual-check".to_string(),
            owner_job_id: Some(job_id),
            purpose: None,
        },
        retry,
        progress,
    )
    .await
    .map_err(Into::into)
}

/// The physical batch item a restarted runner re-attaches a check run to: the
/// grant it joins, the machine running it, and its position in that batch.
#[derive(Clone, Debug)]
pub(crate) struct AdoptedAttempt {
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) executor_id: String,
    pub(crate) item_index: usize,
    pub(crate) item_count: usize,
}

/// Replay one physical batch item through the same check engine used before
/// the restart. The persisted plan preserves partial selections and definition
/// commits; the result's own provenance supplies its verdict environment.
pub(crate) async fn execute_adopted_check_item(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    claim: &crate::storage::check_lane_adoptions::Claim,
    item_index: usize,
) -> Result<ManualConfiguredCheckResult, String> {
    let value = claim.recording_plans.get(item_index).ok_or_else(|| {
        format!(
            "request {} has no recording contract for item {item_index}",
            claim.request_id
        )
    })?;
    let recording: CheckAdoptionRecordingPlan =
        serde_json::from_value(value.clone()).map_err(|error| {
            format!(
                "request {} item {item_index} has an unreadable recording contract: {error}",
                claim.request_id
            )
        })?;
    if claim.check_names.get(item_index) != Some(&recording.plan.name)
        || claim.input_hashes.get(item_index) != Some(&recording.input_hash)
        || claim.commit_sha != recording.evaluated_commit
    {
        return Err(format!("request {} item {item_index} recording contract disagrees with its granted suite, input hash, or commit", claim.request_id));
    }
    let project_key = db
        .query_opt_text(
            "SELECT key FROM projects WHERE id=?1",
            (claim.project_id.clone(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or("the adoption project no longer exists")?;
    let coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        &recording.evaluated_commit,
        &recording.defined_by_commit,
        &recording.tree_hash,
        &recording.input_hash,
    );
    let keyed = vec![(recording.plan.clone(), recording.input_hash.clone())];
    let identity = (claim.request_id.clone(), claim.attempt_id.clone());
    let executor = claim.executor_id.clone();
    let item_count = claim.check_names.len();
    let result_shape = recording.result_shape;
    if matches!(result_shape, CheckAdoptionResultShape::Single) && item_count != 1 {
        return Err("a single-command adoption contract names multiple batch items".into());
    }
    orch.fleet.defer_result_ack(&identity.0).retain();
    // Binding can end because the fleet settled loss, not because a command
    // produced a verdict. Decide that before entering the shared recorder.
    let executed = orch.fleet.bind_adopted(identity.clone(), &executor).await;
    let current =
        crate::storage::check_lane_adoptions::lookup(&orch.db.local, &identity.0, &identity.1)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("the adopted physical grant no longer has a durable claim")?;
    if matches!(current.state.as_str(), "lost" | "cancelled")
        || orch.fleet.check_claim(&identity).is_none()
    {
        return Err(current.evidence.unwrap_or_else(|| {
            "the fleet settled or released the adopted physical grant before recording".into()
        }));
    }
    let submitted_identity = identity.clone();
    let outcomes = run_planned_checks_with_board(
        db.clone(),
        &claim.project_id,
        CheckRunCommit {
            evaluated: &recording.evaluated_commit,
            defined_by: &recording.defined_by_commit,
        },
        &recording.tree_hash,
        &recording.job_id,
        &keyed,
        &[coordinate],
        &[InfraScopes::head_only(
            &recording.job_id,
            &recording.evaluated_commit,
        )],
        &recording.tool_use_id,
        CheckExecMode::Shared,
        Some(orch),
        None,
        &recording.effective_env,
        true,
        move |_, _, _| {
            let identity = submitted_identity.clone();
            let outcome = executed.outcome.clone();
            let account = executed.account.clone();
            async move {
                let mut result = match result_shape {
                    CheckAdoptionResultShape::Batch => {
                        split_write_check_batch_outcome(outcome, &account, item_count)
                            .0
                            .remove(item_index)
                    }
                    CheckAdoptionResultShape::Single => {
                        check_result_from_cell_outcome(outcome, &account, None)
                    }
                };
                if let Ok(result) = &mut result {
                    result.publication =
                        Some(crate::fleet::PublicationCoordination::for_request(identity));
                }
                result
            }
        },
        |_| {},
    )
    .await;
    let outcome = outcomes
        .into_iter()
        .next()
        .ok_or("the adopted item produced no outcome")?;
    if outcome.left_running.is_none() {
        let observation = outcome.recorded.as_ref().ok_or_else(|| {
            outcome
                .not_recorded
                .clone()
                .unwrap_or_else(|| "the adopted item produced no durable observation".into())
        })?;
        orch.fleet
            .record_check_claim_observation(&identity, &recording.plan.name, &observation.id)
            .await?;
    }
    Ok(manual_configured_check_result(
        &recording.plan.name,
        &project_key,
        recording.evaluated_commit,
        recording.tree_hash,
        recording.input_hash,
        Some(&recording.plan.command),
        outcome,
    ))
}

/// The producer a configured check run records itself as.
pub(crate) struct CheckRunProducer {
    pub(crate) adopt: Option<AdoptedAttempt>,
    /// Stream identity. Its prefix decides the observation's `cadence` label
    /// (see [`observation_cadence`]).
    pub(crate) tool_use_id: String,
    /// How the run's build cell names its owner on the Work surface.
    pub(crate) node_kind: String,
    /// The real job this run serves, when there is one. The main cadence has
    /// none: its `main:{project}` identity names no job row.
    pub(crate) owner_job_id: Option<String>,
    /// The standing question this run answers, when at most one live answer
    /// may exist at a time.
    pub(crate) purpose: Option<crate::fleet::purpose::PurposeKey>,
}

/// Execute one resolved configured-check contract and record its observation.
/// The executing half of the trusted manual producer, shared with the main
/// cadence.
pub(crate) async fn execute_check_contract(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    contract: CheckContractSnapshot,
    producer: CheckRunProducer,
    retry: bool,
    progress: ManualCheckProgress,
) -> Result<ManualConfiguredCheckResult, String> {
    if let Some(adopt) = &producer.adopt {
        let claim = orch
            .fleet
            .check_claim(&(adopt.request_id.clone(), adopt.attempt_id.clone()))
            .ok_or("the adopted physical grant has no open durable claim")?;
        if claim.executor_id != adopt.executor_id
            || claim.check_names.len() != adopt.item_count
            || claim.check_names.get(adopt.item_index) != Some(&contract.plan.name)
            || claim.input_hashes.get(adopt.item_index) != Some(&contract.context.input_hash)
        {
            return Err(
                "the rebuilt check contract does not match its adopted physical item".into(),
            );
        }
        let mut result = execute_adopted_check_item(orch, db, &claim, adopt.item_index).await?;
        // The claim is matched by suite and input hash, not commit, so the
        // contract's capture is reported only when it names the judged head.
        result.capture = contract
            .capture
            .filter(|capture| capture.head_after == result.commit_sha);
        return Ok(result);
    }
    let check_name = contract.plan.name.clone();
    let check_name = check_name.as_str();
    let capture = contract.capture;
    let placement = contract.placement;
    let context = contract.context;
    let repository_path = contract.repository_path;
    let repository = PathBuf::from(&repository_path);
    let plan = contract.plan;
    let transport_env = {
        let change = contract.change;
        let name = check_name.to_string();
        tokio::task::spawn_blocking(move || base_relative_transport_for(&change, &name))
            .await
            .map_err(|error| format!("manual check transport task failed: {error}"))
            .and_then(|transport| transport)
    }
    .map_err(|error| {
        format!(
            "Manual-check planning infrastructure failure: could not compute the base-relative \
             inputs {check_name} runs against: {error}"
        )
    })?;
    let keyed = vec![(plan.clone(), context.input_hash.clone())];
    let sealed_coordinates = vec![contract.sealed_coordinate];
    let progress_notify = progress.clone();
    let status_board = CheckStatusBoard::new(
        &keyed,
        Arc::new(move |checks, phase| progress_notify(manual_progress_update(&checks, phase))),
    );
    let timeout_ms = contract.timeout_ms;
    let store_dir = crate::jj::project_store_dir(&orch.config_dir, &repository);
    let CheckRunProducer {
        tool_use_id,
        node_kind,
        owner_job_id,
        purpose,
        adopt: _,
    } = producer;
    let project_id = context.project_id.clone();
    let job_id = context.job_id.clone();
    let commit_sha = context.commit_sha.clone();
    let command_check = contract.configured_check;
    let submission_orch = orch.clone();
    let submission_repo = repository_path.clone();
    let submission_store = store_dir.clone();
    let submission_project = project_id.clone();
    let submission_job = job_id.clone();
    let submission_commit = commit_sha.clone();
    let submission_tool = tool_use_id.clone();
    let snapshot_command = plan.command.clone();
    let infra_scopes = plan_infra_scopes(
        Some(orch),
        InfraScopeInputs {
            project_id: &project_id,
            job_id: &job_id,
            commit_sha: &commit_sha,
            repository: &repository_path,
            mutation_policy: MutationPolicy::PureVerdict,
            work_class: crate::fleet::PlacementWorkClass::ReviewChecks,
        },
        &[CheckPlacementQuestion {
            selector: placement.selector.clone(),
            verdict_platforms: placement.verdict_platforms.clone(),
            compiles_rust: CheckPlacementQuestion::command_compiles_rust(&plan.command),
        }],
    )
    .await;
    let mut item_template = manual_check_batch_item(
        check_name,
        &context.input_hash,
        &command_check,
        &placement,
        plan.resource_class,
        timeout_ms,
        infra_scopes[0].clone(),
        transport_env,
    );
    item_template.recording_plan = Some(CheckAdoptionRecordingPlan::new(
        &plan,
        &sealed_coordinates[0],
        &job_id,
        &tool_use_id,
        &item_template.env,
    ));
    let attempts = Arc::new(std::sync::Mutex::new(Vec::<(String, String)>::new()));
    let submitted_attempts = attempts.clone();
    let outcomes = run_planned_checks_with_board(
        db.clone(),
        &project_id,
        CheckRunCommit {
            evaluated: &commit_sha,
            defined_by: &contract.defined_by_commit,
        },
        &context.tree_hash,
        &job_id,
        &keyed,
        &sealed_coordinates,
        &infra_scopes,
        &tool_use_id,
        CheckExecMode::Shared,
        Some(orch),
        Some(status_board.clone()),
        &[],
        retry,
        move |_index, command, stream_id| {
            let orch = submission_orch.clone();
            let repository = submission_repo.clone();
            let store_dir = submission_store.clone();
            let project_id = submission_project.clone();
            let job_id = submission_job.clone();
            let commit_sha = submission_commit.clone();
            let tool_use_id = submission_tool.clone();
            let item_template = item_template.clone();
            let snapshot_command = snapshot_command.clone();
            let status_board = status_board.clone();
            let node_kind = node_kind.clone();
            let owner_job_id = owner_job_id.clone();
            let purpose = purpose.clone();
            let attempts = submitted_attempts.clone();
            async move {
                require_snapshot_command(&snapshot_command, &command)?;
                let mut batch_outcome = submit_planned_check_batch(
                    &orch,
                    PlannedCheckBatchRequest {
                        project_id: project_id.clone(),
                        repository,
                        store_dir,
                        sealed_commit: commit_sha,
                        requesting_job_id: job_id.clone(),
                        owner: cairn_common::executor_protocol::CellOwnerRef {
                            project_id,
                            project_key: None,
                            issue_number: None,
                            job_id: owner_job_id,
                            execution_seq: None,
                            node_kind: Some(node_kind),
                        },
                        affinity_key: Some(job_id),
                        priority: CellPriority::ReviewCheck,
                        env: Vec::new(),
                        items: vec![PlannedCheckBatchItem {
                            command,
                            stream_id,
                            ..item_template
                        }],
                        run_context: None,
                        mutation_policy: MutationPolicy::PureVerdict,
                        status_board: Some(status_board.clone()),
                        on_group_complete: None,
                        purpose,
                        cell_groups: Default::default(),
                        publication: None,
                    },
                )
                .await?;
                if let Some(request) = &batch_outcome.request {
                    attempts
                        .lock()
                        .unwrap()
                        .push((request.request_id.clone(), request.attempt_id.clone()));
                }
                batch_outcome.results.remove(&0).ok_or_else(|| {
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!("missing manual batch outcome for {tool_use_id}"),
                    )
                })?
            }
        },
        move |_| {},
    )
    .await;
    let outcome = outcomes.into_iter().next().ok_or_else(|| {
        format!("Cairn lost the result of {check_name}. Run the same command again.")
    })?;
    // The engine's one recorder has returned. Only now may a remote result
    // release the executor's retained attempt and its granted cell.
    let attempts = attempts.lock().unwrap().clone();
    for identity in attempts {
        if let Some(claim) = orch.fleet.check_claim(&identity) {
            if let Some(recorded) = &outcome.recorded {
                orch.fleet
                    .record_check_claim_observation(&identity, check_name, &recorded.id)
                    .await?;
            } else if outcome.left_running.is_none()
                && orch.fleet.adoption_owner(&identity).is_some()
            {
                let evidence = outcome
                    .not_recorded
                    .as_deref()
                    .unwrap_or("the check recorder returned no durable observation");
                crate::storage::check_lane_adoptions::transition(
                    &orch.db.local,
                    &identity.0,
                    &identity.1,
                    "lost",
                    Some(evidence),
                    None,
                    None,
                )
                .await
                .map_err(|error| format!("could not record lost check claim: {error}"))?;
                orch.fleet
                    .cancel_adopted(&identity.0, &identity.1, &claim.executor_id);
            }
        }
    }
    let mut result = manual_configured_check_result(
        check_name,
        &context.project_key,
        commit_sha,
        context.tree_hash,
        context.input_hash,
        Some(plan.command.as_str()),
        outcome,
    );
    let machine = result.observation_id.as_deref().and_then(|id| {
        crate::execution::cache::get_check_observation_machine(db.clone(), &project_id, id)
            .ok()
            .flatten()
    });
    let instruments = keyed[0]
        .0
        .verdict_runtime
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    enrich_manual_check_result(orch, &mut result, machine, &placement, &instruments);
    result.capture = capture;
    Ok(result)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct ManualCheckRecoveryContext {
    pub(super) project_key: String,
    /// The configured command the request ran, so a recovered reply judges its
    /// green by the same bar the live one did. Absent from requests recorded
    /// before it was kept; those recover as if it were a test runner.
    #[serde(default)]
    pub(super) command: Option<String>,
    pub(super) placement: crate::execution::check_placement::EffectiveCheckPlacement,
    /// How the request captured the commit it judged, so a recovered reply
    /// names the same head and base the live one did. Absent from requests
    /// recorded before it was kept.
    #[serde(default)]
    pub(super) capture: Option<CheckCapture>,
    pub(super) runtime_instruments: Vec<String>,
}

pub(super) fn enrich_manual_check_result(
    orch: &Orchestrator,
    result: &mut ManualConfiguredCheckResult,
    machine: Option<crate::execution::cache::ObservationMachine>,
    placement: &crate::execution::check_placement::EffectiveCheckPlacement,
    instruments: &[String],
) {
    if let Some(machine) = machine {
        result.executor = machine.executor_id.map(|id| CheckMachine {
            name: orch.fleet.executor_name_for_text(&id),
            os: machine
                .verdict_platform
                .clone()
                .unwrap_or_else(|| "unknown".into()),
        });
        result.verdict_platform = machine.verdict_platform;
        let runtime = cairn_checkenv::runtime_versions(
            instruments.iter().map(String::as_str),
            machine.toolchain_fingerprint.as_deref(),
        );
        result.runtime = (!runtime.is_empty())
            .then(|| crate::execution::check_identity::runtime_label(&runtime));
    }
    result.placement_unmet = unmet_placement(
        placement,
        result.executor.as_ref(),
        result.verdict_platform.as_deref(),
    );
    if result.placement_unmet.is_some() {
        result.disposition = "unmet".into();
    }
    result.placement_note = placement.note.clone();
}

/// Recover the exact source attached directly or by shared publication identity.
/// A commit alias can move on retry, so coordinates cannot answer a handle.
pub(crate) async fn recover_manual_check_result(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    project_id: &str,
    handle: &str,
) -> Result<ManualConfiguredCheckResult, String> {
    let project = project_id.to_string();
    let handle = handle.to_string();
    let (mut result, machine, context) = db.read(move |conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT h.commit_sha,h.tree_hash,h.input_hash,h.disposition,h.context_json,
                    o.id,o.public_handle,o.check_name,o.environment_fingerprint,o.reusable,o.ran_at,
                    o.exit_code,o.verdict,o.failure_kind,o.verdict_result_json,o.output_tail,o.duration_ms,
                    o.executor_id,o.verdict_platform,o.toolchain_fingerprint
               FROM manual_check_handles h
               JOIN check_result_observations o ON o.project_id=h.project_id
                AND o.id=COALESCE(h.observation_id, (
                    SELECT p.id FROM check_result_observations p
                     WHERE p.project_id=h.project_id AND p.publication_id=h.publication_id
                       AND p.publication_id IS NOT NULL
                ))
              WHERE h.project_id=?1 AND h.handle=?2",
            cairn_db::turso::params![project, handle],
        ).await?;
        let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::internal(
            "manual check request has no attached recorded observation",
        ))?;
        let context: ManualCheckRecoveryContext = serde_json::from_str(&row.text(4)?)
            .map_err(|error| crate::storage::DbError::internal(format!("invalid manual check recovery context: {error}")))?;
        let parsed = row.opt_text(14)?.map(|json| serde_json::from_str::<ParsedCheckResult>(&json))
            .transpose().map_err(|error| crate::storage::DbError::internal(format!("invalid recorded check result: {error}")))?;
        let executor_id = row.opt_text(17)?;
        let environment_fingerprint = row.text(8)?;
        let not_recorded = executor_id.as_deref()
            .is_some_and(|id| id != crate::fleet::COLOCATED_EXECUTOR_ID)
            .then_some(environment_fingerprint.is_empty())
            .filter(|empty| *empty)
            .map(|_| "the selected legacy executor did not report a complete verdict environment identity".to_string());
        let check_name = row.text(7)?;
        let outcome = CheckOutcome {
            left_running: None,
            consumer_guard: None,
            name: check_name.clone(),
            passed: row.text(12)? == "passed",
            exit_code: Some(row.i64(11)? as i32),
            failure_kind: row.opt_text(13)?.as_deref().and_then(CheckFailureKind::from_stored),
            parsed,
            output_tail: row.text(15)?,
            cached: row.text(3)? == "cached",
            duration_ms: row.i64(16)?,
            suppressed_after: None,
            recorded: Some(crate::execution::cache::RecordedCheckObservation {
                id: row.text(5)?, public_handle: row.text(6)?,
                ran_at: row.i64(10)?, environment_fingerprint,
                executor_id: executor_id.clone(), reusable: row.i64(9)? != 0,
            }),
            not_recorded,
        };
        let result = manual_configured_check_result(
            &check_name, &context.project_key, row.text(0)?, row.text(1)?, row.text(2)?,
            context.command.as_deref(), outcome,
        );
        let machine = crate::execution::cache::ObservationMachine {
            executor_id, verdict_platform: row.opt_text(18)?, toolchain_fingerprint: row.opt_text(19)?,
        };
        Ok((result, machine, context))
    })).await.map_err(|error| format!("could not recover recorded manual check: {error}"))?;
    enrich_manual_check_result(
        orch,
        &mut result,
        Some(machine),
        &context.placement,
        &context.runtime_instruments,
    );
    result.capture = context.capture;
    Ok(result)
}
