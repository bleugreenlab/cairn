//! Publish as placement (cairn/5962): one publish action, one publication
//! ledger, and legs placed on fleet builders as ordinary counted batches.
//!
//! A publication is started by its action (`cairn://p/{project}/publish`,
//! append `{action:"start"}`) or by the heartbeat. The driver claims the
//! oldest queued publication of a project once none is running there, runs
//! its plan leg, lays out a row per (target, leg) the plan has work for, runs
//! each target's build then verify in parallel across targets, and settles the
//! publication. Every leg is a one-item check batch with a free-form command,
//! owned by no job, so placement, admission and capacity waiting are the same
//! as for any other cell. A leg no builder can ever take is deferred with the
//! fleet's own reason and the publication ends with nothing queued (tenet 953).
//!
//! A deferred row is final, like every settled row. A verify leg defers until
//! the live per-target manifest names its commit, and nothing revisits it:
//! once the live release serves the commit, a later plan has no work for that
//! target and places no row. In this phase a Cairn verify runs by starting with
//! `force` against a commit the live release already names; in Phase B the
//! verify follows Cairn's own publish leg and meets its fact at once.
//!
//! The ledger is the actor's only state. A restart fails whatever was running
//! and settles its publication; the next start re-plans from the live
//! release, because the plan is a pure function of the release and the commit.
//!
//! A target is live when the publication's commit lists it in
//! `publish.liveTargets` (snapshotted at start). A live target's build,
//! manifest and publish run as ONE batch on a `publishOnly` executor whose
//! isolation measured PASS: the release secret is in that account's home, so
//! the bytes are built, re-hashed and uploaded without leaving it. The batch
//! is still two ledger rows, `build` and `publish`, split at the manifest's
//! result line (`progress`). Its verify follows only a publish that
//! succeeded. After every target, when a live publish succeeded or the plan
//! read the combined manifest as stale, `combine` runs on a publisher too
//! (whichever system finishes last makes the combined manifest current) and
//! a combined verify follows it. Order: plan < build < publish(target) <
//! verify(target), and every publish < combine < verify(all).
//!
//! Every other target is a shadow, as in Phase A: its build leg produces
//! evidence (asset hashes and a manifest-shape comparison against the live
//! per-target manifest) and uploads nothing, which the schema enforces per
//! leg. Shadow rows are laid out for manual and heartbeat starts only; a
//! merge starts a publication only when the merged commit lists a live
//! target, and lays out rows for its live targets alone, so a merge adds no
//! shadow release-compile load on the hosts GitHub Actions' builds need.
//! Windows stays refused: its signing is GitHub's (cairn/7055).
pub mod plan;
mod progress;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cairn_common::executor_protocol::{CellOwnerRef, ExecutorSelector, PublicationCustody};
use cairn_db::storage::publications::{self as ledger, LegAsset, LegUpdate, NewPublication};
use serde_json::Value;

use crate::config::project_settings::CheckResourceClass;
use crate::execution::cache::InfraScopes;
use crate::execution::checks::{
    submit_planned_check_batch, CheckExecutionFailure, PlannedCheckBatchItem,
    PlannedCheckBatchOutcome, PlannedCheckBatchRequest,
};
use crate::execution::wire::CheckResourceIdentityInput;
use crate::fleet::{CellPriority, KnownMachinePlatform, MutationPolicy};
use crate::mcp::handlers::run::CheckExecResult;
use crate::orchestrator::Orchestrator;
use crate::security::RedactionPolicy;
use crate::storage::{LocalDb, RowExt};
use plan::{LegVerdict, TargetSpec, ALL_TARGETS};

pub use cairn_db::storage::publications::{Publication, PublicationLeg, Started, TargetState};

/// How often the heartbeat starts a publication, matching the workflow's
/// cron.
const HEARTBEAT: Duration = Duration::from_secs(12 * 60 * 60);
/// A build leg compiles release binaries; past this it is not progressing.
/// Phase A's bound, which a live build-and-publish batch keeps: the upload is
/// minutes beside the compile, and this is no new kill rule (tenet 292).
const BUILD_TIMEOUT_MS: u64 = 4 * 60 * 60 * 1000;
/// Every other leg reads, plans or verifies.
const SHORT_TIMEOUT_MS: u64 = 30 * 60 * 1000;
/// The script whose presence on a project's checkout marks it as one that
/// publishes an executor (only Cairn's own repository).
const LEG_SCRIPT: &str = "scripts/publish-leg.ts";
/// Tools a `toolPin` publication may verify (`scripts/tool-pin.ts`).
const PINNED_TOOLS: &[&str] = &["codex", "claude"];

/// What a start asks for.
#[derive(Debug, Clone, Default)]
pub struct StartRequest {
    /// `executor` (the default) or `toolPin`.
    pub kind: Option<String>,
    pub commit: Option<String>,
    pub force: Vec<String>,
    pub tool: Option<String>,
    pub requested_by: Option<String>,
    /// `manual`, `heartbeat` or `merge`.
    pub trigger: String,
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn new_id() -> String {
    let at = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("pub-{at}-{}", &suffix[..8])
}

/// Validate what a start asks for, before anything is written.
pub fn validate(request: &StartRequest) -> Result<(), String> {
    match request.kind.as_deref().unwrap_or("executor") {
        "executor" => {
            if request.tool.is_some() {
                return Err("`tool` applies only to kind toolPin".into());
            }
        }
        "toolPin" => {
            let tool = request.tool.as_deref().ok_or("kind toolPin needs `tool`")?;
            if !PINNED_TOOLS.contains(&tool) {
                return Err(format!(
                    "`tool` must be one of {}, not `{tool}`",
                    PINNED_TOOLS.join(", ")
                ));
            }
            if !request.force.is_empty() {
                return Err("`force` applies only to kind executor".into());
            }
        }
        other => return Err(format!("`kind` must be executor or toolPin, not `{other}`")),
    }
    if let Some(commit) = &request.commit {
        if commit.len() != 40
            || !commit
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(format!(
                "`commit` must be a full lowercase sha, not `{commit}`"
            ));
        }
    }
    for target in &request.force {
        if plan::target_spec(target).is_none() {
            return Err(format!(
                "`force` names unknown target `{target}`; expected {}",
                plan::EXECUTOR_TARGETS.map(|spec| spec.triple).join(", ")
            ));
        }
    }
    Ok(())
}

/// Record a start and wake the project's driver. One pending publication per
/// project (tenet 653): a start replaces one still queued and queues behind
/// one running.
///
/// The commit is resolved here (the one asked for, else the default branch's
/// head), and its `publish.liveTargets` snapshotted with it, so the targets a
/// publication may upload are the ones its own commit declared. A list that
/// does not validate refuses the start.
pub async fn start(
    orch: &Orchestrator,
    project_id: &str,
    request: StartRequest,
) -> Result<Started, String> {
    validate(&request)?;
    let context = project_context(orch, project_id).await?;
    let commit = resolve_commit(&context, request.commit.as_deref()).await?;
    let live_targets = if request.kind.as_deref().unwrap_or("executor") == "executor" {
        live_targets_at(&context.store_dir, &commit).await?
    } else {
        Vec::new()
    };
    record_start(orch, project_id, request, commit, live_targets).await
}

async fn record_start(
    orch: &Orchestrator,
    project_id: &str,
    request: StartRequest,
    commit: String,
    live_targets: Vec<String>,
) -> Result<Started, String> {
    let id = new_id();
    let mode = if live_targets.is_empty() {
        "shadow"
    } else {
        "live"
    };
    let started = ledger::start(
        &orch.db.local,
        NewPublication {
            id: id.clone(),
            project_id: project_id.to_string(),
            kind: request.kind.clone().unwrap_or_else(|| "executor".into()),
            commit: Some(commit),
            trigger: request.trigger.clone(),
            requested_by: request.requested_by.clone(),
            force_targets: request.force.clone(),
            tool: request.tool.clone(),
            mode: mode.into(),
            live_targets,
            created_at: now_ms(),
        },
        &format!("a newer {} start", request.trigger),
    )
    .await
    .map_err(|error| format!("could not record the publication: {error}"))?;
    spawn_drive(orch, project_id);
    Ok(started)
}

/// The commit a publication builds: the one asked for, else the default
/// branch's head. A commit asked for must already be on the default branch.
///
/// Every leg of a publication runs that commit's own scripts, and any leg may
/// land on a publisher (a secret-bearing one only there), whose account holds
/// the release secret. So the trusted-source boundary is the default branch:
/// what reaches it went through review and merge, and nothing else is ever
/// built here, live or shadow. An agent that may start a publication can
/// therefore choose only among merged commits, never run an unmerged one in
/// the publisher's account.
async fn resolve_commit(context: &ProjectContext, asked: Option<&str>) -> Result<String, String> {
    let head = cairn_vcs::resolve_coordinate(&context.store_dir, &context.default_branch)
        .await
        .map_err(|error| format!("{} is unresolvable: {error}", context.default_branch))?;
    let Some(commit) = asked else {
        return Ok(head);
    };
    let store_dir = context.store_dir.clone();
    let on_branch = cairn_vcs::history::contains(&store_dir, commit, &head)
        .await
        .map_err(|error| error.to_string());
    require_on_default_branch(commit, &context.default_branch, on_branch)?;
    Ok(commit.to_string())
}

/// Refuse a commit that is not on the default branch, or whose ancestry could
/// not be read: an unknown answer is a refusal, never a pass.
fn require_on_default_branch(
    commit: &str,
    branch: &str,
    on_branch: Result<bool, String>,
) -> Result<(), String> {
    match on_branch {
        Ok(true) => Ok(()),
        Ok(false) => Err(format!(
            "a publication builds only commits on {branch}, and {commit} is not on it: its scripts would run in the publisher's account, which holds the release secret. Merge it first."
        )),
        Err(error) => Err(format!(
            "a publication builds only commits on {branch}, and whether {commit} is on it could not be read: {error}"
        )),
    }
}

/// The live targets `commit` declares (`publish.liveTargets`), validated.
/// No config, or no `publish` section, declares none.
async fn live_targets_at(store_dir: &std::path::Path, commit: &str) -> Result<Vec<String>, String> {
    let (store_dir, commit) = (store_dir.to_path_buf(), commit.to_string());
    let bytes = tokio::task::spawn_blocking(move || {
        crate::mcp::handlers::read::file_at_commit(
            store_dir,
            commit,
            crate::execution::checks::CHECKS_CONFIG_PATH,
        )
    })
    .await
    .map_err(|error| format!("reading the commit's config failed: {error}"))??;
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    let content = String::from_utf8(bytes)
        .map_err(|error| format!("the commit's .cairn/config.yaml is not UTF-8: {error}"))?;
    let (settings, _) = crate::config::project_settings::parse_project_settings(&content)
        .map_err(|error| format!("the commit's .cairn/config.yaml does not load: {error}"))?;
    plan::validate_live_targets(
        &settings
            .publish
            .map(|publish| publish.live_targets)
            .unwrap_or_default(),
    )
}

/// A merge advanced the project's default branch. Live mode brings the merge
/// trigger: when the merged commit lists live targets, start a `merge`
/// publication of it through the one-pending rule. Its plan leg is the path
/// filter: a merge that changed no publication input plans every target as
/// carried and places nothing more. With no live targets a merge starts
/// nothing, as in Phase A.
pub(crate) fn note_base_advance(orch: &Orchestrator, project_id: &str) {
    let orch = orch.clone();
    let project_id = project_id.to_string();
    tokio::spawn(async move {
        if let Err(error) = start_on_merge(&orch, &project_id).await {
            log::warn!("publications: the merge trigger for project {project_id}: {error}");
        }
    });
}

async fn start_on_merge(orch: &Orchestrator, project_id: &str) -> Result<(), String> {
    let context = project_context(orch, project_id).await?;
    if !std::path::Path::new(&context.repository)
        .join(LEG_SCRIPT)
        .exists()
    {
        return Ok(());
    }
    let commit = resolve_commit(&context, None).await?;
    let live_targets = live_targets_at(&context.store_dir, &commit).await?;
    if !merge_starts_a_publication(&live_targets) {
        return Ok(());
    }
    let request = StartRequest {
        trigger: "merge".into(),
        ..StartRequest::default()
    };
    record_start(orch, project_id, request, commit, live_targets).await?;
    Ok(())
}

/// Whether a merge's commit is published: only once it lists a live target.
fn merge_starts_a_publication(live_targets: &[String]) -> bool {
    !live_targets.is_empty()
}

/// Run the project's queued publications one at a time until none is left.
/// Safe to call whenever: the ledger's claim admits one driver per project.
pub(crate) fn spawn_drive(orch: &Orchestrator, project_id: &str) {
    let orch = orch.clone();
    let project_id = project_id.to_string();
    tokio::spawn(async move {
        loop {
            let claimed = match ledger::claim_next(&orch.db.local, &project_id).await {
                Ok(Some(publication)) => publication,
                Ok(None) => return,
                Err(error) => {
                    log::warn!("publications for project {project_id}: claim failed: {error}");
                    return;
                }
            };
            let id = claimed.id.clone();
            // Supervised, so a panicking run still settles its publication
            // rather than holding the project's one running slot forever.
            let run = tokio::spawn({
                let orch = orch.clone();
                async move { run_publication(&orch, &claimed).await }
            })
            .await;
            let fault = match run {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(join) => Some(format!("the publication actor panicked: {join}")),
            };
            if let Some(fault) = fault {
                log::warn!("publication {id}: {fault}");
                if let Err(error) = ledger::abandon(&orch.db.local, &id, &fault, now_ms()).await {
                    log::warn!("publication {id}: could not settle after `{fault}`: {error}");
                }
            }
        }
    });
}

/// Boot: settle what a restart interrupted, drive whatever is queued, and
/// start the heartbeat.
pub(crate) async fn boot(orch: &Orchestrator) {
    match ledger::recover_at_boot(&orch.db.local, now_ms()).await {
        Ok(settled) if !settled.is_empty() => log::info!(
            "publications: the restart interrupted {} publication(s), settled as `{}`: {}",
            settled.len(),
            ledger::RESTARTED_MID_LEG,
            settled.join(", ")
        ),
        Ok(_) => {}
        Err(error) => log::warn!("publications: boot recovery failed: {error}"),
    }
    match ledger::projects_with_queued(&orch.db.local).await {
        Ok(projects) => {
            for project_id in projects {
                spawn_drive(orch, &project_id);
            }
        }
        Err(error) => log::warn!("publications: could not read queued publications: {error}"),
    }
    spawn_heartbeat(orch);
}

/// Start a publication for every project that publishes an executor, every
/// [`HEARTBEAT`]. It closes whatever a merge left undone, the combined
/// manifest included. The first tick waits a full interval.
fn spawn_heartbeat(orch: &Orchestrator) {
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut ticks =
            tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            for project_id in publishing_projects(&orch).await {
                let request = StartRequest {
                    trigger: "heartbeat".into(),
                    ..StartRequest::default()
                };
                if let Err(error) = start(&orch, &project_id, request).await {
                    log::warn!("publications heartbeat for project {project_id}: {error}");
                }
            }
        }
    });
}

async fn publishing_projects(orch: &Orchestrator) -> Vec<String> {
    let mut projects = Vec::new();
    for db in orch.db.all_dbs().await {
        let rows = db
            .query_all(
                "SELECT id, repo_path FROM projects WHERE repo_path != '' AND default_branch IS NOT NULL",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await;
        match rows {
            Ok(rows) => projects.extend(
                rows.into_iter()
                    .filter(|(_, repo)| std::path::Path::new(repo).join(LEG_SCRIPT).exists())
                    .map(|(id, _)| id),
            ),
            Err(error) => log::warn!("publications heartbeat: project scan failed: {error}"),
        }
    }
    projects
}

/// The ledger's answer for one target's executor at one protocol, for the
/// rebuild gate (cairn/6236).
pub async fn target_state(
    db: &LocalDb,
    protocol: i64,
    target: &str,
) -> Result<TargetState, String> {
    ledger::target_state(db, protocol, target)
        .await
        .map_err(|error| error.to_string())
}

/// A publication as the build center shows it: its row, its project's key,
/// and its legs.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicationView {
    pub publication: Publication,
    pub project_key: String,
    pub uri: String,
    pub legs: Vec<PublicationLeg>,
}

/// The newest publications on this install, each with its legs.
pub async fn recent(orch: &Orchestrator, limit: usize) -> Result<Vec<PublicationView>, String> {
    let rows = ledger::list_recent(&orch.db.local, limit)
        .await
        .map_err(|error| error.to_string())?;
    let mut views = Vec::with_capacity(rows.len());
    for publication in rows {
        let legs = ledger::legs(&orch.db.local, &publication.id)
            .await
            .map_err(|error| error.to_string())?;
        let project_key = publication_project_key(orch, &publication).await;
        views.push(PublicationView {
            uri: cairn_uri::build_project_publication_uri(&project_key, &publication.id),
            publication,
            project_key,
            legs,
        });
    }
    Ok(views)
}

/// The last `lines` lines of one leg's log, or none before it has one.
pub async fn leg_log_tail(
    orch: &Orchestrator,
    publication: &str,
    target: &str,
    leg: &str,
    lines: usize,
) -> Result<Vec<String>, String> {
    for segment in [publication, target, leg] {
        if segment.is_empty()
            || !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || segment.starts_with('.')
        {
            return Err(format!("`{segment}` is not a publication, target or leg"));
        }
    }
    let path = leg_log_path(&orch.config_dir, publication, target, leg);
    match tokio::fs::read_to_string(&path).await {
        Ok(text) => {
            let all: Vec<&str> = text.lines().collect();
            let from = all.len().saturating_sub(lines.min(500));
            Ok(all[from..].iter().map(|line| line.to_string()).collect())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.to_string()),
    }
}

/// Where a leg's log lives on this install.
pub(crate) fn leg_log_path(
    config_dir: &std::path::Path,
    publication: &str,
    target: &str,
    leg: &str,
) -> std::path::PathBuf {
    config_dir
        .join("publications")
        .join(publication)
        .join(format!("{target}.{leg}.log"))
}

/// The project a publication runs in, resolved once.
struct ProjectContext {
    project_id: String,
    project_key: String,
    repository: String,
    store_dir: std::path::PathBuf,
    default_branch: String,
}

async fn project_context(orch: &Orchestrator, project_id: &str) -> Result<ProjectContext, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let (project_key, repository, default_branch) = db
        .query_opt(
            "SELECT key, repo_path, default_branch FROM projects WHERE id = ?1",
            (project_id.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("project {project_id} does not exist"))?;
    let default_branch =
        default_branch.ok_or_else(|| format!("project {project_key} has no default branch"))?;
    let store_dir = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = std::path::PathBuf::from(&repository);
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("coordinate repository task failed: {error}"))??
    };
    Ok(ProjectContext {
        project_id: project_id.to_string(),
        project_key,
        repository,
        store_dir,
        default_branch,
    })
}

/// Places one leg's batch: production submits it to the fleet, a test answers
/// it from a script.
pub(crate) trait LegPlacer: Send + Sync {
    fn place(
        &self,
        batch: PlannedCheckBatchRequest,
    ) -> futures_util::future::BoxFuture<'_, Result<PlannedCheckBatchOutcome, String>>;
}

struct FleetPlacer(Orchestrator);

impl LegPlacer for FleetPlacer {
    fn place(
        &self,
        batch: PlannedCheckBatchRequest,
    ) -> futures_util::future::BoxFuture<'_, Result<PlannedCheckBatchOutcome, String>> {
        Box::pin(submit_planned_check_batch(&self.0, batch))
    }
}

/// What a running publication reads and writes beyond its own row: the
/// ledger, where logs go, how legs are placed, and the fleet as it stood when
/// the publication was claimed.
struct Actor {
    db: Arc<LocalDb>,
    config_dir: PathBuf,
    policy: Arc<RedactionPolicy>,
    placer: Arc<dyn LegPlacer>,
    name_of: Arc<dyn Fn(&str) -> String + Send + Sync>,
    machines: Vec<KnownMachinePlatform>,
    project_key: String,
}

impl Actor {
    async fn of(orch: &Orchestrator, publication: &Publication) -> Self {
        let fleet = orch.clone();
        Self {
            db: orch.db.local.clone(),
            config_dir: orch.config_dir.clone(),
            policy: crate::security::redaction_policy(&orch.config_dir),
            placer: Arc::new(FleetPlacer(orch.clone())),
            name_of: Arc::new(move |executor: &str| fleet.fleet.executor_name_for_text(executor)),
            machines: orch.fleet.known_machine_platforms(),
            project_key: publication_project_key(orch, publication).await,
        }
    }
}

fn now_unix_ms() -> u64 {
    now_ms().max(0) as u64
}

/// One leg to place: what it runs, where it may go, and how long it may take.
struct LegSpec {
    target: String,
    leg: &'static str,
    command: String,
    env: Vec<(String, String)>,
    os: &'static str,
    required_toolchains: Vec<String>,
    avoid: Vec<String>,
    timeout_ms: u64,
    custody: Option<PublicationCustody>,
    /// Set for the publisher's build-and-publish batch: the row it opens when
    /// its build settles.
    then: Option<&'static str>,
}

/// What one placed leg came back with. For a build-and-publish batch,
/// `verdict` is the publish row's and `build` the build row's.
struct LegEnding {
    verdict: LegVerdict,
    build: Option<LegVerdict>,
}

async fn run_publication(orch: &Orchestrator, publication: &Publication) -> Result<(), String> {
    let context = project_context(orch, &publication.project_id).await?;
    let commit = match &publication.commit {
        Some(commit) => commit.clone(),
        None => resolve_commit(&context, None).await?,
    };
    ledger::record_plan(&orch.db.local, &publication.id, Some(&commit), None, None)
        .await
        .map_err(|error| error.to_string())?;
    let actor = Actor::of(orch, publication).await;
    let summary = if publication.kind == "toolPin" {
        run_tool_pin(&actor, &context, publication, &commit).await?
    } else {
        run_executor(&actor, &context, publication, &commit).await?
    };
    ledger::settle(&orch.db.local, &publication.id, &summary, now_ms())
        .await
        .map_err(|error| error.to_string())
}

async fn run_tool_pin(
    actor: &Actor,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
) -> Result<String, String> {
    let tool = publication
        .tool
        .clone()
        .ok_or("a toolPin publication names no tool")?;
    add_leg(actor, publication, &tool, "verify", 0, false).await?;
    let spec = TargetSpec {
        triple: "x86_64-unknown-linux-gnu",
        os: "linux",
        builder_arch: "x86_64",
    };
    let custody = plan::custody_for(false);
    let builders = plan::builders_for(spec, &actor.machines, custody, now_unix_ms());
    if builders.eligible.is_empty() {
        settle_leg(
            actor,
            publication,
            &tool,
            "verify",
            &plan_deferral(&tool, spec, &builders),
        )
        .await?;
        return Ok(format!("{tool} pin verify deferred: no eligible builder"));
    }
    let ending = place_leg(
        actor,
        context,
        publication,
        commit,
        LegSpec {
            target: tool.clone(),
            leg: "verify",
            command: plan::tool_verify_command(&tool),
            env: Vec::new(),
            os: "linux",
            required_toolchains: Vec::new(),
            avoid: builders.avoid,
            timeout_ms: SHORT_TIMEOUT_MS,
            custody: Some(custody),
            then: None,
        },
    )
    .await?;
    Ok(format!("{tool} pin verify {}", ending.0.verdict.state()))
}

fn plan_deferral(target: &str, spec: TargetSpec, builders: &plan::Builders) -> LegVerdict {
    let reason = plan::no_builder_reason(spec, builders);
    LegVerdict::Deferred(reason.replacen(spec.triple, target, 1))
}

/// The machine a combine runs on: a Linux publisher, as for the Linux
/// targets' publishes.
const COMBINE_MACHINE: TargetSpec = TargetSpec {
    triple: "x86_64-unknown-linux-gnu",
    os: "linux",
    builder_arch: "x86_64",
};

async fn run_executor(
    actor: &Actor,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
) -> Result<String, String> {
    let live_targets = &publication.live_targets;
    let now = now_unix_ms();
    add_leg(actor, publication, ALL_TARGETS, "plan", 0, false).await?;
    let plan_ending = place_leg(
        actor,
        context,
        publication,
        commit,
        LegSpec {
            target: ALL_TARGETS.into(),
            leg: "plan",
            command: plan::plan_command(
                &publication.id,
                &plan::eligible_targets_csv(&actor.machines, live_targets, now),
                &publication.force_targets,
            ),
            env: Vec::new(),
            os: "linux",
            required_toolchains: Vec::new(),
            avoid: Vec::new(),
            timeout_ms: SHORT_TIMEOUT_MS,
            custody: Some(plan::custody_for(false)),
            then: None,
        },
    )
    .await?;
    let (verdict, documents) = plan_ending;
    if verdict.verdict != LegVerdict::Succeeded {
        return Ok(format!(
            "plan {}: {}",
            verdict.verdict.state(),
            verdict.verdict.reason().unwrap_or_default()
        ));
    }
    let Some(plan_document) = documents.last().cloned() else {
        return Ok("plan succeeded but printed no plan document".into());
    };
    ledger::record_plan(
        &actor.db,
        &publication.id,
        None,
        plan_document["protocolVersion"].as_i64(),
        Some(&plan_document),
    )
    .await
    .map_err(|error| error.to_string())?;
    // A merge lays out its live targets alone: shadow evidence is gathered on
    // the action and the heartbeat, never on every merge.
    let merge = publication.trigger == "merge";
    let targets: Vec<(plan::PlannedTarget, bool)> = plan::planned_targets(&plan_document)
        .into_iter()
        .map(|target| {
            let live = live_targets.contains(&target.triple);
            (target, live)
        })
        .filter(|(_, live)| *live || !merge)
        .collect();
    let stale_combined = plan::combined_live(&plan_document) == Some(false);
    if targets.is_empty() && !(stale_combined && !live_targets.is_empty()) {
        return Ok("the live release already serves every target at this commit".into());
    }
    for (index, (target, live)) in targets.iter().enumerate() {
        let seq = 1 + index as i64 * 3;
        add_leg(actor, publication, &target.triple, "build", seq, *live).await?;
        if *live {
            add_leg(actor, publication, &target.triple, "publish", seq + 1, true).await?;
        }
        add_leg(actor, publication, &target.triple, "verify", seq + 2, *live).await?;
    }
    let plan_text = plan_document.to_string();
    let runs = targets.iter().map(|(target, live)| {
        run_target(
            actor,
            context,
            publication,
            commit,
            target,
            *live,
            &plan_text,
            now,
        )
    });
    let endings = futures_util::future::join_all(runs).await;
    let mut counts = BTreeMap::<&'static str, usize>::new();
    let mut published = false;
    for ending in endings {
        let ending = ending?;
        published |= ending.published;
        for state in ending.states {
            *counts.entry(state).or_default() += 1;
        }
    }
    if !live_targets.is_empty() && (published || stale_combined) {
        let seq = 1 + targets.len() as i64 * 3;
        for state in run_combine(actor, context, publication, commit, seq, now).await? {
            *counts.entry(state).or_default() += 1;
        }
    }
    let mode = if live_targets.is_empty() {
        "shadow".to_string()
    } else {
        format!("live ({})", live_targets.join(", "))
    };
    Ok(format!(
        "{mode} publication of {} target(s) at protocol v{}: {}",
        targets.len(),
        plan_document["protocolVersion"],
        counts
            .iter()
            .map(|(state, count)| format!("{count} {state}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// What one target's legs ended in, and whether its publish succeeded.
struct TargetEnding {
    states: Vec<&'static str>,
    published: bool,
}

const PUBLISH_DID_NOT_SUCCEED: &str = "deferred: publish did not succeed";
const BUILD_DID_NOT_SUCCEED: &str = "deferred: build did not succeed";

/// One target's legs. A shadow target builds then verifies, the verify
/// waiting on the live release whether or not the build succeeded. A live
/// target builds and publishes in one publisher batch, and verifies only
/// what it published.
#[allow(clippy::too_many_arguments)]
async fn run_target(
    actor: &Actor,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
    target: &plan::PlannedTarget,
    live: bool,
    plan_text: &str,
    now: u64,
) -> Result<TargetEnding, String> {
    let triple = target.triple.as_str();
    let legs: &[&str] = if live {
        &["build", "publish", "verify"]
    } else {
        &["build", "verify"]
    };
    let Some(spec) = plan::target_spec(triple) else {
        let refused = LegVerdict::Refused(format!(
            "the plan names a target this runner does not know: {triple}"
        ));
        for leg in legs {
            settle_leg(actor, publication, triple, leg, &refused).await?;
        }
        return Ok(TargetEnding {
            states: legs.iter().map(|_| "refused").collect(),
            published: false,
        });
    };
    let custody = plan::custody_for(live);
    let builders = plan::builders_for(spec, &actor.machines, custody, now);
    let unplaced = if spec.os == "windows" {
        Some(LegVerdict::Refused(plan::WINDOWS_SIGNING_REFUSAL.into()))
    } else if target.deferred || builders.eligible.is_empty() {
        // The plan was told which targets have a builder, so its deferral and
        // this one agree; the fleet's own account is the reason recorded, in
        // placement's words for a publisher it refused.
        Some(LegVerdict::Deferred(plan::no_builder_reason(
            spec, &builders,
        )))
    } else {
        None
    };
    let build_spec = LegSpec {
        target: triple.into(),
        leg: "build",
        command: if live {
            plan::live_build_command(&publication.id, triple, commit)
        } else {
            plan::build_command(&publication.id, triple, commit)
        },
        env: vec![(plan::PLAN_ENV.into(), plan_text.into())],
        os: spec.os,
        required_toolchains: vec!["rust".into()],
        avoid: builders.avoid.clone(),
        timeout_ms: BUILD_TIMEOUT_MS,
        custody: Some(custody),
        then: live.then_some("publish"),
    };
    let verify_spec = LegSpec {
        target: triple.into(),
        leg: "verify",
        command: plan::verify_command(&publication.id, triple, commit, live),
        env: vec![(plan::PLAN_ENV.into(), plan_text.into())],
        os: "linux",
        required_toolchains: Vec::new(),
        avoid: Vec::new(),
        timeout_ms: SHORT_TIMEOUT_MS,
        custody: Some(plan::custody_for(false)),
        then: None,
    };
    if !live {
        let build = match unplaced {
            Some(verdict) => {
                settle_leg(actor, publication, triple, "build", &verdict).await?;
                verdict
            }
            None => {
                place_leg(actor, context, publication, commit, build_spec)
                    .await?
                    .0
                    .verdict
            }
        };
        // Verify reads the live release anonymously and waits on the fact that
        // it names this commit, so it runs whether or not this shadow build did.
        let verify = place_leg(actor, context, publication, commit, verify_spec)
            .await?
            .0
            .verdict;
        return Ok(TargetEnding {
            states: vec![build.state(), verify.state()],
            published: false,
        });
    }
    let (build, publish) = match unplaced {
        Some(verdict) => {
            settle_leg(actor, publication, triple, "build", &verdict).await?;
            settle_leg(actor, publication, triple, "publish", &verdict).await?;
            (verdict.clone(), verdict)
        }
        None => {
            let ending = place_leg(actor, context, publication, commit, build_spec)
                .await?
                .0;
            (
                ending.build.unwrap_or(LegVerdict::Succeeded),
                ending.verdict,
            )
        }
    };
    let verify = if publish == LegVerdict::Succeeded {
        place_leg(actor, context, publication, commit, verify_spec)
            .await?
            .0
            .verdict
    } else {
        let deferred = LegVerdict::Deferred(PUBLISH_DID_NOT_SUCCEED.into());
        settle_leg(actor, publication, triple, "verify", &deferred).await?;
        deferred
    };
    Ok(TargetEnding {
        states: vec![build.state(), publish.state(), verify.state()],
        published: publish == LegVerdict::Succeeded,
    })
}

/// Combine, then the combined verify. Combine needs the release secret, so a
/// fleet with no measured Linux publisher defers both with nothing queued.
async fn run_combine(
    actor: &Actor,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
    seq: i64,
    now: u64,
) -> Result<Vec<&'static str>, String> {
    add_leg(actor, publication, ALL_TARGETS, "combine", seq, true).await?;
    add_leg(actor, publication, ALL_TARGETS, "verify", seq + 1, true).await?;
    let custody = plan::custody_for(true);
    let builders = plan::builders_for(COMBINE_MACHINE, &actor.machines, custody, now);
    let (combine, combined_sha256) = if builders.eligible.is_empty() {
        let verdict = plan_deferral(ALL_TARGETS, COMBINE_MACHINE, &builders);
        settle_leg(actor, publication, ALL_TARGETS, "combine", &verdict).await?;
        (verdict, None)
    } else {
        let (ending, documents) = place_leg(
            actor,
            context,
            publication,
            commit,
            LegSpec {
                target: ALL_TARGETS.into(),
                leg: "combine",
                command: plan::combine_command(&publication.id, commit),
                env: Vec::new(),
                os: COMBINE_MACHINE.os,
                required_toolchains: Vec::new(),
                avoid: builders.avoid,
                timeout_ms: SHORT_TIMEOUT_MS,
                custody: Some(custody),
                then: None,
            },
        )
        .await?;
        // The bytes combine uploaded (or found already live), which the
        // combined verify waits for at the public URL.
        let sha256 = assets_of(&documents)
            .and_then(|assets| assets.into_iter().next())
            .map(|asset| asset.sha256);
        (ending.verdict, sha256)
    };
    let verify = if combine != LegVerdict::Succeeded {
        let deferred = LegVerdict::Deferred("deferred: combine did not succeed".into());
        settle_leg(actor, publication, ALL_TARGETS, "verify", &deferred).await?;
        deferred
    } else if let Some(sha256) = combined_sha256 {
        place_leg(
            actor,
            context,
            publication,
            commit,
            LegSpec {
                target: ALL_TARGETS.into(),
                leg: "verify",
                command: plan::combined_verify_command(&publication.id, commit, &sha256),
                env: Vec::new(),
                os: "linux",
                required_toolchains: Vec::new(),
                avoid: Vec::new(),
                timeout_ms: SHORT_TIMEOUT_MS,
                custody: Some(plan::custody_for(false)),
                then: None,
            },
        )
        .await?
        .0
        .verdict
    } else {
        let deferred = LegVerdict::Deferred(
            "deferred: combine succeeded but recorded no digest, so which combined manifest to verify is unknown"
                .into(),
        );
        settle_leg(actor, publication, ALL_TARGETS, "verify", &deferred).await?;
        deferred
    };
    Ok(vec![combine.state(), verify.state()])
}

async fn add_leg(
    actor: &Actor,
    publication: &Publication,
    target: &str,
    leg: &str,
    seq: i64,
    live: bool,
) -> Result<(), String> {
    let mode = if live { "live" } else { "shadow" };
    ledger::add_leg(&actor.db, &publication.id, target, leg, seq, mode)
        .await
        .map_err(|error| error.to_string())
}

async fn settle_leg(
    actor: &Actor,
    publication: &Publication,
    target: &str,
    leg: &str,
    verdict: &LegVerdict,
) -> Result<(), String> {
    ledger::update_leg(
        &actor.db,
        &publication.id,
        target,
        leg,
        LegUpdate {
            state: Some(verdict.state().into()),
            reason: verdict.reason().map(str::to_string),
            ended_at: Some(now_ms()),
            ..LegUpdate::default()
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// The assets a leg's result documents record, from the last one naming any.
pub(crate) fn assets_of(documents: &[Value]) -> Option<Vec<LegAsset>> {
    documents
        .iter()
        .rev()
        .find_map(|document| {
            document["assets"]
                .as_array()
                .filter(|assets| !assets.is_empty())
                .cloned()
        })
        .and_then(|assets| serde_json::from_value(Value::Array(assets)).ok())
}

fn last_field(documents: &[Value], key: &str) -> Option<Value> {
    documents
        .iter()
        .rev()
        .map(|document| &document[key])
        .find(|value| !value.is_null())
        .cloned()
}

fn documents_of(documents: &[Value], legs: &[&str]) -> Vec<Value> {
    documents
        .iter()
        .filter(|document| {
            document["leg"]
                .as_str()
                .is_some_and(|leg| legs.contains(&leg))
        })
        .cloned()
        .collect::<Vec<_>>()
}

/// The two rows of a build-and-publish batch, from how it ended. The build
/// succeeded once its manifest did, which a publish record also proves; a
/// build that did not succeed leaves its publish deferred. A publish that
/// exited 0 without printing its record is not taken as one.
fn split_verdicts(
    result: &Result<CheckExecResult, CheckExecutionFailure>,
    target: &str,
    documents: &[Value],
    streamed_build: bool,
) -> (LegVerdict, LegVerdict) {
    let build_documents = documents_of(documents, &["build", "manifest"]);
    let publish_documents = documents_of(documents, &["publish"]);
    let manifest_succeeded = documents_of(documents, &["manifest"])
        .iter()
        .any(|document| {
            ["error", "deferred", "refused"]
                .iter()
                .all(|key| document[*key].is_null())
        });
    let build = if streamed_build || manifest_succeeded || !publish_documents.is_empty() {
        LegVerdict::Succeeded
    } else {
        match result {
            Ok(ran) => match plan::completed_verdict(ran, &build_documents) {
                LegVerdict::Succeeded => {
                    LegVerdict::Failed("the batch ended without its manifest's result".into())
                }
                verdict => verdict,
            },
            Err(failure) => plan::failure_verdict(target, failure),
        }
    };
    let publish = if build != LegVerdict::Succeeded {
        LegVerdict::Deferred(BUILD_DID_NOT_SUCCEED.into())
    } else {
        match result {
            Ok(ran) => match plan::completed_verdict(ran, &publish_documents) {
                LegVerdict::Succeeded if publish_documents.is_empty() => {
                    LegVerdict::Failed("the batch ended without the publish leg's result".into())
                }
                verdict => verdict,
            },
            Err(failure) => plan::failure_verdict(target, failure),
        }
    };
    (build, publish)
}

/// Place one leg as a one-item check batch owned by no job, record what it
/// came back with, and write its log. Returns the ending and the result
/// documents it printed.
async fn place_leg(
    actor: &Actor,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
    spec: LegSpec,
) -> Result<(LegEnding, Vec<Value>), String> {
    let started_at = now_ms();
    let log_path = leg_log_path(&actor.config_dir, &publication.id, &spec.target, spec.leg);
    let log_ref = cairn_uri::build_project_publication_leg_log_uri(
        &actor.project_key,
        &publication.id,
        &spec.target,
        spec.leg,
    );
    ledger::update_leg(
        &actor.db,
        &publication.id,
        &spec.target,
        spec.leg,
        LegUpdate {
            state: Some("running".into()),
            started_at: Some(started_at),
            log_ref: Some(log_ref.clone()),
            ..LegUpdate::default()
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let name = format!("publish {} {}", spec.target, spec.leg);
    // Progress lands in the row and the log as it happens; see `progress`.
    let split_done = Arc::new(AtomicBool::new(false));
    let (events, received) = tokio::sync::mpsc::unbounded_channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    let recorder = tokio::spawn(progress::record(
        actor.db.clone(),
        progress::LegLog {
            publication: publication.id.clone(),
            target: spec.target.clone(),
            leg: spec.leg.to_string(),
            path: log_path.clone(),
            split: spec.then.map(|next_leg| progress::PhaseSplit {
                next_leg,
                log_ref: log_ref.clone(),
                done: split_done.clone(),
            }),
        },
        actor.policy.clone(),
        {
            let name_of = actor.name_of.clone();
            move |executor: &str| name_of(executor)
        },
        received,
        done,
    ));
    let board = crate::execution::checks::CheckStatusBoard::observed(
        &name,
        Arc::new(move |event| {
            let _ = events.send(event);
        }),
    );
    let owner_key = format!("publication:{}", publication.id);
    let outcome = actor
        .placer
        .place(PlannedCheckBatchRequest {
            project_id: context.project_id.clone(),
            repository: context.repository.clone(),
            store_dir: context.store_dir.clone(),
            sealed_commit: commit.to_string(),
            requesting_job_id: owner_key.clone(),
            owner: CellOwnerRef {
                project_id: context.project_id.clone(),
                project_key: Some(context.project_key.clone()),
                issue_number: None,
                job_id: None,
                execution_seq: None,
                node_kind: Some(name.clone()),
            },
            affinity_key: None,
            priority: CellPriority::ReviewCheck,
            env: Vec::new(),
            items: vec![PlannedCheckBatchItem {
                recording_plan: None,
                index: 0,
                name: name.clone(),
                input_hash: format!("{}:{}:{}:{commit}", publication.id, spec.target, spec.leg),
                resource_identity: CheckResourceIdentityInput::Checkpoint {
                    name: format!("publish {}", spec.leg),
                    command: spec.command.clone(),
                },
                command: spec.command.clone(),
                stream_id: format!(
                    "publication:{}:{}:{}",
                    publication.id, spec.target, spec.leg
                ),
                env: spec.env,
                verdict_environment_names: Vec::new(),
                timeout_ms: spec.timeout_ms,
                executor: Some(ExecutorSelector {
                    os: Some(spec.os.into()),
                    required_toolchains: spec.required_toolchains,
                    ..ExecutorSelector::default()
                }),
                verdict_platforms: vec![spec.os.into()],
                resource_class: CheckResourceClass::Shared,
                infra_scopes: InfraScopes::head_only(&owner_key, commit),
                avoid_executor_ids: spec.avoid,
            }],
            cell_groups: Default::default(),
            run_context: None,
            mutation_policy: MutationPolicy::PureVerdict,
            status_board: Some(board),
            on_group_complete: None,
            purpose: None,
            publication: spec.custody,
        })
        .await;
    let _ = finished.send(());
    let progress_lines = recorder.await.unwrap_or_default();
    let request_id = outcome
        .as_ref()
        .ok()
        .and_then(|outcome| outcome.request.as_ref())
        .map(|request| request.request_id.clone());
    let result = match outcome {
        Ok(mut outcome) => outcome.results.remove(&0).unwrap_or_else(|| {
            Err(CheckExecutionFailure::Process(
                "placement returned no result for the leg".into(),
            ))
        }),
        Err(error) => Err(CheckExecutionFailure::Process(format!(
            "submission failed: {error}"
        ))),
    };
    let marker = if spec.leg == "plan" {
        plan::PLAN_RESULT_MARKER
    } else {
        plan::LEG_RESULT_MARKER
    };
    let (documents, builder, output) = match &result {
        Ok(ran) => (
            plan::marked_documents(&ran.output, marker),
            builder_name(actor, ran),
            ran.output.clone(),
        ),
        Err(_) => (Vec::new(), None, String::new()),
    };
    let (build, verdict) = match spec.then {
        Some(_) => {
            let (build, publish) = split_verdicts(
                &result,
                &spec.target,
                &documents,
                split_done.load(Ordering::SeqCst),
            );
            (Some(build), publish)
        }
        None => (
            None,
            match &result {
                Ok(ran) => plan::completed_verdict(ran, &documents),
                Err(failure) => plan::failure_verdict(&spec.target, failure),
            },
        ),
    };
    let output = if output.is_empty() {
        verdict.reason().unwrap_or_default().to_string()
    } else {
        output
    };
    // The complete record replaces what streamed: Cairn's own lines, then the
    // whole output, including any tail withheld until the batch settled.
    let mut text = progress_lines.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    let mut body = output;
    crate::security::Sanitizer::structural(&actor.policy).text_in_place(&mut body);
    text.push_str(&body);
    if !text.ends_with('\n') {
        text.push('\n');
    }
    if let Some(build) = &build {
        text.push_str(&ending_line("build", build));
    }
    text.push_str(&ending_line(spec.then.unwrap_or(spec.leg), &verdict));
    if let Err(error) = tokio::fs::write(&log_path, text).await {
        log::warn!(
            "publication {}: could not write the {} {} log at {}: {error}",
            publication.id,
            spec.target,
            spec.leg,
            log_path.display()
        );
    }
    let ended_at = now_ms();
    match (spec.then, &build) {
        (Some(next_leg), Some(build)) => {
            let build_documents = documents_of(&documents, &["build", "manifest"]);
            let publish_documents = documents_of(&documents, &[next_leg]);
            if !split_done.load(Ordering::SeqCst) {
                ledger::update_leg(
                    &actor.db,
                    &publication.id,
                    &spec.target,
                    spec.leg,
                    LegUpdate {
                        state: Some(build.state().into()),
                        reason: build.reason().map(str::to_string),
                        builder: builder.clone(),
                        ended_at: Some(ended_at),
                        assets: assets_of(&documents_of(&documents, &["build"])),
                        verifier: last_field(&documents_of(&documents, &["manifest"]), "verifier"),
                        build_jobs: last_field(&build_documents, "buildJobs"),
                        ..LegUpdate::default()
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            let succeeded = verdict == LegVerdict::Succeeded;
            ledger::update_leg(
                &actor.db,
                &publication.id,
                &spec.target,
                next_leg,
                LegUpdate {
                    state: Some(verdict.state().into()),
                    reason: verdict.reason().map(str::to_string),
                    builder: builder.filter(|_| *build == LegVerdict::Succeeded),
                    // The stream opened it at the manifest; otherwise it opens
                    // and closes here.
                    started_at: (*build == LegVerdict::Succeeded
                        && !split_done.load(Ordering::SeqCst))
                    .then_some(ended_at),
                    ended_at: Some(ended_at),
                    assets: assets_of(&publish_documents).filter(|_| succeeded),
                    artifact_location: last_field(&publish_documents, "artifactLocation")
                        .and_then(|value| value.as_str().map(str::to_string))
                        .filter(|_| succeeded),
                    log_ref: Some(log_ref),
                    request_id: request_id.filter(|_| *build == LegVerdict::Succeeded),
                    ..LegUpdate::default()
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        _ => {
            let verifier = last_field(&documents, "verifier").filter(|_| spec.leg != "plan");
            ledger::update_leg(
                &actor.db,
                &publication.id,
                &spec.target,
                spec.leg,
                LegUpdate {
                    state: Some(verdict.state().into()),
                    reason: verdict.reason().map(str::to_string),
                    builder,
                    ended_at: Some(ended_at),
                    assets: assets_of(&documents),
                    verifier,
                    build_jobs: last_field(&documents, "buildJobs"),
                    ..LegUpdate::default()
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        }
    }
    Ok((LegEnding { verdict, build }, documents))
}

fn ending_line(leg: &str, verdict: &LegVerdict) -> String {
    format!(
        "{}{leg} ended {}{}\n",
        progress::CAIRN_LINE,
        verdict.state(),
        verdict
            .reason()
            .map(|reason| format!(": {reason}"))
            .unwrap_or_default()
    )
}

fn builder_name(actor: &Actor, ran: &CheckExecResult) -> Option<String> {
    ran.provenance
        .as_ref()
        .map(|meta| meta.executor_id.as_str())
        .filter(|id| !id.is_empty())
        .map(|id| (actor.name_of)(id))
}

async fn publication_project_key(orch: &Orchestrator, publication: &Publication) -> String {
    let id = publication.project_id.clone();
    match crate::execution::routing::owning_db_for_project(&orch.db, &id).await {
        Ok(db) => db
            .query_opt(
                "SELECT key FROM projects WHERE id = ?1",
                (id.clone(),),
                |row| row.text(0),
            )
            .await
            .ok()
            .flatten()
            .unwrap_or(id),
        Err(_) => id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_start_is_validated_before_anything_is_written() {
        let ok = StartRequest {
            trigger: "manual".into(),
            ..StartRequest::default()
        };
        assert!(validate(&ok).is_ok());
        let cases = [
            (
                StartRequest {
                    kind: Some("docker".into()),
                    ..ok.clone()
                },
                "kind",
            ),
            (
                StartRequest {
                    commit: Some("abc".into()),
                    ..ok.clone()
                },
                "commit",
            ),
            (
                StartRequest {
                    force: vec!["mips".into()],
                    ..ok.clone()
                },
                "force",
            ),
            (
                StartRequest {
                    tool: Some("codex".into()),
                    ..ok.clone()
                },
                "tool",
            ),
            (
                StartRequest {
                    kind: Some("toolPin".into()),
                    ..ok.clone()
                },
                "tool",
            ),
            (
                StartRequest {
                    kind: Some("toolPin".into()),
                    tool: Some("rm".into()),
                    ..ok.clone()
                },
                "tool",
            ),
        ];
        for (request, key) in cases {
            let error = validate(&request).unwrap_err();
            assert!(error.contains(key), "{error}");
        }
        assert!(validate(&StartRequest {
            kind: Some("toolPin".into()),
            tool: Some("codex".into()),
            ..ok
        })
        .is_ok());
    }

    mod actor {
        use super::super::*;
        use cairn_common::executor_protocol::{
            CellCommandClass, PublishIsolationEvidence, PublishIsolationVerdict,
        };
        use serde_json::json;
        use std::sync::Mutex;

        const LINUX: &str = "x86_64-unknown-linux-gnu";
        const MAC: &str = "aarch64-apple-darwin";

        /// What one placed batch asked for.
        #[derive(Debug, Clone)]
        struct Placed {
            name: String,
            command: String,
            custody: Option<PublicationCustody>,
        }

        type Script = Box<dyn Fn(&str) -> (i32, String) + Send + Sync>;

        /// Answers each leg by its name (`publish <target> <leg>`) from a
        /// script, and records the order and custody it was placed with.
        struct FakePlacer {
            script: Script,
            placed: Mutex<Vec<Placed>>,
        }

        impl LegPlacer for FakePlacer {
            fn place(
                &self,
                batch: PlannedCheckBatchRequest,
            ) -> futures_util::future::BoxFuture<'_, Result<PlannedCheckBatchOutcome, String>>
            {
                let item = &batch.items[0];
                self.placed.lock().unwrap().push(Placed {
                    name: item.name.clone(),
                    command: item.command.clone(),
                    custody: batch.publication,
                });
                let (exit, output) = (self.script)(&item.name);
                Box::pin(async move {
                    Ok(PlannedCheckBatchOutcome {
                        results: std::collections::HashMap::from([(
                            0,
                            Ok(CheckExecResult {
                                exit_code: Some(exit),
                                output,
                                timed_out: false,
                                duration_ms: None,
                                provenance: None,
                                publication: None,
                            }),
                        )]),
                        request: None,
                        delta: None,
                        store_dir: None,
                    })
                })
            }
        }

        fn machine(name: &str, os: &str, role: crate::fleet::MachineRole) -> KnownMachinePlatform {
            KnownMachinePlatform {
                executor_id: name.into(),
                name: name.into(),
                os: os.into(),
                arch: if os == "macos" { "aarch64" } else { "x86_64" }.into(),
                toolchains: Some(vec!["rust".into(), "bun".into()]),
                role,
                publish_isolation: (role == crate::fleet::MachineRole::PublishOnly).then(|| {
                    PublishIsolationEvidence {
                        verdict: PublishIsolationVerdict::Pass,
                        measured_at_unix_ms: now_unix_ms(),
                        lines: Vec::new(),
                    }
                }),
            }
        }

        fn result(document: Value) -> String {
            format!("{}{document}\n", plan::LEG_RESULT_MARKER)
        }

        const COMBINED_SHA: &str =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        /// A combine leg's result: the combined manifest it uploaded.
        fn combined_output() -> String {
            result(json!({"leg": "combine", "target": "all", "assets": [
                {"name": "executor-manifest-v171.json", "sha256": COMBINED_SHA, "size": 9}
            ]}))
        }

        fn asset() -> Value {
            json!([{"name": "cairn-exec-v171-x", "sha256": "a".repeat(64), "size": 3}])
        }

        fn plan_output(targets: &[(&str, bool)], combined_live: bool) -> String {
            let plans: Vec<Value> = targets
                .iter()
                .map(|(target, publish)| {
                    json!({"target": target, "publish": publish, "deferred": false, "reason": "changed"})
                })
                .collect();
            format!(
                "{}{}\n",
                plan::PLAN_RESULT_MARKER,
                json!({"protocolVersion": 171, "plans": plans, "combinedLive": combined_live})
            )
        }

        /// A build-and-publish batch's output: build, manifest, then publish
        /// (succeeding or not).
        fn publisher_output(publish_ok: bool) -> (i32, String) {
            let mut out = result(json!({"leg": "build", "target": LINUX, "assets": asset()}));
            out.push_str(&result(json!({"leg": "manifest", "target": LINUX, "verifier": {"check": "manifest-shape", "result": "pass"}})));
            if publish_ok {
                out.push_str(&result(json!({"leg": "publish", "target": LINUX, "assets": asset(), "artifactLocation": "https://github.com/bleugreenlab/cairn/releases/tag/executor-protocol-v171"})));
                (0, out)
            } else {
                out.push_str(&result(
                    json!({"leg": "publish", "target": LINUX, "error": "upload refused: 403"}),
                ));
                (1, out)
            }
        }

        struct Harness {
            _dir: tempfile::TempDir,
            db: Arc<LocalDb>,
            actor: Actor,
            placer: Arc<FakePlacer>,
            context: ProjectContext,
        }

        async fn harness(machines: Vec<KnownMachinePlatform>, script: Script) -> Harness {
            let dir = tempfile::tempdir().unwrap();
            let db = Arc::new(
                LocalDb::open_migrated_for_test(dir.path().join("ledger.db"))
                    .await
                    .unwrap(),
            );
            let placer = Arc::new(FakePlacer {
                script,
                placed: Mutex::new(Vec::new()),
            });
            let actor = Actor {
                db: db.clone(),
                config_dir: dir.path().to_path_buf(),
                policy: crate::security::redaction_policy(dir.path()),
                placer: placer.clone(),
                name_of: Arc::new(|id: &str| id.to_string()),
                machines,
                project_key: "cairn".into(),
            };
            let context = ProjectContext {
                project_id: "p".into(),
                project_key: "cairn".into(),
                repository: dir.path().display().to_string(),
                store_dir: dir.path().to_path_buf(),
                default_branch: "main".into(),
            };
            Harness {
                _dir: dir,
                db,
                actor,
                placer,
                context,
            }
        }

        impl Harness {
            async fn run(
                &self,
                trigger: &str,
                live_targets: &[&str],
            ) -> (String, Vec<PublicationLeg>) {
                ledger::start(
                    &self.db,
                    NewPublication {
                        id: "pub-1".into(),
                        project_id: "p".into(),
                        kind: "executor".into(),
                        commit: Some("c".repeat(40)),
                        trigger: trigger.into(),
                        requested_by: None,
                        force_targets: vec![],
                        tool: None,
                        mode: if live_targets.is_empty() {
                            "shadow"
                        } else {
                            "live"
                        }
                        .into(),
                        live_targets: live_targets.iter().map(|t| t.to_string()).collect(),
                        created_at: 1,
                    },
                    "",
                )
                .await
                .unwrap();
                let publication = ledger::claim_next(&self.db, "p").await.unwrap().unwrap();
                let summary =
                    run_executor(&self.actor, &self.context, &publication, &"c".repeat(40))
                        .await
                        .unwrap();
                (summary, ledger::legs(&self.db, "pub-1").await.unwrap())
            }

            fn placed(&self) -> Vec<Placed> {
                self.placer.placed.lock().unwrap().clone()
            }
        }

        fn row<'a>(legs: &'a [PublicationLeg], target: &str, leg: &str) -> &'a PublicationLeg {
            legs.iter()
                .find(|row| row.target == target && row.leg == leg)
                .unwrap_or_else(|| panic!("no {target} {leg} row in {legs:#?}"))
        }

        fn states(legs: &[PublicationLeg]) -> Vec<(String, String, String, String)> {
            legs.iter()
                .map(|row| {
                    (
                        row.target.clone(),
                        row.leg.clone(),
                        row.mode.clone(),
                        row.state.clone(),
                    )
                })
                .collect()
        }

        fn fleet() -> Vec<KnownMachinePlatform> {
            use crate::fleet::MachineRole;
            vec![
                machine("fedora", "linux", MachineRole::General),
                machine("cairn-publish", "linux", MachineRole::PublishOnly),
                machine("mac", "macos", MachineRole::General),
            ]
        }

        /// A live target builds and publishes in one publisher batch carrying
        /// the release secret's custody, verifies what it published, and the
        /// publication ends with combine and the combined verify, in order. A
        /// shadow target beside it keeps Phase A's shape and uploads nothing.
        #[tokio::test]
        async fn a_live_target_publishes_then_verifies_then_combines_in_order() {
            let h = harness(
                fleet(),
                Box::new(|name: &str| match name {
                    "publish all plan" => (0, plan_output(&[(LINUX, true), (MAC, true)], true)),
                    "publish x86_64-unknown-linux-gnu build" => publisher_output(true),
                    "publish aarch64-apple-darwin build" => (
                        0,
                        result(json!({"leg": "manifest", "target": MAC, "assets": asset()})),
                    ),
                    "publish all combine" => (0, combined_output()),
                    _ => (0, result(json!({"leg": "verify"}))),
                }),
            )
            .await;
            let (summary, legs) = h.run("manual", &[LINUX]).await;
            assert!(
                summary.starts_with("live (x86_64-unknown-linux-gnu)"),
                "{summary}"
            );
            let mut got = states(&legs);
            got.sort();
            let ok = |target: &str, leg: &str, mode: &str| {
                (target.into(), leg.into(), mode.into(), "succeeded".into())
            };
            let mut expected = vec![
                ok("all", "plan", "shadow"),
                ok(LINUX, "build", "live"),
                ok(LINUX, "publish", "live"),
                ok(LINUX, "verify", "live"),
                ok(MAC, "build", "shadow"),
                ok(MAC, "verify", "shadow"),
                ok("all", "combine", "live"),
                ok("all", "verify", "live"),
            ];
            expected.sort();
            assert_eq!(got, expected);
            let publish = row(&legs, LINUX, "publish");
            assert_eq!(
                publish.artifact_location.as_deref(),
                Some("https://github.com/bleugreenlab/cairn/releases/tag/executor-protocol-v171")
            );
            assert_eq!(publish.assets.len(), 1);
            assert_eq!(row(&legs, LINUX, "build").assets.len(), 1);
            assert!(row(&legs, MAC, "build").artifact_location.is_none());
            let placed = h.placed();
            let order: Vec<&str> = placed
                .iter()
                .map(|placed| placed.name.as_str())
                .filter(|name| !name.contains(MAC))
                .collect();
            assert_eq!(
                order,
                [
                    "publish all plan",
                    "publish x86_64-unknown-linux-gnu build",
                    "publish x86_64-unknown-linux-gnu verify",
                    "publish all combine",
                    "publish all verify",
                ]
            );
            let custody = |name: &str| {
                placed
                    .iter()
                    .find(|placed| placed.name == name)
                    .unwrap()
                    .custody
                    .map(|custody| custody.publisher_required)
            };
            assert_eq!(
                custody("publish x86_64-unknown-linux-gnu build"),
                Some(true)
            );
            assert_eq!(custody("publish all combine"), Some(true));
            assert_eq!(custody("publish aarch64-apple-darwin build"), Some(false));
            assert_eq!(
                custody("publish x86_64-unknown-linux-gnu verify"),
                Some(false)
            );
            let live_build = placed
                .iter()
                .find(|placed| placed.name == "publish x86_64-unknown-linux-gnu build")
                .unwrap();
            assert_eq!(
                CellCommandClass::classify(&live_build.command),
                CellCommandClass::ReleaseBuild
            );
            assert!(live_build.command.contains("publish-leg.ts publish"));
            let combined_verify = placed
                .iter()
                .find(|placed| placed.name == "publish all verify")
                .unwrap();
            assert!(
                combined_verify
                    .command
                    .ends_with(&format!("--after-publish --expect-sha256 {}", COMBINED_SHA)),
                "the combined verify waits for the bytes its combine uploaded: {}",
                combined_verify.command
            );
        }

        /// A publish that fails leaves its build succeeded and defers its
        /// verify; with nothing published and the combined manifest current,
        /// no combine is laid out.
        #[tokio::test]
        async fn a_failed_publish_defers_its_verify() {
            let h = harness(
                fleet(),
                Box::new(|name: &str| match name {
                    "publish all plan" => (0, plan_output(&[(LINUX, true)], true)),
                    _ => publisher_output(false),
                }),
            )
            .await;
            let (_, legs) = h.run("manual", &[LINUX]).await;
            assert_eq!(row(&legs, LINUX, "build").state, "succeeded");
            let publish = row(&legs, LINUX, "publish");
            assert_eq!(publish.state, "failed");
            assert!(publish
                .reason
                .as_deref()
                .unwrap()
                .contains("upload refused: 403"));
            assert!(publish.artifact_location.is_none());
            let verify = row(&legs, LINUX, "verify");
            assert_eq!(
                (verify.state.as_str(), verify.reason.as_deref()),
                ("deferred", Some(PUBLISH_DID_NOT_SUCCEED))
            );
            assert!(legs.iter().all(|row| row.leg != "combine"));
            assert_eq!(h.placed().len(), 2, "plan and the publisher batch only");
        }

        /// A fleet with no measured publisher defers every live row in
        /// placement's words and queues nothing; a merge lays out no shadow
        /// rows; a stale combined manifest still asks for combine, which
        /// defers for the same reason.
        #[tokio::test]
        async fn without_a_publisher_live_rows_defer_with_nothing_queued() {
            use crate::fleet::MachineRole;
            let h = harness(
                vec![
                    machine("fedora", "linux", MachineRole::General),
                    machine("mac", "macos", MachineRole::General),
                ],
                Box::new(|name: &str| match name {
                    "publish all plan" => (0, plan_output(&[(LINUX, true), (MAC, true)], false)),
                    other => panic!("{other} must not be placed"),
                }),
            )
            .await;
            let (_, legs) = h.run("merge", &[LINUX]).await;
            assert!(
                legs.iter().all(|row| row.target != MAC),
                "a merge lays out no shadow rows"
            );
            assert!(legs.iter().all(PublicationLeg::is_terminal), "{legs:#?}");
            for leg in ["build", "publish"] {
                let row = row(&legs, LINUX, leg);
                assert_eq!(row.state, "deferred", "{leg}");
                let reason = row.reason.as_deref().unwrap();
                assert!(
                    reason.contains("fedora is not a publisher executor (role general)")
                        && reason.contains("release secrets go only to publishOnly executors"),
                    "{reason}"
                );
            }
            assert_eq!(row(&legs, LINUX, "verify").state, "deferred");
            assert_eq!(row(&legs, "all", "combine").state, "deferred");
            assert_eq!(row(&legs, "all", "verify").state, "deferred");
            assert_eq!(h.placed().len(), 1, "only the plan was placed");
        }

        /// The heartbeat closes a combined manifest a run left stale even
        /// when no target has anything to publish.
        #[tokio::test]
        async fn a_stale_combined_manifest_is_combined_with_no_target_to_publish() {
            let h = harness(
                fleet(),
                Box::new(|name: &str| match name {
                    "publish all plan" => (0, plan_output(&[(LINUX, false)], false)),
                    "publish all combine" => (0, combined_output()),
                    _ => (0, result(json!({"leg": "verify"}))),
                }),
            )
            .await;
            let (_, legs) = h.run("heartbeat", &[LINUX]).await;
            let mut got = states(&legs);
            got.sort();
            assert_eq!(
                got.iter()
                    .map(|(t, l, _, s)| format!("{t} {l} {s}"))
                    .collect::<Vec<_>>(),
                [
                    "all combine succeeded",
                    "all plan succeeded",
                    "all verify succeeded"
                ]
            );
        }

        /// With no live targets nothing here changes: no publish rows, no
        /// combine, whatever the combined manifest reads.
        #[tokio::test]
        async fn with_no_live_targets_the_publication_is_phase_a_shadow() {
            let h = harness(
                fleet(),
                Box::new(|name: &str| match name {
                    "publish all plan" => (0, plan_output(&[(LINUX, true)], false)),
                    _ => (0, result(json!({"leg": "manifest"}))),
                }),
            )
            .await;
            let (summary, legs) = h.run("manual", &[]).await;
            assert!(summary.starts_with("shadow publication"), "{summary}");
            assert!(legs
                .iter()
                .all(|row| row.mode == "shadow" && row.leg != "publish" && row.leg != "combine"));
        }

        /// The trusted-source boundary: an agent may name a commit, but only
        /// one already on the default branch; an unmerged one, or one whose
        /// ancestry cannot be read, is refused before anything is recorded,
        /// so no leg (live or shadow, which may land on a publisher too) ever
        /// runs it.
        #[test]
        fn only_a_commit_on_the_default_branch_is_ever_published() {
            let sha = "d".repeat(40);
            assert_eq!(require_on_default_branch(&sha, "main", Ok(true)), Ok(()));
            let unmerged = require_on_default_branch(&sha, "main", Ok(false)).unwrap_err();
            assert!(
                unmerged.contains("only commits on main") && unmerged.contains("Merge it first"),
                "{unmerged}"
            );
            let unknown =
                require_on_default_branch(&sha, "main", Err("unknown commit".into())).unwrap_err();
            assert!(
                unknown.contains("could not be read: unknown commit"),
                "{unknown}"
            );
        }

        #[test]
        fn a_merge_starts_a_publication_only_when_its_commit_lists_live_targets() {
            assert!(!merge_starts_a_publication(&[]));
            assert!(merge_starts_a_publication(&[LINUX.to_string()]));
        }
    }

    #[test]
    fn leg_logs_live_under_the_publication() {
        let path = leg_log_path(
            std::path::Path::new("/home/c/.cairn"),
            "pub-1",
            "t",
            "build",
        );
        assert_eq!(
            path,
            std::path::Path::new("/home/c/.cairn/publications/pub-1/t.build.log")
        );
    }
}
