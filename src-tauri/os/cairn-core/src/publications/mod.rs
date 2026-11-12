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
//! Phase A publishes nothing: every publication is a shadow, whose build legs
//! produce evidence (asset hashes and a manifest-shape comparison against the
//! live per-target manifest) while GitHub Actions remains host, uploader and
//! signer. Live targets, the publish and combine legs, and the publisher
//! executor role arrive with Phase B. Shadow runs start from the action and
//! the heartbeat only, never from a merge, so they add no release-compile load
//! on the hosts GitHub Actions' own builds need.
pub mod plan;
mod progress;

use std::time::Duration;

use cairn_common::executor_protocol::{CellOwnerRef, ExecutorSelector};
use cairn_db::storage::publications::{self as ledger, LegAsset, LegUpdate, NewPublication};
use serde_json::Value;

use crate::config::project_settings::CheckResourceClass;
use crate::execution::cache::InfraScopes;
use crate::execution::checks::{
    submit_planned_check_batch, CheckExecutionFailure, PlannedCheckBatchItem,
    PlannedCheckBatchRequest,
};
use crate::execution::wire::CheckResourceIdentityInput;
use crate::fleet::{CellPriority, MutationPolicy};
use crate::mcp::handlers::run::CheckExecResult;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use plan::{LegVerdict, TargetSpec, ALL_TARGETS};

pub use cairn_db::storage::publications::{Publication, PublicationLeg, Started, TargetState};

/// How often the heartbeat starts a shadow publication, matching the
/// workflow's cron.
const HEARTBEAT: Duration = Duration::from_secs(12 * 60 * 60);
/// A build leg compiles release binaries; past this it is not progressing.
const BUILD_TIMEOUT_MS: u64 = 4 * 60 * 60 * 1000;
/// Every other leg reads, plans or verifies.
const SHORT_TIMEOUT_MS: u64 = 30 * 60 * 1000;
/// The script whose presence on a project's checkout marks it as one that
/// publishes an executor (only Cairn's own repository).
const LEG_SCRIPT: &str = "scripts/publish-leg.ts";
/// Every publication in this phase builds evidence only.
const MODE: &str = "shadow";
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
    /// `manual` or `heartbeat`.
    pub trigger: String,
}

fn now_ms() -> i64 {
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
pub async fn start(
    orch: &Orchestrator,
    project_id: &str,
    request: StartRequest,
) -> Result<Started, String> {
    validate(&request)?;
    let id = new_id();
    let started = ledger::start(
        &orch.db.local,
        NewPublication {
            id: id.clone(),
            project_id: project_id.to_string(),
            kind: request.kind.clone().unwrap_or_else(|| "executor".into()),
            commit: request.commit.clone(),
            trigger: request.trigger.clone(),
            requested_by: request.requested_by.clone(),
            force_targets: request.force.clone(),
            tool: request.tool.clone(),
            mode: MODE.into(),
            created_at: now_ms(),
        },
        &format!("a newer {} start", request.trigger),
    )
    .await
    .map_err(|error| format!("could not record the publication: {error}"))?;
    spawn_drive(orch, project_id);
    Ok(started)
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

/// Start a shadow publication for every project that publishes an executor,
/// every [`HEARTBEAT`]. The first tick waits a full interval.
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
}

/// What one placed leg came back with.
struct LegEnding {
    verdict: LegVerdict,
    documents: Vec<Value>,
}

async fn run_publication(orch: &Orchestrator, publication: &Publication) -> Result<(), String> {
    let context = project_context(orch, &publication.project_id).await?;
    let commit = match &publication.commit {
        Some(commit) => commit.clone(),
        None => cairn_vcs::resolve_coordinate(&context.store_dir, &context.default_branch)
            .await
            .map_err(|error| format!("{} is unresolvable: {error}", context.default_branch))?,
    };
    ledger::record_plan(&orch.db.local, &publication.id, Some(&commit), None, None)
        .await
        .map_err(|error| error.to_string())?;
    let summary = if publication.kind == "toolPin" {
        run_tool_pin(orch, &context, publication, &commit).await?
    } else {
        run_executor(orch, &context, publication, &commit).await?
    };
    ledger::settle(&orch.db.local, &publication.id, &summary, now_ms())
        .await
        .map_err(|error| error.to_string())
}

async fn run_tool_pin(
    orch: &Orchestrator,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
) -> Result<String, String> {
    let tool = publication
        .tool
        .clone()
        .ok_or("a toolPin publication names no tool")?;
    add_leg(orch, publication, &tool, "verify", 0).await?;
    let spec = TargetSpec {
        triple: "x86_64-unknown-linux-gnu",
        os: "linux",
        builder_arch: "x86_64",
    };
    let builders = plan::builders_for(spec, &orch.fleet.known_machine_platforms());
    if builders.eligible.is_empty() {
        settle_leg(
            orch,
            publication,
            &tool,
            "verify",
            &plan_deferral(&tool, spec, &builders),
        )
        .await?;
        return Ok(format!("{tool} pin verify deferred: no eligible builder"));
    }
    let ending = place_leg(
        orch,
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
        },
    )
    .await?;
    Ok(format!("{tool} pin verify {}", ending.verdict.state()))
}

fn plan_deferral(target: &str, spec: TargetSpec, builders: &plan::Builders) -> LegVerdict {
    let reason = plan::no_builder_reason(spec, builders);
    LegVerdict::Deferred(reason.replacen(spec.triple, target, 1))
}

async fn run_executor(
    orch: &Orchestrator,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
) -> Result<String, String> {
    add_leg(orch, publication, ALL_TARGETS, "plan", 0).await?;
    let machines = orch.fleet.known_machine_platforms();
    let plan_ending = place_leg(
        orch,
        context,
        publication,
        commit,
        LegSpec {
            target: ALL_TARGETS.into(),
            leg: "plan",
            command: plan::plan_command(
                &publication.id,
                &plan::eligible_targets_csv(&machines),
                &publication.force_targets,
            ),
            env: Vec::new(),
            os: "linux",
            required_toolchains: Vec::new(),
            avoid: Vec::new(),
            timeout_ms: SHORT_TIMEOUT_MS,
        },
    )
    .await?;
    if plan_ending.verdict != LegVerdict::Succeeded {
        return Ok(format!(
            "plan {}: {}",
            plan_ending.verdict.state(),
            plan_ending.verdict.reason().unwrap_or_default()
        ));
    }
    let Some(plan_document) = plan_ending.documents.last().cloned() else {
        return Ok("plan succeeded but printed no plan document".into());
    };
    ledger::record_plan(
        &orch.db.local,
        &publication.id,
        None,
        plan_document["protocolVersion"].as_i64(),
        Some(&plan_document),
    )
    .await
    .map_err(|error| error.to_string())?;
    let targets = plan::planned_targets(&plan_document);
    if targets.is_empty() {
        return Ok("the live release already serves every target at this commit".into());
    }
    for (index, target) in targets.iter().enumerate() {
        let seq = 1 + index as i64 * 2;
        add_leg(orch, publication, &target.triple, "build", seq).await?;
        add_leg(orch, publication, &target.triple, "verify", seq + 1).await?;
    }
    let plan_text = plan_document.to_string();
    let runs = targets.iter().map(|target| {
        run_target(
            orch,
            context,
            publication,
            commit,
            &machines,
            target,
            &plan_text,
        )
    });
    let endings = futures_util::future::join_all(runs).await;
    let mut counts = std::collections::BTreeMap::<&'static str, usize>::new();
    for ending in endings {
        for state in ending? {
            *counts.entry(state).or_default() += 1;
        }
    }
    Ok(format!(
        "shadow publication of {} target(s) at protocol v{}: {}",
        targets.len(),
        plan_document["protocolVersion"],
        counts
            .iter()
            .map(|(state, count)| format!("{count} {state}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// One target's legs, build then verify. Returns the state each leg ended in.
async fn run_target(
    orch: &Orchestrator,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
    machines: &[crate::fleet::KnownMachinePlatform],
    target: &plan::PlannedTarget,
    plan_text: &str,
) -> Result<Vec<&'static str>, String> {
    let triple = target.triple.as_str();
    let Some(spec) = plan::target_spec(triple) else {
        let refused = LegVerdict::Refused(format!(
            "the plan names a target this runner does not know: {triple}"
        ));
        settle_leg(orch, publication, triple, "build", &refused).await?;
        settle_leg(orch, publication, triple, "verify", &refused).await?;
        return Ok(vec!["refused", "refused"]);
    };
    let builders = plan::builders_for(spec, machines);
    let unplaced = if spec.os == "windows" {
        Some(LegVerdict::Refused(plan::WINDOWS_SIGNING_REFUSAL.into()))
    } else if target.deferred || builders.eligible.is_empty() {
        // The plan was told which targets have a builder, so its deferral and
        // this one agree; the fleet's own account is the reason recorded.
        Some(LegVerdict::Deferred(plan::no_builder_reason(
            spec, &builders,
        )))
    } else {
        None
    };
    let build = if let Some(verdict) = unplaced {
        settle_leg(orch, publication, triple, "build", &verdict).await?;
        verdict
    } else {
        place_leg(
            orch,
            context,
            publication,
            commit,
            LegSpec {
                target: triple.into(),
                leg: "build",
                command: plan::build_command(&publication.id, triple, commit),
                env: vec![(plan::PLAN_ENV.into(), plan_text.into())],
                os: spec.os,
                required_toolchains: vec!["rust".into()],
                avoid: builders.avoid.clone(),
                timeout_ms: BUILD_TIMEOUT_MS,
            },
        )
        .await?
        .verdict
    };
    // Verify reads the live release anonymously and waits on the fact that it
    // names this commit, so it runs whether or not this shadow build did.
    let verify = place_leg(
        orch,
        context,
        publication,
        commit,
        LegSpec {
            target: triple.into(),
            leg: "verify",
            command: plan::verify_command(&publication.id, triple, commit),
            env: vec![(plan::PLAN_ENV.into(), plan_text.into())],
            os: "linux",
            required_toolchains: Vec::new(),
            avoid: Vec::new(),
            timeout_ms: SHORT_TIMEOUT_MS,
        },
    )
    .await?
    .verdict;
    Ok(vec![build.state(), verify.state()])
}

async fn add_leg(
    orch: &Orchestrator,
    publication: &Publication,
    target: &str,
    leg: &str,
    seq: i64,
) -> Result<(), String> {
    ledger::add_leg(&orch.db.local, &publication.id, target, leg, seq)
        .await
        .map_err(|error| error.to_string())
}

async fn settle_leg(
    orch: &Orchestrator,
    publication: &Publication,
    target: &str,
    leg: &str,
    verdict: &LegVerdict,
) -> Result<(), String> {
    let now = now_ms();
    ledger::update_leg(
        &orch.db.local,
        &publication.id,
        target,
        leg,
        LegUpdate {
            state: Some(verdict.state().into()),
            reason: verdict.reason().map(str::to_string),
            ended_at: Some(now),
            ..LegUpdate::default()
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// The assets a leg's result documents record, from the last one naming any.
fn assets_of(documents: &[Value]) -> Option<Vec<LegAsset>> {
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

/// Place one leg as a one-item check batch owned by no job, record what it
/// came back with, and write its log.
async fn place_leg(
    orch: &Orchestrator,
    context: &ProjectContext,
    publication: &Publication,
    commit: &str,
    spec: LegSpec,
) -> Result<LegEnding, String> {
    let started_at = now_ms();
    let log_path = leg_log_path(&orch.config_dir, &publication.id, &spec.target, spec.leg);
    let log_ref = cairn_uri::build_project_publication_leg_log_uri(
        &publication_project_key(orch, publication).await,
        &publication.id,
        &spec.target,
        spec.leg,
    );
    ledger::update_leg(
        &orch.db.local,
        &publication.id,
        &spec.target,
        spec.leg,
        LegUpdate {
            state: Some("running".into()),
            started_at: Some(started_at),
            log_ref: Some(log_ref),
            ..LegUpdate::default()
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let name = format!("publish {} {}", spec.target, spec.leg);
    // Progress lands in the row and the log as it happens; see `progress`.
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let (events, received) = tokio::sync::mpsc::unbounded_channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    let recorder = tokio::spawn(progress::record(
        orch.db.local.clone(),
        progress::LegLog {
            publication: publication.id.clone(),
            target: spec.target.clone(),
            leg: spec.leg.to_string(),
            path: log_path.clone(),
        },
        policy.clone(),
        {
            let fleet = orch.clone();
            move |executor: &str| fleet.fleet.executor_name_for_text(executor)
        },
        received,
        done,
    ));
    let board = crate::execution::checks::CheckStatusBoard::observed(
        &name,
        std::sync::Arc::new(move |event| {
            let _ = events.send(event);
        }),
    );
    let owner_key = format!("publication:{}", publication.id);
    let outcome = submit_planned_check_batch(
        orch,
        PlannedCheckBatchRequest {
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
        },
    )
    .await;
    let _ = finished.send(());
    let progress_lines = recorder.await.unwrap_or_default();
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
    let (verdict, documents, builder, output) = match &result {
        Ok(ran) => {
            let documents = plan::marked_documents(&ran.output, marker);
            (
                plan::completed_verdict(ran, &documents),
                documents,
                builder_name(orch, ran),
                ran.output.clone(),
            )
        }
        Err(failure) => {
            let verdict = plan::failure_verdict(&spec.target, failure);
            let text = verdict.reason().unwrap_or_default().to_string();
            (verdict, Vec::new(), None, text)
        }
    };
    // The complete record replaces what streamed: Cairn's own lines, then the
    // whole output, including any tail withheld until the batch settled.
    let mut text = progress_lines.join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    let mut body = output;
    crate::security::Sanitizer::structural(&policy).text_in_place(&mut body);
    text.push_str(&body);
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!(
        "{}ended {}{}\n",
        progress::CAIRN_LINE,
        verdict.state(),
        verdict
            .reason()
            .map(|reason| format!(": {reason}"))
            .unwrap_or_default()
    ));
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
    let verifier = last_field(&documents, "verifier").filter(|_| spec.leg != "plan");
    ledger::update_leg(
        &orch.db.local,
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
    Ok(LegEnding { verdict, documents })
}

fn builder_name(orch: &Orchestrator, ran: &CheckExecResult) -> Option<String> {
    ran.provenance
        .as_ref()
        .map(|meta| meta.executor_id.as_str())
        .filter(|id| !id.is_empty())
        .map(|id| orch.fleet.executor_name_for_text(id))
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
