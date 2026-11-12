//! The `devops.rebuild` operator-request consumer (macOS).
//!
//! An accepted request is the only way a rebuild starts. The request primitive
//! claims the attempt and hands this consumer a checked context. The consumer
//! records a launch intent on the attempt's `rebuilds` ledger row, stages a
//! credential-free actor spec that names the attempt, rechecks the context and
//! bootstraps the one launchd job, `computer.cairn.rebuild`. The actor builds
//! exactly the accepted commit and journals as it always has; ingestion turns
//! the journal into the ledger row, and one reconciler maps that row onto the
//! primitive's report, both while the runner is live and after it restarts.
//!
//! Liveness is launchd's answer, never an assumption. A running or unreadable
//! label, whoever loaded it, refuses a launch and holds a launched attempt
//! open. The actor is a one-shot whose definition outlives its process, so
//! the runner owns unloading it: an exited actor's attempt is settled first,
//! then its label booted out. Nothing in this module ever relaunches an attempt.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cairn_common::identity::PrincipalRef;
use cairn_common::requests::{
    RequestAcceptance, RequestAttemptOutcome, RequestFilter, RequestKind, RequestState,
};
use cairn_db::storage::rebuilds::{
    self as ledger, ActorUnload, FrozenReport, LaunchIntent, RebuildRecord,
};
use cairn_db::storage::requests as request_store;
use serde_json::{json, Value};

use super::{ActorJob, ActorState};
use crate::operator_requests::registry::{
    ActorFacts, ConsumerReport, ExecutorTarget, RequestConsumer,
};
use crate::operator_requests::{recover_claimed_request, AcceptedRequestContext};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

pub(crate) const ACTOR_LABEL: &str = "computer.cairn.rebuild";
const KIND: &str = "rebuild";
const ACTOR: &str = "devops.rebuild";
/// The check whose verdict is the card's compile status: `cargo check
/// --all-targets`, so test targets are covered. Reported, never gated.
const COMPILE_CHECK: &str = "workspace-compile";
const VERIFY_TIMEOUT_SECONDS: u64 = 600;
const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

/// The protected `~/.cairn/rebuilds` write floor (cairn/6384) is proven to hold
/// under nested Seatbelt: both `rebuild_floor_*_live` tests ran and passed from
/// an unconfined terminal on macOS 26.4 arm64 at main `0c29467f`, posted as
/// cairn/6384 comment #13. Turning this off again is a reviewed commit that says
/// why the proof no longer holds and changes the test that pins it; filing,
/// acceptance and launch then refuse with [`FLOOR_REFUSAL`].
pub(crate) const MACOS_FLOOR_PROVEN: bool = true;
pub(crate) const FLOOR_REFUSAL: &str = "rebuild dispatch is held: the protected ~/.cairn/rebuilds write floor (cairn/6384) has no live proof yet. `rebuild_floor_refuses_child_writes_even_after_an_approved_crossing_live` and `rebuild_floor_protects_relocated_roots_and_their_ancestors_live` must pass from an unconfined macOS terminal, with the summary posted on cairn/6384, before a request can launch the rebuild actor";

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// One attempt, one rebuild id, so restart recovery finds the same row.
pub(crate) fn rebuild_id(attempt_id: &str) -> String {
    format!("req-{attempt_id}")
}

fn receipt_id(attempt_id: &str) -> String {
    format!("rebuild:{attempt_id}")
}

fn full_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// launchd, injected so tests never touch the real domain.
#[async_trait]
pub(crate) trait Launchd: Send + Sync {
    async fn state(&self) -> ActorJob;
    async fn bootstrap(&self, plist: &Path) -> Result<(), String>;
    /// Remove the label's definition. Called only for an exited actor: booting
    /// out a running one would kill it mid-install.
    async fn bootout(&self) -> Result<(), String>;
}

/// The operator's `gui/<uid>` domain.
#[cfg(target_os = "macos")]
struct GuiDomain;

#[cfg(target_os = "macos")]
fn gui_domain() -> String {
    // SAFETY: getuid has no preconditions.
    format!("gui/{}", unsafe { libc::getuid() })
}

#[cfg(target_os = "macos")]
async fn launchctl(verb: &str, args: &[std::ffi::OsString]) -> Result<(), String> {
    let mut command = tokio::process::Command::new("/bin/launchctl");
    command.arg(verb).args(args).kill_on_drop(true);
    match tokio::time::timeout(Duration::from_secs(30), command.output()).await {
        Ok(Ok(output)) if output.status.success() => Ok(()),
        Ok(Ok(output)) => Err(format!(
            "launchctl {verb} exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )),
        Ok(Err(error)) => Err(format!("launchctl {verb} could not run: {error}")),
        Err(_) => Err(format!("launchctl {verb} did not answer within 30 seconds")),
    }
}

#[cfg(target_os = "macos")]
#[async_trait]
impl Launchd for GuiDomain {
    async fn state(&self) -> ActorJob {
        super::actor_state().await
    }

    async fn bootstrap(&self, plist: &Path) -> Result<(), String> {
        launchctl("bootstrap", &[gui_domain().into(), plist.into()]).await
    }

    async fn bootout(&self) -> Result<(), String> {
        launchctl(
            "bootout",
            &[format!("{}/{ACTOR_LABEL}", gui_domain()).into()],
        )
        .await
    }
}

/// The project that builds this app, as its own store sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Source {
    pub project_id: String,
    pub project_key: String,
    pub checkout: PathBuf,
    pub main_commit: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitsSince {
    pub count: usize,
    pub capped: bool,
    /// Newest first: (commit, subject).
    pub recent: Vec<(String, String)>,
}

/// The source questions the consumer asks, injected so tests need no jj store.
#[async_trait]
pub(crate) trait SourceStore: Send + Sync {
    async fn resolve(&self, orch: &Orchestrator) -> Result<Source, String>;
    async fn contains(
        &self,
        orch: &Orchestrator,
        source: &Source,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool, String>;
    async fn commits_since(
        &self,
        orch: &Orchestrator,
        source: &Source,
        running: &str,
    ) -> Result<CommitsSince, String>;
    async fn signing_identity(
        &self,
        orch: &Orchestrator,
        source: &Source,
        commit: &str,
    ) -> Result<String, String>;
    /// The Git object repository the acceptance was checked against, which the
    /// actor fetches the accepted commit from: a local path, so no credential.
    async fn object_repository(
        &self,
        orch: &Orchestrator,
        source: &Source,
    ) -> Result<PathBuf, String>;
}

/// Production answers: the runner-owned jj store of the one project whose
/// checkout builds a product named Cairn.
struct ProjectStores;

/// The signing identity `src-tauri/tauri.conf.json` names at `commit`, read
/// from the project's object repository (a working tree or a bare store).
pub(crate) fn signing_identity_at(
    object_repository: &Path,
    commit: &str,
) -> Result<String, String> {
    const TAURI_CONF: &str = "src-tauri/tauri.conf.json";
    let bytes = crate::mcp::handlers::read::file_at_commit(
        object_repository.to_path_buf(),
        commit.to_string(),
        TAURI_CONF,
    )?
    .ok_or_else(|| format!("{TAURI_CONF} is absent at {commit}"))?;
    let text = String::from_utf8(bytes)
        .map_err(|error| format!("{TAURI_CONF} at {commit} is not UTF-8: {error}"))?;
    signing_identity_of(&text)
}

pub(crate) fn signing_identity_of(tauri_conf: &str) -> Result<String, String> {
    let value: Value = serde_json::from_str(tauri_conf)
        .map_err(|error| format!("tauri.conf.json is unreadable: {error}"))?;
    match value["bundle"]["macOS"]["signingIdentity"].as_str() {
        Some(identity) if !identity.trim().is_empty() && identity != "-" => Ok(identity.into()),
        _ => Err("tauri.conf.json names no Developer ID signing identity".into()),
    }
}

/// The bundle identifier of the Cairn app, which is what makes a checkout the
/// one that builds Cairn. `productName` is only a display string; the
/// identifier carries signing, updates and keychain/TCC grants, so it does not
/// drift casually.
pub(crate) const CAIRN_BUNDLE_IDENTIFIER: &str = "com.cairn.desktop";

pub(crate) fn builds_cairn(tauri_conf: &str) -> bool {
    serde_json::from_str::<Value>(tauri_conf)
        .is_ok_and(|value| value["identifier"].as_str() == Some(CAIRN_BUNDLE_IDENTIFIER))
}

#[async_trait]
impl SourceStore for ProjectStores {
    async fn resolve(&self, orch: &Orchestrator) -> Result<Source, String> {
        let projects = orch
            .db
            .local
            .query_all(
                "SELECT id,key,repo_path FROM projects WHERE is_workspace=0 ORDER BY key"
                    .to_string(),
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .map_err(|error| format!("projects are unreadable: {error}"))?;
        let mut found = Vec::new();
        for (id, key, repo) in projects {
            let conf = Path::new(&repo).join("src-tauri").join("tauri.conf.json");
            let Ok(text) = tokio::fs::read_to_string(&conf).await else {
                continue;
            };
            if builds_cairn(&text) {
                found.push((id, key, PathBuf::from(repo)));
            }
        }
        let (project_id, project_key, checkout) = match found.len() {
            1 => found.remove(0),
            0 => return Err(format!("no project on this install builds Cairn (no checkout's src-tauri/tauri.conf.json has identifier {CAIRN_BUNDLE_IDENTIFIER:?})")),
            _ => return Err(format!(
                "several projects build Cairn ({}); the rebuild source is ambiguous",
                found.iter().map(|(_, key, _)| key.as_str()).collect::<Vec<_>>().join(", ")
            )),
        };
        let main =
            crate::mcp::handlers::branch::resolve_for_project_read(orch, &project_id, "main")
                .await?;
        Ok(Source {
            project_id,
            project_key,
            checkout,
            main_commit: main.commit_id,
        })
    }

    async fn contains(
        &self,
        orch: &Orchestrator,
        source: &Source,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool, String> {
        let store = crate::mcp::handlers::branch::resolve_for_project_read(
            orch,
            &source.project_id,
            "main",
        )
        .await?;
        cairn_vcs::history::contains(&store.repository_path, ancestor, descendant)
            .await
            .map_err(|error| error.to_string())
    }

    async fn commits_since(
        &self,
        orch: &Orchestrator,
        source: &Source,
        running: &str,
    ) -> Result<CommitsSince, String> {
        let store = crate::mcp::handlers::branch::resolve_for_project_read(
            orch,
            &source.project_id,
            "main",
        )
        .await?;
        let limit = Duration::from_secs(5);
        let ancestry = cairn_vcs::history::ancestry(
            &store.repository_path,
            running,
            &source.main_commit,
            500,
            limit,
        )
        .await
        .map_err(|error| error.to_string())?;
        let page = cairn_vcs::history::log(
            &store.repository_path,
            cairn_vcs::history::LogQuery {
                to: source.main_commit.clone(),
                from: Some(running.to_string()),
                path: None,
                limit: 20,
                offset: 0,
            },
            limit,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(CommitsSince {
            count: ancestry.ahead.count,
            capped: ancestry.ahead.capped,
            recent: page
                .entries
                .iter()
                .map(|entry| (entry.commit_id.clone(), entry.subject().to_string()))
                .collect(),
        })
    }

    async fn signing_identity(
        &self,
        orch: &Orchestrator,
        source: &Source,
        commit: &str,
    ) -> Result<String, String> {
        let store = crate::mcp::handlers::branch::resolve_for_project_read(
            orch,
            &source.project_id,
            "main",
        )
        .await?;
        let commit = commit.to_string();
        tokio::task::spawn_blocking(move || {
            signing_identity_at(&store.object_repository_path, &commit)
        })
        .await
        .map_err(|error| format!("reading tauri.conf.json did not finish: {error}"))?
    }

    async fn object_repository(
        &self,
        orch: &Orchestrator,
        source: &Source,
    ) -> Result<PathBuf, String> {
        let store = crate::mcp::handlers::branch::resolve_for_project_read(
            orch,
            &source.project_id,
            "main",
        )
        .await?;
        Ok(store.object_repository_path)
    }
}

/// Where a rebuild lives on this machine.
#[derive(Debug, Clone)]
pub(crate) struct RebuildHost {
    /// The Cairn home (`~/.cairn`); the actor's `CAIRN_HOME`.
    pub home: PathBuf,
    /// The protected journal root, `<home>/rebuilds`.
    pub rebuilds: PathBuf,
    /// `~/Library/LaunchAgents`, where the one-shot plist goes.
    pub launch_agents: PathBuf,
    /// The installed bundle this runner runs from, if it runs from one.
    pub app: Option<PathBuf>,
}

impl RebuildHost {
    fn bundled_actor(&self) -> Result<(PathBuf, PathBuf, PathBuf), String> {
        let app = self.app.clone().ok_or(
            "rebuild is unavailable: this runner is not running from an installed Cairn.app, so there is no bundled actor to stage",
        )?;
        let bun = app.join("Contents/MacOS/bun");
        let script = app.join("Contents/Resources/runtime/rebuild/rebuild.js");
        for path in [&bun, &script] {
            if !path.is_file() {
                return Err(format!(
                    "rebuild is unavailable: {} carries no {}",
                    app.display(),
                    path.strip_prefix(&app).unwrap_or(path).display()
                ));
            }
        }
        Ok((app, bun, script))
    }
}

/// The bundle containing this executable, when it is `<X>.app/Contents/MacOS/<bin>`.
pub(crate) fn installed_bundle(exe: &Path) -> Option<PathBuf> {
    let macos = exe.parent()?;
    let contents = macos.parent()?;
    let bundle = contents.parent()?;
    (macos.file_name()? == "MacOS"
        && contents.file_name()? == "Contents"
        && bundle.extension()? == "app")
        .then(|| bundle.to_path_buf())
}

/// What the facts provider gathered, before it is shaped into a document.
#[derive(Debug, Clone)]
pub(crate) struct FactsInputs {
    pub source: Source,
    pub running_version: String,
    pub running_build_id: Option<String>,
    pub running_source_commit: Option<String>,
    pub running_booted_at: Option<i64>,
    pub commits_since: Result<CommitsSince, String>,
    pub checks: Vec<CheckResult>,
    pub build_seconds: Vec<i64>,
    pub signing_identity: Result<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckResult {
    pub check: String,
    pub platform: Option<String>,
    pub verdict: String,
    pub commit: String,
    /// Milliseconds since the Unix epoch.
    pub ran_at_ms: i64,
}

fn median(values: &[i64]) -> Option<i64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    match sorted.len() {
        0 => None,
        n if n % 2 == 1 => Some(sorted[n / 2]),
        n => Some((sorted[n / 2 - 1] + sorted[n / 2]) / 2),
    }
}

/// Shape the card's facts. Every fact carries when it was true: the document's
/// `observedAt`, and each check result's own run time and age. Check results
/// and compile status are reported for the operator, never gated (readiness is
/// "builds only").
pub(crate) fn assemble_facts(inputs: FactsInputs, now: i64) -> ActorFacts {
    let checks: Vec<Value> = inputs
        .checks
        .iter()
        .map(|check| {
            json!({
                "check": check.check, "platform": check.platform, "verdict": check.verdict,
                "commit": check.commit, "ranAt": check.ran_at_ms / 1000,
                "ageSeconds": (now - check.ran_at_ms / 1000).max(0),
            })
        })
        .collect();
    let compile = checks
        .iter()
        .filter(|check| check["check"] == COMPILE_CHECK)
        .cloned()
        .collect::<Vec<_>>();
    let commits_since = match (&inputs.running_source_commit, &inputs.commits_since) {
        (None, _) => {
            json!({"unknown": "the running build carries no source commit (an unstamped build)"})
        }
        (Some(_), Ok(since)) => json!({
            "count": since.count, "capped": since.capped,
            "recent": since.recent.iter().map(|(commit, subject)| json!({"commit": commit, "subject": subject})).collect::<Vec<_>>(),
        }),
        (Some(_), Err(error)) => json!({"unknown": error}),
    };
    let expected = match median(&inputs.build_seconds) {
        Some(seconds) => {
            json!({"medianBuildSeconds": seconds, "samples": inputs.build_seconds.len()})
        }
        None => {
            json!({"unknown": "no rebuild has completed a build in the shared release tree yet"})
        }
    };
    let document = json!({
        "observedAt": now,
        "readiness": "builds only: check results and compile status are reported, never gated",
        "running": {
            "version": inputs.running_version, "buildId": inputs.running_build_id,
            "sourceCommit": inputs.running_source_commit, "bootedAt": inputs.running_booted_at,
        },
        "source": {"project": inputs.source.project_key, "projectId": inputs.source.project_id},
        "candidate": {"branch": "main", "commit": inputs.source.main_commit},
        "commitsSinceRunning": commits_since,
        "checks": checks,
        "compile": if compile.is_empty() { json!({"unknown": format!("no {COMPILE_CHECK} result on main")}) } else { Value::Array(compile) },
        "expectedDuration": expected,
        "signingIdentity": match &inputs.signing_identity { Ok(identity) => json!(identity), Err(error) => json!({"unknown": error}) },
    });
    ActorFacts {
        document,
        running_version: Some(inputs.running_version),
        main_commit: Some(inputs.source.main_commit.clone()),
        candidate_commit: Some(inputs.source.main_commit),
        base_commit: None,
        policy_project_id: Some(inputs.source.project_id),
        drain: None,
    }
}

/// Map a settled ledger row onto the primitive's report. `None` while the
/// actor may still be running: launched, and launchd shows it neither absent
/// nor exited.
pub(crate) fn decide_report(
    row: &RebuildRecord,
    launch: &LaunchIntent,
    actor: ActorState,
    at: i64,
) -> Option<FrozenReport> {
    let launched = launch.bootstrap_requested_at.is_some();
    if launched && !actor.actor_gone() {
        return None;
    }
    let last_phase = row.phases.last().map(|phase| phase.name.clone());
    let (outcome, detail) = match row.outcome.as_deref() {
        Some("done") if row.commit_built.as_deref() == Some(launch.commit.as_str()) => {
            (RequestAttemptOutcome::Done, None)
        }
        Some("done") => (
            RequestAttemptOutcome::Inconclusive,
            Some(format!(
                "the ledger reports done, but built {}, not the accepted {}",
                row.commit_built.as_deref().unwrap_or("no recorded commit"),
                launch.commit
            )),
        ),
        Some("failed") => (RequestAttemptOutcome::Failed, None),
        Some("rolled_back") => (RequestAttemptOutcome::RolledBack, None),
        Some("inconclusive") => (RequestAttemptOutcome::Inconclusive, None),
        Some("cancelled") => (RequestAttemptOutcome::Cancelled, None),
        Some(other) => (
            RequestAttemptOutcome::Failed,
            Some(format!("the ledger holds an unknown outcome {other}")),
        ),
        None if launched => (
            RequestAttemptOutcome::Failed,
            Some(format!(
                "the actor exited without an outcome during {}",
                last_phase.as_deref().unwrap_or("launch")
            )),
        ),
        None => (
            RequestAttemptOutcome::Failed,
            Some("the runner stopped before launching the actor".into()),
        ),
    };
    let built_commit = (outcome == RequestAttemptOutcome::Done).then(|| launch.commit.clone());
    let mut document = json!({
        "rebuild": format!("cairn://rebuilds/{}", row.id),
        "outcome": row.outcome, "summary": row.summary, "detail": detail,
        "lastPhase": last_phase, "launched": launched,
    });
    // What the fence removed before this attempt's own actor could launch.
    if !row.actor_unloads.is_empty() {
        document["removedDefinitions"] = json!(row
            .actor_unloads
            .iter()
            .map(describe_unload)
            .collect::<Vec<_>>());
    }
    Some(FrozenReport {
        receipt_id: receipt_id(&launch.attempt_id),
        document,
        outcome,
        // Either nothing was ever launched, or launchd positively shows the
        // label gone: the attempt's actor is fenced in both cases.
        settled: true,
        built_commit,
        frozen_at: at,
        acknowledged_at: None,
    })
}

fn describe_unload(unload: &ActorUnload) -> String {
    let owner = match &unload.owner {
        Some(owner) => format!("the actor of cairn://rebuilds/{owner}"),
        None => "a definition no request launched".into(),
    };
    let done = if unload.removed {
        "booted out".to_string()
    } else {
        format!(
            "could not be booted out: {}",
            unload.detail.as_deref().unwrap_or("no answer")
        )
    };
    format!(
        "the exited launchd job {} ({owner}; program {}; {}) {done}",
        unload.label,
        unload.program.as_deref().unwrap_or("not printed"),
        unload
            .last_exit
            .as_deref()
            .unwrap_or("no last exit printed"),
    )
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The one-shot job: same shape the slice-one launcher wrote.
pub(crate) fn actor_plist(argv: &[String], env: &[(String, String)], log: &Path) -> String {
    let strings = argv
        .iter()
        .map(|value| format!("<string>{}</string>", escape_xml(value)))
        .collect::<String>();
    let environment = env
        .iter()
        .map(|(key, value)| {
            format!(
                "<key>{}</key><string>{}</string>",
                escape_xml(key),
                escape_xml(value)
            )
        })
        .collect::<String>();
    let log = escape_xml(&log.display().to_string());
    // Verification plus five minutes for cutover.
    let exit_timeout = VERIFY_TIMEOUT_SECONDS + 5 * 60;
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>Label</key><string>{ACTOR_LABEL}</string><key>ProgramArguments</key><array>{strings}</array><key>EnvironmentVariables</key><dict>{environment}</dict><key>RunAtLoad</key><true/><key>ExitTimeOut</key><integer>{exit_timeout}</integer><key>StandardOutPath</key><string>{log}</string><key>StandardErrorPath</key><string>{log}</string></dict></plist>"#
    )
}

/// An explicit allowlist: no run credential, release secret or callback
/// address can reach a process that outlives every run.
fn actor_env(home: &Path) -> Vec<(String, String)> {
    let mut env = vec![
        (
            "PATH".to_string(),
            cairn_common::toolchain_path::agent_shell_path(),
        ),
        ("CAIRN_HOME".to_string(), home.display().to_string()),
    ];
    for key in [
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "TMPDIR",
        "LANG",
        "CAIRN_RUNNER_PORT",
    ] {
        if let Ok(value) = std::env::var(key) {
            env.push((key.to_string(), value));
        }
    }
    env
}

fn create_private_dir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path)
        .map_err(|error| format!("could not create {}: {error}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("could not restrict {}: {error}", path.display()))
}

fn write_private(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .map_err(|error| format!("could not create {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("could not write {}: {error}", path.display()))
}

/// The runner speaks in the journal only for an attempt whose actor it never
/// launched: it records why, as the attempt's terminal outcome, so the refusal
/// travels the same ledger path as every actor outcome.
fn journal_refusal(root: &Path, id: &str, summary: &str) -> Result<(), String> {
    create_private_dir(root)?;
    let event = json!({
        "event": uuid::Uuid::new_v4().to_string(), "rebuild": id,
        "at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "type": "outcome", "outcome": "failed", "summary": summary,
    });
    let path = root.join(format!("{id}.jsonl"));
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(&path)
        .map_err(|error| format!("could not open {}: {error}", path.display()))?;
    writeln!(file, "{event}")
        .map_err(|error| format!("could not write {}: {error}", path.display()))
}

pub(crate) struct RebuildConsumer {
    host: RebuildHost,
    launchd: Arc<dyn Launchd>,
    source: Arc<dyn SourceStore>,
    /// [`MACOS_FLOOR_PROVEN`] in production.
    dispatch_enabled: bool,
    /// The primitive holds this from before its claim until dispatch returns,
    /// and reconciliation holds it too: a claimed attempt with no ledger row
    /// seen under it was abandoned by an earlier runner, never in flight here.
    serial: Arc<tokio::sync::Mutex<()>>,
}

impl RebuildConsumer {
    /// The gate is the constant; no runtime value can change it.
    pub(crate) fn new(
        host: RebuildHost,
        launchd: Arc<dyn Launchd>,
        source: Arc<dyn SourceStore>,
    ) -> Self {
        Self {
            host,
            launchd,
            source,
            dispatch_enabled: MACOS_FLOOR_PROVEN,
            serial: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Exercise the held path in tests only.
    #[cfg(test)]
    fn with_gate_closed_for_test(mut self) -> Self {
        self.dispatch_enabled = false;
        self
    }

    fn gate(&self) -> Result<(), String> {
        if self.dispatch_enabled {
            Ok(())
        } else {
            Err(FLOOR_REFUSAL.into())
        }
    }

    #[cfg(target_os = "macos")]
    fn production(orch: &Orchestrator) -> Self {
        let app = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.canonicalize().ok())
            .and_then(|exe| installed_bundle(&exe));
        let host = RebuildHost {
            home: orch.config_dir.clone(),
            rebuilds: orch.config_dir.join("rebuilds"),
            launch_agents: dirs::home_dir()
                .unwrap_or_default()
                .join("Library/LaunchAgents"),
            app,
        };
        Self::new(host, Arc::new(GuiDomain), Arc::new(ProjectStores))
    }

    async fn gather(&self, orch: &Orchestrator) -> Result<FactsInputs, String> {
        let source = self.source.resolve(orch).await?;
        let boot = crate::orchestrator::wakes::current_boot();
        let running_source_commit = boot.and_then(|boot| boot.source_commit.clone());
        let commits_since = match &running_source_commit {
            Some(running) => self.source.commits_since(orch, &source, running).await,
            None => Err("unstamped build".into()),
        };
        let signing_identity = self
            .source
            .signing_identity(orch, &source, &source.main_commit)
            .await;
        let checks = latest_main_checks(orch, &source.project_id).await;
        let build_seconds = ledger::build_durations(&orch.db.local, 50)
            .await
            .unwrap_or_default();
        Ok(FactsInputs {
            running_version: boot
                .map(|boot| boot.version.clone())
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
            running_build_id: boot.map(|boot| boot.build_id.clone()),
            running_booted_at: boot.map(|boot| boot.booted_at),
            running_source_commit,
            source,
            commits_since,
            checks,
            build_seconds,
            signing_identity,
        })
    }

    /// Every refusal before bootstrap is journalled as the attempt's outcome.
    async fn launch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        id: &str,
    ) -> Result<(), String> {
        let acceptance = context.acceptance().clone();
        self.gate()?;
        self.fence(orch, id).await?;
        let (app, bun, script) = self.host.bundled_actor()?;
        let source = self.source.resolve(orch).await?;
        if acceptance.policy_project_id.as_deref() != Some(source.project_id.as_str()) {
            return Err(format!(
                "the accepted source project {:?} is not the project that builds Cairn now ({})",
                acceptance.policy_project_id, source.project_key
            ));
        }
        let commit = acceptance
            .candidate_commit
            .clone()
            .ok_or("the acceptance froze no commit")?;
        let origin = self.source.object_repository(orch, &source).await?;
        let plist = self.stage(
            id,
            &acceptance,
            context.authorization_event_id(),
            &commit,
            &origin,
            &source,
            &app,
            &bun,
            &script,
        )?;
        // The last check before the physical act, against the live attempt.
        context
            .recheck_for(orch, &acceptance)
            .await
            .map_err(|error| format!("refused at launch: {error}"))?;
        self.fence(orch, id).await?;
        ledger::mark_bootstrap_requested(&orch.db.local, id, now())
            .await
            .map_err(|error| error.to_string())?;
        if let Err(error) = self.launchd.bootstrap(&plist).await {
            // Not a refusal: launchd may have loaded the job before failing to
            // answer. The reconciler settles it from launchd's own state.
            tracing::warn!(rebuild = id, %error, "rebuild actor bootstrap reported an error");
        }
        Ok(())
    }

    /// The physical fence is the label itself, whoever loaded it. An exited
    /// definition holds the label without an actor: its attempt is settled
    /// first, then it is booted out and named on `id`'s row.
    async fn fence(&self, orch: &Orchestrator, id: &str) -> Result<(), String> {
        let job = self.launchd.state().await;
        match job.state {
            ActorState::Absent => Ok(()),
            ActorState::Running => Err(format!("refused: the launchd job {ACTOR_LABEL} is already loaded, whoever loaded it; a second rebuild actor is never started")),
            ActorState::Unknown => Err(format!("refused: launchctl could not say whether {ACTOR_LABEL} is loaded; an unknown actor is never duplicated")),
            ActorState::Exited => {
                let owner = self.launched_owner(orch, &job).await?;
                if let Some(owner) = owner.as_ref().filter(|row| row.consumer_report.is_none()) {
                    super::ingest_once(&orch.db.local, &self.host.rebuilds, job.state).await?;
                    let row = ledger::get(&orch.db.local, &owner.id)
                        .await
                        .map_err(|error| error.to_string())?
                        .ok_or("the exited actor's ledger row vanished")?;
                    if let Err(error) = self.settle(orch, &row, job.state).await {
                        tracing::warn!(rebuild = %row.id, %error, "rebuild request report deferred");
                    }
                    let frozen = ledger::get(&orch.db.local, &owner.id)
                        .await
                        .map_err(|error| error.to_string())?
                        .is_some_and(|row| row.consumer_report.is_some());
                    if !frozen {
                        return Err(format!("refused: the exited actor of cairn://rebuilds/{} still holds {ACTOR_LABEL} and its report could not be frozen; its outcome is never lost to a new launch", owner.id));
                    }
                }
                let unload = self
                    .unload(orch, &job, "fence", owner.as_ref().map(|row| row.id.clone()), id)
                    .await?;
                if unload.removed {
                    tracing::info!(rebuild = id, removed = %describe_unload(&unload), "rebuild fence cleared an exited actor definition");
                    Ok(())
                } else {
                    Err(format!("refused: {}; a second rebuild actor is never started beside it", describe_unload(&unload)))
                }
            }
        }
    }

    /// The ledger row of the launched attempt whose staged runtime the
    /// definition runs. A definition naming no such row (a hand-run rebuild,
    /// a stale staging directory) is never adopted as any attempt's outcome.
    async fn launched_owner(
        &self,
        orch: &Orchestrator,
        job: &ActorJob,
    ) -> Result<Option<RebuildRecord>, String> {
        let Some(program) = job.program.as_deref().map(Path::new) else {
            return Ok(None);
        };
        let Some(dir) = program
            .parent()
            .filter(|_| program.file_name() == Some(std::ffi::OsStr::new(super::ACTOR_RUNTIME)))
            .filter(|dir| dir.parent() == Some(self.host.rebuilds.as_path()))
        else {
            return Ok(None);
        };
        let Some(id) = dir.file_name().and_then(|name| name.to_str()) else {
            return Ok(None);
        };
        Ok(ledger::get(&orch.db.local, id)
            .await
            .map_err(|error| error.to_string())?
            .filter(|row| {
                row.launch
                    .as_ref()
                    .is_some_and(|launch| launch.bootstrap_requested_at.is_some())
            }))
    }

    /// Boot an exited definition out, confirm the label is gone, and record
    /// both on `record_on`'s row.
    async fn unload(
        &self,
        orch: &Orchestrator,
        job: &ActorJob,
        reason: &str,
        owner: Option<String>,
        record_on: &str,
    ) -> Result<ActorUnload, String> {
        let detail = match self.launchd.bootout().await {
            Err(error) => Some(error),
            Ok(()) => match self.launchd.state().await.state {
                ActorState::Absent => None,
                other => Some(format!(
                    "launchctl bootout returned, but launchd still shows {ACTOR_LABEL} {other:?}"
                )),
            },
        };
        let unload = ActorUnload {
            label: ACTOR_LABEL.into(),
            reason: reason.into(),
            program: job.program.clone(),
            last_exit: job.last_exit.clone(),
            owner,
            removed: detail.is_none(),
            detail,
            at: now(),
        };
        ledger::record_actor_unload(&orch.db.local, record_on, unload.clone())
            .await
            .map_err(|error| error.to_string())?;
        Ok(unload)
    }

    /// After a pass's reports: an exited actor whose own attempt has frozen
    /// its report is booted out, so the domain returns to absent. A definition
    /// no launched attempt owns is left to the next launch's fence, which
    /// names it on that attempt's report. One try per attempt: a bootout that
    /// failed is retried by that fence, not every pass.
    async fn unload_settled(&self, orch: &Orchestrator, job: &ActorJob) -> Result<(), String> {
        let Some(owner) = self.launched_owner(orch, job).await? else {
            return Ok(());
        };
        let tried = owner
            .actor_unloads
            .iter()
            .any(|unload| unload.reason == "settled");
        if owner.consumer_report.is_none() || tried {
            return Ok(());
        }
        let unload = self
            .unload(orch, job, "settled", Some(owner.id.clone()), &owner.id)
            .await?;
        if unload.removed {
            Ok(())
        } else {
            Err(describe_unload(&unload))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn stage(
        &self,
        id: &str,
        acceptance: &RequestAcceptance,
        authorization_event_id: &str,
        commit: &str,
        origin: &Path,
        source: &Source,
        app: &Path,
        bun: &Path,
        script: &Path,
    ) -> Result<PathBuf, String> {
        let root = &self.host.rebuilds;
        create_private_dir(root)?;
        let dir = root.join(id);
        fs::create_dir(&dir).map_err(|error| {
            format!(
                "could not create {}: {error}; a launch is never repeated",
                dir.display()
            )
        })?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        let staged_bun = dir.join(super::ACTOR_RUNTIME);
        let staged_script = dir.join("rebuild.js");
        fs::copy(bun, &staged_bun).map_err(|error| format!("could not stage bun: {error}"))?;
        fs::set_permissions(&staged_bun, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        fs::copy(script, &staged_script)
            .map_err(|error| format!("could not stage the actor: {error}"))?;
        let env_file = source.checkout.join(".env");
        // A reference to the checked attempt, never a bearer: the actor holds no
        // credential, and the protected floor is what makes this file the
        // runner's.
        let spec = json!({
            "id": id,
            "home": self.host.home,
            "app": app,
            "tree": root.join("tree"),
            "origin": origin,
            "commit": commit,
            "envFile": env_file.is_file().then_some(env_file),
            "checkoutTarget": source.checkout.join("src-tauri/target"),
            "skipBuild": false,
            "sabotage": false,
            "allowStranding": false,
            "verifyTimeoutSeconds": VERIFY_TIMEOUT_SECONDS,
            "request": {
                "requestIds": acceptance.request_ids,
                "attemptId": acceptance.attempt_id,
                "authorizationEventId": authorization_event_id,
                "rebuildId": id,
                "kind": acceptance.kind.as_str(),
                "target": acceptance.target,
                "commit": commit,
                "factsVersion": acceptance.facts_version,
            },
        });
        let spec_path = dir.join("spec.json");
        write_private(
            &spec_path,
            format!("{}\n", serde_json::to_string_pretty(&spec).unwrap()).as_bytes(),
            0o600,
        )?;
        write_private(&root.join(format!("{id}.jsonl")), b"", 0o600)?;
        fs::create_dir_all(&self.host.launch_agents).map_err(|error| error.to_string())?;
        let plist = self.host.launch_agents.join(format!("{ACTOR_LABEL}.plist"));
        let argv = [
            staged_bun.display().to_string(),
            staged_script.display().to_string(),
            "--actor".to_string(),
            spec_path.display().to_string(),
        ];
        fs::write(
            &plist,
            actor_plist(&argv, &actor_env(&self.host.home), &dir.join("actor.log")),
        )
        .map_err(|error| format!("could not write {}: {error}", plist.display()))?;
        Ok(plist)
    }

    /// Claims an earlier runner made and never recorded a launch for.
    async fn adopt_abandoned_claims(&self, orch: &Orchestrator) -> Result<(), String> {
        let requests = request_store::list_requests(
            &orch.db.local,
            crate::authorization::WORKSPACE_ID,
            &RequestFilter {
                status: Some(RequestState::Acting),
                kind: Some(RequestKind::Rebuild),
                limit: Some(199),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        for request in requests {
            for attempt in &request.attempts {
                if attempt.state != RequestState::Acting || attempt.claim_id.is_none() {
                    continue;
                }
                let id = rebuild_id(&attempt.acceptance.attempt_id);
                if ledger::get(&orch.db.local, &id)
                    .await
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    continue;
                }
                ledger::record_launch_intent(
                    &orch.db.local,
                    &id,
                    intent(&attempt.acceptance, &attempt.authorization_event_id),
                )
                .await
                .map_err(|error| error.to_string())?;
                journal_refusal(
                    &self.host.rebuilds,
                    &id,
                    "the runner stopped after claiming this attempt and before launching its actor",
                )?;
            }
        }
        Ok(())
    }

    /// Observe launchd, then ingest, then decide: an actor that wrote its
    /// outcome and exited is read from a final journal, never declared dead.
    /// Only then is an exited actor's definition unloaded.
    async fn reconcile_locked(&self, orch: &Orchestrator) -> Result<usize, String> {
        self.adopt_abandoned_claims(orch).await?;
        let job = self.launchd.state().await;
        let actor = job.state;
        super::ingest_once(&orch.db.local, &self.host.rebuilds, actor).await?;
        let rows = ledger::unacknowledged_launches(&orch.db.local)
            .await
            .map_err(|error| error.to_string())?;
        let mut reported = 0;
        for row in rows {
            match self.settle(orch, &row, actor).await {
                Ok(true) => reported += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(rebuild = %row.id, %error, "rebuild request report deferred")
                }
            }
        }
        if actor == ActorState::Exited {
            if let Err(error) = self.unload_settled(orch, &job).await {
                tracing::warn!(%error, "rebuild actor definition left loaded");
            }
        }
        Ok(reported)
    }

    pub(crate) async fn reconcile(&self, orch: &Orchestrator) -> Result<usize, String> {
        let _serial = self.serial.lock().await;
        self.reconcile_locked(orch).await
    }

    async fn settle(
        &self,
        orch: &Orchestrator,
        row: &RebuildRecord,
        actor: ActorState,
    ) -> Result<bool, String> {
        let launch = row.launch.as_ref().ok_or("no launch intent")?;
        let attempt =
            request_store::get_attempt_by_event(&orch.db.local, &launch.authorization_event_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or("the launch names no accepted attempt")?;
        if attempt.acceptance.attempt_id != launch.attempt_id {
            return Err("the launch intent names a different attempt".into());
        }
        let adapter = orch.operator_requests.consumer(KIND)?;
        // Restart and live paths alike: the checked context comes from durable
        // state, and recovery is effect-free; only `report` follows.
        let context = recover_claimed_request(
            orch,
            &launch.authorization_event_id,
            &attempt.acceptance,
            adapter,
        )
        .await?;
        let frozen = match &row.consumer_report {
            Some(frozen) => frozen.clone(),
            None => {
                let Some(report) = decide_report(row, launch, actor, now()) else {
                    return Ok(false);
                };
                ledger::freeze_report(&orch.db.local, &row.id, report)
                    .await
                    .map_err(|error| error.to_string())?
            }
        };
        // A replay after a crash before acknowledgement sends the same bytes
        // under the same receipt; the primitive treats it as the ack it missed.
        context
            .report(
                orch,
                &frozen.receipt_id,
                ConsumerReport {
                    outcome: frozen.outcome.clone(),
                    document: frozen.document.clone(),
                    settled: frozen.settled,
                    built_commit: frozen.built_commit.clone(),
                },
            )
            .await?;
        ledger::acknowledge_report(&orch.db.local, &row.id, &frozen.receipt_id, now())
            .await
            .map_err(|error| error.to_string())?;
        Ok(true)
    }
}

fn intent(acceptance: &RequestAcceptance, authorization_event_id: &str) -> LaunchIntent {
    LaunchIntent {
        request_ids: acceptance.request_ids.clone(),
        attempt_id: acceptance.attempt_id.clone(),
        authorization_event_id: authorization_event_id.to_string(),
        kind: acceptance.kind.as_str().to_string(),
        target: acceptance.target.clone(),
        commit: acceptance.candidate_commit.clone().unwrap_or_default(),
        facts_version: acceptance.facts_version,
        recorded_at: now(),
        bootstrap_requested_at: None,
    }
}

/// The latest main-cadence verdict per check and platform, newest first.
async fn latest_main_checks(orch: &Orchestrator, project_id: &str) -> Vec<CheckResult> {
    let Ok(db) = crate::execution::routing::routing_db_for_id(&orch.db, project_id).await else {
        return vec![];
    };
    let since_ms = (now() - 14 * 86_400) * 1000;
    let rows = db
        .query_all(
            "SELECT check_name,verdict_platform,verdict,commit_sha,ran_at FROM check_result_observations WHERE project_id=?1 AND cadence='main' AND ran_at>?2 ORDER BY ran_at DESC LIMIT 2000".to_string(),
            (project_id.to_string(), since_ms),
            |row| {
                Ok(CheckResult {
                    check: row.text(0)?,
                    platform: row.opt_text(1)?,
                    verdict: row.text(2)?,
                    commit: row.text(3)?,
                    ran_at_ms: row.i64(4)?,
                })
            },
        )
        .await
        .unwrap_or_default();
    let mut seen = std::collections::BTreeSet::new();
    let mut latest: Vec<CheckResult> = rows
        .into_iter()
        .filter(|row| seen.insert((row.check.clone(), row.platform.clone())))
        .collect();
    latest.sort_by(|a, b| a.check.cmp(&b.check).then(a.platform.cmp(&b.platform)));
    latest
}

#[async_trait]
impl RequestConsumer for RebuildConsumer {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn actor(&self) -> &'static str {
        ACTOR
    }

    async fn availability(&self, _: &Orchestrator) -> Result<(), String> {
        // Filing is refused too: a card that can never be accepted is worse
        // than an immediate, named refusal to the requesting thread.
        self.gate()?;
        self.host.bundled_actor().map(|_| ())
    }

    async fn exclusion(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Some(self.serial.clone().lock_owned().await)
    }

    async fn facts(
        &self,
        orch: &Orchestrator,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<ActorFacts, String> {
        Ok(assemble_facts(self.gather(orch).await?, now()))
    }

    async fn validate_snapshot(
        &self,
        orch: &Orchestrator,
        acceptance: &RequestAcceptance,
    ) -> Result<(), String> {
        if acceptance.kind != RequestKind::Rebuild
            || acceptance.actor_id != ACTOR
            || acceptance.target.is_some()
            || acceptance.drain.is_some()
            || acceptance.base_commit.is_some()
        {
            return Err(
                "a rebuild acceptance names only the frozen candidate commit and source project"
                    .into(),
            );
        }
        let candidate = acceptance
            .candidate_commit
            .as_deref()
            .filter(|commit| full_commit(commit))
            .ok_or("a rebuild acceptance must freeze a full candidate commit")?;
        self.gate()?;
        let source = self.source.resolve(orch).await?;
        if acceptance.policy_project_id.as_deref() != Some(source.project_id.as_str()) {
            return Err(format!(
                "the accepted source project is not the project that builds Cairn ({})",
                source.project_key
            ));
        }
        if !self
            .source
            .contains(orch, &source, candidate, &source.main_commit)
            .await?
        {
            return Err(format!(
                "the accepted commit {candidate} is not on {}'s main",
                source.project_key
            ));
        }
        Ok(())
    }

    async fn contains_commits(
        &self,
        orch: &Orchestrator,
        candidate: &str,
        required: &[String],
    ) -> Result<(), String> {
        let source = self.source.resolve(orch).await?;
        for commit in required {
            if !self
                .source
                .contains(orch, &source, commit, candidate)
                .await?
            {
                return Err(format!("{candidate} does not contain {commit}"));
            }
        }
        Ok(())
    }

    async fn executor_target(&self, _: &Orchestrator, _: &str) -> Result<ExecutorTarget, String> {
        Err("rebuild has no executor target".into())
    }

    /// The ledger row is the evidence: a report is accepted only when it is the
    /// exact one this consumer froze on the attempt's row.
    async fn verify_report(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String> {
        let attempt = &context.acceptance().attempt_id;
        let row = ledger::get(&orch.db.local, &rebuild_id(attempt))
            .await
            .map_err(|error| error.to_string())?
            .ok_or("the attempt has no rebuilds ledger row")?;
        let frozen = row
            .consumer_report
            .as_ref()
            .ok_or("the ledger row froze no report")?;
        let matches = row
            .launch
            .as_ref()
            .is_some_and(|launch| &launch.attempt_id == attempt)
            && frozen.outcome == report.outcome
            && frozen.document == report.document
            && frozen.settled == report.settled
            && frozen.built_commit == report.built_commit;
        if !matches {
            return Err("the report differs from the one frozen on the rebuilds ledger row".into());
        }
        Ok(report)
    }

    async fn dispatch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String> {
        // Runs under `exclusion`, which the primitive took before claiming.
        let acceptance = context.acceptance().clone();
        let id = rebuild_id(&acceptance.attempt_id);
        let event = context.authorization_event_id();
        if let Err(error) =
            ledger::record_launch_intent(&orch.db.local, &id, intent(&acceptance, event)).await
        {
            // Nothing was staged or launched: settle the attempt directly so the
            // request reopens instead of holding the fence on nothing.
            let document = json!({"rebuild": format!("cairn://rebuilds/{id}"), "detail": format!("the launch intent could not be recorded: {error}"), "launched": false});
            return context
                .report(
                    orch,
                    &receipt_id(&acceptance.attempt_id),
                    ConsumerReport {
                        outcome: RequestAttemptOutcome::Failed,
                        document,
                        settled: true,
                        built_commit: None,
                    },
                )
                .await;
        }
        if let Err(refusal) = self.launch(orch, context, &id).await {
            journal_refusal(&self.host.rebuilds, &id, &refusal)?;
        }
        self.reconcile_locked(orch).await.map(|_| ())
    }

    async fn cancel(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        operator: &PrincipalRef,
    ) -> Result<(), String> {
        let id = rebuild_id(&context.acceptance().attempt_id);
        super::ingest_once(
            &orch.db.local,
            &self.host.rebuilds,
            self.launchd.state().await.state,
        )
        .await?;
        let row = ledger::get(&orch.db.local, &id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("this attempt has not launched a rebuild actor")?;
        if row.outcome.is_some() {
            return Err("the rebuild already has an outcome".into());
        }
        if row
            .launch
            .as_ref()
            .and_then(|launch| launch.bootstrap_requested_at)
            .is_none()
        {
            return Err("this attempt has not launched a rebuild actor".into());
        }
        if row.phases.iter().any(|phase| phase.name == "stop-runner") {
            let phase = row
                .phases
                .last()
                .map(|phase| phase.name.as_str())
                .unwrap_or("stop-runner");
            return Err(format!("cancellation refused during {phase}: the rebuild has passed stop-runner and will finish or roll back by itself"));
        }
        // The actor polls this marker and owns its own stop fence, so a marker
        // that races stop-runner is reported by the actor as too late.
        let marker = self.host.rebuilds.join(&id).join("cancel");
        let content = json!({"requestedAt": chrono::Utc::now().to_rfc3339(), "operator": operator});
        match write_private(&marker, format!("{content}\n").as_bytes(), 0o600) {
            Ok(()) => Ok(()),
            Err(_) if marker.is_file() => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Register the consumer and start its reconciler. Hosts call this once,
/// before the request delivery worker, so no accepted dispatch can find the
/// kind unregistered.
#[cfg(target_os = "macos")]
pub(crate) fn install(orch: &Orchestrator) -> Result<(), String> {
    let consumer = Arc::new(RebuildConsumer::production(orch));
    orch.operator_requests.register(consumer.clone())?;
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(RECONCILE_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(error) = consumer.reconcile(&orch).await {
                tracing::warn!(%error, "rebuild request reconciliation failed");
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests;
