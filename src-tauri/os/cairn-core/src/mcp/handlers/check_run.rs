//! Authenticated manual configured-check producer.
//!
//! This tool is intentionally narrower than `run`: callers select a configured
//! suite and an optional logical coordinate, but cannot supply executable text,
//! identity hashes, environment claims, or a verdict.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Why a caller with no run cannot have an observation.
///
/// This producer's whole value is that its result is *recorded* — keyed to a
/// sealed commit and reusable as review evidence — and the coordinate it records
/// against is the run's job and logical head. A caller with no run has no such
/// coordinate, and inventing one from the process's working directory is exactly
/// the influence [`crate::execution::checks::manual_check_cache_context`]
/// excludes by construction. So the refusal states the contract rather than the
/// missing field: a shell that should have a run and does not has a plumbing
/// problem worth naming, and a shell that legitimately has none is asking for a
/// capability that does not exist yet.
const MISSING_RUN: &str = "cairn check run records an observation against the sealed commit of the run whose work it is, and this request carries no run. Cairn hands every agent shell its run; a shell started outside a run has no job or logical head to record against.";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CheckRunPayload {
    #[serde(default)]
    suite: Option<String>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    observation_handle: Option<String>,
    #[serde(default)]
    retry: bool,
    /// Narrow where the lane runs: a machine name or a platform, nothing else.
    #[serde(default)]
    executor: Option<cairn_common::executor_protocol::ExecutorSelector>,
    /// Who asked for `executor`. Explicit when omitted.
    #[serde(default)]
    selector_source: Option<cairn_common::executor_protocol::CheckSelectorSource>,
}

impl CheckRunPayload {
    /// The validated override this request carries, refused before anything
    /// is queued when it asks for more than a name or a platform.
    fn placement(
        &self,
    ) -> Result<Option<crate::execution::check_placement::CheckPlacementOverride>, String> {
        match (&self.executor, self.selector_source) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(
                "selectorSource names where an executor selector came from, and this request carries none"
                    .into(),
            ),
            (Some(selector), source) => {
                crate::execution::check_placement::CheckPlacementOverride::new(
                    selector.clone(),
                    source.unwrap_or_default(),
                )
                .map(Some)
            }
        }
    }
}

/// `executor` and `sinceUnixMs` are optional so a CLI that predates them reads
/// the diagnostic alone, as it always did.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "state"
)]
enum ManualCheckObservation {
    Queued {
        diagnostic: String,
    },
    Running {
        diagnostic: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        executor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        since_unix_ms: Option<u64>,
    },
    Parked {
        diagnostic: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        executor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        since_unix_ms: Option<u64>,
    },
    Complete {
        result: Box<crate::execution::checks::ManualConfiguredCheckResult>,
    },
    Failed {
        error: String,
    },
}

struct RetainedObservation {
    observation: ManualCheckObservation,
    updated_at: Instant,
}

const SETTLED_OBSERVATION_RETENTION: Duration = Duration::from_secs(10 * 60);

static MANUAL_CHECKS: LazyLock<Arc<Mutex<HashMap<String, RetainedObservation>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

fn is_settled(observation: &ManualCheckObservation) -> bool {
    matches!(
        observation,
        ManualCheckObservation::Complete { .. } | ManualCheckObservation::Failed { .. }
    )
}

fn retain_observation(
    checks: &mut HashMap<String, RetainedObservation>,
    handle: String,
    observation: ManualCheckObservation,
) {
    let now = Instant::now();
    checks.retain(|_, retained| {
        !is_settled(&retained.observation)
            || now.duration_since(retained.updated_at) < SETTLED_OBSERVATION_RETENTION
    });
    checks.insert(
        handle,
        RetainedObservation {
            observation,
            updated_at: now,
        },
    );
}

fn load_observation(handle: &str) -> Result<ManualCheckObservation, String> {
    let mut checks = MANUAL_CHECKS
        .lock()
        .map_err(|_| "manual check observation registry is unavailable".to_string())?;
    let now = Instant::now();
    checks.retain(|_, retained| {
        !is_settled(&retained.observation)
            || now.duration_since(retained.updated_at) < SETTLED_OBSERVATION_RETENTION
    });
    checks
        .get(handle)
        .map(|retained| retained.observation.clone())
        .ok_or_else(|| format!("manual check observation {handle} is unknown or expired"))
}

fn registry_key(project_id: &str, handle: &str) -> String {
    format!("{project_id}/{handle}")
}

/// A terminal reply preserves request-specific facts (notably the evaluated
/// commit on a cache hit) while its foreign key names the immutable source
/// observation. Never search for the latest verdict at the same coordinate.
async fn persist_terminal_observation(
    db: &crate::storage::LocalDb,
    project_id: &str,
    handle: &str,
    observation: &ManualCheckObservation,
) -> Result<(), String> {
    let observation_id = match observation {
        ManualCheckObservation::Complete { result } => result.observation_id.as_deref(),
        _ => None,
    };
    let response = serde_json::to_string(observation).map_err(|error| error.to_string())?;
    let changed = db
        .execute(
            "UPDATE manual_check_handles SET observation_id=?3,response_json=?4
         WHERE project_id=?1 AND handle=?2 AND response_json IS NULL
           AND (?3 IS NULL OR EXISTS (
             SELECT 1 FROM check_result_observations WHERE project_id=?1 AND id=?3))",
            cairn_db::turso::params![project_id, handle, observation_id, response],
        )
        .await
        .map_err(|error| format!("persist manual check observation {handle}: {error}"))?;
    if changed != 1 {
        return Err(format!("persist manual check observation {handle}: handle or project-owned source observation is missing, or the handle already settled"));
    }
    Ok(())
}

async fn recover_observation(
    orch: &Orchestrator,
    db: Arc<crate::storage::LocalDb>,
    project_id: &str,
    handle: &str,
) -> Result<ManualCheckObservation, String> {
    // Authenticate against durable ownership even while the progress cache is
    // warm: an opaque UUID is not authority to read another project's result.
    let row = db
        .query_opt(
            "SELECT h.response_json, COALESCE(h.observation_id,p.id),
                CASE WHEN h.observation_id IS NOT NULL
                     THEN COALESCE(o.project_id=h.project_id,0)
                     ELSE p.id IS NOT NULL END
         FROM manual_check_handles h
         LEFT JOIN check_result_observations o ON o.id=h.observation_id
         LEFT JOIN check_result_observations p
           ON p.project_id=h.project_id AND p.publication_id=h.publication_id
             AND p.publication_id IS NOT NULL
         WHERE h.project_id=?1 AND h.handle=?2",
            cairn_db::turso::params![project_id, handle],
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.i64(2)? != 0)),
        )
        .await
        .map_err(|error| format!("load durable manual check observation {handle}: {error}"))?
        .ok_or_else(|| format!("manual check observation {handle} is unknown in this project"))?;
    if row.1.is_some() && !row.2 {
        return Err(format!(
            "manual check observation {handle} has no project-owned source observation"
        ));
    }
    if let Some(response) = row.0 {
        return serde_json::from_str(&response)
            .map_err(|error| format!("decode durable manual check observation {handle}: {error}"));
    }
    if row.1.is_some() {
        return crate::execution::checks::recover_manual_check_result(orch, db, project_id, handle)
            .await
            .map(|result| ManualCheckObservation::Complete {
                result: Box::new(result),
            });
    }
    if let Ok(observation) = load_observation(&registry_key(project_id, handle)) {
        return Ok(observation);
    }
    Ok(ManualCheckObservation::Failed {
        error: "manual check was interrupted before its terminal answer was persisted; no recorded answer is available for this handle".into(),
    })
}

/// One request handle's answer as JSON, through the same resolver and project
/// fence `cairn check run` polls with, so the `check-observations` resource
/// renders a request handle without a second account of its states.
pub(crate) async fn request_observation_value(
    orch: &Orchestrator,
    db: Arc<crate::storage::LocalDb>,
    project_id: &str,
    handle: &str,
) -> Result<serde_json::Value, String> {
    let observation = recover_observation(orch, db, project_id, handle).await?;
    serde_json::to_value(&observation)
        .map_err(|error| format!("encode manual check observation {handle}: {error}"))
}

/// Records in-memory progress for `handle` as a running request would, for
/// tests outside this module that read a pending request.
#[cfg(test)]
pub(crate) fn record_progress_for_test(
    project_id: &str,
    handle: &str,
    update: crate::execution::checks::ManualCheckProgressUpdate,
) {
    retain_observation(
        &mut MANUAL_CHECKS.lock().unwrap(),
        registry_key(project_id, handle),
        observation_from_progress(update),
    );
}

fn observation_from_progress(
    update: crate::execution::checks::ManualCheckProgressUpdate,
) -> ManualCheckObservation {
    let crate::execution::checks::ManualCheckProgressUpdate {
        state,
        diagnostic,
        executor,
        since_unix_ms,
    } = update;
    match state.as_str() {
        "parked" => ManualCheckObservation::Parked {
            diagnostic,
            executor,
            since_unix_ms,
        },
        "queued" => ManualCheckObservation::Queued { diagnostic },
        _ => ManualCheckObservation::Running {
            diagnostic,
            executor,
            since_unix_ms,
        },
    }
}

type ManualProgressSink =
    Arc<dyn Fn(crate::execution::checks::ManualCheckProgressUpdate) + Send + Sync>;

/// Run one manual check to its end in a task of its own, recording each step
/// under `handle`.
///
/// The requester's callback returns before this ends and holds nothing it
/// could cancel: a requester that stops polling, by its own timeout or by being
/// killed, leaves the run going and its observation settling, and the row the
/// run records is unaffected.
fn spawn_manual_observation<F>(
    db: Arc<crate::storage::LocalDb>,
    project_id: String,
    handle: String,
    run: impl FnOnce(ManualProgressSink) -> F,
) where
    F: std::future::Future<
            Output = Result<crate::execution::checks::ManualConfiguredCheckResult, String>,
        > + Send
        + 'static,
{
    let key = registry_key(&project_id, &handle);
    let progress_handle = key.clone();
    let progress: ManualProgressSink = Arc::new(move |update| {
        let next = observation_from_progress(update);
        if let Ok(mut checks) = MANUAL_CHECKS.lock() {
            retain_observation(&mut checks, progress_handle.clone(), next);
        }
    });
    let running = run(progress);
    tokio::spawn(async move {
        let mut final_state = match running.await {
            Ok(result) => ManualCheckObservation::Complete {
                result: Box::new(result),
            },
            Err(error) => ManualCheckObservation::Failed { error },
        };
        if let Err(error) =
            persist_terminal_observation(&db, &project_id, &handle, &final_state).await
        {
            log::error!("{error}");
            match &mut final_state {
                ManualCheckObservation::Complete { result } => {
                    result.not_recorded = Some(match result.not_recorded.take() {
                        Some(cause) => format!("{cause}; {error}"),
                        None => error,
                    });
                }
                ManualCheckObservation::Failed { error: cause } => {
                    *cause = format!("{cause}; {error}");
                }
                _ => {}
            }
        }
        if let Ok(mut checks) = MANUAL_CHECKS.lock() {
            retain_observation(&mut checks, key, final_state);
        }
    });
}

pub(crate) async fn handle_check_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let result = async {
        let run_id = request
            .run_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| MISSING_RUN.to_string())?;
        let payload: CheckRunPayload = serde_json::from_value(request.payload.clone())
            .map_err(|error| format!("invalid check_run payload: {error}"))?;
        let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .map_err(|error| error.to_string())?;
        let (project_id, project_key) = db
            .query_one(
                "SELECT r.project_id,p.key FROM runs r JOIN projects p ON p.id=r.project_id WHERE r.id=?1",
                cairn_db::turso::params![run_id],
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| format!("resolve manual check request project: {error}"))?;
        if let Some(handle) = payload.observation_handle.as_deref() {
            return recover_observation(orch, db, &project_id, handle)
                .await
                .map(|observation| (None, observation));
        }
        let suite = payload
            .suite
            .as_deref()
            .filter(|suite| !suite.trim().is_empty())
            .ok_or_else(|| "configured suite name must not be empty".to_string())?;
        let placement = payload.placement()?;
        // Resolved, and refused, before a handle exists: a caller told its check
        // was queued must not then learn the revision never named anything. The
        // commit, not the name, is what the run evaluates, so a branch that moves
        // after this point does not change which revision the answer is about.
        let commit = match payload.branch.as_deref() {
            Some(branch) => Some(
                crate::execution::checks::resolve_manual_check_revision(orch, run_id, branch)
                    .await?,
            ),
            None => None,
        };
        let handle = uuid::Uuid::new_v4().to_string();
        db.execute(
            "INSERT INTO manual_check_handles(handle,project_id,created_at) VALUES(?1,?2,?3)",
            cairn_db::turso::params![
                handle.as_str(),
                project_id.as_str(),
                chrono::Utc::now().timestamp_millis()
            ],
        )
        .await
        .map_err(|error| format!("persist manual check handle {handle}: {error}"))?;
        retain_observation(
            &mut *MANUAL_CHECKS
                .lock()
                .map_err(|_| "manual check observation registry is unavailable".to_string())?,
            registry_key(&project_id, &handle),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        );
        let task_orch = orch.clone();
        let task_run_id = run_id.to_string();
        let task_suite = suite.to_string();
        let task_branch = commit;
        let task_retry = payload.retry;
        let task_handle = handle.clone();
        spawn_manual_observation(db, project_id, handle.clone(), move |progress| async move {
            crate::execution::checks::run_manual_configured_check_with_progress(
                &task_orch,
                &task_run_id,
                crate::execution::checks::ManualCheckRequest {
                    check_name: &task_suite,
                    branch: task_branch.as_deref(),
                    placement,
                    retry: task_retry,
                    manual_handle: Some(task_handle.as_str()),
                },
                progress,
            )
            .await
        });
        Ok((
            Some((handle.clone(), format!("cairn://p/{project_key}/check-observations/{handle}"))),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        ))
    }
    .await;

    match result {
        Ok((Some((handle, reference)), observation)) => serde_json::json!({
            "ok": true, "observationHandle": handle, "observationRef": reference, "observation": observation
        })
        .to_string(),
        Ok((None, observation)) => {
            serde_json::json!({ "ok": true, "observation": observation }).to_string()
        }
        Err(error) => serde_json::json!({ "ok": false, "error": error }).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_accepts_only_suite_and_optional_branch() {
        let payload: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "branch": "main"
        }))
        .unwrap();
        assert_eq!(payload.suite.as_deref(), Some("rust-tests"));
        assert_eq!(payload.branch.as_deref(), Some("main"));

        let asserted_command = serde_json::from_value::<CheckRunPayload>(serde_json::json!({
            "suite": "rust-tests",
            "command": "cargo test"
        }));
        assert!(
            asserted_command.is_err(),
            "raw commands must not cross the trusted producer boundary"
        );
    }

    #[test]
    fn payload_carries_a_placement_selector_and_its_source() {
        let payload: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "executor": { "os": "linux" },
            "selectorSource": "inherited"
        }))
        .unwrap();
        let placement = payload.placement().unwrap().unwrap();
        assert_eq!(placement.selector().os.as_deref(), Some("linux"));
        assert_eq!(
            placement.source(),
            cairn_common::executor_protocol::CheckSelectorSource::Inherited
        );

        let defaulted: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "executor": { "name": "fedora" }
        }))
        .unwrap();
        assert_eq!(
            defaulted.placement().unwrap().unwrap().source(),
            cairn_common::executor_protocol::CheckSelectorSource::Explicit
        );

        let capability: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "executor": { "os": "linux", "requiredToolchains": ["rust"] }
        }))
        .unwrap();
        assert!(
            capability.placement().is_err(),
            "capability is the configuration's to declare"
        );

        let orphan: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "selectorSource": "explicit"
        }))
        .unwrap();
        assert!(orphan.placement().is_err());
    }

    #[test]
    fn parked_observation_is_a_first_class_state() {
        let value = serde_json::to_value(ManualCheckObservation::Parked {
            diagnostic: "parked behind frontend-build".into(),
            executor: None,
            since_unix_ms: None,
        })
        .unwrap();
        assert_eq!(value["state"], "parked");
        assert_eq!(value["diagnostic"], "parked behind frontend-build");
        assert!(
            value.get("executor").is_none() && value.get("sinceUnixMs").is_none(),
            "absent facts are omitted, not null: {value}"
        );
    }

    /// The machine and the claim time reach the requester, under the names
    /// the CLI reads.
    #[test]
    fn running_observation_names_its_machine_and_since() {
        let value = serde_json::to_value(observation_from_progress(
            crate::execution::checks::ManualCheckProgressUpdate {
                state: "running".into(),
                diagnostic: "running on fedora since 09:48:09Z".into(),
                executor: Some("fedora".into()),
                since_unix_ms: Some(1_792_403_289_000),
            },
        ))
        .unwrap();
        assert_eq!(value["state"], "running");
        assert_eq!(value["diagnostic"], "running on fedora since 09:48:09Z");
        assert_eq!(value["executor"], "fedora");
        assert_eq!(value["sinceUnixMs"], 1_792_403_289_000_u64);
    }

    #[test]
    fn preparing_reads_as_queued_and_parked_as_parked() {
        let update = |state: &str| crate::execution::checks::ManualCheckProgressUpdate {
            state: state.into(),
            diagnostic: "d".into(),
            executor: None,
            since_unix_ms: Some(1),
        };
        assert!(matches!(
            observation_from_progress(update("queued")),
            ManualCheckObservation::Queued { .. }
        ));
        assert!(matches!(
            observation_from_progress(update("parked")),
            ManualCheckObservation::Parked {
                since_unix_ms: Some(1),
                ..
            }
        ));
    }

    /// The manual check runs in a task its requester does not own. A requester
    /// that stops polling, by timeout or by being killed, neither cancels it
    /// nor keeps it from settling: the observation moves to running and then
    /// to its end state with no poll at all.
    #[tokio::test]
    async fn an_observation_settles_without_its_requester_polling() {
        let (_root, orch) = manual_fixture().await;
        let handle = format!("unpolled-{}", uuid::Uuid::new_v4());
        insert_handle(&orch.db.local, &handle).await;
        let key = registry_key("p", &handle);
        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        spawn_manual_observation(
            orch.db.local.clone(),
            "p".into(),
            handle.clone(),
            |progress| async move {
                progress(crate::execution::checks::ManualCheckProgressUpdate {
                    state: "running".into(),
                    diagnostic: "running on fedora since 09:48:09Z".into(),
                    executor: Some("fedora".into()),
                    since_unix_ms: Some(1_792_403_289_000),
                });
                let _ = finished.await;
                Err("settled with nobody polling".to_string())
            },
        );
        let observed = |handle: &str| {
            MANUAL_CHECKS
                .lock()
                .unwrap()
                .get(handle)
                .map(|retained| retained.observation.clone())
        };
        let until = |wanted: fn(&ManualCheckObservation) -> bool| {
            let handle = key.clone();
            tokio::time::timeout(Duration::from_secs(5), async move {
                while !observed(&handle).as_ref().is_some_and(wanted) {
                    tokio::task::yield_now().await;
                }
            })
        };
        until(|observation| matches!(observation, ManualCheckObservation::Running { .. }))
            .await
            .expect("the run reported its progress on its own");
        finish.send(()).unwrap();
        until(|observation| matches!(observation, ManualCheckObservation::Failed { .. }))
            .await
            .expect("the run settled its observation with nobody polling it");
    }

    const AGENT_BRANCH: &str = "agent/proj-7-builder-0";
    const NODE_URI: &str = "cairn://p/proj/7/1/builder";
    const FOREIGN_NODE_URI: &str = "cairn://p/other/8/1/builder";

    /// A project whose managed store holds `main` and one agent branch a commit
    /// ahead of it, a builder node recording that branch, and a run of it:
    /// everything `--branch` can name. A second project's node records the
    /// same branch name, so a node URI resolving across projects would be
    /// indistinguishable from one resolving correctly.
    struct BranchFixture {
        _root: tempfile::TempDir,
        orch: Orchestrator,
        main: String,
        agent: String,
    }

    async fn branch_fixture(bin: &str) -> BranchFixture {
        use crate::jj::testing::{git, git_stdout, init_project};
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_project(&repo);
        let main = git_stdout(&repo, &["rev-parse", "HEAD"]);
        git(&repo, &["checkout", "-q", "-b", AGENT_BRANCH]);
        std::fs::write(repo.join("shared.rs"), "agent\n").unwrap();
        git(&repo, &["commit", "-q", "-am", "agent work"]);
        let agent = git_stdout(&repo, &["rev-parse", "HEAD"]);
        git(&repo, &["checkout", "-q", "main"]);

        let db = crate::storage::migrated_test_db("check-run-branch.db").await;
        let repo_path = repo.to_string_lossy();
        db.execute_script(format!(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
               VALUES('p','w','P','proj','{repo_path}','main',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
               VALUES('q','w','Q','other','{repo_path}','main',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',7,'I','active','active','none',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('k','q',8,'K','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('e','r','i','p','running',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('f','r','k','q','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
               VALUES('j','p','i','e','running','builder','builder','{AGENT_BRANCH}','main',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,branch,base_branch,created_at,updated_at)
               VALUES('m','q','k','f','running','builder','builder','{AGENT_BRANCH}','main',1,1);
             INSERT INTO runs(id,issue_id,project_id,job_id,status,created_at,updated_at)
               VALUES('run-1','i','p','j','running',1,1);"
        ))
        .await
        .unwrap();

        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(config_dir.join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(Arc::new(db), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            config_dir,
        )
        .jj_binary_path(bin.to_string())
        .build();
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
        crate::jj::ensure_project_store(&jj, &store, &repo).unwrap();
        BranchFixture {
            _root: root,
            orch,
            main,
            agent,
        }
    }

    fn check_run_request(branch: &str) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: Some("run-1".into()),
            tool: "check_run".into(),
            payload: serde_json::json!({ "suite": "rust-tests", "branch": branch }),
            tool_use_id: None,
            origin: None,
        }
    }

    /// `--branch` takes every form `read ?branch=` and `run branch:` take, and
    /// each lands on the commit that form names.
    #[tokio::test]
    async fn branch_takes_a_branch_name_a_commit_id_and_a_node_uri() {
        let Some(bin) = crate::jj::testing::jj_bin() else {
            return;
        };
        let fixture = branch_fixture(&bin).await;
        for (requested, expected) in [
            ("main", &fixture.main),
            (AGENT_BRANCH, &fixture.agent),
            (fixture.agent.as_str(), &fixture.agent),
            (&fixture.agent[..12], &fixture.agent),
            (NODE_URI, &fixture.agent),
        ] {
            let resolved = crate::execution::checks::resolve_manual_check_revision(
                &fixture.orch,
                "run-1",
                requested,
            )
            .await
            .unwrap_or_else(|error| panic!("{requested:?} did not resolve: {error}"));
            assert_eq!(&resolved, expected, "{requested:?}");
        }
    }

    /// A revision that names nothing is refused before an observation exists,
    /// so the caller never sees a queued check turn into a failure, and the
    /// refusal names the forms `--branch` takes.
    #[tokio::test]
    async fn an_unresolvable_branch_is_refused_before_anything_is_queued() {
        let Some(bin) = crate::jj::testing::jj_bin() else {
            return;
        };
        let fixture = branch_fixture(&bin).await;
        for requested in ["agent/proj-7-builder-1", "cairn://p/proj/7/1/coordinator"] {
            let response: serde_json::Value = serde_json::from_str(
                &handle_check_run(&fixture.orch, &check_run_request(requested)).await,
            )
            .unwrap();
            assert_eq!(response["ok"], false, "{requested:?}: {response}");
            assert!(
                response.get("observationHandle").is_none(),
                "{requested:?} minted an observation before it was refused: {response}"
            );
            let error = response["error"].as_str().unwrap();
            assert!(error.contains(requested), "{error}");
            for form in ["branch name", "commit id", "node URI"] {
                assert!(
                    error.contains(form),
                    "{requested:?} omits {form:?}: {error}"
                );
            }
        }
    }

    /// A node of another project is refused rather than resolved, even when
    /// its branch name happens to exist here.
    #[tokio::test]
    async fn a_node_of_another_project_is_refused() {
        let Some(bin) = crate::jj::testing::jj_bin() else {
            return;
        };
        let fixture = branch_fixture(&bin).await;
        let error = crate::execution::checks::resolve_manual_check_revision(
            &fixture.orch,
            "run-1",
            FOREIGN_NODE_URI,
        )
        .await
        .unwrap_err();
        assert!(error.contains("another project"), "{error}");
    }

    async fn manual_fixture() -> (tempfile::TempDir, Orchestrator) {
        let db = crate::storage::migrated_test_db("manual-handle-recovery.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/unused',1,1),('q','w','Q','other','/unused',1,1);
             INSERT INTO runs(id,project_id,status,created_at,updated_at)
               VALUES('run-1','p','running',1,1),('run-other','q','running',1,1);
             INSERT INTO check_result_observations(id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,cadence,output_tail,public_handle,verdict_result_json)
               VALUES('source','p','source-commit','source-tree','rust-tests','input','env',0,'passed',1,1,1,1,1,1,'manual','source output','source-public',
                 '{\"parser\":\"nextest\",\"complete\":true,\"selection\":\"fast\",\"passed\":9,\"failed\":0,\"skipped\":0,\"deferred\":3,\"failures\":[]}');"
        ).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(Arc::new(db), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        (root, orch)
    }

    async fn insert_handle(db: &crate::storage::LocalDb, handle: &str) {
        db.execute(
            "INSERT INTO manual_check_handles(handle,project_id,created_at) VALUES(?1,'p',1)",
            cairn_db::turso::params![handle],
        )
        .await
        .unwrap();
    }

    async fn poll_handle(orch: &Orchestrator, handle: &str, run: &str) -> serde_json::Value {
        let mut request = check_run_request("main");
        request.run_id = Some(run.into());
        request.payload = serde_json::json!({"observationHandle":handle});
        serde_json::from_str(&handle_check_run(orch, &request).await).unwrap()
    }

    fn specimen(disposition: &str) -> ManualCheckObservation {
        let mut result: crate::execution::checks::ManualConfiguredCheckResult =
            serde_json::from_value(serde_json::json!({
                "checkName":"rust-tests","commitSha":"evaluated-commit","treeHash":"evaluated-tree",
                "inputHash":"input","environmentFingerprint":"env",
                "observationRef":"cairn://p/proj/check-observations/source",
                "ranAt":"1970-01-01 00:00:00 UTC","reusable":true,"notRecorded":null,
                "disposition":disposition,"passed":true,"verified":true,"unverifiedReason":null,
                "tier":"fast tier: 9 ran, 3 deferred","exitCode":0,"noVerdict":null,
                "outputTail":"source output","executor":{"name":"machine","os":"linux"},
                "verdictPlatform":"linux","runtime":"bun 1.4.4","placementNote":null,"placementUnmet":null
            })).unwrap();
        result.observation_id = Some("source".into());
        ManualCheckObservation::Complete {
            result: Box::new(result),
        }
    }

    #[tokio::test]
    async fn durable_manual_handle_recovers_without_memory() {
        let (_root, orch) = manual_fixture().await;
        for disposition in ["fresh", "cached"] {
            // Multiple handles may coalesce onto the same immutable source.
            for _ in 0..2 {
                let handle = uuid::Uuid::new_v4().to_string();
                insert_handle(&orch.db.local, &handle).await;
                let expected = specimen(disposition);
                persist_terminal_observation(&orch.db.local, "p", &handle, &expected)
                    .await
                    .unwrap();
                let response = poll_handle(&orch, &handle, "run-1").await;
                assert_eq!(response["ok"], true, "{response}");
                assert_eq!(
                    response["observation"],
                    serde_json::to_value(&expected).unwrap()
                );
                let foreign = poll_handle(&orch, &handle, "run-other").await;
                assert_eq!(foreign["ok"], false, "{foreign}");
            }
        }
    }

    #[tokio::test]
    async fn attached_ledger_source_recovers_when_receipt_and_memory_are_absent() {
        let (_root, orch) = manual_fixture().await;
        for disposition in ["fresh", "cached", "fresh"] {
            let handle = uuid::Uuid::new_v4().to_string();
            insert_handle(&orch.db.local, &handle).await;
            let context = serde_json::json!({
                "project_key":"proj",
                "placement":{"selector":null,"verdict_platforms":[],"executor_name":null,"note":null},
                "runtime_instruments":[],
            }).to_string();
            orch.db
                .local
                .execute(
                    "UPDATE manual_check_handles SET context_json=?2 WHERE handle=?1",
                    cairn_db::turso::params![handle.as_str(), context],
                )
                .await
                .unwrap();
            crate::execution::cache::attach_manual_check_observation(
                &orch.db.local,
                "p",
                &handle,
                "source",
                "evaluated-commit",
                "evaluated-tree",
                "input",
                disposition,
            )
            .await
            .unwrap();
            let response = poll_handle(&orch, &handle, "run-1").await;
            assert_eq!(response["ok"], true, "{response}");
            let result = &response["observation"]["result"];
            assert_eq!(result["commitSha"], "evaluated-commit");
            assert_eq!(result["treeHash"], "evaluated-tree");
            assert_eq!(result["disposition"], disposition);
            assert_eq!(result["outputTail"], "source output");
            assert_eq!(
                result["observationRef"],
                "cairn://p/proj/check-observations/source-public"
            );
            assert_eq!(result["passed"], true);
            assert_eq!(result["verified"], true);
            assert_eq!(result["tier"], "fast tier: 9 ran, 3 deferred");
            assert_eq!(poll_handle(&orch, &handle, "run-other").await["ok"], false);
        }
    }

    /// A recovered reply judges a green that carries no structured result by
    /// the command the request ran, exactly as the live reply did: a test
    /// runner's stays unverified, a build's is verified, and a request recorded
    /// before its command was kept is held to the test runner's bar.
    #[tokio::test]
    async fn recovery_keeps_a_parse_less_test_runner_green_unverified() {
        let (_root, orch) = manual_fixture().await;
        orch.db.local.execute_script(
            "INSERT INTO check_result_observations(id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,cadence,output_tail,public_handle,verdict_result_json)
               VALUES('runner-source','p','c','t','rust-tests','runner-input','env',0,'passed',1,0,1,1,2,1,'manual','no summary','runner-public',NULL),
                     ('build-source','p','c','t','frontend-build','build-input','env',0,'passed',1,1,1,1,3,1,'manual','built','build-public',NULL);"
        ).await.unwrap();
        for (source, command, verified) in [
            ("runner-source", Some("bun run test:rust"), false),
            ("runner-source", None, false),
            ("build-source", Some("bunx --bun vite build"), true),
        ] {
            let handle = uuid::Uuid::new_v4().to_string();
            insert_handle(&orch.db.local, &handle).await;
            let mut context = serde_json::json!({
                "project_key":"proj",
                "placement":{"selector":null,"verdict_platforms":[],"executor_name":null,"note":null},
                "runtime_instruments":[],
            });
            if let Some(command) = command {
                context["command"] = command.into();
            }
            orch.db
                .local
                .execute(
                    "UPDATE manual_check_handles SET context_json=?2 WHERE handle=?1",
                    cairn_db::turso::params![handle.as_str(), context.to_string()],
                )
                .await
                .unwrap();
            crate::execution::cache::attach_manual_check_observation(
                &orch.db.local,
                "p",
                &handle,
                source,
                "c",
                "t",
                "input",
                "fresh",
            )
            .await
            .unwrap();
            let response = poll_handle(&orch, &handle, "run-1").await;
            assert_eq!(response["ok"], true, "{response}");
            let result = &response["observation"]["result"];
            assert_eq!(result["passed"], true, "{command:?}: {result}");
            assert_eq!(result["verified"], verified, "{command:?}: {result}");
            assert_eq!(result["reusable"], verified, "{command:?}: {result}");
            assert_eq!(
                result["observationRef"],
                format!(
                    "cairn://p/proj/check-observations/{}",
                    source.replace("-source", "-public")
                )
            );
            if !verified {
                assert_eq!(
                    result["unverifiedReason"],
                    "the command produced no structured coverage result",
                    "{command:?}: {result}"
                );
            }
        }
    }

    #[tokio::test]
    async fn publication_only_recovery_does_not_confuse_same_coordinate_platform_groups() {
        let (_root, orch) = manual_fixture().await;
        for (source, publication, platform, passed) in [
            ("linux-source", "publication-linux", "linux", true),
            ("macos-source", "publication-macos", "macos", false),
        ] {
            orch.db.local.execute(
                "INSERT INTO check_result_observations(id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,cadence,output_tail,public_handle,verdict_result_json,publication_id,verdict_platform)
                 SELECT ?1,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,
                        ?4,?5,complete,reusable,parser_version,result_schema_version,ran_at+?4+1,
                        duration_ms,cadence,?1,?1 || '-public',verdict_result_json,?2,?3
                 FROM check_result_observations WHERE id='source'",
                cairn_db::turso::params![source,publication,platform,if passed {0} else {1},if passed {"passed"} else {"failed"}],
            ).await.unwrap();
        }
        // Both publishers have recorded, but neither follower has attached the
        // direct source or saved its terminal reply before the simulated restart.
        for (source, publication, platform, passed) in [
            ("linux-source", "publication-linux", "linux", true),
            ("macos-source", "publication-macos", "macos", false),
        ] {
            let handle = uuid::Uuid::new_v4().to_string();
            insert_handle(&orch.db.local, &handle).await;
            orch.db.local.execute(
                "UPDATE manual_check_handles SET publication_id=?2,commit_sha='requested-commit',
                 tree_hash='requested-tree',input_hash='input',disposition='fresh',context_json=?3 WHERE handle=?1",
                cairn_db::turso::params![handle.as_str(),publication,serde_json::json!({
                    "project_key":"proj",
                    "placement":{"selector":null,"verdict_platforms":[],"executor_name":null,"note":null},
                    "runtime_instruments":[],
                }).to_string()],
            ).await.unwrap();
            let response = poll_handle(&orch, &handle, "run-1").await;
            assert_eq!(response["ok"], true, "{response}");
            let result = &response["observation"]["result"];
            assert_eq!(result["commitSha"], "requested-commit");
            assert_eq!(result["treeHash"], "requested-tree");
            assert_eq!(result["passed"], passed);
            assert_eq!(result["outputTail"], source);
            assert_eq!(result["verdictPlatform"], platform);
            assert_eq!(
                result["observationRef"],
                format!("cairn://p/proj/check-observations/{source}-public")
            );
            assert_eq!(poll_handle(&orch, &handle, "run-other").await["ok"], false);
        }
    }

    #[tokio::test]
    async fn fast_and_slow_checks_recover_past_retention() {
        let (_root, orch) = manual_fixture().await;
        let old = Instant::now() - SETTLED_OBSERVATION_RETENTION - Duration::from_secs(1);
        for slow in [false, true] {
            let handle = uuid::Uuid::new_v4().to_string();
            insert_handle(&orch.db.local, &handle).await;
            let key = registry_key("p", &handle);
            MANUAL_CHECKS.lock().unwrap().insert(
                key.clone(),
                RetainedObservation {
                    observation: ManualCheckObservation::Running {
                        diagnostic: "still running".into(),
                        executor: None,
                        since_unix_ms: None,
                    },
                    updated_at: if slow { old } else { Instant::now() },
                },
            );
            assert_eq!(
                poll_handle(&orch, &handle, "run-1").await["observation"]["state"],
                "running"
            );
            assert_eq!(poll_handle(&orch, &handle, "run-other").await["ok"], false);
            let expected = specimen("fresh");
            persist_terminal_observation(&orch.db.local, "p", &handle, &expected)
                .await
                .unwrap();
            MANUAL_CHECKS.lock().unwrap().insert(
                key.clone(),
                RetainedObservation {
                    observation: expected.clone(),
                    updated_at: old,
                },
            );
            // Force the same pruning used by normal registry lookup.
            assert!(load_observation(&key).is_err());
            let response = poll_handle(&orch, &handle, "run-1").await;
            assert_eq!(
                response["observation"],
                serde_json::to_value(expected).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn spawned_fast_check_survives_retention_while_a_slow_check_finishes() {
        let (_root, orch) = manual_fixture().await;
        let fast = uuid::Uuid::new_v4().to_string();
        let slow = uuid::Uuid::new_v4().to_string();
        insert_handle(&orch.db.local, &fast).await;
        insert_handle(&orch.db.local, &slow).await;
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        spawn_manual_observation(orch.db.local.clone(), "p".into(), fast.clone(), |_| async {
            let ManualCheckObservation::Complete { result } = specimen("fresh") else {
                unreachable!()
            };
            Ok(*result)
        });
        spawn_manual_observation(
            orch.db.local.clone(),
            "p".into(),
            slow.clone(),
            |progress| async move {
                progress(crate::execution::checks::ManualCheckProgressUpdate {
                    state: "running".into(),
                    diagnostic: "slow suite".into(),
                    executor: None,
                    since_unix_ms: None,
                });
                released.await.unwrap();
                let ManualCheckObservation::Complete { result } = specimen("fresh") else {
                    unreachable!()
                };
                Ok(*result)
            },
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while poll_handle(&orch, &fast, "run-1").await["observation"]["state"] != "complete" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        {
            let mut checks = MANUAL_CHECKS.lock().unwrap();
            checks
                .get_mut(&registry_key("p", &fast))
                .unwrap()
                .updated_at =
                Instant::now() - SETTLED_OBSERVATION_RETENTION - Duration::from_secs(1);
        }
        assert_eq!(
            poll_handle(&orch, &slow, "run-1").await["observation"]["state"],
            "running"
        );
        assert!(!MANUAL_CHECKS
            .lock()
            .unwrap()
            .contains_key(&registry_key("p", &fast)));
        assert_eq!(
            poll_handle(&orch, &fast, "run-1").await["observation"]["state"],
            "complete"
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while poll_handle(&orch, &slow, "run-1").await["observation"]["state"] != "complete" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        MANUAL_CHECKS
            .lock()
            .unwrap()
            .remove(&registry_key("p", &slow));
        assert_eq!(
            poll_handle(&orch, &slow, "run-1").await["observation"]["state"],
            "complete"
        );
    }

    #[tokio::test]
    async fn receipt_write_failure_preserves_both_recorded_and_unrecorded_verdicts() {
        let (_root, orch) = manual_fixture().await;
        orch.db.local.execute_batch(
            "CREATE TRIGGER reject_manual_receipt BEFORE UPDATE OF response_json ON manual_check_handles
             BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END;"
        ).await.unwrap();
        for attached in [false, true] {
            let handle = uuid::Uuid::new_v4().to_string();
            insert_handle(&orch.db.local, &handle).await;
            if attached {
                orch.db.local.execute(
                    "UPDATE manual_check_handles SET observation_id='source',commit_sha='evaluated-commit',
                     tree_hash='evaluated-tree',input_hash='input',disposition='fresh',context_json=?2 WHERE handle=?1",
                    cairn_db::turso::params![handle.as_str(), serde_json::json!({
                        "project_key":"proj",
                        "placement":{"selector":null,"verdict_platforms":[],"executor_name":null,"note":null},
                        "runtime_instruments":[],
                    }).to_string()],
                ).await.unwrap();
            }
            let ManualCheckObservation::Complete { mut result } = specimen("fresh") else {
                unreachable!()
            };
            if !attached {
                result.observation_id = None;
                result.observation_ref = None;
                result.not_recorded = Some("original recording failure".into());
            }
            spawn_manual_observation(
                orch.db.local.clone(),
                "p".into(),
                handle.clone(),
                |_| async move { Ok(*result) },
            );
            let key = registry_key("p", &handle);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let settled = MANUAL_CHECKS
                        .lock()
                        .unwrap()
                        .get(&key)
                        .is_some_and(|value| is_settled(&value.observation));
                    if settled {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let response = poll_handle(&orch, &handle, "run-1").await;
            assert_eq!(response["observation"]["state"], "complete", "{response}");
            assert_eq!(
                response["observation"]["result"]["passed"], true,
                "{response}"
            );
            assert_eq!(
                response["observation"]["result"]["outputTail"],
                "source output"
            );
            if !attached {
                let cause = response["observation"]["result"]["notRecorded"]
                    .as_str()
                    .unwrap();
                assert!(cause.contains("original recording failure"), "{cause}");
                assert!(cause.contains("injected receipt failure"), "{cause}");
            } else {
                MANUAL_CHECKS.lock().unwrap().remove(&key);
                assert_eq!(
                    poll_handle(&orch, &handle, "run-1").await["observation"]["state"],
                    "complete"
                );
            }
        }
    }

    #[tokio::test]
    async fn durable_errors_and_interrupted_checks_are_explicit() {
        let (_root, orch) = manual_fixture().await;
        let handle = uuid::Uuid::new_v4().to_string();
        insert_handle(&orch.db.local, &handle).await;
        let response = poll_handle(&orch, &handle, "run-1").await;
        assert!(response["observation"]["error"]
            .as_str()
            .unwrap()
            .contains("interrupted"));
        let failed = ManualCheckObservation::Failed {
            error: "runner refused".into(),
        };
        persist_terminal_observation(&orch.db.local, "p", &handle, &failed)
            .await
            .unwrap();
        assert_eq!(
            poll_handle(&orch, &handle, "run-1").await["observation"]["error"],
            "runner refused"
        );
        let error = persist_terminal_observation(&orch.db.local, "q", &handle, &specimen("fresh"))
            .await
            .unwrap_err();
        assert!(
            error.contains("persist manual check observation"),
            "{error}"
        );
        orch.db
            .local
            .execute_batch(
                "CREATE TRIGGER reject_manual_handle_settle BEFORE UPDATE ON manual_check_handles
             BEGIN SELECT RAISE(ABORT, 'injected persistence failure'); END;",
            )
            .await
            .unwrap();
        let another = uuid::Uuid::new_v4().to_string();
        insert_handle(&orch.db.local, &another).await;
        let error = persist_terminal_observation(&orch.db.local, "p", &another, &specimen("fresh"))
            .await
            .unwrap_err();
        assert!(error.contains("injected persistence failure"), "{error}");
    }

    #[test]
    fn pruning_expires_only_settled_observations() {
        let old = Instant::now() - SETTLED_OBSERVATION_RETENTION - Duration::from_secs(1);
        let mut checks = HashMap::from([
            (
                "active".into(),
                RetainedObservation {
                    observation: ManualCheckObservation::Parked {
                        diagnostic: "waiting".into(),
                        executor: None,
                        since_unix_ms: None,
                    },
                    updated_at: old,
                },
            ),
            (
                "settled".into(),
                RetainedObservation {
                    observation: ManualCheckObservation::Failed {
                        error: "done".into(),
                    },
                    updated_at: old,
                },
            ),
        ]);
        retain_observation(
            &mut checks,
            "new".into(),
            ManualCheckObservation::Running {
                diagnostic: "running".into(),
                executor: None,
                since_unix_ms: None,
            },
        );
        assert!(checks.contains_key("active"));
        assert!(!checks.contains_key("settled"));
    }
}
