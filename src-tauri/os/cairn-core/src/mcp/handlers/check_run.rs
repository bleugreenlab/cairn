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
#[derive(Clone, serde::Serialize)]
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
fn spawn_manual_observation<F>(handle: String, run: impl FnOnce(ManualProgressSink) -> F)
where
    F: std::future::Future<
            Output = Result<crate::execution::checks::ManualConfiguredCheckResult, String>,
        > + Send
        + 'static,
{
    let progress_handle = handle.clone();
    let progress: ManualProgressSink = Arc::new(move |update| {
        let next = observation_from_progress(update);
        if let Ok(mut checks) = MANUAL_CHECKS.lock() {
            retain_observation(&mut checks, progress_handle.clone(), next);
        }
    });
    let running = run(progress);
    tokio::spawn(async move {
        let final_state = match running.await {
            Ok(result) => ManualCheckObservation::Complete {
                result: Box::new(result),
            },
            Err(error) => ManualCheckObservation::Failed { error },
        };
        if let Ok(mut checks) = MANUAL_CHECKS.lock() {
            retain_observation(&mut checks, handle, final_state);
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
        if let Some(handle) = payload.observation_handle.as_deref() {
            return load_observation(handle).map(|observation| (None, observation));
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
        retain_observation(
            &mut *MANUAL_CHECKS
                .lock()
                .map_err(|_| "manual check observation registry is unavailable".to_string())?,
            handle.clone(),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        );
        let task_orch = orch.clone();
        let task_run_id = run_id.to_string();
        let task_suite = suite.to_string();
        let task_branch = commit;
        let task_retry = payload.retry;
        spawn_manual_observation(handle.clone(), move |progress| async move {
            crate::execution::checks::run_manual_configured_check_with_progress(
                &task_orch,
                &task_run_id,
                &task_suite,
                task_branch.as_deref(),
                placement,
                task_retry,
                progress,
            )
            .await
        });
        Ok((
            Some(handle),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        ))
    }
    .await;

    match result {
        Ok((Some(handle), observation)) => serde_json::json!({
            "ok": true, "observationHandle": handle, "observation": observation
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
        let handle = format!("unpolled-{}", uuid::Uuid::new_v4());
        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        spawn_manual_observation(handle.clone(), |progress| async move {
            progress(crate::execution::checks::ManualCheckProgressUpdate {
                state: "running".into(),
                diagnostic: "running on fedora since 09:48:09Z".into(),
                executor: Some("fedora".into()),
                since_unix_ms: Some(1_792_403_289_000),
            });
            let _ = finished.await;
            Err("settled with nobody polling".to_string())
        });
        let observed = |handle: &str| {
            MANUAL_CHECKS
                .lock()
                .unwrap()
                .get(handle)
                .map(|retained| retained.observation.clone())
        };
        let until = |wanted: fn(&ManualCheckObservation) -> bool| {
            let handle = handle.clone();
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
