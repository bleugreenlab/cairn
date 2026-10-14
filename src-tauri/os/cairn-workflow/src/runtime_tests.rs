//! Exercises the engine against real schemas while the host only executes workloads.
use super::*;
use async_trait::async_trait;
use cairn_db::turso::params;
use cairn_platform::services::emitter::CapturingEmitter;
use effects::types::{EffectResult, JobEvent, WorkflowEffect};
use runtime::{AiConditionRequest, WorkflowHost, WorkflowOwner, WorkflowRuntime, Workload};
use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};
use storage::{LocalDb, MigrationRunner, TEAM_MIGRATIONS};

struct Host {
    owning: Arc<LocalDb>,
    calls: Mutex<Vec<String>>,
    checkpoint_passes: Mutex<Vec<bool>>,
    head: Mutex<String>,
    /// The host's answer to an AI condition; `None` means none is expected.
    condition_answer: Mutex<Option<Result<String, String>>>,
    condition_requests: Mutex<Vec<AiConditionRequest>>,
    /// The outcome each `condition_evaluated` call reported, in order.
    condition_outcomes: Mutex<Vec<models::ConditionOutcome>>,
    /// SQL the host runs against the owning database while it answers an AI
    /// condition: whatever lands while the model call is in flight.
    during_condition: Mutex<Option<String>>,
}

#[async_trait]
impl WorkflowHost for Host {
    fn database(&self, _: WorkflowOwner<'_>) -> Result<Arc<LocalDb>, String> {
        Ok(self.owning.clone())
    }
    async fn databases(&self) -> Vec<Arc<LocalDb>> {
        vec![self.owning.clone()]
    }
    fn claimed_launch_job_ids(&self) -> HashSet<String> {
        HashSet::new()
    }
    fn resolve_coordinate(&self, _: &Path, _: &str) -> Result<String, String> {
        Ok(self.head.lock().unwrap().clone())
    }
    fn job_head(&self, _: &str) -> Result<String, String> {
        Ok(self.head.lock().unwrap().clone())
    }
    async fn complete_condition(&self, request: AiConditionRequest) -> Result<String, String> {
        let answer = self
            .condition_answer
            .lock()
            .unwrap()
            .clone()
            .expect("programmatic condition must not request a model");
        self.condition_requests.lock().unwrap().push(request);
        let during = self.during_condition.lock().unwrap().take();
        if let Some(sql) = during {
            self.owning.execute_script(&sql).await.unwrap();
        }
        answer
    }
    fn lifecycle_message(&self, _: &str, _: Option<&str>, _: JobEvent) {}
    fn projection_changed(&self, _: &str, _: &advancement::RecomputeOutcome) {}
    fn condition_evaluated(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        outcome: models::ConditionOutcome,
    ) {
        self.calls.lock().unwrap().push("condition".into());
        self.condition_outcomes.lock().unwrap().push(outcome);
    }
    fn continue_job(&self, _: &str, _: &str) -> Result<(), String> {
        self.calls.lock().unwrap().push("continue".into());
        Ok(())
    }
    fn resume_parent(&self, _: &str) -> Result<(), String> {
        panic!("no delegated parent in this fixture")
    }
    fn run_error(&self, _: &str, _: &str) {
        panic!("no run failure in this fixture")
    }
    async fn checkpoint(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<conditions::ExecutedCheckpoint, String> {
        self.calls.lock().unwrap().push("checkpoint".into());
        let passed = self.checkpoint_passes.lock().unwrap().remove(0);
        Ok(conditions::ExecutedCheckpoint {
            coordinate: self.head.lock().unwrap().clone(),
            output: conditions::CheckpointRunOutput {
                passed,
                exit_code: if passed { 0 } else { 1 },
                stdout: String::new(),
                stderr: if passed {
                    String::new()
                } else {
                    "broken check".into()
                },
            },
        })
    }
    async fn execute(&self, workload: Workload) -> Result<Option<EffectResult>, String> {
        match workload {
            Workload::Action {
                action_run_id,
                execution_id,
                ..
            } => {
                self.calls.lock().unwrap().push("action".into());
                self.owning
                    .execute(
                        "UPDATE action_runs SET status = 'complete' WHERE id = ?1",
                        params![action_run_id],
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Some(EffectResult::ActionComplete { execution_id }))
            }
            Workload::AgentJobs(jobs) => {
                assert_eq!(jobs.len(), 1);
                assert_eq!(jobs[0].recipe_node_id.as_deref(), Some("agent"));
                self.calls.lock().unwrap().push("agent".into());
                Ok(None)
            }
        }
    }
}

async fn fixture(team: bool) -> (WorkflowRuntime, Arc<Host>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let private = Arc::new(storage::migrated_test_db("workflow-private.db").await);
    let owning = if team {
        let db = Arc::new(LocalDb::open(dir.path().join("team.db")).await.unwrap());
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    } else {
        private.clone()
    };
    let host = Arc::new(Host {
        owning,
        calls: Mutex::new(vec![]),
        checkpoint_passes: Mutex::new(vec![]),
        head: Mutex::new("head-one".into()),
        condition_answer: Mutex::new(None),
        condition_requests: Mutex::new(vec![]),
        condition_outcomes: Mutex::new(vec![]),
        during_condition: Mutex::new(None),
    });
    let runtime = WorkflowRuntime::new(
        host.clone(),
        private,
        Arc::new(CapturingEmitter::new()),
        dir.path().to_path_buf(),
        None,
    );
    (runtime, host, dir)
}

fn node(id: &str, kind: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "nodeType": kind, "name": id, "position": {"x": 0.0, "y": 0.0}})
}
fn edge(from: &str, port: &str, to: &str) -> serde_json::Value {
    serde_json::json!({"id": format!("{from}-{to}"), "edgeType": "control", "sourceNodeId": from, "sourceHandle": port, "targetNodeId": to, "targetHandle": "control-in"})
}
async fn seed(db: &LocalDb, nodes: Vec<serde_json::Value>, edges: Vec<serde_json::Value>) {
    db.execute_script("INSERT INTO teams(id,name,created_at,updated_at) VALUES('team','Team',1,1);
        INSERT INTO projects(id,team_id,name,key,repo_path,created_at,updated_at) VALUES('p','team','P','proj','/tmp/workflow',1,1);
        INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('issue','p',1,'Issue','active','active','none',1,1);").await.unwrap();
    let snapshot = serde_json::json!({"recipe": {"id":"recipe", "name":"Recipe", "description":null, "trigger":"manual", "nodes":nodes, "edges":edges}, "agents":{}, "skills":{}, "triggerContext":{"issueId":"issue","projectId":"p","triggerType":"manual"}, "delegatedPackets":[], "createdAt":1});
    db.execute("INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot) VALUES('exec','recipe','issue','p','running',1,1,?1)", (snapshot.to_string(),)).await.unwrap();
}
async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql.to_string(), (), |row| {
        use storage::RowExt;
        row.i64(0)
    })
    .await
    .unwrap()
}
fn advance(id: Option<String>) -> WorkflowEffect {
    WorkflowEffect::AdvanceDag {
        execution_id: "exec".into(),
        outbox_entry_id: id,
    }
}

#[tokio::test]
async fn action_condition_agent_cycle_and_duplicate_replay_on_team_schema() {
    let (runtime, host, _dir) = fixture(true).await;
    let mut action = node("action", "action");
    action["actionConfig"] = serde_json::json!({"action":"echo"});
    let mut condition = node("condition", "condition");
    condition["conditionConfig"] = serde_json::json!({"conditionType":"programmatic", "expression":"true", "ports":["yes","no"], "onError":"block"});
    seed(
        &host.owning,
        vec![
            node("trigger", "trigger"),
            action,
            condition,
            node("agent", "agent"),
        ],
        vec![
            edge("trigger", "control-out", "action"),
            edge("action", "control-out", "condition"),
            edge("condition", "yes", "agent"),
        ],
    )
    .await;
    host.owning.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('job','exec','agent','issue','p','pending','agent','agent',1,1)").await.unwrap();
    let entry = effects::outbox::insert_pending_with_payload_async(
        &runtime.private_db,
        "advance_dag",
        "exec",
        "{}",
    )
    .await
    .unwrap();
    let report = effects::run::execute_effects(&runtime, vec![advance(Some(entry))])
        .await
        .unwrap();
    assert_eq!(report.effects_executed, 7);
    assert_eq!(
        *host.calls.lock().unwrap(),
        ["action", "condition", "agent"]
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE status = 'running'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM condition_evaluations WHERE result_port = 'yes'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE state = 'done'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&runtime.private_db, "SELECT COUNT(*) FROM jobs").await,
        0
    );
    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(
        *host.calls.lock().unwrap(),
        ["action", "condition", "agent"],
        "replayed advancement must not repeat claimed jobs or completed actions/conditions"
    );
    assert_eq!(
        count(&host.owning, "SELECT COUNT(*) FROM action_runs").await,
        1
    );
}

/// Seed trigger -> AI condition, with the condition's `no` port feeding an agent.
async fn seed_ai_condition(host: &Host, config: serde_json::Value) {
    let mut condition = node("condition", "condition");
    condition["conditionConfig"] = config;
    seed(
        &host.owning,
        vec![
            node("trigger", "trigger"),
            condition,
            node("agent", "agent"),
        ],
        vec![
            edge("trigger", "control-out", "condition"),
            edge("condition", "no", "agent"),
        ],
    )
    .await;
    host.owning.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('job','exec','agent','issue','p','pending','agent','agent',1,1)").await.unwrap();
}

#[tokio::test]
async fn ai_condition_routes_on_the_hosts_answer() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_ai_condition(
        &host,
        serde_json::json!({"conditionType":"ai", "question":"Does it touch the database?", "ports":["yes","no"], "onError":"block"}),
    )
    .await;
    *host.condition_answer.lock().unwrap() = Some(Ok("No.".into()));

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();

    let requests = host.condition_requests.lock().unwrap().clone();
    assert_eq!(
        requests,
        [AiConditionRequest {
            execution_id: "exec".into(),
            node_id: "condition".into(),
            node_name: "condition".into(),
            question: "Does it touch the database?".into(),
            ports: vec!["yes".into(), "no".into()],
            context: "{}".into(),
            model: None,
        }]
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM condition_evaluations WHERE result_port = 'no' AND error_message IS NULL"
        )
        .await,
        1
    );
    assert_eq!(*host.calls.lock().unwrap(), ["condition", "agent"]);
}

#[tokio::test]
async fn ai_condition_refusal_takes_the_default_port_and_keeps_the_reason() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_ai_condition(
        &host,
        serde_json::json!({"conditionType":"ai", "question":"Q?", "model":"md", "ports":["yes","no"], "onError":"useDefault", "defaultPort":"no"}),
    )
    .await;
    *host.condition_answer.lock().unwrap() = Some(Err("no configured account".into()));

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();

    assert_eq!(
        host.condition_requests.lock().unwrap()[0].model.as_deref(),
        Some("md")
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM condition_evaluations WHERE outcome = 'routed' AND result_port = 'no' AND error_message LIKE '%no configured account%'"
        )
        .await,
        1
    );
}

/// The condition node's entry in the execution's node-status projection.
async fn condition_node_status(db: &LocalDb) -> Option<String> {
    db.read(|conn| Box::pin(async move { queries::node_statuses_conn(conn, "exec").await }))
        .await
        .unwrap()
        .remove("condition")
}

/// `(outcome, result_port, error_message)` for every condition row, oldest first.
async fn condition_rows(db: &LocalDb) -> Vec<(String, String, Option<String>)> {
    db.query_all(
        "SELECT outcome, result_port, error_message FROM condition_evaluations ORDER BY evaluated_at, rowid"
            .to_string(),
        (),
        |row| {
            use storage::RowExt;
            Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?))
        },
    )
    .await
    .unwrap()
}

const BLOCK_ON_ERROR: &str =
    r#"{"conditionType":"ai", "question":"Q?", "ports":["yes","no"], "onError":"block"}"#;

/// Run a blocking AI condition whose host answers `answer`, and assert it left
/// exactly one blocked row carrying `reason`, started nothing downstream, told
/// the host it blocked, and stays parked on a replayed advance.
async fn assert_condition_blocks(config: &str, answer: Result<String, String>, reason: &str) {
    let (runtime, host, _dir) = fixture(true).await;
    seed_ai_condition(&host, serde_json::from_str(config).unwrap()).await;
    *host.condition_answer.lock().unwrap() = Some(answer);

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();

    assert_eq!(
        condition_rows(&host.owning).await,
        [(
            "blocked".to_string(),
            String::new(),
            Some(reason.to_string())
        )]
    );
    assert_eq!(*host.calls.lock().unwrap(), ["condition"]);
    assert_eq!(
        *host.condition_outcomes.lock().unwrap(),
        [models::ConditionOutcome::Blocked]
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM issues WHERE id = 'issue' AND status = 'waiting' AND attention = 'needs_approval'"
        )
        .await,
        1,
        "a blocked condition waits on the user exactly like a blocked checkpoint"
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM executions WHERE id = 'exec' AND status = 'running'"
        )
        .await,
        1
    );

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(
        host.condition_requests.lock().unwrap().len(),
        1,
        "a blocked condition stays parked; advancing must not ask again"
    );
    assert_eq!(condition_rows(&host.owning).await.len(), 1);
}

#[tokio::test]
async fn blocked_ai_condition_refusal_writes_a_blocked_row() {
    let refusal = "AI condition 'condition' can't run: no configured account can serve it. Connect an account in Settings → Providers.";
    assert_condition_blocks(BLOCK_ON_ERROR, Err(refusal.into()), refusal).await;
}

#[tokio::test]
async fn blocked_ai_condition_timeout_writes_a_blocked_row() {
    assert_condition_blocks(
        BLOCK_ON_ERROR,
        Err("response timed out".into()),
        "response timed out",
    )
    .await;
}

#[tokio::test]
async fn blocked_ai_condition_unmatched_answer_writes_a_blocked_row() {
    let ports = vec!["yes".to_string(), "no".to_string()];
    let reason = condition::match_ai_condition_answer("Maybe.", &ports).unwrap_err();
    assert_condition_blocks(BLOCK_ON_ERROR, Ok("Maybe.".into()), &reason).await;
}

#[tokio::test]
async fn blocked_ai_condition_ambiguous_answer_writes_a_blocked_row() {
    let ports = vec!["yes".to_string(), "no".to_string()];
    let reason = condition::match_ai_condition_answer("no, not yes", &ports).unwrap_err();
    assert!(reason.contains("more than one port"), "{reason}");
    assert_condition_blocks(BLOCK_ON_ERROR, Ok("no, not yes".into()), &reason).await;
}

#[tokio::test]
async fn use_default_without_a_default_port_writes_a_blocked_row() {
    assert_condition_blocks(
        r#"{"conditionType":"ai", "question":"Q?", "ports":["yes","no"], "onError":"useDefault"}"#,
        Err("response timed out".into()),
        "onError is useDefault but no defaultPort is configured; evaluation failed: response timed out",
    )
    .await;
}

#[tokio::test]
async fn reevaluating_a_blocked_condition_routes_and_keeps_the_blocked_row() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_ai_condition(&host, serde_json::from_str(BLOCK_ON_ERROR).unwrap()).await;
    *host.condition_answer.lock().unwrap() = Some(Err("no configured account".into()));
    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(*host.calls.lock().unwrap(), ["condition"]);
    assert_eq!(
        condition_node_status(&host.owning).await.as_deref(),
        Some("blocked")
    );

    *host.condition_answer.lock().unwrap() = Some(Ok("no".into()));
    let verdict = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap();

    assert_eq!(
        verdict,
        conditions::ConditionVerdict::Routed {
            port: "no".into(),
            error: None
        }
    );
    assert_eq!(
        *host.calls.lock().unwrap(),
        ["condition", "condition", "agent"]
    );
    assert_eq!(
        condition_rows(&host.owning).await,
        [
            (
                "blocked".to_string(),
                String::new(),
                Some("no configured account".to_string())
            ),
            ("routed".to_string(), "no".to_string(), None),
        ]
    );

    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM issues WHERE id = 'issue' AND attention = 'needs_approval'"
        )
        .await,
        0,
        "a routed re-evaluation releases the issue from waiting"
    );

    assert_eq!(
        condition_node_status(&host.owning).await.as_deref(),
        Some("complete")
    );

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();
    assert!(error.contains("already routed to 'no'"), "{error}");
}

/// Seed the AI condition and block it once on a refusal, leaving one blocked
/// row and an answer of `no` ready for the next evaluation.
async fn block_condition_once(runtime: &WorkflowRuntime, host: &Host) {
    seed_ai_condition(host, serde_json::from_str(BLOCK_ON_ERROR).unwrap()).await;
    *host.condition_answer.lock().unwrap() = Some(Err("no configured account".into()));
    effects::run::execute_effects(runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(condition_rows(&host.owning).await.len(), 1);
    *host.condition_answer.lock().unwrap() = Some(Ok("no".into()));
}

#[tokio::test]
async fn reevaluating_a_superseded_execution_is_refused_before_asking() {
    let (runtime, host, _dir) = fixture(true).await;
    block_condition_once(&runtime, &host).await;
    host.owning
        .execute_script("UPDATE executions SET lifecycle = 'superseded' WHERE id = 'exec'")
        .await
        .unwrap();

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();

    assert!(error.contains("superseded"), "{error}");
    assert_eq!(
        host.condition_requests.lock().unwrap().len(),
        1,
        "a fenced execution must not spend a model call"
    );
    assert_eq!(condition_rows(&host.owning).await.len(), 1);
    assert_eq!(*host.calls.lock().unwrap(), ["condition"]);
}

#[tokio::test]
async fn reevaluating_a_resolved_issues_condition_is_refused() {
    let (runtime, host, _dir) = fixture(true).await;
    block_condition_once(&runtime, &host).await;
    host.owning
        .execute_script("UPDATE issues SET closed_at = 5 WHERE id = 'issue'")
        .await
        .unwrap();

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();

    assert!(error.contains("issue is resolved"), "{error}");
    assert_eq!(host.condition_requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn supersession_during_reevaluation_discards_the_result() {
    let (runtime, host, _dir) = fixture(true).await;
    block_condition_once(&runtime, &host).await;
    *host.during_condition.lock().unwrap() =
        Some("UPDATE executions SET lifecycle = 'superseded' WHERE id = 'exec'".into());

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();

    assert!(error.contains("discarded"), "{error}");
    assert!(error.contains("superseded"), "{error}");
    assert_eq!(
        condition_rows(&host.owning).await.len(),
        1,
        "an answer that arrives after the fence records nothing"
    );
    assert_eq!(
        *host.calls.lock().unwrap(),
        ["condition"],
        "nothing downstream starts"
    );
}

/// Two overlapping re-evaluations: the other one routes `yes` while this one
/// waits on the model, then this one answers `no`. Only the first decision
/// stands; the late `no` must not start the `no` branch too.
#[tokio::test]
async fn a_route_recorded_during_reevaluation_wins_and_the_late_answer_is_discarded() {
    let (runtime, host, _dir) = fixture(true).await;
    block_condition_once(&runtime, &host).await;
    *host.during_condition.lock().unwrap() = Some(
        "INSERT INTO condition_evaluations(id, execution_id, recipe_node_id, outcome, result_port, evaluated_at)
         VALUES ('competing', 'exec', 'condition', 'routed', 'yes', 2147483000)"
            .into(),
    );

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();

    assert!(error.contains("discarded"), "{error}");
    assert!(error.contains("already routed to 'yes'"), "{error}");
    assert_eq!(
        condition_rows(&host.owning).await,
        [
            (
                "blocked".to_string(),
                String::new(),
                Some("no configured account".to_string())
            ),
            ("routed".to_string(), "yes".to_string(), None),
        ]
    );
    assert_eq!(
        *host.calls.lock().unwrap(),
        ["condition"],
        "the `no` branch must not start"
    );
}

/// With no job or action left to keep it running, a blocked condition is what
/// holds the execution open and the issue waiting, across any later recompute,
/// until the execution is fenced.
#[tokio::test]
async fn a_blocked_condition_with_no_pending_work_keeps_the_execution_running_until_fenced() {
    let (runtime, host, _dir) = fixture(true).await;
    let mut condition = node("condition", "condition");
    condition["conditionConfig"] = serde_json::from_str(BLOCK_ON_ERROR).unwrap();
    seed(
        &host.owning,
        vec![
            node("trigger", "trigger"),
            condition,
            node("agent", "agent"),
        ],
        vec![
            edge("trigger", "control-out", "condition"),
            edge("condition", "no", "agent"),
        ],
    )
    .await;
    *host.condition_answer.lock().unwrap() = Some(Err("response timed out".into()));

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM jobs").await, 0);

    let waiting = "SELECT COUNT(*) FROM issues WHERE id = 'issue' AND status = 'waiting' AND attention = 'needs_approval'";
    assert_eq!(
        transitions::status::recompute_execution_status(&host.owning, "exec").await,
        models::ExecutionStatus::Running
    );
    assert_eq!(count(&host.owning, waiting).await, 1);

    host.owning
        .execute_script("UPDATE executions SET lifecycle = 'superseded' WHERE id = 'exec'")
        .await
        .unwrap();
    assert_eq!(
        transitions::status::recompute_execution_status(&host.owning, "exec").await,
        models::ExecutionStatus::Complete,
        "a fenced execution's blocked condition waits on no one"
    );
    transitions::status::recompute_issue_status(&host.owning, "issue").await;
    assert_eq!(count(&host.owning, waiting).await, 0);
}

#[tokio::test]
async fn reevaluating_a_condition_that_never_ran_is_refused() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_ai_condition(&host, serde_json::from_str(BLOCK_ON_ERROR).unwrap()).await;

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();
    assert!(error.contains("has not been evaluated yet"), "{error}");
    assert!(host.condition_requests.lock().unwrap().is_empty());

    let error = advancement::reevaluate_condition(&runtime, "exec", "agent")
        .await
        .unwrap_err();
    assert!(error.contains("not a condition"), "{error}");
}

#[tokio::test]
async fn failed_reduction_leaves_private_outbox_pending() {
    let (runtime, _host, _dir) = fixture(true).await;
    let entry = effects::outbox::insert_pending_with_payload_async(
        &runtime.private_db,
        "advance_dag",
        "exec",
        "{}",
    )
    .await
    .unwrap();
    assert!(
        effects::run::execute_effects(&runtime, vec![advance(Some(entry))])
            .await
            .is_err()
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE state = 'pending'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn cached_checkpoint_pass_does_not_execute_or_add_history() {
    let (runtime, host, _dir) = fixture(true).await;
    let result =
        effects::run::run_checkpoint(&runtime, "missing-job", "check", "false", true).await;
    assert!(matches!(
        result,
        EffectResult::CheckpointComplete {
            passed: true,
            error: None,
            ..
        }
    ));
    assert!(host.calls.lock().unwrap().is_empty());
    assert_eq!(
        count(&host.owning, "SELECT COUNT(*) FROM checkpoint_runs").await,
        0
    );
}

#[tokio::test]
async fn dependent_team_execution_uses_private_outbox_for_restart_replay() {
    let (runtime, host, _dir) = fixture(true).await;
    seed(&host.owning, vec![node("agent", "agent")], vec![]).await;
    host.owning.execute_script("INSERT INTO issues(id,project_id,number,title,status,progress,attention,merged_at,created_at,updated_at) VALUES('resolved','p',2,'Resolved','merged','complete','none',2,1,2);
        INSERT INTO issue_dependencies(issue_id,depends_on_uri,created_at) VALUES('issue','cairn://p/proj/2',1);").await.unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'effect_outbox'"
        )
        .await,
        0,
        "real team schemas exclude the private outbox"
    );
    advancement::release_dependent_executions(&runtime, "resolved")
        .await
        .unwrap();
    assert_eq!(count(&runtime.private_db,"SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'exec' AND state = 'pending'").await,1,"dependent advancement must survive loss of the in-memory effect channel");
}

#[tokio::test]
async fn checkpoint_retry_requires_new_head_and_keeps_attempt_history() {
    let (runtime, host, _dir) = fixture(true).await;
    let mut checkpoint = node("check", "checkpoint");
    checkpoint["parentId"] = "agent".into();
    checkpoint["checkpointConfig"] = serde_json::json!({"command":"test-command"});
    seed(
        &host.owning,
        vec![node("agent", "agent"), checkpoint],
        vec![edge("agent", "control-out", "check")],
    )
    .await;
    host.owning.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('parent','exec','agent','issue','p','idle','agent','agent',1,1);
        INSERT INTO jobs(id,execution_id,recipe_node_id,parent_job_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('check-job','exec','check','parent','issue','p','blocked','check','check',1,1);
        INSERT INTO artifacts(id,job_id,artifact_type,schema_version,data,version,confirmed,created_at,updated_at) VALUES('artifact','check-job','checkpoint',1,'{}',1,0,1,1);").await.unwrap();
    *host.checkpoint_passes.lock().unwrap() = vec![false, true];
    assert!(matches!(
        effects::run::run_checkpoint(&runtime, "check-job", "check", "test-command", false).await,
        EffectResult::CheckpointComplete { passed: false, .. }
    ));
    advancement::rearm_blocked_checkpoints(&runtime, "exec").unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE id = 'check-job' AND status = 'blocked'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM artifacts WHERE job_id = 'check-job'"
        )
        .await,
        1
    );
    *host.head.lock().unwrap() = "head-two".into();
    advancement::rearm_blocked_checkpoints(&runtime, "exec").unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE id = 'check-job' AND status = 'pending'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM artifacts WHERE job_id = 'check-job'"
        )
        .await,
        0
    );
    assert!(matches!(
        effects::run::run_checkpoint(&runtime, "check-job", "check", "test-command", false).await,
        EffectResult::CheckpointComplete { passed: true, .. }
    ));
    assert_eq!(
        count(&host.owning, "SELECT COUNT(*) FROM checkpoint_runs").await,
        2
    );
    assert_eq!(count(&host.owning,"SELECT COUNT(*) FROM checkpoint_runs WHERE attempt = 2 AND passed = 1 AND commit_sha = 'head-two'").await,1);
}
