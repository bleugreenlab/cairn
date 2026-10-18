//! Exercises the engine against real schemas while the host only executes workloads.
use super::*;
use async_trait::async_trait;
use cairn_db::turso::params;
use cairn_platform::services::emitter::CapturingEmitter;
use effects::types::{EffectResult, JobEvent, WorkflowEffect};
use models::decision::{Decision, DecisionAnswer, DecisionNode, MeasuredBy, PORT_QUESTION_NAME};
use runtime::{DecisionRequest, WorkflowHost, WorkflowOwner, WorkflowRuntime, Workload};
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
    /// The host's answer to a decision node; `None` means none is expected.
    condition_answer: Mutex<Option<Result<Decision, String>>>,
    condition_requests: Mutex<Vec<DecisionRequest>>,
    /// The outcome each `condition_evaluated` call reported, in order.
    condition_outcomes: Mutex<Vec<models::ConditionOutcome>>,
    /// SQL the host runs against the owning database while it answers a
    /// decision: whatever lands while the model call is in flight.
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
    fn pr_publish_input_revision(&self, _: &str) -> Result<Option<String>, String> {
        let head = self.head.lock().unwrap().clone();
        if head == "revision-unavailable" {
            return Err("revision lookup failed".into());
        }
        Ok(Some(head))
    }
    async fn decide(&self, request: DecisionRequest) -> Result<Decision, String> {
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
    use storage::RowExt;
    let columns = db
        .query_all("PRAGMA table_info(projects)", (), |row| row.text(1))
        .await
        .unwrap();
    if columns.iter().any(|column| column == "team_id") {
        db.execute_script("INSERT INTO teams(id,name,created_at,updated_at) VALUES('team','Team',1,1);
            INSERT INTO projects(id,team_id,name,key,repo_path,created_at,updated_at) VALUES('p','team','P','proj','/tmp/workflow',1,1);").await.unwrap();
    } else {
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('workspace','Workspace',1,1);
            INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','workspace','P','proj','/tmp/workflow',1,1);").await.unwrap();
    }
    db.execute_script("INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('issue','p',1,'Issue','active','active','none',1,1);").await.unwrap();
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

/// A decision node with id `condition`.
fn decision_node(definition: DecisionNode) -> serde_json::Value {
    let mut decision = node("condition", "decision");
    decision["decisionConfig"] = serde_json::to_value(definition).unwrap();
    decision
}

fn ports() -> Vec<String> {
    vec!["yes".to_string(), "no".to_string()]
}

fn yes_or_no() -> DecisionNode {
    DecisionNode::port_choice("Q?", &ports(), None)
}

fn decided(answer: DecisionAnswer) -> Decision {
    Decision {
        provider: "decisions".into(),
        model: "typesafe-ai/jev".into(),
        answers: vec![answer],
        input_tokens: Some(12),
    }
}

/// A native answer picking `port` at probability 0.9.
fn picked(port: &str) -> Decision {
    decided(DecisionAnswer::Choice {
        name: PORT_QUESTION_NAME.into(),
        measured_by: MeasuredBy::Native,
        pick: port.into(),
        confidence: Some(0.9),
        probabilities: vec![],
    })
}

fn refused() -> Decision {
    decided(DecisionAnswer::Refusal {
        name: PORT_QUESTION_NAME.into(),
        measured_by: MeasuredBy::Native,
        reason: Some("not enough context".into()),
    })
}

const REFUSED_REASON: &str = "the model refused to answer 'route': not enough context";

/// A frozen pre-decision snapshot's condition node, as stored before the
/// `decision` node kind existed.
fn legacy_ai_condition(config: serde_json::Value) -> serde_json::Value {
    let mut condition = node("condition", "condition");
    condition["conditionConfig"] = config;
    condition
}

/// Seed trigger -> `condition`, with the condition's `no` port feeding an agent.
async fn seed_condition(host: &Host, condition: serde_json::Value) {
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

/// The typed answer stored on every condition row, oldest first.
async fn stored_decisions(db: &LocalDb) -> Vec<Option<Decision>> {
    db.query_all(
        "SELECT raw_result FROM condition_evaluations ORDER BY evaluated_at, rowid".to_string(),
        (),
        |row| {
            use storage::RowExt;
            row.opt_text(0)
        },
    )
    .await
    .unwrap()
    .into_iter()
    .map(|raw| raw.map(|raw| serde_json::from_str(&raw).unwrap()))
    .collect()
}

#[tokio::test]
async fn a_decision_node_routes_on_the_pick_and_records_the_answer() {
    let (runtime, host, _dir) = fixture(true).await;
    let definition =
        DecisionNode::port_choice("Does it touch the database?", &ports(), Some("sm".into()));
    seed_condition(&host, decision_node(definition.clone())).await;
    *host.condition_answer.lock().unwrap() = Some(Ok(picked("no")));

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();

    let requests = host.condition_requests.lock().unwrap().clone();
    assert_eq!(
        requests,
        [DecisionRequest {
            execution_id: "exec".into(),
            node_id: "condition".into(),
            node_name: "condition".into(),
            decision: definition,
            input: "{}".into(),
        }]
    );
    assert_eq!(
        condition_rows(&host.owning).await,
        [("routed".to_string(), "no".to_string(), None)]
    );
    assert_eq!(
        stored_decisions(&host.owning).await,
        [Some(picked("no"))],
        "the evaluation row is the decision's record"
    );
    assert_eq!(*host.calls.lock().unwrap(), ["condition", "agent"]);
}

#[tokio::test]
async fn a_frozen_ai_condition_is_asked_as_a_decision() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_condition(
        &host,
        legacy_ai_condition(serde_json::json!({"conditionType":"ai", "question":"Does it touch the database?", "model":"md", "ports":["yes","no"], "onError":"block"})),
    )
    .await;
    *host.condition_answer.lock().unwrap() = Some(Ok(picked("no")));

    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();

    assert_eq!(
        host.condition_requests.lock().unwrap()[0].decision,
        DecisionNode::port_choice("Does it touch the database?", &ports(), Some("md".into()))
    );
    assert_eq!(*host.calls.lock().unwrap(), ["condition", "agent"]);
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

/// Run `condition` with the host answering `answer` (`None` for a node that
/// must not ask), and assert it left exactly one blocked row carrying
/// `reason`, started nothing downstream, told the host it blocked, and stays
/// parked on a replayed advance.
async fn assert_condition_blocks(
    condition: serde_json::Value,
    answer: Option<Result<Decision, String>>,
    reason: &str,
) -> (Arc<Host>, tempfile::TempDir) {
    let (runtime, host, dir) = fixture(true).await;
    seed_condition(&host, condition).await;
    let asks = usize::from(answer.is_some());
    *host.condition_answer.lock().unwrap() = answer;

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
        asks,
        "a blocked condition stays parked; advancing must not ask again"
    );
    assert_eq!(condition_rows(&host.owning).await.len(), 1);
    (host, dir)
}

#[tokio::test]
async fn a_refused_decision_routes_nowhere_and_records_the_refusal() {
    let (host, _dir) = assert_condition_blocks(
        decision_node(yes_or_no()),
        Some(Ok(refused())),
        REFUSED_REASON,
    )
    .await;
    assert_eq!(stored_decisions(&host.owning).await, [Some(refused())]);
}

/// A legacy `useDefault` survives migration as configuration nobody reads: a
/// decision has no default port, so a refusal never becomes one.
#[tokio::test]
async fn a_refused_frozen_ai_condition_never_takes_its_default_port() {
    assert_condition_blocks(
        legacy_ai_condition(serde_json::json!({"conditionType":"ai", "question":"Q?", "ports":["yes","no"], "onError":"useDefault", "defaultPort":"no"})),
        Some(Ok(refused())),
        REFUSED_REASON,
    )
    .await;
}

#[tokio::test]
async fn a_pick_below_its_methods_threshold_routes_nowhere() {
    let mut definition = yes_or_no();
    definition.thresholds.native = Some(0.95);
    definition.thresholds.logprobs = Some(0.5);
    let (host, _dir) = assert_condition_blocks(
        decision_node(definition),
        Some(Ok(picked("no"))),
        "'route' picked 'no' with native confidence 0.90, below the native threshold 0.95",
    )
    .await;
    assert_eq!(stored_decisions(&host.owning).await, [Some(picked("no"))]);
}

#[tokio::test]
async fn a_decision_with_no_offer_writes_a_blocked_row() {
    let refusal =
        "Decision node 'condition' can't run: no configured provider can answer a decision.";
    let (host, _dir) = assert_condition_blocks(
        decision_node(yes_or_no()),
        Some(Err(refusal.into())),
        refusal,
    )
    .await;
    assert_eq!(stored_decisions(&host.owning).await, [None]);
}

#[tokio::test]
async fn a_decision_timeout_writes_a_blocked_row() {
    assert_condition_blocks(
        decision_node(yes_or_no()),
        Some(Err("response timed out".into())),
        "response timed out",
    )
    .await;
}

#[tokio::test]
async fn use_default_without_a_default_port_writes_a_blocked_row() {
    let failure =
        condition::evaluate_programmatic_condition("missing > 1", &serde_json::json!({}), &ports())
            .unwrap_err();
    assert_condition_blocks(
        legacy_ai_condition(serde_json::json!({"expression":"missing > 1", "ports":["yes","no"], "onError":"useDefault"})),
        None,
        &format!("onError is useDefault but no defaultPort is configured; evaluation failed: {failure}"),
    )
    .await;
}

#[tokio::test]
async fn reevaluating_a_blocked_condition_routes_and_keeps_the_blocked_row() {
    let (runtime, host, _dir) = fixture(true).await;
    seed_condition(&host, decision_node(yes_or_no())).await;
    *host.condition_answer.lock().unwrap() = Some(Err("no configured account".into()));
    effects::run::execute_effects(&runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(*host.calls.lock().unwrap(), ["condition"]);
    assert_eq!(
        condition_node_status(&host.owning).await.as_deref(),
        Some("blocked")
    );

    *host.condition_answer.lock().unwrap() = Some(Ok(picked("no")));
    let verdict = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap();

    assert_eq!(
        verdict,
        conditions::ConditionVerdict::Routed {
            port: "no".into(),
            error: None,
            decision: Some(picked("no")),
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
    assert!(error.message.contains("already routed to 'no'"), "{error}");
}

/// Seed the decision node and block it once on a failed call, leaving one blocked
/// row and an answer of `no` ready for the next evaluation.
async fn block_condition_once(runtime: &WorkflowRuntime, host: &Host) {
    seed_condition(host, decision_node(yes_or_no())).await;
    *host.condition_answer.lock().unwrap() = Some(Err("no configured account".into()));
    effects::run::execute_effects(runtime, vec![advance(None)])
        .await
        .unwrap();
    assert_eq!(condition_rows(&host.owning).await.len(), 1);
    *host.condition_answer.lock().unwrap() = Some(Ok(picked("no")));
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

    assert!(error.message.contains("superseded"), "{error}");
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

    assert!(error.message.contains("issue is resolved"), "{error}");
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

    assert!(error.message.contains("discarded"), "{error}");
    assert!(error.message.contains("superseded"), "{error}");
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

    assert!(error.message.contains("discarded"), "{error}");
    assert!(error.message.contains("already routed to 'yes'"), "{error}");
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
    let condition = decision_node(yes_or_no());
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
    seed_condition(&host, decision_node(yes_or_no())).await;

    let error = advancement::reevaluate_condition(&runtime, "exec", "condition")
        .await
        .unwrap_err();
    assert!(
        error.message.contains("has not been evaluated yet"),
        "{error}"
    );
    assert!(host.condition_requests.lock().unwrap().is_empty());

    let error = advancement::reevaluate_condition(&runtime, "exec", "agent")
        .await
        .unwrap_err();
    assert!(error.message.contains("not a condition"), "{error}");
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

async fn pr_fixture(team: bool) -> (WorkflowRuntime, Arc<Host>, tempfile::TempDir, String) {
    let (runtime, host, dir) = fixture(team).await;
    let mut pr = node("pr", "pr");
    pr["actionConfig"] = serde_json::json!({});
    seed(
        &host.owning,
        vec![node("trigger", "trigger"), pr],
        vec![edge("trigger", "control-out", "pr")],
    )
    .await;
    let (_, effects) = effects::dag::reduce_dag(&runtime, "exec").await.unwrap();
    let id = effects
        .iter()
        .find_map(|e| match e {
            WorkflowEffect::ExecuteAction { action_run_id, .. } => Some(action_run_id.clone()),
            _ => None,
        })
        .expect("first PR dispatch");
    (runtime, host, dir, id)
}

#[tokio::test]
async fn reduce_dag_dispatches_a_rearmed_pr_action_once() {
    for team in [false, true] {
        let (runtime, host, _dir, id) = pr_fixture(team).await;
        advancement::mark_action_run_failed(&runtime, &id, "push rejected").unwrap();
        *host.head.lock().unwrap() = "new-revision".into();
        let (_, effects) = effects::dag::reduce_dag(&runtime, "exec").await.unwrap();
        let actions: Vec<_> = effects
            .iter()
            .filter_map(|e| match e {
                WorkflowEffect::ExecuteAction { action_run_id, .. } => Some(action_run_id),
                _ => None,
            })
            .collect();
        assert_eq!(actions, vec![&id]);
        let attempts = pr_publish_attempts::list_publish_attempts(&host.owning, &id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].attempt, 2);
        assert_eq!(attempts[1].detail.as_deref(), Some("push rejected"));
        assert_eq!(attempts[1].outcome.as_deref(), Some("failed"));
        assert!(effects::dag::reduce_dag(&runtime, "exec")
            .await
            .unwrap()
            .1
            .iter()
            .all(|e| !matches!(e, WorkflowEffect::ExecuteAction { .. })));
        assert_eq!(
            count(&host.owning, "SELECT COUNT(*) FROM action_runs").await,
            1
        );
    }
}

#[tokio::test]
async fn pr_retry_unchanged_failure_stays_terminal_and_cap_stops_changed_inputs() {
    let (runtime, host, _dir, id) = pr_fixture(false).await;
    advancement::mark_action_run_failed(&runtime, &id, "permanent").unwrap();
    for _ in 0..3 {
        assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
            .unwrap()
            .is_empty());
    }
    for number in 2..=5 {
        *host.head.lock().unwrap() = format!("revision-{number}");
        assert_eq!(
            advancement::rearm_failed_pr_actions(&runtime, "exec")
                .unwrap()
                .len(),
            1
        );
        advancement::mark_action_run_failed(&runtime, &id, "still rejected").unwrap();
    }
    *host.head.lock().unwrap() = "revision-6".into();
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
    assert_eq!(
        pr_publish_attempts::publish_attempt_count(&host.owning, &id)
            .await
            .unwrap(),
        5
    );
}

#[tokio::test]
async fn explicit_pr_retry_preserves_history_resets_cycle_and_persists_dispatch() {
    let (runtime, host, _dir, id) = pr_fixture(true).await;
    advancement::mark_action_run_failed(&runtime, &id, "timeout").unwrap();
    pr_publish_attempts::retry_pr_publish(&runtime, &id)
        .await
        .unwrap();
    let attempts = pr_publish_attempts::list_publish_attempts(&host.owning, &id)
        .await
        .unwrap();
    assert_eq!((attempts[0].cycle, attempts[0].attempt), (2, 1));
    assert_eq!(attempts[1].detail.as_deref(), Some("timeout"));
    assert_eq!(attempts[0].input_revision, attempts[1].input_revision);
    let intents = runtime
        .private_db
        .query_all(
            "SELECT payload_json FROM effect_outbox WHERE kind='pr_publish' AND dedupe_key=?1",
            (attempts[0].id.clone(),),
            |r| {
                use storage::RowExt;
                r.text(0)
            },
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    let replay = pr_publish_attempts::replay_dispatch(&runtime, &intents[0], i64::MAX)
        .await
        .unwrap();
    assert!(
        matches!(replay,Some(WorkflowEffect::ExecuteAction {action_run_id,..}) if action_run_id==id)
    );
    assert!(pr_publish_attempts::claim_dispatch(&runtime, &id)
        .await
        .unwrap());
    assert!(!pr_publish_attempts::claim_dispatch(&runtime, &id)
        .await
        .unwrap());
    // A startup replay can recover a claim left running by the previous host.
    assert!(
        pr_publish_attempts::replay_dispatch(&runtime, &intents[0], i64::MAX)
            .await
            .unwrap()
            .is_some()
    );
    assert!(pr_publish_attempts::claim_dispatch(&runtime, &id)
        .await
        .unwrap());
    advancement::mark_action_run_failed(&runtime, &id, "failed again").unwrap();
    assert!(
        pr_publish_attempts::replay_dispatch(&runtime, &intents[0], i64::MAX)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn published_pr_and_generic_actions_are_never_rearmed() {
    let (runtime, host, _dir, id) = pr_fixture(false).await;
    host.owning
        .execute(
            "UPDATE action_runs SET status='blocked' WHERE id=?1",
            (id.clone(),),
        )
        .await
        .unwrap();
    *host.head.lock().unwrap() = "changed".into();
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
    assert!(
        pr_publish_attempts::validate_pr_publish_retry(&runtime, &id)
            .await
            .unwrap_err()
            .contains("refresh")
    );
    host.owning
        .execute(
            "UPDATE action_runs SET status='failed',action_config_id='builtin:shell' WHERE id=?1",
            (id.clone(),),
        )
        .await
        .unwrap();
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
    assert!(pr_publish_attempts::retry_pr_publish(&runtime, &id)
        .await
        .is_err());
}

#[tokio::test]
async fn pr_replay_does_not_steal_a_live_claim_and_discards_orphans() {
    let (runtime, host, _dir, id) = pr_fixture(false).await;
    let attempt = pr_publish_attempts::latest_publish_attempt(&host.owning, &id)
        .await
        .unwrap()
        .unwrap();
    let payload = runtime
        .private_db
        .query_one(
            "SELECT payload_json FROM effect_outbox WHERE dedupe_key=?1",
            (attempt.id.clone(),),
            |r| {
                use storage::RowExt;
                r.text(0)
            },
        )
        .await
        .unwrap();
    assert!(pr_publish_attempts::claim_dispatch(&runtime, &id)
        .await
        .unwrap());
    assert!(pr_publish_attempts::replay_dispatch(&runtime, &payload, 0)
        .await
        .unwrap()
        .is_none());
    let mut older_intent: serde_json::Value = serde_json::from_str(&payload).unwrap();
    older_intent["created_at_millis"] = serde_json::json!(0);
    assert!(
        pr_publish_attempts::replay_dispatch(&runtime, &older_intent.to_string(), 1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind='pr_publish' AND state='pending'"
        )
        .await,
        1
    );
    let mut orphan: serde_json::Value = serde_json::from_str(&payload).unwrap();
    orphan["attempt_id"] = serde_json::json!("orphan");
    effects::outbox::insert_pending_with_payload_async(
        &runtime.private_db,
        "pr_publish",
        "orphan",
        &orphan.to_string(),
    )
    .await
    .unwrap();
    assert!(
        pr_publish_attempts::replay_dispatch(&runtime, &orphan.to_string(), i64::MAX)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE dedupe_key='orphan' AND state='done'"
        )
        .await,
        1
    );
    advancement::mark_action_run_failed(&runtime, &id, "failure").unwrap();
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind='pr_publish' AND state!='done'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn pr_retry_missing_baseline_snapshot_type_and_supersession_fail_closed() {
    let (runtime, host, _dir, id) = pr_fixture(false).await;
    advancement::mark_action_run_failed(&runtime, &id, "failure").unwrap();
    host.owning
        .execute(
            "UPDATE pr_publish_attempts SET input_revision=NULL WHERE action_run_id=?1",
            (id.clone(),),
        )
        .await
        .unwrap();
    *host.head.lock().unwrap() = "new".into();
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
    // The SQL builtin marker alone must never authorize an ordinary action.
    host.owning.execute_script("UPDATE executions SET snapshot=replace(snapshot,'\"nodeType\":\"pr\"','\"nodeType\":\"action\"') WHERE id='exec'").await.unwrap();
    assert!(
        pr_publish_attempts::validate_pr_publish_retry(&runtime, &id)
            .await
            .unwrap_err()
            .contains("Only a PR")
    );
    host.owning.execute_script("UPDATE executions SET snapshot=replace(snapshot,'\"nodeType\":\"action\"','\"nodeType\":\"pr\"'),lifecycle='superseded' WHERE id='exec'").await.unwrap();
    assert!(
        pr_publish_attempts::validate_pr_publish_retry(&runtime, &id)
            .await
            .unwrap_err()
            .contains("superseded")
    );
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn concurrent_pr_rearm_claims_one_attempt() {
    let (runtime, host, _dir, id) = pr_fixture(false).await;
    advancement::mark_action_run_failed(&runtime, &id, "failure").unwrap();
    *host.head.lock().unwrap() = "changed".into();
    let first = runtime.clone();
    let second = runtime.clone();
    let a = std::thread::spawn(move || advancement::rearm_failed_pr_actions(&first, "exec"));
    let b = std::thread::spawn(move || advancement::rearm_failed_pr_actions(&second, "exec"));
    let results = (a.join().unwrap().unwrap(), b.join().unwrap().unwrap());
    assert_eq!(results.0.len() + results.1.len(), 1);
    assert_eq!(
        pr_publish_attempts::list_publish_attempts(&host.owning, &id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind='pr_publish' AND state!='done'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn initial_pr_revision_failure_keeps_a_durable_dispatch_and_unknown_baseline() {
    let (runtime, host, _dir) = fixture(false).await;
    *host.head.lock().unwrap() = "revision-unavailable".into();
    let mut pr = node("pr", "pr");
    pr["actionConfig"] = serde_json::json!({});
    seed(
        &host.owning,
        vec![node("trigger", "trigger"), pr],
        vec![edge("trigger", "control-out", "pr")],
    )
    .await;
    let (_, effects) = effects::dag::reduce_dag(&runtime, "exec").await.unwrap();
    let id = effects
        .iter()
        .find_map(|e| match e {
            WorkflowEffect::ExecuteAction { action_run_id, .. } => Some(action_run_id.clone()),
            _ => None,
        })
        .unwrap();
    let attempt = pr_publish_attempts::latest_publish_attempt(&host.owning, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(attempt.input_revision.is_none());
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind='pr_publish' AND state='pending'"
        )
        .await,
        1
    );
    assert!(pr_publish_attempts::claim_dispatch(&runtime, &id)
        .await
        .unwrap());
    advancement::mark_action_run_failed(&runtime, &id, "failed before publication").unwrap();
    *host.head.lock().unwrap() = "now-known".into();
    assert!(advancement::rearm_failed_pr_actions(&runtime, "exec")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn initial_pr_insert_failure_acknowledges_unclaimed_intent() {
    let (runtime, host, _dir) = fixture(false).await;
    let mut pr = node("pr", "pr");
    pr["actionConfig"] = serde_json::json!({});
    seed(
        &host.owning,
        vec![node("trigger", "trigger"), pr],
        vec![edge("trigger", "control-out", "pr")],
    )
    .await;
    let node = advancement::load_nodes_from_execution(host.owning.clone(), "exec")
        .unwrap()
        .into_iter()
        .find(|n| n.id == "pr")
        .unwrap();
    assert!(advancement::create_action_run(&runtime, "missing-execution", &node).is_err());
    assert_eq!(
        count(&host.owning, "SELECT COUNT(*) FROM action_runs").await,
        0
    );
    assert_eq!(
        count(&host.owning, "SELECT COUNT(*) FROM pr_publish_attempts").await,
        0
    );
    assert_eq!(
        count(
            &runtime.private_db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind='pr_publish' AND state='done'"
        )
        .await,
        1
    );
}
