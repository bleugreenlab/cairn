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
    /// The stored `output` an action completes with; `None` stores none.
    action_output: Mutex<Option<String>>,
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
    async fn pr_publish_input_revision(&self, _: &str) -> Result<Option<String>, String> {
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
    fn resume_parent(&self, _: &str, _: crate::runtime::ParentResume) -> Result<(), String> {
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
                let output = self.action_output.lock().unwrap().clone();
                self.owning
                    .execute(
                        "UPDATE action_runs SET status = 'complete', output = ?2 WHERE id = ?1",
                        params![action_run_id, output.as_deref()],
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
        action_output: Mutex::new(None),
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
/// Two runs of one checkpoint record at once. Each counts the job's rows and
/// inserts the next attempt as its own row, so only a key over (job, attempt)
/// makes them conflict; without it both record the same attempt and the cap
/// undercounts (cairn/6166). Looped because the interleaving is a matter of
/// luck on any one round.
#[test]
fn racing_checkpoint_records_never_reuse_an_attempt_number() {
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (runtime, host, _dir) = tokio.block_on(fixture(false));
    tokio.block_on(async {
        seed(&host.owning, vec![], vec![]).await;
        host.owning
            .execute_script(
                "INSERT INTO jobs(id,project_id,issue_id,execution_id,uri_segment,status,created_at,updated_at)
                 VALUES('job','p','issue','exec','checkpoint','running',1,1);",
            )
            .await
            .unwrap();
    });
    let output = crate::conditions::CheckpointRunOutput {
        passed: false,
        exit_code: 1,
        stdout: String::new(),
        stderr: String::new(),
    };
    for round in 0..100_i64 {
        let (runtime, output) = (&runtime, &output);
        let mut attempts: Vec<i64> = storage::race_on_threads((0..2).map(|_| {
            move || {
                crate::checkpoint_runs::record_checkpoint_run(runtime, "job", "false", None, output)
                    .unwrap()
            }
        }));
        attempts.sort();
        assert_eq!(
            attempts,
            vec![2 * round + 1, 2 * round + 2],
            "round {round}: each run records its own attempt"
        );
    }
}

fn advance(id: Option<String>) -> WorkflowEffect {
    WorkflowEffect::AdvanceDag {
        execution_id: "exec".into(),
        outbox_entry_id: id,
    }
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
        usage: cairn_domain::decision::input_usage(Some(12), None),
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

fn pool(id: &str) -> Option<Vec<crate::models::allotment::AllotmentEntry>> {
    Some(vec![crate::models::allotment::AllotmentEntry::Pool(
        id.to_string(),
    )])
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
        DecisionNode::port_choice("Does it touch the database?", &ports(), pool("md"))
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

async fn pr_fixture(team: bool) -> (WorkflowRuntime, Arc<Host>, tempfile::TempDir, String) {
    pr_fixture_with_review(team, false).await
}

async fn pr_fixture_with_review(
    team: bool,
    with_review: bool,
) -> (WorkflowRuntime, Arc<Host>, tempfile::TempDir, String) {
    let (runtime, host, dir) = fixture(team).await;
    let mut pr = node("pr", "pr");
    pr["actionConfig"] = serde_json::json!({});
    let mut nodes = vec![node("trigger", "trigger"), pr];
    let mut edges = vec![edge("trigger", "control-out", "pr")];
    if with_review {
        nodes.push(node("review", "agent"));
        edges.push(edge("pr", "open", "review"));
    }
    seed(&host.owning, nodes, edges).await;
    if with_review {
        host.owning.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('review-job','exec','review','issue','p','pending','Review','review',1,1)").await.unwrap();
    }
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
async fn failed_pr_publication_retry_revives_review_and_advances_resolution_once() {
    for team in [false, true] {
        let (runtime, host, _dir, id) = pr_fixture_with_review(team, true).await;
        let generation = count(
            &host.owning,
            "SELECT resolution_generation FROM issues WHERE id='issue'",
        )
        .await;
        advancement::mark_action_run_failed(&runtime, &id, "push rejected").unwrap();
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='failed'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM executions WHERE id='exec' AND status='failed'"
            )
            .await,
            1
        );
        assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM issues WHERE id='issue' AND status='failed' AND completed_at IS NOT NULL").await, 1);
        assert_eq!(
            count(
                &host.owning,
                "SELECT resolution_generation FROM issues WHERE id='issue'"
            )
            .await,
            generation
        );

        pr_publish_attempts::retry_pr_publish(&runtime, &id)
            .await
            .unwrap();
        for _ in 0..2 {
            advancement::recompute_execution_jobs(&runtime, "exec").unwrap();
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='pending'"
                )
                .await,
                1
            );
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT COUNT(*) FROM executions WHERE id='exec' AND status='running'"
                )
                .await,
                1
            );
            assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM issues WHERE id='issue' AND status='active' AND completed_at IS NULL").await, 1);
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT resolution_generation FROM issues WHERE id='issue'"
                )
                .await,
                generation + 1
            );
        }
        assert!(
            effects::dag::reduce_dag(&runtime, "exec")
                .await
                .unwrap()
                .0
                .is_empty(),
            "Review must still wait for the open port"
        );
        assert!(pr_publish_attempts::claim_dispatch(&runtime, &id)
            .await
            .unwrap());
        // Mirror the host's successful publication boundary: open port first,
        // then the blocked merge/close gate and the settled publication attempt.
        ports::fire_pr_node_port(&host.owning, "exec", "pr", "open")
            .await
            .unwrap();
        host.owning
            .write(|conn| {
                let id = id.clone();
                Box::pin(async move {
                    conn.execute(
                        "UPDATE action_runs SET status='blocked',error_message=NULL WHERE id=?1",
                        (id.as_str(),),
                    )
                    .await?;
                    pr_publish_attempts::settle_publish_attempt_conn(
                        conn,
                        &id,
                        "published",
                        "PR published",
                    )
                    .await
                })
            })
            .await
            .unwrap();
        pr_publish_attempts::acknowledge_settled_dispatch(&runtime, &id)
            .await
            .unwrap();
        for _ in 0..2 {
            advancement::recompute_execution_jobs(&runtime, "exec").unwrap();
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='pending'"
                )
                .await,
                1
            );
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT COUNT(*) FROM executions WHERE id='exec' AND status='running'"
                )
                .await,
                1
            );
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT COUNT(*) FROM issues WHERE id='issue' AND status='failed'"
                )
                .await,
                0
            );
            assert_eq!(
                count(
                    &host.owning,
                    "SELECT resolution_generation FROM issues WHERE id='issue'"
                )
                .await,
                generation + 1
            );
        }
        let (ready, effects) = effects::dag::reduce_dag(&runtime, "exec").await.unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "review-job");
        assert_eq!(ready[0].recipe_node_id.as_deref(), Some("review"));
        assert!(effects
            .iter()
            .all(|effect| !matches!(effect, WorkflowEffect::ExecuteAction { .. })));
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='running'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM action_runs WHERE status='blocked'"
            )
            .await,
            1
        );
        let attempts = pr_publish_attempts::list_publish_attempts(&host.owning, &id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!((attempts[0].cycle, attempts[0].attempt), (2, 1));
        assert_eq!(attempts[0].outcome.as_deref(), Some("published"));
        assert_eq!(attempts[1].outcome.as_deref(), Some("failed"));
        assert_eq!(attempts[1].detail.as_deref(), Some("push rejected"));
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM turns WHERE job_id='review-job'"
            )
            .await,
            0,
            "inherited failure must not manufacture a failed Review turn"
        );
    }
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

async fn seed_failed_builder_dag(db: &LocalDb, with_planner: bool) {
    let mut nodes = vec![
        node("trigger", "trigger"),
        node("builder", "agent"),
        node("pr", "pr"),
        node("review", "agent"),
    ];
    let mut edges = vec![
        edge("trigger", "control-out", "builder"),
        edge("builder", "control-out", "pr"),
        edge("pr", "open", "review"),
    ];
    if with_planner {
        nodes.push(node("planner", "agent"));
        edges[0] = edge("trigger", "control-out", "planner");
        edges.push(edge("planner", "control-out", "builder"));
    }
    seed(db, nodes, edges).await;
    db.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,started_at,created_at,updated_at) VALUES('builder-job','exec','builder','issue','p','running','builder','builder',1,1,1);
        INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('review-job','exec','review','issue','p','pending','review','review',1,1);
        INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at) VALUES('failure-turn','builder-session','builder-job',1,'failed','initial',2,3,3);").await.unwrap();
    if with_planner {
        db.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,started_at,created_at,updated_at) VALUES('planner-job','exec','planner','issue','p','complete','planner','planner',1,1,1);
            INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at) VALUES('planner-turn','planner-session','planner-job',1,'complete','initial',1,2,2);").await.unwrap();
    }
}

#[tokio::test]
async fn failed_builder_through_unstarted_pr_settles_review_execution_and_issue() {
    for with_planner in [false, true] {
        let (_runtime, host, _dir) = fixture(false).await;
        seed_failed_builder_dag(&host.owning, with_planner).await;
        let outcome = host
            .owning
            .write(|conn| {
                Box::pin(async move {
                    advancement::recompute_execution_jobs_conn(conn, "exec", &HashSet::new()).await
                })
            })
            .await
            .unwrap();
        assert_eq!(
            count(&host.owning, "SELECT COUNT(*) FROM action_runs").await,
            0
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='failed'"
            )
            .await,
            1,
            "review behind a PR with no action row must inherit the failure"
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM executions WHERE id='exec' AND status='failed'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM issues WHERE id='issue' AND status='failed'"
            )
            .await,
            1
        );
        assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
    }
}

#[tokio::test]
async fn failure_recording_rolls_back_or_settles_every_projection_atomically() {
    for team in [false, true] {
        let (_runtime, host, _dir) = fixture(team).await;
        seed_failed_builder_dag(&host.owning, true).await;
        host.owning
            .execute(
                "UPDATE turns SET state='running',ended_at=NULL WHERE id='failure-turn'",
                (),
            )
            .await
            .unwrap();
        let failed = host
            .owning
            .write(|conn| {
                Box::pin(async move {
                    advancement::record_job_failure_conn(
                        conn,
                        "builder-job",
                        "launch failed: executable missing",
                        &HashSet::new(),
                    )
                    .await?;
                    Err::<(), _>(storage::DbError::internal("simulated crash before commit"))
                })
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM turns WHERE id='failure-turn' AND state='running' AND failure_reason IS NULL").await, 1);
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='pending'"
            )
            .await,
            1
        );
        let (_, outcome) = host
            .owning
            .write(|conn| {
                Box::pin(async move {
                    advancement::record_job_failure_conn(
                        conn,
                        "builder-job",
                        &"launch failure ".repeat(200),
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE status='failed'"
            )
            .await,
            2
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM issues WHERE status='failed'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM turns WHERE state='failed' AND length(failure_reason) <= 512"
            )
            .await,
            1
        );
        let inherited = outcome
            .changes
            .iter()
            .find(|change| change.job_id == "review-job")
            .unwrap();
        assert_eq!(inherited.cause, advancement::ChangeCause::InheritedFailure);
        assert!(!inherited.is_announced());
        assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
    }
}

#[tokio::test]
async fn quiet_dependency_healing_preserves_unproven_stopped_held_and_malformed_work() {
    let controls = [
        "UPDATE turns SET state='cancelled' WHERE id='failure-turn'; UPDATE jobs SET status='pending' WHERE id='builder-job';",
        "UPDATE turns SET state='yielded',yield_reason='provider_limit' WHERE id='failure-turn'; UPDATE jobs SET current_turn_id='failure-turn' WHERE id='builder-job';",
        "DELETE FROM turns;",
        "UPDATE executions SET snapshot='{broken';",
        "UPDATE executions SET lifecycle='superseded';",
        "UPDATE executions SET lifecycle='pending';",
        "INSERT INTO execution_gates(id,execution_id,seq,predicate_json,created_at) VALUES('gate','exec',0,'{\"fact\":\"fleet_slot\"}',1);"
    ];
    for sql in controls {
        let (_runtime, host, _dir) = fixture(false).await;
        seed_failed_builder_dag(&host.owning, false).await;
        host.owning.execute_script(sql).await.unwrap();
        let outcome = host
            .owning
            .write(|conn| {
                Box::pin(async move {
                    advancement::recompute::heal_failed_dependencies_conn(
                        conn,
                        "exec",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        assert!(outcome.changes.is_empty(), "{sql}");
        assert!(outcome.newly_terminal_issue_ids.is_empty());
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='pending'"
            )
            .await,
            1,
            "{sql}"
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM issues WHERE quiet_failure_generation IS NOT NULL"
            )
            .await,
            0,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn viable_parallel_work_delays_resolution_until_its_own_failure() {
    let (_runtime, host, _dir) = fixture(false).await;
    seed_failed_builder_dag(&host.owning, true).await;
    let raw = host
        .owning
        .query_opt_text("SELECT snapshot FROM executions WHERE id='exec'", ())
        .await
        .unwrap()
        .unwrap();
    let mut snapshot: serde_json::Value = serde_json::from_str(&raw).unwrap();
    snapshot["recipe"]["nodes"]
        .as_array_mut()
        .unwrap()
        .push(node("parallel", "agent"));
    snapshot["recipe"]["edges"]
        .as_array_mut()
        .unwrap()
        .push(edge("trigger", "control-out", "parallel"));
    host.owning
        .execute(
            "UPDATE executions SET snapshot=?1 WHERE id='exec'",
            (snapshot.to_string(),),
        )
        .await
        .unwrap();
    host.owning.execute_script("INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,created_at,updated_at) VALUES('parallel-job','exec','parallel','issue','p','pending','parallel','parallel',1,1);").await.unwrap();
    let first = host
        .owning
        .write(|conn| {
            Box::pin(async move {
                advancement::recompute_execution_jobs_conn(conn, "exec", &HashSet::new()).await
            })
        })
        .await
        .unwrap();
    assert!(first.newly_terminal_issue_ids.is_empty());
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE id='parallel-job' AND status='pending'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM executions WHERE status='running'"
        )
        .await,
        1
    );
    let (_, last) = host
        .owning
        .write(|conn| {
            Box::pin(async move {
                advancement::record_job_failure_conn(
                    conn,
                    "parallel-job",
                    "independent parallel failure",
                    &HashSet::new(),
                )
                .await
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.newly_terminal_issue_ids, ["issue"]);
    assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM issues WHERE status='failed' AND quiet_failure_generation IS NULL").await, 1);
}

#[tokio::test]
async fn a_run_only_failure_settles_its_unreachable_descendants() {
    let (_runtime, host, _dir) = fixture(false).await;
    seed_failed_builder_dag(&host.owning, false).await;
    host.owning.execute_script("DELETE FROM turns;
        INSERT INTO runs(id,job_id,project_id,status,error_message,created_at,updated_at) VALUES('failed-run','builder-job','p','crashed','process failed before first turn',2,3);").await.unwrap();
    let outcome = host
        .owning
        .write(|conn| {
            Box::pin(async move {
                advancement::recompute_execution_jobs_conn(conn, "exec", &HashSet::new()).await
            })
        })
        .await
        .unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE status='failed'"
        )
        .await,
        2
    );
    assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
}

#[tokio::test]
async fn a_failing_launch_claim_does_not_hide_its_dependency_failure() {
    let (_runtime, host, _dir) = fixture(false).await;
    seed_failed_builder_dag(&host.owning, false).await;
    host.owning
        .execute(
            "UPDATE turns SET state='pending',ended_at=NULL WHERE id='failure-turn'",
            (),
        )
        .await
        .unwrap();
    let (_, outcome) = host
        .owning
        .write(|conn| {
            Box::pin(async move {
                let claims = HashSet::from(["builder-job".to_string()]);
                advancement::record_job_failure_conn(
                    conn,
                    "builder-job",
                    "promoted launch failed",
                    &claims,
                )
                .await
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE status='failed'"
        )
        .await,
        2
    );
    assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
}

#[tokio::test]
async fn bounded_quiet_healing_advances_past_ineligible_candidates_on_later_sweeps() {
    let (runtime, host, _dir) = fixture(false).await;
    seed_failed_builder_dag(&host.owning, false).await;
    host.owning
        .execute("UPDATE jobs SET status='failed' WHERE id='builder-job'", ())
        .await
        .unwrap();
    host.owning.write(|conn| Box::pin(async move {
        for n in 0..256 {
            let execution_id = format!("a-{n:04}");
            conn.execute(
                "INSERT INTO executions(id,recipe_id,project_id,status,started_at,lifecycle,snapshot)
                 VALUES(?1,'recipe','p','running',1,'superseded','malformed')",
                (execution_id.as_str(),),
            ).await?;
            for status in ["pending", "failed"] {
                conn.execute(
                    "INSERT INTO jobs(id,execution_id,project_id,status,created_at,updated_at)
                     VALUES(?1,?2,'p',?3,1,1)",
                    params![format!("{execution_id}-{status}"), execution_id.as_str(), status],
                ).await?;
            }
        }
        Ok(())
    })).await.unwrap();
    advancement::recompute::heal_failed_dependencies(&runtime).await;
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='pending'"
        )
        .await,
        1,
        "one pass is bounded to 256 candidate executions"
    );
    advancement::recompute::heal_failed_dependencies(&runtime).await;
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM jobs WHERE id='review-job' AND status='failed'"
        )
        .await,
        1,
        "a later pass must reach the viable repair rather than restarting forever"
    );
    assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM issues WHERE status='failed' AND quiet_failure_generation=resolution_generation").await, 1);
    assert_eq!(
        count(&runtime.private_db, "SELECT COUNT(*) FROM effect_outbox").await,
        0
    );
}

#[tokio::test]
async fn failed_planner_crosses_an_already_inherited_failed_builder_and_unstarted_pr() {
    for quiet in [false, true] {
        let (_runtime, host, _dir) = fixture(false).await;
        seed_failed_builder_dag(&host.owning, true).await;
        host.owning
            .execute_script(
                "UPDATE jobs SET status='failed' WHERE id IN ('planner-job','builder-job');
             UPDATE turns SET state='failed' WHERE id='planner-turn';
             DELETE FROM turns WHERE id='failure-turn';",
            )
            .await
            .unwrap();
        let outcome = host
            .owning
            .write(|conn| {
                Box::pin(async move {
                    if quiet {
                        advancement::recompute::heal_failed_dependencies_conn(
                            conn,
                            "exec",
                            &HashSet::new(),
                        )
                        .await
                    } else {
                        advancement::recompute_execution_jobs_conn(conn, "exec", &HashSet::new())
                            .await
                    }
                })
            })
            .await
            .unwrap();
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM jobs WHERE status='failed'"
            )
            .await,
            3
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM executions WHERE status='failed'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM issues WHERE status='failed'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &host.owning,
                "SELECT COUNT(*) FROM turns WHERE state='failed'"
            )
            .await,
            1,
            "inherited failures must not acquire synthetic failed work turns"
        );
        if quiet {
            assert!(outcome.newly_terminal_issue_ids.is_empty());
            assert!(outcome
                .changes
                .iter()
                .all(|change| change.cause == advancement::ChangeCause::Repaired));
        } else {
            assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
            assert!(outcome
                .changes
                .iter()
                .all(|change| change.cause == advancement::ChangeCause::InheritedFailure));
        }
    }
}

#[tokio::test]
async fn a_live_retry_after_quiet_failure_gets_a_new_resolution_identity() {
    let (_runtime, host, _dir) = fixture(false).await;
    seed_failed_builder_dag(&host.owning, false).await;
    host.owning
        .execute("UPDATE jobs SET status='failed' WHERE id='builder-job'", ())
        .await
        .unwrap();
    host.owning
        .write(|conn| {
            Box::pin(async move {
                advancement::recompute::heal_failed_dependencies_conn(conn, "exec", &HashSet::new())
                    .await
            })
        })
        .await
        .unwrap();
    let quiet_generation = host
        .owning
        .query_one(
            "SELECT resolution_generation FROM issues WHERE id='issue'",
            (),
            |row| {
                use storage::RowExt;
                row.i64(0)
            },
        )
        .await
        .unwrap();
    host.owning.execute_script(
        "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
         VALUES('retry-after-quiet','builder-session','builder-job',2,'running','retry',4,4);"
    ).await.unwrap();
    host.owning
        .write(|conn| {
            Box::pin(async move {
                advancement::recompute_execution_jobs_conn(conn, "exec", &HashSet::new()).await
            })
        })
        .await
        .unwrap();
    assert_eq!(
        count(
            &host.owning,
            "SELECT COUNT(*) FROM issues WHERE status='active'"
        )
        .await,
        1
    );
    let resumed_generation = host
        .owning
        .query_one(
            "SELECT resolution_generation FROM issues WHERE id='issue'",
            (),
            |row| {
                use storage::RowExt;
                row.i64(0)
            },
        )
        .await
        .unwrap();
    assert_eq!(
        resumed_generation,
        quiet_generation + 1,
        "a genuine retry reopens failed work without necessarily calling explicit unresolve"
    );
    let (_, outcome) = host
        .owning
        .write(|conn| {
            Box::pin(async move {
                advancement::record_job_failure_conn(
                    conn,
                    "builder-job",
                    "independent retry failure",
                    &HashSet::new(),
                )
                .await
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.newly_terminal_issue_ids, ["issue"]);
    assert_eq!(count(&host.owning, "SELECT COUNT(*) FROM issues WHERE status='failed' AND quiet_failure_generation <> resolution_generation").await, 1, "the historical quiet marker must not suppress the retry's new failure");
}
