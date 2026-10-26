//! The operator's durable on/off choice for a job's native provider goal, and
//! the goal state the job pane's toggle shows.
//!
//! A Codex job can carry a native goal: once a turn ends, Codex starts the next
//! one by itself. Whether it may is the operator's call, recorded on
//! `jobs.goal_intent` so it survives process replacement, window rotation,
//! compaction, and restart. Only the operator changes it: the toggle, or Stop,
//! which turns it off. A delivered message never does; it runs its own turn and
//! the goal stays as the operator left it.
//!
//! The live pause claim (`GoalPause`) is separate and records why the provider
//! goal is paused right now. Every path that could lift that pause consults the
//! intent first, so with the goal off nothing but the operator restarts it.

use super::*;
use crate::agent_process::process::{GoalPause, GoalPauseOwner, JobPauseState};
use crate::backends::stdin::GoalObservation;
use crate::storage::run_db_blocking;
use cairn_models::wire::codex_app_server::goal::GoalStatus;

/// Whether a job's native goal may continue on its own after a turn ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GoalIntent {
    On,
    Off,
}

impl GoalIntent {
    fn as_column(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Off => "off",
        }
    }

    fn from_column(value: &str) -> Self {
        if value == "off" {
            Self::Off
        } else {
            Self::On
        }
    }
}

/// What the toggle shows for a job's goal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GoalDisplay {
    /// Codex continues on its own after a turn ends.
    On,
    /// The operator turned it off, or pressed Stop.
    Off,
    /// Paused by a condition that lifts by itself.
    Waiting,
    /// Paused, and Cairn holds no record of why.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobGoalState {
    /// Whether the job's current session is a Codex thread carrying a native
    /// goal. Claude sessions and standing threads have none to show.
    pub visible: bool,
    pub intent: GoalIntent,
    pub state: GoalDisplay,
    /// For `waiting`, what the goal waits on, phrased to follow "Waiting on".
    pub waiting_on: Option<String>,
}

struct GoalFacts {
    status: String,
    session_id: Option<String>,
    backend: Option<String>,
    intent: GoalIntent,
    carries_goal: bool,
}

async fn goal_facts_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<GoalFacts>> {
    let mut rows = conn
        .query(
            "SELECT j.status, j.current_session_id, j.goal_intent, s.backend
               FROM jobs j
               LEFT JOIN sessions s ON s.id = j.current_session_id
              WHERE j.id = ?1",
            (job_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let status = row.text(0)?;
    let session_id = row.opt_text(1)?;
    let intent = GoalIntent::from_column(&row.text(2)?);
    let backend = row.opt_text(3)?;
    // A standing thread's objective is `None` by construction, so this one
    // read also hides the toggle for them.
    let carries_goal = crate::todos::base_goal_objective_for_job_conn(conn, job_id)
        .await?
        .is_some();
    Ok(Some(GoalFacts {
        status,
        session_id,
        backend,
        intent,
        carries_goal,
    }))
}

fn load_goal_facts(orch: &Orchestrator, job_id: &str) -> Result<Option<GoalFacts>, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move { goal_facts_conn(conn, &job_id).await })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

/// The intent governing a job's goal.
pub(crate) fn goal_intent_for_job(orch: &Orchestrator, job_id: &str) -> Result<GoalIntent, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        db.query_opt_text("SELECT goal_intent FROM jobs WHERE id = ?1", (job_id,))
            .await
            .map(|value| value.map_or(GoalIntent::On, |value| GoalIntent::from_column(&value)))
            .map_err(|error| error.to_string())
    })
}

/// The intent governing a run's goal. A run with no job (a project chat) has no
/// toggle and no goal to hold, so it reads as on.
pub(crate) fn goal_intent_for_run(orch: &Orchestrator, run_id: &str) -> Result<GoalIntent, String> {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|error| error.to_string())?;
        db.query_opt_text(
            "SELECT j.goal_intent FROM runs r JOIN jobs j ON j.id = r.job_id WHERE r.id = ?1",
            (run_id,),
        )
        .await
        .map(|value| value.map_or(GoalIntent::On, |value| GoalIntent::from_column(&value)))
        .map_err(|error| error.to_string())
    })
}

/// Persist the operator's choice and tell watchers the job changed.
pub(crate) fn record_goal_intent(
    orch: &Orchestrator,
    job_id: &str,
    intent: GoalIntent,
) -> Result<(), String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    let change = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        db.execute(
            "UPDATE jobs SET goal_intent = ?1 WHERE id = ?2",
            (intent.as_column(), job_id.as_str()),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(crate::notify::job_db_change_for_id(&db, &job_id, "update").await)
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(())
}

/// The goal state the job pane's toggle shows.
pub fn job_goal_state(orch: &Orchestrator, job_id: &str) -> Result<JobGoalState, String> {
    let facts = load_goal_facts(orch, job_id)?.ok_or_else(|| format!("Job {job_id} not found"))?;
    goal_state_from_facts(orch, &facts)
}

fn goal_state_from_facts(orch: &Orchestrator, facts: &GoalFacts) -> Result<JobGoalState, String> {
    let hidden = JobGoalState {
        visible: false,
        intent: facts.intent,
        state: match facts.intent {
            GoalIntent::On => GoalDisplay::On,
            GoalIntent::Off => GoalDisplay::Off,
        },
        waiting_on: None,
    };
    if facts.backend.as_deref() != Some("codex") || !facts.carries_goal {
        return Ok(hidden);
    }
    let shown = |state, waiting_on| JobGoalState {
        visible: true,
        intent: facts.intent,
        state,
        waiting_on,
    };
    if facts.intent == GoalIntent::Off {
        return Ok(shown(GoalDisplay::Off, None));
    }
    let live_run = facts
        .session_id
        .as_deref()
        .and_then(|session_id| orch.process_state.find_process_by_session(session_id));
    let Some(run_id) = live_run else {
        // No process yet: the next launch opens with the goal running.
        return Ok(shown(GoalDisplay::On, None));
    };
    match crate::backends::stdin::inspect_goal(&orch.process_state, &run_id)? {
        // A live thread without a goal has nothing for the toggle to control.
        None => Ok(hidden),
        Some(observation) => {
            let (state, waiting_on) = describe_goal(observation);
            Ok(shown(state, waiting_on))
        }
    }
}

fn describe_goal(observation: GoalObservation) -> (GoalDisplay, Option<String>) {
    let waiting = |on: &str| (GoalDisplay::Waiting, Some(on.to_string()));
    match observation.status {
        GoalStatus::Active => (GoalDisplay::On, None),
        GoalStatus::Paused => match observation.claim {
            None => (GoalDisplay::Unknown, None),
            Some(GoalPause::Owned(owner)) => waiting(match owner {
                GoalPauseOwner::DurableWait { .. } => "a durable wait",
                GoalPauseOwner::DependencyWait { .. } => "a delegated child",
                GoalPauseOwner::UserInput { .. } => "an answer to its question",
            }),
            Some(GoalPause::JobState(JobPauseState::Idle)) => {
                waiting("the job to run again (send it a message)")
            }
            Some(GoalPause::JobState(JobPauseState::Blocked)) => waiting("the job to be unblocked"),
            Some(GoalPause::Interrupted) => {
                waiting("the job to run again after a stop (send it a message)")
            }
            Some(GoalPause::OperatorCompact) => waiting("compaction to finish"),
        },
        GoalStatus::Blocked => waiting("Codex, which reports the goal blocked"),
        GoalStatus::UsageLimited => waiting("usage limits to reset"),
        GoalStatus::BudgetLimited => waiting("more token budget"),
        GoalStatus::Complete => waiting("nothing: the goal is marked complete"),
    }
}

/// The operator's toggle. Off pauses the live goal now; On reactivates it when
/// the job is runnable, and otherwise leaves it waiting on whatever holds it,
/// which then lifts by itself.
pub fn set_job_goal_intent(
    orch: &Orchestrator,
    job_id: &str,
    intent: GoalIntent,
) -> Result<JobGoalState, String> {
    let facts = load_goal_facts(orch, job_id)?.ok_or_else(|| format!("Job {job_id} not found"))?;
    record_goal_intent(orch, job_id, intent)?;
    let live_run = facts
        .session_id
        .as_deref()
        .and_then(|session_id| orch.process_state.find_process_by_session(session_id));
    if let Some(run_id) = live_run {
        let (runnable, job_pause) = match (intent, facts.status.as_str()) {
            (_, "running" | "pending") => (JobRunnable::Attested, None),
            (GoalIntent::On, "idle") => (JobRunnable::Unknown, Some(JobPauseState::Idle)),
            (GoalIntent::On, "blocked") => (JobRunnable::Unknown, Some(JobPauseState::Blocked)),
            _ => (JobRunnable::Unknown, None),
        };
        crate::backends::stdin::apply_goal_intent(
            &orch.process_state,
            &run_id,
            intent,
            runnable,
            job_pause,
        )?;
    }
    job_goal_state(orch, job_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};
    use serde_json::Value;
    use std::sync::Mutex;

    type Requests = Arc<Mutex<Vec<Value>>>;

    async fn goal_env(job_status: &str, backend: &str) -> (Orchestrator, Arc<LocalDb>) {
        let db = Arc::new(migrated_test_db("goal-intent").await);
        db.execute_script(&format!(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,attention,created_at,updated_at)
               VALUES('i','p',1,'Ship it','active','none',1,1);
             INSERT INTO jobs(id,issue_id,project_id,status,node_name,current_session_id,created_at,updated_at)
               VALUES('j','i','p','{job_status}','builder','s',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('s','j','{backend}','native-thread','open',1,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,status,session_id,created_at,updated_at)
               VALUES('r','p','i','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t','s','r','j',1,'running','initial',1,1);
             UPDATE jobs SET current_turn_id='t' WHERE id='j';"
        ))
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            db.clone(),
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        let orch = Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root)
            .build();
        (orch, db)
    }

    /// A Codex control whose provider goal is a live, mutable status.
    fn codex_control(
        status: &str,
    ) -> (
        Box<dyn crate::agent_process::process::SessionControl>,
        Arc<Mutex<String>>,
        Requests,
    ) {
        let goal = Arc::new(Mutex::new(status.to_string()));
        let provider_goal = goal.clone();
        let reply = |status: &str| {
            Ok(serde_json::json!({"result": {"goal": {
                "threadId": "native-thread", "objective": "Ship it", "status": status,
                "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
                "createdAt": 1, "updatedAt": 2
            }}}))
        };
        let requests: Requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(
            cairn_models::codex::app_server::AppServerClient::for_test_responding(
                move |request| match request["method"].as_str().unwrap() {
                    "thread/goal/get" => reply(&provider_goal.lock().unwrap()),
                    "thread/goal/set" => {
                        let status = request["params"]["status"].as_str().unwrap().to_string();
                        *provider_goal.lock().unwrap() = status.clone();
                        reply(&status)
                    }
                    "turn/interrupt" => Ok(Value::Null),
                    method => Err(format!("unexpected goal fixture method: {method}")),
                },
                requests.clone(),
            ),
        );
        let control = Box::new(cairn_models::session::codex::CodexControl::new(
            client,
            "native-thread".into(),
            "/tmp/p".into(),
            None,
            None,
            None,
            Arc::new(Mutex::new(Some("provider-live".into()))),
        ));
        (control, goal, requests)
    }

    fn register_codex(orch: &Orchestrator, run_id: &str, status: &str) -> Arc<Mutex<String>> {
        let (control, goal, _) = codex_control(status);
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(control))),
            Some("s".into()),
            Some("j".into()),
        );
        handle.backend = Some("codex".into());
        handle.begin_turn("t");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register(run_id.into(), handle);
        goal
    }

    fn status(goal: &Arc<Mutex<String>>) -> String {
        goal.lock().unwrap().clone()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn toggling_off_pauses_the_running_goal_and_survives_process_replacement() {
        let (orch, db) = goal_env("running", "codex").await;
        let goal = register_codex(&orch, "r", "active");
        assert_eq!(
            job_goal_state(&orch, "j").unwrap(),
            JobGoalState {
                visible: true,
                intent: GoalIntent::On,
                state: GoalDisplay::On,
                waiting_on: None,
            }
        );

        let state = set_job_goal_intent(&orch, "j", GoalIntent::Off).unwrap();
        assert_eq!(state.state, GoalDisplay::Off);
        assert_eq!(status(&goal), "paused");
        assert_eq!(
            db.query_opt_text("SELECT goal_intent FROM jobs WHERE id='j'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("off")
        );

        // The process is replaced: a new run of the same job opens a fresh
        // thread whose goal the provider starts active.
        orch.process_state.processes.lock().unwrap().remove("r");
        db.execute(
            "INSERT INTO runs(id,project_id,issue_id,job_id,status,session_id,created_at,updated_at)
             VALUES('r2','p','i','j','live','s',2,2)",
            (),
        )
        .await
        .unwrap();
        let objective = cairn_models::interface::session::NativeGoal::Objective("Ship it".into());
        let (mut control, relaunched, _) = codex_control("active");
        crate::backends::codex::apply_goal_intent_at_launch(
            &orch,
            "r2",
            &objective,
            control.as_mut(),
        )
        .unwrap();
        assert_eq!(status(&relaunched), "paused", "the relaunch starts paused");

        // With the goal on, a launch leaves the provider's goal alone.
        record_goal_intent(&orch, "j", GoalIntent::On).unwrap();
        let (mut control, untouched, requests) = codex_control("active");
        crate::backends::codex::apply_goal_intent_at_launch(
            &orch,
            "r2",
            &objective,
            control.as_mut(),
        )
        .unwrap();
        assert_eq!(status(&untouched), "active");
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn toggling_on_reactivates_a_runnable_job() {
        let (orch, _db) = goal_env("running", "codex").await;
        let goal = register_codex(&orch, "r", "active");
        set_job_goal_intent(&orch, "j", GoalIntent::Off).unwrap();
        assert_eq!(status(&goal), "paused");

        let state = set_job_goal_intent(&orch, "j", GoalIntent::On).unwrap();
        assert_eq!(status(&goal), "active");
        assert_eq!(state.intent, GoalIntent::On);
        assert_eq!(state.state, GoalDisplay::On);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn toggling_on_an_idle_job_waits_on_the_job_rather_than_running() {
        let (orch, _db) = goal_env("idle", "codex").await;
        let goal = register_codex(&orch, "r", "paused");
        record_goal_intent(&orch, "j", GoalIntent::Off).unwrap();

        let state = set_job_goal_intent(&orch, "j", GoalIntent::On).unwrap();
        assert_eq!(status(&goal), "paused");
        assert_eq!(state.state, GoalDisplay::Waiting);
        assert!(
            state
                .waiting_on
                .as_deref()
                .is_some_and(|on| on.contains("run again")),
            "{state:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unattributed_pause_reads_as_unknown_never_as_off() {
        let (orch, _db) = goal_env("running", "codex").await;
        register_codex(&orch, "r", "paused");
        let state = job_goal_state(&orch, "j").unwrap();
        assert_eq!(state.intent, GoalIntent::On);
        assert_eq!(state.state, GoalDisplay::Unknown);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_toggle_is_hidden_for_claude_sessions_and_standing_threads() {
        let (orch, _db) = goal_env("running", "claude").await;
        assert!(!job_goal_state(&orch, "j").unwrap().visible);

        let (orch, db) = goal_env("running", "codex").await;
        db.execute_script(
            "PRAGMA foreign_keys = OFF;
             UPDATE jobs SET thread_id='thread-1', uri_segment='thread' WHERE id='j';
             PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();
        assert!(!job_goal_state(&orch, "j").unwrap().visible);
    }

    #[test]
    fn a_waiting_goal_names_what_holds_it() {
        let paused = |claim| GoalObservation {
            status: GoalStatus::Paused,
            claim,
        };
        assert_eq!(
            describe_goal(paused(Some(GoalPause::Owned(
                GoalPauseOwner::DurableWait {
                    wait_id: "w".into()
                }
            )))),
            (GoalDisplay::Waiting, Some("a durable wait".into()))
        );
        assert_eq!(
            describe_goal(paused(Some(GoalPause::JobState(JobPauseState::Blocked)))).0,
            GoalDisplay::Waiting
        );
        assert_eq!(describe_goal(paused(None)), (GoalDisplay::Unknown, None));
        assert_eq!(
            describe_goal(GoalObservation {
                status: GoalStatus::Active,
                claim: Some(GoalPause::Interrupted),
            }),
            (GoalDisplay::On, None)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn operator_stop_turns_the_goal_off() {
        let (orch, _db) = goal_env("running", "codex").await;
        let goal = register_codex(&orch, "r", "active");
        orch.process_state.set_current_turn_id("r", Some("t"));

        crate::orchestrator::lifecycle::operator_stop_job(&orch, "j").unwrap();

        assert_eq!(goal_intent_for_job(&orch, "j").unwrap(), GoalIntent::Off);
        assert_eq!(status(&goal), "paused");
        assert_eq!(job_goal_state(&orch, "j").unwrap().state, GoalDisplay::Off);
    }
}
