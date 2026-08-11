use super::review_push::{
    bounded_rearm_candidates, finish_superseded_turn_end_checks, rearm_one_bounded_failed_review,
    release_cooled_infrastructure_suppression, spawn_turn_end_checks,
};
use super::{
    create_review_push_for_pr_open, evaluate_review_readiness, record_bounded_rearm_lookup_failure,
};
use crate::db::DbState;
use crate::orchestrator::attention_push::{
    latest_push_fingerprint, list_pending, stamp_delivered, Boundary, Push, Wake,
};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, RowExt, SearchIndex};
use std::sync::Arc;

const ISSUE_URI: &str = "cairn://p/PRJ/7";
const PLANBUILD_YAML: &str = include_str!("../../../../../packs/core/recipes/planbuild.yaml");
const REVIEW_KEY: &str = "review:cairn://p/PRJ/7";

async fn test_db() -> LocalDb {
    crate::storage::migrated_test_db("review-push.db").await
}

#[tokio::test]
async fn later_requester_that_wins_single_flight_becomes_the_durable_current_owner() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let first = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist first requester");
    let winner = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist concurrent requester");

    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch, "j-prod", &winner, "claimed", None,
    )
    .expect("the later requester wins single-flight");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch,
        "j-prod",
        &first,
        "superseded",
        Some("another turn-end check attempt won the slot"),
    )
    .expect("terminalize the losing requester");

    let current =
        crate::execution::checks_turn_end::current_turn_end_attempt(&orch.db.local, "j-prod")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(current.id, winner);
    assert_eq!(current.state, "claimed");
}

#[tokio::test]
async fn cancelled_wave_rearms_after_a_dormant_nodes_base_advances() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    let stale = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("claim the stale wave's single-flight slot");
    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod").await;
    assert!(
        stale.is_cancelled(),
        "the base advance cancels the stale wave"
    );

    orch.end_turn_end_checks("j-prod");
    finish_superseded_turn_end_checks(&orch, "j-prod", stale.is_cancelled());

    assert!(
        wave_scheduled(&orch, "j-prod"),
        "a dormant PR owner receives a successor without another agent turn"
    );
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn bounded_rearm_resolves_agent_branch_from_managed_store() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let backing = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(backing.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::JjEnv::resolve(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, backing.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/CAIRN-3604-builder-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    std::fs::write(workspace.join("agent-only.rs"), "wave path\n").unwrap();
    crate::jj::seal(&jj, &workspace, "agent branch", None).unwrap();
    let commit = crate::jj::head_commit(&jj, &workspace).unwrap();
    let tree_hash = crate::jj::logical_tree_hash(&jj, &store, &commit).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();

    insert_artifact(&orch.db.local, "create-pr", 1).await;
    insert_failed_check(&orch.db.local, "infrastructure", 1).await;
    attach_planbuild_topology(&orch.db.local, "j-prod", NodeRole::Builder).await;
    orch.db
        .local
        .execute("UPDATE jobs SET status='running' WHERE id='j-prod'", ())
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1 WHERE job_id='j-prod'",
            (tree_hash.as_str(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();

    let candidate = super::review_push::BoundedRearmCandidate {
        job_id: "j-prod".to_string(),
        tree_hash,
        ran_at: 1,
    };
    assert!(
        super::review_push::bounded_candidate_matches_current_tree(&orch, &candidate)
            .await
            .unwrap(),
        "the wave path must derive the managed store and resolve its agent-only branch"
    );
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "the maintenance cadence must nominate the dormant running PR owner"
    );
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the maintenance cadence must autonomously dispatch a fresh wave"
    );
}

#[tokio::test]
async fn bounded_rearm_lookup_failure_becomes_visible_infrastructure_evidence() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_failed_check(&db, "capacity", 1).await;
    let orch = test_orchestrator(db);

    record_bounded_rearm_lookup_failure(&orch, "j-prod", "object database unavailable")
        .await
        .unwrap();

    let row = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT failure_kind, output_tail, infra_failure_streak
                           FROM check_result_cache WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                let row = rows.next().await?.expect("check result");
                Ok::<_, crate::storage::DbError>((row.text(0)?, row.text(1)?, row.i64(2)?))
            })
        })
        .await
        .unwrap();
    assert_eq!(row.0, "infrastructure");
    assert!(row.1.contains("object database unavailable"), "{}", row.1);
    assert_eq!(row.2, 2);
}

/// `check_result_cache` holds two row families, and the wave-preparation repaint
/// must only ever touch one of them. The other is the hot projection of an
/// immutable observation: painting `infrastructure` over a green would leave the
/// row contradicting the very observation it names, and would strip that green of
/// reuse, because the reusable lookup requires a null `failure_kind`.
#[tokio::test]
async fn bounded_rearm_lookup_failure_leaves_a_recorded_verdict_alone() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_failed_check(&db, "capacity", 1).await;
    // A genuine green for a DIFFERENT check on the same job: a real immutable
    // observation, plus the hot row that projects it. This is the exact shape the
    // repaint must not touch — the row names the observation, so contradicting it
    // would make the cache disagree with the evidence it points at.
    db.execute_script(
        "INSERT INTO check_result_observations
           (id, project_id, commit_sha, tree_hash, check_name, input_hash,
            environment_fingerprint, exit_code, verdict, complete, reusable,
            parser_version, result_schema_version, ran_at, duration_ms, job_id,
            cadence, output_tail)
         VALUES('obs-1','p-rev','commit','tree','typecheck','input-tc','env-a',0,'passed',1,1,
                1,1,1,5,'j-prod','review','ok');
         INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, environment_fingerprint,
            result_schema_version, source_observation_id, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','input-tc','typecheck','env-a',1,'obs-1',0,1,'ok',5,1,'j-prod',NULL,0);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    record_bounded_rearm_lookup_failure(&orch, "j-prod", "object database unavailable")
        .await
        .unwrap();

    let rows = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT check_name, passed, COALESCE(failure_kind,''), infra_failure_streak
                           FROM check_result_cache WHERE job_id='j-prod'
                          ORDER BY check_name",
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.i64(1)?, row.text(2)?, row.i64(3)?));
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("review".to_string(), 0, "infrastructure".to_string(), 2),
            ("typecheck".to_string(), 1, String::new(), 0),
        ],
        "the infra row takes the failure and the streak; the recorded verdict is untouched"
    );
}

#[tokio::test]
async fn bounded_rearm_selects_retryable_review_work() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    insert_failed_check(&db, "infrastructure", 0).await;

    let orch = test_orchestrator(db);
    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap()[0].job_id,
        "j-prod",
        "transient infrastructure failures are eligible below the retry bound"
    );

    orch.db
        .local
        .execute("UPDATE jobs SET status='running' WHERE id='j-prod'", ())
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            // `ran_at` is Unix MILLISECONDS; ten minutes back is past the cooldown.
            "UPDATE check_result_cache SET infra_failure_streak=?1, ran_at=(unixepoch()-600)*1000",
            (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,),
        )
        .await
        .unwrap();
    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap()[0].job_id,
        "j-prod",
        "a dormant open-PR owner is retried after the bounded cooldown even while its job remains running"
    );

    orch.db
        .local
        .execute("UPDATE turns SET state='running' WHERE id='t-prod'", ())
        .await
        .unwrap();
    assert!(
        bounded_rearm_candidates(&orch.db).await.unwrap().is_empty(),
        "a live agent turn must never be mistaken for dormant review work"
    );
    orch.db
        .local
        .execute("UPDATE turns SET state='complete' WHERE id='t-prod'", ())
        .await
        .unwrap();
    release_cooled_infrastructure_suppression(&orch, "j-prod")
        .await
        .unwrap();
    let streak: i64 = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT infra_failure_streak FROM check_result_cache WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                rows.next().await?.expect("check result").i64(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(streak, 0, "the paced re-arm must admit a fresh execution");

    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET infra_failure_streak=?1, ran_at=unixepoch()*1000",
            (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,),
        )
        .await
        .unwrap();
    assert!(
        bounded_rearm_candidates(&orch.db).await.unwrap().is_empty(),
        "the attempt bound remains a hot-loop circuit breaker during the cooldown"
    );

    orch.db
        .local
        .execute_script(
            "UPDATE turns SET start_reason='memory_review' WHERE id='t-prod';
             INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment,
                              node_name, branch, created_at, updated_at)
               VALUES('j-later','e-rev','p-rev','i-rev','running','later','builder','later',2,2);
             INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES('t-later','s-later','j-later',1,'complete','follow_up',2,2);
             INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name,
                confirmed, created_at, updated_at)
               VALUES('a-later','j-later','create-pr',1,'{}',1,'create-pr',1,2,2);
             INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
               VALUES('p-rev','tree','later-input','review',-1,0,'failed',0,2,
                      'j-later','infrastructure',1);",
        )
        .await
        .unwrap();
    let candidates = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.job_id.as_str())
            .collect::<Vec<_>>(),
        vec!["j-later"],
        "a completed memory-review head must not starve later dormant PR recovery"
    );
}

#[tokio::test]
async fn stale_bounded_input_is_not_a_rearm_candidate_after_a_newer_result() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    insert_failed_check(&db, "capacity", 1).await;
    db.execute_script(
        "UPDATE check_result_cache SET ran_at=1;
         INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','new-input','review',0,1,'passed',1,1,'j-prod',NULL,0);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap(),
        Vec::new(),
        "rowid breaks equal-second ties, so an older capacity input on the same tree cannot remain latest"
    );
}

#[tokio::test]
async fn bounded_rearm_enumerates_open_team_databases() {
    let local = test_db().await;
    let team = test_db().await;
    seed(&team, "initial").await;
    insert_artifact(&team, "create-pr", 1).await;
    insert_failed_check(&team, "capacity", 1).await;
    let orch = test_orchestrator(local);
    orch.db
        .register_team_db_for_test("team-a".to_string(), Arc::new(team))
        .await;

    let candidates = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].job_id, "j-prod");
}

async fn insert_failed_check(db: &LocalDb, kind: &str, streak: i64) {
    db.execute_script(&format!(
        "INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','input','review',-1,0,'failed',0,1,'j-prod','{kind}',{streak});"
    ))
    .await
    .unwrap();
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

/// Producing builder node `j-prod` (issue `i-rev` / `cairn://p/PRJ/7`, exec
/// seq 1) whose just-ended turn carries `start_reason`, a watcher job
/// `j-watch`, and an active issue subscription for BOTH so the producing
/// node's self-exclusion is exercised.
async fn seed(db: &LocalDb, start_reason: &str) {
    db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p-rev','w','Project','PRJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e-rev','r','i-rev','p-rev','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, created_at, updated_at)
              VALUES('j-prod','e-rev','p-rev','i-rev','complete','builder','builder','b',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
              VALUES('j-watch','p-rev','i-rev','running','watcher',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('r-prod','p-rev','j-prod','i-rev',1,1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
              VALUES('t-prod','s-prod','j-prod',1,'complete','{start_reason}',1,1);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-watch','j-watch','issue','{ISSUE_URI}','active','agent',1,1,0);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-prod','j-prod','issue','{ISSUE_URI}','active','agent',1,1,0);
            "
        ))
        .await
        .unwrap();
}

enum NodeRole {
    Builder,
    Planner,
    Review,
}

/// Give execution `e-rev` the bundled PlanBuild snapshot and bind `job_id` to
/// one of its nodes. Only the turn-end-cadence tests need it: a live DAG makes
/// the issue non-quiescent (its other nodes have no jobs in this fixture), which
/// is the correct answer for a real execution but not what the review-push
/// tests around it are modelling.
async fn attach_planbuild_topology(db: &LocalDb, job_id: &str, role: NodeRole) {
    let recipe = crate::models::RecipeFile::from_yaml(PLANBUILD_YAML)
        .expect("bundled planbuild recipe parses")
        .into_recipe(Some("default".to_string()), None);
    // `into_recipe` reassigns node ids, so nodes are keyed by agent config.
    let node_id = |agent: &str| {
        recipe
            .nodes
            .iter()
            .find(|node| {
                node.agent_config
                    .as_ref()
                    .and_then(|c| c.agent_config_id.as_deref())
                    == Some(agent)
            })
            .unwrap_or_else(|| panic!("planbuild has an agent node for '{agent}'"))
            .id
            .clone()
    };
    let node_id = match role {
        NodeRole::Builder => node_id("build"),
        NodeRole::Planner => node_id("planner"),
        NodeRole::Review => node_id("pr-review"),
    };
    let snapshot = crate::models::ExecutionSnapshot::new(
        crate::models::RecipeSnapshot {
            id: recipe.id.clone(),
            name: recipe.name.clone(),
            description: recipe.description.clone(),
            trigger: recipe.trigger.clone(),
            nodes: recipe.nodes.clone(),
            edges: recipe.edges.clone(),
        },
        std::collections::HashMap::new(),
        std::collections::HashMap::new(),
        crate::models::TriggerContext {
            issue_id: Some("i-rev".to_string()),
            project_id: "p-rev".to_string(),
            trigger_type: crate::models::TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    );
    let snapshot_json = snapshot.to_json().expect("snapshot serializes");
    let job_id = job_id.to_string();
    db.write(move |conn| {
        let snapshot_json = snapshot_json.clone();
        let node_id = node_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = 'e-rev'",
                (snapshot_json.as_str(),),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET recipe_node_id = ?1 WHERE id = ?2",
                (node_id.as_str(), job_id.as_str()),
            )
            .await?;
            Ok::<_, crate::storage::DbError>(())
        })
    })
    .await
    .unwrap();
}

async fn insert_open_pr(db: &LocalDb) {
    db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-rev','j-prod','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
}

async fn insert_artifact(db: &LocalDb, artifact_type: &str, confirmed: i64) {
    db.execute_script(&format!(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-rev','j-prod','{artifact_type}',1,'{{}}',1,'{artifact_type}',{confirmed},1,1);"
        ))
        .await
        .unwrap();
}

async fn pending(orch: &Orchestrator, recipient: &str) -> Vec<Push> {
    list_pending(&orch.db.local, recipient).await.unwrap()
}

async fn run_review_push(orch: &Orchestrator) {
    // Both trigger edges now run through the single readiness evaluator; this
    // helper models any semantic or recovery re-evaluation edge.
    evaluate_review_readiness(orch, "i-rev").await;
}

async fn run_pr_open(orch: &Orchestrator) {
    // The PR-open edge now defers to the same readiness evaluator; the source
    // branch is retained only for the caller's signature/logging.
    create_review_push_for_pr_open(orch, "i-rev", "b").await;
}

#[tokio::test]
async fn work_idle_with_unconfirmed_create_pr_artifact_pushes_review() {
    // The create-pr idle: the PR is not open yet, but the unconfirmed
    // artifact is observable -> the second predicate arm fires.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 0).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    // The producing node is never a recipient of its own review.
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_with_open_pr_pushes_review() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/PRJ/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn settled_after_memory_review_completes_fires_review() {
    // The common build path: the builder's WORK turn produced the PR, then a
    // trailing memory-review turn ran and completed. Once that reflection turn
    // terminalizes the issue is settled, so the review fires normally — there is
    // deliberately no separate memory-review gate that would block it forever
    // (a builder's latest turn is permanently its memory-review turn; CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await; // t-prod: memory_review turn, state complete
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn work_idle_with_confirmed_create_pr_artifact_pushes_review() {
    // CAIRN-1999 shape: the create-pr artifact was already confirmed by the
    // artifact lifecycle, but the parent still needs a review push for the
    // child output even if no PR-open edge creates one.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_without_reviewable_output_no_push() {
    // A work turn with neither an open PR nor a create-pr/unconfirmed-plan
    // artifact -> nothing reviewable.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "plan", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn successive_work_idles_collapse_to_one_undelivered() {
    // Two work-turn idles with the SAME reviewable state and no delivery in
    // between yield one undelivered review row: the first creates it, the
    // second is skipped by the change-trigger (CAIRN-1889) because the
    // undelivered push already carries the same fingerprint.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    run_review_push(&orch).await;

    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn unchanged_fingerprint_skips_review_even_after_delivery() {
    // One review fires (fp=A); after it is delivered, a second work-turn idle
    // with the SAME reviewable state must NOT re-create a review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);

    // Deliver the first push: it leaves the supersede partial index but stays
    // in the table for the fingerprint lookup.
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    // Same diffstat -> skipped, no re-wake.
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "an unchanged reviewable state must not re-create a review push"
    );
}

#[tokio::test]
async fn changed_diffstat_creates_new_review_after_delivery() {
    // New commits change the diffstat -> a fresh review push, even after the
    // first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=10, deletions=2 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn mergeability_only_change_does_not_refire_review() {
    // A mergeability settle touches non-diffstat columns only -> same
    // fingerprint -> no new review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

// --- CAIRN-1891: the PR-open edge of the review push ---------------------

#[tokio::test]
async fn pr_open_with_quiescent_producer_pushes_one_review() {
    // The producing builder's head turn is complete (quiescent) and the PR is
    // now open -> exactly one review to the watcher, never to the producing
    // node itself. This is the wake the create-pr idle edge cannot fire.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/PRJ/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn pr_open_with_running_producer_does_not_push() {
    // The quiescence gate: a producing node still mid-turn (a `synchronize`
    // landing during active work) does NOT fire a review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_self_suspended_producer_does_not_push() {
    // A producing node self-suspended on its own work (yielded waiting on a
    // dependency/sub-agent) is not quiescent either -> no review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script(
        "UPDATE turns SET state='yielded', yield_reason='dependency_wait' WHERE id='t-prod';",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_resolves_builder_by_branch_not_mr_job() {
    // The live CAIRN-1891 job-identity bug: the merge_request is owned by a
    // separate pr-action node (blocked while the PR is open -> a running turn,
    // never quiescent), while the builder that did the work is a DIFFERENT job
    // on the same branch. Gating on `mr.job_id` would always bail; the gate
    // must resolve and check the builder via `source_branch`.
    let db = test_db().await;
    seed(&db, "initial").await; // builder j-prod: branch 'b', turn complete (quiescent)
                                // The pr-action node owns the merge_request and — reproducing the live
                                // shape — has NO joinable execution (execution_id NULL), so an arm-1 query
                                // that joined through mr.job_id would drop the row and read the open PR as
                                // unreviewable. The builder (j-prod) is the joinable node.
    db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES('j-prnode','p-rev','i-rev','blocked','pr','pr',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-rev','j-prnode','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "must resolve the builder by source_branch and fire, not gate on the blocked pr-node"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_changed_head_sha_refires_even_with_same_diffstat() {
    // Head SHA is the precise change key: two different commits can share a
    // diffstat, so a real new commit must re-review even when +/- is unchanged.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE merge_requests SET head_sha='sha-aaa' WHERE id='mr-rev';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    // New commit, SAME diffstat, different head SHA -> must re-fire.
    orch.db
        .local
        .execute_script("UPDATE merge_requests SET head_sha='sha-bbb' WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(
        second.len(),
        1,
        "a changed head SHA must re-create the review even with an unchanged diffstat"
    );
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn running_memory_review_turn_defers_then_fires_on_settle() {
    // A running memory-review turn keeps the issue unsettled (issue_settled's
    // liveness check includes memory-review turns), so no review fires
    // mid-reflection. Once that reflection turn completes the issue settles and
    // the review fires — exactly once (CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a running memory-review turn must defer the review"
    );

    orch.db
        .local
        .execute_script("UPDATE turns SET state='complete' WHERE id='t-prod';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "the review fires once the reflection turn settles"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_same_diffstat_is_deduped() {
    // A mergeability-only settle re-delivers the open PR with an unchanged
    // diffstat -> the fingerprint matches the delivered push, so no re-wake.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

#[tokio::test]
async fn pr_open_changed_diffstat_creates_new_review() {
    // New commits change the diffstat between webhook deliveries -> a fresh
    // review push, even after the first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=20, deletions=4 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn pr_open_and_node_idle_share_one_creator() {
    // Both edges run the same row creator: the PR-open edge creates the
    // review, and a subsequent node-idle edge against the unchanged diffstat
    // is deduped by the same fingerprint logic to the one undelivered row.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let after_pr_open = pending(&orch, "j-watch").await;
    assert_eq!(after_pr_open.len(), 1);
    let row_id = after_pr_open[0].id.clone();

    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    assert_eq!(
        after_idle[0].id, row_id,
        "both edges share one push row keyed review:{{issue}}"
    );
}

#[tokio::test]
async fn pr_open_after_idle_artifact_push_supersedes_to_pr_fingerprint() {
    // The CAIRN-2410 incident, reconstructed. A builder's `create-pr` artifact
    // auto-confirms on write (CAIRN-1219); the coordinator's node-idle edge
    // pushes a review with an `artifact:` fingerprint while no merge_requests
    // row exists yet. The PR opens ~42ms later. Before the fix the PR-open edge
    // never ran on the first-class PR-node path, so this second edge never
    // re-fired and the wake was lost. Now the idle-edge artifact push, then the
    // PR opening, then the PR-open edge supersedes the SAME review:{issue} row to
    // a `pr:` fingerprint on a rousing push that wakes the idle watcher.
    let db = test_db().await;
    seed(&db, "initial").await;
    // The reviewable artifact is a CONFIRMED create-pr (the auto-confirm the
    // incident hinges on), and there is no merge_requests row yet.
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    // Idle edge fires first: one review push fingerprinted on the artifact.
    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    let idle_row = after_idle[0].id.clone();
    let idle_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        idle_fp.starts_with("artifact:"),
        "idle edge fingerprints on the artifact, got {idle_fp}"
    );

    // The PR opens: the merge_requests row lands (the 42ms-late seed).
    insert_open_pr(&orch.db.local).await;

    // The PR-open edge re-evaluates the same review key. The reviewable ref is
    // now the open PR, so the fingerprint changes to `pr:` and the row is
    // superseded in place — still exactly one undelivered review.
    run_pr_open(&orch).await;
    let after_open = pending(&orch, "j-watch").await;
    assert_eq!(
        after_open.len(),
        1,
        "supersede-by-key collapses the idle and PR-open pushes to one undelivered row"
    );
    assert_eq!(
        after_open[0].id, idle_row,
        "the PR-open push supersedes the idle push in place (same review:{{issue}} key)"
    );
    assert_eq!(
        after_open[0].wake,
        Wake::Wake,
        "the superseding review is rousing, so an idle watcher is woken"
    );
    let open_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        open_fp.starts_with("pr:"),
        "the PR-open edge fingerprints on the open PR, got {open_fp}"
    );
}

// --- CAIRN-2483: the issue-quiescence gate -------------------------------

#[tokio::test]
async fn review_check_outcome_does_not_gate_the_review() {
    // Check outcomes are child feedback. A settled child with reviewable output
    // wakes its parent without a second, check-specific readiness state.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    evaluate_review_readiness(&orch, "i-rev").await;
    assert_eq!(
        pending(&orch, "j-watch").await.len(),
        1,
        "review readiness depends on semantic child state, not check outcome"
    );
}

#[tokio::test]
async fn in_flight_turn_end_checks_do_not_defer_the_review() {
    // Detached advisory checks may outlive the child work. They must not suppress
    // the coordinator's only durable wake once the issue itself is quiescent.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
    run_review_push(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "an in-flight turn-end suite must not hold the review"
    );
    assert_eq!(watcher[0].wake, Wake::Wake);
    assert!(pending(&orch, "j-prod").await.is_empty());

    orch.end_turn_end_checks("j-prod");
    run_review_push(&orch).await;
    assert_eq!(
        pending(&orch, "j-watch").await.len(),
        1,
        "check completion re-evaluates idempotently without duplicating the wake"
    );
}

#[tokio::test]
async fn transient_action_run_defers_then_blocked_fires() {
    // The pr action opening the PR (a pending/running action_run) keeps the issue
    // unsettled until it terminalizes or blocks (the open-PR human gate).
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    orch.db
        .local
        .execute_script(
            "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at)
             VALUES('ar-pr','e-rev','pr','builtin:pr','i-rev','p-rev','running',1);",
        )
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a transient pr action_run must hold the review"
    );

    // The action blocks (PR open, human gate) -> settled -> the review fires.
    orch.db
        .local
        .execute_script("UPDATE action_runs SET status='blocked' WHERE id='ar-pr';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn render_push_resolved_inlines_referent_content() {
    // CAIRN-1891 Deliverable 2: a drained push renders its referent content
    // inline, not just the URI. The header carries the wake level + the
    // content_ref URI; a resolved body is appended beneath it.
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let push = Push {
        id: "p-render".into(),
        recipient: "j-watch".into(),
        content_ref: ISSUE_URI.into(),
        wake: Wake::Wake,
        boundary: Boundary::Event,
        key: REVIEW_KEY.into(),
        created_at: 1,
        delivered_event_id: None,
    };
    let rendered =
        crate::orchestrator::attention_delivery::render_push_resolved(&orch, &push).await;

    let header = format!("Attention update (wake): {ISSUE_URI}");
    assert!(
        rendered.starts_with(&header),
        "header must carry the wake level + content_ref URI: {rendered}"
    );
    assert!(
        rendered.len() > header.len(),
        "expected resolved referent content inlined beneath the URI header: {rendered}"
    );
}

#[tokio::test]
async fn turn_end_cancel_resolves_immediately_when_already_cancelled() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    assert!(!cancel.is_cancelled());
    cancel.cancel();
    assert!(cancel.is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("an already-cancelled token resolves without blocking");
}

#[tokio::test]
async fn turn_end_cancel_wakes_a_parked_waiter() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    let waiter = {
        let cancel = cancel.clone();
        tokio::spawn(async move { cancel.cancelled().await })
    };
    // Let the waiter park on `notified()` before signalling.
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("cancel wakes the parked waiter")
        .expect("the waiter task joins cleanly");
}

#[tokio::test]
async fn cancel_turn_end_checks_signals_the_in_flight_token() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("first claim wins the single-flight slot");
    assert!(
        orch.try_begin_turn_end_checks("j-prod").is_none(),
        "a second claim while one is in flight is refused"
    );
    assert!(!cancel.is_cancelled());

    orch.cancel_turn_end_checks("j-prod");
    assert!(
        cancel.is_cancelled(),
        "the cancel lever signals the in-flight suite's token"
    );

    orch.end_turn_end_checks("j-prod");
    // After release the slot is free again and a stale cancel is a no-op.
    orch.cancel_turn_end_checks("j-prod");
    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
}

#[tokio::test]
async fn branch_advance_cancels_the_in_flight_review_suite() {
    // A commit sealing mid-turn advances the branch; the branch-advance hook
    // cancels the job's in-flight when:review suite so its heavy compiles stop
    // starving the builder's own when:write checks. Idempotent afterward.
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("claim the job's single-flight slot");
    assert!(!cancel.is_cancelled());

    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod").await;
    assert!(
        cancel.is_cancelled(),
        "a sealed commit cancels the in-flight review suite for the job"
    );

    orch.end_turn_end_checks("j-prod");
    // No suite in flight ⇒ the branch-advance cancel is a harmless no-op, and the
    // single-flight slot remains claimable.
    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod").await;
    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
}

/// Two jobs on one branch and one job on another, so a branch-advance can be
/// asked to distinguish them — plus a second project carrying a job on a branch
/// of the SAME name, which only project scoping can tell apart.
async fn seed_branch_sharers(db: &LocalDb) {
    db.execute_script(
        "
        INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p-rev','w','Project','PRJ','/tmp/repo',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-node','p-rev','i-rev','running','builder','agent/shared',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-task','p-rev','i-rev','running','task','agent/shared',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-other','p-rev','i-rev','running','sibling','agent/other',1,1);

        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p-far','w','Far','FAR','/tmp/far',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('i-far','p-far',7,'Far','active','active','none',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-far','p-far','i-far','running','builder','agent/shared',1,1);
        ",
    )
    .await
    .unwrap();
}

/// A task commits into the branch it inherited from the node that owns it. The
/// node's in-flight review wave is now keyed to a tree that no longer exists, so
/// it must be cancelled even though the commit arrived under a different job id
/// — the gap that let a full exclusive lane keep burning against a superseded
/// tree and then throw its verdicts away.
#[tokio::test]
async fn a_commit_supersedes_every_review_suite_on_the_same_branch() {
    let db = test_db().await;
    seed_branch_sharers(&db).await;
    let orch = test_orchestrator(db);

    let node = orch.try_begin_turn_end_checks("j-node").unwrap();
    let other = orch.try_begin_turn_end_checks("j-other").unwrap();

    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-task").await;

    assert!(
        node.is_cancelled(),
        "a task's commit supersedes the wave of the node whose branch it shares"
    );
    assert!(
        !other.is_cancelled(),
        "a job on a different branch keeps its wave: its inputs did not change"
    );
}

/// A branch name identifies a tree only inside one repository. Names like `main`
/// recur across projects and generated names are unique only per project, so
/// matching a branch by name alone would let a commit in one project destroy
/// live review waves in an unrelated one whose tree it never touched.
#[tokio::test]
async fn a_commit_spares_a_same_named_branch_in_another_project() {
    let db = test_db().await;
    seed_branch_sharers(&db).await;
    let orch = test_orchestrator(db);

    let near = orch.try_begin_turn_end_checks("j-node").unwrap();
    let far = orch.try_begin_turn_end_checks("j-far").unwrap();

    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-task").await;

    assert!(
        near.is_cancelled(),
        "the branch's own project still supersedes as before"
    );
    assert!(
        !far.is_cancelled(),
        "another project's identically named branch is a different tree"
    );
}

/// The committing job's own suite is cancelled even when its branch is unknown,
/// so an unreadable or branchless job row degrades to the previous behavior
/// rather than to no cancellation at all.
#[tokio::test]
async fn a_branchless_job_still_cancels_its_own_suite() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let own = orch.try_begin_turn_end_checks("j-unknown").unwrap();
    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-unknown").await;
    assert!(own.is_cancelled());
}

// --- CAIRN-3154: only a turn that really ended earns a review wave -------

/// Replace `j-prod`'s single turn with one in `state`, yielded for `reason`
/// when given, so the turn-end hook reads one deterministic head turn.
async fn set_prod_head_turn(db: &LocalDb, state: &str, reason: Option<&str>) {
    let reason = reason
        .map(|r| format!("'{r}'"))
        .unwrap_or_else(|| "NULL".to_string());
    db.execute_script(&format!(
        "DELETE FROM turns WHERE job_id='j-prod';
         INSERT INTO turns(id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
           VALUES('t-prod','s-prod','j-prod',1,'{state}',{reason},'initial',1,1);"
    ))
    .await
    .unwrap();
}

/// Did the turn-end hook schedule a review wave for `job_id`? The hook claims
/// the job's single-flight slot before detaching the suite, so the slot is the
/// observable. The probe restores the slot either way, leaving each subsequent
/// turn-end in a test independently observable.
///
/// Deterministic: under a test runtime `detach_onto_runtime` reaches
/// `tokio::spawn`, so the detached suite — the only other party that releases
/// the slot — cannot poll until the test next awaits.
fn wave_scheduled(orch: &Orchestrator, job_id: &str) -> bool {
    let claimed_by_probe = orch.try_begin_turn_end_checks(job_id).is_some();
    orch.end_turn_end_checks(job_id);
    !claimed_by_probe
}

/// The reported defect: the heavy `when:review` suites fired at the turn end of
/// EVERY job, so a builder that delegated work multiplied minutes of exclusive
/// machine time by its sub-agent turn count, all against intermediate trees
/// nobody reviews. A delegated task is materialized into the DAG with no node
/// of its own to ship from, so its turn end now schedules nothing (CAIRN-3334).
#[tokio::test]
async fn a_delegated_sub_agents_turn_end_schedules_no_review_wave() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    db.execute_script(
        "INSERT INTO jobs(id, execution_id, parent_job_id, project_id, issue_id, status,
                          uri_segment, node_name, branch, task_index, created_at, updated_at)
           VALUES('j-task','e-rev','j-prod','p-rev','i-rev','complete','explore','Explore','b',0,1,1);
         INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
           VALUES('t-task','s-task','j-task',1,'complete','initial',1,1);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-task");
    assert!(
        !wave_scheduled(&orch, "j-task"),
        "a sub-agent task ships no PR, so its turn end runs no review cadence"
    );

    // The builder it belongs to is unaffected: its branch is the one that ships.
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the builder whose branch ships the PR still runs its review cadence"
    );
}

/// Scoping is by recipe topology, not by agent name: within the SAME recipe the
/// planner (whose output terminates in a plan artifact) and the review node
/// (which has no context-out at all) run nothing, while the builder does.
#[tokio::test]
async fn only_the_node_whose_branch_ships_runs_the_review_cadence() {
    for (role, expected) in [
        (NodeRole::Builder, true),
        (NodeRole::Planner, false),
        (NodeRole::Review, false),
    ] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", role).await;
        let orch = test_orchestrator(db);

        spawn_turn_end_checks(&orch, "j-prod");
        assert_eq!(
            wave_scheduled(&orch, "j-prod"),
            expected,
            "a node's review cadence follows its recipe topology"
        );
    }
}

#[tokio::test]
async fn a_turn_ended_by_self_suspension_schedules_no_review_wave() {
    // The agent kicked off its own tests and self-suspended waiting on them.
    // That yield reaches the turn-end hook looking like an idle, but the job is
    // mid-work: launching the heavy review lanes here would put them in
    // contention with the very tests being waited on.
    let db = test_db().await;
    seed(&db, "initial").await;
    // A builder that would otherwise qualify, so the mid-work guard is what is
    // under test here rather than the topology gate ahead of it.
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    for reason in ["wait", "dependency_wait", "user_input", "permission"] {
        set_prod_head_turn(&orch.db.local, "yielded", Some(reason)).await;

        spawn_turn_end_checks(&orch, "j-prod");

        assert!(
            !wave_scheduled(&orch, "j-prod"),
            "a turn yielded on the agent's own {reason} is not a turn end"
        );
    }
}

#[tokio::test]
async fn the_review_wave_lands_once_after_the_resume() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    // Two suspended turn-ends in a row schedule nothing, and leave nothing owed:
    // there is no make-up wave per skipped turn.
    for _ in 0..2 {
        set_prod_head_turn(&orch.db.local, "yielded", Some("wait")).await;
        spawn_turn_end_checks(&orch, "j-prod");
        assert!(!wave_scheduled(&orch, "j-prod"));
    }

    // The wait resolves and the synthetic continuation turn starts. The
    // suspension's late interrupt ack lands on this hook here; it must not
    // schedule a wave against the pre-resume tree, nor take the single-flight
    // slot away from the continuation's own turn-end.
    set_prod_head_turn(&orch.db.local, "running", None).await;
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "the resume boundary must not double-fire ahead of the real turn-end"
    );

    // The continuation reaches a real turn end: one wave, against the tree that
    // exists now.
    set_prod_head_turn(&orch.db.local, "complete", None).await;
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the first real turn-end after the resume runs the suite"
    );
}

#[tokio::test]
async fn a_plain_turn_end_still_schedules_its_review_wave() {
    // The unchanged path: `seed` leaves `t-prod` complete, which is what an
    // ordinary turn ending through the warm transition looks like.
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-prod");

    assert!(
        wave_scheduled(&orch, "j-prod"),
        "a completed head turn is a real turn end and still fires the cadence"
    );
}

/// Startup must re-derive the review wake for a job the check cadence skips
/// (CAIRN-3347). A planner parked at its plan gate is settled, owns reviewable
/// output, and ships no PR — so a re-arm that only re-spawns the cadence restores
/// nothing for it, and its coordinator stays asleep across the restart.
#[tokio::test]
async fn startup_rearm_recovers_the_wake_for_a_gated_job_that_ships_no_pr() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
    db.execute_script(
        "UPDATE jobs SET status='blocked', branch=NULL WHERE id='j-prod';
         INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                               output_name, created_at, updated_at)
           VALUES('a-plan','j-prod','plan',0,'{}',1,'plan',1,1);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    super::rearm_review_checks_on_startup(&orch).await;

    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "a planner's branch ships no PR, so the re-arm schedules no check wave for it"
    );
    let pending = list_pending(&orch.db.local, "j-watch").await.unwrap();
    let review: Vec<&Push> = pending.iter().filter(|p| p.key == REVIEW_KEY).collect();
    assert_eq!(
        review.len(),
        1,
        "the watcher's wake is re-derived directly, not through the cadence"
    );
    assert_eq!(review[0].wake, Wake::Wake);
    assert!(
        review[0].content_ref.ends_with("/plan"),
        "the watcher is pointed at the plan awaiting confirmation, got {}",
        review[0].content_ref
    );
}

/// Every settled stored status is a re-arm candidate, including `cancelled`
/// (CAIRN-3347). The recompute hook filters derived *transitions* and so can
/// never see an archived job — cancellation is an explicit sticky override the
/// sweep skips — which makes this query the only place archived-but-reviewable
/// work can reach its watcher.
#[tokio::test]
async fn every_settled_status_is_a_rearm_candidate() {
    for status in ["idle", "complete", "failed", "blocked", "cancelled"] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
        db.execute_script(&format!(
            "UPDATE jobs SET status='{status}', branch=NULL WHERE id='j-prod';
             INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                                   output_name, created_at, updated_at)
               VALUES('a-plan','j-prod','plan',0,'{{}}',1,'plan',1,1);"
        ))
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        super::rearm_review_checks_on_startup(&orch).await;

        let pending = list_pending(&orch.db.local, "j-watch").await.unwrap();
        assert_eq!(
            pending.iter().filter(|p| p.key == REVIEW_KEY).count(),
            1,
            "a settled '{status}' job owning reviewable output must reach its watcher"
        );
    }
}

/// The re-arm now runs as a detached task after the transport is already serving
/// (CAIRN-3382), which is safe for exactly one reason: it is idempotent across
/// boots. This pins both halves of that claim — a task-spawned re-arm reaches the
/// same outcome the blocking one did, and running it a second time (the next boot
/// after one that was interrupted or never finished) neither duplicates the
/// watcher's wake nor changes what it points at.
#[tokio::test]
async fn a_detached_rearm_converges_and_stays_convergent_across_boots() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
    db.execute_script(
        "UPDATE jobs SET status='blocked', branch=NULL WHERE id='j-prod';
         INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                               output_name, created_at, updated_at)
           VALUES('a-plan','j-prod','plan',0,'{}',1,'plan',1,1);",
    )
    .await
    .unwrap();
    let orch = std::sync::Arc::new(test_orchestrator(db));

    async fn review_wakes(orch: &Orchestrator) -> Vec<String> {
        list_pending(&orch.db.local, "j-watch")
            .await
            .unwrap()
            .into_iter()
            .filter(|push| push.key == REVIEW_KEY)
            .map(|push| push.content_ref)
            .collect()
    }

    // Boot one, spawned rather than awaited inline — what the runtime now does.
    let first_waves = {
        let orch = orch.clone();
        tokio::spawn(async move { super::rearm_review_checks_on_startup(&orch).await })
    }
    .await
    .expect("the background re-arm task runs to completion");
    assert!(
        first_waves >= 1,
        "the settled candidate must be counted for the boot log"
    );
    let after_first = review_wakes(&orch).await;
    assert_eq!(
        after_first.len(),
        1,
        "a detached re-arm must reach the watcher exactly as a blocking one did"
    );

    // Boot two, over state boot one already re-armed.
    let second_waves = super::rearm_review_checks_on_startup(&orch).await;
    assert_eq!(
        second_waves, first_waves,
        "the candidate set is a function of stored state, not of how many boots ran"
    );
    assert_eq!(
        review_wakes(&orch).await,
        after_first,
        "re-running the re-arm must not duplicate or move the watcher's wake"
    );
}

/// The launchability guard, at the moment it is cheapest to honour: a wave for
/// a resolved issue or a cancelled job is never armed at all, so it claims no
/// single-flight slot and spends no minutes planning against a tree nobody will
/// review (CAIRN-3345).
#[tokio::test]
async fn a_wave_is_never_armed_for_a_resolved_issue_or_a_cancelled_job() {
    for (script, why) in [
        (
            "UPDATE issues SET status='merged' WHERE id='i-rev';",
            "a merged issue has no tree left to review",
        ),
        (
            "UPDATE issues SET status='closed' WHERE id='i-rev';",
            "a closed issue has no tree left to review",
        ),
        (
            "UPDATE jobs SET status='cancelled' WHERE id='j-prod';",
            "a cancelled job's work was withdrawn",
        ),
    ] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
        db.execute_script(script).await.unwrap();
        let orch = test_orchestrator(db);

        spawn_turn_end_checks(&orch, "j-prod");
        assert!(!wave_scheduled(&orch, "j-prod"), "{why}");
    }
}

/// The negative half, so the guard cannot pass by refusing everything: an active
/// issue with a live job still arms its wave.
#[tokio::test]
async fn a_live_issue_still_arms_its_wave() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-prod");
    assert!(wave_scheduled(&orch, "j-prod"));
}

#[tokio::test]
async fn resolving_an_issue_cancels_its_jobs_turn_end_checks() {
    // The issue-scoped lever the merge/close path pulls: every job of the issue
    // with an in-flight suite is signalled to quit (CAIRN-2648).
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("claim the builder job's slot");
    assert!(!cancel.is_cancelled());

    crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(
        &orch,
        &orch.db.local,
        "i-rev",
    )
    .await;

    assert!(
        cancel.is_cancelled(),
        "resolving issue i-rev quits its builder job's in-flight suite"
    );
    orch.end_turn_end_checks("j-prod");
}

#[tokio::test]
async fn repeated_turn_ends_preserve_attempt_history_and_current_ordering() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let owner = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist owner attempt");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch, "j-prod", &owner, "claimed", None,
    )
    .expect("claim owner");
    let successor = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist successor attempt");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch,
        "j-prod",
        &successor,
        "superseded",
        Some("owner remains active"),
    )
    .expect("terminalize successor");

    let rows = orch.db.local.read(|conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT id,state FROM turn_end_check_attempts WHERE job_id='j-prod' ORDER BY created_at,id",
            (),
        ).await?;
        let mut found=Vec::new();
        while let Some(row)=rows.next().await? { found.push((row.text(0)?,row.text(1)?)); }
        Ok::<_,crate::storage::DbError>(found)
    })).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(owner.clone(), "claimed".to_string())));
    assert!(rows.contains(&(successor.clone(), "superseded".to_string())));
    let current =
        crate::execution::checks_turn_end::current_turn_end_attempt(&orch.db.local, "j-prod")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(current.id, owner);
    assert_eq!(current.state, "claimed");
}

#[tokio::test]
async fn startup_reconciliation_terminalizes_every_moving_attempt_with_named_reason() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    for state in ["requested", "claimed", "runtime_started", "submitted"] {
        let id =
            crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod").unwrap();
        crate::execution::checks_turn_end::transition_turn_end_attempt(
            &orch, "j-prod", &id, state, None,
        )
        .unwrap();
    }

    crate::execution::checks_turn_end::reconcile_turn_end_attempts_on_startup(&orch).await;

    let rows = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT state,reason FROM turn_end_check_attempts WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push((row.text(0)?, row.text(1)?));
                }
                Ok::<_, crate::storage::DbError>(found)
            })
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows
        .iter()
        .all(|(state, reason)| state == "failed" && reason.contains("host restarted")));
}
