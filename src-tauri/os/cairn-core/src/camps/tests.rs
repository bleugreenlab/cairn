use super::*;
use crate::models::{CampRole, CreateProject};
use crate::services::Clock;
use crate::storage::{migrated_test_db, RowExt};

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> i64 {
        1234
    }
    fn now_u64(&self) -> u64 {
        1234
    }
}

async fn project(db: &LocalDb, id: &str, key: &str) {
    crate::projects::crud::create_db(
        db,
        &FixedClock,
        &CreateProject {
            id: Some(id.to_string()),
            name: key.to_string(),
            key: key.to_string(),
            repo_path: String::new(),
            team_id: None,
        },
    )
    .await
    .unwrap();
}

async fn thread(db: &LocalDb, project_id: &str, name: &str) -> String {
    crate::threads::crud::create(
        db,
        CreateThread {
            project_id: project_id.to_string(),
            name: Some(name.to_string()),
            jurisdiction: None,
            definition: None,
            migrated_from_number: None,
            model: None,
        },
    )
    .await
    .unwrap()
    .id
}

#[tokio::test]
async fn a_project_without_threads_is_given_a_general_thread_that_leads_base_camp() {
    let db = migrated_test_db("camps-birth-empty").await;
    project(&db, "p-empty", "empty").await;

    let camp = ensure_project_base_camp(&db, "p-empty").await.unwrap();
    let general = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = 'p-empty' AND name = 'general'",
            (),
        )
        .await
        .unwrap()
        .expect("birth created the general thread");
    assert_eq!(camp.lead_thread_id, general);
    let participants = db.camp_participants(&camp).await.unwrap();
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0].role, CampRole::Lead);

    let again = ensure_project_base_camp(&db, "p-empty").await.unwrap();
    assert_eq!(again.id, camp.id, "birth is idempotent");
    let threads = db
        .query_all(
            "SELECT id FROM threads WHERE project_id = 'p-empty'",
            (),
            |row| row.get::<String>(0).map_err(Into::into),
        )
        .await
        .unwrap();
    assert_eq!(threads.len(), 1, "a retry creates no second general thread");
}

#[tokio::test]
async fn existing_threads_are_seated_and_later_threads_join_at_birth() {
    let db = migrated_test_db("camps-birth-existing").await;
    project(&db, "p-full", "full").await;
    let general = thread(&db, "p-full", "general").await;
    let devops = thread(&db, "p-full", "devops").await;

    let camp = ensure_project_base_camp(&db, "p-full").await.unwrap();
    assert_eq!(
        camp.lead_thread_id, general,
        "an existing general thread is reused"
    );

    let later = thread(&db, "p-full", "later").await;
    let participants = db.camp_participants(&camp).await.unwrap();
    // The lead first; the watchers seated in the same second tie on time.
    assert_eq!(participants[0].subject_key, general);
    let mut watchers: Vec<_> = participants[1..]
        .iter()
        .map(|p| p.subject_key.clone())
        .collect();
    watchers.sort();
    let mut expected = vec![devops.clone(), later.clone()];
    expected.sort();
    assert_eq!(watchers, expected);
    assert!(participants[1..].iter().all(|p| p.role == CampRole::Watch));
    assert_eq!(
        db.camps_for_thread(&later).await.unwrap()[0].1,
        CampRole::Watch,
        "a thread born after base camp joins it inside its own creation"
    );
}

/// One base camp notice is one fact row, and it reaches each participant at
/// its own role: the lead and a `wake` participant are woken with the camp's
/// framing, a `watch` participant is told passively.
#[tokio::test]
async fn a_camp_notice_is_one_fact_delivered_to_each_participant_by_role() {
    use crate::models::CampLevel;
    use cairn_db::storage::camps::{set_camp_level_conn, CampAssignedBy};

    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
    // `t`/`watcher` is the fixture's thread session; add a lead and a quiet one.
    db.execute_batch(
        "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('lead-job','tl','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('lead-run','lead-job','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq','p','quiet','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('quiet-job','tq','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('quiet-run','quiet-job','running',1,1);",
    )
    .await
    .unwrap();
    let (base, _) = db.ensure_base_camp("p", "tl", 1).await.unwrap();
    let camp = base.clone();
    db.write(move |conn| {
        let camp = camp.clone();
        Box::pin(async move {
            set_camp_level_conn(
                conn,
                &camp,
                &crate::models::CampSubject::Thread {
                    thread_id: "t".into(),
                },
                CampLevel::Wake,
                "rebuilds change what I route",
                CampAssignedBy::Agent,
                None,
                2,
            )
            .await
        })
    })
    .await
    .unwrap();

    let id = notify_camp(
        &orch,
        CampNotice {
            project_id: "p",
            project_key: "cairn",
            camp_slug: crate::models::BASE_CAMP_SLUG,
            kind: "rebuild",
            identity: "build-change:test".into(),
            summary: "Cairn was rebuilt.".into(),
            detail_uri: None,
        },
    )
    .await
    .unwrap()
    .expect("a new identity mints a row");
    assert!(
        notify_camp(
            &orch,
            CampNotice {
                project_id: "p",
                project_key: "cairn",
                camp_slug: crate::models::BASE_CAMP_SLUG,
                kind: "rebuild",
                identity: "build-change:test".into(),
                summary: "Cairn was rebuilt.".into(),
                detail_uri: None,
            },
        )
        .await
        .unwrap()
        .is_none(),
        "the same notice minted twice is one fact"
    );
    crate::routes::drain_fact_log(&orch).await;

    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM fact_log WHERE source = 'camp_notice'",
            ()
        )
        .await
        .unwrap(),
        Some(1),
        "one row however many participants"
    );
    let mut wakes = db
        .query_all(
            "SELECT job_id, clause_ref, delivered_action FROM fact_log_wakes
             WHERE first_row_id = ?1 ORDER BY job_id",
            (id,),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap();
    wakes.sort();
    assert_eq!(
        wakes,
        vec![
            (
                "lead-job".to_string(),
                "addressed:camp_notice.camp@lead".to_string(),
                Some("delivered".to_string())
            ),
            (
                "quiet-job".to_string(),
                "camp-watch:camp_notice.camp@watch".to_string(),
                Some("suppressed".to_string())
            ),
            (
                "watcher".to_string(),
                "addressed:camp_notice.camp@wake".to_string(),
                Some("delivered".to_string())
            ),
        ]
    );
    let told = db
        .query_opt_text(
            "SELECT content FROM messages WHERE recipient_run_id = 'watcher-run' ORDER BY created_at DESC LIMIT 1",
            (),
        )
        .await
        .unwrap()
        .expect("the wake participant was told");
    assert!(
        told.starts_with("From camp base (your role: wake): Cairn was rebuilt."),
        "{told}"
    );
}

/// A team project's base camp lives in its team replica, and so do its
/// participants' jobs. The notice reaches them: their jurisdiction is the
/// team project the camp named, not a private-database lookup that cannot
/// find them.
#[tokio::test]
async fn a_team_camp_notice_reaches_participants_that_exist_only_in_the_replica() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
    let team = std::sync::Arc::new(migrated_test_db("camps-team-replica").await);
    let project = "teamT~6f1c0a52-8f0e-4d55-9c1a-2b8d3a1e7f01";
    let thread = "teamT~6f1c0a52-8f0e-4d55-9c1a-2b8d3a1e7f02";
    let job = "teamT~6f1c0a52-8f0e-4d55-9c1a-2b8d3a1e7f03";
    team.execute_batch(&format!(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('{project}','w','Team','tp','',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('{thread}','{project}','general','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('{job}','{thread}','{project}','running','thread',1,1);"
    ))
    .await
    .unwrap();
    team.ensure_base_camp(project, thread, 1).await.unwrap();
    orch.db
        .register_team_db_for_test("teamT".to_string(), team.clone())
        .await;
    orch.db.set_route("tp", Some("teamT".to_string())).await;

    notify_camp(
        &orch,
        CampNotice {
            project_id: project,
            project_key: "tp",
            camp_slug: crate::models::BASE_CAMP_SLUG,
            kind: "rebuild",
            identity: "build-change:team".into(),
            summary: "Cairn was rebuilt.".into(),
            detail_uri: None,
        },
    )
    .await
    .unwrap();
    crate::routes::drain_fact_log(&orch).await;
    let reached = db
        .query_all(
            "SELECT clause_ref FROM fact_log_wakes WHERE job_id = ?1",
            (job.to_string(),),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(reached, ["addressed:camp_notice.camp@lead"]);
}
