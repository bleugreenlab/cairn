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

#[tokio::test]
async fn a_project_with_threads_but_no_general_is_given_one_to_lead_base_camp() {
    let db = migrated_test_db("camps-birth-no-general").await;
    // A project whose threads predate base camp, as at the first boot.
    bare_project(&db, "p-busy", "busy", &[]).await;
    let main = thread(&db, "p-busy", "main").await;
    let coaching = thread(&db, "p-busy", "coaching").await;

    let camp = ensure_project_base_camp(&db, "p-busy").await.unwrap();
    let general = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = 'p-busy' AND name = 'general'",
            (),
        )
        .await
        .unwrap()
        .expect("birth mints general beside the existing threads");
    assert_eq!(
        camp.lead_thread_id, general,
        "general leads, not the oldest thread"
    );
    let participants = db.camp_participants(&camp).await.unwrap();
    assert_eq!(participants[0].subject_key, general);
    let mut watchers: Vec<_> = participants[1..]
        .iter()
        .map(|p| (p.subject_key.clone(), p.role))
        .collect();
    watchers.sort();
    let mut expected = vec![(main, CampRole::Watch), (coaching, CampRole::Watch)];
    expected.sort();
    assert_eq!(watchers, expected);
}

async fn bare_project(db: &LocalDb, id: &str, key: &str, threads: &[(&str, &str)]) {
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, repository_id,
                               created_at, updated_at, is_workspace)
         VALUES (?1, 'default', ?2, ?2, '', ?1, 1, 1, 0)",
        (id.to_string(), key.to_string()),
    )
    .await
    .unwrap();
    for (index, (thread_id, name)) in threads.iter().enumerate() {
        db.execute(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'active', ?4, ?4)",
            (
                thread_id.to_string(),
                id.to_string(),
                name.to_string(),
                index as i64 + 1,
            ),
        )
        .await
        .unwrap();
    }
}

async fn general_of(db: &LocalDb, project_id: &str) -> Option<String> {
    db.query_opt_text(
        "SELECT id FROM threads WHERE project_id = ?1 AND name = 'general'",
        (project_id.to_string(),),
    )
    .await
    .unwrap()
}

/// Base camps an earlier birth led by the project's oldest thread are moved to
/// a general thread minted at boot; a lead that was handed over is not, and a
/// second boot writes nothing.
#[tokio::test]
async fn boot_repairs_fallback_led_base_camps_once_and_leaves_handed_leads() {
    use cairn_db::storage::camps::{accept_lead_conn, offer_lead_conn};
    let local = std::sync::Arc::new(migrated_test_db("camps-boot-repair").await);
    // As the oldest-thread fallback left them: `rs` led by `main`, and `crs`
    // led by `coaching` until it handed the lead to `hardware`.
    bare_project(&local, "p-rs", "rs", &[("rs-main", "main"), ("rs-x", "x")]).await;
    bare_project(
        &local,
        "p-crs",
        "crs",
        &[("crs-coaching", "coaching"), ("crs-hw", "hardware")],
    )
    .await;
    local.ensure_base_camp("p-rs", "rs-main", 10).await.unwrap();
    let (crs, _) = local
        .ensure_base_camp("p-crs", "crs-coaching", 10)
        .await
        .unwrap();
    let c = crs.clone();
    local
        .write(move |conn| {
            let c = c.clone();
            Box::pin(async move { offer_lead_conn(conn, &c, "crs-hw", None, 11).await })
        })
        .await
        .unwrap();
    let offered = local
        .camp_by_slug("p-crs", BASE_CAMP_SLUG)
        .await
        .unwrap()
        .unwrap();
    local
        .write(move |conn| {
            let o = offered.clone();
            Box::pin(async move { accept_lead_conn(conn, &o, "crs-hw", None, 12).await })
        })
        .await
        .unwrap();
    // A project that has no base camp yet is born with general as lead.
    bare_project(&local, "p-new", "fresh", &[("new-a", "a")]).await;

    let search = std::sync::Arc::new(
        crate::storage::SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap(),
    );
    let dbs = DbState::new(local.clone(), search);
    let report = bootstrap_base_camps(&dbs).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.repaired, ["rs"]);
    assert_eq!(report.established, ["fresh"]);

    let rs_general = general_of(&local, "p-rs")
        .await
        .expect("general minted for rs");
    let rs = local
        .camp_by_slug("p-rs", BASE_CAMP_SLUG)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rs.lead_thread_id, rs_general);
    let participants = local.camp_participants(&rs).await.unwrap();
    assert_eq!(participants.len(), 3);
    assert_eq!(participants[0].subject_key, rs_general);
    assert!(participants[1..]
        .iter()
        .all(|p| p.role == CampRole::Watch && !p.lead));
    let (kind, old, new) = local
        .query_opt(
            "SELECT kind, old_value, new_value FROM camp_events
             WHERE camp_id = ?1 ORDER BY id DESC LIMIT 1",
            (rs.id.clone(),),
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kind, "lead_changed");
    assert_eq!(
        old.as_deref(),
        Some("rs-main"),
        "the history names the old lead"
    );
    assert_eq!(new, Some(rs_general));

    let crs_after = local
        .camp_by_slug("p-crs", BASE_CAMP_SLUG)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(crs_after.lead_thread_id, "crs-hw", "a handed lead stays");
    assert_eq!(
        general_of(&local, "p-crs").await,
        None,
        "nothing minted there"
    );
    let fresh = local
        .camp_by_slug("p-new", BASE_CAMP_SLUG)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Some(fresh.lead_thread_id),
        general_of(&local, "p-new").await
    );

    let count = |sql: &'static str| {
        let local = local.clone();
        async move { local.query_opt_i64(sql, ()).await.unwrap().unwrap() }
    };
    let (events, threads, camps) = (
        count("SELECT COUNT(*) FROM camp_events").await,
        count("SELECT COUNT(*) FROM threads").await,
        count("SELECT MAX(updated_at) FROM camps").await,
    );
    let again = bootstrap_base_camps(&dbs).await;
    assert!(again.errors.is_empty() && again.repaired.is_empty() && again.established.is_empty());
    assert_eq!(count("SELECT COUNT(*) FROM camp_events").await, events);
    assert_eq!(count("SELECT COUNT(*) FROM threads").await, threads);
    assert_eq!(count("SELECT MAX(updated_at) FROM camps").await, camps);
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
    // The wake is owed to the thread; its session is resolved at delivery.
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
            "SELECT clause_ref FROM fact_log_wakes WHERE recipient_key = ?1",
            (format!("thread:{thread}"),),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(reached, ["addressed:camp_notice.camp@lead"]);
}

/// Specimen 4: one base camp notice is one fact row and one receipt per
/// participant, however many threads the project has, and neither a second
/// drain nor a restart's recovery adds a message or loses a receipt.
#[tokio::test]
async fn a_camp_notice_is_one_row_and_one_receipt_per_thread_at_any_size() {
    for threads in [1usize, 3, 25] {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let mut seed = String::new();
        for n in 0..threads {
            seed.push_str(&format!(
                "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('n{n}','p','n{n}','active',1,1);
                 INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('n{n}-job','n{n}','p','running','thread',1,1);
                 INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('n{n}-run','n{n}-job','running',1,1);"
            ));
        }
        db.execute_batch(&seed).await.unwrap();
        db.ensure_base_camp("p", "t", 1).await.unwrap();
        notify_camp(
            &orch,
            CampNotice {
                project_id: "p",
                project_key: "cairn",
                camp_slug: crate::models::BASE_CAMP_SLUG,
                kind: "rebuild",
                identity: format!("build-change:{threads}"),
                summary: "Cairn was rebuilt.".into(),
                detail_uri: None,
            },
        )
        .await
        .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        let count = |sql: &'static str| {
            let db = db.clone();
            async move { db.query_opt_i64(sql, ()).await.unwrap().unwrap() }
        };
        let participants = threads as i64 + 1;
        assert_eq!(
            count("SELECT COUNT(*) FROM fact_log WHERE source = 'camp_notice'").await,
            1
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM delivery_receipts WHERE state = 'delivered'").await,
            participants,
            "{threads} threads"
        );
        let told = "SELECT COUNT(*) FROM messages WHERE content LIKE 'From camp base%'";
        assert_eq!(count(told).await, participants);

        crate::storage::recover_fact_log(&db, i64::MAX / 2, 0)
            .await
            .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        assert_eq!(count(told).await, participants, "recovery adds nothing");
        assert_eq!(
            count("SELECT COUNT(*) FROM delivery_receipts").await,
            participants
        );
    }
}
