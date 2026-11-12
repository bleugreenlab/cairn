use super::*;
use crate::models::{CampSubject, CreateProject};
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

/// A project is born with its general thread and its base camp, whose one
/// member is that thread; establishing base camp again writes nothing.
#[tokio::test]
async fn a_project_is_born_with_a_general_thread_and_base_camp() {
    let db = migrated_test_db("camps-birth").await;
    crate::projects::crud::create(
        &db,
        &FixedClock,
        CreateProject {
            id: Some("p-new".to_string()),
            name: "new".to_string(),
            key: "new".to_string(),
            repo_path: String::new(),
            team_id: None,
        },
        None,
        None,
    )
    .await
    .unwrap();
    let general = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = 'p-new' AND name = 'general'",
            (),
        )
        .await
        .unwrap()
        .expect("birth created the general thread");
    let camp = ensure_project_base_camp(&db, "p-new").await.unwrap();
    assert!(camp.is_base());
    let members = db.camp_members(&camp).await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].subject_key, general);
    let again = ensure_project_base_camp(&db, "p-new").await.unwrap();
    assert_eq!(again.id, camp.id, "birth is idempotent");
}

/// The boot backfill gives a project with no base camp one and leaves every
/// other alone; a second boot writes nothing.
#[tokio::test]
async fn boot_establishes_missing_base_camps_once() {
    let local = std::sync::Arc::new(migrated_test_db("camps-boot").await);
    bare_project(&local, "p-old", "old", &[("old-a", "a")]).await;
    local.ensure_base_camp("p-old", 10).await.unwrap();
    bare_project(&local, "p-new", "fresh", &[("new-a", "a")]).await;

    let search = std::sync::Arc::new(
        crate::storage::SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap(),
    );
    let dbs = DbState::new(local.clone(), search);
    let report = bootstrap_base_camps(&dbs).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.established, ["fresh"]);
    let fresh = local
        .camp_by_slug("p-new", BASE_CAMP_SLUG)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(local.camp_members(&fresh).await.unwrap().len(), 1);

    let count = |sql: &'static str| {
        let local = local.clone();
        async move { local.query_opt_i64(sql, ()).await.unwrap().unwrap() }
    };
    let (threads, camps) = (
        count("SELECT COUNT(*) FROM threads").await,
        count("SELECT MAX(updated_at) FROM camps").await,
    );
    let again = bootstrap_base_camps(&dbs).await;
    assert!(again.errors.is_empty() && again.established.is_empty());
    assert_eq!(count("SELECT COUNT(*) FROM threads").await, threads);
    assert_eq!(count("SELECT MAX(updated_at) FROM camps").await, camps);
}

fn rebuild_notice<'a>(camp_slug: &'a str, identity: &str) -> CampNotice<'a> {
    CampNotice {
        project_id: "p",
        project_key: "cairn",
        camp_slug,
        kind: "rebuild",
        identity: identity.to_string(),
        summary: "Cairn was rebuilt.".into(),
        detail_uri: None,
        actor_job_id: None,
    }
}

/// The fixture's thread `t` (session `watcher`), plus `tl` and `tq` with
/// running sessions and a closed thread `tc`.
async fn notice_fixture() -> (
    tempfile::TempDir,
    crate::orchestrator::Orchestrator,
    std::sync::Arc<LocalDb>,
) {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
    db.execute_batch(
        "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('lead-job','tl','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('lead-run','lead-job','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq','p','quiet','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('quiet-job','tq','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('quiet-run','quiet-job','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tc','p','closed','closed',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('closed-job','tc','p','idle','thread',1,1);",
    )
    .await
    .unwrap();
    (temp, orch, db)
}

async fn wakes_of(db: &LocalDb, row: i64) -> Vec<(String, String, Option<String>)> {
    let mut wakes = db
        .query_all(
            "SELECT recipient_key, clause_ref, delivered_action FROM fact_log_wakes
             WHERE first_row_id = ?1",
            (row,),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap();
    wakes.sort();
    wakes
}

/// One base camp notice is one fact row, and it reaches every open thread
/// passively: it rides along at each one's next turn and wakes none. A closed
/// thread is not told.
#[tokio::test]
async fn a_base_camp_notice_is_one_fact_told_passively_to_every_open_thread() {
    let (_temp, orch, db) = notice_fixture().await;
    db.ensure_base_camp("p", 1).await.unwrap();

    let id = notify_camp(&orch, rebuild_notice(BASE_CAMP_SLUG, "build-change:test"))
        .await
        .unwrap()
        .expect("a new identity mints a row");
    assert!(
        notify_camp(&orch, rebuild_notice(BASE_CAMP_SLUG, "build-change:test"))
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
        "one row however many members"
    );
    let passive = |thread: &str| {
        (
            format!("thread:{thread}"),
            "camp-passive:camp_notice.camp".to_string(),
            Some("suppressed".to_string()),
        )
    };
    assert_eq!(
        wakes_of(&db, id).await,
        vec![passive("t"), passive("tl"), passive("tq")],
        "every open thread is told passively; the closed one is not told"
    );
    let told = db
        .query_opt_text(
            "SELECT content FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = 'watcher-run') ORDER BY created_at DESC LIMIT 1",
            (),
        )
        .await
        .unwrap()
        .expect("the thread was told");
    assert!(
        told.starts_with("From camp base: Cairn was rebuilt."),
        "{told}"
    );
}

/// A notice to a camp wakes each of its thread members once, named by the
/// camp, and reaches no other thread.
#[tokio::test]
async fn a_camp_notice_wakes_each_thread_member() {
    let (_temp, orch, db) = notice_fixture().await;
    db.write(|conn| {
        Box::pin(async move {
            cairn_db::storage::camps::create_camp_conn(
                conn,
                &cairn_db::storage::camps::NewCamp {
                    project_id: "p".into(),
                    slug: "talk".into(),
                    title: "Talk".into(),
                },
                &CampSubject::Thread {
                    thread_id: "tl".into(),
                },
                &[
                    CampSubject::Thread {
                        thread_id: "t".into(),
                    },
                    CampSubject::Operator,
                ],
                1,
            )
            .await
        })
    })
    .await
    .unwrap();
    let id = notify_camp(&orch, rebuild_notice("talk", "talk:test"))
        .await
        .unwrap()
        .unwrap();
    crate::routes::drain_fact_log(&orch).await;
    let woke = |thread: &str| {
        (
            format!("thread:{thread}"),
            "addressed:camp_notice.camp".to_string(),
            Some("delivered".to_string()),
        )
    };
    assert_eq!(wakes_of(&db, id).await, vec![woke("t"), woke("tl")]);
    let told = db
        .query_opt_text(
            "SELECT content FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = 'watcher-run') ORDER BY created_at DESC LIMIT 1",
            (),
        )
        .await
        .unwrap()
        .expect("a member was woken");
    assert!(
        told.starts_with("From camp talk: Cairn was rebuilt."),
        "{told}"
    );
}

/// A team project's base camp lives in its team replica, and so do its
/// threads' jobs. The notice reaches them: their jurisdiction is the team
/// project the camp named, not a private-database lookup that cannot find
/// them.
#[tokio::test]
async fn a_team_camp_notice_reaches_threads_that_exist_only_in_the_replica() {
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
    team.ensure_base_camp(project, 1).await.unwrap();
    orch.db
        .register_team_db_for_test("teamT".to_string(), team.clone())
        .await;
    orch.db.set_route("tp", Some("teamT".to_string())).await;

    notify_camp(
        &orch,
        CampNotice {
            project_id: project,
            project_key: "tp",
            ..rebuild_notice(BASE_CAMP_SLUG, "build-change:team")
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
    assert_eq!(reached, ["camp-passive:camp_notice.camp"]);
}

/// A coalesced camp wake carrying several facts shows each fact's camp line,
/// in one message from the camp, rather than a generic count of rows.
#[tokio::test]
async fn a_coalesced_camp_wake_shows_each_fact_s_camp_line() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
    db.execute_batch(
        "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('n0','p','n0','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('n0-job','n0','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('n0-run','n0-job','running',1,1);",
    )
    .await
    .unwrap();
    db.ensure_base_camp("p", 1).await.unwrap();
    let mut rows = Vec::new();
    for (identity, summary) in [
        ("drain:a", "bgsrv-ub drains."),
        ("drain:b", "bgsrv-ub is back."),
    ] {
        rows.push(
            notify_camp(
                &orch,
                CampNotice {
                    kind: "drain",
                    summary: summary.into(),
                    ..rebuild_notice(BASE_CAMP_SLUG, identity)
                },
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    crate::routes::drain_fact_log(&orch).await;
    let told = db
        .query_all(
            "SELECT sender_name, content FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = 'n0-run')",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(told.len(), 1, "one wake: {told:?}");
    assert_eq!(told[0].0, "cairn://p/cairn/camps/base");
    assert_eq!(
        told[0].1,
        format!(
            "From camp base: bgsrv-ub drains. \u{2014} cairn://p/cairn/events/{}\nFrom camp base: bgsrv-ub is back. \u{2014} cairn://p/cairn/events/{}",
            rows[0], rows[1]
        )
    );
    // The persisted direct renders as its camp lines wherever it is shown.
    let message = crate::models::Message {
        id: "m".into(),
        channel_type: crate::models::ChannelType::Direct,
        channel_id: None,
        sender_run_id: None,
        sender_name: told[0].0.clone(),
        recipients: Vec::new(),
        content: told[0].1.clone(),
        created_at: 1,
        urgency: None,
    };
    assert_eq!(
        cairn_attention::message_render::render_direct_message(&message, "n0"),
        told[0].1
    );
}

/// Specimen 4: one base camp notice is one fact row and one receipt per
/// thread, however many threads the project has, and neither a second drain
/// nor a restart's recovery adds a message or loses a receipt.
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
        db.ensure_base_camp("p", 1).await.unwrap();
        notify_camp(
            &orch,
            rebuild_notice(BASE_CAMP_SLUG, &format!("build-change:{threads}")),
        )
        .await
        .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        let count = |sql: &'static str| {
            let db = db.clone();
            async move { db.query_opt_i64(sql, ()).await.unwrap().unwrap() }
        };
        let members = threads as i64 + 1;
        assert_eq!(
            count("SELECT COUNT(*) FROM fact_log WHERE source = 'camp_notice'").await,
            1
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM delivery_receipts WHERE state = 'delivered'").await,
            members,
            "{threads} threads"
        );
        let told = "SELECT COUNT(*) FROM messages WHERE content LIKE 'From camp base%'";
        assert_eq!(count(told).await, members);

        crate::storage::recover_fact_log(&db, i64::MAX / 2, 0)
            .await
            .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        assert_eq!(count(told).await, members, "recovery adds nothing");
        assert_eq!(
            count("SELECT COUNT(*) FROM delivery_receipts").await,
            members
        );
    }
}
