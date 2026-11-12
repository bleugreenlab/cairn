use super::*;
use crate::storage::migrated_test_db;
use cairn_db::storage::camps::{
    pitch_camp_conn, set_camp_level_conn, set_issue_camp_conn, CampAssignedBy, NewCamp,
};

/// The state line counts by status, omits what is zero, and never quotes an
/// issue's text.
#[test]
fn the_state_line_counts_open_work_and_the_last_post() {
    let counts = IssueCounts::from_statuses(["active", "waiting", "waiting", "merged"]);
    assert_eq!(counts.open(), 3);
    assert_eq!(
        state_line(&counts, Some(1_000), 1_000 + 2 * 3_600),
        "3 open (1 active, 2 waiting) · 1 merged · last post 2h ago"
    );
    assert_eq!(state_line(&IssueCounts::default(), None, 0), "");
}

/// Base camp's view carries every pitched camp as the roadmap, each with its
/// joined issues counted; a tenet that cites a joined issue is linked to the
/// camp, and one that only mentions it in words is not.
#[tokio::test]
async fn base_camp_carries_the_roadmap_and_a_camp_its_linked_tenets() {
    let db = migrated_test_db("camp-view").await;
    db.execute_batch(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i1','p',7,'Seven','active',1,10);
         INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i2','p',8,'Eight','merged',1,20);
         INSERT INTO tenets(id,project_id,number,proposing_thread_id,statement,rationale,status,origin,proposer_principal_json,created_at,updated_at)
           VALUES('n1','p',3,'tl','Ship rebuilds as actions','see cairn://p/cairn/7','proposed','standing','{}',1,1);
         INSERT INTO tenets(id,project_id,number,proposing_thread_id,statement,status,origin,proposer_principal_json,created_at,updated_at)
           VALUES('n2','p',4,'tl','Issue 7 is unrelated prose','proposed','standing','{}',1,1);",
    )
    .await
    .unwrap();
    db.ensure_base_camp("p", "tl", 1).await.unwrap();
    db.write(|conn| {
        Box::pin(async move {
            let camp = pitch_camp_conn(
                conn,
                &NewCamp {
                    project_id: "p".into(),
                    slug: "rebuild".into(),
                    title: "Rebuild".into(),
                    lead_thread_id: "tl".into(),
                },
                None,
                2,
            )
            .await?;
            set_issue_camp_conn(conn, "i1", Some(&camp), None, 3).await?;
            set_issue_camp_conn(conn, "i2", Some(&camp), None, 4).await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let base = camp_view(&db, None, "p", "cairn", "base", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(base.roadmap.len(), 1);
    let rebuild = &base.roadmap[0];
    assert_eq!(rebuild.slug, "rebuild");
    assert_eq!(rebuild.issue_numbers, vec![7, 8]);
    assert_eq!(rebuild.issues.open(), 1);
    assert_eq!(rebuild.issues.merged, 1);
    assert!(base.description.is_none() && base.questions.is_empty());

    let camp = camp_view(&db, None, "p", "cairn", "rebuild", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        camp.roadmap.is_empty(),
        "only base camp carries the roadmap"
    );
    assert_eq!(
        camp.tenets
            .iter()
            .map(|tenet| tenet.number)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert!(
        camp.history.iter().all(|entry| !entry.text.contains('_')),
        "every journal kind reads as words: {:?}",
        camp.history
    );
}

/// A thread that closes while it is a participant leaves the census and the
/// count, and reappears when it reopens with its level and `since` intact:
/// closing makes membership dormant, it does not delete it.
#[tokio::test]
async fn a_closed_thread_is_not_a_participant_until_it_reopens() {
    let db = migrated_test_db("camp-view-closed").await;
    db.execute_batch(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tx','p','visuals','active',1,1);",
    )
    .await
    .unwrap();
    db.write(|conn| {
        Box::pin(async move {
            let camp = pitch_camp_conn(
                conn,
                &NewCamp {
                    project_id: "p".into(),
                    slug: "rebuild".into(),
                    title: "Rebuild".into(),
                    lead_thread_id: "tl".into(),
                },
                None,
                2,
            )
            .await?;
            set_camp_level_conn(
                conn,
                &camp,
                &CampSubject::Thread {
                    thread_id: "tx".into(),
                },
                CampLevel::Wake,
                "owns the visuals",
                CampAssignedBy::Agent,
                None,
                3,
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    async fn seen(db: &LocalDb) -> (usize, Vec<CensusEntry>) {
        let view = camp_view(db, None, "p", "cairn", "rebuild", 100)
            .await
            .unwrap()
            .unwrap();
        (view.summary.participants, view.census)
    }
    async fn set_status(db: &LocalDb, status: &str) {
        db.execute(
            "UPDATE threads SET status = ?1 WHERE id = 'tx'",
            (status.to_string(),),
        )
        .await
        .unwrap();
    }

    let (count, census) = seen(&db).await;
    assert_eq!(count, 2);
    assert_eq!(census.len(), 2);

    set_status(&db, "closed").await;
    let (count, census) = seen(&db).await;
    assert_eq!(count, 1, "a closed thread is not counted");
    assert!(census.iter().all(|entry| entry.subject_key != "tx"));

    set_status(&db, "active").await;
    let (count, census) = seen(&db).await;
    assert_eq!(count, 2);
    let back = census
        .iter()
        .find(|entry| entry.subject_key == "tx")
        .expect("a reopened thread is a participant again");
    assert_eq!(back.level, Some(CampLevel::Wake));
    assert_eq!(back.role, CampRole::Wake);
    assert_eq!(back.since, 3);
}
