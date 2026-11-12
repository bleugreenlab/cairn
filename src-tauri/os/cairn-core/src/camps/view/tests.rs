use super::*;
use crate::storage::migrated_test_db;
use cairn_db::storage::camps::{create_camp_conn, NewCamp};

async fn project(db: &LocalDb) {
    db.execute_batch(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tx','p','visuals','active',2,2);",
    )
    .await
    .unwrap();
}

async fn set_status(db: &LocalDb, status: &str) {
    db.execute(
        "UPDATE threads SET status = ?1 WHERE id = 'tx'",
        (status.to_string(),),
    )
    .await
    .unwrap();
}

/// A thread that closes while it is a member leaves the member list and the
/// count, and reappears with its `since` intact when it reopens: closing makes
/// membership dormant, it does not delete it.
#[tokio::test]
async fn a_closed_thread_is_not_a_member_until_it_reopens() {
    let db = migrated_test_db("camp-view-closed").await;
    project(&db).await;
    db.write(|conn| {
        Box::pin(async move {
            create_camp_conn(
                conn,
                &NewCamp {
                    project_id: "p".into(),
                    slug: "rebuild".into(),
                    title: "Rebuild".into(),
                },
                &CampSubject::Thread {
                    thread_id: "tl".into(),
                },
                &[
                    CampSubject::Thread {
                        thread_id: "tx".into(),
                    },
                    CampSubject::Operator,
                ],
                3,
            )
            .await
        })
    })
    .await
    .unwrap();

    async fn seen(db: &LocalDb) -> (CampSummary, Vec<MemberEntry>) {
        let view = camp_view(db, None, "p", "cairn", "rebuild")
            .await
            .unwrap()
            .unwrap();
        (view.summary, view.members)
    }

    let (summary, members) = seen(&db).await;
    assert_eq!(summary.members, 3);
    assert!(summary.operator_member);
    let mut labels: Vec<_> = summary
        .member_labels
        .iter()
        .map(|label| label.label.as_str())
        .collect();
    labels.sort();
    assert_eq!(labels, ["general", "operator", "visuals"]);
    assert_eq!(members.len(), 3);

    set_status(&db, "closed").await;
    let (summary, members) = seen(&db).await;
    assert_eq!(summary.members, 2, "a closed thread is not counted");
    assert!(members.iter().all(|entry| entry.subject_key != "tx"));

    set_status(&db, "active").await;
    let (summary, members) = seen(&db).await;
    assert_eq!(summary.members, 3);
    let back = members
        .iter()
        .find(|entry| entry.subject_key == "tx")
        .expect("a reopened thread is a member again");
    assert_eq!(back.since, 3);
}

/// Base camp's members are the project's open threads, counted and never
/// listed by name, and the operator is never one of them.
#[tokio::test]
async fn base_camp_counts_every_open_thread_and_lists_none() {
    let db = migrated_test_db("camp-view-base").await;
    project(&db).await;
    db.ensure_base_camp("p", 1).await.unwrap();
    let base = camp_view(&db, None, "p", "cairn", "base")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(base.summary.members, 2);
    assert!(base.summary.member_labels.is_empty());
    assert!(!base.summary.operator_member);
    set_status(&db, "closed").await;
    let base = camp_view(&db, None, "p", "cairn", "base")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(base.summary.members, 1);
    assert_eq!(base.members.len(), 1);
}
