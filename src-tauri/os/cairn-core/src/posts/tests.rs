//! The one publish path: who may address a camp, what a camp post writes, and
//! whom it reaches.

use super::*;
use crate::models::CampSubject;
use crate::routes::fixture::orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceTransport, VerificationMethod, VerificationRecord,
    VerificationStatus, VerificationStrength,
};
use cairn_db::storage::camps::{create_camp_conn, NewCamp};

fn author(
    principal: PrincipalRef,
    verification: VerificationRecord,
    transport: AppearanceTransport,
    address: Address,
) -> (PrincipalRef, AppearanceSnapshot) {
    let evidence = AppearanceEvidence::new(transport, address, verification, 900, None).unwrap();
    let appearance = AppearanceSnapshot::new(principal.clone(), evidence, vec![], None).unwrap();
    (principal, appearance)
}

fn thread_author(name: &str) -> (PrincipalRef, AppearanceSnapshot) {
    let run = "d575806b-1a4d-4a1e-9b0e-2f3c4d5e6f70";
    let node = format!("cairn://p/cairn/{name}");
    author(
        PrincipalRef::Agent {
            node: node.clone(),
            run_id: Some(run.to_string()),
        },
        VerificationRecord::new(
            VerificationMethod::NodeSession,
            VerificationStatus::Verified,
            None,
            None,
            Some(run.to_string()),
            None,
            VerificationStrength::new("session-bound").unwrap(),
            900,
        )
        .unwrap(),
        AppearanceTransport::ResourcePatch,
        Address::Resource { node },
    )
}

fn operator() -> (PrincipalRef, AppearanceSnapshot) {
    author(
        PrincipalRef::Human {
            issuer: "https://identity.example".into(),
            subject: "operator".into(),
            organization: None,
        },
        VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("https://identity.example".to_string()),
            Some("operator".to_string()),
            None,
            None,
            VerificationStrength::new("strong").unwrap(),
            900,
        )
        .unwrap(),
        AppearanceTransport::AuthenticatedOperator,
        Address::Invoke { origin: None },
    )
}

/// Beside the fixture's `ui` thread (job `watcher`) and issue node (`writer`):
/// `general` and `quiet`, all three in base camp; `general` and `ui` are the
/// thread members of camp `rebuild`, beside the operator.
async fn camps(db: &LocalDb) -> (Camp, Camp) {
    db.execute_batch(
        "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tl','p','general','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('lead-job','tl','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('lead-run','lead-job','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq','p','quiet','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('quiet-job','tq','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('quiet-run','quiet-job','running',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('q','w','Other','other','/tmp/other',1,1);",
    )
    .await
    .unwrap();
    let (base, _) = db.ensure_base_camp("p", 1).await.unwrap();
    let rebuild = db
        .write(|conn| {
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
                            thread_id: "t".into(),
                        },
                        CampSubject::Operator,
                    ],
                    2,
                )
                .await
            })
        })
        .await
        .unwrap();
    (base, rebuild)
}

fn to(camp: &Camp) -> PostTarget {
    PostTarget::Camp {
        project_id: "p".into(),
        camp_id: camp.id.clone(),
    }
}

async fn publish(
    orch: &Orchestrator,
    target: PostTarget,
    (author, appearance): (PrincipalRef, AppearanceSnapshot),
    actor: Option<&str>,
) -> Result<Published<Post>, PublishError> {
    publish_post(
        orch,
        PublishPost {
            target,
            title: Some("Interface agreed".into()),
            content: "The rebuild interface is agreed.".into(),
            author,
            appearance,
            actor_job_id: actor.map(str::to_string),
        },
    )
    .await
}

async fn wakes(db: &LocalDb, source: &str) -> Vec<(String, String)> {
    let mut wakes = db
        .query_all(
            "SELECT job_id, clause_ref FROM fact_log_wakes WHERE source = ?1",
            (source.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap();
    wakes.sort();
    wakes
}

async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_opt_i64(sql, ()).await.unwrap().unwrap_or_default()
}

/// One camp post is one row and one fact, and the fact wakes every other
/// thread member of the camp, never the author's own thread. A comment on it
/// reaches the camp the same way.
#[tokio::test]
async fn a_camp_post_is_one_row_and_one_fact_that_wakes_the_other_members() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    let (_, rebuild) = camps(&db).await;

    let published = publish(&orch, to(&rebuild), thread_author("ui"), Some("watcher"))
        .await
        .unwrap();
    assert_eq!(
        published.record.camp_id.as_deref(),
        Some(rebuild.id.as_str())
    );
    assert_eq!(published.record.project_id.as_deref(), Some("p"));
    crate::routes::drain_fact_log(&orch).await;

    assert_eq!(count(&db, "SELECT COUNT(*) FROM posts").await, 1);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM fact_log WHERE source = 'post_created'"
        )
        .await,
        1
    );
    let fields = db
        .query_opt_text(
            "SELECT fields_json FROM fact_log WHERE source = 'post_created'",
            (),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        fields.contains("\"camp\":\"cairn://p/cairn/camps/rebuild\""),
        "{fields}"
    );
    assert_eq!(
        wakes(&db, "post_created").await,
        vec![(
            "lead-job".to_string(),
            "addressed:post_created.camp".to_string()
        )],
        "the other member is woken; the author is not woken by its own post"
    );

    publish_comment(
        &orch,
        PublishComment {
            post_id: published.record.id,
            content: "Agreed from general.".into(),
            author: thread_author("general").0,
            appearance: thread_author("general").1,
            actor_job_id: Some("lead-job".into()),
        },
    )
    .await
    .unwrap();
    crate::routes::drain_fact_log(&orch).await;
    let told = wakes(&db, "post_commented").await;
    assert!(
        told.iter().any(|(job, clause)| job == "watcher"
            && (clause == "addressed:post_commented.camp"
                || clause == "addressed:post_commented.postAuthor")),
        "the post's author, a member, hears the comment: {told:?}"
    );
    assert!(
        !told.iter().any(|(job, _)| job == "lead-job"),
        "the commenter is not woken by its own comment: {told:?}"
    );
}

/// The operator posts to base camp. The fact has no actor job and reaches
/// every open thread passively through the camp, never as a direct message
/// from the operator, and wakes none of them.
#[tokio::test]
async fn an_operator_base_camp_post_reaches_every_thread_passively() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    let (base, _) = camps(&db).await;

    publish(&orch, to(&base), operator(), None).await.unwrap();
    crate::routes::drain_fact_log(&orch).await;

    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM fact_log WHERE source = 'post_created' AND actor_job_id IS NULL"
        )
        .await,
        1
    );
    let reached = wakes(&db, "post_created").await;
    assert!(
        reached
            .iter()
            .all(|(_, clause)| clause == "camp-passive:post_created.camp"),
        "every delivery is a passive base camp delivery: {reached:?}"
    );
    let jobs: Vec<_> = reached.iter().map(|(job, _)| job.as_str()).collect();
    assert_eq!(jobs, ["lead-job", "quiet-job", "watcher"], "{reached:?}");
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM fact_log_wakes WHERE source = 'post_created' AND level != 'passive'"
        )
        .await,
        0,
        "no thread is woken"
    );
}

/// Every refusal leaves no row: a run that acts for no thread, a thread that
/// is not a member of the camp, an archived camp, and a camp of another
/// project. Base camp takes any thread of its project.
#[tokio::test]
async fn refused_camp_posts_write_nothing_and_base_camp_takes_any_thread() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    let (base, rebuild) = camps(&db).await;

    let refusals = [
        (to(&rebuild), "writer", "acts for no thread"),
        (to(&rebuild), "quiet-job", "is not a member"),
        (
            PostTarget::Camp {
                project_id: "q".into(),
                camp_id: rebuild.id.clone(),
            },
            "watcher",
            "own project",
        ),
    ];
    for (target, actor, expected) in refusals {
        match publish(&orch, target, thread_author("ui"), Some(actor)).await {
            Err(PublishError::Refused(message)) => {
                assert!(message.contains(expected), "{message}")
            }
            other => panic!(
                "expected a refusal naming {expected:?}, got {:?}",
                other.err()
            ),
        }
    }
    db.execute_batch(&format!(
        "UPDATE camps SET status = 'archived', archived_at = 5 WHERE id = '{}'",
        rebuild.id
    ))
    .await
    .unwrap();
    assert!(matches!(
        publish(&orch, to(&rebuild), thread_author("ui"), Some("watcher")).await,
        Err(PublishError::Refused(message)) if message.contains("archived")
    ));
    assert_eq!(count(&db, "SELECT COUNT(*) FROM posts").await, 0);

    publish(&orch, to(&base), thread_author("quiet"), Some("quiet-job"))
        .await
        .expect("any thread of the project posts to base camp");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM posts").await, 1);
}
