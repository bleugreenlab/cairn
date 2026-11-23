use super::*;
use crate::execution::gates::sweep::tests::{env, resolve_issue};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn branch_system_message_includes_live_notice_but_not_resolved_notice() {
    let env = env("notice-message-boundary.db").await;
    env.db
        .execute("UPDATE jobs SET branch='agent-notice' WHERE id='coord'", ())
        .await
        .unwrap();
    crate::resource_notices::set(&env.orch, "prj", "cairn://p/prj/*/*/*/rebase", "operator",
        &json!({"text":"message-boundary-token","until":[{"fact":"issue_resolution","issue":"cairn://p/prj/1","resolution":"merged"}]}),
        false).await.unwrap();
    notify_on_branch_jobs(
        &env.orch,
        &env.db,
        "p",
        "agent-notice",
        "before-resolution",
        DeliveryUrgency::Passive,
        "notice-before",
    )
    .await;
    let before = pending_system_note(&env.db, "before-resolution").await;
    assert!(before.contains("message-boundary-token"), "{before}");

    resolve_issue(&env, 1, "merged").await;
    notify_on_branch_jobs(
        &env.orch,
        &env.db,
        "p",
        "agent-notice",
        "after-resolution",
        DeliveryUrgency::Passive,
        "notice-after",
    )
    .await;
    let after = pending_system_note(&env.db, "after-resolution").await;
    assert!(!after.contains("message-boundary-token"), "{after}");
}

async fn pending_system_note(db: &LocalDb, token: &str) -> String {
    let pending = crate::orchestrator::attention_push::list_pending_live(db, "coord")
        .await
        .unwrap();
    for push in pending {
        let Some(id) = crate::orchestrator::attention_push::direct_message_id_from_key(&push.key)
        else {
            continue;
        };
        let content = db
            .query_opt_text(
                "SELECT content FROM messages WHERE id=?1",
                (id.to_string(),),
            )
            .await
            .unwrap()
            .expect("a direct push must reference a stored message");
        if content.contains(token) {
            return content;
        }
    }
    panic!("base advance must queue a system direct containing {token}");
}
