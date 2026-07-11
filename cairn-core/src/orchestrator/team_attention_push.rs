//! Best-effort cloud push delivery for team-owned issue attention.

use async_trait::async_trait;
use cairn_common::ids::{parse_route_scope, RouteScope};
use serde::{Deserialize, Serialize};

use super::{AttentionEvent, AttentionFact, Orchestrator};
use crate::models::{IssueAttention, IssueStatus};
use crate::storage::RowExt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamAttentionPayload {
    pub team_id: String,
    pub project_key: String,
    pub issue_number: i32,
    pub kind: String,
    pub title: String,
    pub body: String,
}

#[async_trait]
pub trait TeamAttentionSender: Send + Sync {
    async fn send(
        &self,
        url: &str,
        device_jwt: &str,
        payload: &TeamAttentionPayload,
    ) -> Result<(), String>;
}

#[derive(Default)]
pub struct HttpTeamAttentionSender {
    client: reqwest::Client,
}

#[async_trait]
impl TeamAttentionSender for HttpTeamAttentionSender {
    async fn send(
        &self,
        url: &str,
        device_jwt: &str,
        payload: &TeamAttentionPayload,
    ) -> Result<(), String> {
        let response = self
            .client
            .post(url)
            .bearer_auth(device_jwt)
            .json(payload)
            .send()
            .await
            .map_err(|error| format!("failed to request team attention push: {error}"))?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(format!("team attention push failed ({status}): {body}"))
    }
}

#[derive(Clone)]
struct NotificationIntent {
    team_id: String,
    kind: &'static str,
    need: &'static str,
    fingerprint: String,
}

fn notification_intent(event: &AttentionEvent) -> Option<NotificationIntent> {
    let RouteScope::Team(team_id) = parse_route_scope(&event.issue_id).ok()? else {
        return None;
    };

    let detail_uri = event.fact.key().detail_uri;
    let review_ready = detail_uri
        .as_deref()
        .is_some_and(|detail| detail.ends_with("/pr"));
    let (kind, need) = match (&event.fact, &event.attention, &event.status) {
        (AttentionFact::Resolved { final_status }, _, _)
            if *final_status == IssueStatus::Failed =>
        {
            ("blocked", "failed and needs attention")
        }
        (AttentionFact::Resolved { .. }, _, _) => ("finished", "finished and is ready for review"),
        (AttentionFact::AgentIdleWithWork { .. }, _, _) if review_ready => {
            ("finished", "finished and is ready for review")
        }
        (_, IssueAttention::NeedsInput, _) => ("question", "needs an answer"),
        (_, IssueAttention::NeedsAuthorization, _) => ("permission", "needs permission approval"),
        (_, IssueAttention::NeedsApproval, _) if review_ready => {
            ("finished", "finished and is ready for review")
        }
        (_, IssueAttention::NeedsApproval, _) => ("blocked", "is blocked and needs approval"),
        _ => return None,
    };

    let detail = detail_uri.unwrap_or_else(|| {
        if matches!(event.fact, AttentionFact::Resolved { .. }) {
            event.updated_at.to_string()
        } else {
            event.issue_uri.clone()
        }
    });
    Some(NotificationIntent {
        team_id,
        kind,
        need,
        fingerprint: format!("{}:{kind}:{detail}:{}", event.issue_id, event.updated_at),
    })
}

/// Schedule a fail-open notification without delaying the attention event itself.
pub fn maybe_notify(orch: &Orchestrator, event: &AttentionEvent) {
    let Some(intent) = notification_intent(event) else {
        return;
    };
    let dbs = orch.db.clone();
    let local = orch.db.local.clone();
    let sender = orch.team_attention_sender.clone();
    let dedupe = orch.team_attention_push_dedupe.clone();
    let api_url = orch.api_config.push_notify_url();
    let event = event.clone();

    tokio::spawn(async move {
        {
            let mut seen = dedupe.lock().await;
            if seen.get(&event.issue_id) == Some(&intent.fingerprint) {
                return;
            }
            seen.insert(event.issue_id.clone(), intent.fingerprint.clone());
        }
        let result = async {
            let db = crate::issues::crud::owning_db_for_issue(&dbs, &event.issue_id)
                .await
                .map_err(|error| error.to_string())?;
            let issue_id = event.issue_id.clone();
            let (project_key, number, issue_title): (String, i32, String) = db
                .query_one(
                    "SELECT p.key, i.number, i.title FROM issues i JOIN projects p ON p.id = i.project_id WHERE i.id = ?1",
                    (issue_id,),
                    |row| Ok((row.text(0)?, row.i64(1)? as i32, row.text(2)?)),
                )
                .await
                .map_err(|error| error.to_string())?;
            let device_jwt = crate::account::team_sync::read_device_jwt(&local)
                .await?
                .ok_or_else(|| "no connected account device JWT".to_string())?;
            let title = format!("{project_key}-{number} {need}", need = intent.need);
            let body = format!("{project_key}-{number}: {issue_title}");
            sender
                .send(
                    &api_url,
                    &device_jwt,
                    &TeamAttentionPayload {
                        team_id: intent.team_id,
                        project_key,
                        issue_number: number,
                        kind: intent.kind.to_string(),
                        title,
                        body,
                    },
                )
                .await
        }
        .await;
        if let Err(error) = result {
            log::warn!(
                "team attention push skipped for {}: {error}",
                event.issue_uri
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ApiConfig;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    const TEAM_ID: &str = "team1";
    const ISSUE_ID: &str = "team1~123e4567-e89b-42d3-a456-426614174000";

    struct RecordingSender {
        calls: Arc<Mutex<Vec<TeamAttentionPayload>>>,
        called: Arc<Notify>,
        fail: bool,
    }

    #[async_trait]
    impl TeamAttentionSender for RecordingSender {
        async fn send(
            &self,
            _url: &str,
            _device_jwt: &str,
            payload: &TeamAttentionPayload,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push(payload.clone());
            self.called.notify_waiters();
            if self.fail {
                Err("offline".to_string())
            } else {
                Ok(())
            }
        }
    }

    async fn setup(
        team: bool,
        fail: bool,
    ) -> (
        Orchestrator,
        Arc<Mutex<Vec<TeamAttentionPayload>>>,
        Arc<Notify>,
    ) {
        let local = crate::storage::migrated_test_db("team-attention-local.db").await;
        let encrypted = crate::account::jwt::encrypt_jwt_for_storage("device-jwt").unwrap();
        local.execute(
            "INSERT INTO account(user_id, email, name, device_id, plan, jwt_encrypted, jwt_expires_at, org_memberships, connected_at, updated_at) VALUES('u','u@example.com','User','d','free',?1,9999999999,'[]',1,1)",
            (encrypted,),
        ).await.unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let dbs = Arc::new(DbState::new(Arc::new(local), search));
        if team {
            let team_db =
                Arc::new(crate::storage::migrated_test_db("team-attention-team.db").await);
            team_db.execute_script(&format!(
                "INSERT INTO teams(id,name,sync_url,replica_path,created_at) VALUES('{TEAM_ID}','Team','http://sync','/tmp/team.db',1);
                 INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
                 INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
                 INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('{ISSUE_ID}','p',7,'Fix the thing','waiting','active','needs_authorization',1,2);"
            )).await.unwrap();
            dbs.insert_team_db_for_test(TEAM_ID, team_db).await;
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let called = Arc::new(Notify::new());
        let sender = Arc::new(RecordingSender {
            calls: calls.clone(),
            called: called.clone(),
            fail,
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, root.join("config"))
            .api_config(ApiConfig {
                base_url: "http://unused".to_string(),
            })
            .team_attention_sender(sender)
            .build();
        (orch, calls, called)
    }

    fn permission_event(issue_id: &str) -> AttentionEvent {
        AttentionEvent {
            issue_id: issue_id.to_string(),
            issue_uri: "cairn://p/PROJ/7".to_string(),
            fact: AttentionFact::Permission {
                detail_uri: "cairn://p/PROJ/7/1/builder/permissions/p1".to_string(),
                content: super::super::attention::PermissionContent {
                    tool_name: "run".to_string(),
                    tool_use_id: "tool-1".to_string(),
                    input: serde_json::json!({}),
                },
            },
            attention: IssueAttention::NeedsAuthorization,
            status: IssueStatus::Waiting,
            updated_at: 2,
        }
    }

    async fn wait_for_call(called: &Notify) {
        tokio::time::timeout(std::time::Duration::from_secs(2), called.notified())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn team_attention_sends_once_with_human_payload() {
        let (orch, calls, called) = setup(true, false).await;
        let event = permission_event(ISSUE_ID);
        orch.emit_attention_event(event.clone());
        wait_for_call(&called).await;
        orch.emit_attention_event(event);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        {
            let recorded = calls.lock().unwrap();
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].team_id, TEAM_ID);
            assert_eq!(recorded[0].project_key, "PROJ");
            assert_eq!(recorded[0].issue_number, 7);
            assert_eq!(recorded[0].kind, "permission");
            assert_eq!(recorded[0].title, "PROJ-7 needs permission approval");
            assert_eq!(recorded[0].body, "PROJ-7: Fix the thing");
        }

        let mut reentered = permission_event(ISSUE_ID);
        reentered.updated_at = 3;
        orch.emit_attention_event(reentered);
        wait_for_call(&called).await;
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn newly_opened_pr_notifies_while_attention_is_none() {
        let (orch, calls, called) = setup(true, false).await;
        let event = AttentionEvent {
            issue_id: ISSUE_ID.to_string(),
            issue_uri: "cairn://p/PROJ/7".to_string(),
            fact: AttentionFact::AgentIdleWithWork {
                detail_uri: "cairn://p/PROJ/7/1/builder/pr".to_string(),
            },
            attention: IssueAttention::None,
            status: IssueStatus::Active,
            updated_at: 3,
        };
        orch.emit_attention_event(event);
        wait_for_call(&called).await;
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].kind, "finished");
        assert_eq!(calls[0].title, "PROJ-7 finished and is ready for review");
    }

    #[tokio::test]
    async fn local_attention_never_sends() {
        let (orch, calls, _) = setup(false, false).await;
        orch.emit_attention_event(permission_event("123e4567-e89b-42d3-a456-426614174000"));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sender_failure_does_not_block_attention_delivery() {
        let (orch, calls, called) = setup(true, true).await;
        let mut attention = orch.attention_changed.subscribe();
        orch.emit_attention_event(permission_event(ISSUE_ID));
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), attention.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.issue_id, ISSUE_ID);
        wait_for_call(&called).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
    }
}
