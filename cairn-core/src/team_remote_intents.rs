//! Owner-side delivery of remote intents pulled into a team replica.

use crate::mcp::handlers::permission::{PermissionDecision, PermissionScope};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::params;
use serde::Deserialize;
use serde_json::{json, Value};

const BATCH_LIMIT: i64 = 32;
const STALE_LEASE_MS: i64 = 5 * 60 * 1000;
const MAX_ERROR_BYTES: usize = 1024;

#[derive(Debug, Clone)]
struct Intent {
    id: String,
    execution_id: String,
    kind: String,
    target_uri: String,
    payload_json: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepResult {
    pub claimed: usize,
    pub succeeded: usize,
    pub failed: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagePayload {
    content: String,
    #[serde(default)]
    escalate: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionPayload {
    decision: String,
    scope: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptPayload {
    response: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommentPayload {
    content: String,
}

pub async fn sweep_remote_intents_all_open_teams(
    orch: &Orchestrator,
    device_id: &str,
) -> SweepResult {
    let team_ids = orch.db.open_team_ids().await;
    if team_ids.is_empty() {
        return SweepResult::default();
    }
    let mut total = SweepResult::default();
    for team_id in team_ids {
        if let Some(db) = orch.db.team_db(&team_id).await {
            let result = sweep_remote_intents_for_team(orch, &db, device_id).await;
            total.claimed += result.claimed;
            total.succeeded += result.succeeded;
            total.failed += result.failed;
        }
    }
    total
}

pub async fn sweep_remote_intents_for_team(
    orch: &Orchestrator,
    team_db: &LocalDb,
    device_id: &str,
) -> SweepResult {
    let candidates = match scan_candidates(team_db, device_id).await {
        Ok(rows) => rows,
        Err(error) => {
            log::warn!("remote intent scan failed: {error}");
            return SweepResult::default();
        }
    };
    let mut result = SweepResult::default();
    for intent in candidates {
        match claim(team_db, &intent.id, device_id).await {
            Ok(true) => result.claimed += 1,
            Ok(false) => continue,
            Err(error) => {
                log::warn!("remote intent claim {} failed: {error}", intent.id);
                continue;
            }
        }
        let owned = match verify_owner(team_db, &intent.execution_id, device_id).await {
            Ok(owned) => owned,
            Err(error) => {
                if let Err(write_error) =
                    finish_failure(team_db, &intent.id, device_id, &error).await
                {
                    log::warn!(
                        "remote intent {} ownership-check failure write-back failed: {write_error}",
                        intent.id
                    );
                } else {
                    result.failed += 1;
                }
                continue;
            }
        };
        if !owned {
            if let Err(error) = relinquish_claim(team_db, &intent.id, device_id).await {
                log::warn!(
                    "remote intent {} ownership-change relinquish failed: {error}",
                    intent.id
                );
            }
            continue;
        }
        let outcome = dispatch(orch, team_db, &intent).await;
        match outcome {
            Ok(value) => match finish_success(team_db, &intent.id, device_id, value).await {
                Ok(true) => result.succeeded += 1,
                Ok(false) => log::warn!(
                    "remote intent {} success write-back lost ownership",
                    intent.id
                ),
                Err(error) => log::warn!(
                    "remote intent {} success write-back failed: {error}",
                    intent.id
                ),
            },
            Err(error) => {
                if is_retryable_dispatch_error(&error) {
                    if let Err(write_error) = relinquish_claim(team_db, &intent.id, device_id).await
                    {
                        log::warn!(
                            "remote intent {} retryable-error relinquish failed: {write_error}",
                            intent.id
                        );
                    }
                    continue;
                }
                if let Err(write_error) =
                    finish_failure(team_db, &intent.id, device_id, &error).await
                {
                    log::warn!(
                        "remote intent {} failure write-back failed: {write_error}",
                        intent.id
                    );
                } else {
                    result.failed += 1;
                }
            }
        }
    }
    result
}

async fn scan_candidates(db: &LocalDb, device_id: &str) -> Result<Vec<Intent>, String> {
    let device_id = device_id.to_string();
    let stale_before = now_ms() - STALE_LEASE_MS;
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT ri.id, ri.execution_id, ri.kind, ri.target_uri, ri.payload_json
             FROM remote_intents ri JOIN executions e ON e.id = ri.execution_id
             WHERE e.runner_device_id = ?1 AND (
               ri.status = 'pending' OR
               (ri.status = 'processing' AND ri.claimed_by_device_id = ?1 AND ri.claimed_at < ?2)
             ) ORDER BY ri.created_at, ri.id LIMIT ?3",
                    params![device_id.as_str(), stale_before, BATCH_LIMIT],
                )
                .await?;
            let mut intents = Vec::new();
            while let Some(row) = rows.next().await? {
                intents.push(Intent {
                    id: row.text(0)?,
                    execution_id: row.text(1)?,
                    kind: row.text(2)?,
                    target_uri: row.text(3)?,
                    payload_json: row.text(4)?,
                });
            }
            Ok(intents)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn claim(db: &LocalDb, id: &str, device_id: &str) -> Result<bool, String> {
    let id = id.to_string();
    let device_id = device_id.to_string();
    let now = now_ms();
    let stale_before = now - STALE_LEASE_MS;
    db.write(|conn| {
        let id = id.clone();
        let device_id = device_id.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE remote_intents SET status='processing', claimed_by_device_id=?2,
               claimed_at=?3, attempt_count=attempt_count+1, updated_at=?3
             WHERE id=?1 AND (status='pending' OR
               (status='processing' AND claimed_by_device_id=?2 AND claimed_at < ?4))
               AND EXISTS (SELECT 1 FROM executions e
                 WHERE e.id=remote_intents.execution_id AND e.runner_device_id=?2)",
                    params![id.as_str(), device_id.as_str(), now, stale_before],
                )
                .await?;
            Ok(changed == 1)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn relinquish_claim(db: &LocalDb, id: &str, device_id: &str) -> Result<bool, String> {
    let id = id.to_string();
    let device_id = device_id.to_string();
    let now = now_ms();
    db.write(|conn| {
        let id = id.clone();
        let device_id = device_id.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE remote_intents SET status='pending', claimed_by_device_id=NULL,
                       claimed_at=NULL, updated_at=?3
                     WHERE id=?1 AND status='processing' AND claimed_by_device_id=?2",
                    params![id.as_str(), device_id.as_str(), now],
                )
                .await?;
            Ok(changed == 1)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn verify_owner(db: &LocalDb, execution_id: &str, device_id: &str) -> Result<bool, String> {
    let execution_id = execution_id.to_string();
    let device_id = device_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM executions WHERE id=?1 AND runner_device_id=?2",
                    params![execution_id.as_str(), device_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn validate_target(
    db: &LocalDb,
    execution_id: &str,
    project: &str,
    number: i32,
    exec_seq: i32,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let project = project.to_uppercase();
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT 1 FROM executions e JOIN issues i ON i.id=e.issue_id JOIN projects p ON p.id=i.project_id
             WHERE e.id=?1 AND upper(p.key)=?2 AND i.number=?3 AND e.seq=?4",
            params![execution_id.as_str(), project.as_str(), number, exec_seq],
        ).await?;
        if rows.next().await?.is_some() { Ok(()) } else { Err(crate::storage::DbError::Row("target does not belong to execution".into())) }
    })).await.map_err(|e| e.to_string())
}

async fn dispatch(orch: &Orchestrator, db: &LocalDb, intent: &Intent) -> Result<Value, String> {
    let resource =
        parse_uri(&intent.target_uri).ok_or_else(|| "invalid canonical target URI".to_string())?;
    match (intent.kind.as_str(), resource) {
        (
            "node_message",
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            },
        )
        | (
            "node_message",
            CairnResource::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            },
        ) => {
            validate_target(db, &intent.execution_id, &project, number, exec_seq).await?;
            let payload: MessagePayload = decode(&intent.payload_json)?;
            reject_empty(&payload.content)?;
            let request = user_request();
            let message = crate::mcp::handlers::messages::append_direct_message_for_remote_intent(
                orch,
                &request,
                &project,
                number,
                exec_seq,
                &node_id,
                None,
                &payload.content,
                payload.escalate,
                &intent.id,
            )
            .await?;
            Ok(json!({"outcome":"created_or_repaired","message":message}))
        }
        (
            "node_message",
            CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
        )
        | (
            "node_message",
            CairnResource::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
        ) => {
            validate_target(db, &intent.execution_id, &project, number, exec_seq).await?;
            let payload: MessagePayload = decode(&intent.payload_json)?;
            reject_empty(&payload.content)?;
            let request = user_request();
            let message = crate::mcp::handlers::messages::append_direct_message_for_remote_intent(
                orch,
                &request,
                &project,
                number,
                exec_seq,
                &node_id,
                Some(&task_name),
                &payload.content,
                payload.escalate,
                &intent.id,
            )
            .await?;
            Ok(json!({"outcome":"created_or_repaired","message":message}))
        }
        (
            "permission_response",
            CairnResource::NodePermission {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
        ) => {
            validate_target(db, &intent.execution_id, &project, number, exec_seq).await?;
            let payload: PermissionPayload = decode(&intent.payload_json)?;
            let decision = match payload.decision.as_str() {
                "allow" => PermissionDecision::Allow,
                "deny" => PermissionDecision::Deny,
                _ => return Err("decision must be allow or deny".into()),
            };
            let scope = match payload.scope.as_str() {
                "once" => PermissionScope::Once,
                "session" => PermissionScope::Session,
                _ => return Err("scope must be once or session".into()),
            };
            let outcome = crate::mcp::handlers::permission::answer_node_permission(
                orch, &project, number, exec_seq, &node_id, &segment, decision, scope,
            )
            .await?;
            Ok(json!({"outcome": if outcome.duplicate {"duplicate"} else {"created"}}))
        }
        (
            "prompt_response",
            CairnResource::NodeQuestion {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
        ) => {
            validate_target(db, &intent.execution_id, &project, number, exec_seq).await?;
            let payload: PromptPayload = decode(&intent.payload_json)?;
            let outcome = crate::mcp::handlers::planning::answer_node_question(
                orch,
                &project,
                number,
                exec_seq,
                &node_id,
                &segment,
                &json!({"response": payload.response}),
            )
            .await?;
            Ok(json!({"outcome": if outcome.duplicate {"duplicate"} else {"created"}}))
        }
        ("issue_comment", CairnResource::Issue { project, number }) => {
            validate_issue_target(db, &intent.execution_id, &project, number).await?;
            let payload: CommentPayload = decode(&intent.payload_json)?;
            reject_empty(&payload.content)?;
            let message =
                crate::mcp::handlers::comments_artifacts::append_issue_comment_for_remote_intent(
                    orch,
                    &user_request(),
                    &project,
                    number,
                    &payload.content,
                    &intent.id,
                )
                .await?;
            Ok(json!({"outcome":"created_or_repaired","message":message}))
        }
        (
            known @ ("node_message" | "permission_response" | "prompt_response" | "issue_comment"),
            _,
        ) => Err(format!("target URI is not valid for {known}")),
        _ => Err("unsupported remote intent kind".to_string()),
    }
}

async fn validate_issue_target(
    db: &LocalDb,
    execution_id: &str,
    project: &str,
    number: i32,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let project = project.to_uppercase();
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT 1 FROM executions e JOIN issues i ON i.id=e.issue_id JOIN projects p ON p.id=i.project_id WHERE e.id=?1 AND upper(p.key)=?2 AND i.number=?3",
            params![execution_id.as_str(), project.as_str(), number],
        ).await?;
        if rows.next().await?.is_some() { Ok(()) } else { Err(crate::storage::DbError::Row("comment target does not belong to execution issue".into())) }
    })).await.map_err(|e| e.to_string())
}

async fn finish_success(
    db: &LocalDb,
    id: &str,
    device_id: &str,
    result: Value,
) -> Result<bool, String> {
    finish(db, id, device_id, Some(result.to_string()), None).await
}

async fn finish_failure(
    db: &LocalDb,
    id: &str,
    device_id: &str,
    error: &str,
) -> Result<bool, String> {
    finish(db, id, device_id, None, Some(bound_error(error))).await
}

async fn finish(
    db: &LocalDb,
    id: &str,
    device_id: &str,
    result: Option<String>,
    error: Option<String>,
) -> Result<bool, String> {
    let id = id.to_string();
    let device_id = device_id.to_string();
    let now = now_ms();
    db.write(|conn| {
        let id = id.clone();
        let device_id = device_id.clone();
        let result = result.clone();
        let error = error.clone();
        Box::pin(async move {
            let (status, completed_result, completed_error) = if error.is_some() {
                ("failed", None, error)
            } else {
                ("succeeded", result, None)
            };
            let changed = conn
                .execute(
                    "UPDATE remote_intents SET status=?3, completed_at=?4, result_json=?5, error=?6, updated_at=?4
                     WHERE id=?1 AND status='processing' AND claimed_by_device_id=?2",
                    params![id.as_str(), device_id.as_str(), status, now, completed_result, completed_error],
                )
                .await?;
            Ok(changed == 1)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

fn decode<'a, T: Deserialize<'a>>(json: &'a str) -> Result<T, String> {
    serde_json::from_str(json).map_err(|e| format!("invalid payload: {e}"))
}

fn reject_empty(content: &str) -> Result<(), String> {
    if content.trim().is_empty() {
        Err("content must not be empty".into())
    } else {
        Ok(())
    }
}

fn is_retryable_dispatch_error(error: &str) -> bool {
    error.starts_with("Failed to record issue comment side-channel notices:")
}

fn bound_error(error: &str) -> String {
    let mut value = error.to_string();
    while value.len() > MAX_ERROR_BYTES {
        value.pop();
    }
    value
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn user_request() -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "remote_intent".into(),
        payload: Value::Null,
        tool_use_id: None,
        thread_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TEAM_MIGRATIONS};

    async fn team_db() -> (tempfile::TempDir, LocalDb) {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("team.turso.db"))
            .await
            .unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute_batch(
            "INSERT INTO teams(id,name,created_at,updated_at) VALUES('team','Team',1,1);
             INSERT INTO projects(id,team_id,name,key,repo_path,created_at,updated_at)
               VALUES('project','team','Project','PROJ','/tmp/project',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('issue','project',1,'Issue','active',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,runner_device_id)
               VALUES('exec-owned','recipe','issue','project','running',1,1,'device-a');
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,runner_device_id)
               VALUES('exec-peer','recipe','issue','project','running',1,2,'device-b');
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,runner_device_id)
               VALUES('exec-null','recipe','issue','project','running',1,3,NULL);"
        ).await.unwrap();
        (temp, db)
    }

    async fn insert_intent(db: &LocalDb, id: &str, execution_id: &str) {
        db.execute(
            "INSERT INTO remote_intents(id,execution_id,kind,target_uri,payload_json,status,created_at,updated_at)
             VALUES(?1,?2,'issue_comment','cairn://p/PROJ/1','{\"content\":\"hello\"}','pending',1,1)",
            params![id, execution_id],
        ).await.unwrap();
    }

    #[test]
    fn payloads_are_strict_and_errors_are_bounded() {
        assert!(decode::<MessagePayload>(r#"{"content":"hello","extra":true}"#).is_err());
        assert!(decode::<MessagePayload>(r#"{"content":"hello","escalate":false}"#).is_ok());
        assert_eq!(bound_error(&"x".repeat(4096)).len(), MAX_ERROR_BYTES);
    }

    #[tokio::test]
    async fn only_matching_explicit_owner_is_scanned_and_claimed() {
        let (_temp, db) = team_db().await;
        insert_intent(&db, "owned", "exec-owned").await;
        insert_intent(&db, "peer", "exec-peer").await;
        insert_intent(&db, "null", "exec-null").await;

        let candidates = scan_candidates(&db, "device-a").await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "owned");
        assert!(claim(&db, "owned", "device-a").await.unwrap());
        assert!(!claim(&db, "peer", "device-a").await.unwrap());
        assert!(!claim(&db, "null", "device-a").await.unwrap());
    }

    #[tokio::test]
    async fn same_host_claim_is_single_admission_and_terminal_rows_do_not_rescan() {
        let (_temp, db) = team_db().await;
        insert_intent(&db, "owned", "exec-owned").await;
        let (first, second) = tokio::join!(
            claim(&db, "owned", "device-a"),
            claim(&db, "owned", "device-a")
        );
        assert_ne!(first.unwrap(), second.unwrap());
        assert!(finish_success(&db, "owned", "device-a", json!({"ok":true}))
            .await
            .unwrap());
        assert!(scan_candidates(&db, "device-a").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mismatch_and_malformed_payload_fail_without_mutation() {
        let (_temp, db) = team_db().await;
        assert!(validate_target(&db, "exec-owned", "PROJ", 1, 2)
            .await
            .is_err());
        assert!(decode::<CommentPayload>(r#"{"content":"","unknown":1}"#).is_err());
        assert!(reject_empty("  ").is_err());
    }

    #[tokio::test]
    async fn retryable_post_comment_error_restores_pending() {
        let (_temp, db) = team_db().await;
        insert_intent(&db, "partial-comment", "exec-owned").await;
        assert!(claim(&db, "partial-comment", "device-a").await.unwrap());
        let error = "Failed to record issue comment side-channel notices: transient write failure";
        assert!(is_retryable_dispatch_error(error));
        assert!(relinquish_claim(&db, "partial-comment", "device-a")
            .await
            .unwrap());
        let candidates = scan_candidates(&db, "device-a").await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "partial-comment");
    }

    #[tokio::test]
    async fn ownership_change_relinquishes_for_new_owner() {
        let (_temp, db) = team_db().await;
        insert_intent(&db, "reassigned", "exec-owned").await;
        assert!(claim(&db, "reassigned", "device-a").await.unwrap());
        db.execute(
            "UPDATE executions SET runner_device_id='device-b' WHERE id='exec-owned'",
            (),
        )
        .await
        .unwrap();
        assert!(!verify_owner(&db, "exec-owned", "device-a").await.unwrap());
        assert!(relinquish_claim(&db, "reassigned", "device-a")
            .await
            .unwrap());
        let candidates = scan_candidates(&db, "device-b").await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "reassigned");
        assert!(claim(&db, "reassigned", "device-b").await.unwrap());
    }
}
