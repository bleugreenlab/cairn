use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::channels::{agent_message, provider_notify_fit};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::channels::{admit_send, check_send_length};
use cairn_common::uri::CairnResource;

fn required_text<'a>(
    index: usize,
    item: &ChangeItem,
    payload: &'a serde_json::Value,
    key: &str,
) -> ResourceMutationResult<&'a str> {
    payload
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("payload.{key} is required and must be a non-empty string"),
            )
        })
}

/// An agent sending free text to a bound conversation.
///
/// Every refusal happens before the intent is ledgered, in a fixed order: the
/// payload shape, then that the sender is a live agent node (attribution comes
/// from the authenticated run, never the payload), then conversation admission
/// against the deliverable-conversation enumeration, then the provider's length
/// limit. A successful write reports the enqueue and never claims delivery:
/// the router sends it, and a failure lands on the `channel_outbound` row and the
/// provider's `last_send_error` like any other outbound.
pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::ChannelsConversations, ChangeMode::Append) = (resource, item.mode) else {
        return Ok(None);
    };
    let payload = append_payload(index, item)?;
    let conversation = required_text(index, item, payload, "conversation")?;
    let content = required_text(index, item, payload, "content")?;
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            build_failure(
                index,
                item,
                format!("Only a live agent node may send a channel message: {error}"),
            )
        })?;
    let admitted = admit_send(orch, conversation)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let provider = admitted.address.provider().id();
    // Budget the message as it will be rendered: the sender's attribution line
    // shares some providers' limits, so the body alone is not the measure.
    let header = agent_message::context_header(&agent.home);
    let (overflow, limit) = provider_notify_fit(provider, &header, content).unwrap_or((0, None));
    check_send_length(provider, overflow, limit)
        .map_err(|error| build_failure(index, item, error))?;
    let address = admitted.address.to_string();
    let warning = admitted
        .row
        .last_error
        .as_deref()
        .map(|error| {
            format!(
                " Warning: {provider}'s most recent outbound delivery failed ({error}); this message may fail the same way."
            )
        })
        .unwrap_or_default();

    if dry_run {
        return Ok(Some(format!(
            "Would send a message to {address} as {}.{warning}",
            agent.home
        )));
    }
    let id = agent_message::enqueue(
        &orch.db.local,
        &admitted.address,
        &agent.home,
        Some(&agent.job_id),
        content,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    Ok(Some(format!(
        "Queued message {id} to {address} (pending); delivery and any failure are recorded on channel_outbound {id} and surface as this provider's last_send_error on cairn://channels/conversations.{warning}"
    )))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch_resource_change;
    use crate::db::DbState;
    use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn orchestrator() -> Orchestrator {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p','w','P','prj','/tmp/p',1,1);
                 INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-general', 'p', 'general', 'active', 1, 1);",
            )
            .await
            .unwrap();
        let session = crate::threads::ensure_thread_session(&local, "t-general")
            .await
            .unwrap();
        local
            .execute_script(&format!(
                "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                 VALUES ('r-live', 'p', '{session}', 'live', 1, 1);"
            ))
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: run_id.map(str::to_string),
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    fn send(conversation: &str) -> ChangeItem {
        ChangeItem {
            target: "cairn://channels/conversations".into(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::json!({
                "conversation": conversation,
                "content": "hello"
            })),
        }
    }

    async fn refusal(orch: &Orchestrator, run_id: Option<&str>, conversation: &str) -> String {
        let error = dispatch_resource_change(orch, &request(run_id), 0, &send(conversation), false)
            .await
            .unwrap_err()
            .error;
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap(),
            Some(0),
            "a refused send ledgers nothing: {error}"
        );
        error
    }

    #[tokio::test]
    async fn a_send_without_a_live_agent_run_is_refused_before_admission() {
        let orch = orchestrator().await;
        let error = refusal(&orch, None, "discord:1/2").await;
        assert!(
            error.contains("Only a live agent node may send a channel message"),
            "{error}"
        );
        let error = refusal(&orch, Some("r-gone"), "discord:1/2").await;
        assert!(
            error.contains("Only a live agent node may send a channel message"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_live_agent_send_to_an_unadmitted_conversation_is_refused() {
        let orch = orchestrator().await;
        let error = refusal(&orch, Some("r-live"), "Discord:1/2").await;
        assert!(error.contains("expected discord:1/2"), "{error}");
        let error = refusal(&orch, Some("r-live"), "discord:1/2").await;
        assert!(error.contains("no active binding"), "{error}");
    }
}
