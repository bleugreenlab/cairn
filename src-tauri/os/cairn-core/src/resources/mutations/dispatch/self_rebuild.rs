//! Self-rebuild outcome dispatch: a run records a pending outcome, and the
//! detached installer delivers it once. Storage, rendering and the run-less
//! allowlist predicate live in `resources::self_rebuild`.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::self_rebuild::{self as outcomes, Claim, Outcome, ReportTo};
use cairn_common::uri::{build_self_rebuild_outcome_uri, CairnResource};

const RUNLESS_RECORD_REFUSAL: &str = "Only a run may record a pending self-rebuild outcome: this request speaks for no run. Start the self-rebuild from an agent run, or use human mode (no --thread-uri), which reports locally.";

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::SelfRebuild, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let report_to = payload_non_empty_str(payload, "reportTo", &[])
                .ok_or_else(|| build_failure(index, item, "payload.reportTo is required"))?;
            let run_id = request
                .run_id
                .as_deref()
                .filter(|run_id| !run_id.is_empty())
                .ok_or_else(|| build_failure(index, item, RUNLESS_RECORD_REFUSAL))?;
            let target = ReportTo::parse(report_to).map_err(|e| build_failure(index, item, e))?;
            // A thread that cannot be reported to fails here, before the
            // rebuild touches the installed app, not after.
            if let ReportTo::Thread { project, name } = &target {
                outcomes::thread_id(orch, project, name)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
            }
            let report_to = target.uri();
            if dry_run {
                format!("Would record a pending self-rebuild outcome reporting to {report_to}")
            } else {
                let id = outcomes::record(&orch.db.local, &report_to, run_id, outcomes::now_ms())
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                let uri = build_self_rebuild_outcome_uri(&id);
                *applied_data = Some(serde_json::json!({ "outcome": uri }));
                format!(
                    "Recorded pending self-rebuild outcome {uri}, reporting to {report_to}. Deliver it once with an append of {{content}} to {uri}."
                )
            }
        }
        (CairnResource::SelfRebuildOutcome { id }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                match outcomes::load(&orch.db.local, id)
                    .await
                    .map_err(|e| build_failure(index, item, e))?
                {
                    None => return Err(build_failure(index, item, outcomes::missing_refusal(id))),
                    Some(Outcome {
                        delivered_at: Some(at),
                        ..
                    }) => {
                        return Err(build_failure(
                            index,
                            item,
                            outcomes::delivered_refusal(id, at),
                        ))
                    }
                    Some(outcome) => format!(
                        "Would deliver {} chars to {}",
                        content.len(),
                        outcome.report_to
                    ),
                }
            } else {
                deliver(orch, request, index, item, id, content).await?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

async fn deliver(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    id: &str,
    content: &str,
) -> ResourceMutationResult<String> {
    let db = &orch.db.local;
    let claimed_at = outcomes::now_ms();
    let outcome = match outcomes::claim(db, id, claimed_at)
        .await
        .map_err(|e| build_failure(index, item, e))?
    {
        Claim::Claimed(outcome) => outcome,
        Claim::Missing => return Err(build_failure(index, item, outcomes::missing_refusal(id))),
        Claim::AlreadyDelivered { at } => {
            return Err(build_failure(
                index,
                item,
                outcomes::delivered_refusal(id, at),
            ))
        }
    };
    let appended = match ReportTo::parse(&outcome.report_to) {
        Ok(target) => outcomes::append_report(orch, request, &target, content).await,
        Err(error) => Err(error),
    };
    match appended {
        Ok(_) => Ok(format!(
            "Delivered self-rebuild outcome {} to {}",
            build_self_rebuild_outcome_uri(id),
            outcome.report_to
        )),
        Err(error) => {
            // The message did not land, so the outcome is still undelivered:
            // give the claim back for the installer's next attempt.
            if let Err(release) = outcomes::release(db, id, claimed_at).await {
                log::warn!("failed to release self-rebuild outcome {id} after a failed delivery: {release}");
            }
            Err(build_failure(
                index,
                item,
                format!(
                    "Self-rebuild outcome {} could not be delivered to {}: {error}. It is still pending, so delivery may be retried.",
                    build_self_rebuild_outcome_uri(id),
                    outcome.report_to
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::dispatch_resource_change;
    use super::*;
    use crate::db::DbState;
    use crate::models::CreateThread;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use std::sync::Arc;

    async fn orch_with_thread() -> Orchestrator {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p','w','P','prj','/tmp/p',1,1);",
            )
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        crate::threads::crud::create(
            &orch.db.local,
            CreateThread {
                project_id: "p".into(),
                name: Some("roadmap".into()),
                jurisdiction: None,
                definition: None,
                migrated_from_number: None,
                model: None,
            },
        )
        .await
        .unwrap();
        orch
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: run_id.map(str::to_string),
            tool: "write".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin: None,
        }
    }

    fn append(target: &str, payload: serde_json::Value) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Append,
            payload: Some(payload),
        }
    }

    async fn count(orch: &Orchestrator, sql: &str) -> i64 {
        orch.db
            .local
            .query_one(sql, (), |row| row.i64(0))
            .await
            .unwrap()
    }

    async fn record(orch: &Orchestrator) -> String {
        let change = dispatch_resource_change(
            orch,
            &request(Some("run-1")),
            0,
            &append(
                "cairn://self-rebuild",
                serde_json::json!({"reportTo": "cairn://p/prj/roadmap"}),
            ),
            false,
        )
        .await
        .unwrap();
        let uri = change.data.unwrap()["outcome"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(change.summary.contains(&uri), "{}", change.summary);
        uri
    }

    #[tokio::test]
    async fn an_outcome_is_delivered_once_and_then_refused_with_nothing_written() {
        let orch = orch_with_thread().await;
        let uri = record(&orch).await;
        let messages = "SELECT COUNT(*) FROM messages";
        let before = count(&orch, messages).await;

        let delivery = append(
            &uri,
            serde_json::json!({"content": "Self-rebuild succeeded."}),
        );
        // The installer runs outside any run.
        dispatch_resource_change(&orch, &request(None), 0, &delivery, false)
            .await
            .unwrap();
        assert_eq!(count(&orch, messages).await, before + 1);

        let again = dispatch_resource_change(&orch, &request(None), 0, &delivery, false)
            .await
            .unwrap_err();
        assert!(again.error.contains("already delivered"), "{}", again.error);
        assert_eq!(count(&orch, messages).await, before + 1);
    }

    #[tokio::test]
    async fn a_runless_record_is_refused_by_name() {
        let orch = orch_with_thread().await;
        let refused = dispatch_resource_change(
            &orch,
            &request(None),
            0,
            &append(
                "cairn://self-rebuild",
                serde_json::json!({"reportTo": "cairn://p/prj/roadmap"}),
            ),
            false,
        )
        .await
        .unwrap_err();
        assert!(refused.error.contains("Only a run"), "{}", refused.error);
        assert_eq!(
            count(&orch, "SELECT COUNT(*) FROM self_rebuild_outcomes").await,
            0
        );
    }

    #[tokio::test]
    async fn a_missing_outcome_is_refused_with_nothing_written() {
        let orch = orch_with_thread().await;
        let before = count(&orch, "SELECT COUNT(*) FROM messages").await;
        let refused = dispatch_resource_change(
            &orch,
            &request(None),
            0,
            &append(
                "cairn://self-rebuild/5d2b7c1e-0000-4000-8000-000000000001",
                serde_json::json!({"content": "done"}),
            ),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            refused.error.contains("No self-rebuild outcome"),
            "{}",
            refused.error
        );
        assert_eq!(count(&orch, "SELECT COUNT(*) FROM messages").await, before);
    }

    #[tokio::test]
    async fn a_record_naming_no_thread_or_issue_is_refused() {
        let orch = orch_with_thread().await;
        for report_to in ["cairn://p/prj/nowhere", "cairn://p/prj/roadmap/messages"] {
            let refused = dispatch_resource_change(
                &orch,
                &request(Some("run-1")),
                0,
                &append(
                    "cairn://self-rebuild",
                    serde_json::json!({ "reportTo": report_to }),
                ),
                false,
            )
            .await;
            assert!(refused.is_err(), "{report_to}");
        }
        assert_eq!(
            count(&orch, "SELECT COUNT(*) FROM self_rebuild_outcomes").await,
            0
        );
    }

    #[tokio::test]
    async fn the_outcome_reads_back_its_delivery_state() {
        let orch = orch_with_thread().await;
        let uri = record(&orch).await;
        let id = uri.rsplit('/').next().unwrap();
        let pending = outcomes::render_outcome(&orch.db.local, id).await;
        assert!(
            pending.contains("reportTo: cairn://p/prj/roadmap"),
            "{pending}"
        );
        assert!(pending.contains("delivered: no"), "{pending}");
        dispatch_resource_change(
            &orch,
            &request(None),
            0,
            &append(&uri, serde_json::json!({"content": "done"})),
            false,
        )
        .await
        .unwrap();
        let delivered = outcomes::render_outcome(&orch.db.local, id).await;
        assert!(delivered.contains("delivered: yes"), "{delivered}");
        assert!(
            !delivered.contains("done"),
            "the read carries no content: {delivered}"
        );
    }
}
