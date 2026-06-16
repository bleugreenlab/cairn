//! Full issue deletion shared by the host `delete_issue` command and the
//! `cairn://p/PROJECT/NUMBER` resource `delete` mutation.

use crate::issues::{crud, relations};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::sync::SyncMessage;
use turso::params;

/// Collect the run ids belonging to an issue so their live sessions can be
/// killed before the issue and its worktrees are torn down.
async fn run_ids_for_issue(db: &LocalDb, issue_id: &str) -> Result<Vec<String>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE issue_id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            let mut run_ids = Vec::new();
            while let Some(row) = rows.next().await? {
                run_ids.push(row.text(0)?);
            }
            Ok(run_ids)
        })
    })
    .await
    .map_err(|e| format!("Failed to load issue run ids: {e}"))
}

/// Delete an issue and every piece of dependent state: tear down its worktrees,
/// kill any live agent sessions, remove the database rows, then propagate the
/// deletion to the embedding corpus, remote sync, and the UI event stream.
///
/// This is the single source of truth for issue deletion. The `delete_issue`
/// Tauri command and the resource `delete` mutation both call it so they perform
/// identical side effects.
pub async fn delete_issue(orch: &Orchestrator, issue_id: &str) -> Result<(), String> {
    // Resolve the canonical URI before the row is gone; used to evict the
    // issue's embedding from the corpus.
    let issue_uri = relations::issue_uri_for_id_db(&orch.db.local, issue_id)
        .await
        .ok();
    let run_ids = run_ids_for_issue(&orch.db.local, issue_id).await?;

    crate::execution::teardown::teardown_worktrees(
        orch,
        crate::execution::teardown::TeardownScope::Issue(issue_id.to_string()),
    )
    .await?;

    for run_id in &run_ids {
        let _ = crate::orchestrator::lifecycle::kill_session(orch, run_id);
    }

    crud::delete_db(&orch.db.local, issue_id)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(uri) = issue_uri.as_deref() {
        orch.enqueue_resource_delete(uri);
    }

    orch.sync(SyncMessage::Delete {
        table: "issues".to_string(),
        id: issue_id.to_string(),
    });
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "delete"}),
    );

    log::info!("Deleted issue {}", issue_id);
    Ok(())
}
