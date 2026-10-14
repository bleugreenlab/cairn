//! Label resource mutations.

use crate::models::{CreateLabel, UpdateLabel};
use crate::orchestrator::Orchestrator;

fn emit_db_change(orch: &Orchestrator, table: &str, action: &str) {
    if let Err(error) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": table, "action": action}),
    ) {
        log::error!("Failed to emit db-change event: {}", error);
    }
}

pub(super) async fn apply_label_create(
    orch: &Orchestrator,
    payload: &serde_json::Value,
) -> Result<String, String> {
    let name = super::payload_trimmed_non_empty_str(payload, "name", &[])
        .ok_or("payload.name is required and must be a non-empty string")?
        .to_string();
    let color = payload
        .get("color")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let label =
        crate::labels::crud::create_label(&orch.db.local, CreateLabel { name, color }).await?;
    emit_db_change(orch, "labels", "insert");
    Ok(format!("Created label '{}': {}", label.id, label.name))
}

pub(super) async fn apply_label_patch(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    label_id: &str,
) -> Result<String, String> {
    let name = super::payload_str(payload, "name", &[]).map(ToOwned::to_owned);
    let color = super::payload_str(payload, "color", &[]).map(ToOwned::to_owned);
    crate::labels::crud::update_label(&orch.db.local, label_id, UpdateLabel { name, color })
        .await?;
    emit_db_change(orch, "labels", "update");
    emit_db_change(orch, "issue_labels", "update");
    Ok(format!("Updated label '{label_id}'"))
}

pub(super) async fn apply_label_delete(
    orch: &Orchestrator,
    label_id: &str,
) -> Result<String, String> {
    crate::labels::crud::delete_label(&orch.db.local, label_id).await?;
    emit_db_change(orch, "labels", "delete");
    emit_db_change(orch, "issue_labels", "delete");
    emit_db_change(orch, "issues", "update");
    Ok(format!("Deleted label '{label_id}'"))
}
