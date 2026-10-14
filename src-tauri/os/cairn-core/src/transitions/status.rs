use crate::issues::crud;
use crate::services::{EventEmitter, RealClock};
use crate::storage::LocalDb;
pub use cairn_workflow::transitions::status::*;

pub async fn unresolve_issue(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    issue_id: &str,
) -> Result<(), String> {
    crud::unresolve(db, &RealClock, issue_id)
        .await
        .map_err(|e| e.to_string())?;

    let issue = crud::get(db, issue_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Issue not found after resolution: {issue_id}"))?;
    let _ = emitter.emit(
        "db-change",
        crate::notify::issue_db_change(&issue, "update"),
    );
    Ok(())
}
