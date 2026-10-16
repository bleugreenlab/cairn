//! Execution read queries, as the rest of the system sees them.
//!
//! Everything is `cairn_workflow::queries`, re-exported, except the functions
//! that hand execution DTOs to a surface. Those are shadowed here so the DTOs
//! leave carrying their gate census: the predicate vocabulary that renders a
//! gate lives in this crate, below which an execution can only say that it has
//! gates, not what they are waiting on.

pub use cairn_workflow::queries::*;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::execution::gates::census::census_for_executions_conn;
use crate::models::{
    Execution, ExecutionDetail, ExecutionFilters, ExecutionGateCensus, ExecutionListResult,
    TriggeredExecution,
};
use crate::storage::LocalDb;

pub fn get_execution_for_issue(
    db: Arc<LocalDb>,
    issue_id: &str,
) -> Result<Option<Execution>, String> {
    let mut execution = cairn_workflow::queries::get_execution_for_issue(db.clone(), issue_id)?;
    if let Some(execution) = execution.as_mut() {
        let mut census = load_census(db, vec![execution.id.clone()])?;
        execution.gate_census = census.remove(&execution.id);
    }
    Ok(execution)
}

pub fn list_executions_for_issue(
    db: Arc<LocalDb>,
    issue_id: &str,
) -> Result<Vec<Execution>, String> {
    let mut executions = cairn_workflow::queries::list_executions_for_issue(db.clone(), issue_id)?;
    let mut census = load_census(db, executions.iter().map(|e| e.id.clone()).collect())?;
    for execution in &mut executions {
        execution.gate_census = census.remove(&execution.id);
    }
    Ok(executions)
}

pub fn list_all_executions(
    db: Arc<LocalDb>,
    filters: &ExecutionFilters,
    limit: i64,
    offset: i64,
) -> Result<ExecutionListResult, String> {
    let mut result =
        cairn_workflow::queries::list_all_executions(db.clone(), filters, limit, offset)?;
    let mut census = load_census(db, result.items.iter().map(|e| e.id.clone()).collect())?;
    for item in &mut result.items {
        item.gate_census = census.remove(&item.id);
    }
    Ok(result)
}

pub fn get_execution_detail(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<ExecutionDetail, String> {
    let mut detail = cairn_workflow::queries::get_execution_detail(db.clone(), execution_id)?;
    // Read through the census query rather than from `detail.gates`: whether a
    // gated execution can still start is a fact about its lineage and issue,
    // which the detail does not carry.
    let mut census = load_census(db, vec![detail.execution.id.clone()])?;
    detail.execution.gate_census = census.remove(&detail.execution.id);
    Ok(detail)
}

pub fn list_triggered_executions_for_execution(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<(String, TriggeredExecution)>, String> {
    let mut triggered =
        cairn_workflow::queries::list_triggered_executions_for_execution(db.clone(), execution_id)?;
    let mut census = load_census(
        db,
        triggered
            .iter()
            .map(|(_, execution)| execution.id.clone())
            .collect(),
    )?;
    for (_, execution) in &mut triggered {
        execution.gate_census = census.remove(&execution.id);
    }
    Ok(triggered)
}

fn load_census(
    db: Arc<LocalDb>,
    execution_ids: Vec<String>,
) -> Result<HashMap<String, ExecutionGateCensus>, String> {
    if execution_ids.is_empty() {
        return Ok(HashMap::new());
    }
    block_on(async move {
        db.read(move |conn| {
            let execution_ids = execution_ids.clone();
            Box::pin(async move { census_for_executions_conn(conn, &execution_ids).await })
        })
        .await
        .map_err(|error| format!("Failed to load execution gate census: {error}"))
    })
}

/// The same off-runtime execution the shadowed queries use: these are called
/// from synchronous command handlers that may already sit inside a runtime.
fn block_on<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to start query database runtime: {e}"))?
            .block_on(future)
    })
    .join()
    .map_err(|_| "Execution gate census query panicked".to_string())?
}
