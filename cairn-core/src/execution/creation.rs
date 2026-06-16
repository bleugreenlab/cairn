//! Recipe execution startup logic.

use std::sync::Arc;

use crate::models::Job;
use crate::storage::LocalDb;

/// Create jobs for an execution using its stored snapshot.
pub fn create_jobs_for_execution(db: Arc<LocalDb>, execution_id: &str) -> Result<Vec<Job>, String> {
    super::advancement::create_jobs_for_execution(db, execution_id)
}
