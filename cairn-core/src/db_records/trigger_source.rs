//! database models for the execution_trigger_sources junction table.

/// A record linking a source job to the execution it triggered.
#[derive(Debug, Clone)]
pub struct DbTriggerSource {
    pub id: String,
    pub source_job_id: String,
    pub triggered_execution_id: String,
    pub created_at: i32,
}

/// record for execution_trigger_sources.
#[derive(Debug)]
pub struct NewTriggerSource<'a> {
    pub id: &'a str,
    pub source_job_id: &'a str,
    pub triggered_execution_id: &'a str,
    pub created_at: i32,
}
