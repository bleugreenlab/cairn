//! Execution models for database records (renamed from RecipeExecution)

#[derive(Debug, Clone)]
pub struct DbExecution {
    pub id: String,
    pub recipe_id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub status: String,
    pub started_at: i32,
    pub completed_at: Option<i32>,
    pub snapshot: Option<String>,
    pub seq: Option<i32>,
    pub initiator_sub: Option<String>,
    pub initiator_org_id: Option<String>,
    pub triggered_by: String,
    /// Stable per-machine device id that OWNS this execution (CAIRN-2629); only
    /// the owning machine claims and runs its jobs. NULL for legacy rows.
    pub runner_device_id: Option<String>,
}

#[derive(Debug)]
pub struct NewExecution<'a> {
    pub id: &'a str,
    pub recipe_id: &'a str,
    pub issue_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub status: &'a str,
    pub started_at: i32,
    pub completed_at: Option<i32>,
    pub snapshot: Option<&'a str>,
    pub seq: Option<i32>,
    pub initiator_sub: Option<&'a str>,
    pub initiator_org_id: Option<&'a str>,
    pub triggered_by: &'a str,
}

#[derive(Debug, Default)]
pub struct UpdateExecutionChangeset {
    pub status: Option<String>,
    pub completed_at: Option<Option<i32>>,
    pub snapshot: Option<String>,
}
