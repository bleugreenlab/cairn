//! Database record conversion into domain values.
use cairn_domain::*;

/// Convert DbJob to Job
impl TryFrom<crate::db_records::DbJob> for Job {
    type Error = String;

    fn try_from(db: crate::db_records::DbJob) -> Result<Self, Self::Error> {
        let status: JobStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid job status: {}", e))?;

        let model = db.model.as_ref().map(crate::models::Model::new);

        Ok(Job {
            id: db.id,
            execution_id: db.execution_id,
            recipe_node_id: db.recipe_node_id,
            parent_job_id: db.parent_job_id,
            branch: db.branch,
            pack_anchor: db.pack_anchor,
            current_session_id: db.current_session_id,
            status,
            agent_config_id: db.agent_config_id,
            issue_id: db.issue_id,
            thread_id: db.thread_id,
            project_id: db.project_id,
            task_description: db.task_description,
            model,
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
            completed_at: db.completed_at.map(|t| t as i64),
            started_at: db.started_at.map(|t| t as i64),
            available_tabs: Job::default_available_tabs(),
            initial_tab: Job::default_initial_tab(),
            parent_tool_use_id: db.parent_tool_use_id,
            task_index: db.task_index,
            node_name: db.node_name,
            exec_seq: None, // Populated by query functions via execution join
            base_branch: db.base_branch,
            uri_segment: db.uri_segment,
        })
    }
}
