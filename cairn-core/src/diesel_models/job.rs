//! Job models for Diesel ORM (replaces Timeline Node)

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = jobs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbJob {
    pub id: String,
    pub execution_id: Option<String>,
    pub manager_id: Option<String>,
    pub recipe_node_id: Option<String>,
    pub parent_job_id: Option<String>,
    pub worktree_path: Option<String>,
    pub branch: Option<String>,
    pub base_commit: Option<String>,
    pub current_session_id: Option<String>,
    pub resume_session_id: Option<String>,
    pub status: String,
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<String>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<String>,
    pub node_name: Option<String>,
    pub base_branch: Option<String>,
    pub current_turn_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = jobs)]
pub struct NewJob<'a> {
    pub id: &'a str,
    pub execution_id: Option<&'a str>,
    pub manager_id: Option<&'a str>,
    pub recipe_node_id: Option<&'a str>,
    pub parent_job_id: Option<&'a str>,
    pub worktree_path: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub base_commit: Option<&'a str>,
    pub current_session_id: Option<&'a str>,
    pub resume_session_id: Option<&'a str>,
    pub status: &'a str,
    pub agent_config_id: Option<&'a str>,
    pub issue_id: Option<&'a str>,
    pub project_id: &'a str,
    pub task_description: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<&'a str>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub base_branch: Option<&'a str>,
    pub current_turn_id: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = jobs)]
pub struct UpdateJobChangeset<'a> {
    pub worktree_path: Option<Option<&'a str>>,
    pub branch: Option<Option<&'a str>>,
    pub base_commit: Option<Option<&'a str>>,
    pub current_session_id: Option<Option<&'a str>>,
    pub resume_session_id: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub updated_at: Option<i32>,
    pub completed_at: Option<Option<i32>>,
    pub started_at: Option<Option<i32>>,
    pub model: Option<Option<&'a str>>,
}
