//! Run and event models for database records

// ============================================================================
// Run models
// ============================================================================

#[derive(Debug)]
pub struct DbRun {
    pub id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub status: Option<String>,
    pub session_id: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i32>,
    pub exited_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub chat_id: Option<String>,
    pub backend: Option<String>,
    pub exit_reason: Option<String>,
    pub start_mode: Option<String>,
}

#[derive(Debug)]
pub struct NewRun<'a> {
    pub id: &'a str,
    pub issue_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub started_at: Option<i32>,
    pub exited_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub chat_id: Option<&'a str>,
    pub backend: Option<&'a str>,
    pub exit_reason: Option<&'a str>,
    pub start_mode: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateRunChangeset<'a> {
    pub status: Option<&'a str>,
    pub session_id: Option<Option<&'a str>>,
    pub error_message: Option<Option<&'a str>>,
    pub started_at: Option<Option<i32>>,
    pub exited_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
    pub backend: Option<Option<&'a str>>,
    pub exit_reason: Option<Option<&'a str>>,
}

// ============================================================================
// Event models
// ============================================================================

#[derive(Debug)]
pub struct DbEvent {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: i32,
    pub timestamp: i32,
    pub event_type: String,
    pub data: String,
    pub parent_tool_use_id: Option<String>,
    pub created_at: i32,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub turn_id: Option<String>,
}

#[derive(Debug)]
pub struct NewEvent<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub session_id: Option<&'a str>,
    pub sequence: i32,
    pub timestamp: i32,
    pub event_type: &'a str,
    pub data: &'a str,
    pub parent_tool_use_id: Option<&'a str>,
    pub created_at: i32,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub turn_id: Option<&'a str>,
}
