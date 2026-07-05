//! Permission request models for database records

#[derive(Debug)]
pub struct DbPermissionRequest {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: String,
    pub status: String,
    pub response: Option<String>,
    pub created_at: i32,
    pub responded_at: Option<i32>,
    pub turn_id: Option<String>,
}

#[derive(Debug)]
pub struct NewPermissionRequest<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub tool_use_id: &'a str,
    pub tool_name: &'a str,
    pub tool_input: &'a str,
    pub status: &'a str,
    pub created_at: i32,
    pub turn_id: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdatePermissionRequestChangeset<'a> {
    pub status: Option<&'a str>,
    pub response: Option<Option<&'a str>>,
    pub responded_at: Option<Option<i32>>,
}
