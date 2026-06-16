//! Job terminal models for database records (renamed from Node Terminal)

#[derive(Debug)]
pub struct DbJobTerminal {
    pub id: String,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
    pub run_id: Option<String>,
    pub session_id: String,
    pub command: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i32,
    pub exited_at: Option<i32>,
    pub slug: Option<String>,
}

#[derive(Debug)]
pub struct NewJobTerminal<'a> {
    pub id: &'a str,
    pub job_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub session_id: &'a str,
    pub command: &'a str,
    pub title: Option<&'a str>,
    pub description: Option<&'a str>,
    pub status: &'a str,
    pub exit_code: Option<i32>,
    pub created_at: i32,
    pub exited_at: Option<i32>,
    pub slug: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateJobTerminalChangeset<'a> {
    pub status: Option<&'a str>,
    pub exit_code: Option<Option<i32>>,
    pub exited_at: Option<Option<i32>>,
}
