//! Memory models for database records

#[derive(Debug)]
pub struct DbMemory {
    pub id: String,
    pub name: Option<String>,
    pub project_id: Option<String>,
    pub content: String,
    pub status: String,
    pub scope: String,
    pub scope_value: String,
    pub job_id: String,
    pub node_seq: i32,
    pub promoted_commit_sha: Option<String>,
    pub reason: Option<String>,
    pub triage_decision: Option<String>,
    pub deferred_scope: Option<String>,
    pub deferred_scope_value: Option<String>,
    pub provenance_uri: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewMemory<'a> {
    pub id: &'a str,
    pub name: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub content: &'a str,
    pub status: &'a str,
    pub scope: &'a str,
    pub scope_value: &'a str,
    pub job_id: &'a str,
    pub node_seq: i32,
    pub promoted_commit_sha: Option<&'a str>,
    pub reason: Option<&'a str>,
    pub triage_decision: Option<&'a str>,
    pub deferred_scope: Option<&'a str>,
    pub deferred_scope_value: Option<&'a str>,
    pub provenance_uri: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
}
