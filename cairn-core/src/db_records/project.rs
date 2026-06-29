//! Project models for database records

#[derive(Debug)]
pub struct DbProject {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    pub context: Option<String>,
    pub docs_enabled: Option<i32>,
    pub default_branch: Option<String>,
    pub next_issue_number: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub ci_commands: Option<String>,
    pub setup_commands: Option<String>,
    pub terminal_commands: Option<String>,
    pub config: Option<String>,
    pub hidden: i32,
    pub is_workspace: i32,
}

#[derive(Debug)]
pub struct NewProject<'a> {
    pub id: &'a str,
    pub workspace_id: &'a str,
    pub name: &'a str,
    pub key: &'a str,
    pub repo_path: &'a str,
    pub context: Option<&'a str>,
    pub docs_enabled: Option<i32>,
    pub default_branch: Option<&'a str>,
    pub next_issue_number: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Default)]
pub struct UpdateProjectChangeset {
    pub updated_at: Option<i32>,
    pub ci_commands: Option<Option<String>>,
    pub setup_commands: Option<Option<String>>,
    pub terminal_commands: Option<Option<String>>,
    pub next_issue_number: Option<i32>,
}
