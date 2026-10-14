//! Issue models for database records

#[derive(Debug)]
pub struct DbIssue {
    pub id: String,
    pub project_id: String,
    pub number: i32,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub progress: String,
    pub attention: String,
    pub priority: Option<i32>,
    pub completed_at: Option<i32>,
    pub dismissed_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub model: Option<String>,
    pub merged_at: Option<i32>,
    pub closed_at: Option<i32>,
}

#[derive(Debug)]
pub struct NewIssue<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub number: i32,
    pub title: &'a str,
    pub description: Option<&'a str>,
    pub status: &'a str,
    pub progress: &'a str,
    pub attention: &'a str,
    pub priority: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub model: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateIssueChangeset<'a> {
    pub title: Option<&'a str>,
    pub description: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub progress: Option<&'a str>,
    pub attention: Option<&'a str>,
    pub priority: Option<i32>,
    pub completed_at: Option<Option<i32>>,
    pub dismissed_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
    pub model: Option<Option<&'a str>>,
    pub merged_at: Option<Option<i32>>,
    pub closed_at: Option<Option<i32>>,
}
