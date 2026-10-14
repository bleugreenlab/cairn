//! Merge request models for database records

#[derive(Debug)]
pub struct DbMergeRequest {
    pub id: String,
    pub job_id: String,
    pub project_id: String,
    pub issue_id: Option<String>,
    // Authoritative state
    pub title: String,
    pub body: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    pub status: String,
    pub merge_method: String,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub changed_files: Option<i32>,
    pub commit_count: Option<i32>,
    pub merged_commit: Option<String>,
    pub checks_json: Option<String>,
    pub checks_status: Option<String>,
    pub opened_at: i32,
    pub merged_at: Option<i32>,
    pub closed_at: Option<i32>,
    pub updated_at: i32,
    // GitHub sync
    pub github_pr_number: Option<i32>,
    pub github_pr_url: Option<String>,
    pub github_state: Option<String>,
    pub github_review: Option<String>,
    pub github_mergeable: Option<String>,
    pub github_fetched_at: Option<i32>,
}

#[derive(Debug)]
pub struct NewMergeRequest<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub project_id: &'a str,
    pub issue_id: Option<&'a str>,
    pub title: &'a str,
    pub body: Option<&'a str>,
    pub source_branch: &'a str,
    pub target_branch: &'a str,
    pub status: &'a str,
    pub merge_method: &'a str,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub changed_files: Option<i32>,
    pub commit_count: Option<i32>,
    pub checks_json: Option<&'a str>,
    pub checks_status: Option<&'a str>,
    pub opened_at: i32,
    pub updated_at: i32,
    pub github_pr_number: Option<i32>,
    pub github_pr_url: Option<&'a str>,
    pub github_state: Option<&'a str>,
}

/// Changeset for updating merge request status and lifecycle timestamps
#[derive(Debug, Default)]
pub struct UpdateMergeRequestStatus<'a> {
    pub status: Option<&'a str>,
    pub merged_at: Option<Option<i32>>,
    pub closed_at: Option<Option<i32>>,
    pub merged_commit: Option<Option<&'a str>>,
    pub updated_at: Option<i32>,
}

/// Changeset for updating GitHub sync fields (from webhooks or fetch)
#[derive(Debug, Default)]
pub struct UpdateMergeRequestGitHub<'a> {
    pub title: Option<&'a str>,
    pub body: Option<Option<&'a str>>,
    pub additions: Option<Option<i32>>,
    pub deletions: Option<Option<i32>>,
    pub checks_status: Option<Option<&'a str>>,
    pub checks_json: Option<Option<&'a str>>,
    pub github_pr_number: Option<Option<i32>>,
    pub github_pr_url: Option<Option<&'a str>>,
    pub github_state: Option<Option<&'a str>>,
    pub github_review: Option<Option<&'a str>>,
    pub github_mergeable: Option<Option<&'a str>>,
    pub github_fetched_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}

/// Changeset for updating diff stats from git
#[derive(Debug, Default)]
pub struct UpdateMergeRequestDiffStats {
    pub additions: Option<Option<i32>>,
    pub deletions: Option<Option<i32>>,
    pub changed_files: Option<Option<i32>>,
    pub commit_count: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}
