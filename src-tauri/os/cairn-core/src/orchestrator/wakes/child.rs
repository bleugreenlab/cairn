//! Application session establishment supplied to attention recipient lookup.
pub async fn coordinating_job_for_child_issue(
    db: &crate::storage::LocalDb,
    child_issue_uri: &str,
) -> Result<Option<String>, String> {
    cairn_attention::wakes::coordinating_job_for_child_issue(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        child_issue_uri,
    )
    .await
}

pub(crate) async fn coordinating_job_for_child(
    conn: &cairn_db::turso::Connection,
    child_issue_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    cairn_attention::wakes::coordinating_job_for_child(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        conn,
        child_issue_id,
    )
    .await
}

pub async fn watcher_jobs_for_issue(
    db: &crate::storage::LocalDb,
    issue_uri: &str,
) -> Result<Vec<String>, String> {
    cairn_attention::wakes::watcher_jobs_for_issue(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        issue_uri,
    )
    .await
}

pub async fn coordinated_child_issue_uris_for_job(
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Result<Vec<String>, String> {
    cairn_attention::wakes::coordinated_child_issue_uris_for_job(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        job_id,
    )
    .await
}
