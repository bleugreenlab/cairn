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
    conn: &crate::storage::TrackedConnection,
    child_issue_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    cairn_attention::wakes::coordinating_job_for_child(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        conn,
        child_issue_id,
    )
    .await
}

/// [`cairn_attention::wakes::gate_owner_for_child_conn`] with the application's
/// recipient host: who a child issue's gates wait on, without establishing a
/// session.
pub(crate) async fn gate_owner_for_child_conn(
    conn: &crate::storage::TrackedConnection,
    child_issue_id: &str,
) -> crate::storage::DbResult<cairn_attention::wakes::GateOwner> {
    cairn_attention::wakes::gate_owner_for_child_conn(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        conn,
        child_issue_id,
    )
    .await
}

/// The jobs an issue question reaches now, sorted: each recipient the
/// resolver finds, as its job (a thread as its current session, or
/// `thread:<id>` when it has none). Tests restate the old watcher list as
/// this set.
#[cfg(test)]
pub(crate) async fn issue_watchers(
    db: &crate::storage::LocalDb,
    issue_uri: &str,
) -> Result<Vec<String>, String> {
    let mut jobs = Vec::new();
    for candidate in issue_recipients(db, issue_uri, "question").await? {
        let job = match &candidate.recipient {
            cairn_attention::wakes::StableRecipient::Job(job) => job.clone(),
            cairn_attention::wakes::StableRecipient::Thread(thread_id) => {
                let thread_id = thread_id.clone();
                db.read(move |conn| {
                    let thread_id = thread_id.clone();
                    Box::pin(async move {
                        cairn_attention::wakes::thread_session_conn(conn, &thread_id).await
                    })
                })
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_else(|| candidate.recipient.key())
            }
        };
        jobs.push(job);
    }
    jobs.sort();
    Ok(jobs)
}

/// Everyone an issue-sourced fact of `fact_kind` reaches, as the resolver
/// derives them for an issue push: the set, the levels and the reasons.
#[cfg(test)]
pub(crate) async fn issue_recipients(
    db: &crate::storage::LocalDb,
    issue_uri: &str,
    fact_kind: &str,
) -> Result<Vec<cairn_attention::wakes::Candidate>, String> {
    use cairn_attention::push::Wake;
    let mut audience = cairn_attention::wakes::FactAudience::new(fact_kind, Wake::Wake, Wake::Wake);
    audience.source = Some(cairn_attention::wakes::SourceRef::issue(issue_uri));
    cairn_attention::wakes::resolve_recipients(
        std::sync::Arc::new(crate::attention_host::CoreRecipients),
        db,
        &audience,
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
