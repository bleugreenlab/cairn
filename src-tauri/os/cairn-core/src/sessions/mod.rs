//! Session management — durable backend conversation identity.
//!
//! Sessions track the backend conversation (e.g. Claude CLI's `--session-id`).
//! Their status describes whether the conversation is resumable, not whether
//! a process is currently running.

pub mod queries;

/// Log, without changing anything, every open session whose provider was
/// launched under its id and never confirmed it.
///
/// The job's next continuation rotates such a session onto a fresh id; nothing
/// here rotates eagerly. The warning exists so an operator reading a boot log
/// can see which jobs are waiting on that rotation.
pub(crate) async fn log_unconfirmed_launch_sessions_on_startup(
    orch: &crate::orchestrator::Orchestrator,
) {
    for db in orch.db.all_dbs().await {
        let sessions = match queries::list_unconfirmed_spawned_launch_sessions(&db).await {
            Ok(sessions) => sessions,
            Err(error) => {
                log::warn!("Could not list sessions with unconfirmed launches: {error}");
                continue;
            }
        };
        for session in sessions {
            if !crate::backends::backend_for_name(Some(&session.backend))
                .fresh_start_claims_session_id()
            {
                continue;
            }
            log::warn!(
                "Session {} (job {}, lineage generation {}) launched its provider {} time(s) without confirming it; its next continuation rotates it onto a fresh session",
                session.session_id,
                session.job_id.as_deref().unwrap_or("none"),
                session.sequence,
                session.crashed_spawned_runs,
            );
        }
    }
}
