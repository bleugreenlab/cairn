//! Canonical teardown for the graph rooted at jobs owned by one entity.

use crate::storage::DbResult;
use cairn_db::turso::params;

#[derive(Debug, Clone, Copy)]
pub(crate) enum JobOwner {
    Issue,
    Thread,
}

impl JobOwner {
    fn column(self) -> &'static str {
        match self {
            Self::Issue => "issue_id",
            Self::Thread => "thread_id",
        }
    }

    fn runs(self, jobs: &str) -> String {
        match self {
            // Historical issue runs may predate jobs or otherwise have no job_id.
            Self::Issue => format!("SELECT id FROM runs WHERE issue_id = ?1 OR job_id IN ({jobs})"),
            Self::Thread => format!("SELECT id FROM runs WHERE job_id IN ({jobs})"),
        }
    }
}

/// Delete every job selected by one owner and the non-cascading graph beneath it.
///
/// The owner column comes from a closed enum, never user input. Pointer cycles are
/// broken before leaves are removed in immediate-foreign-key order.
pub(crate) async fn delete_owned_jobs(
    conn: &cairn_db::turso::Connection,
    owner: JobOwner,
    owner_id: &str,
) -> DbResult<()> {
    let column = owner.column();
    let jobs = format!("SELECT id FROM jobs WHERE {column} = ?1");
    let runs = owner.runs(&jobs);

    conn.execute(
        &format!(
            "UPDATE jobs SET current_session_id = NULL, current_turn_id = NULL,
             resume_session_id = NULL WHERE {column} = ?1"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!(
            "UPDATE turns SET predecessor_id = NULL
             WHERE job_id IN ({jobs}) OR run_id IN ({runs})"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!(
            "UPDATE sessions SET replaced_by_id = NULL, parent_session_id = NULL
             WHERE job_id IN ({jobs})"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("UPDATE artifacts SET parent_version_id = NULL WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;

    for table in ["events", "prompts", "permission_requests"] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE run_id IN ({runs})"),
            params![owner_id],
        )
        .await?;
    }
    conn.execute(
        &format!("DELETE FROM turns WHERE job_id IN ({jobs}) OR run_id IN ({runs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM sessions WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM execution_trigger_sources WHERE source_job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM merge_requests WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM runs WHERE id IN ({runs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM memories WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    for table in [
        "thread_compaction_entries",
        "thread_compactions",
        "thread_compaction_marks",
    ] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE job_id IN ({jobs})"),
            params![owner_id],
        )
        .await?;
    }
    conn.execute(
        &format!("DELETE FROM jobs WHERE {column} = ?1"),
        params![owner_id],
    )
    .await?;
    Ok(())
}
