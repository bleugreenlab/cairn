//! The resolution cleanups still owed, as a stored set.
//!
//! Resolving an issue settles what its jobs hold: processes, terminals and
//! their output, batches, check requests, execution environments, and the
//! unanswered tool calls of the turns it cancelled. Settlement can be left
//! pending (an executor away, an exit not yet reported), so something must
//! retry it. The retry reads `resolution_cleanups` and nothing else; it never
//! re-derives which resolved owners still hold resources.
//!
//! The set is kept true at the moments it changes, in the transaction that
//! changes it:
//!
//! - an issue resolving records an `issue` entry ([`record_issue`], from
//!   `issues::crud::resolve`), and reopening deletes it ([`clear_issue_any`]);
//! - settlement that leaves nothing pending deletes the entry for the
//!   generation it settled ([`clear_issue`]);
//! - a resource acquired for an owner whose issue is already resolved records
//!   an entry ([`note_acquisition`]). Every production insert into a table
//!   settlement reads is held to this by a source scan in this module's tests;
//! - a turn cancelled by resolution records a `calls` entry through the
//!   `resolution_cleanups_cancelled_turn` trigger (the migration's), and the
//!   retry deletes it once the turn has no open call ([`clear_calls`]).

use crate::storage::{DbResult, LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::storage::TrackedConnection;
use cairn_db::turso::params;

/// One owed cleanup, as [`pending`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cleanup {
    /// A resolved issue that may still hold resources, as of the resolution
    /// generation it was resolved under.
    Issue {
        issue_id: String,
        resolution: Resolution,
        generation: i64,
        /// This enrolment's token: [`clear_issue`] with it deletes this
        /// enrolment and no later one.
        token: String,
    },
    /// A resolution-cancelled turn that may still have unanswered calls.
    Calls {
        turn_id: String,
        run_id: String,
        resolution: Resolution,
    },
}

fn resolution_text(resolution: Resolution) -> &'static str {
    match resolution {
        Resolution::Merged => "merged",
        Resolution::Closed => "closed",
    }
}

pub(crate) fn parse_resolution(status: &str) -> Option<Resolution> {
    match status {
        "merged" => Some(Resolution::Merged),
        "closed" => Some(Resolution::Closed),
        _ => None,
    }
}

/// Record that `issue_id`, resolved as `resolution`, may still hold resources.
/// The generation is the issue's own, read in the same statement, and the row
/// is written only while the issue reads resolved. Every write mints a new
/// token, so a settlement already under way cannot delete this enrolment.
pub(crate) async fn record_issue(
    conn: &TrackedConnection,
    issue_id: &str,
    resolution: Resolution,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO resolution_cleanups(kind,subject_id,run_id,resolution,resolution_generation,token,recorded_at)
         SELECT 'issue', id, NULL, ?2, resolution_generation, lower(hex(randomblob(16))), unixepoch()
           FROM issues WHERE id=?1 AND status IN ('merged','closed')
         ON CONFLICT(kind,subject_id) DO UPDATE SET
             resolution=excluded.resolution,
             resolution_generation=excluded.resolution_generation,
             token=excluded.token,
             recorded_at=excluded.recorded_at",
        params![issue_id, resolution_text(resolution)],
    )
    .await?;
    Ok(())
}

/// The token of the issue's current enrolment, if it has one. A settlement
/// reads it before it settles anything and clears with it afterwards.
pub(crate) async fn issue_enrolment(db: &LocalDb, issue_id: &str) -> DbResult<Option<String>> {
    db.query_opt(
        "SELECT token FROM resolution_cleanups WHERE kind='issue' AND subject_id=?1",
        params![issue_id],
        |row| row.opt_text(0),
    )
    .await
    .map(Option::flatten)
}

/// Delete the issue's entry if it is still the enrolment `token` names. An
/// acquisition, a re-resolution or a reopen-then-resolve that wrote the entry
/// since keeps its newer enrolment.
pub(crate) async fn clear_issue(db: &LocalDb, issue_id: &str, token: &str) -> DbResult<()> {
    db.execute(
        "DELETE FROM resolution_cleanups
         WHERE kind='issue' AND subject_id=?1 AND token=?2",
        params![issue_id, token],
    )
    .await?;
    Ok(())
}

/// Delete the issue's entry whatever its generation: the issue reopened, and
/// a retry must never touch reopened work. `calls` entries are kept.
pub(crate) async fn clear_issue_any(conn: &TrackedConnection, issue_id: &str) -> DbResult<()> {
    conn.execute(
        "DELETE FROM resolution_cleanups WHERE kind='issue' AND subject_id=?1",
        params![issue_id],
    )
    .await?;
    Ok(())
}

/// Delete a turn's `calls` entry: none of its calls is open any more.
pub(crate) async fn clear_calls(db: &LocalDb, turn_id: &str) -> DbResult<()> {
    db.execute(
        "DELETE FROM resolution_cleanups WHERE kind='calls' AND subject_id=?1",
        params![turn_id],
    )
    .await?;
    Ok(())
}

/// Every owed cleanup in `db`, in one statement.
pub(crate) async fn pending(db: &LocalDb) -> DbResult<Vec<Cleanup>> {
    let rows = db
        .query_all(
            "SELECT kind, subject_id, run_id, resolution, resolution_generation, token
             FROM resolution_cleanups",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.text(3)?,
                    row.opt_i64(4)?,
                    row.opt_text(5)?,
                ))
            },
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(kind, subject, run, resolution, generation, token)| {
            let resolution = parse_resolution(&resolution)?;
            match kind.as_str() {
                "issue" => Some(Cleanup::Issue {
                    issue_id: subject,
                    resolution,
                    generation: generation.unwrap_or_default(),
                    token: token.unwrap_or_default(),
                }),
                "calls" => Some(Cleanup::Calls {
                    turn_id: subject,
                    run_id: run?,
                    resolution,
                }),
                _ => None,
            }
        })
        .collect())
}

/// A resource was just acquired for `job_id`. If an issue the job's resources
/// settle under (its own, an ancestor's, or the source of the branch it was cut
/// from) is already resolved, record that issue as owed a settlement, so the
/// retry finds the resource. Never refuses the acquisition.
///
/// `db` is the job's own database. Call after the acquisition commits: a
/// resolution that commits in between records its own entry, and one that
/// commits before is seen here.
pub(crate) async fn note_acquisition(db: &LocalDb, job_id: &str) -> Result<usize, String> {
    let found = crate::execution::teardown::resolved_resource_issues(db, job_id).await?;
    if found.is_empty() {
        return Ok(0);
    }
    let recorded = found.len();
    db.write(|conn| {
        let found = found.clone();
        Box::pin(async move {
            for (issue_id, status) in &found {
                if let Some(resolution) = parse_resolution(status) {
                    record_issue(conn, issue_id, resolution).await?;
                }
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok(recorded)
}

/// [`note_acquisition`] inside the transaction that acquires the resource, for
/// a site that holds that transaction but not its database.
pub(crate) async fn note_acquisition_conn(conn: &TrackedConnection, job_id: &str) -> DbResult<()> {
    let mut rows = conn
        .query(
            crate::execution::teardown::RESOLVED_RESOURCE_ISSUES_SQL,
            params![job_id],
        )
        .await?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().await? {
        found.push((row.text(0)?, row.text(1)?));
    }
    drop(rows);
    for (issue_id, status) in found {
        if let Some(resolution) = parse_resolution(&status) {
            record_issue(conn, &issue_id, resolution).await?;
        }
    }
    Ok(())
}

/// [`note_acquisition`] for a site that holds the orchestrator's databases but
/// not the job's own: the job's database is routed to first. A failure is
/// logged and never fails the acquisition it follows.
pub(crate) async fn note_job_acquisition(dbs: &crate::db::DbState, job_id: &str) {
    let result = match crate::execution::routing::owning_db_for_job(dbs, job_id).await {
        Ok(db) => note_acquisition(&db, job_id).await,
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = result {
        tracing::warn!(target: "execution::teardown", %job_id, %error,
            "could not check a new resource's owner against resolved issues");
    }
}

/// [`note_acquisition`] on `db`, logging instead of failing.
pub(crate) async fn note_acquisition_logged(db: &LocalDb, job_id: &str) {
    if let Err(error) = note_acquisition(db, job_id).await {
        tracing::warn!(target: "execution::teardown", %job_id, %error,
            "could not check a new resource's owner against resolved issues");
    }
}

/// Record an entry whenever an executor is first seen holding a job's
/// residency: a placement for a resolved owner, or an executor re-advertising
/// an environment after this runner restarted. Installed once per runner.
pub(crate) fn observe_job_residencies(orch: &crate::orchestrator::Orchestrator) {
    let dbs = orch.db.clone();
    orch.fleet.subscribe_job_residency_observed(move |job_id| {
        let dbs = dbs.clone();
        cairn_common::task::spawn(async move {
            note_job_acquisition(&dbs, &job_id).await;
        });
    });
}

#[cfg(test)]
#[path = "resolution_cleanups_tests.rs"]
mod tests;
