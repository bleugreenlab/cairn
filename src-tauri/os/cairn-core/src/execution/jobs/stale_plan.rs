//! Freshness evidence belongs beside the plan, not in its user-authored schema.
//! All comparisons use immutable store coordinates captured for this launch.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use cairn_vcs::history::{self, PlannedPathChanges};
use cairn_workflow::advancement::{format_resolved_inputs, ResolvedInput};

use super::{DbJob, LocalDb, Orchestrator};

const QUERY_LIMIT: Duration = Duration::from_secs(5);

/// The launch's input sections, each plan followed by its freshness notice,
/// and whether every plan comparison completed. An incomplete comparison must
/// not advance the checkpoint, so the next re-entry retries it.
pub(super) async fn format_launch_inputs(
    db: &LocalDb,
    inputs: &[ResolvedInput],
    coordinate: Option<(&Path, &Result<String, String>)>,
) -> (String, bool) {
    let mut sections = Vec::new();
    let mut complete = true;
    for input in inputs {
        sections.push(format_resolved_inputs(std::slice::from_ref(input)));
        if input.artifact_type == "plan" {
            if let Some((store, start)) = coordinate {
                let (note, checked) = plan_notice(db, input, store, start).await;
                complete &= checked;
                sections.push(note);
            }
        }
    }
    (sections.join("\n\n"), complete)
}

async fn plan_notice(
    db: &LocalDb,
    input: &ResolvedInput,
    store: &Path,
    start: &Result<String, String>,
) -> (String, bool) {
    let checked = match start {
        Ok(start) => checked_notice(db, input, store, start).await,
        Err(error) => Err(error.clone()),
    };
    match checked {
        Ok(note) => (note, true),
        Err(error) => (unchecked(&error), false),
    }
}

fn unchecked(error: &str) -> String {
    format!(
        "Plan freshness could not be checked: {error}. Re-read the plan's named files and re-verify its steps before building."
    )
}

async fn checked_notice(
    db: &LocalDb,
    input: &ResolvedInput,
    store: &Path,
    start: &str,
) -> Result<String, String> {
    let Some(source) = &input.source else {
        return Ok("Plan freshness: this input has no stored artifact version; its written commit is unknown. Re-read its named files before building.".to_string());
    };
    let producer = source.producer_job_id.clone();
    let output = source.output_name.clone();
    let version = source.version;
    let (written, fallback) = db
        .read(move |conn| {
            let (producer, output) = (producer.clone(), output.clone());
            Box::pin(async move {
                use crate::storage::RowExt;
                let mut rows = conn
                    .query(
                        "SELECT a.written_at_commit, j.base_commit FROM artifacts a
                 JOIN jobs j ON j.id=a.job_id
                 WHERE a.job_id=?1 AND a.output_name=?2 AND a.version=?3",
                        cairn_db::turso::params![producer, output, version],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::internal("Plan artifact version not found")
                })?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let markdown = input
        .data
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let Some(written) = written else {
        let range = match fallback {
            Some(base) => {
                let report = history::planned_path_changes(store, &base, start, "", QUERY_LIMIT)
                    .await
                    .map_err(|error| error.to_string())?;
                format!(
                    "{} commits since the planner's recorded branch base `{}` (which may have advanced since writing); the commit this plan was written against was not recorded",
                    report.range_commit_count, short(&base)
                )
            }
            None => "range commit count unavailable because the producer has no recorded base"
                .to_string(),
        };
        return Ok(format!(
            "Plan v{version} has no recorded written-commit provenance; its written commit is unknown ({range}). Re-read its named files and re-verify its steps before building."
        ));
    };
    let report = history::planned_path_changes(store, &written, start, markdown, QUERY_LIMIT)
        .await
        .map_err(|error| error.to_string())?;
    Ok(render_notice(
        version,
        &report,
        chrono::Utc::now().timestamp(),
    ))
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

fn render_notice(version: i64, report: &PlannedPathChanges, now: i64) -> String {
    let commit = short(&report.written_commit.commit_id);
    let named = report.named_files.len();
    if named == 0 {
        return format!(
            "Plan v{version} was written against `{commit}`; it names no resolvable files in that tree ({} commits in the comparison range). Re-read the relevant files before building.",
            report.range_commit_count,
        );
    }
    if report.changed_files.is_empty() {
        return format!("Plan v{version} was written against `{commit}`; none of the {named} named files changed since.");
    }
    let timestamp = report.written_commit.committer_time;
    let date = chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "date unavailable".to_string());
    let days = now.saturating_sub(timestamp).max(0) / 86_400;
    let mut lines = vec![format!(
        "**The base moved since this plan was written.** Plan v{version} was written against `{commit}` ({date}, {days} days ago). {} of the {named} files it names changed on the prepared base `{}` ({} commits in the comparison range):",
        report.changed_files.len(), short(&report.prepared_start_commit), report.range_commit_count,
    )];
    for file in &report.changed_files {
        lines.push(format!("- `{}`:", file.path));
        for change in &file.commits {
            lines.push(format!(
                "  - `{}` {}",
                short(&change.commit_id),
                change.subject().replace(['\n', '\r'], " ")
            ));
        }
        if file.omitted_commits > 0 {
            lines.push(format!(
                "  - {} more touching commits omitted.",
                file.omitted_commits
            ));
        }
    }
    lines.push("Re-read these files and re-verify the plan's steps that touch them before building. Say in your first update what changed and whether the plan still holds. If it does not, message the plan's confirmer before building.".to_string());
    lines.join("\n")
}

/// Mark `turn_id` as carrying freshness evidence measured against `base`.
/// Composing a prompt is not delivering it: the job's checkpoint advances only
/// when this turn reaches a process ([`confirm_delivered`]). A launch that
/// fails, a held re-entry that is withdrawn, or a head that Compact supersedes
/// never starts its turn, so the next continuation repeats the evidence. A
/// failed write costs one repeated notice.
pub(super) async fn record_pending_notice(db: &LocalDb, turn_id: &str, base: &str) {
    let (turn, base) = (turn_id.to_string(), base.to_string());
    let written = db
        .write(move |conn| {
            let (turn, base) = (turn.clone(), base.clone());
            Box::pin(async move {
                conn.execute(
                    "UPDATE turns SET plan_notice_base_commit = ?1 WHERE id = ?2",
                    cairn_db::turso::params![base, turn],
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(error) = written {
        log::warn!("[plan freshness] turn {turn_id}: recording the notice base failed: {error}");
    }
}

/// A process on `run_id` now holds the prompt `turn_id` carried. Advance its
/// job's checkpoint to the base that prompt's freshness evidence measured
/// against. Called only after the backend handoff succeeded: a cold session
/// start, or a warm `send_user_message`, never at turn publication, which
/// precedes the warm send. A failure is logged and costs one repeated notice;
/// it never fails the delivery that called it.
pub(super) fn confirm_delivered(orch: &Orchestrator, turn_id: &str, run_id: &str) {
    let confirmed = super::run_db_borrowed(async {
        let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .map_err(|error| error.to_string())?;
        confirm_delivered_on(&db, turn_id).await
    });
    if let Err(error) = confirmed {
        log::warn!("[plan freshness] turn {turn_id}: confirming its notice failed: {error}");
    }
}

pub(super) async fn confirm_delivered_on(db: &LocalDb, turn_id: &str) -> Result<(), String> {
    let turn = turn_id.to_string();
    db.write(move |conn| {
        let turn = turn.clone();
        Box::pin(async move {
            use crate::storage::RowExt;
            let mut rows = conn
                .query(
                    "SELECT job_id, plan_notice_base_commit FROM turns WHERE id = ?1",
                    cairn_db::turso::params![turn],
                )
                .await?;
            let carried = match rows.next().await? {
                Some(row) => (row.opt_text(0)?, row.opt_text(1)?),
                None => (None, None),
            };
            drop(rows);
            let (Some(job), Some(base)) = carried else {
                return Ok(());
            };
            conn.execute(
                "UPDATE jobs SET plan_checked_base_commit = ?1 WHERE id = ?2",
                cairn_db::turso::params![base, job],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The checkpoint and the base branch the job's branch is carried from.
async fn checked_base_and_base_branch(
    db: &LocalDb,
    job_id: &str,
) -> Result<(Option<String>, String), String> {
    let job = job_id.to_string();
    db.read(move |conn| {
        let job = job.clone();
        Box::pin(async move {
            use crate::storage::RowExt;
            let mut rows = conn
                .query(
                    "SELECT j.plan_checked_base_commit,
                            COALESCE(NULLIF(j.base_branch, ''), p.default_branch, 'main')
                     FROM jobs j LEFT JOIN projects p ON p.id = j.project_id
                     WHERE j.id = ?1",
                    cairn_db::turso::params![job],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::internal("Job not found"))?;
            Ok((row.opt_text(0)?, row.text(1)?))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The live fork point of a job's branch with its base: the newest base
/// commit the branch contains. It excludes the builder's own commits (not
/// ancestors of the base) and base commits not yet carried (not ancestors of
/// the branch). `jobs.base_commit` is archival and is not consulted.
pub(super) async fn live_fork_point(
    db: &LocalDb,
    job_id: &str,
    store: &Path,
    branch: &str,
) -> Result<String, String> {
    let (_, base_branch) = checked_base_and_base_branch(db, job_id).await?;
    fork_point(store, branch, &base_branch).await
}

async fn fork_point(store: &Path, branch: &str, base_branch: &str) -> Result<String, String> {
    cairn_vcs::merge_base(store, branch, base_branch)
        .await
        .map(|resolved| resolved.base)
        .map_err(|error| format!("resolving `{branch}` against `{base_branch}`: {error}"))
}

/// Freshness evidence for a re-entry, given only when the live fork point is
/// not the base the builder was last delivered evidence against. A completed
/// comparison is recorded as pending on `turn_id`, the turn whose prompt
/// carries it, and becomes the checkpoint only when that turn reaches a
/// process. A lookup failure is a visible diagnostic, never silence or a claim
/// that nothing changed, and records nothing, so the next re-entry retries.
/// Addressed and system re-entry receive the same evidence; this helper does
/// not admit, hold, claim or deliver.
pub(super) async fn resume_notice(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    job: &DbJob,
    repository: Option<&Path>,
    turn_id: &str,
) -> Option<String> {
    let (Some(repository), Some(branch)) = (repository, job.branch.as_deref()) else {
        return None;
    };
    let job_id = job.id.as_str();
    match ships_pr(&db, job).await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(error) => return Some(unchecked(&error)),
    }
    let (checked, base_branch) = match checked_base_and_base_branch(&db, job_id).await {
        Ok(read) => read,
        Err(error) => return Some(unchecked(&error)),
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, repository);
    let fork = match fork_point(&store, branch, &base_branch).await {
        Ok(fork) => fork,
        Err(error) => return Some(unchecked(&error)),
    };
    if checked.as_deref() == Some(fork.as_str()) {
        return None;
    }
    let inputs = match super::resolve_inputs_and_schema(db.clone(), job.clone()).await {
        Ok((inputs, _)) => inputs,
        Err(error) => return Some(unchecked(&error)),
    };
    let start = Ok(fork);
    let mut notes = Vec::new();
    let mut complete = true;
    for plan in inputs.iter().filter(|input| input.artifact_type == "plan") {
        let (note, plan_checked) = plan_notice(&db, plan, &store, &start).await;
        complete &= plan_checked;
        notes.push(note);
    }
    if complete {
        if let Ok(fork) = &start {
            record_pending_notice(&db, turn_id, fork).await;
        }
    }
    (!notes.is_empty()).then(|| notes.join("\n\n"))
}

pub(super) async fn ships_pr(db: &LocalDb, job: &DbJob) -> Result<bool, String> {
    let (Some(node), Some(execution)) = (job.recipe_node_id.clone(), job.execution_id.clone())
    else {
        return Ok(false);
    };
    db.read(move |conn| {
        let (node, execution) = (node.clone(), execution.clone());
        Box::pin(async move { super::node_ships_a_pr_conn(conn, &node, &execution).await })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use history::{CommitInfo, PlannedFileChanges};

    fn commit(id: &str, subject: &str) -> CommitInfo {
        CommitInfo {
            commit_id: id.into(),
            parents: vec![],
            author_name: "Test".into(),
            author_email: "test@example.invalid".into(),
            author_time: 0,
            committer_time: 0,
            description: subject.into(),
        }
    }

    #[test]
    fn seed_names_changed_planned_files_but_no_change_has_no_warning() {
        let mut report = PlannedPathChanges {
            written_commit: commit("aaaaaaaaaaaa", "plan base"),
            prepared_start_commit: "bbbbbbbbbbbb".into(),
            range_commit_count: 1,
            named_files: vec!["src/a.rs".into(), "src/b.rs".into()],
            changed_files: vec![PlannedFileChanges {
                path: "src/a.rs".into(),
                commits: vec![commit("cccccccccccc", "cairn/42 change A")],
                omitted_commits: 2,
            }],
        };
        let moved = render_notice(3, &report, 86_400);
        assert!(moved.contains("**The base moved"));
        assert!(moved.contains("src/a.rs"));
        assert!(moved.contains("cccccccccccc"));
        assert!(moved.contains("cairn/42 change A"));
        assert!(!moved.contains("src/b.rs"));
        assert!(moved.contains("2 more"));
        report.changed_files.clear();
        let unchanged = render_notice(3, &report, 86_400);
        assert!(!unchanged.contains("The base moved"));
        assert!(!unchanged.contains('\n'));
        report.named_files.clear();
        let unresolved = render_notice(3, &report, 86_400);
        assert!(unresolved.contains("no resolvable files"));
        assert!(unresolved.contains("1 commits"));
    }
}
