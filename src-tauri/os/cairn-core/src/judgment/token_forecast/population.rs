//! The refit's population, read from the database: every issue created on or
//! after a floor that has an execution, with its resolution, open executions,
//! and the billable tokens of its primary maker.
//!
//! Ports `bench/token-forecast/sql/issues.sql`, `sql/tokens.sql`, and the
//! primary-maker rule of `extract.py::build`: tokens join `token_rollup.job_id`
//! to `jobs.issue_id` (sub-agent jobs carry the same issue), every execution
//! counts, the rollup's model column is the selected model, so the maker comes
//! from its name, and the primary maker is the one that spent the most
//! billable tokens.

use std::collections::{BTreeMap, HashMap};

use cairn_db::turso::params;

use super::applicability::maker;
use super::fit::PopulationIssue;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Every issue created on or after `floor` with at least one execution.
pub async fn load(db: &LocalDb, floor: i64) -> DbResult<Vec<PopulationIssue>> {
    let mut issues = db
        .query_all(
            "SELECT i.id, p.key, i.number, i.status, i.created_at, i.merged_at, i.closed_at,
                    (SELECT count(*) FROM executions e
                     WHERE e.issue_id = i.id AND e.status IN ('running', 'held'))
             FROM issues i
             LEFT JOIN projects p ON p.id = i.project_id
             WHERE i.created_at >= ?1
               AND EXISTS (SELECT 1 FROM executions e WHERE e.issue_id = i.id)
             ORDER BY i.created_at, i.id",
            params![floor],
            |row| {
                let status = row.text(3)?;
                let resolved_at = match status.as_str() {
                    "merged" => row.opt_i64(5)?,
                    "closed" => row.opt_i64(6)?,
                    _ => None,
                };
                Ok(PopulationIssue {
                    issue_id: row.text(0)?,
                    project_key: row.opt_text(1)?,
                    number: row.i64(2)?,
                    created_at: row.i64(4)?,
                    resolved_at,
                    open_executions: row.i64(7)?,
                    last_bucket: None,
                    primary_billable: 0,
                    primary_maker: None,
                })
            },
        )
        .await?;
    let tokens = db
        .query_all(
            "SELECT j.issue_id, lower(coalesce(t.model, '')), sum(t.billable_tokens),
                    max(t.bucket_start)
             FROM token_rollup t
             JOIN jobs j ON j.id = t.job_id
             JOIN issues i ON i.id = j.issue_id
             WHERE i.created_at >= ?1
             GROUP BY j.issue_id, lower(coalesce(t.model, ''))",
            params![floor],
            |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?, row.opt_i64(3)?)),
        )
        .await?;
    // Billable per maker per issue; a BTreeMap so a tie names the same maker
    // on every read.
    let mut by_issue: HashMap<String, (BTreeMap<String, i64>, Option<i64>)> = HashMap::new();
    for (issue_id, model, billable, last_bucket) in tokens {
        let entry = by_issue.entry(issue_id).or_default();
        let maker = maker(Some(&model)).unwrap_or_else(|| "unknown".to_string());
        *entry.0.entry(maker).or_default() += billable;
        entry.1 = entry.1.max(last_bucket);
    }
    for issue in &mut issues {
        let Some((makers, last_bucket)) = by_issue.get(&issue.issue_id) else {
            continue;
        };
        issue.last_bucket = *last_bucket;
        if let Some((primary, billable)) =
            makers.iter().filter(|(_, billable)| **billable > 0).fold(
                None::<(&String, i64)>,
                |best, (maker, billable)| match best {
                    Some((_, top)) if top >= *billable => best,
                    _ => Some((maker, *billable)),
                },
            )
        {
            issue.primary_maker = Some(primary.clone());
            issue.primary_billable = billable;
        }
    }
    Ok(issues)
}
