//! Literal branch standing for overview reads; no preparation or bookmark repair.
use std::collections::{BTreeSet, HashMap};

use cairn_db::turso::params;
use futures_util::{stream, StreamExt};

use super::{ancestry_answer, capped, escape_table, AncestryAnswer, Orchestrator, QUERY_LIMIT};
use crate::resources::common::read_resource;
use crate::storage::{LocalDb, RowExt};

pub(in crate::resources) enum BranchSummaryScope {
    Issue(i32),
    Thread(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchRow {
    issue: i64,
    branch: String,
    base: String,
}

/// Kept separate from the database-only renderers so graph reads never hold a
/// database read connection while waiting for the store.
pub(in crate::resources) async fn branch_summaries(
    orch: &Orchestrator,
    db: &LocalDb,
    project: &str,
    scope: BranchSummaryScope,
) -> String {
    let project = project.to_string();
    let heading = match &scope {
        BranchSummaryScope::Issue(_) => "Branches",
        BranchSummaryScope::Thread(_) => "Child branches",
    };
    let loaded = read_resource(db, move |conn| {
        Box::pin(async move {
            let mut projects = conn.query(
                "SELECT id, repo_path, default_branch FROM projects WHERE key = ?1 LIMIT 1",
                params![project],
            ).await.map_err(|e| e.to_string())?;
            let Some(row) = projects.next().await.map_err(|e| e.to_string())? else {
                return Err("project not found".to_string());
            };
            let project_id = row.text(0).map_err(|e| e.to_string())?;
            let repo_path = row.opt_text(1).map_err(|e| e.to_string())?;
            let stored_default = row.opt_text(2).map_err(|e| e.to_string())?;
            let default_branch = crate::config::project_settings::effective_default_branch(
                repo_path.as_deref().map(std::path::Path::new),
                stored_default.as_deref(),
            );
            drop(projects);
            let (predicate, selector) = match scope {
                BranchSummaryScope::Issue(number) => ("i.number = CAST(?2 AS INTEGER)", number.to_string()),
                BranchSummaryScope::Thread(name) => ("i.parent_thread_id = (SELECT id FROM threads WHERE project_id = ?1 AND name = ?2)", name),
            };
            let sql = format!(
                "SELECT DISTINCT i.number, j.branch, COALESCE(j.base_branch, ?3)
                 FROM issues i JOIN jobs j ON j.issue_id = i.id
                 WHERE i.project_id = ?1 AND {predicate}
                   AND j.branch IS NOT NULL AND j.branch != ''
                 ORDER BY i.number, j.branch, COALESCE(j.base_branch, ?3)"
            );
            let mut rows = conn.query(&sql, params![project_id.as_str(), selector, default_branch])
                .await.map_err(|e| e.to_string())?;
            let mut branches = Vec::new();
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                branches.push(BranchRow {
                    issue: row.i64(0).map_err(|e| e.to_string())?,
                    branch: row.text(1).map_err(|e| e.to_string())?,
                    base: row.text(2).map_err(|e| e.to_string())?,
                });
            }
            Ok((project_id, repo_path, branches))
        })
    }).await;
    let (project_id, repo_path, rows) = match loaded {
        Ok(loaded) => loaded,
        Err(error) => {
            return format!(
                "\n## {heading}\n\nBranch standing unknown: {}\n",
                escape_table(&error)
            )
        }
    };
    if rows.is_empty() {
        return format!("\n## {heading}\n\nNo recorded branches.\n");
    }
    let store = repo_path
        .map(|repo| crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(&repo)));
    let deadline = tokio::time::Instant::now() + QUERY_LIMIT;
    let names = branch_names(&rows);
    let coordinates = collect_with_deadline(names.into_iter().collect(), deadline, |name| {
        let store = store.clone();
        async move {
            match store {
                None => Err("no local repository".to_string()),
                Some(store) => cairn_vcs::resolve_coordinate(&store, &name)
                    .await
                    .map_err(|e| e.to_string()),
            }
        }
    })
    .await;
    let pairs: BTreeSet<_> = rows
        .iter()
        .map(|row| (row.branch.clone(), row.base.clone()))
        .collect();
    let answers = collect_with_deadline(pairs.into_iter().collect(), deadline, |(branch, base)| {
        let store = store.clone();
        let coordinates = &coordinates;
        let project_id = &project_id;
        async move {
            match (coordinates.get(&base), coordinates.get(&branch), store) {
                (Some(Ok(from)), Some(Ok(to)), Some(store)) => {
                    ancestry_answer(project_id, store, from.clone(), to.clone()).await
                }
                _ => Err("head or base is unresolved".to_string()),
            }
        }
    })
    .await;
    render_summaries(heading, &rows, &coordinates, &answers)
}

/// Both resolution and ancestry share one overview-wide deadline. Stop
/// admitting work when it expires, retaining every uncompleted key as unknown.
async fn collect_with_deadline<K, V, F, Fut>(
    keys: Vec<K>,
    deadline: tokio::time::Instant,
    compute: F,
) -> HashMap<K, Result<V, String>>
where
    K: Clone + Eq + std::hash::Hash,
    F: Fn(K) -> Fut,
    Fut: std::future::Future<Output = Result<V, String>>,
{
    let mut answers: HashMap<_, _> = keys
        .iter()
        .cloned()
        .map(|key| (key, Err("overview graph budget exhausted".to_string())))
        .collect();
    if tokio::time::Instant::now() >= deadline {
        return answers;
    }
    let completed: Vec<_> = stream::iter(keys.into_iter().map(|key| {
        let result = compute(key.clone());
        async move { (key, result.await) }
    }))
    .buffer_unordered(8)
    .take_until(tokio::time::sleep_until(deadline))
    .collect()
    .await;
    answers.extend(completed);
    answers
}

fn branch_names(rows: &[BranchRow]) -> BTreeSet<String> {
    rows.iter()
        .flat_map(|row| [row.branch.clone(), row.base.clone()])
        .collect()
}

fn coordinate_cell(coordinate: &Result<String, String>) -> String {
    match coordinate {
        Ok(commit) => format!("`{commit}`"),
        Err(error) => format!("unknown ({})", escape_table(error)),
    }
}

fn render_summaries(
    heading: &str,
    rows: &[BranchRow],
    coordinates: &HashMap<String, Result<String, String>>,
    answers: &HashMap<(String, String), Result<AncestryAnswer, String>>,
) -> String {
    let mut out = format!("\n## {heading}\n\nLiteral store heads, not prepared or tested trees. Ahead/behind is relative to the declared base's current head.\n\n| Issue | Branch | Head | Declared base | Base head | Ahead | Behind | Ancestry |\n|---|---|---|---|---|---|---|---|\n");
    for row in rows {
        let (ahead, behind, standing) = match &answers[&(row.branch.clone(), row.base.clone())] {
            Ok(answer) => (
                capped(answer.ahead, answer.ahead_capped),
                capped(answer.behind, answer.behind_capped),
                if answer.merge_base.is_some() {
                    "known".to_string()
                } else {
                    "no common ancestor".to_string()
                },
            ),
            Err(error) => (
                "unknown".to_string(),
                "unknown".to_string(),
                escape_table(error),
            ),
        };
        out.push_str(&format!(
            "| {} | `{}` | {} | `{}` | {} | {ahead} | {behind} | {standing} |\n",
            row.issue,
            escape_table(&row.branch),
            coordinate_cell(&coordinates[&row.branch]),
            escape_table(&row.base),
            coordinate_cell(&coordinates[&row.base]),
        ));
    }
    out
}

#[cfg(test)]
mod tests;
