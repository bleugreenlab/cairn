//! Orchestrator doc operations.

use std::path::PathBuf;

use crate::docs;
use crate::models::{DocContent, DocFile, DocReference};
use crate::storage::{run_db_blocking, DbResult, RowExt};
use cairn_common::ids;

use super::Orchestrator;

const DEFAULT_DOC_ROOTS: &[&str] = &["docs/", "*.md"];

fn doc_roots_from_config(config_json: Option<String>) -> Vec<String> {
    if let Some(json_str) = config_json {
        if let Ok(config) = serde_json::from_str::<serde_json::Value>(&json_str) {
            if let Some(roots) = config.get("docRoots").and_then(|v| v.as_array()) {
                let roots: Vec<String> = roots
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                if !roots.is_empty() {
                    return roots;
                }
            }
        }
    }

    DEFAULT_DOC_ROOTS.iter().map(|s| s.to_string()).collect()
}

fn doc_reference_from_row(row: &turso::Row) -> DbResult<DocReference> {
    Ok(DocReference {
        id: row.text(0)?,
        issue_id: row.text(1)?,
        doc_path: row.text(2)?,
        created_at: row.i64(3)?,
    })
}

impl Orchestrator {
    fn project_repo_path(&self, project_id: &str) -> Result<PathBuf, String> {
        let db = self.db.local.clone();
        let project_id = project_id.to_string();
        let missing_id = project_id.clone();
        let repo_path = run_db_blocking(move || async move {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT repo_path
                             FROM projects
                             WHERE id = ?1",
                            (project_id.as_str(),),
                        )
                        .await?;
                    crate::storage::next_text(&mut rows, 0).await
                })
            })
            .await
            .map_err(|e| e.to_string())
        })?;

        repo_path
            .map(PathBuf::from)
            .ok_or_else(|| format!("Project not found: {}", missing_id))
    }

    fn project_docs_config(&self, project_id: &str) -> Result<(PathBuf, Vec<String>), String> {
        let db = self.db.local.clone();
        let project_id = project_id.to_string();
        let missing_id = project_id.clone();
        let row = run_db_blocking(move || async move {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT repo_path, config
                             FROM projects
                             WHERE id = ?1",
                            (project_id.as_str(),),
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| Ok((row.text(0)?, row.opt_text(1)?)))
                        .transpose()
                })
            })
            .await
            .map_err(|e| e.to_string())
        })?;

        let (repo_path, config_json) =
            row.ok_or_else(|| format!("Project not found: {}", missing_id))?;
        Ok((PathBuf::from(repo_path), doc_roots_from_config(config_json)))
    }

    /// List documentation files for a project.
    pub fn list_docs(&self, project_id: &str) -> Result<Vec<DocFile>, String> {
        let (repo_path, roots) = self.project_docs_config(project_id)?;
        docs::scan_docs(&repo_path, &roots)
    }

    /// Read a documentation file's content.
    pub fn read_doc(&self, project_id: &str, doc_path: &str) -> Result<DocContent, String> {
        let repo_path = self.project_repo_path(project_id)?;
        docs::read_doc(&repo_path, doc_path)
    }

    /// Write a documentation file and git commit.
    pub fn write_doc(&self, project_id: &str, doc_path: &str, content: &str) -> Result<(), String> {
        let repo_path = self.project_repo_path(project_id)?;
        docs::write_doc(&repo_path, doc_path, content)?;

        // Git add + commit
        let commit_msg = format!("Update {} via Cairn", doc_path);
        let output = std::process::Command::new("git")
            .current_dir(&repo_path)
            .args(["add", doc_path])
            .output()
            .map_err(|e| format!("Failed to git add: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "git add failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let output = std::process::Command::new("git")
            .current_dir(&repo_path)
            .args(["commit", "-m", &commit_msg])
            .output()
            .map_err(|e| format!("Failed to git commit: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "git commit failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let _ = self.services.emitter.emit_empty("doc-changed");

        Ok(())
    }

    /// Attach a doc reference to an issue.
    pub fn attach_doc(&self, issue_id: &str, doc_path: &str) -> Result<DocReference, String> {
        let db = self.db.local.clone();
        let id = ids::mint_child(issue_id);
        let issue_id = issue_id.to_string();
        let doc_path = doc_path.to_string();
        let created_at = chrono::Utc::now().timestamp_millis();

        let reference = DocReference {
            id: id.clone(),
            issue_id: issue_id.clone(),
            doc_path: doc_path.clone(),
            created_at,
        };

        run_db_blocking(move || {
            let reference = reference.clone();
            async move {
                db.write(|conn| {
                    let reference = reference.clone();
                    Box::pin(async move {
                        conn.execute(
                            "INSERT INTO doc_references(id, issue_id, doc_path, created_at)
                             VALUES (?1, ?2, ?3, ?4)",
                            (
                                reference.id.as_str(),
                                reference.issue_id.as_str(),
                                reference.doc_path.as_str(),
                                reference.created_at,
                            ),
                        )
                        .await?;
                        Ok(())
                    })
                })
                .await
                .map_err(|e| format!("Failed to attach doc: {}", e))?;

                Ok(reference)
            }
        })
    }

    /// Detach a doc reference.
    pub fn detach_doc(&self, reference_id: &str) -> Result<(), String> {
        let db = self.db.local.clone();
        let reference_id = reference_id.to_string();
        run_db_blocking(move || async move {
            db.write(|conn| {
                let reference_id = reference_id.clone();
                Box::pin(async move {
                    conn.execute(
                        "DELETE FROM doc_references WHERE id = ?1",
                        (reference_id.as_str(),),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| format!("Failed to detach doc: {}", e))
        })
    }

    /// List doc references for an issue.
    pub fn list_doc_references(&self, issue_id: &str) -> Result<Vec<DocReference>, String> {
        let db = self.db.local.clone();
        let issue_id = issue_id.to_string();
        run_db_blocking(move || async move {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT id, issue_id, doc_path, created_at
                             FROM doc_references
                             WHERE issue_id = ?1
                             ORDER BY created_at DESC",
                            (issue_id.as_str(),),
                        )
                        .await?;
                    let mut references = Vec::new();
                    while let Some(row) = rows.next().await? {
                        references.push(doc_reference_from_row(&row)?);
                    }
                    Ok(references)
                })
            })
            .await
            .map_err(|e| e.to_string())
        })
    }
}
