//! Documentation operations — filesystem scanning and DB reference queries.

use cairn_common::ids;
use std::fs;
use std::path::Path;
use turso::params;

use crate::models::{DocContent, DocFile, DocReference};
use crate::storage::{DbError, LocalDb, RowExt};

const DEFAULT_DOC_ROOTS: &[&str] = &["docs/", "*.md"];

/// Get doc_roots configuration for a project
pub async fn get_doc_roots(db: &LocalDb, project_id: &str) -> Result<Vec<String>, String> {
    let project_id = project_id.to_string();
    let config_json = db
        .read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT config FROM projects WHERE id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("project not found".to_string()))?;
                row.opt_text(0)
            })
        })
        .await
        .map_err(|e| format!("Project not found: {e}"))?;

    if let Some(ref json_str) = config_json {
        if let Ok(config) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Some(roots) = config.get("docRoots").and_then(|v| v.as_array()) {
                let roots: Vec<String> = roots
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                if !roots.is_empty() {
                    return Ok(roots);
                }
            }
        }
    }

    Ok(DEFAULT_DOC_ROOTS.iter().map(|s| s.to_string()).collect())
}

/// Validate that a path is within the repo and doesn't contain path traversal
pub fn validate_doc_path(repo_path: &Path, doc_path: &str) -> Result<std::path::PathBuf, String> {
    // Reject paths with .. to prevent traversal
    if doc_path.contains("..") {
        return Err("Invalid path: path traversal not allowed".to_string());
    }

    let full_path = repo_path.join(doc_path);

    // Ensure the resolved path is still within the repo
    let canonical_repo = repo_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve repo path: {}", e))?;

    if let Ok(canonical_doc) = full_path.canonicalize() {
        if !canonical_doc.starts_with(&canonical_repo) {
            return Err("Invalid path: outside repository".to_string());
        }
    }

    Ok(full_path)
}

/// Check if a path matches a glob pattern
fn matches_glob(path: &Path, pattern: &str) -> bool {
    if pattern.ends_with('/') {
        // Directory pattern
        let dir_name = pattern.trim_end_matches('/');
        path.starts_with(dir_name)
    } else if pattern.contains('*') {
        // Wildcard pattern
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            let pattern_ext = pattern.trim_start_matches('*');
            file_name.ends_with(pattern_ext)
        } else {
            false
        }
    } else {
        // Exact match
        path == Path::new(pattern)
    }
}

/// Recursively scan a directory for markdown files
fn scan_directory_impl(
    repo_root: &Path,
    current_path: &Path,
    roots: &[String],
) -> Result<Vec<DocFile>, String> {
    let mut files = Vec::new();

    if !current_path.is_dir() {
        return Ok(files);
    }

    let entries =
        fs::read_dir(current_path).map_err(|e| format!("Failed to read directory: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let entry_path = entry.path();

        // Skip hidden files and directories
        if let Some(name) = entry_path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with('.') {
                continue;
            }
        }

        // Get path relative to repo root
        let relative_path = entry_path
            .strip_prefix(repo_root)
            .map_err(|e| format!("Failed to get relative path: {}", e))?;

        if entry_path.is_dir() {
            // Check if this directory matches any root pattern
            let should_scan = roots.iter().any(|root| matches_glob(relative_path, root));

            if should_scan {
                let children = scan_directory_impl(repo_root, &entry_path, roots)?;
                if !children.is_empty() {
                    files.push(DocFile {
                        path: relative_path.to_string_lossy().to_string(),
                        name: entry_path
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string(),
                        is_directory: true,
                        children: Some(children),
                    });
                }
            }
        } else if entry_path.extension().and_then(|e| e.to_str()) == Some("md") {
            // Check if this file matches any root pattern
            let should_include = roots.iter().any(|root| matches_glob(relative_path, root));

            if should_include {
                files.push(DocFile {
                    path: relative_path.to_string_lossy().to_string(),
                    name: entry_path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                    is_directory: false,
                    children: None,
                });
            }
        }
    }

    // Sort: directories first, then files, alphabetically
    files.sort_by(|a, b| match (a.is_directory, b.is_directory) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });

    Ok(files)
}

/// Scan a repository for documentation files based on configured doc roots.
pub fn scan_docs(repo_path: &Path, roots: &[String]) -> Result<Vec<DocFile>, String> {
    scan_directory_impl(repo_path, repo_path, roots)
}

/// Read a documentation file's content.
pub fn read_doc(repo_path: &Path, doc_path: &str) -> Result<DocContent, String> {
    let full_path = validate_doc_path(repo_path, doc_path)?;

    let content =
        fs::read_to_string(&full_path).map_err(|e| format!("Failed to read file: {}", e))?;

    Ok(DocContent {
        path: doc_path.to_string(),
        content,
    })
}

/// Write a documentation file. Does not handle git commit (caller's responsibility).
pub fn write_doc(repo_path: &Path, doc_path: &str, content: &str) -> Result<(), String> {
    let full_path = validate_doc_path(repo_path, doc_path)?;

    // Ensure parent directory exists
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory: {}", e))?;
    }

    fs::write(&full_path, content).map_err(|e| format!("Failed to write file: {}", e))
}

/// Attach a doc reference to an issue.
pub async fn attach_doc(
    db: &LocalDb,
    issue_id: &str,
    doc_path: &str,
) -> Result<DocReference, String> {
    let id = ids::mint_child(issue_id);
    let issue_id = issue_id.to_string();
    let doc_path = doc_path.to_string();
    let created_at = chrono::Utc::now().timestamp_millis();

    db.execute(
        "INSERT INTO doc_references(id, issue_id, doc_path, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            id.as_str(),
            issue_id.as_str(),
            doc_path.as_str(),
            created_at
        ],
    )
    .await
    .map_err(|e| format!("Failed to attach doc: {e}"))?;

    Ok(DocReference {
        id,
        issue_id,
        doc_path,
        created_at,
    })
}

/// Detach a doc reference by ID.
pub async fn detach_doc(db: &LocalDb, reference_id: &str) -> Result<(), String> {
    let reference_id = reference_id.to_string();
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
    .map_err(|e| format!("Failed to detach doc: {e}"))
}

/// List doc references for an issue.
pub async fn list_doc_references(
    db: &LocalDb,
    issue_id: &str,
) -> Result<Vec<DocReference>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
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
                references.push(DocReference {
                    id: row.text(0)?,
                    issue_id: row.text(1)?,
                    doc_path: row.text(2)?,
                    created_at: row.i64(3)?,
                });
            }
            Ok(references)
        })
    })
    .await
    .map_err(|e| e.to_string())
}
