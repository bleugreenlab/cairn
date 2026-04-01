//! File operation MCP handlers.
//!
//! Handles: edit (unified file mutations), read

/// Sentinel value for commit_msg that skips git add/commit entirely.
/// The file is written to disk but left as an unstaged change.
const NO_COMMIT: &str = "NO_COMMIT";

use crate::config::agents as config_agents;
use crate::diesel_models::NewFileChange;
use crate::mcp::git::{validate_file_path_dry, validate_read_path};
use crate::mcp::types::{IssueHistoryMode, McpCallbackRequest, ReadFilePayload};
use crate::orchestrator::Orchestrator;
use crate::schema::{file_changes, issues, jobs, merge_requests, projects};
use diesel::prelude::*;

use super::lookup_run_by_cwd;

/// Get agent name prefix for commit messages if the run is using an agent.
/// Returns empty string if no agent is configured.
fn get_agent_commit_prefix(orch: &Orchestrator, cwd: &str) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return String::new(),
    };

    // Look up the run by cwd to find job_id
    let job_id = match lookup_run_by_cwd(&mut conn, cwd) {
        Ok(ctx) => ctx.job_id,
        Err(_) => return String::new(),
    };

    // Look up agent_config_id and project_id from the job
    let (agent_config_id, project_id): (Option<String>, String) = match jobs::table
        .find(&job_id)
        .select((jobs::agent_config_id, jobs::project_id))
        .first::<(Option<String>, String)>(&mut *conn)
    {
        Ok(result) => result,
        Err(_) => return String::new(),
    };

    // If we have an agent_config_id, look up the agent name from files
    if let Some(agent_id) = agent_config_id {
        // Get project path for file-based config lookup
        let project_path: Option<std::path::PathBuf> = projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(std::path::PathBuf::from);

        let config_dir = orch.config_dir.clone();
        match config_agents::get_agent(&config_dir, &agent_id, project_path.as_deref()) {
            Ok(Some(agent)) => format!("[{}] ", agent.name),
            _ => String::new(),
        }
    } else {
        String::new()
    }
}

/// Record a file change in the database for issue history tracking.
/// Best-effort: failures are logged but don't affect the write/edit result.
fn record_file_change(orch: &Orchestrator, cwd: &str, file_path: &str, status: &str) {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return,
    };

    let job_id = match lookup_run_by_cwd(&mut conn, cwd) {
        Ok(ctx) => ctx.job_id,
        Err(_) => return,
    };

    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let new = NewFileChange {
        id: &id,
        job_id: &job_id,
        file_path,
        status,
        additions: None,
        deletions: None,
        previous_path: None,
        created_at: now,
    };

    if let Err(e) = diesel::insert_into(file_changes::table)
        .values(&new)
        .execute(&mut *conn)
    {
        log::warn!("Failed to record file change: {}", e);
    }
}

/// A validated, ready-to-apply file change.
enum PreparedChange {
    /// Write new content to this path (add or update).
    Write {
        path: String,
        full_path: std::path::PathBuf,
        content: String,
        /// Whether this is a new file (didn't exist before this batch).
        is_new: bool,
    },
    /// Delete this file.
    Delete {
        path: String,
        full_path: std::path::PathBuf,
    },
}

/// Handle unified edit tool call - applies multiple file changes and commits atomically.
///
/// Uses a validate-first approach: all inputs are validated and diffs resolved
/// before any file is touched on disk. This prevents partial-apply failures
/// where some files are written but the batch fails on a later change.
pub async fn handle_edit(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let services = &orch.services;
    let payload: crate::mcp::types::FileChangePayload =
        match serde_json::from_value(request.payload.clone()) {
            Ok(p) => p,
            Err(e) => return format!("Invalid payload: {}", e),
        };

    log::info!(
        "edit for cwd={}, {} changes",
        request.cwd,
        payload.changes.len()
    );

    let worktree = std::path::Path::new(&request.cwd);

    // ── Phase 1: Validate all changes and resolve diffs ──────────────
    // Nothing is written to disk in this phase. Uses validate_file_path_dry
    // to check worktree containment without creating directories.
    //
    // `in_flight` tracks accumulated content for files with multiple changes
    // so later edits compose on top of earlier ones rather than clobbering.
    let mut prepared: Vec<PreparedChange> = Vec::with_capacity(payload.changes.len());
    let mut summaries: Vec<String> = Vec::new();
    let mut in_flight: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    for change in &payload.changes {
        match change.kind.as_str() {
            "add" => {
                let content = match &change.content {
                    Some(c) => c.clone(),
                    None => {
                        return format!("kind=add requires content field for '{}'", change.path)
                    }
                };

                // Dry validation — no directory creation
                let full_path = match validate_file_path_dry(worktree, &change.path) {
                    Ok(p) => p,
                    Err(e) => return format!("Invalid file path '{}': {}", change.path, e),
                };

                // Track whether this is truly a new file (for accurate status recording)
                let is_new = !full_path.exists() && !in_flight.contains_key(&change.path);

                in_flight.insert(change.path.clone(), content.clone());
                prepared.push(PreparedChange::Write {
                    path: change.path.clone(),
                    full_path,
                    content,
                    is_new,
                });
                summaries.push(format!("+{}", change.path));
            }
            "update" => {
                // Dry validation — file must already exist for updates
                // (unless a prior add in this batch created it)
                let full_path = match validate_file_path_dry(worktree, &change.path) {
                    Ok(p) => p,
                    Err(e) => return format!("Invalid file path '{}': {}", change.path, e),
                };

                // Use in-flight content from a prior change in this batch,
                // or read from disk for the first touch.
                let content = if let Some(prev) = in_flight.get(&change.path) {
                    prev.clone()
                } else {
                    if !full_path.exists() {
                        return format!("File does not exist: {}", change.path);
                    }
                    match std::fs::read_to_string(&full_path) {
                        Ok(c) => c,
                        Err(e) => return format!("Failed to read '{}': {}", change.path, e),
                    }
                };

                // Two modes: unified diff OR old_string/new_string find-replace
                let patched = if let Some(ref diff_text) = change.diff {
                    if let Err(e) =
                        crate::mcp::diff::validate_single_file_diff(diff_text, &change.path)
                    {
                        return format!("Invalid diff for '{}': {}", change.path, e);
                    }
                    match crate::mcp::diff::apply_unified_diff(&content, diff_text) {
                        Ok(p) => p,
                        Err(e) => {
                            return format!("Failed to apply diff to '{}': {}", change.path, e)
                        }
                    }
                } else if let (Some(ref old), Some(ref new)) =
                    (&change.old_string, &change.new_string)
                {
                    // Check for wildcard anchor pattern (head\n~~~~~\ntail)
                    if let Some(anchors) = crate::mcp::wildcard::parse_wildcard(old) {
                        match crate::mcp::wildcard::apply_wildcard_edit(&content, &anchors, new) {
                            Ok((result, _replaced)) => result,
                            Err(e) => {
                                return format!("Wildcard edit failed on '{}': {}", change.path, e)
                            }
                        }
                    } else if !content.contains(old.as_str()) {
                        return format!(
                            "old_string not found in '{}'. Make sure the text matches exactly.",
                            change.path
                        );
                    } else if change.replace_all.unwrap_or(false) {
                        content.replace(old.as_str(), new.as_str())
                    } else {
                        content.replacen(old.as_str(), new.as_str(), 1)
                    }
                } else {
                    return format!(
                        "kind=update requires either 'diff' or 'old_string'+'new_string' for '{}'",
                        change.path
                    );
                };

                in_flight.insert(change.path.clone(), patched.clone());
                prepared.push(PreparedChange::Write {
                    path: change.path.clone(),
                    full_path,
                    content: patched,
                    is_new: false, // update always targets existing content
                });
                summaries.push(format!("~{}", change.path));
            }
            "delete" => {
                let full_path = match validate_file_path_dry(worktree, &change.path) {
                    Ok(p) => p,
                    Err(e) => return format!("Invalid file path '{}': {}", change.path, e),
                };

                prepared.push(PreparedChange::Delete {
                    path: change.path.clone(),
                    full_path,
                });
                summaries.push(format!("-{}", change.path));
            }
            other => {
                return format!(
                    "Unknown change kind '{}' for '{}'. Expected: add, update, delete",
                    other, change.path
                );
            }
        }
    }

    // ── Phase 2: Apply all changes to disk ───────────────────────────
    // All validation passed — now create directories, write, and delete.
    let mut affected_paths: Vec<String> = Vec::with_capacity(prepared.len());

    for change in &prepared {
        match change {
            PreparedChange::Write {
                path,
                full_path,
                content,
                ..
            } => {
                // Create parent directories now (deferred from Phase 1)
                if let Some(parent) = full_path.parent() {
                    if !parent.exists() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            return format!(
                                "Failed to create parent directories for '{}': {}",
                                path, e
                            );
                        }
                    }
                }
                if let Err(e) = std::fs::write(full_path, content) {
                    return format!("Failed to write '{}': {}", path, e);
                }
                affected_paths.push(path.clone());
            }
            PreparedChange::Delete { path, full_path } => {
                if full_path.exists() {
                    if let Err(e) = std::fs::remove_file(full_path) {
                        return format!("Failed to delete '{}': {}", path, e);
                    }
                }
                affected_paths.push(path.clone());
            }
        }
    }

    // ── Phase 3: Commit (or skip for NO_COMMIT) ─────────────────────
    if payload.commit_msg == NO_COMMIT {
        let _ = services.emitter.emit(
            "worktree-changed",
            serde_json::json!({"worktree_path": request.cwd}),
        );
        let summary = summaries.join(", ");
        return format!(
            "Applied {} changes ({}) (not committed)",
            payload.changes.len(),
            summary
        );
    }

    let agent_prefix = get_agent_commit_prefix(orch, &request.cwd);
    let final_commit_msg = if payload.commit_msg == "^" {
        "^".to_string()
    } else {
        format!("{}{}", agent_prefix, payload.commit_msg)
    };

    let path_refs: Vec<&str> = affected_paths.iter().map(|s| s.as_str()).collect();
    match crate::mcp::git::git_commit_files(worktree, &path_refs, &final_commit_msg) {
        Ok(result) => {
            // Record file changes only after successful commit.
            // Use is_new from prepared changes for accurate status (kind=add on
            // an existing file is a modify, not an add).
            for prep in &prepared {
                let (path, status) = match prep {
                    PreparedChange::Write { path, is_new, .. } => {
                        (path.as_str(), if *is_new { "added" } else { "modified" })
                    }
                    PreparedChange::Delete { path, .. } => (path.as_str(), "deleted"),
                };
                record_file_change(orch, &request.cwd, path, status);
            }

            let _ = services.emitter.emit(
                "worktree-changed",
                serde_json::json!({"worktree_path": request.cwd}),
            );

            let pr_suffix = result
                .pr_number
                .map(|pr| format!(" (updated PR#{})", pr))
                .unwrap_or_default();

            let summary = summaries.join(", ");
            let action = if payload.commit_msg == "^" {
                "amended commit"
            } else {
                "committed"
            };

            format!(
                "Applied {} changes ({}) and {} {}{}",
                payload.changes.len(),
                summary,
                action,
                result.sha,
                pr_suffix,
            )
        }
        Err(e) => format!("Applied changes but commit failed: {}", e),
    }
}

/// Returns MIME type if path has a known image extension supported by the Claude API
fn get_image_mime_type(path: &std::path::Path) -> Option<&'static str> {
    match path.extension()?.to_str()?.to_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    }
}

/// Size threshold for large file warning (250KB)
const LARGE_FILE_THRESHOLD: u64 = 250_000;

/// Handle read tool call - reads file content with optional offset/limit
pub async fn handle_read_file(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: ReadFilePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!(
        "read for cwd={}, file={}, issue_history={:?}",
        request.cwd,
        payload.path,
        payload.issue_history
    );

    // Use cwd directly as the worktree path
    let worktree = std::path::Path::new(&request.cwd);

    // Validate path (allows absolute paths, requires file exists)
    let full_path = match validate_read_path(worktree, &payload.path) {
        Ok(p) => p,
        Err(e) => return format!("Invalid file path: {}", e),
    };

    // Check if this is a directory
    if full_path.is_dir() {
        return format_directory_listing(&full_path);
    }

    // Check if this is an image file
    if let Some(mime_type) = get_image_mime_type(&full_path) {
        // Read as bytes and base64 encode
        let bytes = match std::fs::read(&full_path) {
            Ok(b) => b,
            Err(e) => return format!("Failed to read file: {}", e),
        };
        let data = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
        return serde_json::json!({
            "is_image": true,
            "mime_type": mime_type,
            "data": data
        })
        .to_string();
    }

    // Check file size BEFORE reading to prevent massive tool results
    let metadata = match std::fs::metadata(&full_path) {
        Ok(m) => m,
        Err(e) => return format!("Failed to get file metadata: {}", e),
    };

    // If file is large and no limit specified, guide the agent to read in sections
    if metadata.len() > LARGE_FILE_THRESHOLD && payload.limit.is_none() {
        let estimated_lines = metadata.len() / 50; // ~50 bytes per line estimate
        return format!(
            "⚠️ File is large (~{} lines). Read in sections using offset and limit:\n\n\
            Example: read with limit=500 for first 500 lines\n\
            Then: read with offset=500, limit=500 for next section\n\n\
            File: {}\n\
            Size: {} bytes\n\
            Lines: ~{} (estimated)",
            estimated_lines,
            full_path.display(),
            metadata.len(),
            estimated_lines
        );
    }

    // Read file content as text
    let content = match std::fs::read_to_string(&full_path) {
        Ok(c) => c,
        Err(e) => return format!("Failed to read file: {}", e),
    };

    // Apply offset and limit (line-based, matching native Read behavior)
    let lines: Vec<&str> = content.lines().collect();
    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(2000); // Default like native Read

    // Format with line numbers (cat -n style, 1-based)
    let formatted: Vec<String> = lines
        .iter()
        .enumerate()
        .skip(offset)
        .take(limit)
        .map(|(i, line)| format!("{:>6}\t{}", i + 1, line))
        .collect();

    let mut result = formatted.join("\n");

    // Append file history if requested
    if let Some(ref mode) = payload.issue_history {
        // Compute relative path from cwd for lookup
        let relative_path = if full_path.starts_with(worktree) {
            full_path
                .strip_prefix(worktree)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| payload.path.clone())
        } else {
            payload.path.clone()
        };

        let history = get_file_issue_history(orch, &relative_path, mode);
        if !history.is_empty() {
            result.push_str("\n\n");
            result.push_str(&history);
        }
    }

    result
}

/// Format a directory listing with directories first, then files with sizes.
fn format_directory_listing(dir_path: &std::path::Path) -> String {
    let entries = match std::fs::read_dir(dir_path) {
        Ok(e) => e,
        Err(e) => return format!("Failed to read directory: {}", e),
    };

    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<(String, u64)> = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip .git directory contents but include dotfiles
        if name == ".git" {
            dirs.push(name);
            continue;
        }

        match entry.file_type() {
            Ok(ft) if ft.is_dir() => dirs.push(name),
            Ok(_) => {
                let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                files.push((name, size));
            }
            Err(_) => continue,
        }
    }

    dirs.sort_by_key(|a| a.to_lowercase());
    files.sort_by_key(|a| a.0.to_lowercase());

    let mut output = format!("{}/\n", dir_path.display());

    for name in &dirs {
        output.push_str(&format!("  {}/\n", name));
    }

    for (name, size) in &files {
        let size_str = format_file_size(*size);
        output.push_str(&format!("  {:<40} {}\n", name, size_str));
    }

    output
}

/// Format bytes into human-readable size.
fn format_file_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Get issue history for a file path
fn get_file_issue_history(orch: &Orchestrator, file_path: &str, mode: &IssueHistoryMode) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return String::new(),
    };

    // Normalize path - remove leading ./ or /
    let normalized_path = file_path.trim_start_matches("./").trim_start_matches('/');

    // Query file_changes joined with jobs -> issues to get issue info
    // We need: issue number, issue title, job date, PR number (if any)
    #[allow(clippy::type_complexity)]
    let results: Vec<(
        String,         // file_changes.status
        Option<i32>,    // file_changes.additions
        Option<i32>,    // file_changes.deletions
        i32,            // file_changes.created_at
        String,         // jobs.id
        Option<String>, // issues.id
        i32,            // issues.number
        String,         // issues.title
        String,         // projects.key
    )> = file_changes::table
        .inner_join(jobs::table.on(file_changes::job_id.eq(jobs::id)))
        .inner_join(issues::table.on(jobs::issue_id.eq(issues::id.nullable())))
        .inner_join(projects::table.on(issues::project_id.eq(projects::id)))
        .filter(file_changes::file_path.eq(normalized_path))
        .select((
            file_changes::status,
            file_changes::additions,
            file_changes::deletions,
            file_changes::created_at,
            jobs::id,
            issues::id.nullable(),
            issues::number,
            issues::title,
            projects::key,
        ))
        .order(file_changes::created_at.desc())
        .limit(20) // Limit to recent history
        .load(&mut *conn)
        .unwrap_or_default();

    if results.is_empty() {
        return String::new();
    }

    let mut output = String::from("## Issue History\n\n");

    match mode {
        IssueHistoryMode::Minimal => {
            for (
                status,
                _adds,
                _dels,
                created_at,
                _job_id,
                _issue_id,
                number,
                title,
                project_key,
            ) in &results
            {
                let date = chrono::DateTime::from_timestamp(*created_at as i64, 0)
                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());

                output.push_str(&format!(
                    "- {}-{} ({}): {} [{}]\n",
                    project_key, number, date, title, status
                ));
            }
        }
        IssueHistoryMode::Verbose => {
            for (
                status,
                additions,
                deletions,
                created_at,
                job_id,
                _issue_id,
                number,
                title,
                project_key,
            ) in &results
            {
                let date = chrono::DateTime::from_timestamp(*created_at as i64, 0)
                    .map(|dt| dt.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());

                output.push_str(&format!("### {}-{} - {}\n", project_key, number, title));
                output.push_str(&format!("- **Date:** {}\n", date));
                output.push_str(&format!("- **Status:** {}\n", status));

                if let (Some(a), Some(d)) = (additions, deletions) {
                    output.push_str(&format!("- **Changes:** +{} -{}\n", a, d));
                }

                // Try to get PR info for this job (job -> merge_requests)
                let pr_info: Option<(Option<i32>, Option<String>)> = merge_requests::table
                    .filter(merge_requests::job_id.eq(job_id))
                    .select((
                        merge_requests::github_pr_number,
                        merge_requests::github_pr_url,
                    ))
                    .first(&mut *conn)
                    .ok();

                if let Some((Some(pr_number), Some(pr_url))) = pr_info {
                    output.push_str(&format!("- **PR:** [#{}]({})\n", pr_number, pr_url));
                }

                output.push_str(&format!(
                    "- **URI:** cairn://{}/{}\n\n",
                    project_key, number
                ));
            }
        }
    }

    output
}
