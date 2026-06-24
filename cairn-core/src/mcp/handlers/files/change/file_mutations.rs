use super::types::{
    build_failure, mode_name, AppliedChange, ChangeFailure, CommitReport, IndexedChange,
    IndexedFailure, IndexedResult, TargetHash,
};
use crate::config::agents as config_agents;
use crate::mcp::diff::PatchEnvelopeFileChange;
use crate::mcp::git::{
    did_you_mean_block, normalize_change_target, path_escapes_worktree, resolve_change_target,
    GitAuthor,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use turso::params;

/// Rejection returned when a file-target change is attempted in a non-worktree
/// cwd (the project's live checkout). Changes can only happen in a worktree, so
/// the batch is refused BEFORE any edit is written and the checkout is never
/// touched. Resource writes (issues, messages, todos, tasks) are unaffected.
pub(super) const NON_WORKTREE_CHANGE_ERROR: &str =
    "Changes can only be made in a worktree. This agent runs on the project's live \
     checkout (no worktree); file edits are rejected here and the checkout is left \
     untouched. Resource writes (issues, messages, todos, tasks) still work.";

/// File-target keys parsed from a change item's `payload`. Mirrors how resource
/// handlers read structured keys from `item.payload`, so every change item is
/// uniformly `{target, mode, payload}` regardless of target family.
#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct FileChangePayload {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub diff: Option<String>,
    #[serde(default)]
    pub patch: Option<String>,
    #[serde(default)]
    pub old_string: Option<String>,
    #[serde(default)]
    pub new_string: Option<String>,
    #[serde(default)]
    pub replace_all: Option<bool>,
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

async fn resolve_project_id_for_cwd(orch: &Orchestrator, cwd: &str) -> Option<String> {
    let cwd = cwd.to_string();
    orch.db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.project_id
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN projects p ON j.project_id = p.id
                        WHERE r.status IN ('starting', 'live')
                          AND (j.worktree_path = ?1 OR (p.repo_path = ?1 AND j.issue_id IS NULL))
                        ORDER BY
                            CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                            r.created_at DESC
                        LIMIT 1
                        ",
                        params![cwd.as_str()],
                    )
                    .await?;

                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .ok()
        .flatten()
}

async fn resolve_git_author_for_cwd(orch: &Orchestrator, cwd: &str) -> Option<GitAuthor> {
    let project_id = resolve_project_id_for_cwd(orch, cwd).await;
    orch.resolve_git_identity_for_project(project_id.as_deref())
        .map(|(name, email)| GitAuthor::new(name, email))
}

async fn get_agent_commit_prefix_async(orch: &Orchestrator, cwd: &str) -> Result<String, String> {
    let cwd = cwd.to_string();
    let job_data = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.agent_config_id, j.project_id, p.repo_path
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN projects p ON j.project_id = p.id
                        WHERE r.status IN ('starting', 'live')
                          AND (j.worktree_path = ?1 OR (p.repo_path = ?1 AND j.issue_id IS NULL))
                        ORDER BY
                            CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                            r.created_at DESC
                        LIMIT 1
                        ",
                        (cwd.as_str(),),
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.text(1)?, row.text(2)?)))
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some((Some(agent_id), _project_id, repo_path)) = job_data else {
        return Ok(String::new());
    };

    match config_agents::get_agent(
        &orch.config_dir,
        &agent_id,
        Some(std::path::Path::new(&repo_path)),
    ) {
        Ok(Some(agent)) => Ok(format!("[{}] ", agent.name)),
        _ => Ok(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::changed_line_counts;

    #[test]
    fn line_counts_added_file() {
        assert_eq!(changed_line_counts(None, Some("one\ntwo\n")), (2, 0));
    }

    #[test]
    fn line_counts_deleted_file() {
        assert_eq!(changed_line_counts(Some("one\ntwo\n"), None), (0, 2));
    }

    #[test]
    fn line_counts_modified_file() {
        let before = "keep\nremove\nold\n";
        let after = "keep\nadd\nnew\n";
        assert_eq!(changed_line_counts(Some(before), Some(after)), (2, 2));
    }
}

async fn record_file_change_async(
    orch: &Orchestrator,
    cwd: &str,
    file_path: &str,
    status: &str,
    additions: i32,
    deletions: i32,
) -> Result<(), String> {
    let cwd = cwd.to_string();
    let file_path = file_path.to_string();
    let status = status.to_string();
    orch.db
        .local
        .write(|conn| {
            let cwd = cwd.clone();
            let file_path = file_path.clone();
            let status = status.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.id
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN projects p ON j.project_id = p.id
                        WHERE r.status IN ('starting', 'live')
                          AND (j.worktree_path = ?1 OR (p.repo_path = ?1 AND j.issue_id IS NULL))
                        ORDER BY
                            CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                            r.created_at DESC
                        LIMIT 1
                        ",
                        (cwd.as_str(),),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Ok(());
                };
                let job_id = row.text(0)?;
                let id = uuid::Uuid::new_v4().to_string();
                let now = chrono::Utc::now().timestamp() as i32;

                conn.execute(
                    "
                    INSERT INTO file_changes (
                        id, job_id, file_path, status, additions, deletions, previous_path, created_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7)
                    ",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        file_path.as_str(),
                        status.as_str(),
                        additions,
                        deletions,
                        now
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
}

fn changed_line_counts(before: Option<&str>, after: Option<&str>) -> (i32, i32) {
    let before_lines = before
        .map(|content| content.lines().collect::<Vec<_>>())
        .unwrap_or_default();
    let after_lines = after
        .map(|content| content.lines().collect::<Vec<_>>())
        .unwrap_or_default();

    let mut lcs_lengths = vec![vec![0usize; after_lines.len() + 1]; before_lines.len() + 1];
    for (before_index, before_line) in before_lines.iter().enumerate() {
        for (after_index, after_line) in after_lines.iter().enumerate() {
            lcs_lengths[before_index + 1][after_index + 1] = if before_line == after_line {
                lcs_lengths[before_index][after_index] + 1
            } else {
                lcs_lengths[before_index][after_index + 1]
                    .max(lcs_lengths[before_index + 1][after_index])
            };
        }
    }

    let unchanged = lcs_lengths[before_lines.len()][after_lines.len()];
    (
        after_lines.len().saturating_sub(unchanged) as i32,
        before_lines.len().saturating_sub(unchanged) as i32,
    )
}

/// A validated, ready-to-apply file change.
pub(super) enum PreparedChange {
    /// Write new content to this path (create or patch).
    Write {
        change_pos: usize,
        target_uri: String,
        repo_path: String,
        full_path: std::path::PathBuf,
        content: String,
        /// Whether this is a new file (didn't exist before this batch).
        is_new: bool,
        /// Whether the resolved path is outside the worktree (a fence-approved
        /// or `Fence::Allow` escaping write). Outside writes are applied to
        /// disk but excluded from git staging / `file_changes` recording, since
        /// they are not part of the worktree's branch.
        outside_worktree: bool,
    },
    /// Delete this file.
    Delete {
        change_pos: usize,
        target_uri: String,
        repo_path: String,
        full_path: std::path::PathBuf,
        outside_worktree: bool,
    },
}

impl PreparedChange {
    pub(super) fn change_pos(&self) -> usize {
        match self {
            PreparedChange::Write { change_pos, .. }
            | PreparedChange::Delete { change_pos, .. } => *change_pos,
        }
    }

    pub(super) fn target_uri(&self) -> &str {
        match self {
            PreparedChange::Write { target_uri, .. }
            | PreparedChange::Delete { target_uri, .. } => target_uri,
        }
    }
}

pub(super) type PreparedFileChanges = (Vec<PreparedChange>, Vec<String>);

pub(super) struct FileBatchSuccess {
    pub(super) applied: Vec<AppliedChange>,
    pub(super) affected_paths: Vec<String>,
    pub(super) recorded_changes: Vec<RecordFileChange>,
}

pub(super) struct RecordFileChange {
    path: String,
    status: &'static str,
    additions: i32,
    deletions: i32,
}

/// Build a helpful message when a literal (non-wildcard) `old_string` is not
/// found. Detects a malformed/unescaped wildcard marker and suggests collapsing
/// a shared-prefix/suffix edit with `~~*~~`.
pub(crate) fn literal_not_found_diagnostic(old: &str, new: &str) -> String {
    let mut msg = String::from("old_string not found; make sure the text matches exactly.");

    // A tilde run that isn't a valid `~~*~~` marker is almost always a typo.
    if old.contains("~~") && !old.contains(crate::mcp::wildcard::WILDCARD_TOKEN) {
        msg.push_str(
            "\nThat `~~` looks like a wildcard marker — use `~~*~~` to punch a hole between two anchors, or escape a literal with `\\~~*~~`.",
        );
    } else if let Some((prefix, suffix)) = shared_affixes(old, new) {
        msg.push_str(&format!(
            "\nold_string and new_string share a head (`{}`) and tail (`{}`) — you can collapse the differing middle with `~~*~~`, keeping just the shared head and tail as anchors.",
            truncate_for_hint(prefix),
            truncate_for_hint(suffix),
        ));
    }

    msg
}

/// Shorten a hint fragment so diagnostics stay readable.
fn truncate_for_hint(s: &str) -> String {
    const MAX: usize = 40;
    let first_line = s.lines().next().unwrap_or("");
    if first_line.chars().count() > MAX {
        let truncated: String = first_line.chars().take(MAX).collect();
        format!("{truncated}…")
    } else if first_line.len() < s.len() {
        format!("{first_line}…")
    } else {
        first_line.to_string()
    }
}

/// If `old` and `new` share a nontrivial common prefix and suffix with a
/// differing middle, return the shared prefix and suffix (a replace-the-middle
/// edit that a wildcard could express more robustly).
fn shared_affixes<'a>(old: &'a str, new: &'a str) -> Option<(&'a str, &'a str)> {
    if old == new {
        return None;
    }

    // Common prefix (char-aligned).
    let mut prefix_end = 0;
    for (a, b) in old.char_indices().zip(new.char_indices()) {
        let ((oi, oc), (_, nc)) = (a, b);
        if oc != nc {
            break;
        }
        prefix_end = oi + oc.len_utf8();
    }

    // Common suffix (char-aligned), not overlapping the prefix on either side.
    let mut suffix_len = 0;
    let old_rest = &old[prefix_end..];
    let new_rest = &new[prefix_end..];
    for (oc, nc) in old_rest.chars().rev().zip(new_rest.chars().rev()) {
        if oc != nc {
            break;
        }
        suffix_len += oc.len_utf8();
    }

    let prefix = &old[..prefix_end];
    let suffix = &old[old.len() - suffix_len..];

    // Require a meaningful shared head and tail, and an actually-differing middle.
    let nontrivial = prefix.trim().len() >= 4 && suffix.trim().len() >= 4;
    let differing_middle =
        prefix_end < old.len() - suffix_len || prefix_end < new.len() - suffix_len;
    if nontrivial && differing_middle {
        Some((prefix, suffix))
    } else {
        None
    }
}

#[cfg(test)]
pub(super) fn hash_file_target(
    worktree: &std::path::Path,
    item: &ChangeItem,
) -> Result<TargetHash, String> {
    hash_file_target_uri(worktree, &item.target)
}

pub(super) fn hash_file_target_uri(
    worktree: &std::path::Path,
    target: &str,
) -> Result<TargetHash, String> {
    // Preview hashing tolerates escaping targets (allow_escape = true) so a
    // dry-run reports an out-of-worktree crossing instead of rejecting it; the
    // fence is only raised on the real apply path.
    let normalized_target = normalize_change_target(target, true)?;
    let full_path = resolve_change_target(worktree, &normalized_target, true)
        .map_err(|e| format!("Invalid file target: {e}"))?;
    if full_path.exists() {
        let bytes = std::fs::read(&full_path)
            .map_err(|e| format!("Failed to read '{normalized_target}' for preview hash: {e}"))?;
        Ok(TargetHash {
            target: normalized_target,
            kind: "file".to_string(),
            exists: true,
            hash: sha256_hex(&bytes),
        })
    } else {
        Ok(TargetHash {
            target: normalized_target,
            kind: "file".to_string(),
            exists: false,
            hash: "missing".to_string(),
        })
    }
}

fn read_in_flight_or_disk(
    worktree: &std::path::Path,
    normalized_target: &str,
    full_path: &std::path::Path,
    in_flight: &std::collections::HashMap<String, Option<String>>,
    change: &IndexedChange<'_>,
    item: &ChangeItem,
) -> IndexedResult<String> {
    if let Some(content) = in_flight.get(normalized_target) {
        return content.clone().ok_or_else(|| {
            build_failure(
                change.index,
                item,
                format!(
                    "File does not exist{}",
                    did_you_mean_block(worktree, normalized_target)
                ),
            )
        });
    }

    if !full_path.exists() {
        return Err(build_failure(
            change.index,
            item,
            format!(
                "File does not exist{}",
                did_you_mean_block(worktree, normalized_target)
            ),
        ));
    }

    std::fs::read_to_string(full_path).map_err(|e| {
        build_failure(
            change.index,
            item,
            format!("Failed to read '{normalized_target}': {e}"),
        )
    })
}

fn envelope_path_to_target(path: &str) -> String {
    if path.starts_with("file:") {
        path.to_string()
    } else {
        format!("file:{path}")
    }
}

fn in_flight_is_new(
    full_path: &std::path::Path,
    normalized_target: &str,
    in_flight: &std::collections::HashMap<String, Option<String>>,
) -> bool {
    match in_flight.get(normalized_target) {
        Some(None) => true,
        Some(Some(_)) => false,
        None => !full_path.exists(),
    }
}

struct PreparedSinks<'a> {
    in_flight: &'a mut std::collections::HashMap<String, Option<String>>,
    prepared: &'a mut Vec<PreparedChange>,
    summaries: &'a mut Vec<String>,
}

struct UnifiedPatchContext<'a> {
    worktree: &'a std::path::Path,
    allow_escape: bool,
    change_pos: usize,
    change: &'a IndexedChange<'a>,
    item: &'a ChangeItem,
    carrier_target: &'a str,
    carrier_repo_path: &'a str,
}

fn prepare_unified_patch_change(
    ctx: &UnifiedPatchContext<'_>,
    envelope_change: PatchEnvelopeFileChange,
    sinks: &mut PreparedSinks<'_>,
) -> IndexedResult<()> {
    let envelope_path = match &envelope_change {
        PatchEnvelopeFileChange::Add { path, .. }
        | PatchEnvelopeFileChange::Update { path, .. }
        | PatchEnvelopeFileChange::Delete { path } => path,
    };
    let section_target = envelope_path_to_target(envelope_path);
    let normalized_target = normalize_change_target(&section_target, ctx.allow_escape)
        .map_err(|e| build_failure(ctx.change.index, ctx.item, e))?;

    if !ctx.carrier_repo_path.is_empty() && normalized_target != ctx.carrier_target {
        return Err(build_failure(
            ctx.change.index,
            ctx.item,
            format!(
                "envelope target path does not match change.target ('{}' != '{}')",
                normalized_target, ctx.carrier_target
            ),
        ));
    }

    let repo_path = normalized_target
        .strip_prefix("file:")
        .unwrap_or_default()
        .to_string();
    let full_path = resolve_change_target(ctx.worktree, &normalized_target, ctx.allow_escape)
        .map_err(|e| {
            build_failure(
                ctx.change.index,
                ctx.item,
                format!("Invalid file target: {e}"),
            )
        })?;
    let outside_worktree = path_escapes_worktree(ctx.worktree, &full_path);

    match envelope_change {
        PatchEnvelopeFileChange::Add { content, .. } => {
            let is_new = in_flight_is_new(&full_path, &normalized_target, sinks.in_flight);
            sinks
                .in_flight
                .insert(normalized_target.clone(), Some(content.clone()));
            sinks.prepared.push(PreparedChange::Write {
                change_pos: ctx.change_pos,
                target_uri: normalized_target.clone(),
                repo_path,
                full_path,
                content,
                is_new,
                outside_worktree,
            });
            sinks.summaries.push(format!("+{normalized_target}"));
        }
        PatchEnvelopeFileChange::Update { diff, .. } => {
            let content = read_in_flight_or_disk(
                ctx.worktree,
                &normalized_target,
                &full_path,
                sinks.in_flight,
                ctx.change,
                ctx.item,
            )?;
            let patched = crate::mcp::diff::apply_unified_diff(&content, &diff).map_err(|e| {
                build_failure(
                    ctx.change.index,
                    ctx.item,
                    format!("Failed to apply diff: {e}"),
                )
            })?;
            sinks
                .in_flight
                .insert(normalized_target.clone(), Some(patched.clone()));
            sinks.prepared.push(PreparedChange::Write {
                change_pos: ctx.change_pos,
                target_uri: normalized_target.clone(),
                repo_path,
                full_path,
                content: patched,
                is_new: false,
                outside_worktree,
            });
            sinks.summaries.push(format!("~{normalized_target}"));
        }
        PatchEnvelopeFileChange::Delete { .. } => {
            sinks.in_flight.insert(normalized_target.clone(), None);
            sinks.prepared.push(PreparedChange::Delete {
                change_pos: ctx.change_pos,
                target_uri: normalized_target.clone(),
                repo_path,
                full_path,
                outside_worktree,
            });
            sinks.summaries.push(format!("-{normalized_target}"));
        }
    }

    Ok(())
}

pub(super) fn prepare_file_changes(
    worktree: &std::path::Path,
    changes: &[IndexedChange<'_>],
    allow_escape: bool,
) -> IndexedResult<PreparedFileChanges> {
    let mut prepared: Vec<PreparedChange> = Vec::with_capacity(changes.len());
    let mut summaries: Vec<String> = Vec::with_capacity(changes.len());
    let mut in_flight: std::collections::HashMap<String, Option<String>> =
        std::collections::HashMap::new();

    for (change_pos, change) in changes.iter().enumerate() {
        let item = change.item;
        let normalized_target = normalize_change_target(&item.target, allow_escape)
            .map_err(|e| build_failure(change.index, item, e))?;
        let repo_path = normalized_target
            .strip_prefix("file:")
            .unwrap_or_default()
            .to_string();

        // Every change item is `{target, mode, payload}`; file-target keys ride
        // under `payload` just like resource-target keys. An empty payload parses
        // to all-None and the per-mode arms below reject what they require.
        let fp: FileChangePayload = match item.payload.as_ref() {
            Some(value) => serde_json::from_value(value.clone()).map_err(|e| {
                build_failure(change.index, item, format!("Invalid file payload: {e}"))
            })?,
            None => FileChangePayload::default(),
        };

        match item.mode {
            ChangeMode::Create | ChangeMode::Replace => {
                let content = fp.content.clone().ok_or_else(|| {
                    build_failure(
                        change.index,
                        item,
                        format!("mode={} requires content", mode_name(item.mode)),
                    )
                })?;
                let full_path = resolve_change_target(worktree, &normalized_target, allow_escape)
                    .map_err(|e| {
                    build_failure(change.index, item, format!("Invalid file target: {e}"))
                })?;
                let outside_worktree = path_escapes_worktree(worktree, &full_path);
                let is_new = in_flight_is_new(&full_path, &normalized_target, &in_flight);
                in_flight.insert(normalized_target.clone(), Some(content.clone()));
                prepared.push(PreparedChange::Write {
                    change_pos,
                    target_uri: normalized_target.clone(),
                    repo_path: repo_path.clone(),
                    full_path,
                    content,
                    is_new,
                    outside_worktree,
                });
                let marker = if item.mode == ChangeMode::Create {
                    "+"
                } else {
                    "~"
                };
                summaries.push(format!("{marker}{normalized_target}"));
            }
            ChangeMode::Append => {
                let appended = fp.content.clone().ok_or_else(|| {
                    build_failure(change.index, item, "mode=append requires content")
                })?;
                let full_path = resolve_change_target(worktree, &normalized_target, allow_escape)
                    .map_err(|e| {
                    build_failure(change.index, item, format!("Invalid file target: {e}"))
                })?;
                let outside_worktree = path_escapes_worktree(worktree, &full_path);
                let is_new = in_flight_is_new(&full_path, &normalized_target, &in_flight);
                let content = match read_in_flight_or_disk(
                    worktree,
                    &normalized_target,
                    &full_path,
                    &in_flight,
                    change,
                    item,
                ) {
                    Ok(existing) => format!("{existing}{appended}"),
                    Err(_) if is_new => appended,
                    Err(failure) => return Err(failure),
                };
                in_flight.insert(normalized_target.clone(), Some(content.clone()));
                prepared.push(PreparedChange::Write {
                    change_pos,
                    target_uri: normalized_target.clone(),
                    repo_path: repo_path.clone(),
                    full_path,
                    content,
                    is_new,
                    outside_worktree,
                });
                summaries.push(format!("~{normalized_target}"));
            }
            ChangeMode::Patch => {
                let full_path = resolve_change_target(worktree, &normalized_target, allow_escape)
                    .map_err(|e| {
                    build_failure(change.index, item, format!("Invalid file target: {e}"))
                })?;
                let outside_worktree = path_escapes_worktree(worktree, &full_path);

                let content = read_in_flight_or_disk(
                    worktree,
                    &normalized_target,
                    &full_path,
                    &in_flight,
                    change,
                    item,
                )?;

                let patched = if let Some(ref diff_text) = fp.diff {
                    let normalized_diff = crate::mcp::diff::normalize_single_file_patch(
                        diff_text,
                        &normalized_target,
                    )
                    .map_err(|e| build_failure(change.index, item, format!("Invalid diff: {e}")))?;
                    crate::mcp::diff::apply_unified_diff(&content, &normalized_diff).map_err(
                        |e| build_failure(change.index, item, format!("Failed to apply diff: {e}")),
                    )?
                } else if let (Some(ref old), Some(ref new)) = (&fp.old_string, &fp.new_string) {
                    if let Some(anchors) = crate::mcp::wildcard::parse_wildcard(old) {
                        crate::mcp::wildcard::apply_wildcard_edit(&content, &anchors, new)
                            .map(|(result, _)| result)
                            .map_err(|e| {
                                build_failure(
                                    change.index,
                                    item,
                                    format!("Wildcard edit failed: {e}"),
                                )
                            })?
                    } else {
                        // Not a wildcard edit: strip escaping backslashes so an
                        // escaped marker (`\\~~*~~`) targets a literal `~~*~~`.
                        let literal_old = crate::mcp::wildcard::unescape_literal(old);
                        if !content.contains(literal_old.as_str()) {
                            return Err(build_failure(
                                change.index,
                                item,
                                literal_not_found_diagnostic(&literal_old, new),
                            ));
                        } else if fp.replace_all.unwrap_or(false) {
                            content.replace(literal_old.as_str(), new.as_str())
                        } else {
                            content.replacen(literal_old.as_str(), new.as_str(), 1)
                        }
                    }
                } else {
                    return Err(build_failure(
                        change.index,
                        item,
                        "mode=patch requires diff or old_string+new_string",
                    ));
                };

                in_flight.insert(normalized_target.clone(), Some(patched.clone()));
                prepared.push(PreparedChange::Write {
                    change_pos,
                    target_uri: normalized_target.clone(),
                    repo_path: repo_path.clone(),
                    full_path,
                    content: patched,
                    is_new: false,
                    outside_worktree,
                });
                summaries.push(format!("~{normalized_target}"));
            }
            ChangeMode::UnifiedPatch => {
                let patch_text = fp.patch.as_ref().ok_or_else(|| {
                    build_failure(
                        change.index,
                        item,
                        "mode=unified_patch requires payload.patch",
                    )
                })?;
                let envelope_changes =
                    crate::mcp::diff::parse_patch_envelope(patch_text).map_err(|e| {
                        build_failure(change.index, item, format!("Invalid patch: {e}"))
                    })?;
                if !repo_path.is_empty() && envelope_changes.len() != 1 {
                    return Err(build_failure(
                        change.index,
                        item,
                        "target file:path can carry exactly one envelope file section; use target file: for multi-file envelopes",
                    ));
                }

                let ctx = UnifiedPatchContext {
                    worktree,
                    allow_escape,
                    change_pos,
                    change,
                    item,
                    carrier_target: &normalized_target,
                    carrier_repo_path: &repo_path,
                };
                let mut sinks = PreparedSinks {
                    in_flight: &mut in_flight,
                    prepared: &mut prepared,
                    summaries: &mut summaries,
                };
                for envelope_change in envelope_changes {
                    prepare_unified_patch_change(&ctx, envelope_change, &mut sinks)?;
                }
            }
            ChangeMode::Rename => {
                return Err(build_failure(
                    change.index,
                    item,
                    "mode=rename is resolved through the ast-grep rename engine and must not reach the file-batch path",
                ));
            }
            ChangeMode::Delete => {
                let full_path = resolve_change_target(worktree, &normalized_target, allow_escape)
                    .map_err(|e| {
                    build_failure(change.index, item, format!("Invalid file target: {e}"))
                })?;
                let outside_worktree = path_escapes_worktree(worktree, &full_path);
                in_flight.insert(normalized_target.clone(), None);
                prepared.push(PreparedChange::Delete {
                    change_pos,
                    target_uri: normalized_target.clone(),
                    repo_path: repo_path.clone(),
                    full_path,
                    outside_worktree,
                });
                summaries.push(format!("-{normalized_target}"));
            }
            _ => {
                return Err(build_failure(
                    change.index,
                    item,
                    format!(
                        "Unsupported file mode '{}'; expected create, append, patch, unified_patch, replace, or delete",
                        mode_name(item.mode)
                    ),
                ));
            }
        }
    }

    Ok((prepared, summaries))
}

pub(super) fn apply_file_batch(
    request: &McpCallbackRequest,
    changes: &[IndexedChange<'_>],
    allow_escape: bool,
) -> IndexedResult<FileBatchSuccess> {
    let worktree = std::path::Path::new(&request.cwd);
    let (prepared, summaries) = prepare_file_changes(worktree, changes, allow_escape)?;
    apply_prepared(changes, &prepared, &summaries)
}

/// Apply already-prepared file changes to disk: write or delete each path,
/// record the in-worktree changes for the `changed` resource, and build the
/// per-change `applied` report. Factored out of [`apply_file_batch`] so the
/// rename mode reuses the exact disk-application + recording step with synthetic
/// prepared changes computed from the ast-grep rename plan.
pub(super) fn apply_prepared(
    changes: &[IndexedChange<'_>],
    prepared: &[PreparedChange],
    summaries: &[String],
) -> IndexedResult<FileBatchSuccess> {
    let mut affected_paths: Vec<String> = Vec::with_capacity(prepared.len());
    let mut recorded_changes: Vec<RecordFileChange> = Vec::with_capacity(prepared.len());

    for change in prepared.iter() {
        match change {
            PreparedChange::Write {
                change_pos,
                target_uri,
                repo_path,
                full_path,
                content,
                is_new,
                outside_worktree,
            } => {
                let indexed_change = &changes[*change_pos];
                let previous_content = if !*outside_worktree && full_path.exists() {
                    Some(
                        std::fs::read(full_path)
                            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                            .map_err(|e| {
                                build_failure(
                                    indexed_change.index,
                                    indexed_change.item,
                                    format!("Failed to read '{target_uri}': {e}"),
                                )
                            })?,
                    )
                } else {
                    None
                };

                if let Some(parent) = full_path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            build_failure(
                                indexed_change.index,
                                indexed_change.item,
                                format!(
                                    "Failed to create parent directories for '{target_uri}': {e}"
                                ),
                            )
                        })?;
                    }
                }
                std::fs::write(full_path, content).map_err(|e| {
                    build_failure(
                        indexed_change.index,
                        indexed_change.item,
                        format!("Failed to write '{target_uri}': {e}"),
                    )
                })?;
                // Outside-worktree writes are applied to disk but never staged
                // or recorded: they are not part of the worktree's branch.
                if !*outside_worktree {
                    let (additions, deletions) =
                        changed_line_counts(previous_content.as_deref(), Some(content));
                    affected_paths.push(repo_path.clone());
                    recorded_changes.push(RecordFileChange {
                        path: repo_path.clone(),
                        status: if *is_new { "added" } else { "modified" },
                        additions,
                        deletions,
                    });
                }
            }
            PreparedChange::Delete {
                change_pos,
                target_uri,
                repo_path,
                full_path,
                outside_worktree,
            } => {
                let indexed_change = &changes[*change_pos];
                let previous_content = if !*outside_worktree && full_path.exists() {
                    Some(
                        std::fs::read(full_path)
                            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                            .map_err(|e| {
                                build_failure(
                                    indexed_change.index,
                                    indexed_change.item,
                                    format!("Failed to read '{target_uri}': {e}"),
                                )
                            })?,
                    )
                } else {
                    None
                };

                if full_path.exists() {
                    std::fs::remove_file(full_path).map_err(|e| {
                        build_failure(
                            indexed_change.index,
                            indexed_change.item,
                            format!("Failed to delete '{target_uri}': {e}"),
                        )
                    })?;
                }
                if !*outside_worktree {
                    let (additions, deletions) =
                        changed_line_counts(previous_content.as_deref(), None);
                    affected_paths.push(repo_path.clone());
                    recorded_changes.push(RecordFileChange {
                        path: repo_path.clone(),
                        status: "deleted",
                        additions,
                        deletions,
                    });
                }
            }
        }
    }

    let applied = prepared
        .iter()
        .zip(summaries.iter())
        .map(|(prepared_change, summary)| {
            let change = &changes[prepared_change.change_pos()];
            AppliedChange {
                index: change.index,
                target: match prepared_change {
                    PreparedChange::Write { target_uri, .. }
                    | PreparedChange::Delete { target_uri, .. } => target_uri.clone(),
                },
                mode: mode_name(change.item.mode).to_string(),
                kind: "file".to_string(),
                summary: summary.clone(),
                data: None,
            }
        })
        .collect::<Vec<_>>();

    Ok(FileBatchSuccess {
        applied,
        affected_paths,
        recorded_changes,
    })
}

pub(super) fn emit_worktree_changed(orch: &Orchestrator, cwd: &str) {
    let _ = orch.services.emitter.emit(
        "worktree-changed",
        serde_json::json!({"worktree_path": cwd}),
    );
}

/// Parse a `mode:"rename"` item's payload into the route file, the symbol
/// locator, and the new name. The payload contract is enforced here — mirroring
/// how file modes validate their payload at apply time — `new_name` is required
/// and exactly one of `old_name` | `symbol_at` must be present.
pub(super) fn parse_rename_spec(
    worktree: &std::path::Path,
    item: &ChangeItem,
) -> Result<
    (
        std::path::PathBuf,
        crate::symbols::rename::RenameSpec,
        String,
    ),
    String,
> {
    #[derive(Deserialize)]
    struct RenamePayload {
        #[serde(default)]
        new_name: Option<String>,
        #[serde(default)]
        old_name: Option<String>,
        #[serde(default)]
        symbol_at: Option<String>,
    }
    let payload: RenamePayload = match item.payload.as_ref() {
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|e| format!("Invalid rename payload: {e}"))?,
        None => {
            return Err(
                "mode=rename requires payload {new_name, and one of old_name | symbol_at}"
                    .to_string(),
            )
        }
    };
    let new_name = payload
        .new_name
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "mode=rename requires new_name".to_string())?;

    match (payload.old_name, payload.symbol_at) {
        (Some(_), Some(_)) => {
            Err("mode=rename: provide exactly one of old_name or symbol_at, not both".to_string())
        }
        (None, None) => {
            Err("mode=rename requires exactly one of old_name or symbol_at".to_string())
        }
        (Some(old_name), None) => {
            if old_name.is_empty() {
                return Err("mode=rename: old_name is empty".to_string());
            }
            let normalized = normalize_change_target(&item.target, false)?;
            let full_path = resolve_change_target(worktree, &normalized, false)
                .map_err(|e| format!("Invalid file target: {e}"))?;
            Ok((
                full_path,
                crate::symbols::rename::RenameSpec::Name(old_name),
                new_name,
            ))
        }
        (None, Some(symbol_at)) => {
            let (path, position) = crate::symbols::rename::parse_at(&symbol_at, worktree)?;
            Ok((
                path.clone(),
                crate::symbols::rename::RenameSpec::At(path, position),
                new_name,
            ))
        }
    }
}

/// Build synthetic prepared changes for a rename plan plus the per-file `applied`
/// report. A move yields a write at the destination and a delete of the old path
/// (two prepared changes) but a single rename `applied` entry, so the returned
/// `applied` list is authoritative and the caller overrides whatever
/// [`apply_prepared`] derives from the prepared/summaries zip. Every prepared
/// change references index 0 of the single-item synthetic slice the rename
/// branch passes to [`apply_prepared`].
pub(super) fn prepare_rename_changes(
    worktree: &std::path::Path,
    rename_index: usize,
    plan: &crate::symbols::rename::RenamePlan,
) -> (Vec<PreparedChange>, Vec<AppliedChange>, Vec<String>) {
    let mut prepared: Vec<PreparedChange> = Vec::new();
    let mut applied: Vec<AppliedChange> = Vec::new();
    let mut summaries: Vec<String> = Vec::new();

    let rel = |path: &std::path::Path| -> String {
        path.strip_prefix(worktree)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/")
    };
    let rename_applied = |index: usize, target: String, summary: String| AppliedChange {
        index,
        target,
        mode: "rename".to_string(),
        kind: "file".to_string(),
        summary,
        data: None,
    };

    for edit in &plan.file_edits {
        let src_rel = rel(&edit.worktree_path);
        let src_target = format!("file:{src_rel}");
        match (&edit.new_content, &edit.move_to) {
            (Some(content), Some(dest)) => {
                let dest_rel = rel(dest);
                let dest_target = format!("file:{dest_rel}");
                prepared.push(PreparedChange::Write {
                    change_pos: 0,
                    target_uri: dest_target.clone(),
                    repo_path: dest_rel,
                    full_path: dest.clone(),
                    content: content.clone(),
                    is_new: true,
                    outside_worktree: false,
                });
                summaries.push(format!("R {src_target}\u{2192}{dest_target}"));
                prepared.push(PreparedChange::Delete {
                    change_pos: 0,
                    target_uri: src_target.clone(),
                    repo_path: src_rel,
                    full_path: edit.worktree_path.clone(),
                    outside_worktree: false,
                });
                summaries.push(format!("-{src_target}"));
                applied.push(rename_applied(
                    rename_index,
                    dest_target.clone(),
                    format!(
                        "R {src_target}\u{2192}{dest_target} ({} sites)",
                        edit.site_count
                    ),
                ));
            }
            (Some(content), None) => {
                prepared.push(PreparedChange::Write {
                    change_pos: 0,
                    target_uri: src_target.clone(),
                    repo_path: src_rel,
                    full_path: edit.worktree_path.clone(),
                    content: content.clone(),
                    is_new: false,
                    outside_worktree: false,
                });
                summaries.push(format!("~{src_target}"));
                applied.push(rename_applied(
                    rename_index,
                    src_target.clone(),
                    format!("~{src_target} ({} sites)", edit.site_count),
                ));
            }
            (None, _) => {
                prepared.push(PreparedChange::Delete {
                    change_pos: 0,
                    target_uri: src_target.clone(),
                    repo_path: src_rel,
                    full_path: edit.worktree_path.clone(),
                    outside_worktree: false,
                });
                summaries.push(format!("-{src_target}"));
                applied.push(rename_applied(
                    rename_index,
                    src_target.clone(),
                    format!("-{src_target}"),
                ));
            }
        }
    }

    (prepared, applied, summaries)
}

pub(super) async fn finalize_file_commit(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    commit_msg: Option<&str>,
    affected_paths: &[String],
    recorded_changes: &[RecordFileChange],
    first_change: &IndexedChange<'_>,
    promoted_memory_uris: &[String],
) -> IndexedResult<Option<CommitReport>> {
    if affected_paths.is_empty() {
        return Ok(None);
    }

    // Record the file changes for this job as soon as they're applied — NOT only
    // when committed. The `changed` resource (`cairn://.../changed`) reads the
    // `file_changes` table, and a gated artifact is reviewed *before* its node
    // commits/PRs, so uncommitted worktree edits must already be recorded or the
    // review loop has no diff to show.
    for change in recorded_changes {
        if let Err(e) = record_file_change_async(
            orch,
            &request.cwd,
            &change.path,
            change.status,
            change.additions,
            change.deletions,
        )
        .await
        {
            log::warn!("Failed to record file change: {}", e);
        }
    }

    // Route worktree mutations through the VCS seam (jj for a worktree). A
    // non-worktree cwd is rejected up front in `handle_change`, so this path
    // always sees a jj workspace.
    let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, std::path::Path::new(&request.cwd));

    let Some(commit_msg) = commit_msg else {
        // The file edits are already on disk. With no commit_msg, restore the
        // worktree to HEAD (preserving the worktree==HEAD invariant the
        // session-archival scheme depends on) and tell the agent to pass one.
        let restore = vcs.discard(std::path::Path::new(&request.cwd));
        emit_worktree_changed(orch, &request.cwd);
        let error = match restore {
            Ok(()) => "File edits require a descriptive commit_msg; the worktree was \
                       restored to HEAD. Pass commit_msg so the edits are committed."
                .to_string(),
            Err(re) => format!(
                "File edits require a descriptive commit_msg, and restoring the \
                 worktree to HEAD failed: {re}"
            ),
        };
        return Err(Box::new(IndexedFailure {
            failure: ChangeFailure {
                index: first_change.index,
                target: first_change.item.target.clone(),
                mode: mode_name(first_change.item.mode).to_string(),
                kind: "file".to_string(),
                error: error.clone(),
            },
            commit: Some(CommitReport {
                status: "failed".to_string(),
                sha: None,
                pr_number: None,
                message: Some(error),
            }),
        }));
    };

    if commit_msg == "^" && !promoted_memory_uris.is_empty() {
        return Err(Box::new(IndexedFailure {
            failure: ChangeFailure {
                index: first_change.index,
                target: first_change.item.target.clone(),
                mode: mode_name(first_change.item.mode).to_string(),
                kind: "file".to_string(),
                error: "promote_memory cannot ride an amend; make a new canon commit".to_string(),
            },
            commit: Some(CommitReport {
                status: "failed".to_string(),
                sha: None,
                pr_number: None,
                message: Some(
                    "promote_memory cannot ride an amend; make a new canon commit".to_string(),
                ),
            }),
        }));
    }

    let agent_prefix = get_agent_commit_prefix_async(orch, &request.cwd)
        .await
        .unwrap_or_default();
    let final_commit_msg = if commit_msg == "^" {
        "^".to_string()
    } else if promoted_memory_uris.is_empty() {
        format!("{}{}", agent_prefix, commit_msg)
    } else {
        let provenance = promoted_memory_uris.join("\n");
        format!(
            "{}{}\n\n{}",
            agent_prefix,
            commit_msg.trim_end(),
            provenance
        )
    };

    let author = resolve_git_author_for_cwd(orch, &request.cwd).await;

    let mut unique_paths: Vec<&str> = Vec::new();
    let mut seen_paths = std::collections::HashSet::new();
    for path in affected_paths {
        if seen_paths.insert(path.as_str()) {
            unique_paths.push(path.as_str());
        }
    }

    match vcs.seal_files(
        std::path::Path::new(&request.cwd),
        &unique_paths,
        &final_commit_msg,
        author.as_ref(),
    ) {
        Ok(result) => {
            // File changes were already recorded above (on apply); committing
            // does not re-record them.
            emit_worktree_changed(orch, &request.cwd);
            Ok(Some(CommitReport {
                status: if commit_msg == "^" {
                    "amended"
                } else {
                    "committed"
                }
                .to_string(),
                sha: Some(result.sha),
                pr_number: result.pr_number,
                message: None,
            }))
        }
        Err(e) => {
            // The edits were applied but the commit failed. Restore the worktree
            // to HEAD so a failed write does not strand uncommitted dirt.
            let restore = vcs.discard(std::path::Path::new(&request.cwd));
            emit_worktree_changed(orch, &request.cwd);
            let error = match restore {
                Ok(()) => format!(
                    "Applied file changes but commit failed: {e}; the worktree was restored to HEAD."
                ),
                Err(re) => format!(
                    "Applied file changes but commit failed: {e}; additionally failed to restore the worktree to HEAD: {re}"
                ),
            };
            Err(Box::new(IndexedFailure {
                failure: ChangeFailure {
                    index: first_change.index,
                    target: first_change.item.target.clone(),
                    mode: mode_name(first_change.item.mode).to_string(),
                    kind: "file".to_string(),
                    error: error.clone(),
                },
                commit: Some(CommitReport {
                    status: "failed".to_string(),
                    sha: None,
                    pr_number: None,
                    message: Some(error),
                }),
            }))
        }
    }
}
