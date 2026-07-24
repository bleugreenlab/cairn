use super::types::{
    build_failure, mode_name, AppliedChange, ChangeFailure, CommitReport, IndexedChange,
    IndexedFailure, IndexedResult, TargetHash,
};
use crate::config::agents as config_agents;
use crate::mcp::diff::PatchEnvelopeFileChange;
use crate::mcp::file_targets::{
    did_you_mean_block, normalize_change_target, path_escapes_worktree, resolve_change_target,
};
use crate::mcp::git::GitAuthor;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use cairn_db::turso::params;
use serde::Deserialize;
use sha2::{Digest, Sha256};

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

pub(super) fn logical_paths_for_changes(
    worktree: &std::path::Path,
    changes: &[IndexedChange<'_>],
    allow_escape: bool,
) -> Result<Vec<String>, String> {
    let mut paths = std::collections::BTreeSet::new();
    for change in changes {
        let item = change.item;
        if item.mode == ChangeMode::UnifiedPatch {
            let payload: FileChangePayload = item
                .payload
                .clone()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            let patch = payload
                .patch
                .as_deref()
                .ok_or_else(|| "mode=unified_patch requires payload.patch".to_string())?;
            for section in crate::mcp::diff::parse_patch_envelope(patch)? {
                let path = match section {
                    PatchEnvelopeFileChange::Add { path, .. }
                    | PatchEnvelopeFileChange::Update { path, .. }
                    | PatchEnvelopeFileChange::Delete { path } => path,
                };
                paths.insert(path.trim_start_matches('/').to_string());
            }
            continue;
        }
        let target = normalize_change_target(&item.target, allow_escape)?;
        let full = resolve_change_target(worktree, &target, allow_escape)?;
        if !path_escapes_worktree(worktree, &full) {
            paths.insert(target.strip_prefix("file:").unwrap_or_default().to_string());
        }
    }
    Ok(paths.into_iter().collect())
}

pub(super) fn apply_logical_file_batch(
    request: &McpCallbackRequest,
    changes: &[IndexedChange<'_>],
    allow_escape: bool,
    snapshot: &std::collections::HashMap<String, Option<String>>,
) -> IndexedResult<FileBatchSuccess> {
    let worktree = std::path::Path::new(&request.cwd);
    let (prepared, summaries) =
        prepare_file_changes_with_snapshot(worktree, changes, allow_escape, snapshot.clone())?;
    apply_prepared_logical(changes, &prepared, &summaries, snapshot)
}

pub(super) fn apply_prepared_logical(
    changes: &[IndexedChange<'_>],
    prepared: &[PreparedChange],
    summaries: &[String],
    snapshot: &std::collections::HashMap<String, Option<String>>,
) -> IndexedResult<FileBatchSuccess> {
    let mut affected_paths = Vec::new();
    let mut recorded_changes = Vec::new();
    let mut logical_mutations = Vec::new();
    for prepared_change in prepared {
        match prepared_change {
            PreparedChange::Write {
                change_pos,
                target_uri,
                repo_path,
                full_path,
                content,
                is_new,
                outside_worktree,
            } => {
                let indexed = &changes[*change_pos];
                if *outside_worktree {
                    if let Some(parent) = full_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|error| build_failure(indexed.index, indexed.item, format!("Failed to create parent directories for '{target_uri}': {error}")))?;
                    }
                    std::fs::write(full_path, content).map_err(|error| {
                        build_failure(
                            indexed.index,
                            indexed.item,
                            format!("Failed to write '{target_uri}': {error}"),
                        )
                    })?;
                } else {
                    let before = snapshot.get(target_uri).and_then(|value| value.as_deref());
                    let (additions, deletions) = changed_line_counts(before, Some(content));
                    affected_paths.push(repo_path.clone());
                    recorded_changes.push(RecordFileChange {
                        path: repo_path.clone(),
                        status: if *is_new { "added" } else { "modified" },
                        additions,
                        deletions,
                    });
                    logical_mutations.push(cairn_vcs::LogicalTreeMutation {
                        path: repo_path.clone(),
                        content: Some(content.as_bytes().to_vec()),
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
                let indexed = &changes[*change_pos];
                if *outside_worktree {
                    if full_path.exists() {
                        std::fs::remove_file(full_path).map_err(|error| {
                            build_failure(
                                indexed.index,
                                indexed.item,
                                format!("Failed to delete '{target_uri}': {error}"),
                            )
                        })?;
                    }
                } else {
                    let before = snapshot.get(target_uri).and_then(|value| value.as_deref());
                    let (additions, deletions) = changed_line_counts(before, None);
                    affected_paths.push(repo_path.clone());
                    recorded_changes.push(RecordFileChange {
                        path: repo_path.clone(),
                        status: "deleted",
                        additions,
                        deletions,
                    });
                    logical_mutations.push(cairn_vcs::LogicalTreeMutation {
                        path: repo_path.clone(),
                        content: None,
                    });
                }
            }
        }
    }
    let applied = prepared
        .iter()
        .zip(summaries)
        .map(|(prepared_change, summary)| {
            let change = &changes[prepared_change.change_pos()];
            AppliedChange {
                index: change.index,
                target: prepared_change.target_uri().to_string(),
                mode: mode_name(change.item.mode).to_string(),
                kind: "file".to_string(),
                summary: summary.clone(),
                data: None,
            }
        })
        .collect();
    Ok(FileBatchSuccess {
        applied,
        affected_paths,
        recorded_changes,
        logical_mutations,
    })
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

pub(crate) async fn record_file_change_async(
    orch: &Orchestrator,
    cwd: &str,
    file_path: &str,
    status: &str,
    additions: i32,
    deletions: i32,
    previous_path: Option<&str>,
) -> Result<(), String> {
    let cwd = cwd.to_string();
    let file_path = file_path.to_string();
    let status = status.to_string();
    let previous_path = previous_path.map(str::to_string);
    orch.db
        .local
        .write(|conn| {
            let cwd = cwd.clone();
            let file_path = file_path.clone();
            let status = status.clone();
            let previous_path = previous_path.clone();
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
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                    ",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        file_path.as_str(),
                        status.as_str(),
                        additions,
                        deletions,
                        previous_path.as_deref(),
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

pub(crate) fn changed_line_counts(before: Option<&str>, after: Option<&str>) -> (i32, i32) {
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
    pub(super) logical_mutations: Vec<cairn_vcs::LogicalTreeMutation>,
}

pub(super) struct PostSealPublication {
    pub(super) db: std::sync::Arc<crate::storage::LocalDb>,
    pub(super) project_id: String,
    pub(super) repository: std::path::PathBuf,
}

pub(super) struct CompletedCommit {
    pub(super) report: Option<CommitReport>,
    pub(super) publication_requirement: crate::merge_requests::queries::PublicationRequirement,
    pub(super) publication: Option<PostSealPublication>,
}

pub(super) enum CommitOutcome {
    Done(CompletedCommit),
}

pub(super) struct RecordFileChange {
    pub(super) path: String,
    pub(super) status: &'static str,
    pub(super) additions: i32,
    pub(super) deletions: i32,
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

const MAX_AMBIGUOUS_MATCH_EXCERPTS: usize = 10;
const AMBIGUOUS_MATCH_CONTEXT_LINES: usize = 2;
const AMBIGUOUS_MATCH_CONTINUATION_LINES: usize = 40;
const MAX_AMBIGUOUS_EXCERPT_LINES: usize = 8;
const MAX_DIAGNOSTIC_LINE_CHARS: usize = 180;

/// Build a message when a literal (non-wildcard) `old_string` matches more than
/// one site and `replace_all` was not set. Editing the first match silently
/// would let the caller believe they edited the unique site they meant, so this
/// remains an explicit refusal. The matching excerpts make the safe follow-up
/// edit possible without a separate discovery read.
fn non_unique_match_diagnostic(
    target: &str,
    content: &str,
    old_string: &str,
    count: usize,
    disk_matches_snapshot: bool,
) -> String {
    let path = target.strip_prefix("file:").unwrap_or(target);
    let line_starts = line_starts(content);
    let lines: Vec<&str> = content.split('\n').collect();
    let locations: Vec<usize> = content
        .match_indices(old_string)
        .take(MAX_AMBIGUOUS_MATCH_EXCERPTS + 1)
        .map(|(offset, _)| offset)
        .collect();
    let shown = locations.len().min(MAX_AMBIGUOUS_MATCH_EXCERPTS);

    let mut message = format!(
        "old_string matched {count} sites in {path}; refusing to edit just the first one. \
         Add surrounding context so old_string uniquely identifies the one site you mean, \
         or pass replace_all:true to rewrite all {count} matches.\n\nMatching locations \
         (showing {shown} of {count}):"
    );

    for (index, &match_start) in locations.iter().take(shown).enumerate() {
        let start_line = line_index_for_byte(&line_starts, match_start, content.len());
        let end_probe = old_string
            .char_indices()
            .last()
            .map(|(offset, _)| match_start + offset)
            .unwrap_or(match_start);
        let end_line = line_index_for_byte(&line_starts, end_probe, content.len());
        let start_column = char_column(&line_starts, content, start_line, match_start);
        let end_column = char_column(&line_starts, content, end_line, end_probe);
        let excerpt_start = start_line.saturating_sub(AMBIGUOUS_MATCH_CONTEXT_LINES);
        let excerpt_end = (end_line + AMBIGUOUS_MATCH_CONTEXT_LINES + 1).min(lines.len());
        let location = if start_line == end_line {
            format!("{}:{start_column}-{end_column}", start_line + 1)
        } else {
            format!(
                "{}:{start_column}-{}:{end_column}",
                start_line + 1,
                end_line + 1
            )
        };

        message.push_str(&format!("\n\nMatch {} — {path}:{location}\n", index + 1));
        let excerpt_lines = bounded_excerpt_lines(excerpt_start, excerpt_end);
        let mut previous_line = None;
        for line_index in excerpt_lines {
            if let Some(previous) = previous_line {
                if line_index > previous + 1 {
                    message.push_str(&format!(
                        "  ..... | … {} lines omitted …\n",
                        line_index - previous - 1
                    ));
                }
            }
            let marker = if (start_line..=end_line).contains(&line_index) {
                '>'
            } else {
                ' '
            };
            let focus = match line_index {
                line if line == start_line && line == end_line => {
                    Some((start_column - 1, end_column))
                }
                line if line == start_line => Some((start_column - 1, lines[line].chars().count())),
                line if line == end_line => Some((0, end_column)),
                line if (start_line..=end_line).contains(&line) => {
                    Some((0, lines[line].chars().count()))
                }
                _ => None,
            };
            let rendered = diagnostic_line_excerpt(
                lines[line_index],
                focus,
                start_line == end_line && line_index == start_line,
            );
            message.push_str(&format!(
                "{marker} {:>5} | {}\n",
                line_index + 1,
                rendered.text
            ));
            if let Some(focus_marker) = rendered.focus_marker {
                message.push_str(&format!("        | {focus_marker}\n"));
            }
            previous_line = Some(line_index);
        }
        message.pop();
    }

    if count > shown {
        if disk_matches_snapshot {
            let next_match_start = locations[shown];
            let next_line = line_index_for_byte(&line_starts, next_match_start, content.len());
            let offset = next_line.saturating_sub(AMBIGUOUS_MATCH_CONTEXT_LINES);
            message.push_str(&format!(
                "\n\n{} more matches omitted. Continue at the next omitted match with:\n\
                 `{target}?offset={offset}&limit={AMBIGUOUS_MATCH_CONTINUATION_LINES}`",
                count - shown
            ));
        } else {
            message.push_str(&format!(
                "\n\n{} more matches omitted. These locations exist only in the rejected batch's \
                 in-flight snapshot, so a file read URI would be stale. Apply the preceding changes \
                 in a separate write, then retry this patch to receive a continuation URI for the \
                 committed file.",
                count - shown
            ));
        }
    }

    message
}

fn line_starts(content: &str) -> Vec<usize> {
    std::iter::once(0)
        .chain(content.match_indices('\n').map(|(index, _)| index + 1))
        .collect()
}

fn line_index_for_byte(line_starts: &[usize], byte_offset: usize, content_len: usize) -> usize {
    let offset = byte_offset.min(content_len);
    line_starts
        .partition_point(|&line_start| line_start <= offset)
        .saturating_sub(1)
}

fn char_column(
    line_starts: &[usize],
    content: &str,
    line_index: usize,
    byte_offset: usize,
) -> usize {
    content[line_starts[line_index]..byte_offset]
        .chars()
        .count()
        + 1
}

fn bounded_excerpt_lines(start: usize, end: usize) -> Vec<usize> {
    let count = end.saturating_sub(start);
    if count <= MAX_AMBIGUOUS_EXCERPT_LINES {
        return (start..end).collect();
    }

    let head = MAX_AMBIGUOUS_EXCERPT_LINES / 2;
    let tail = MAX_AMBIGUOUS_EXCERPT_LINES - head;
    (start..start + head).chain(end - tail..end).collect()
}

struct DiagnosticLineExcerpt {
    text: String,
    focus_marker: Option<String>,
}

fn diagnostic_line_excerpt(
    line: &str,
    focus: Option<(usize, usize)>,
    show_focus_marker: bool,
) -> DiagnosticLineExcerpt {
    let chars: Vec<char> = line.chars().collect();
    let mut start = 0;
    let mut end = chars.len();
    if chars.len() > MAX_DIAGNOSTIC_LINE_CHARS {
        start = focus
            .map(|(focus_start, focus_end)| {
                let midpoint = focus_start.saturating_add(focus_end).saturating_div(2);
                midpoint.saturating_sub(MAX_DIAGNOSTIC_LINE_CHARS / 2)
            })
            .unwrap_or(0);
        start = start.min(chars.len() - MAX_DIAGNOSTIC_LINE_CHARS);
        if let Some((_, focus_end)) = focus {
            start = start.max(focus_end.saturating_sub(MAX_DIAGNOSTIC_LINE_CHARS));
        }
        end = (start + MAX_DIAGNOSTIC_LINE_CHARS).min(chars.len());
    }

    let has_prefix_ellipsis = start > 0;
    let mut text = chars[start..end].iter().collect::<String>();
    if has_prefix_ellipsis {
        text.insert(0, '…');
    }
    if end < chars.len() {
        text.push('…');
    }

    let focus_marker = if show_focus_marker {
        focus.and_then(|(focus_start, focus_end)| {
            let visible_start = focus_start.clamp(start, end);
            let visible_end = focus_end.clamp(start, end);
            (visible_end > visible_start).then(|| {
                let marker_start = visible_start - start + usize::from(has_prefix_ellipsis);
                let visible_len = visible_end - visible_start;
                let marker_len = visible_len.min(20);
                let mut marker = format!("{}{}", " ".repeat(marker_start), "^".repeat(marker_len));
                if visible_len > marker_len {
                    marker.push('…');
                }
                marker
            })
        })
    } else {
        None
    };

    DiagnosticLineExcerpt { text, focus_marker }
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
    prepare_file_changes_with_snapshot(
        worktree,
        changes,
        allow_escape,
        std::collections::HashMap::new(),
    )
}

pub(super) fn prepare_file_changes_with_snapshot(
    worktree: &std::path::Path,
    changes: &[IndexedChange<'_>],
    allow_escape: bool,
    mut in_flight: std::collections::HashMap<String, Option<String>>,
) -> IndexedResult<PreparedFileChanges> {
    let mut prepared: Vec<PreparedChange> = Vec::with_capacity(changes.len());
    let mut summaries: Vec<String> = Vec::with_capacity(changes.len());

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
                        let replace_all = fp.replace_all.unwrap_or(false);
                        // Count occurrences against the in-flight working content the
                        // patch operates on, so an earlier item in the same batch that
                        // made old_string non-unique surfaces a clear count error
                        // instead of silently editing the first match.
                        let matches = content.matches(literal_old.as_str()).count();
                        if matches == 0 {
                            return Err(build_failure(
                                change.index,
                                item,
                                literal_not_found_diagnostic(&literal_old, new),
                            ));
                        } else if matches > 1 && !replace_all {
                            return Err(build_failure(
                                change.index,
                                item,
                                non_unique_match_diagnostic(
                                    &normalized_target,
                                    &content,
                                    &literal_old,
                                    matches,
                                    std::fs::read_to_string(&full_path)
                                        .is_ok_and(|disk_content| disk_content == content),
                                ),
                            ));
                        } else if replace_all {
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

pub(crate) fn emit_worktree_changed(orch: &Orchestrator, cwd: &str) {
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_file_commit(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    commit_msg: Option<&str>,
    affected_paths: &[String],
    _recorded_changes: &[RecordFileChange],
    first_change: &IndexedChange<'_>,
    promoted_memory_uris: &[String],
    logical_resolution: &super::super::branch::BranchResolution,
    logical_mutations: &[cairn_vcs::LogicalTreeMutation],
) -> IndexedResult<CommitOutcome> {
    if affected_paths.is_empty() {
        return Ok(CommitOutcome::Done(CompletedCommit {
            report: None,
            publication_requirement:
                crate::merge_requests::queries::PublicationRequirement::DeferredUntilPublication,
            publication: None,
        }));
    }

    // Route worktree mutations through the VCS seam (jj for a worktree). A
    // non-worktree cwd is rejected up front in `handle_write`, so this path
    // always sees a jj workspace.
    let (managed_context, routed_db) =
        match super::super::run_context::lookup_run_routed(&orch.db, request).await {
            Ok((run, db)) => {
                let context =
                    crate::execution::jobs::workspace_identity::resolve_managed_workspace_context(
                        db.clone(),
                        run.job_id,
                    )
                    .await
                    .ok()
                    .flatten();
                (context, Some(db))
            }
            Err(_) => (None, None),
        };
    let Some(commit_msg) = commit_msg else {
        let error = "File edits require a descriptive commit_msg; no project tree changes were published. Pass commit_msg so the proposed tree can be committed.".to_string();
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

    let publication_mode = if commit_msg == "^" {
        cairn_vcs::PublicationMode::Amend
    } else {
        cairn_vcs::PublicationMode::Child {
            description: final_commit_msg,
            author: author.map(|author| cairn_vcs::PublicationAuthor {
                name: author.name,
                email: author.email,
            }),
        }
    };
    let repository_path = logical_resolution.repository_path.clone();
    let branch = logical_resolution.rev.clone();
    let expected_head = logical_resolution.commit_id.clone();
    let mutations = logical_mutations.to_vec();
    let publication = tokio::task::spawn_blocking(move || {
        cairn_vcs::publish_logical_mutations(
            &repository_path,
            &branch,
            &expected_head,
            mutations,
            publication_mode,
        )
    })
    .await
    .map_err(|error| {
        Box::new(IndexedFailure {
            failure: ChangeFailure {
                index: first_change.index,
                target: first_change.item.target.clone(),
                mode: mode_name(first_change.item.mode).to_string(),
                kind: "file".to_string(),
                error: format!("Logical-head publication worker failed: {error}"),
            },
            commit: None,
        })
    })?;
    match publication {
        Ok(result) => {
            // File changes were already recorded above (on apply); committing
            // does not re-record them.
            emit_worktree_changed(orch, &request.cwd);
            let publication = match (&managed_context, &routed_db) {
                (Some(context), Some(db)) => Some(PostSealPublication {
                    db: db.clone(),
                    project_id: context.identity.project_id.clone(),
                    repository: context.identity.project_root.clone(),
                }),
                _ => None,
            };
            let publication_requirement = match (&managed_context, &routed_db) {
                (Some(context), Some(db)) => {
                    crate::merge_requests::queries::publication_requirement_for_managed_branch(
                        db,
                        &context.current_job_id,
                        &context.identity.project_id,
                        &context.identity.branch,
                    )
                    .await
                }
                // An unmanaged or ambiguously routed sealed workspace cannot be
                // proven to have no open PR, so preserve the fail-closed behavior.
                _ => crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr,
            };
            Ok(CommitOutcome::Done(CompletedCommit {
                report: Some(CommitReport {
                    status: if commit_msg == "^" {
                        "amended"
                    } else {
                        "committed"
                    }
                    .to_string(),
                    sha: Some(result.head),
                    pr_number: None,
                    message: result.amend_note,
                }),
                publication_requirement,
                publication,
            }))
        }
        Err(e) => {
            let error = format!(
                "Logical-head publication failed: {e}; no project tree changes were published."
            );
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
