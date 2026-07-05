use super::file_mutations::{hash_file_target_uri, prepare_file_changes};
use super::handle_write;
use super::types::{
    build_failure, empty_change_report, mode_name, resource_failure, AppliedChange, ChangeFailure,
    ChangeReport, IndexedChange,
};
use crate::mcp::handlers::target::{target_family, TargetFamily};
use crate::mcp::types::{ChangeMode, ChangePayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::mutations::{dispatch_resource_change, hash_resource_target};
use crate::storage::RowExt;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::params;

#[derive(Debug, Clone)]
pub(super) struct ChangePreviewEvent {
    pub(super) run_id: String,
    pub(super) sequence: i32,
    pub(super) tool_use_id: String,
    pub(super) data: serde_json::Value,
    pub(super) status: Option<String>,
}

pub(super) fn is_change_tool_name(name: &str) -> bool {
    // `write` is the current verb name; `change` is recognized so previews
    // recorded under the legacy name still resolve when applied.
    name == "write" || name.ends_with("__write") || name == "change" || name.ends_with("__change")
}

/// Whether a stored change tool-use input is preview-shaped — its tool result is
/// a preview report and it participates in the apply round-trip. An explicit
/// `preview` boolean wins; when it is absent (the case for a bare
/// `mode:"rename"` call, which defaults to the preview path without the agent
/// passing `preview:true`), a rename change item makes the call preview-shaped.
/// This is what lets the transcript-event matcher recognize a rename preview
/// even though `preview:true` is not literally in its input.
pub(super) fn input_is_preview_shaped(input: &serde_json::Value) -> bool {
    if let Some(explicit) = input.get("preview").and_then(|value| value.as_bool()) {
        return explicit;
    }
    input
        .get("changes")
        .and_then(|changes| changes.as_array())
        .map(|changes| {
            changes
                .iter()
                .any(|item| item.get("mode").and_then(|mode| mode.as_str()) == Some("rename"))
        })
        .unwrap_or(false)
}

fn tool_id(tool: &serde_json::Value) -> Option<&str> {
    tool.get("id")
        .or_else(|| tool.get("toolUseId"))
        .and_then(|value| value.as_str())
}

pub(super) fn find_change_preview_in_assistant_rows(
    run_id: &str,
    rows: Vec<(i32, String, Option<String>)>,
    latest_sequence: Option<i32>,
    tool_use_id: Option<&str>,
) -> Result<Option<ChangePreviewEvent>, String> {
    let tool_use_id = tool_use_id.map(str::to_string);
    for (sequence, data, status) in rows {
        if let Some(latest_sequence) = latest_sequence {
            if sequence != latest_sequence {
                return Ok(None);
            }
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            if tool_use_id.is_none() {
                return Ok(None);
            }
            continue;
        };
        let Some(tool_uses) = value.get("toolUses").and_then(|value| value.as_array()) else {
            if tool_use_id.is_none() {
                return Ok(None);
            }
            continue;
        };
        let mut matching_preview_count = 0usize;
        let mut matching_preview: Option<ChangePreviewEvent> = None;
        for tool in tool_uses {
            let id_matches = match tool_use_id.as_deref() {
                Some(expected) => tool_id(tool).map(|id| id == expected).unwrap_or(false),
                None => true,
            };
            let is_change = tool
                .get("name")
                .and_then(|value| value.as_str())
                .map(is_change_tool_name)
                .unwrap_or(false);
            let is_preview = tool
                .get("input")
                .map(input_is_preview_shaped)
                .unwrap_or(false);
            if id_matches && is_change && is_preview {
                let Some(actual_tool_use_id) = tool_id(tool) else {
                    continue;
                };
                matching_preview_count += 1;
                matching_preview = Some(ChangePreviewEvent {
                    run_id: run_id.to_string(),
                    sequence,
                    tool_use_id: actual_tool_use_id.to_string(),
                    data: value.clone(),
                    status: status.clone(),
                });
            }
        }
        if matching_preview_count > 1 && tool_use_id.is_none() {
            return Err("Current assistant event contains multiple preview change calls; cannot attach event-level preview status without tool_use_id".to_string());
        }
        if matching_preview.is_some() {
            return Ok(matching_preview);
        }
        if tool_use_id.is_none() {
            return Ok(None);
        }
    }
    Ok(None)
}

async fn find_current_change_event(
    orch: &Orchestrator,
    run_id: &str,
    tool_use_id: Option<&str>,
) -> Result<Option<ChangePreviewEvent>, String> {
    let run_id = run_id.to_string();
    let tool_use_id = tool_use_id.map(str::to_string);
    orch.db
        .local
        .read(|conn| {
            let run_id = run_id.clone();
            let tool_use_id = tool_use_id.clone();
            Box::pin(async move {
                let latest_sequence = if tool_use_id.is_none() {
                    let mut latest_rows = conn
                        .query(
                            "SELECT MAX(sequence) FROM events WHERE run_id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    latest_rows
                        .next()
                        .await?
                        .and_then(|row| row.i64(0).ok())
                        .map(|seq| seq as i32)
                } else {
                    None
                };

                // Live-only reader (no archival reconstruction): change-preview
                // matching runs during an active session, before teardown
                // rewrites events to coordinates. It never sees an archived row.
                let mut rows = conn
                    .query(
                        "SELECT sequence, data, change_preview_status
                         FROM events
                         WHERE run_id = ?1 AND event_type = 'assistant'
                         ORDER BY sequence DESC",
                        (run_id.as_str(),),
                    )
                    .await?;
                let mut event_rows = Vec::new();
                while let Some(row) = rows.next().await? {
                    event_rows.push((row.i64(0)? as i32, row.text(1)?, row.opt_text(2)?));
                }
                find_change_preview_in_assistant_rows(
                    &run_id,
                    event_rows,
                    latest_sequence,
                    tool_use_id.as_deref(),
                )
                .map_err(crate::storage::DbError::Row)
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn update_preview_status(
    orch: &Orchestrator,
    run_id: &str,
    sequence: i32,
    status: &str,
    applied_at: Option<i64>,
) -> Result<(), String> {
    let run_id = run_id.to_string();
    let status = status.to_string();
    orch.db
        .local
        .write(|conn| {
            let run_id = run_id.clone();
            let status = status.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE events
                     SET change_preview_status = ?1, change_applied_at = COALESCE(?2, change_applied_at)
                     WHERE run_id = ?3 AND sequence = ?4",
                    params![status.as_str(), applied_at, run_id.as_str(), sequence as i64],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
}

fn extract_event_seq_from_apply_target(target: &str) -> Result<i32, String> {
    match parse_uri(target) {
        Some(CairnResource::NodeChatEvent { event_seq, .. })
        | Some(CairnResource::TaskChatEvent { event_seq, .. }) => Ok(event_seq),
        _ => Err("mode=apply target must be a node or task transcript event URI".to_string()),
    }
}

fn tool_input_from_event(event: &ChangePreviewEvent) -> Result<serde_json::Value, String> {
    let tool_uses = event
        .data
        .get("toolUses")
        .and_then(|value| value.as_array())
        .ok_or_else(|| "Preview event does not contain tool uses".to_string())?;
    for tool in tool_uses {
        let id_matches = tool_id(tool)
            .map(|id| id == event.tool_use_id)
            .unwrap_or(false);
        let is_change = tool
            .get("name")
            .and_then(|value| value.as_str())
            .map(is_change_tool_name)
            .unwrap_or(false);
        if id_matches && is_change {
            return tool
                .get("input")
                .cloned()
                .ok_or_else(|| "Preview change event is missing tool input".to_string());
        }
    }
    Err("Preview event does not contain the matching change tool use".to_string())
}

async fn load_tool_result_for_event(
    orch: &Orchestrator,
    event: &ChangePreviewEvent,
) -> Result<Option<String>, String> {
    let run_id = event.run_id.clone();
    let tool_use_id = event.tool_use_id.clone();
    let sequence = event.sequence;
    orch.db
        .local
        .read(|conn| {
            let run_id = run_id.clone();
            let tool_use_id = tool_use_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data
                         FROM events
                         WHERE run_id = ?1 AND sequence > ?2 AND event_type IN ('user', 'tool_result')
                         ORDER BY sequence ASC",
                        params![run_id.as_str(), sequence as i64],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    let data = row.text(0)?;
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
                        continue;
                    };
                    let matches_id = value
                        .get("toolUseId")
                        .or_else(|| value.get("tool_use_id"))
                        .and_then(|value| value.as_str())
                        .map(|id| id == tool_use_id)
                        .unwrap_or(false);
                    if matches_id {
                        return Ok(value
                            .get("toolResult")
                            .or_else(|| value.get("tool_result"))
                            .and_then(|value| value.as_str())
                            .map(ToOwned::to_owned));
                    }
                }
                Ok(None)
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn build_current_event_uri(
    orch: &Orchestrator,
    run_id: &str,
    event_seq: i32,
) -> Option<String> {
    let run_id = run_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number, e.seq, j.uri_segment, j.id, j.parent_job_id, j.task_index
                         FROM runs r
                         JOIN jobs j ON r.job_id = j.id
                         JOIN projects p ON j.project_id = p.id
                         LEFT JOIN issues i ON j.issue_id = i.id
                         LEFT JOIN executions e ON j.execution_id = e.id
                         WHERE r.id = ?1
                         LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let project = row.text(0)?;
                let Some(number) = row.opt_i64(1)? else {
                    return Ok(None);
                };
                let exec_seq = row.opt_i64(2)?.unwrap_or(1);
                let node_segment = row.opt_text(3)?.unwrap_or_else(|| row.text(4).unwrap_or_default());
                let parent_job_id = row.opt_text(5)?;
                let task_index = row.opt_i64(6)?;
                let mut run_seq_rows = conn
                    .query(
                        "SELECT COUNT(*)
                         FROM runs prior
                         WHERE prior.job_id = (SELECT job_id FROM runs WHERE id = ?1)
                           AND prior.created_at <= (SELECT created_at FROM runs WHERE id = ?1)",
                        (run_id.as_str(),),
                    )
                    .await?;
                let run_seq = run_seq_rows
                    .next()
                    .await?
                    .and_then(|row| row.i64(0).ok())
                    .unwrap_or(1);
                if let Some(parent_id) = parent_job_id {
                    let mut parent_rows = conn
                        .query("SELECT COALESCE(uri_segment, id) FROM jobs WHERE id = ?1 LIMIT 1", (parent_id.as_str(),))
                        .await?;
                    let parent_segment = parent_rows
                        .next()
                        .await?
                        .and_then(|parent_row| parent_row.text(0).ok())
                        .unwrap_or_else(|| "parent".to_string());
                    let task_segment = if node_segment.is_empty() {
                        task_index
                            .map(|idx| format!("task-{}", idx + 1))
                            .unwrap_or_else(|| "task".to_string())
                    } else {
                        node_segment
                    };
                    Ok(Some(format!(
                        "cairn://p/{}/{}/{}/{}/task/{}/chat/{}/{}",
                        project, number, exec_seq, parent_segment, task_segment, run_seq, event_seq
                    )))
                } else {
                    Ok(Some(format!(
                        "cairn://p/{}/{}/{}/{}/chat/{}/{}",
                        project, number, exec_seq, node_segment, run_seq, event_seq
                    )))
                }
            })
        })
        .await
        .ok()
        .flatten()
}

pub(super) async fn preview_change(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &ChangePayload,
) -> String {
    if payload
        .changes
        .iter()
        .any(|item| item.mode == ChangeMode::Apply)
    {
        return "Invalid payload: preview=true cannot be used with mode=apply".to_string();
    }

    let worktree = std::path::Path::new(&request.cwd);
    let mut applied = Vec::new();
    let mut target_hashes = Vec::new();
    let mut index = 0;

    while index < payload.changes.len() {
        let item = &payload.changes[index];

        // Rename preview: compute the edit set read-only and report one entry per
        // edited file, pushing a target hash for every edited/moved path so the
        // apply round-trip's drift guard rejects a stale apply. A move's new path
        // hashes as "missing" on both preview and apply, so the guard still holds.
        if item.mode == ChangeMode::Rename
            && matches!(target_family(&item.target), Ok(TargetFamily::File))
        {
            let rename_result = match super::file_mutations::parse_rename_spec(worktree, item) {
                Ok((route_file, spec, new_name)) => {
                    crate::symbols::rename::compute_plan(worktree, &route_file, spec, &new_name)
                }
                Err(error) => Err(error),
            };
            let plan = match rename_result {
                Ok(plan) => plan,
                Err(error) => {
                    let failure = build_failure(index, item, error);
                    return serde_json::to_string(&empty_change_report(
                        applied,
                        vec![failure.failure],
                        failure.commit,
                        false,
                        true,
                    ))
                    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
                }
            };
            let rel = |path: &std::path::Path| -> String {
                path.strip_prefix(worktree)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            };
            for edit in &plan.file_edits {
                let src_target = format!("file:{}", rel(&edit.worktree_path));
                match hash_file_target_uri(worktree, &src_target) {
                    Ok(hash) => target_hashes.push(hash),
                    Err(error) => {
                        let failure = build_failure(index, item, error);
                        return serde_json::to_string(&empty_change_report(
                            applied,
                            vec![failure.failure],
                            failure.commit,
                            false,
                            true,
                        ))
                        .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
                    }
                }
                let (target, summary) = match (&edit.new_content, &edit.move_to) {
                    (Some(_), Some(dest)) => {
                        let dest_target = format!("file:{}", rel(dest));
                        match hash_file_target_uri(worktree, &dest_target) {
                            Ok(hash) => target_hashes.push(hash),
                            Err(error) => {
                                let failure = build_failure(index, item, error);
                                return serde_json::to_string(&empty_change_report(
                                    applied,
                                    vec![failure.failure],
                                    failure.commit,
                                    false,
                                    true,
                                ))
                                .unwrap_or_else(|e| {
                                    format!("Failed to serialize change report: {e}")
                                });
                            }
                        }
                        (
                            dest_target.clone(),
                            format!(
                                "Would apply R {src_target}\u{2192}{dest_target} ({} sites)",
                                edit.site_count
                            ),
                        )
                    }
                    (Some(_), None) => (
                        src_target.clone(),
                        format!("Would apply ~{src_target} ({} sites)", edit.site_count),
                    ),
                    (None, _) => (src_target.clone(), format!("Would apply -{src_target}")),
                };
                applied.push(AppliedChange {
                    index,
                    target,
                    mode: "rename".to_string(),
                    kind: "file".to_string(),
                    summary,
                    data: None,
                });
            }
            index += 1;
            continue;
        }

        let family = match target_family(&item.target) {
            Ok(family) => family,
            Err(error) => {
                let failure = build_failure(index, item, error);
                return serde_json::to_string(&empty_change_report(
                    applied,
                    vec![failure.failure],
                    None,
                    false,
                    true,
                ))
                .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
            }
        };

        if family == TargetFamily::Resource {
            match dispatch_resource_change(orch, request, index, item, true).await {
                Ok(change) => applied.push(change.into()),
                Err(failure) => {
                    let failure = resource_failure(failure);
                    return serde_json::to_string(&empty_change_report(
                        applied,
                        vec![failure.failure],
                        failure.commit,
                        false,
                        true,
                    ))
                    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
                }
            }
            match hash_resource_target(orch, request, item).await {
                Ok(hash) => target_hashes.push(hash.into()),
                Err(error) => {
                    let failure = build_failure(index, item, error);
                    return serde_json::to_string(&empty_change_report(
                        applied,
                        vec![failure.failure],
                        failure.commit,
                        false,
                        true,
                    ))
                    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
                }
            }
            index += 1;
            continue;
        }

        let start = index;
        while index < payload.changes.len()
            && matches!(
                target_family(&payload.changes[index].target),
                Ok(TargetFamily::File)
            )
            && payload.changes[index].mode != ChangeMode::Rename
        {
            index += 1;
        }
        let batch = payload.changes[start..index]
            .iter()
            .enumerate()
            .map(|(offset, item)| IndexedChange {
                index: start + offset,
                item,
            })
            .collect::<Vec<_>>();

        // Preview never fences: resolve allowing escape so an out-of-worktree
        // crossing is reported in the change report rather than rejected.
        match prepare_file_changes(worktree, &batch, true) {
            Ok((prepared, summaries)) => {
                for prepared_change in &prepared {
                    let change = &batch[prepared_change.change_pos()];
                    match hash_file_target_uri(worktree, prepared_change.target_uri()) {
                        Ok(hash) => target_hashes.push(hash),
                        Err(error) => {
                            let failure = build_failure(change.index, change.item, error);
                            return serde_json::to_string(&empty_change_report(
                                applied,
                                vec![failure.failure],
                                failure.commit,
                                false,
                                true,
                            ))
                            .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
                        }
                    }
                }
                applied.extend(prepared.iter().zip(summaries.iter()).map(
                    |(prepared_change, summary)| {
                        let change = &batch[prepared_change.change_pos()];
                        AppliedChange {
                            index: change.index,
                            target: prepared_change.target_uri().to_string(),
                            mode: mode_name(change.item.mode).to_string(),
                            kind: "file".to_string(),
                            summary: format!("Would apply {summary}"),
                            data: None,
                        }
                    },
                ));
            }
            Err(failure) => {
                return serde_json::to_string(&empty_change_report(
                    applied,
                    vec![failure.failure],
                    failure.commit,
                    false,
                    true,
                ))
                .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
            }
        }
    }

    let (event_uri, preview_status_error) = match request.run_id.as_deref() {
        Some(run_id) => match find_current_change_event(orch, run_id, request.tool_use_id.as_deref()).await {
            Ok(Some(event)) => {
                let event_uri = build_current_event_uri(orch, run_id, event.sequence).await;
                if let Err(error) = update_preview_status(orch, run_id, event.sequence, "pending", None).await {
                    (event_uri, Some(error))
                } else {
                    (event_uri, None)
                }
            }
            Ok(None) => (
                None,
                Some("Preview event is not yet visible in the transcript; retry after the tool call is persisted".to_string()),
            ),
            Err(error) => (None, Some(error)),
        },
        None => (
            None,
            Some("preview=true requires run_id so the transcript event can be marked pending".to_string()),
        ),
    };

    if let Some(error) = preview_status_error {
        return serde_json::to_string(&empty_change_report(
            applied,
            vec![ChangeFailure {
                index: 0,
                target: "change preview event".to_string(),
                mode: "preview".to_string(),
                kind: "event".to_string(),
                error,
            }],
            None,
            false,
            true,
        ))
        .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
    }

    serde_json::to_string(&ChangeReport {
        applied,
        failures: Vec::new(),
        commit: None,
        partial_success: false,
        transactional: true,
        preview: true,
        event_uri: event_uri.clone(),
        apply_uri: event_uri,
        target_hashes,
    })
    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"))
}

pub(super) async fn handle_apply_change(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &ChangePayload,
) -> Option<String> {
    if !payload
        .changes
        .iter()
        .any(|item| item.mode == ChangeMode::Apply)
    {
        return None;
    }
    if payload.preview.unwrap_or(false) {
        return Some("Invalid payload: preview=true cannot be used with mode=apply".to_string());
    }
    if payload.changes.len() != 1 || payload.changes[0].mode != ChangeMode::Apply {
        return Some("Invalid payload: mode=apply must be the only change item".to_string());
    }
    let Some(run_id) = request.run_id.as_deref() else {
        return Some("Invalid payload: mode=apply requires current run_id".to_string());
    };
    // Apply is the step that writes, so it — not the read-only preview — carries
    // the commit_msg that commits the landed edits.
    let Some(apply_commit_msg) = payload.commit_msg.clone() else {
        return Some(
            "Invalid payload: mode=apply requires a commit_msg so the landed edits are committed"
                .to_string(),
        );
    };
    let event_seq = match extract_event_seq_from_apply_target(&payload.changes[0].target) {
        Ok(seq) => seq,
        Err(error) => return Some(error),
    };

    let event = match orch
        .db
        .local
        .read(|conn| {
            let run_id = run_id.to_string();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data, change_preview_status
                         FROM events
                         WHERE run_id = ?1 AND sequence = ?2 AND event_type = 'assistant'
                         LIMIT 1",
                        params![run_id.as_str(), event_seq as i64],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let data = row.text(0)?;
                let status = row.opt_text(1)?;
                let value = serde_json::from_str::<serde_json::Value>(&data)
                    .map_err(|e| crate::storage::DbError::Row(e.to_string()))?;
                let Some(tool_uses) = value.get("toolUses").and_then(|value| value.as_array())
                else {
                    return Ok(None);
                };
                for tool in tool_uses {
                    let is_change = tool
                        .get("name")
                        .and_then(|value| value.as_str())
                        .map(is_change_tool_name)
                        .unwrap_or(false);
                    let preview = tool
                        .get("input")
                        .map(input_is_preview_shaped)
                        .unwrap_or(false);
                    if is_change && preview {
                        let Some(tool_use_id) = tool_id(tool).map(ToOwned::to_owned) else {
                            continue;
                        };
                        return Ok(Some(ChangePreviewEvent {
                            run_id: run_id.clone(),
                            sequence: event_seq,
                            tool_use_id,
                            data: value,
                            status,
                        }));
                    }
                }
                Ok(None)
            })
        })
        .await
    {
        Ok(Some(event)) => event,
        Ok(None) => return Some("No pending change preview found for apply target".to_string()),
        Err(error) => return Some(format!("Failed to load preview event: {error}")),
    };

    if event.status.as_deref() != Some("pending") {
        return Some(format!(
            "Preview is not pending (status: {})",
            event.status.as_deref().unwrap_or("none")
        ));
    }

    let result = match load_tool_result_for_event(orch, &event).await {
        Ok(Some(result)) => result,
        Ok(None) => return Some("Preview tool result is not available yet".to_string()),
        Err(error) => return Some(format!("Failed to load preview result: {error}")),
    };
    let preview_report: ChangeReport = match serde_json::from_str(&result) {
        Ok(report) => report,
        Err(error) => return Some(format!("Preview result is not a change report: {error}")),
    };

    let original_input = match tool_input_from_event(&event) {
        Ok(input) => input,
        Err(error) => return Some(error),
    };
    let mut original_payload: ChangePayload = match serde_json::from_value(original_input) {
        Ok(payload) => payload,
        Err(error) => return Some(format!("Stored preview payload is invalid: {error}")),
    };
    original_payload.preview = Some(false);
    // The apply call's commit_msg lands the edits; the stored preview carried
    // none (a preview writes nothing).
    original_payload.commit_msg = Some(apply_commit_msg);

    let mut stale = Vec::new();
    for expected in &preview_report.target_hashes {
        let actual = if expected.kind == "file" {
            hash_file_target_uri(std::path::Path::new(&request.cwd), &expected.target)
        } else {
            let item = original_payload
                .changes
                .iter()
                .find(|item| item.target == expected.target)
                .or_else(|| original_payload.changes.first());
            let Some(item) = item else {
                stale.push(format!("{}: missing original change", expected.target));
                continue;
            };
            hash_resource_target(orch, request, item)
                .await
                .map(Into::into)
        };
        match actual {
            Ok(actual) if actual == *expected => {}
            Ok(actual) => stale.push(format!(
                "{}: expected {} (exists={}), found {} (exists={})",
                expected.target, expected.hash, expected.exists, actual.hash, actual.exists
            )),
            Err(error) => stale.push(format!("{}: {error}", expected.target)),
        }
    }

    if !stale.is_empty() {
        let _ = update_preview_status(orch, &event.run_id, event.sequence, "stale", None).await;
        return Some(format!(
            "Preview is stale; no changes applied:\n{}",
            stale.join("\n")
        ));
    }

    let mut apply_request = request.clone();
    apply_request.payload = match serde_json::to_value(original_payload) {
        Ok(value) => value,
        Err(error) => {
            return Some(format!(
                "Failed to serialize stored preview payload: {error}"
            ));
        }
    };
    let result = Box::pin(handle_write(orch, &apply_request)).await;
    if let Ok(report) = serde_json::from_str::<ChangeReport>(&result) {
        if report.failures.is_empty() {
            let now = chrono::Utc::now().timestamp();
            let _ =
                update_preview_status(orch, &event.run_id, event.sequence, "applied", Some(now))
                    .await;
        }
    }
    Some(result)
}
