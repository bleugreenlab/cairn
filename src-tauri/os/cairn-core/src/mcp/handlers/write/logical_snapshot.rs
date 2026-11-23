use super::super::branch::BranchResolution;
use super::file_mutations::logical_paths_for_changes;
use super::types::{IndexedChange, TargetHash};
use crate::mcp::file_targets::resolve_logical_file_target;
use crate::mcp::handlers::target::{target_family, TargetFamily};
use crate::mcp::types::{ChangeMode, ChangePayload, McpCallbackRequest};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub(super) type LogicalSnapshot = HashMap<String, Option<String>>;

/// Both interpretation paths read one immutable logical head, never a projection.
pub(super) fn load(
    resolution: &BranchResolution,
    request: &McpCallbackRequest,
    payload: &ChangePayload,
    allow_escape: bool,
) -> Result<LogicalSnapshot, String> {
    let has_rename = payload.changes.iter().any(|change| {
        change.mode == ChangeMode::Rename
            && matches!(target_family(&change.target), Ok(TargetFamily::File))
    });
    let indexed = payload
        .changes
        .iter()
        .enumerate()
        .filter(|(_, change)| matches!(target_family(&change.target), Ok(TargetFamily::File)))
        .map(|(index, item)| IndexedChange { index, item })
        .collect::<Vec<_>>();
    let paths =
        logical_paths_for_changes(std::path::Path::new(&request.cwd), &indexed, allow_escape)?;
    let files: Vec<(String, Option<Vec<u8>>)> = if has_rename {
        super::super::read::files_at_commit(
            resolution.object_repository_path.clone(),
            resolution.commit_id.clone(),
        )?
        .into_iter()
        // Rename only parses bundled source languages. Unrelated binary assets
        // are not text inputs, but explicitly edited paths still must be loaded.
        .filter(|(path, _)| {
            paths.contains(path)
                || crate::symbols::engine::lang_for_path(std::path::Path::new(path)).is_some()
        })
        .map(|(path, bytes)| (path, Some(bytes)))
        .collect()
    } else {
        paths
            .into_iter()
            .map(|path| {
                super::super::read::file_at_commit(
                    resolution.object_repository_path.clone(),
                    resolution.commit_id.clone(),
                    &path,
                )
                .map(|bytes| (path, bytes))
            })
            .collect::<Result<_, _>>()?
    };
    files
        .into_iter()
        .map(|(path, bytes)| {
            let content = bytes
                .map(String::from_utf8)
                .transpose()
                .map_err(|_| format!("File `{path}` is not valid UTF-8"))?;
            Ok((format!("file:{path}"), content))
        })
        .collect()
}

pub(super) fn hash_target(snapshot: &LogicalSnapshot, target: &str) -> Result<TargetHash, String> {
    let path = resolve_logical_file_target(target)?.relative_path;
    let target = format!("file:{path}");
    let content = snapshot.get(&target).and_then(Option::as_ref);
    Ok(TargetHash {
        target,
        kind: "file".to_string(),
        exists: content.is_some(),
        hash: content
            .map(|content| format!("{:x}", Sha256::digest(content.as_bytes())))
            .unwrap_or_else(|| "missing".to_string()),
    })
}

/// Structural rename depends on the entire namespace, including files that
/// did not contain a reference when the preview was computed.
pub(super) fn hash_namespace(snapshot: &LogicalSnapshot) -> TargetHash {
    let mut files = snapshot
        .iter()
        .filter_map(|(path, content)| content.as_ref().map(|content| (path, content)))
        .collect::<Vec<_>>();
    files.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let mut hash = Sha256::new();
    for (path, content) in files {
        hash.update((path.len() as u64).to_le_bytes());
        hash.update(path.as_bytes());
        hash.update((content.len() as u64).to_le_bytes());
        hash.update(content.as_bytes());
    }
    TargetHash {
        target: "file:".to_string(),
        kind: "logical-tree".to_string(),
        exists: true,
        hash: format!("{:x}", hash.finalize()),
    }
}
