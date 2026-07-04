use super::*;

/// Verify a read against the stored agent-seen bytes and rewrite it.
///
/// Computes the candidate rendering with the exact same code reconstruction will
/// use ([`reconstruct::reconstruct_read`]) so a byte match guarantees a faithful
/// round-trip. An unresolvable target (resource/glob/out-of-repo) renders a stub
/// and falls to plain zstd; a resolved-but-differing render (drift, dirty tree,
/// composition seam) falls to zstd and counts as a mismatch.
pub(super) fn try_read(
    event: &Event,
    tool: Option<&(String, Value)>,
    current: Option<&Coord>,
    store: Option<&ObjectStore>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let paths = tool.and_then(|(_, input)| read_paths(input));
    let stored = tool_result_text(&event.data);
    match (paths, current, stored) {
        (Some(paths), Some((commit, change_id)), Some(stored)) => {
            let candidate = reconstruct::reconstruct_read(commit, &paths, store);
            if candidate == stored {
                // Pure-file fast path: every target resolved and the batch
                // reproduced byte-for-byte.
                let render_sha = sha256_hex(stored.as_bytes());
                let data = read_stub(&event.data, &paths);
                summary.gitcoord_read += 1;
                summary.bytes_after += data.len();
                updates.push(EventUpdate {
                    id: event.id.clone(),
                    shape: ArchivedShape::GitcoordRead {
                        commit: commit.to_string(),
                        render_sha,
                        data,
                    },
                    change_id: change_id.clone(),
                });
            } else if candidate.contains(reconstruct::STUB_PREFIX) {
                // At least one target could not resolve from git (a resource,
                // glob, directory, or out-of-repo path in a mixed batch).
                // Coordinatize the reproducible file sections per-section and
                // store the rest verbatim, falling to plain zstd if none qualify.
                try_hybrid_read(
                    event,
                    &paths,
                    commit,
                    change_id.clone(),
                    store,
                    &stored,
                    updates,
                    summary,
                )?;
            } else {
                // Every target resolved but the render differs (replay drift,
                // dirty tree, composition seam): the coordinate can't be trusted.
                push_zstd(event, Some("read"), updates, summary)?;
                summary.mismatch_fallback += 1;
            }
        }
        _ => push_zstd(event, Some("read"), updates, summary)?,
    }
    Ok(())
}

/// Per-section hybrid archival for a mixed read batch whose pure-file fast path
/// failed because some target could not resolve from git. Coordinatize each
/// reproducible `file:` section and store the remaining sections (and the
/// separators, footers, and affordances) verbatim in a NUL-placeholder skeleton.
///
/// A file section that does not appear verbatim in the stored result — budget-
/// truncated in the mixed batch, a glob/directory the file producer rejects, or an
/// unresolvable blob — is left in the skeleton: per-section degradation, never a
/// batch-wide fallback. With no coordinatizable section the whole event falls to
/// plain zstd (the prior behavior). Otherwise the shape is verified by splicing
/// the skeleton back and comparing to the stored bytes before they are discarded.
#[allow(clippy::too_many_arguments)]
fn try_hybrid_read(
    event: &Event,
    paths: &[String],
    commit: &str,
    change_id: Option<String>,
    store: Option<&ObjectStore>,
    stored: &str,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let mut skeleton = String::new();
    let mut cursor = 0usize;
    let mut indices: Vec<usize> = Vec::new();
    for (index, target) in paths.iter().enumerate() {
        if reconstruct::repo_relative_path(target).is_none() {
            continue; // non-file target: stays verbatim in the skeleton.
        }
        let section = reconstruct::render_archived_file_section(commit, target, store);
        if section.contains(reconstruct::STUB_PREFIX) {
            continue; // glob/directory/unresolvable: stays verbatim.
        }
        // Ordered cursor scan in path order: a verbatim match both proves the
        // section reproduces byte-exactly and locates its span to excise.
        if let Some(relative) = stored[cursor..].find(&section) {
            let start = cursor + relative;
            let end = start + section.len();
            skeleton.push_str(&stored[cursor..start]);
            skeleton.push_str(&reconstruct::section_placeholder(index));
            cursor = end;
            indices.push(index);
        }
        // No verbatim match (truncated in the mixed batch): left verbatim.
    }
    skeleton.push_str(&stored[cursor..]);

    if indices.is_empty() {
        // Nothing reproducible (e.g. a pure-resource batch): the prior zstd path.
        return push_zstd(event, Some("read"), updates, summary);
    }

    // Verify-then-discard: splice the skeleton back with the same renderer that
    // built it and bail to zstd on any mismatch. Passes by construction; the guard
    // is against future render drift, exactly as the pure-file read's compare.
    let rebuilt = reconstruct::splice_hybrid_skeleton(&skeleton, commit, paths, &indices, store);
    if rebuilt != stored {
        return push_zstd(event, Some("read"), updates, summary);
    }

    let render_sha = sha256_hex(stored.as_bytes());
    let data = hybrid_stub(&event.data, paths, &indices);
    let blob = compress(skeleton.as_bytes())?;
    summary.hybrid_read += 1;
    summary.bytes_after += data.len() + blob.len();
    updates.push(EventUpdate {
        id: event.id.clone(),
        shape: ArchivedShape::HybridRead {
            commit: commit.to_string(),
            render_sha,
            data,
            blob,
        },
        change_id,
    });
    Ok(())
}
