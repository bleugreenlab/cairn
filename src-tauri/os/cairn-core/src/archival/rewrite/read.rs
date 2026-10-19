use super::*;

/// Verify a read against the stored agent-seen bytes and rewrite it.
///
/// Each durable candidate commit is rendered with exactly the code
/// reconstruction will use ([`reconstruct::reconstruct_read`]), so a byte match
/// guarantees a faithful round-trip; the first match wins. When none matches and
/// the first candidate's render carries a stub (a resource, glob, or
/// out-of-repo target in the batch), the read is archived per-section as a
/// hybrid. A fully resolved render that differs at every candidate (replay
/// drift, an uncommitted edit, a composition divergence) falls to zstd and
/// counts as a mismatch.
pub(super) fn try_read(
    event: &Event,
    tool: Option<&(String, Value)>,
    candidates: &[String],
    store: &ObjectStore,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let paths = tool.and_then(|(_, input)| read_paths(input));
    let stored = tool_result_text(&event.data);
    let (Some(paths), Some(stored)) = (paths, stored) else {
        return push_zstd(event, Some("read"), updates, summary);
    };

    let mut first: Option<(&str, String)> = None;
    for commit in candidates {
        let rendered = reconstruct::reconstruct_read(commit, &paths, Some(store));
        if rendered == stored {
            // Pure-file fast path: every target resolved and the batch
            // reproduced byte-for-byte at this commit.
            let render_sha = sha256_hex(stored.as_bytes());
            let data = read_stub(&event.data, &paths);
            summary.gitcoord_read += 1;
            summary.bytes_after += data.len();
            updates.push(EventUpdate {
                id: event.id.clone(),
                shape: ArchivedShape::GitcoordRead {
                    commit: commit.clone(),
                    render_sha,
                    data,
                },
            });
            return Ok(());
        }
        if first.is_none() {
            first = Some((commit, rendered));
        }
    }

    match first {
        Some((commit, rendered)) if rendered.contains(reconstruct::STUB_PREFIX) => {
            try_hybrid_read(event, &paths, commit, store, &stored, updates, summary)
        }
        Some(_) => {
            push_zstd(event, Some("read"), updates, summary)?;
            summary.mismatch_fallback += 1;
            Ok(())
        }
        None => push_zstd(event, Some("read"), updates, summary),
    }
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
/// plain zstd. Otherwise the shape is verified by splicing the skeleton back and
/// comparing to the stored bytes before they are discarded.
fn try_hybrid_read(
    event: &Event,
    paths: &[String],
    commit: &str,
    store: &ObjectStore,
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
        let section = reconstruct::render_archived_file_section(commit, target, Some(store));
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
    }
    skeleton.push_str(&stored[cursor..]);

    if indices.is_empty() {
        return push_zstd(event, Some("read"), updates, summary);
    }

    // Verify-then-discard: splice the skeleton back with the same renderer that
    // built it and bail to zstd on any mismatch.
    let rebuilt =
        reconstruct::splice_hybrid_skeleton(&skeleton, commit, paths, &indices, Some(store));
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
    });
    Ok(())
}
