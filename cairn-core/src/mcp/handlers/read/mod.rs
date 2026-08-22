//! Structured batch read: producers, the shared view layer, and the assembler.
//!
//! A `read_batch` callback carries the resolved target array. [`batch`] classifies
//! each target, runs its producer to a [`cairn_common::read::ReadSegment`], and
//! [`view`] windows/truncates each under one shared budget while emitting the
//! enriched `=== uri [suffix] ===` header and an always-valid continue footer.

pub mod batch;
pub mod file;
pub(crate) mod object_read;
pub(crate) mod overlay;

use cairn_common::read::ReadSegment;

pub(crate) use batch::handle_read_batch;
pub use file::handle_read_file;
pub(crate) use file::produce_file_segment;

pub fn files_at_commit(
    repository_path: std::path::PathBuf,
    commit_id: String,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    object_read::ObjectReadService::new(repository_path, commit_id, String::new())?
        .files()
        .map_err(|error| error.to_string())
}

pub fn file_at_commit(
    repository_path: std::path::PathBuf,
    commit_id: String,
    path: &str,
) -> Result<Option<Vec<u8>>, String> {
    let service =
        object_read::ObjectReadService::new(repository_path, commit_id, path.to_string())?;
    match service.bytes() {
        Ok(bytes) => Ok(Some(bytes)),
        Err(cairn_codec::objects::ObjectReadError::PathNotFound(_)) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

// The shared view/render layer moved down into `storage::render` so `storage`
// carries no upward `crate::mcp` edge. Re-export it under the original `view`
// name, and re-export the non-content segment constructors, so every producer
// and suppression path uses the same structured states.
pub(crate) use crate::storage::render as view;
pub(crate) use crate::storage::render::duplicate_segment;
pub(crate) use crate::storage::render::error_segment;

#[cfg(test)]
mod archived_reconstruct_tests;

/// A producer outcome: a finished segment, or a fence suspension that must abort
/// the whole batch so the permission flow can re-dispatch it once approved.
///
/// `Segment` legitimately dwarfs `Suspended`: this is a short-lived, per-target
/// outcome that is `Segment` on every successful read, so boxing the common
/// variant would add an allocation on the hot path to satisfy a size heuristic.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Produced {
    Segment(ReadSegment),
    Suspended(String),
}

/// The mcp read layer's implementation of the archived-file render seam. Only
/// this layer can reproduce a read's rendered [`ReadSegment`] from raw bytes (it
/// re-runs the byte renderers and the in-process ast-grep/outline machinery), so
/// it registers the implementation with `storage::render` at `Orchestrator`
/// construction and archival reconstruction — which lives below this layer — calls
/// back through the registry.
struct McpArchivedFileRenderer;

impl crate::storage::render::ArchivedFileRenderer for McpArchivedFileRenderer {
    fn produce_archived_file_segment(
        &self,
        target: &str,
        bytes: &[u8],
    ) -> Result<ReadSegment, String> {
        file::produce_archived_file_segment(target, bytes)
    }
}

/// Register the mcp archived-file renderer with `storage::render`. Idempotent
/// (the registry is a `OnceLock`), so calling it at each `Orchestrator`
/// construction is harmless. Must run before any archived-read reconstruction;
/// without it, archived reads degrade to labeled coordinate stubs.
pub fn register_archived_file_renderer() {
    static R: McpArchivedFileRenderer = McpArchivedFileRenderer;
    crate::storage::render::set_archived_file_renderer(&R);
}

// Grep, ast, and outline segments all carry their own counts: the grep window
// counts structured lines before rendering, and `symbols::render::Rendered`
// counts results as it assembles them. Nothing re-derives a count by reading a
// rendered body back, because rendered text cannot answer it — under the
// heading layout a file named `1:foo.rs` reads exactly like a match row, and
// one named `--` like a group separator.
