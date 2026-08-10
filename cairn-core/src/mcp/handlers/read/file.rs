//! File target producers and the legacy single-target `read` callback.

use crate::mcp::file_targets::validate_read_path;
use crate::mcp::handlers::read::{error_segment, grep_body_counts, grep_counts, Produced};
use crate::mcp::handlers::target::invalid_target_error;
use crate::mcp::types::{IssueHistoryMode, McpCallbackRequest, ReadFilePayload};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use cairn_common::query::{split_target_query, QueryParam};
use cairn_common::read::{ImageBlock, NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
use cairn_common::uri::{build_issue_uri, parse_uri, CairnResource};
use cairn_db::turso::params;
use serde::Serialize;

/// Size threshold for eliding glob content previews (250KB)
const LARGE_FILE_THRESHOLD: u64 = 250_000;

const MAT_LEVEL5_MAGIC: &[u8] = b"MATLAB 5.0 MAT-file";
const HDF5_MAGIC: &[u8] = b"\x89HDF\r\n\x1a\n";

fn render_mat_file_summary(path: &std::path::Path, bytes: &[u8]) -> Option<Result<String, String>> {
    if !path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("mat"))
    {
        return None;
    }

    if bytes.starts_with(MAT_LEVEL5_MAGIC) {
        return Some(render_level5_mat_summary(bytes));
    }

    let hdf5_offset = bytes
        .windows(HDF5_MAGIC.len())
        .take(513)
        .position(|window| window == HDF5_MAGIC);
    if let Some(offset) = hdf5_offset {
        let creation_info = String::from_utf8_lossy(&bytes[..offset.min(128)])
            .trim_matches(['\0', ' '])
            .to_string();
        let mut summary = "MAT-file format: MATLAB v7.3 (HDF5)".to_string();
        if !creation_info.is_empty() {
            summary.push_str("\nHeader: ");
            summary.push_str(&creation_info);
        }
        summary.push_str(
            "\n\nFull variable inspection requires the bundled reader:\n\
             `run {target:\"cairn://skills/matlab/scripts/inspect-mat.py\", payload:{args:[\"<path>\"]}}`",
        );
        return Some(Ok(summary));
    }

    None
}

fn render_level5_mat_summary(bytes: &[u8]) -> Result<String, String> {
    let mat = matfile::MatFile::parse(std::io::Cursor::new(bytes))
        .map_err(|error| format!("{error:?}"))?;
    let mut lines = vec!["MAT-file format: MATLAB Level 5 (v7.2 or earlier)".to_string()];
    if mat.arrays().is_empty() {
        lines.push("Variables: (no supported numeric arrays)".to_string());
    } else {
        lines.push("Variables:".to_string());
        for array in mat.arrays() {
            let (class, preview, complex) = numeric_preview(array.data());
            let dimensions = array
                .size()
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(" × ");
            let complex_suffix = if complex { ", complex" } else { "" };
            lines.push(format!(
                "- `{}`: {} [{}]{}; preview: [{}]",
                array.name(),
                class,
                dimensions,
                complex_suffix,
                preview
            ));
        }
    }
    Ok(lines.join("\n"))
}

fn numeric_preview(data: &matfile::NumericData) -> (&'static str, String, bool) {
    macro_rules! preview {
        ($class:literal, $real:expr, $imag:expr) => {{
            let values = $real
                .iter()
                .take(8)
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let omitted = $real.len().saturating_sub(values.len());
            let mut rendered = values.join(", ");
            if omitted > 0 {
                rendered.push_str(&format!(", … (+{omitted})"));
            }
            ($class, rendered, $imag.is_some())
        }};
    }
    match data {
        matfile::NumericData::Int8 { real, imag } => preview!("int8", real, imag),
        matfile::NumericData::UInt8 { real, imag } => preview!("uint8", real, imag),
        matfile::NumericData::Int16 { real, imag } => preview!("int16", real, imag),
        matfile::NumericData::UInt16 { real, imag } => preview!("uint16", real, imag),
        matfile::NumericData::Int32 { real, imag } => preview!("int32", real, imag),
        matfile::NumericData::UInt32 { real, imag } => preview!("uint32", real, imag),
        matfile::NumericData::Int64 { real, imag } => preview!("int64", real, imag),
        matfile::NumericData::UInt64 { real, imag } => preview!("uint64", real, imag),
        matfile::NumericData::Single { real, imag } => preview!("single", real, imag),
        matfile::NumericData::Double { real, imag } => preview!("double", real, imag),
    }
}

struct BranchReadContext {
    service: super::object_read::ObjectReadService,
    repo_path: String,
    project_id: String,
    repository_path: std::path::PathBuf,
    default_commit_id: String,
}

struct IgnoredMaterializationRead {
    project_id: String,
    repo_path: String,
    commit_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchGrepTarget {
    SingleFile,
    Tree,
}

fn classify_branch_grep_target(context: &BranchReadContext) -> Result<BranchGrepTarget, String> {
    match object_target_kind(
        &context.service,
        context.service.commit_id(),
        &context.repo_path,
    ) {
        Ok(BranchTargetKind::File) => Ok(BranchGrepTarget::SingleFile),
        Ok(BranchTargetKind::Directory) => Ok(BranchGrepTarget::Tree),
        Err(object_error) => {
            if context
                .service
                .is_ignored_path(&context.repo_path)
                .map_err(|error| error.to_string())?
            {
                // An ignored logical target is necessarily a direct materialization
                // projection here. Its runner-side path is deliberately absent, so
                // consulting process cwd would incorrectly turn it into a tree grep.
                Ok(BranchGrepTarget::SingleFile)
            } else {
                Err(object_error.to_string())
            }
        }
    }
}

/// Classify a grep target as one file or a tree, and resolve the output mode
/// that classification implies.
///
/// One classification drives both answers. A logical project target resolves
/// through the branch's object tree, and its `full_path` is a bare
/// repository-relative path that never exists under the process cwd — so
/// stat-ing it would call every logical single-file grep a tree and default it
/// to `files_with_matches`, rendering the bare filename the caller already
/// typed where the matched lines belong. The filesystem is consulted only when
/// there is no branch classification to consult. A `glob`/`type` push-down
/// always means a multi-file walk, whatever the target is.
fn resolve_grep_target_mode(
    branch_target: Option<BranchGrepTarget>,
    full_path: &std::path::Path,
    payload: &crate::mcp::handlers::search::GrepPayload,
) -> (bool, String) {
    let single_file = branch_target
        .map(|target| target == BranchGrepTarget::SingleFile)
        .unwrap_or_else(|| full_path.is_file())
        && payload.glob.is_none()
        && payload.file_type.is_none();
    let requested_context = payload.context.is_some()
        || payload.context_alias.is_some()
        || payload.after_context.is_some()
        || payload.before_context.is_some();
    let mode = crate::mcp::handlers::search::resolve_grep_output_mode(
        payload.output_mode.as_deref(),
        requested_context,
        single_file,
    )
    .to_string();
    (single_file, mode)
}

fn classify_branch_direct_read(
    context: &BranchReadContext,
) -> Result<Result<Vec<u8>, IgnoredMaterializationRead>, String> {
    match context.service.bytes() {
        Ok(bytes) => Ok(Ok(bytes)),
        Err(object_error) => {
            if !context
                .service
                .is_ignored_path(&context.repo_path)
                .map_err(|error| error.to_string())?
            {
                return Err(object_error.to_string());
            }
            Ok(Err(IgnoredMaterializationRead {
                project_id: context.project_id.clone(),
                repo_path: context.repo_path.clone(),
                commit_id: context.service.commit_id().to_string(),
            }))
        }
    }
}

async fn resolve_branch_direct_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    classified: Result<Result<Vec<u8>, IgnoredMaterializationRead>, String>,
) -> Result<Vec<u8>, String> {
    match classified? {
        Ok(bytes) => Ok(bytes),
        Err(plan) => read_ignored_materialization(orch, request, plan).await,
    }
}

/// Read one ignored project path's bytes from the live materialization.
///
/// This is the CAIRN-3048 contract, reached by name so an intercepted `run`
/// search over an ignored path routes exactly where the equivalent read routes,
/// rather than growing a second interpretation of where ignored content lives.
pub(crate) async fn read_ignored_path(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_id: &str,
    repo_path: &str,
    commit_id: &str,
) -> Result<Vec<u8>, String> {
    read_ignored_materialization(
        orch,
        request,
        IgnoredMaterializationRead {
            project_id: project_id.to_string(),
            repo_path: repo_path.to_string(),
            commit_id: commit_id.to_string(),
        },
    )
    .await
}

async fn read_ignored_materialization(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    plan: IgnoredMaterializationRead,
) -> Result<Vec<u8>, String> {
    let (run, _) = super::super::run_context::lookup_run_routed(&orch.db, request).await?;
    if run.project_id != plan.project_id {
        return Err("This file belongs to a different project than this run.".into());
    }
    let repository = cairn_common::executor_protocol::RepositoryIdentity {
        project_id: plan.project_id.clone(),
        repository_id: plan.project_id.clone(),
        object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
    };
    let candidate = orch
        .fleet
        .select_materialization_read_candidate(
            &run.run_id,
            &run.job_id,
            &run.project_id,
            &repository,
            &plan.commit_id,
        )
        .map_err(|kind| format!("{kind:?}"))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let result = orch
        .fleet
        .read_resident_materialization(
            &candidate.executor_id,
            candidate.generation,
            cairn_common::executor_protocol::MaterializationReadRequest {
                fence: candidate.fence,
                cell_id: candidate.cell_id,
                project_id: run.project_id,
                repository,
                base_commit: plan.commit_id,
                materialization_generation: candidate.materialization_generation,
                path: plan.repo_path,
                deadline_unix_ms: now.saturating_add(30_000),
                byte_cap: 32 * 1024 * 1024,
            },
        )
        .await;
    match result {
        cairn_common::executor_protocol::MaterializationReadResult::Bytes { bytes } => Ok(bytes),
        cairn_common::executor_protocol::MaterializationReadResult::Failed { kind, diagnostic } => {
            Err(format!("{kind:?}: {diagnostic}"))
        }
    }
}

fn overlay_entries(
    orch: &Orchestrator,
    context: &BranchReadContext,
) -> Result<Vec<super::object_read::ContentEntry>, String> {
    orch.project_overlays
        .entries(
            &context.project_id,
            &context.repository_path,
            &context.default_commit_id,
            context.service.commit_id(),
            context.service.prefix(),
            context.service.limits(),
        )
        .map_err(|error| error.to_string())
}

fn load_overlay_entries(
    orch: &Orchestrator,
    context: &BranchReadContext,
    entries: &[super::object_read::ContentEntry],
) -> Result<Vec<(String, Vec<u8>)>, String> {
    orch.project_overlays
        .load_entries(
            &context.project_id,
            &context.repository_path,
            entries,
            context.service.limits(),
        )
        .map_err(|error| error.to_string())
}

/// A rendered glob projection body paired with the number of files it stands for.
///
/// The count comes from the matched-path list the projection sliced, never from
/// the rendered string. A body is not a file list in general: an empty result is
/// one line of prose, a timed-out walk carries a trailing warning, and `content`
/// spans many lines per file. Counting lines back out of any of those reports a
/// file count the projection never had — most visibly an absence headed
/// `[1 files]`.
struct GlobProjection {
    body: String,
    /// Files rendered in `body`, after `offset`/`limit` slicing.
    files: usize,
}

/// The rejection every glob projection returns for an unexpected `output_mode`.
/// `parse_file_projection` validates the mode up front, so this is defensive
/// against a value that reached a projection without passing through it.
fn invalid_glob_output_mode(mode: &str) -> String {
    format!("Invalid output_mode '{mode}'. Must be 'content', 'files_with_matches', or 'count'.")
}

fn run_overlay_glob_projection(
    files: Vec<super::object_read::ContentEntry>,
    prefix: &str,
    pattern: &str,
    offset: Option<i64>,
    limit: Option<usize>,
    output_mode: Option<&str>,
    load: impl FnOnce(&[super::object_read::ContentEntry]) -> Result<Vec<(String, Vec<u8>)>, String>,
) -> Result<GlobProjection, String> {
    let matcher = cairn_symbols::search_util::build_glob_matcher(pattern)?;
    let mut files: Vec<_> = files
        .into_iter()
        .filter(|entry| {
            matcher.is_match(&entry.path)
                || std::path::Path::new(&entry.path)
                    .file_name()
                    .is_some_and(|name| matcher.is_match(name))
        })
        .collect();
    files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    if files.is_empty() {
        return Ok(GlobProjection {
            body: crate::mcp::handlers::search::glob_no_matches_body(pattern, prefix),
            files: 0,
        });
    }

    let start = resolve_offset(offset, files.len());
    let files: Vec<_> = files
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .collect();
    let matched = files.len();
    let body = match output_mode.unwrap_or("files_with_matches") {
        "files_with_matches" => files
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        "count" => files
            .iter()
            .map(|entry| format!("{}:1", entry.path))
            .collect::<Vec<_>>()
            .join("\n"),
        "content" => load(&files)?
            .into_iter()
            .map(|(path, bytes)| {
                let body = if bytes.len() as u64 > LARGE_FILE_THRESHOLD {
                    format!(
                        "(file is large: {} bytes — read it directly with offset/limit to view)",
                        bytes.len()
                    )
                } else {
                    String::from_utf8(bytes)
                        .unwrap_or_else(|error| format!("(failed to read: {error})"))
                };
                format!("=== {path} ===\n{body}")
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
        other => return Err(invalid_glob_output_mode(other)),
    };
    Ok(GlobProjection {
        body,
        files: matched,
    })
}

fn overlay_files(
    orch: &Orchestrator,
    context: &BranchReadContext,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    orch.project_overlays
        .files(
            &context.project_id,
            &context.repository_path,
            &context.default_commit_id,
            context.service.commit_id(),
            context.service.prefix(),
            context.service.limits(),
        )
        .map_err(|error| error.to_string())
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

fn object_text_files(
    service: &super::object_read::ObjectReadService,
) -> Result<Vec<(String, String)>, String> {
    Ok(service
        .files()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter_map(|(path, bytes)| String::from_utf8(bytes).ok().map(|text| (path, text)))
        .collect())
}

fn run_object_ast(
    service: &super::object_read::ObjectReadService,
    repo_path: &str,
    pattern: &str,
    glob: Option<&str>,
) -> Result<(crate::symbols::render::Rendered, bool), String> {
    if service.entries().is_ok() {
        Ok((
            crate::symbols::search::search_texts(&object_text_files(service)?, pattern, glob),
            true,
        ))
    } else {
        let source = String::from_utf8(service.bytes().map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
        Ok((
            crate::symbols::search::search_text(
                repo_path,
                &source,
                crate::symbols::engine::lang_for_path(std::path::Path::new(repo_path)),
                pattern,
            ),
            false,
        ))
    }
}

fn run_object_outline(
    service: &super::object_read::ObjectReadService,
    repo_path: &str,
    glob: Option<&str>,
) -> Result<(String, bool), String> {
    if service.entries().is_ok() {
        Ok((
            crate::symbols::outline::outline_texts(&object_text_files(service)?, glob).body,
            true,
        ))
    } else {
        let source = String::from_utf8(service.bytes().map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
        Ok((
            crate::symbols::outline::outline_text(
                &source,
                crate::symbols::engine::lang_for_path(std::path::Path::new(repo_path)),
            ),
            false,
        ))
    }
}

fn format_virtual_directory_listing(
    display_path: &std::path::Path,
    entries: Vec<(String, bool, u64)>,
) -> String {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for (name, is_dir, size) in entries {
        if is_dir {
            dirs.push(name);
        } else {
            files.push((name, size));
        }
    }
    dirs.sort_by_key(|name| name.to_lowercase());
    files.sort_by_key(|(name, _)| name.to_lowercase());
    let mut output = format!("{}/\n", display_path.display());
    for name in dirs {
        output.push_str(&format!("  {name}/\n"));
    }
    for (name, size) in files {
        output.push_str(&format!("  {:<40} {}\n", name, format_file_size(size)));
    }
    output
}

#[cfg(test)]
fn run_object_glob_projection(
    service: &super::object_read::ObjectReadService,
    pattern: &str,
    offset: Option<i64>,
    limit: Option<usize>,
    output_mode: Option<&str>,
) -> Result<String, String> {
    let matcher = cairn_symbols::search_util::build_glob_matcher(pattern)?;
    let mut files: Vec<_> = service
        .files()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|(path, _)| {
            matcher.is_match(path)
                || std::path::Path::new(path)
                    .file_name()
                    .is_some_and(|name| matcher.is_match(name))
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    if files.is_empty() {
        return Ok(crate::mcp::handlers::search::glob_no_matches_body(
            pattern,
            service.prefix(),
        ));
    }
    let start = resolve_offset(offset, files.len());
    let files = files
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX));
    Ok(match output_mode.unwrap_or("files_with_matches") {
        "files_with_matches" => files.map(|(path, _)| path).collect::<Vec<_>>().join("\n"),
        "count" => files
            .map(|(path, _)| format!("{path}:1"))
            .collect::<Vec<_>>()
            .join("\n"),
        "content" => files
            .map(|(path, bytes)| {
                let body = if bytes.len() as u64 > LARGE_FILE_THRESHOLD {
                    format!(
                        "(file is large: {} bytes — read it directly with offset/limit to view)",
                        bytes.len()
                    )
                } else {
                    String::from_utf8(bytes)
                        .unwrap_or_else(|error| format!("(failed to read: {error})"))
                };
                format!("=== {path} ===\n{body}")
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
        other => return Err(invalid_glob_output_mode(other)),
    })
}

#[derive(Debug, Clone)]
enum ReadProjection {
    None,
    Glob {
        pattern: String,
        offset: Option<i64>,
        limit: Option<usize>,
        output_mode: Option<String>,
    },
    Grep(crate::mcp::handlers::search::GrepPayload),
    /// `?ast=<pattern>` structural search over a file or directory tree
    /// (composes with `?glob=`). Sibling to grep; backed by the in-process
    /// ast-grep engine.
    Ast {
        pattern: String,
        glob: Option<String>,
    },
    /// `?outline` signature-skeleton lens over a file or directory tree
    /// (composes with `?glob=`). A flag projection — no pattern.
    Outline {
        glob: Option<String>,
    },
}

type FileProjection = (
    ReadProjection,
    Option<IssueHistoryMode>,
    Option<i64>,
    Option<usize>,
    bool,
    Option<String>,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchTargetKind {
    File,
    Directory,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct TextFileResponse {
    kind: &'static str,
    content: String,
    total_lines: usize,
    shown_lines: usize,
    offset: usize,
    limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<String>,
}

fn find_query_value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .rev()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
}

fn parse_optional_i64(value: Option<&str>, key: &str) -> Result<Option<i64>, String> {
    value
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
        })
        .transpose()
}

fn parse_optional_usize(value: Option<&str>, key: &str) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
        })
        .transpose()
}

fn parse_issue_history_query(value: Option<&str>) -> Result<Option<IssueHistoryMode>, String> {
    value
        .map(|value| match value {
            "true" | "minimal" => Ok(IssueHistoryMode::Minimal),
            "verbose" => Ok(IssueHistoryMode::Verbose),
            _ => Err(format!(
                "Invalid value for query parameter 'issue_history': {value}"
            )),
        })
        .transpose()
}

fn branch_repo_path(relative_path: &str) -> Result<&str, String> {
    if std::path::Path::new(relative_path).is_absolute() || relative_path.contains("..") {
        return Err(
            "?branch is only supported for repository-relative logical file: targets".to_string(),
        );
    }
    Ok(relative_path)
}

fn object_target_kind(
    service: &super::object_read::ObjectReadService,
    rev: &str,
    repo_path: &str,
) -> Result<BranchTargetKind, String> {
    if repo_path.is_empty() {
        return Ok(BranchTargetKind::Directory);
    }
    if service.entries().is_ok() {
        Ok(BranchTargetKind::Directory)
    } else if service.bytes().is_ok() {
        Ok(BranchTargetKind::File)
    } else {
        Err(format!(
            "Entered path does not exist at branch/rev '{rev}': file:{repo_path}"
        ))
    }
}

fn resolve_offset(offset: Option<i64>, total_lines: usize) -> usize {
    match offset.unwrap_or(0) {
        raw if raw < 0 => total_lines.saturating_sub(raw.unsigned_abs() as usize),
        raw => (raw as usize).min(total_lines),
    }
}

/// Render a windowed, line-numbered text view from a file's bytes.
///
/// Single source of truth for the live read pipeline's text rendering: line
/// windowing (offset/limit, negative tail) and `{:>6}\t` line numbering. The
/// per-batch character budget is applied later by the assembler's view layer, so
/// this returns the full windowed content. `char_offset`, when set, skips that
/// many characters of the first shown line's text — the resume half of the
/// single-huge-line character-fallback continuation. The live read fills `bytes`
/// from disk; archival reconstruction fills it from a git blob. Reading through
/// an in-memory cursor preserves `read_line` semantics (UTF-8 validation,
/// final-line-without-newline counting) byte-for-byte against a file read.
fn render_text_from_bytes(
    bytes: &[u8],
    offset: Option<i64>,
    limit: Option<usize>,
    char_offset: Option<usize>,
) -> std::io::Result<TextFileResponse> {
    use std::io::BufRead;

    let total_lines = {
        let mut reader = std::io::BufReader::new(std::io::Cursor::new(bytes));
        let mut line = String::new();
        let mut count = 0usize;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            count += 1;
        }
        count
    };

    let offset = resolve_offset(offset, total_lines);
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(bytes));
    let mut line = String::new();
    let mut content = String::new();
    let mut index = 0usize;
    let mut shown = 0usize;

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if index >= offset {
            if limit.is_some_and(|limit| shown >= limit) {
                break;
            }
            let line_text = line.trim_end_matches(['\r', '\n']);
            let formatted = if shown == 0 {
                if let Some(skip) = char_offset {
                    let trimmed: String = line_text.chars().skip(skip).collect();
                    format!("{:>6}\t{}", index + 1, trimmed)
                } else {
                    format!("{:>6}\t{}", index + 1, line_text)
                }
            } else {
                format!("{:>6}\t{}", index + 1, line_text)
            };
            if !content.is_empty() {
                content.push('\n');
            }
            content.push_str(&formatted);
            shown += 1;
        }
        index += 1;
    }

    Ok(TextFileResponse {
        kind: "text",
        content,
        total_lines,
        shown_lines: shown,
        offset,
        limit,
        history: None,
    })
}

/// Produce the same [`ReadSegment`] the live read producer would have built from
/// `bytes` for a single `file:` target.
///
/// Reconstruction calls this per target in an archived `gitcoord` read and then
/// runs the resulting segments through the very same `assemble` the live
/// `read_batch` ran, so a reconstructed batch reproduces the live envelope
/// byte-for-byte: enriched headers, continue footers, fair-share budgets, and the
/// empty-body header collapse. The query parsing ([`parse_file_projection`]) and
/// the byte renderers (`render_text_from_bytes`, `render_single_file_grep`) are
/// the same ones [`produce_file_segment`] uses, so there is no frozen second copy
/// of the windowing/grep logic — only the disk read is swapped for the resolved
/// git blob. Returns `Err` for any target a single blob cannot faithfully address
/// (a glob projection, a multi-file grep, or an `issue_history` request, whose
/// history is sourced from the DB rather than the blob); the caller then falls to
/// zstd.
pub(crate) fn produce_archived_file_segment(
    target: &str,
    bytes: &[u8],
) -> Result<ReadSegment, String> {
    let split = split_target_query(target)?;
    if !split.identity.starts_with("file:") {
        return Err(format!(
            "archival read target is not a filesystem read: {}",
            split.identity
        ));
    }
    let payload = ReadFilePayload {
        path: target.to_string(),
        offset: None,
        limit: None,
        issue_history: None,
    };
    let (projection, issue_history, offset, limit, _annotations, branch) =
        parse_file_projection(&split.params, &payload)?;
    if branch.is_some() {
        return Err("Archived file reconstruction does not support ?branch; branch reads are resolved live from jj".to_string());
    }
    // History rows come from the DB at read time, not the blob, so a recorded
    // `issue_history` read can never round-trip from a coordinate. The live read
    // appended the history below the body; make the ineligibility explicit instead
    // of rendering a body that will never compare equal.
    if issue_history.is_some() {
        return Err(
            "archival read target requests issue_history, which is sourced from the DB, not the blob"
                .to_string(),
        );
    }
    let char_offset = parse_optional_usize(
        find_query_value(&split.params, "char_offset"),
        "char_offset",
    )?;

    let uri = target.to_string();
    let rel_path = split
        .identity
        .strip_prefix("file:")
        .unwrap_or(&split.identity);
    let path = std::path::Path::new(rel_path);

    match projection {
        ReadProjection::Glob { .. } => Err(
            "archival read target uses a glob projection, which is never gitcoord-addressed"
                .to_string(),
        ),
        ReadProjection::Ast { pattern, glob } => {
            // ast-grep is in-process and stateless: it parses source text with no
            // live-server or whole-tree dependency, so a single-file structural
            // search reconstructs identically from the blob bytes. Only the
            // multi-file `glob` walk is unreproducible from one blob — mirror the
            // grep arm and reject just that case.
            if glob.is_some() {
                return Err(
                    "archival read target uses a multi-file ast search (glob), which is never gitcoord-addressed"
                        .to_string(),
                );
            }
            let src = String::from_utf8_lossy(bytes);
            let lang = crate::symbols::engine::lang_for_path(path);
            let rendered = crate::symbols::search::search_text(rel_path, &src, lang, &pattern);
            let (matches, _files) = grep_counts(&rendered.body);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = Some(matches);
            meta.file_count = None;
            Ok(ReadSegment::text(rendered.body, meta))
        }
        ReadProjection::Outline { glob } => {
            // Single-file outline reconstructs from blob bytes for the same
            // reason ast search does; only the multi-file glob walk cannot.
            if glob.is_some() {
                return Err(
                    "archival read target uses a multi-file outline (glob), which is never gitcoord-addressed"
                        .to_string(),
                );
            }
            let src = String::from_utf8_lossy(bytes);
            let lang = crate::symbols::engine::lang_for_path(path);
            let body = crate::symbols::outline::outline_text(&src, lang);
            let (matches, _files) = grep_counts(&body);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = Some(matches);
            meta.file_count = None;
            Ok(ReadSegment::text(body, meta))
        }
        ReadProjection::Grep(mut grep_payload) => {
            // A single blob addresses exactly one file. A `glob`/`type` push-down
            // means the live read ran multi-file ripgrep over the tree, which a
            // blob cannot reproduce — ineligible.
            if grep_payload.glob.is_some() || grep_payload.file_type.is_some() {
                return Err(
                    "archival read target uses a multi-file grep (glob/type), which is never gitcoord-addressed"
                        .to_string(),
                );
            }
            // Resolve the effective output mode through the very function the
            // live producer uses, with the classification a blob makes certain:
            // one file. It touches no disk, so the path need not exist during
            // reconstruction, and the two can never drift apart.
            let (_single_file, effective_mode) =
                resolve_grep_target_mode(Some(BranchGrepTarget::SingleFile), path, &grep_payload);
            grep_payload.output_mode = Some(effective_mode.clone());

            let label = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            // A failed search reconstructs as the same error segment the live
            // read rendered, not as an ineligible target: the coordinate is
            // faithfully addressable, the search itself is what failed.
            let body = match crate::mcp::handlers::search::render_single_file_grep(
                bytes,
                &label,
                &grep_payload,
            ) {
                Ok(body) => body,
                Err(error) => return Ok(error_segment(uri, error)),
            };
            let (match_count, file_count) =
                grep_body_counts(&body, &effective_mode, &grep_payload.pattern);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = match_count;
            // Mirrors the live producer's single-file grep arm: no file dimension
            // except under `files_with_matches`, whose body is the file list.
            meta.file_count = (effective_mode == "files_with_matches").then_some(file_count);
            Ok(ReadSegment::text(body, meta))
        }
        ReadProjection::None => {
            if let Some(mime_type) = get_image_mime_type(path) {
                let data =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                let mut segment = ReadSegment::text(
                    String::new(),
                    SegmentMeta::new(uri, SegmentKind::Image, NaturalUnit::Line),
                );
                segment.images.push(ImageBlock::inline(mime_type, data));
                Ok(segment)
            } else if let Some(summary) = render_mat_file_summary(path, bytes) {
                let body =
                    summary.map_err(|error| format!("Failed to render MAT file: {error}"))?;
                Ok(ReadSegment::text(
                    body,
                    SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line),
                ))
            } else {
                let response = render_text_from_bytes(bytes, offset, limit, char_offset)
                    .map_err(|error| format!("Failed to render archived read: {error}"))?;
                let mut meta = SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line);
                meta.total_units = Some(response.total_lines);
                meta.shown_units = response.shown_lines;
                meta.offset = response.offset;
                meta.limit = response.limit;
                meta.char_continuation = char_offset.is_some();
                Ok(ReadSegment {
                    body: response.content,
                    affordance: None,
                    images: Vec::new(),
                    history: None,
                    meta,
                })
            }
        }
    }
}

fn parse_file_projection(
    params: &[QueryParam],
    payload: &ReadFilePayload,
) -> Result<FileProjection, String> {
    let query_issue_history = parse_issue_history_query(find_query_value(params, "issue_history"))?;
    let query_offset = parse_optional_i64(find_query_value(params, "offset"), "offset")?;
    let query_limit = parse_optional_usize(find_query_value(params, "limit"), "limit")?;
    let grep = find_query_value(params, "grep");
    let glob = find_query_value(params, "glob");
    let ast = find_query_value(params, "ast");
    let outline = find_query_value(params, "outline");
    let annotations_suppressed = find_query_value(params, "annotations") == Some("none");
    let branch = find_query_value(params, "branch")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if find_query_value(params, "branch") == Some("") {
        return Err(
            "Empty 'branch' value; provide a bookmark, commit/change id, or node URI".to_string(),
        );
    }

    if find_query_value(params, "search").is_some() {
        return Err(
            "'search' is only supported on cairn:// collection resources, not filesystem reads"
                .to_string(),
        );
    }
    if find_query_value(params, "path").is_some() {
        return Err("Query parameter 'path' is not supported on read targets; use the read path itself as the identity target".to_string());
    }

    let issue_history = payload.issue_history.clone().or(query_issue_history);
    let offset = payload.offset.or(query_offset);
    let limit = payload.limit.or(query_limit);

    // An empty `grep=` is most often someone who meant a plain (line-window)
    // read, so report that before the grep+offset/limit conflict — the
    // "omit grep" guidance is more actionable than the head_limit redirect when
    // there is no pattern at all.
    if grep == Some("") {
        return Err(
            "Empty 'grep' pattern; omit 'grep' for a plain read or provide a search pattern"
                .to_string(),
        );
    }
    // grep paginates by matches, not by line window. offset slices raw
    // lines, so the two combined are ambiguous. `limit` aliases to `head_limit`
    // when no explicit head_limit is present. Checked
    // before the allowed-keys scan so the message is specific rather than a
    // generic "unsupported parameter". offset/limit may arrive via the query
    // string (direct callers) or the payload (cairn-cmd peels them off); both
    // are folded into the locals above.
    if grep.is_some() && offset.is_some() {
        return Err("'offset' is a line-window and does not combine with 'grep'; use 'head_limit' or 'limit' to cap the number of matches".to_string());
    }

    let allowed_keys = if grep.is_some() {
        [
            "grep",
            "glob",
            "type",
            "output_mode",
            "context",
            "-A",
            "-B",
            "-C",
            "-i",
            "-n",
            "head_limit",
            "limit",
            "multiline",
            "branch",
        ]
        .as_slice()
    } else if ast.is_some() {
        ["ast", "glob", "branch"].as_slice()
    } else if outline.is_some() {
        ["outline", "glob", "branch"].as_slice()
    } else if glob.is_some() {
        ["glob", "offset", "limit", "output_mode", "branch"].as_slice()
    } else {
        [
            "offset",
            "limit",
            "issue_history",
            "annotations",
            "char_offset",
            "branch",
        ]
        .as_slice()
    };

    if let Some(unsupported) = params
        .iter()
        .find(|param| !allowed_keys.contains(&param.key.as_str()))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for filesystem read target",
            unsupported.key
        ));
    }

    let projection = if let Some(pattern) = grep {
        // The grep field mapping is shared with the universal body-grep parser
        // (`search::build_grep_payload`) so there is no frozen second copy.
        ReadProjection::Grep(crate::mcp::handlers::search::build_grep_payload(
            params,
            pattern.to_string(),
            glob.map(|value| value.to_string()),
            find_query_value(params, "type").map(|value| value.to_string()),
            find_query_value(params, "output_mode").map(|value| value.to_string()),
            None,
            limit.and_then(|value| u32::try_from(value).ok()),
        )?)
    } else if let Some(pattern) = ast {
        if pattern.is_empty() {
            return Err(
                "Empty 'ast' pattern; provide a structural pattern, e.g. ast=$RECV.unwrap()"
                    .to_string(),
            );
        }
        ReadProjection::Ast {
            pattern: pattern.to_string(),
            glob: glob.map(|value| value.to_string()),
        }
    } else if outline.is_some() {
        ReadProjection::Outline {
            glob: glob.map(|value| value.to_string()),
        }
    } else if let Some(pattern) = glob {
        let output_mode = find_query_value(params, "output_mode").map(|value| value.to_string());
        if let Some(mode) = output_mode.as_deref() {
            if !matches!(mode, "files_with_matches" | "content" | "count") {
                return Err(format!(
                    "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
                    mode
                ));
            }
        }
        ReadProjection::Glob {
            pattern: pattern.to_string(),
            offset,
            limit,
            output_mode,
        }
    } else {
        ReadProjection::None
    };

    Ok((
        projection,
        issue_history,
        offset,
        limit,
        annotations_suppressed,
        branch,
    ))
}

/// Run a glob filesystem projection in the requested output mode.
///
/// - `files_with_matches` (default): the matched paths, sliced by offset/limit.
/// - `count`: one `path:1` line per matched file, sliced by offset/limit. A
///   glob path matches exactly once, so the count is always 1 — this keeps the
///   `path:count` shape consistent with grep and the `/changed` projection.
/// - `content`: the contents of the matched files (sliced by offset/limit),
///   each under a `=== <path> ===` header, with oversized files elided.
///
/// Every mode renders from the one matched-path list, so the projection reports
/// its own file count, and a failed walk returns `Err` rather than folding its
/// message into a body the header would then count as a result.
async fn run_glob_projection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    offset: Option<i64>,
    limit: Option<usize>,
    output_mode: Option<&str>,
) -> Result<GlobProjection, String> {
    use crate::mcp::handlers::search::{
        glob_matched_paths, glob_no_matches_body, glob_timeout_warning,
    };

    let matches = glob_matched_paths(orch, request).await?;

    // Nothing matched at all: say so, once, in the shared wording. A window that
    // overshoots a non-empty match set is a different thing and renders empty.
    if matches.paths.is_empty() {
        let mut body = glob_no_matches_body(&matches.pattern, matches.search_dir.display());
        if matches.timed_out {
            body.push_str(&glob_timeout_warning());
        }
        return Ok(GlobProjection { body, files: 0 });
    }

    let start = resolve_offset(offset, matches.paths.len());
    let window: Vec<&std::path::PathBuf> = matches
        .paths
        .iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .collect();

    let mut body = match output_mode.unwrap_or("files_with_matches") {
        "files_with_matches" => window
            .iter()
            .map(|rel| rel.display().to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        "count" => window
            .iter()
            .map(|rel| format!("{}:1", rel.display()))
            .collect::<Vec<_>>()
            .join("\n"),
        "content" => window
            .iter()
            .map(|rel| {
                let body = read_glob_content_file(&matches.search_dir.join(rel));
                format!("=== {} ===\n{}", rel.display(), body)
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
        other => return Err(invalid_glob_output_mode(other)),
    };
    if matches.timed_out {
        body.push_str(&glob_timeout_warning());
    }

    Ok(GlobProjection {
        body,
        files: window.len(),
    })
}

/// Read one file for a glob `content` projection, eliding files past the
/// large-file threshold so a broad glob can't dump megabytes at once.
fn read_glob_content_file(path: &std::path::Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() > LARGE_FILE_THRESHOLD => format!(
            "(file is large: {} bytes — read it directly with offset/limit to view)",
            meta.len()
        ),
        Ok(_) => match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) => format!("(failed to read: {})", error),
        },
        Err(error) => format!("(failed to stat: {})", error),
    }
}

/// String entry for a single `file:` or `cairn://` read.
///
/// Used by the permission preview and the worktree-fence re-dispatch of a single
/// read. The batch path calls [`produce_file_segment`] directly; this wrapper
/// renders one produced segment to its composed text.
pub async fn handle_read_file(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: ReadFilePayload = match crate::mcp::handlers::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    let split = match split_target_query(&payload.path) {
        Ok(split) => split,
        Err(error) => return format!("Invalid read target: {error}"),
    };

    if split.identity.starts_with("cairn://") {
        let resource = match parse_uri(&split.identity) {
            Some(resource) => resource,
            None => return format!("Invalid cairn resource URI: {}", split.identity),
        };
        let resource_request = McpCallbackRequest {
            thread_id: None,
            cwd: request.cwd.clone(),
            run_id: request.run_id.clone(),
            tool: request.tool.clone(),
            payload: serde_json::json!({ "uri": payload.path }),
            tool_use_id: request.tool_use_id.clone(),
        };
        return match &resource {
            CairnResource::NodeTerminal { .. } | CairnResource::ProjectTerminal { .. } => {
                let read_cursors = std::sync::Mutex::new(std::collections::HashMap::new());
                crate::mcp::handlers::resources::handle_read_resource(
                    orch,
                    &resource_request,
                    &read_cursors,
                )
                .await
            }
            _ => {
                crate::mcp::handlers::issue_resources::handle_read_issue_resource(
                    orch,
                    &resource_request,
                )
                .await
            }
        };
    }

    if !split.identity.starts_with("file:") {
        return invalid_target_error(&split.identity);
    }

    match produce_file_segment(orch, request, &payload).await {
        Produced::Segment(segment) => {
            crate::mcp::handlers::read::view::render_segment(
                segment,
                crate::mcp::handlers::read::view::READ_BATCH_CHAR_BUDGET,
            )
            .text
        }
        Produced::Suspended(message) => message,
    }
}

/// Produce a structured [`ReadSegment`] for a `file:` read target.
///
/// Owns the worktree-fence gate, path validation, and the text / grep / glob /
/// image / directory arms. A fence suspension returns [`Produced::Suspended`] so
/// the whole batch aborts and the permission flow can re-dispatch it; every
/// other failure becomes an inline `Error`-kind segment so a partial failure
/// never aborts the batch.
pub(crate) async fn produce_file_segment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &ReadFilePayload,
) -> Produced {
    let uri = payload.path.clone();
    let split = match split_target_query(&payload.path) {
        Ok(split) => split,
        Err(error) => {
            return Produced::Segment(error_segment(uri, format!("Invalid read target: {error}")))
        }
    };

    let (projection, issue_history, offset, limit, _annotations_suppressed, branch) =
        match parse_file_projection(&split.params, payload) {
            Ok(parsed) => parsed,
            Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
        };
    let char_offset = match parse_optional_usize(
        find_query_value(&split.params, "char_offset"),
        "char_offset",
    ) {
        Ok(value) => value,
        Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
    };

    let worktree = std::path::Path::new(&request.cwd);

    // Explicit host crossings are gated before existence validation. Logical
    // relative targets never derive authority from process cwd.
    if let Ok(full) = crate::mcp::file_targets::resolve_file_path_lenient(worktree, &split.identity)
    {
        // The desktop operator credential is refused rather than prompted, and
        // the check is deliberately independent of the read denylist below: a
        // denied read raises an APPROVABLE crossing, and allowing a containment
        // crossing is something an agent may do through its own `permissions`
        // resource. A prompt here would therefore be one self-approval away
        // from disclosing the credential that approves authority.
        if let Some(refusal) =
            crate::authorization::protected::read_refusal(&orch.config_dir, &full)
        {
            return Produced::Segment(error_segment(uri, refusal.to_string()));
        }
        if crate::mcp::file_targets::target_crosses_logical_root(&split.identity).unwrap_or(true)
            && crate::mcp::file_targets::path_within_any(&full, &orch.sandbox_deny_read())
        {
            use crate::mcp::handlers::fence;
            if let Some((run_id, fence_mode)) = fence::resolve_run_fence(orch, request).await {
                match fence::raise_fence(
                    orch,
                    &run_id,
                    fence_mode,
                    request,
                    fence::Crossing::read_denied(&full),
                )
                .await
                {
                    fence::FenceDecision::Allow => {}
                    fence::FenceDecision::Deny(msg) => {
                        return Produced::Segment(error_segment(uri, msg))
                    }
                    fence::FenceDecision::Suspended => {
                        return Produced::Suspended(
                            "Read suspended pending logical namespace approval; resume will \
                             continue once it is answered."
                                .to_string(),
                        );
                    }
                }
            }
        }
    }

    let logical_target = crate::mcp::file_targets::resolve_logical_file_target(&split.identity);
    let logical_project_target = logical_target.is_ok()
        && super::super::run_context::lookup_run_routed(&orch.db, request)
            .await
            .is_ok();
    let resolved_target = match if branch.is_some() || logical_project_target {
        logical_target
    } else {
        validate_read_path(worktree, &split.identity)
    } {
        Ok(target) => target,
        Err(error) => {
            return Produced::Segment(error_segment(uri, format!("Invalid file target: {error}")))
        }
    };

    let branch_context = if branch.is_some() || logical_project_target {
        let repo_path = match branch_repo_path(&resolved_target.relative_path) {
            Ok(path) => path.to_string(),
            Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
        };
        let resolution = match if let Some(branch) = branch.as_deref() {
            crate::mcp::handlers::branch::resolve_for_read(orch, request, branch).await
        } else {
            crate::mcp::handlers::branch::resolve_current_for_read(orch, request).await
        } {
            Ok(resolution) => resolution,
            Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
        };
        let repository_path = resolution.object_repository_path;
        let service = match super::object_read::ObjectReadService::new(
            repository_path.clone(),
            resolution.commit_id,
            repo_path.clone(),
        ) {
            Ok(service) => service,
            Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
        };
        Some(BranchReadContext {
            service,
            repo_path,
            project_id: resolution.project_id,
            repository_path,
            default_commit_id: resolution.default_commit_id,
        })
    } else {
        None
    };

    match projection {
        ReadProjection::Glob {
            pattern,
            offset,
            limit,
            output_mode,
        } => {
            let projection = if let Some(context) = branch_context.as_ref() {
                overlay_entries(orch, context).and_then(|files| {
                    run_overlay_glob_projection(
                        files,
                        context.service.prefix(),
                        &pattern,
                        offset,
                        limit,
                        output_mode.as_deref(),
                        |entries| load_overlay_entries(orch, context, entries),
                    )
                })
            } else {
                let glob_request = McpCallbackRequest {
                    thread_id: None,
                    cwd: request.cwd.clone(),
                    run_id: request.run_id.clone(),
                    tool: "read".to_string(),
                    payload: serde_json::json!({
                        "pattern": pattern,
                        "path": resolved_target.full_path,
                    }),
                    tool_use_id: request.tool_use_id.clone(),
                };
                run_glob_projection(orch, &glob_request, offset, limit, output_mode.as_deref())
                    .await
            };
            let projection = match projection {
                Ok(projection) => projection,
                Err(error) => return Produced::Segment(error_segment(uri, error)),
            };
            let mut meta = SegmentMeta::new(uri, SegmentKind::Glob, NaturalUnit::File);
            // `[N files]` describes a body that *is* the file list, so only
            // `files_with_matches` carries it; a `count` tally or `content` dump
            // reports no suffix. The count is the projection's own, so a pattern
            // that matched nothing reads `[0 files]` rather than counting the one
            // line of prose that says so.
            if output_mode.as_deref().unwrap_or("files_with_matches") == "files_with_matches" {
                meta.file_count = Some(projection.files);
            }
            Produced::Segment(ReadSegment::text(projection.body, meta))
        }
        ReadProjection::Grep(mut grep_payload) => {
            let full_path = &resolved_target.full_path;
            let branch_target = if let Some(context) = branch_context.as_ref() {
                match classify_branch_grep_target(context) {
                    Ok(target) => Some(target),
                    Err(error) => return Produced::Segment(error_segment(uri, error)),
                }
            } else {
                None
            };
            let (single_file, effective_mode) =
                resolve_grep_target_mode(branch_target, full_path, &grep_payload);
            grep_payload.output_mode = Some(effective_mode.clone());

            // Every producer below returns `Err` for a failed search rather than
            // folding the failure text into its body, so an error can never reach
            // the count/header projection and be presented as a result.
            let rendered = if let Some(context) = branch_context.as_ref() {
                if single_file {
                    let bytes = match classify_branch_direct_read(context) {
                        Ok(Ok(bytes)) => Ok(bytes),
                        Ok(Err(plan)) => read_ignored_materialization(orch, request, plan).await,
                        Err(error) => Err(error),
                    };
                    match bytes {
                        Ok(bytes) => {
                            let label = full_path
                                .file_name()
                                .map(|name| name.to_string_lossy().into_owned())
                                .unwrap_or_default();
                            crate::mcp::handlers::search::render_single_file_grep(
                                &bytes,
                                &label,
                                &grep_payload,
                            )
                        }
                        Err(error) => {
                            return Produced::Segment(error_segment(uri, error.to_string()))
                        }
                    }
                } else {
                    match overlay_files(orch, context) {
                        Ok(files) => {
                            crate::mcp::handlers::search::render_tree_grep(&files, &grep_payload)
                        }
                        Err(error) => {
                            return Produced::Segment(error_segment(uri, error.to_string()))
                        }
                    }
                }
            } else if single_file {
                match std::fs::read(full_path) {
                    Ok(bytes) => {
                        let label = full_path
                            .file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        crate::mcp::handlers::search::render_single_file_grep(
                            &bytes,
                            &label,
                            &grep_payload,
                        )
                    }
                    Err(error) => {
                        return Produced::Segment(error_segment(
                            uri,
                            format!("Failed to read file: {error}"),
                        ))
                    }
                }
            } else {
                grep_payload.path = Some(full_path.display().to_string());
                let grep_request = McpCallbackRequest {
                    thread_id: None,
                    cwd: request.cwd.clone(),
                    run_id: request.run_id.clone(),
                    tool: "read".to_string(),
                    payload: serde_json::to_value(&grep_payload).unwrap_or_default(),
                    tool_use_id: request.tool_use_id.clone(),
                };
                crate::mcp::handlers::search::handle_grep(orch, &grep_request).await
            };
            let body = match rendered {
                Ok(body) => body,
                Err(error) => return Produced::Segment(error_segment(uri, error)),
            };
            let (match_count, file_count) =
                grep_body_counts(&body, &effective_mode, &grep_payload.pattern);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = match_count;
            // A `content`/`count` body over one named file has no useful file
            // dimension — the caller named the file. A `files_with_matches` body
            // *is* the file dimension, so it always reports one, and a match is
            // never rendered as a bare filename with no count beside it.
            meta.file_count =
                (!single_file || effective_mode == "files_with_matches").then_some(file_count);
            Produced::Segment(ReadSegment::text(body, meta))
        }
        ReadProjection::Ast { pattern, glob } => {
            let (rendered, is_directory) = if let Some(context) = branch_context.as_ref() {
                let result = if context.service.entries().is_ok() {
                    overlay_files(orch, context).map(|files| {
                        let texts = files
                            .into_iter()
                            .filter_map(|(path, bytes)| {
                                String::from_utf8(bytes).ok().map(|text| (path, text))
                            })
                            .collect::<Vec<_>>();
                        (
                            crate::symbols::search::search_texts(&texts, &pattern, glob.as_deref()),
                            true,
                        )
                    })
                } else {
                    run_object_ast(
                        &context.service,
                        &context.repo_path,
                        &pattern,
                        glob.as_deref(),
                    )
                };
                match result {
                    Ok(value) => value,
                    Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
                }
            } else {
                let target = &resolved_target.full_path;
                (
                    crate::symbols::search::search(worktree, target, &pattern, glob.as_deref()),
                    target.is_dir(),
                )
            };
            let (matches, files) = grep_counts(&rendered.body);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = Some(matches);
            if is_directory {
                meta.file_count = Some(files);
            }
            Produced::Segment(ReadSegment::text(rendered.body, meta))
        }
        ReadProjection::Outline { glob } => {
            let (body, is_directory) = if let Some(context) = branch_context.as_ref() {
                let result = if context.service.entries().is_ok() {
                    overlay_files(orch, context).map(|files| {
                        let texts = files
                            .into_iter()
                            .filter_map(|(path, bytes)| {
                                String::from_utf8(bytes).ok().map(|text| (path, text))
                            })
                            .collect::<Vec<_>>();
                        (
                            crate::symbols::outline::outline_texts(&texts, glob.as_deref()).body,
                            true,
                        )
                    })
                } else {
                    run_object_outline(&context.service, &context.repo_path, glob.as_deref())
                };
                match result {
                    Ok(value) => value,
                    Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
                }
            } else {
                let target = &resolved_target.full_path;
                (
                    crate::symbols::outline::outline(worktree, target, glob.as_deref()).body,
                    target.is_dir(),
                )
            };
            let (matches, _files) = grep_counts(&body);
            let mut meta = SegmentMeta::new(uri, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = Some(matches);
            if is_directory {
                meta.file_count = Some(crate::symbols::outline::file_count(&body));
            }
            Produced::Segment(ReadSegment::text(body, meta))
        }
        ReadProjection::None => {
            let full_path = &resolved_target.full_path;
            let branch_kind = if let Some(context) = branch_context.as_ref() {
                match object_target_kind(
                    &context.service,
                    context.service.commit_id(),
                    &context.repo_path,
                ) {
                    Ok(kind) => Some(kind),
                    Err(_)
                        if context
                            .service
                            .is_ignored_path(&context.repo_path)
                            .unwrap_or(false) =>
                    {
                        None
                    }
                    Err(error) => return Produced::Segment(error_segment(uri, error.to_string())),
                }
            } else {
                None
            };

            if branch_kind
                .map(|kind| kind == BranchTargetKind::Directory)
                .unwrap_or_else(|| full_path.is_dir())
            {
                let body = if let Some(context) = branch_context.as_ref() {
                    match context.service.listing() {
                        Ok(entries) => format_virtual_directory_listing(full_path, entries),
                        Err(error) => {
                            return Produced::Segment(error_segment(uri, error.to_string()))
                        }
                    }
                } else {
                    format_directory_listing(full_path)
                };
                return Produced::Segment(ReadSegment::text(
                    body,
                    SegmentMeta::new(uri, SegmentKind::Directory, NaturalUnit::Line),
                ));
            }

            if let Some(mime_type) = get_image_mime_type(full_path) {
                let bytes = if let Some(context) = branch_context.as_ref() {
                    match resolve_branch_direct_read(
                        orch,
                        request,
                        classify_branch_direct_read(context),
                    )
                    .await
                    {
                        Ok(bytes) => bytes,
                        Err(error) => return Produced::Segment(error_segment(uri, error)),
                    }
                } else {
                    match std::fs::read(full_path) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            return Produced::Segment(error_segment(
                                uri,
                                format!("Failed to read file: {error}"),
                            ))
                        }
                    }
                };
                let data =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                let mut segment = ReadSegment::text(
                    String::new(),
                    SegmentMeta::new(uri, SegmentKind::Image, NaturalUnit::Line),
                );
                segment.images.push(ImageBlock::inline(mime_type, data));
                return Produced::Segment(segment);
            }

            let bytes = if let Some(context) = branch_context.as_ref() {
                match resolve_branch_direct_read(
                    orch,
                    request,
                    classify_branch_direct_read(context),
                )
                .await
                {
                    Ok(bytes) => bytes,
                    Err(error) => return Produced::Segment(error_segment(uri, error)),
                }
            } else {
                match std::fs::read(full_path) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return Produced::Segment(error_segment(
                            uri,
                            format!("Failed to read file: {error}"),
                        ))
                    }
                }
            };
            if let Some(summary) = render_mat_file_summary(full_path, &bytes) {
                return match summary {
                    Ok(body) => Produced::Segment(ReadSegment::text(
                        body,
                        SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line),
                    )),
                    Err(error) => Produced::Segment(error_segment(
                        uri,
                        format!("Failed to render MAT file: {error}"),
                    )),
                };
            }
            let response = match render_text_from_bytes(&bytes, offset, limit, char_offset) {
                Ok(response) => response,
                Err(error) => {
                    return Produced::Segment(error_segment(
                        uri,
                        format!("Failed to read file: {error}"),
                    ))
                }
            };

            let history = if let Some(ref mode) = issue_history {
                let history =
                    get_file_issue_history(orch, &resolved_target.relative_path, mode).await;
                if history.is_empty() {
                    None
                } else {
                    Some(history)
                }
            } else {
                None
            };

            let mut meta = SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line);
            meta.total_units = Some(response.total_lines);
            meta.shown_units = response.shown_lines;
            meta.offset = response.offset;
            meta.limit = response.limit;
            meta.char_continuation = char_offset.is_some();
            Produced::Segment(ReadSegment {
                body: response.content,
                affordance: None,
                images: Vec::new(),
                history,
                meta,
            })
        }
    }
}

struct FileIssueHistoryRow {
    status: String,
    additions: Option<i64>,
    deletions: Option<i64>,
    created_at: i64,
    issue_number: i32,
    issue_title: String,
    project_key: String,
    pr_number: Option<i64>,
    pr_url: Option<String>,
}

/// Get issue history for a file path.
async fn get_file_issue_history(
    orch: &Orchestrator,
    file_path: &str,
    mode: &IssueHistoryMode,
) -> String {
    let normalized_path = file_path.trim_start_matches("./").trim_start_matches('/');
    let rows = orch
        .db
        .local
        .read(|conn| {
            let normalized_path = normalized_path.to_string();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT fc.status, fc.additions, fc.deletions, fc.created_at,
                               i.number, i.title, p.key,
                               mr.github_pr_number, mr.github_pr_url
                        FROM file_changes fc
                        JOIN jobs j ON fc.job_id = j.id
                        JOIN issues i ON j.issue_id = i.id
                        JOIN projects p ON i.project_id = p.id
                        LEFT JOIN merge_requests mr ON mr.job_id = j.id
                        WHERE fc.file_path = ?1
                        ORDER BY fc.created_at DESC
                        LIMIT 20
                        ",
                        params![normalized_path.as_str()],
                    )
                    .await?;

                let mut history = Vec::new();
                while let Some(row) = rows.next().await? {
                    history.push(FileIssueHistoryRow {
                        status: row.text(0)?,
                        additions: row.opt_i64(1)?,
                        deletions: row.opt_i64(2)?,
                        created_at: row.i64(3)?,
                        issue_number: row.i64(4)? as i32,
                        issue_title: row.text(5)?,
                        project_key: row.text(6)?,
                        pr_number: row.opt_i64(7)?,
                        pr_url: row.opt_text(8)?,
                    });
                }
                Ok(history)
            })
        })
        .await
        .unwrap_or_default();

    if rows.is_empty() {
        return String::new();
    }

    let mut output = String::from("## Issue History\n\n");
    for row in rows {
        let date = crate::clock::date(row.created_at).unwrap_or_else(|| row.created_at.to_string());
        output.push_str(&format!(
            "- **{}-{}:** {}\n",
            row.project_key, row.issue_number, row.issue_title
        ));
        output.push_str(&format!("- **Change:** {} on {}\n", row.status, date));

        if matches!(mode, IssueHistoryMode::Verbose) {
            if row.additions.is_some() || row.deletions.is_some() {
                output.push_str(&format!(
                    "- **Stats:** +{} -{}\n",
                    row.additions.unwrap_or(0),
                    row.deletions.unwrap_or(0)
                ));
            }
            if let (Some(number), Some(url)) = (row.pr_number, row.pr_url.as_deref()) {
                output.push_str(&format!("- **PR:** [#{}]({})\n", number, url));
            }
            output.push_str(&format!(
                "- **URI:** {}\n",
                build_issue_uri(&row.project_key, row.issue_number)
            ));
        }

        output.push('\n');
    }

    output
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_codec::testutil::{commit_all, git, init_repo, write_file};
    use cairn_common::query::parse_query_params;

    #[test]
    fn level5_mat_file_summary_lists_numeric_variables_and_values() {
        let bytes = include_bytes!("../../../../tests/fixtures/matlab/level5.mat");
        let summary = render_mat_file_summary(std::path::Path::new("data.mat"), bytes)
            .expect("recognized MAT file")
            .expect("rendered summary");
        assert!(summary.contains("MATLAB Level 5"), "{summary}");
        // Fixture: matfile's MIT-licensed tests/two_arrays.mat. Keeping a known
        // parser fixture here distinguishes our summary contract from generator quirks.
        assert!(summary.contains("`A`: double [2 × 2]"), "{summary}");
        assert!(summary.contains("preview: [1, 3, 2, 4]"), "{summary}");
        assert!(summary.contains("`B`: double [2 × 3]"), "{summary}");
    }

    #[test]
    fn v73_mat_file_summary_identifies_hdf5_and_guides_deep_inspection() {
        let bytes = include_bytes!("../../../../tests/fixtures/matlab/v73.mat");
        let summary = render_mat_file_summary(std::path::Path::new("data.mat"), bytes)
            .expect("recognized MAT file")
            .expect("rendered summary");
        assert!(summary.contains("MATLAB v7.3 (HDF5)"), "{summary}");
        assert!(summary.contains("Created on:"), "{summary}");
        assert!(
            summary.contains("cairn://skills/matlab/scripts/inspect-mat.py"),
            "{summary}"
        );
    }

    #[test]
    fn mat_extension_without_mat_magic_is_not_claimed() {
        assert!(
            render_mat_file_summary(std::path::Path::new("notes.mat"), b"plain text").is_none()
        );
    }

    fn legacy_glob_projection(
        root: &std::path::Path,
        pattern: &str,
        offset: Option<i64>,
        limit: Option<usize>,
        output_mode: Option<&str>,
    ) -> String {
        let matcher = cairn_symbols::search_util::build_glob_matcher(pattern).unwrap();
        let mut files: Vec<_> = ignore::WalkBuilder::new(root)
            .hidden(false)
            .build()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_some_and(|kind| kind.is_file()))
            .filter_map(|entry| {
                let relative = entry.path().strip_prefix(root).ok()?.to_path_buf();
                (matcher.is_match(&relative)
                    || relative
                        .file_name()
                        .is_some_and(|name| matcher.is_match(name)))
                .then_some(relative)
            })
            .collect();
        files.sort();
        let start = resolve_offset(offset, files.len());
        let files = files
            .into_iter()
            .skip(start)
            .take(limit.unwrap_or(usize::MAX));
        match output_mode.unwrap_or("files_with_matches") {
            "files_with_matches" => files
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("\n"),
            "count" => files
                .map(|path| format!("{}:1", path.display()))
                .collect::<Vec<_>>()
                .join("\n"),
            "content" => files
                .map(|path| {
                    let bytes = std::fs::read(root.join(&path)).unwrap();
                    let body = if bytes.len() as u64 > LARGE_FILE_THRESHOLD {
                        format!(
                            "(file is large: {} bytes — read it directly with offset/limit to view)",
                            bytes.len()
                        )
                    } else {
                        String::from_utf8(bytes)
                            .unwrap_or_else(|error| format!("(failed to read: {error})"))
                    };
                    format!("=== {} ===\n{body}", path.display())
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
            other => panic!("unexpected mode {other}"),
        }
    }

    fn projection_fixture() -> (
        tempfile::TempDir,
        super::super::object_read::ObjectReadService,
        String,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "fixture/base-only.txt", b"removed before head\n");
        let base = commit_all(repo, "overlay base");
        std::fs::remove_file(repo.join("fixture/base-only.txt")).unwrap();
        write_file(repo, "fixture/.gitignore", b"ignored/*\n!ignored/keep.rs\n");
        write_file(
            repo,
            "fixture/src/lib.rs",
            b"pub fn visible() {\n    println!(\"needle\");\n}\n",
        );
        write_file(
            repo,
            "fixture/src/app.ts",
            b"export function app() { return 'needle' }\n",
        );
        write_file(repo, "fixture/ignored/drop.rs", b"fn dropped() {}\n");
        write_file(repo, "fixture/ignored/keep.rs", b"fn kept() {}\n");
        write_file(repo, "fixture/notes.txt", b"before\nneedle\nafter\n");
        write_file(repo, "fixture/binary.bin", b"needle\0binary\n");
        write_file(repo, "fixture/image.png", b"\x89PNG\r\n\x1a\nfixture");
        write_file(
            repo,
            "fixture/large.txt",
            &vec![b'x'; LARGE_FILE_THRESHOLD as usize + 1],
        );
        git(repo, &["add", "-f", "."]);
        let commit = commit_all(repo, "projection parity");
        let service = super::super::object_read::ObjectReadService::new(
            repo.to_path_buf(),
            commit,
            "fixture".to_string(),
        )
        .unwrap();
        (dir, service, base)
    }

    #[test]
    fn overlay_glob_projection_reports_zero_files_when_nothing_matched() {
        // A pattern that matches nothing renders one line of prose. Counting the
        // rendered body's lines would head that absence `[1 files]`.
        let (dir, service, base) = projection_fixture();
        let registry = super::super::overlay::ProjectOverlayRegistry::default();
        let entries = registry
            .entries(
                "project",
                dir.path(),
                &base,
                service.commit_id(),
                "fixture",
                service.limits(),
            )
            .unwrap();

        for mode in ["files_with_matches", "count", "content"] {
            let projection = run_overlay_glob_projection(
                entries.clone(),
                service.prefix(),
                "render*",
                None,
                None,
                Some(mode),
                |_| Ok(Vec::new()),
            )
            .unwrap();
            assert_eq!(projection.files, 0, "mode {mode}");
            assert_eq!(
                projection.body, "No files matched pattern 'render*' in fixture",
                "mode {mode}"
            );
        }
    }

    #[test]
    fn ignored_direct_grep_routes_materialization_bytes_as_a_single_file() {
        let (dir, service, _) = projection_fixture();
        let ignored_path = "fixture/ignored/generated.log";
        assert!(
            !dir.path().join(ignored_path).exists(),
            "the runner filesystem must not provide the ignored artifact"
        );
        let context = BranchReadContext {
            service: super::super::object_read::ObjectReadService::new(
                dir.path().to_path_buf(),
                service.commit_id().to_string(),
                ignored_path.to_string(),
            )
            .unwrap(),
            repo_path: ignored_path.to_string(),
            project_id: "project".into(),
            repository_path: dir.path().to_path_buf(),
            default_commit_id: service.commit_id().to_string(),
        };

        assert_eq!(
            classify_branch_grep_target(&context).unwrap(),
            BranchGrepTarget::SingleFile
        );
        let plan = classify_branch_direct_read(&context)
            .unwrap()
            .expect_err("ignored object-absent path must request a live materialization read");
        assert_eq!(plan.repo_path, ignored_path);

        // Model the bounded executor response. Rendering these bytes as one file is
        // the observable contract; a tree grep would exclude this ignored path.
        let fake_materialization_bytes = b"before\nerror: generated failure\nafter\n";
        let grep = crate::mcp::handlers::search::GrepPayload {
            pattern: "error".into(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some("content".into()),
            context: None,
            after_context: None,
            before_context: None,
            context_alias: None,
            case_insensitive: None,
            line_numbers: Some(true),
            head_limit: None,
            offset: None,
            multiline: None,
        };
        let rendered = crate::mcp::handlers::search::render_single_file_grep(
            fake_materialization_bytes,
            "generated.log",
            &grep,
        )
        .unwrap();
        assert!(
            rendered.contains("2:error: generated failure"),
            "{rendered}"
        );
    }

    #[test]
    fn store_native_branch_projections_match_materialized_filesystem() {
        let (dir, service, base) = projection_fixture();
        let root = dir.path().join("fixture");
        let registry = super::super::overlay::ProjectOverlayRegistry::default();
        let overlay_entries = registry
            .entries(
                "project",
                dir.path(),
                &base,
                service.commit_id(),
                "fixture",
                service.limits(),
            )
            .unwrap();
        let overlay_files = registry
            .load_entries("project", dir.path(), &overlay_entries, service.limits())
            .unwrap();
        assert_eq!(overlay_files, service.files().unwrap());

        for path in [
            "fixture/notes.txt",
            "fixture/binary.bin",
            "fixture/image.png",
        ] {
            let object = super::super::object_read::ObjectReadService::new(
                dir.path().to_path_buf(),
                service.commit_id().to_string(),
                path.to_string(),
            )
            .unwrap()
            .bytes()
            .unwrap();
            assert_eq!(
                object,
                std::fs::read(dir.path().join(path)).unwrap(),
                "{path}"
            );
        }
        let notes = super::super::object_read::ObjectReadService::new(
            dir.path().to_path_buf(),
            service.commit_id().to_string(),
            "fixture/notes.txt".to_string(),
        )
        .unwrap()
        .bytes()
        .unwrap();
        assert_eq!(
            render_text_from_bytes(&notes, Some(1), Some(1), None).unwrap(),
            render_text_from_bytes(
                &std::fs::read(root.join("notes.txt")).unwrap(),
                Some(1),
                Some(1),
                None,
            )
            .unwrap()
        );
        assert_eq!(
            render_text_from_bytes(&notes, None, Some(4), Some(2)).unwrap(),
            render_text_from_bytes(
                &std::fs::read(root.join("notes.txt")).unwrap(),
                None,
                Some(4),
                Some(2),
            )
            .unwrap()
        );

        assert_eq!(
            format_virtual_directory_listing(&root, service.listing().unwrap()),
            format_directory_listing(&root)
        );
        for mode in ["files_with_matches", "count", "content"] {
            let overlay = run_overlay_glob_projection(
                overlay_entries.clone(),
                service.prefix(),
                "*.txt",
                Some(0),
                Some(8),
                Some(mode),
                |entries| {
                    registry
                        .load_entries("project", dir.path(), entries, service.limits())
                        .map_err(|error| error.to_string())
                },
            )
            .unwrap();
            assert_eq!(
                overlay.body,
                run_object_glob_projection(&service, "*.txt", Some(0), Some(8), Some(mode))
                    .unwrap(),
                "overlay glob mode {mode}"
            );
            assert_eq!(
                overlay.body,
                legacy_glob_projection(&root, "*.txt", Some(0), Some(8), Some(mode)),
                "glob mode {mode}"
            );
            // Every mode projects the same matched set (notes.txt, large.txt),
            // so the count the header reports must not vary with the shape the
            // body happened to be written in.
            assert_eq!(overlay.files, 2, "overlay glob file count, mode {mode}");
        }

        let grep = crate::mcp::handlers::search::GrepPayload {
            pattern: "needle".to_string(),
            path: None,
            glob: Some("*.txt".to_string()),
            file_type: None,
            output_mode: Some("content".to_string()),
            context: None,
            after_context: None,
            before_context: None,
            context_alias: Some(1),
            case_insensitive: None,
            line_numbers: Some(true),
            head_limit: None,
            offset: None,
            multiline: None,
        };
        let files = service.files().unwrap();
        let object_grep =
            crate::mcp::handlers::search::render_tree_grep(&overlay_files, &grep).unwrap();
        assert_eq!(
            object_grep,
            crate::mcp::handlers::search::render_tree_grep(&files, &grep).unwrap()
        );
        let filesystem_grep = crate::mcp::handlers::search::grep_search(
            grep.clone(),
            &root,
            "content",
            true,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(object_grep, filesystem_grep);

        let mut binary_grep = grep.clone();
        binary_grep.glob = None;
        assert_eq!(
            crate::mcp::handlers::search::render_tree_grep(&overlay_files, &binary_grep),
            crate::mcp::handlers::search::render_tree_grep(&files, &binary_grep)
        );

        let object_ast = run_object_ast(&service, "fixture", "fn $NAME() { $$$ }", Some("*.rs"))
            .unwrap()
            .0;
        let filesystem_ast =
            crate::symbols::search::search(&root, &root, "fn $NAME() { $$$ }", Some("*.rs"));
        assert_eq!(object_ast, filesystem_ast);
        let overlay_texts = overlay_files
            .iter()
            .filter_map(|(path, bytes)| {
                String::from_utf8(bytes.clone())
                    .ok()
                    .map(|text| (path.clone(), text))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            crate::symbols::search::search_texts(
                &overlay_texts,
                "fn $NAME() { $$$ }",
                Some("*.rs")
            ),
            object_ast
        );

        let object_outline = run_object_outline(&service, "fixture", Some("*.rs"))
            .unwrap()
            .0;
        let filesystem_outline = crate::symbols::outline::outline(&root, &root, Some("*.rs")).body;
        assert_eq!(object_outline, filesystem_outline);
        assert_eq!(
            crate::symbols::outline::outline_texts(&overlay_texts, Some("*.rs")).body,
            object_outline
        );
    }

    #[test]
    fn ordinary_managed_projection_dispatch_is_object_backed() {
        let whole_source = include_str!("file.rs");
        assert!(whole_source.contains("branch.is_some() || logical_project_target"));
        let context = whole_source.find("let branch_context =").unwrap();
        let dispatch = whole_source[context..]
            .find("\n    match projection {")
            .map(|offset| context + offset)
            .unwrap();
        let source = &whole_source[dispatch..];
        for (start, end) in [
            ("ReadProjection::Glob", "ReadProjection::Grep"),
            ("ReadProjection::Grep", "ReadProjection::Ast"),
            ("ReadProjection::Ast", "ReadProjection::Outline"),
            ("ReadProjection::Outline", "ReadProjection::None"),
            ("ReadProjection::None", "struct FileIssueHistoryRow"),
        ] {
            let start = source.find(start).unwrap();
            let end = source[start..]
                .find(end)
                .map(|offset| start + offset)
                .unwrap();
            let arm = &source[start..end];
            assert!(arm.contains("branch_context.as_ref()"), "{start}");
        }
        assert!(source.contains("run_overlay_glob_projection"));
        assert!(source.contains("overlay_files(orch, context)"));
        assert!(source.contains("search_texts"));
        assert!(source.contains("outline_texts"));
        assert!(source.contains("read_ignored_materialization"));
    }

    fn payload() -> ReadFilePayload {
        ReadFilePayload {
            path: "file:x.rs".to_string(),
            offset: None,
            limit: None,
            issue_history: None,
        }
    }

    fn project(query: &str) -> Result<FileProjection, String> {
        let params = parse_query_params(query).unwrap();
        parse_file_projection(&params, &payload())
    }

    #[test]
    fn grep_with_offset_is_rejected() {
        let err = project("grep=foo&offset=2").unwrap_err();
        assert!(err.contains("head_limit"), "{err}");
    }

    #[test]
    fn grep_with_limit_aliases_to_head_limit() {
        let (projection, _, _, _, _, _) = project("grep=foo&limit=2").unwrap();
        match projection {
            ReadProjection::Grep(grep) => assert_eq!(grep.head_limit, Some(2)),
            other => panic!("expected grep projection, got {other:?}"),
        }
    }

    #[test]
    fn empty_grep_is_rejected() {
        let err = project("grep=").unwrap_err();
        assert!(err.to_lowercase().contains("empty"), "{err}");
    }

    #[test]
    fn empty_grep_with_offset_reports_empty_grep_first() {
        // The empty-grep guidance wins over the grep+offset conflict: with no
        // pattern, "omit grep for a plain read" is the actionable message.
        let err = project("grep=&offset=2").unwrap_err();
        assert!(err.to_lowercase().contains("empty"), "{err}");
    }

    #[test]
    fn grep_with_head_limit_is_accepted() {
        let (projection, _, _, _, _, _) = project("grep=foo&head_limit=5").unwrap();
        match projection {
            ReadProjection::Grep(grep) => {
                assert_eq!(grep.pattern, "foo");
                assert_eq!(grep.head_limit, Some(5));
                assert_eq!(grep.offset, None);
            }
            other => panic!("expected grep projection, got {other:?}"),
        }
    }

    #[test]
    fn grep_with_context_is_accepted() {
        let (projection, _, _, _, _, _) = project("grep=foo&-C=3").unwrap();
        match projection {
            ReadProjection::Grep(grep) => assert_eq!(grep.context_alias, Some(3)),
            other => panic!("expected grep projection, got {other:?}"),
        }
    }

    #[test]
    fn grep_with_bare_case_insensitive_flag_is_accepted() {
        let (projection, _, _, _, _, _) = project("grep=foo&-i&-C=3").unwrap();
        match projection {
            ReadProjection::Grep(grep) => {
                assert_eq!(grep.case_insensitive, Some(true));
                assert_eq!(grep.context_alias, Some(3));
            }
            other => panic!("expected grep projection, got {other:?}"),
        }
    }

    #[test]
    fn glob_with_offset_and_limit_still_slices() {
        let (projection, _, _, _, _, _) = project("glob=*.rs&offset=1&limit=3").unwrap();
        match projection {
            ReadProjection::Glob {
                pattern,
                offset,
                limit,
                output_mode,
            } => {
                assert_eq!(pattern, "*.rs");
                assert_eq!(offset, Some(1));
                assert_eq!(limit, Some(3));
                assert_eq!(output_mode, None);
            }
            other => panic!("expected glob projection, got {other:?}"),
        }
    }

    #[test]
    fn glob_with_output_mode_content_is_accepted() {
        let (projection, _, _, _, _, _) = project("glob=*.rs&output_mode=content").unwrap();
        match projection {
            ReadProjection::Glob {
                pattern,
                output_mode,
                ..
            } => {
                assert_eq!(pattern, "*.rs");
                assert_eq!(output_mode.as_deref(), Some("content"));
            }
            other => panic!("expected glob projection, got {other:?}"),
        }
    }

    #[test]
    fn glob_with_count_output_mode_is_accepted() {
        let (projection, _, _, _, _, _) = project("glob=*.rs&output_mode=count").unwrap();
        assert!(matches!(
            projection,
            ReadProjection::Glob {
                output_mode: Some(ref mode),
                ..
            } if mode == "count"
        ));
    }

    #[test]
    fn glob_with_invalid_output_mode_is_rejected() {
        let err = project("glob=*.rs&output_mode=bogus").unwrap_err();
        assert!(err.contains("output_mode"), "{err}");
    }

    #[test]
    fn plain_read_offset_limit_unchanged() {
        let (projection, _, offset, limit, _, _) = project("offset=10&limit=20").unwrap();
        assert!(matches!(projection, ReadProjection::None));
        assert_eq!(offset, Some(10));
        assert_eq!(limit, Some(20));
    }

    #[test]
    fn branch_query_is_accepted_on_file_projections() {
        let (projection, _, _, _, _, branch) = project("branch=main").unwrap();
        assert!(matches!(projection, ReadProjection::None));
        assert_eq!(branch.as_deref(), Some("main"));

        let (projection, _, _, _, _, branch) =
            project("grep=needle&branch=agent/CAIRN-1-builder-0").unwrap();
        assert!(matches!(projection, ReadProjection::Grep(_)));
        assert_eq!(branch.as_deref(), Some("agent/CAIRN-1-builder-0"));

        let (projection, _, _, _, _, branch) =
            project("glob=**/*.rs&branch=cairn://p/CAIRN/1/1/builder").unwrap();
        assert!(matches!(projection, ReadProjection::Glob { .. }));
        assert_eq!(branch.as_deref(), Some("cairn://p/CAIRN/1/1/builder"));
    }

    #[test]
    fn empty_branch_query_is_rejected() {
        let err = project("branch=").unwrap_err();
        assert!(err.to_lowercase().contains("empty"), "{err}");
    }

    #[test]
    fn filesystem_sql_query_param_remains_unsupported() {
        let err = project("sql=SELECT 1").unwrap_err();
        assert!(
            err.contains("Unsupported query parameter 'sql' for filesystem read target"),
            "{err}"
        );
    }

    fn text(bytes: &[u8], offset: Option<i64>, limit: Option<usize>) -> (String, usize, usize) {
        let response = render_text_from_bytes(bytes, offset, limit, None).unwrap();
        assert_eq!(response.kind, "text");
        assert_eq!(response.limit, limit);
        assert!(response.history.is_none());
        (response.content, response.total_lines, response.offset)
    }

    #[test]
    fn render_text_numbers_every_line() {
        let (content, total, offset) = text(b"a\nb\nc\n", None, None);
        assert_eq!(content, "     1\ta\n     2\tb\n     3\tc");
        assert_eq!(total, 3);
        assert_eq!(offset, 0);
    }

    #[test]
    fn render_text_offset_skips_leading_lines() {
        let (content, total, offset) = text(b"a\nb\nc\n", Some(1), None);
        assert_eq!(content, "     2\tb\n     3\tc");
        assert_eq!(total, 3);
        assert_eq!(offset, 1);
    }

    #[test]
    fn render_text_limit_caps_lines() {
        let (content, total, offset) = text(b"a\nb\nc\n", None, Some(2));
        assert_eq!(content, "     1\ta\n     2\tb");
        assert_eq!(total, 3);
        assert_eq!(offset, 0);
    }

    #[test]
    fn render_text_offset_and_limit_combine() {
        let (content, total, offset) = text(b"a\nb\nc\nd\n", Some(1), Some(2));
        assert_eq!(content, "     2\tb\n     3\tc");
        assert_eq!(total, 4);
        assert_eq!(offset, 1);
    }

    #[test]
    fn render_text_negative_offset_tails() {
        let (content, total, offset) = text(b"a\nb\nc\nd\n", Some(-2), None);
        assert_eq!(content, "     3\tc\n     4\td");
        assert_eq!(total, 4);
        assert_eq!(offset, 2);
    }

    #[test]
    fn render_text_final_line_without_newline_counts() {
        // BufRead::read_line surfaces a trailing newline-less line, so it counts
        // and renders just like any other line.
        let (content, total, _) = text(b"a\nb", None, None);
        assert_eq!(content, "     1\ta\n     2\tb");
        assert_eq!(total, 2);
    }

    #[test]
    fn render_text_empty_file_is_blank() {
        let (content, total, offset) = text(b"", None, None);
        assert_eq!(content, "");
        assert_eq!(total, 0);
        assert_eq!(offset, 0);
    }

    #[test]
    fn render_text_emits_full_window_without_budget() {
        // The per-file budget moved to the assembler's view layer; the renderer
        // now returns every windowed line, however wide.
        let line = "x".repeat(1_000);
        let mut bytes = String::new();
        for _ in 0..500 {
            bytes.push_str(&line);
            bytes.push('\n');
        }
        let response = render_text_from_bytes(bytes.as_bytes(), None, None, None).unwrap();
        assert_eq!(response.total_lines, 500);
        assert_eq!(response.content.matches('\n').count(), 499);
    }

    #[test]
    fn render_text_char_offset_skips_first_line_text() {
        // char_offset skips characters of the first shown line's text while the
        // line number is preserved — the resume half of the char fallback.
        let (content, _total, _offset) = text_with(b"abcdef\nghij\n", None, None, Some(3));
        assert_eq!(content, "     1\tdef\n     2\tghij");
    }

    fn text_with(
        bytes: &[u8],
        offset: Option<i64>,
        limit: Option<usize>,
        char_offset: Option<usize>,
    ) -> (String, usize, usize) {
        let response = render_text_from_bytes(bytes, offset, limit, char_offset).unwrap();
        (response.content, response.total_lines, response.offset)
    }

    async fn grep_test_orch() -> (crate::orchestrator::Orchestrator, tempfile::TempDir) {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::sync::Arc;

        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let worktree = tempfile::tempdir().unwrap();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .build();
        (orch, worktree)
    }

    fn grep_request(cwd: &std::path::Path) -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: cwd.display().to_string(),
            run_id: None,
            tool: "read_batch".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    async fn grep_segment(
        orch: &Orchestrator,
        worktree: &std::path::Path,
        target: &str,
    ) -> ReadSegment {
        let payload = ReadFilePayload {
            path: target.to_string(),
            offset: None,
            limit: None,
            issue_history: None,
        };
        match produce_file_segment(orch, &grep_request(worktree), &payload).await {
            Produced::Segment(seg) => seg,
            Produced::Suspended(_) => panic!("unexpected fence suspension"),
        }
    }

    /// The wave-1 regression: a directory grep with a context flag and no
    /// explicit output_mode must render content (`path:N:text` match lines plus
    /// `path:N-text` context lines) AND classify the segment as a content grep so
    /// the `[N matches in M files]` header suffix survives. Before the fix, the
    /// body was content but the metadata was silently `files_with_matches`
    /// (match_count unset), dropping the suffix.
    #[tokio::test]
    async fn directory_grep_with_context_renders_content_and_counts_matches() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::create_dir(worktree.path().join("d")).unwrap();
        std::fs::write(
            worktree.path().join("d/a.txt"),
            "alpha\nbeta\nNEEDLE\ngamma\ndelta\n",
        )
        .unwrap();

        let seg = grep_segment(&orch, worktree.path(), "file:d?grep=NEEDLE&-C=1").await;

        assert!(
            seg.body.contains("a.txt:3:NEEDLE"),
            "expected a `path:N:text` match line, got:\n{}",
            seg.body
        );
        assert!(
            seg.body.contains("a.txt:2-beta") && seg.body.contains("a.txt:4-gamma"),
            "expected `path:N-text` context lines, got:\n{}",
            seg.body
        );
        // The metadata must agree with the body: a content grep carries a match
        // count so the enriched header renders `[N matches in M files]`.
        assert_eq!(seg.meta.match_count, Some(1), "body:\n{}", seg.body);
        assert_eq!(seg.meta.file_count, Some(1));
    }

    /// An explicit `output_mode=files_with_matches` wins even when a context flag
    /// is present: the body is paths only and the segment stays
    /// files_with_matches (no match count).
    #[tokio::test]
    async fn directory_grep_explicit_files_with_matches_overrides_context() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::create_dir(worktree.path().join("d")).unwrap();
        std::fs::write(
            worktree.path().join("d/a.txt"),
            "alpha\nbeta\nNEEDLE\ngamma\ndelta\n",
        )
        .unwrap();

        let seg = grep_segment(
            &orch,
            worktree.path(),
            "file:d?grep=NEEDLE&-C=1&output_mode=files_with_matches",
        )
        .await;

        assert_eq!(
            seg.body.trim(),
            "a.txt",
            "expected paths only, got:\n{}",
            seg.body
        );
        assert!(
            !seg.body.contains("NEEDLE"),
            "explicit fwm must not render content"
        );
        assert_eq!(seg.meta.match_count, None);
    }

    /// The reported regression: a logical project target has no host path, so
    /// its `full_path` is a bare repository-relative path that never stats as a
    /// file. Resolving the output mode from that stat classified every logical
    /// single-file grep as a tree and defaulted it to `files_with_matches`,
    /// rendering the filename the caller already typed — a hit that reads like a
    /// miss. The branch classification, not the filesystem, decides.
    #[test]
    fn logical_single_file_grep_defaults_to_content_without_a_host_path() {
        let payload = grep_projection("grep=needle");
        let logical = std::path::Path::new("src/does/not/exist/on/this/host.rs");
        assert!(!logical.is_file());

        assert_eq!(
            resolve_grep_target_mode(Some(BranchGrepTarget::SingleFile), logical, &payload),
            (true, "content".to_string())
        );
        assert_eq!(
            resolve_grep_target_mode(Some(BranchGrepTarget::Tree), logical, &payload),
            (false, "files_with_matches".to_string())
        );
    }

    /// A `glob`/`type` push-down is a multi-file walk even against a single-file
    /// target, so it keeps the tree default.
    #[test]
    fn grep_with_a_glob_pushdown_is_never_a_single_file() {
        let payload = grep_projection("grep=needle&glob=*.rs");
        assert_eq!(
            resolve_grep_target_mode(
                Some(BranchGrepTarget::SingleFile),
                std::path::Path::new("src/lib.rs"),
                &payload
            ),
            (false, "files_with_matches".to_string())
        );
    }

    fn grep_projection(query: &str) -> crate::mcp::handlers::search::GrepPayload {
        match project(query).unwrap().0 {
            ReadProjection::Grep(payload) => payload,
            other => panic!("expected grep projection, got {other:?}"),
        }
    }

    /// A single-file grep with no explicit output_mode defaults to content — the
    /// pre-existing single-file behavior is unchanged by the fix.
    #[tokio::test]
    async fn single_file_grep_defaults_to_content() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::write(
            worktree.path().join("a.txt"),
            "alpha\nbeta\nNEEDLE\ngamma\n",
        )
        .unwrap();

        let seg = grep_segment(&orch, worktree.path(), "file:a.txt?grep=NEEDLE").await;

        assert!(
            seg.body.contains("a.txt:3:NEEDLE"),
            "single-file grep should render content, got:\n{}",
            seg.body
        );
        assert_eq!(seg.meta.match_count, Some(1));
        // A single-file grep does not carry a file count (you named the file).
        assert_eq!(seg.meta.file_count, None);
    }

    /// Every grep mode's header must agree with the body it labels. A
    /// `files_with_matches` body is the file list, so it counts files; a `count`
    /// body is `path:N` tallies, which read as zero matches under the content
    /// shape and rendered `[0 matches]` over a body full of them.
    #[tokio::test]
    async fn grep_header_counts_follow_the_output_mode() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::create_dir(worktree.path().join("d")).unwrap();
        std::fs::write(worktree.path().join("d/a.txt"), "NEEDLE\nx\nNEEDLE\n").unwrap();
        std::fs::write(worktree.path().join("d/b.txt"), "y\nNEEDLE\n").unwrap();

        let tree_files = grep_segment(&orch, worktree.path(), "file:d?grep=NEEDLE").await;
        assert_eq!(tree_files.meta.match_count, None);
        assert_eq!(tree_files.meta.file_count, Some(2), "{}", tree_files.body);

        let tree_count = grep_segment(
            &orch,
            worktree.path(),
            "file:d?grep=NEEDLE&output_mode=count",
        )
        .await;
        assert_eq!(tree_count.meta.match_count, Some(3), "{}", tree_count.body);
        assert_eq!(tree_count.meta.file_count, Some(2));

        let file_files = grep_segment(
            &orch,
            worktree.path(),
            "file:d/a.txt?grep=NEEDLE&output_mode=files_with_matches",
        )
        .await;
        assert_eq!(file_files.meta.match_count, None);
        assert_eq!(file_files.meta.file_count, Some(1), "{}", file_files.body);

        let file_count = grep_segment(
            &orch,
            worktree.path(),
            "file:d/a.txt?grep=NEEDLE&output_mode=count",
        )
        .await;
        assert_eq!(file_count.meta.match_count, Some(2), "{}", file_count.body);
        assert_eq!(file_count.meta.file_count, None);
    }

    /// A failed search is an error, never a counted result. The failure text is
    /// an ordinary string, so folding it into the body would have let the
    /// files-with-matches count read one line of error prose as one matched
    /// file and head it `[1 files]` — asserting a match that never happened.
    #[tokio::test]
    async fn grep_failure_is_an_error_segment_not_a_counted_result() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::create_dir(worktree.path().join("d")).unwrap();
        std::fs::write(worktree.path().join("d/a.txt"), "NEEDLE\n").unwrap();

        // Default tree mode — the files_with_matches path the header now counts.
        let tree = grep_segment(&orch, worktree.path(), "file:d?grep=(unclosed").await;
        assert_eq!(tree.meta.kind, SegmentKind::Error, "{}", tree.body);
        assert_eq!(tree.meta.match_count, None);
        assert_eq!(tree.meta.file_count, None);
        assert!(tree.body.contains("Invalid regex pattern"), "{}", tree.body);

        // And on the single-file path, in every mode it can reach.
        for target in [
            "file:d/a.txt?grep=(unclosed",
            "file:d/a.txt?grep=(unclosed&output_mode=files_with_matches",
            "file:d/a.txt?grep=(unclosed&output_mode=count",
        ] {
            let segment = grep_segment(&orch, worktree.path(), target).await;
            assert_eq!(segment.meta.kind, SegmentKind::Error, "{target}");
            assert_eq!(segment.meta.match_count, None, "{target}");
            assert_eq!(segment.meta.file_count, None, "{target}");
        }
    }

    /// An empty result stays empty in every mode: the no-match body is prose,
    /// and counting its single line as a matched file would render an absence as
    /// `[1 files]`.
    #[tokio::test]
    async fn grep_with_no_matches_reports_zero_in_every_mode() {
        let (orch, worktree) = grep_test_orch().await;
        std::fs::create_dir(worktree.path().join("d")).unwrap();
        std::fs::write(worktree.path().join("d/a.txt"), "alpha\nbeta\n").unwrap();

        let tree = grep_segment(&orch, worktree.path(), "file:d?grep=ABSENT").await;
        assert!(tree.body.starts_with("No matches found"), "{}", tree.body);
        assert_eq!(tree.meta.match_count, None);
        assert_eq!(tree.meta.file_count, Some(0));

        let file = grep_segment(&orch, worktree.path(), "file:d/a.txt?grep=ABSENT").await;
        assert!(file.body.starts_with("No matches found"), "{}", file.body);
        assert_eq!(file.meta.match_count, Some(0));
        assert_eq!(file.meta.file_count, None);
    }

    #[test]
    fn archived_image_target_produces_image_block_not_json() {
        // The archived image path emits an empty-body Image segment plus a base64
        // ImageBlock — the same shape the live producer builds — not the pre-#1485
        // JSON envelope.
        let bytes = b"\x89PNG\r\n\x1a\nbinary-bytes\x00\xff";
        let segment = produce_archived_file_segment("file:logo.png", bytes).unwrap();
        assert_eq!(segment.meta.kind, SegmentKind::Image);
        assert!(segment.body.is_empty(), "image segments carry no text body");
        assert_eq!(segment.images.len(), 1);
        assert_eq!(segment.images[0].mime_type, "image/png");
        let expected = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
        assert_eq!(segment.images[0].data, expected);
    }
}
