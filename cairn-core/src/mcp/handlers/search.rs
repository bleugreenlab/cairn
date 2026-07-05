//! Search MCP handlers.
//!
//! Handles: search, glob, grep

use crate::mcp::types::McpCallbackRequest;
use crate::models::{SearchContentType, SearchFilters};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::query::QueryParam;
use cairn_symbols::search_util::{build_glob_matcher, format_grep_line};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Payload for search tool
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPayload {
    pub query: String,
    pub content_types: Option<Vec<String>>,
    pub project_id: Option<String>,
    pub issue_id: Option<String>,
    pub since: Option<i64>,
    pub limit: Option<usize>,
}

/// Handle search tool call - searches across issues, comments, artifacts, and events.
pub async fn handle_search(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: SearchPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    let SearchPayload {
        query,
        content_types,
        project_id,
        issue_id,
        since,
        limit,
    } = payload;

    log::info!("search called: query={}", query);

    // Get project context via run chain (internal agents), or none (external callers)
    let ctx = lookup_search_project_context(&orch.db.local, request).await;

    // Build filters - use provided project_id or default to current project from run chain
    let project_id = project_id.or_else(|| ctx.as_ref().map(|c| c.project_id.clone()));

    let filters = SearchFilters {
        project_id,
        issue_id,
        content_types,
        since,
        limit,
    };

    // Drain every open database's outbox so team-replica writes are indexed
    // before the query, not only at startup.
    if let Err(error) = orch.db.apply_pending_search().await {
        return format!("Search failed: {error}");
    }
    match crate::search::search_content(
        &orch.db.local,
        &orch.db.search_index,
        &query,
        Some(filters),
    )
    .await
    {
        Ok(results) => {
            format_search_results(&results, ctx.as_ref().map(|c| c.project_key.as_str()))
        }
        Err(e) => format!("Search failed: {}", e),
    }
}

#[derive(Debug, Clone)]
struct SearchProjectContext {
    project_id: String,
    project_key: String,
}

async fn lookup_search_project_context(
    db: &LocalDb,
    request: &McpCallbackRequest,
) -> Option<SearchProjectContext> {
    let result = if let Some(ref run_id) = request.run_id {
        lookup_search_project_context_by_run_id(db, run_id).await
    } else {
        lookup_search_project_context_by_cwd(db, &request.cwd).await
    };

    match result {
        Ok(ctx) => ctx,
        Err(error) => {
            log::debug!("search project context lookup failed: {}", error);
            None
        }
    }
}

async fn lookup_search_project_context_by_run_id(
    db: &LocalDb,
    run_id: &str,
) -> DbResult<Option<SearchProjectContext>> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT p.id, p.key
                    FROM runs r
                    LEFT JOIN jobs j ON r.job_id = j.id
                    JOIN projects p ON p.id = COALESCE(j.project_id, r.project_id)
                    WHERE r.id = ?1
                    LIMIT 1
                    ",
                    (run_id.as_str(),),
                )
                .await?;

            rows.next()
                .await?
                .map(|row| search_project_context_from_row(&row))
                .transpose()
        })
    })
    .await
}

async fn lookup_search_project_context_by_cwd(
    db: &LocalDb,
    cwd: &str,
) -> DbResult<Option<SearchProjectContext>> {
    let cwd = cwd.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT p.id, p.key
                    FROM runs r
                    JOIN jobs j ON r.job_id = j.id
                    JOIN projects p ON j.project_id = p.id
                    WHERE r.status IN ('starting', 'live')
                      AND (
                        j.worktree_path = ?1
                        OR (p.repo_path = ?1 AND j.issue_id IS NULL)
                      )
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
                .map(|row| search_project_context_from_row(&row))
                .transpose()
        })
    })
    .await
}

fn search_project_context_from_row(row: &cairn_db::turso::Row) -> DbResult<SearchProjectContext> {
    Ok(SearchProjectContext {
        project_id: row.text(0)?,
        project_key: row.text(1)?,
    })
}

fn should_walk_entry(entry_path: &Path, walk_root: &Path, deny_read: &[PathBuf]) -> bool {
    if entry_path == walk_root {
        return true;
    }

    !crate::mcp::file_targets::path_within_any(entry_path, deny_read)
}

/// Format search results as human-readable text for the agent.
pub(crate) fn format_search_results(
    results: &[crate::models::SearchResult],
    project_key: Option<&str>,
) -> String {
    if results.is_empty() {
        return "No results found.".to_string();
    }

    let mut output = format!("Found {} result(s):\n\n", results.len());

    for (i, result) in results.iter().enumerate() {
        let type_label = match result.content_type {
            SearchContentType::Issue => "Issue",
            SearchContentType::Comment => "Comment",
            SearchContentType::Artifact => "Artifact",
            SearchContentType::Event => "Event",
            SearchContentType::Message => "Message",
        };

        // Use the URI from the search result directly
        let uri = if result.uri.is_empty() {
            let key = project_key.unwrap_or("PROJECT");
            match result.id.parse::<i32>() {
                Ok(number) if number > 0 => cairn_common::uri::build_issue_uri(key, number),
                _ => cairn_common::uri::build_project_uri(key),
            }
        } else {
            result.uri.clone()
        };

        output.push_str(&format!("{}. [{}] {}\n", i + 1, type_label, result.title));
        output.push_str(&format!("   URI: {}\n", uri));
        output.push_str(&format!("   {}\n\n", result.snippet));
    }

    output
}

// ---------------------------------------------------------------------------
// Glob / Grep handlers
// ---------------------------------------------------------------------------

const WALK_TIMEOUT: Duration = Duration::from_secs(30);
const GREP_TIMEOUT: Duration = Duration::from_secs(30);

/// Payload for glob tool (matches cairn-cmd GlobInput)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobPayload {
    pub pattern: String,
    pub path: Option<String>,
}

/// Payload for grep tool (matches cairn-cmd GrepInput)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrepPayload {
    pub pattern: String,
    pub path: Option<String>,
    pub glob: Option<String>,
    #[serde(rename = "type")]
    pub file_type: Option<String>,
    pub output_mode: Option<String>,
    pub context: Option<u32>,
    #[serde(rename = "-A")]
    pub after_context: Option<u32>,
    #[serde(rename = "-B")]
    pub before_context: Option<u32>,
    #[serde(rename = "-C")]
    pub context_alias: Option<u32>,
    #[serde(rename = "-i")]
    pub case_insensitive: Option<bool>,
    #[serde(rename = "-n")]
    pub line_numbers: Option<bool>,
    pub head_limit: Option<u32>,
    pub offset: Option<u32>,
    pub multiline: Option<bool>,
}

/// Resolve the search directory from the payload path and the agent cwd.
fn resolve_search_dir(cwd: &str, path: Option<&str>) -> PathBuf {
    match path {
        Some(p) => {
            let p = Path::new(p);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                Path::new(cwd).join(p)
            }
        }
        None => PathBuf::from(cwd),
    }
}

/// Default grep output mode for a target. Grepping a single file should print
/// matching lines (you already named the file — echoing it back is useless and
/// dead-ends agents into shelling out to grep), while grepping a directory
/// lists which files matched.
fn default_grep_output_mode(search_path: &Path) -> &'static str {
    if search_path.is_file() {
        "content"
    } else {
        "files_with_matches"
    }
}

/// Resolve the effective grep output mode. An explicit `output_mode` always
/// wins. Absent one, context flags (`-C`/`-A`/`-B`/`context`) force `content`:
/// asking for context lines around a bare filename is meaningless, so a
/// directory grep with `-C=N` would otherwise silently drop both the context
/// and the matched lines. With no override and no context request, fall back to
/// the target-aware default (see `default_grep_output_mode`).
pub fn resolve_grep_output_mode<'a>(
    explicit: Option<&'a str>,
    requested_context: bool,
    search_path: &Path,
) -> &'a str {
    explicit.unwrap_or_else(|| {
        if requested_context {
            "content"
        } else {
            default_grep_output_mode(search_path)
        }
    })
}

/// Matched files for a glob, as paths relative to the resolved search dir,
/// sorted most-recently-modified first.
pub(crate) struct GlobMatches {
    pub search_dir: PathBuf,
    pub pattern: String,
    pub paths: Vec<PathBuf>,
    pub timed_out: bool,
}

pub(crate) async fn glob_matched_paths(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<GlobMatches, String> {
    let payload: GlobPayload = serde_json::from_value(request.payload.clone())
        .map_err(|e| format!("Invalid payload: {}", e))?;

    let search_dir = resolve_search_dir(&request.cwd, payload.path.as_deref());
    log::info!(
        "glob called: pattern={}, dir={}",
        payload.pattern,
        search_dir.display()
    );

    // Validate with Cairn's exact matcher before the warm-index attempt so
    // invalid-glob errors stay identical whether the index is warm or cold.
    build_glob_matcher(&payload.pattern)?;

    let deny_read = orch.sandbox_deny_read();
    if let Some(paths) =
        try_worktree_index_glob(orch, request, &search_dir, &payload.pattern, &deny_read).await
    {
        return Ok(GlobMatches {
            search_dir,
            pattern: payload.pattern,
            paths,
            timed_out: false,
        });
    }

    let pattern = payload.pattern.clone();
    let search_dir_for_walk = search_dir.clone();
    let result = tokio::time::timeout(
        WALK_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            glob_matched_paths_walk(search_dir_for_walk, pattern, deny_read)
        }),
    )
    .await;

    let (paths, timed_out) = match result {
        Ok(Ok(Ok((paths, timed_out)))) => (paths, timed_out),
        Ok(Ok(Err(error))) => return Err(error),
        Ok(Err(error)) => return Err(format!("glob task failed: {}", error)),
        Err(_) => {
            log::warn!("glob walk timed out after {:?}", WALK_TIMEOUT);
            (Vec::new(), true)
        }
    };

    Ok(GlobMatches {
        search_dir,
        pattern: payload.pattern,
        paths,
        timed_out,
    })
}

pub(crate) fn glob_matched_paths_walk(
    search_dir: PathBuf,
    pattern: String,
    deny_read: Vec<PathBuf>,
) -> Result<(Vec<PathBuf>, bool), String> {
    let matcher = build_glob_matcher(&pattern)?;
    let filter_root = search_dir.clone();
    let walker = ignore::WalkBuilder::new(&search_dir)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .filter_entry(move |entry| should_walk_entry(entry.path(), &filter_root, &deny_read))
        .build();

    let deadline = std::time::Instant::now() + WALK_TIMEOUT;
    let mut matches: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    let mut timed_out = false;

    for entry in walker {
        if std::time::Instant::now() > deadline {
            log::warn!("glob walk timed out after {:?}", WALK_TIMEOUT);
            timed_out = true;
            break;
        }
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(&search_dir).unwrap_or(path);
        if matcher.is_match(relative) {
            let mtime = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            matches.push((relative.to_path_buf(), mtime));
        }
    }

    matches.sort_by(|a, b| b.1.cmp(&a.1));
    Ok((matches.into_iter().map(|(p, _)| p).collect(), timed_out))
}

/// Append the shared "search timed out" warning to a glob result body.
pub(crate) fn glob_timeout_warning() -> String {
    format!(
        "\n\n[WARNING: Search timed out after {}s — results are incomplete. \
         Try a more specific path or pattern to narrow the search.]",
        WALK_TIMEOUT.as_secs()
    )
}

/// Handle glob tool call — pattern-match files with path scoping.
pub async fn handle_glob(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let GlobMatches {
        search_dir,
        pattern,
        paths,
        timed_out,
    } = match glob_matched_paths(orch, request).await {
        Ok(matches) => matches,
        Err(error) => return error,
    };

    if paths.is_empty() && !timed_out {
        return format!(
            "No files matched pattern '{}' in {}",
            pattern,
            search_dir.display()
        );
    }

    let mut result: String = paths
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join("\n");

    if timed_out {
        result.push_str(&glob_timeout_warning());
    }

    result
}

/// Handle grep tool call — in-process ripgrep search.
pub async fn handle_grep(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: GrepPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    let search_path = resolve_search_dir(&request.cwd, payload.path.as_deref());
    log::info!(
        "grep called: pattern={}, path={}",
        payload.pattern,
        search_path.display()
    );

    // Validate output mode early. An explicit `output_mode` always wins;
    // otherwise the default is target-aware (see `default_grep_output_mode`),
    // except that context flags (`-C`/`-A`/`-B`/`context`) force `content`.
    let requested_context = payload.context.is_some()
        || payload.context_alias.is_some()
        || payload.after_context.is_some()
        || payload.before_context.is_some();
    let output_mode = resolve_grep_output_mode(
        payload.output_mode.as_deref(),
        requested_context,
        &search_path,
    );
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        );
    }

    let output_mode = output_mode.to_string();
    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let offset = payload.offset.unwrap_or(0) as usize;
    let head_limit = payload.head_limit;
    let pattern = payload.pattern.clone();
    let deny_read = orch.sandbox_deny_read();

    // Warm-index fast path (CAIRN-2303): a worktree-root grep is served by the
    // resident fff index instead of re-walking the tree. Eligible only for a
    // plain line-wise grep whose root is exactly a known worktree and whose
    // shape the index supports; every other case (sub-directory root, multiline
    // regex, file-type walk, or an index still warming its first scan) falls
    // through to the ripgrep walk below, which remains the universal correct
    // answer.
    if let Some(body) = try_worktree_index_grep(
        orch,
        request,
        &search_path,
        &payload,
        &output_mode,
        show_line_numbers,
        &deny_read,
    )
    .await
    {
        return finalize_grep_output(body, &pattern, offset, head_limit);
    }

    let result = tokio::time::timeout(
        GREP_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            grep_search(
                payload,
                &search_path,
                &output_mode,
                show_line_numbers,
                deny_read,
            )
        }),
    )
    .await;

    match result {
        Ok(Ok(Ok(output))) => finalize_grep_output(output, &pattern, offset, head_limit),
        Ok(Ok(Err(e))) => e,
        Ok(Err(e)) => format!("grep task failed: {}", e),
        Err(_) => format!("grep timed out after {:?}", GREP_TIMEOUT),
    }
}

/// Attempt the warm-index glob fast path for a worktree-root or subdirectory
/// target. Returns
/// `Some(paths)` when the resident fff index served the query, or `None` to
/// fall through to the canonical filesystem walk.
async fn try_worktree_index_glob(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    search_path: &Path,
    pattern: &str,
    deny_read: &[PathBuf],
) -> Option<Vec<PathBuf>> {
    let (ctx, _db) = super::run_context::lookup_run_routed(&orch.db, request)
        .await
        .ok()?;
    let worktree_raw = PathBuf::from(ctx.worktree_path?);
    let scope = resolve_worktree_scope(orch, search_path, &worktree_raw)?;

    let pattern = pattern.to_string();
    let subdir_for_search = scope.subdir.clone();
    let deny = deny_read.to_vec();
    let paths = tokio::task::spawn_blocking(move || {
        scope
            .search
            .try_glob(&pattern, subdir_for_search.as_deref(), &deny)
    })
    .await
    .ok()??;
    log::debug!(
        "worktree_search: served glob for {} from warm index{}",
        scope.worktree_canon.display(),
        scope
            .subdir
            .as_deref()
            .map(|rel| format!(" subdir={rel}"))
            .unwrap_or_default()
    );
    Some(paths)
}

/// Attempt the warm-index grep fast path for a worktree-root or subdirectory
/// target. Returns
/// `Some(body)` (the joined, unwindowed output lines) when the resident fff
/// index served the query, or `None` to fall through to the ripgrep walk. Every
/// `None` is deliberate: an unsupported query shape, a run with no worktree, a
/// root outside the worktree, a file target, or an index that has not finished
/// its first background scan yet.
async fn try_worktree_index_grep(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    search_path: &Path,
    payload: &GrepPayload,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: &[PathBuf],
) -> Option<String> {
    // Line-wise index only: multiline regex and file-type walks stay on ripgrep
    // (fff greps line by line; its file-type constraint semantics differ from
    // ripgrep's `--type`, so that walk is left untouched).
    if payload.multiline == Some(true) || payload.file_type.is_some() {
        return None;
    }

    // Resolve the run's worktree; project-level runs (no worktree) are skipped.
    let (ctx, _db) = super::run_context::lookup_run_routed(&orch.db, request)
        .await
        .ok()?;
    // Key the pool on the raw DB `worktree_path` so teardown's `drop_worktree`
    // (which reads the same column) matches the entry created here.
    let worktree_raw = PathBuf::from(ctx.worktree_path?);
    let scope = resolve_worktree_scope(orch, search_path, &worktree_raw)?;

    let combined_context = payload.context_alias.or(payload.context);
    let params = crate::worktree_search::WorktreeGrepParams {
        pattern: payload.pattern.clone(),
        subdir: scope.subdir.clone(),
        globs: payload.glob.clone().into_iter().collect(),
        max_per_file: None,
        output_mode: output_mode.to_string(),
        case_insensitive: payload.case_insensitive,
        before_context: payload.before_context.or(combined_context).unwrap_or(0) as usize,
        after_context: payload.after_context.or(combined_context).unwrap_or(0) as usize,
        show_line_numbers,
    };
    let deny = deny_read.to_vec();

    // fff grep is CPU-bound (up to its time budget), so run it off the async
    // runtime exactly like the ripgrep walk it replaces.
    let body = tokio::task::spawn_blocking(move || scope.search.try_grep(&params, &deny))
        .await
        .ok()??;
    log::debug!(
        "worktree_search: served grep for {} from warm index{}",
        scope.worktree_canon.display(),
        scope
            .subdir
            .as_deref()
            .map(|rel| format!(" subdir={rel}"))
            .unwrap_or_default()
    );
    Some(body)
}

pub(crate) struct WorktreeIndexScope {
    pub(crate) search: Arc<crate::worktree_search::WorktreeSearch>,
    pub(crate) subdir: Option<String>,
    worktree_canon: PathBuf,
}

/// Resolve a candidate search root to a resident worktree index plus optional
/// worktree-relative subdirectory. This is the shared canonical gate for warm
/// `?grep=`, `?glob=`, and translated `run()` search commands: file targets,
/// paths outside the run worktree, missing/cold worktrees, and vanished paths all
/// return `None` so the caller can use its canonical fallback.
pub(crate) fn resolve_worktree_scope(
    orch: &Orchestrator,
    search_path: &Path,
    worktree_raw: &Path,
) -> Option<WorktreeIndexScope> {
    // Compare canonicalized paths so `.`/symlink/cwd spellings still match.
    // Single-file greps stay on the byte-parity walk; subdirectories are served
    // by prefix-filtering and rebasing worktree-relative index paths.
    let search_canon = std::fs::canonicalize(search_path).ok()?;
    let worktree_canon = std::fs::canonicalize(worktree_raw).ok()?;
    let subdir = worktree_index_search_subdir(&search_canon, &worktree_canon)?;
    let search = orch.worktree_search.get_or_create(worktree_raw)?;
    Some(WorktreeIndexScope {
        search,
        subdir,
        worktree_canon,
    })
}

fn worktree_index_search_subdir(
    search_canon: &Path,
    worktree_canon: &Path,
) -> Option<Option<String>> {
    if !search_canon.is_dir() {
        return None;
    }
    if search_canon == worktree_canon {
        return Some(None);
    }
    let rel = search_canon.strip_prefix(worktree_canon).ok()?;
    let rel = rel
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    (!rel.is_empty()).then_some(Some(rel))
}

/// A content source for a single-file grep: a filesystem path (live directory
/// walk) or an in-memory byte slice (single-file read / archival reconstruction).
#[derive(Clone, Copy)]
enum GrepSource<'a> {
    Path(&'a Path),
    Bytes(&'a [u8]),
}

/// Run a configured searcher over one source, dispatching to `search_path` for
/// the filesystem walk and `search_slice` for in-memory bytes.
fn run_search<S: grep_searcher::Sink>(
    searcher: &mut grep_searcher::Searcher,
    matcher: &grep_regex::RegexMatcher,
    source: GrepSource<'_>,
    sink: S,
) -> Result<(), S::Error> {
    match source {
        GrepSource::Path(path) => searcher.search_path(matcher, path, sink),
        GrepSource::Bytes(bytes) => searcher.search_slice(matcher, bytes, sink),
    }
}

/// Build the regex matcher for a grep, honoring case-insensitivity and
/// multiline. Shared by the directory walker and the single-file bytes renderer.
fn build_grep_matcher(payload: &GrepPayload) -> Result<grep_regex::RegexMatcher, String> {
    let mut builder = grep_regex::RegexMatcherBuilder::new();
    if payload.case_insensitive.unwrap_or(false) {
        builder.case_insensitive(true);
    }
    if payload.multiline.unwrap_or(false) {
        builder.multi_line(true);
        builder.dot_matches_new_line(true);
    }
    builder
        .build(&payload.pattern)
        .map_err(|e| format!("Invalid regex pattern '{}': {}", payload.pattern, e))
}

/// Build the searcher (binary detection, line numbers, context window) for a
/// grep. Shared by the directory walker and the single-file bytes renderer.
fn build_grep_searcher(payload: &GrepPayload) -> grep_searcher::Searcher {
    let mut builder = grep_searcher::SearcherBuilder::new();
    builder
        .binary_detection(grep_searcher::BinaryDetection::quit(b'\x00'))
        .line_number(true);

    let context = payload.context_alias.or(payload.context);
    if let Some(c) = context {
        builder.before_context(c as usize).after_context(c as usize);
    }
    if let Some(a) = payload.after_context {
        builder.after_context(a as usize);
    }
    if let Some(b) = payload.before_context {
        builder.before_context(b as usize);
    }
    if payload.multiline.unwrap_or(false) {
        builder.multi_line(true);
    }

    builder.build()
}

/// Collect grep output lines for one source under the requested output mode,
/// appending to `output_lines`. This is the single per-file formatter shared by
/// the directory walker (`grep_search`) and the single-file bytes renderer
/// (`render_single_file_grep`), so both emit identical match/context/count lines.
fn grep_collect_into(
    searcher: &mut grep_searcher::Searcher,
    matcher: &grep_regex::RegexMatcher,
    relative: &str,
    source: GrepSource<'_>,
    output_mode: &str,
    show_line_numbers: bool,
    output_lines: &mut Vec<String>,
) {
    use grep_searcher::Searcher;

    match output_mode {
        "files_with_matches" => {
            let mut found = false;
            let sink = grep_searcher::sinks::UTF8(|_, _| {
                found = true;
                Ok(false)
            });
            let _ = run_search(searcher, matcher, source, sink);
            if found {
                output_lines.push(relative.to_string());
            }
        }
        "count" => {
            let mut count: u64 = 0;
            let sink = grep_searcher::sinks::UTF8(|_, _| {
                count += 1;
                Ok(true)
            });
            let _ = run_search(searcher, matcher, source, sink);
            if count > 0 {
                output_lines.push(format!("{}:{}", relative, count));
            }
        }
        "content" => {
            struct ContentSink {
                relative: String,
                show_line_numbers: bool,
                lines: Vec<String>,
                needs_separator: bool,
            }

            impl grep_searcher::Sink for ContentSink {
                type Error = std::io::Error;

                fn matched(
                    &mut self,
                    _searcher: &Searcher,
                    mat: &grep_searcher::SinkMatch<'_>,
                ) -> Result<bool, Self::Error> {
                    self.needs_separator = true;
                    let text = std::str::from_utf8(mat.bytes())
                        .unwrap_or("")
                        .trim_end_matches('\n')
                        .trim_end_matches('\r');
                    let line_number = self.show_line_numbers.then(|| mat.line_number()).flatten();
                    self.lines
                        .push(format_grep_line(&self.relative, line_number, ':', text));
                    Ok(true)
                }

                fn context(
                    &mut self,
                    _searcher: &Searcher,
                    ctx: &grep_searcher::SinkContext<'_>,
                ) -> Result<bool, Self::Error> {
                    let text = std::str::from_utf8(ctx.bytes())
                        .unwrap_or("")
                        .trim_end_matches('\n')
                        .trim_end_matches('\r');
                    let line_number = self.show_line_numbers.then(|| ctx.line_number()).flatten();
                    self.lines
                        .push(format_grep_line(&self.relative, line_number, '-', text));
                    Ok(true)
                }

                fn context_break(&mut self, _searcher: &Searcher) -> Result<bool, Self::Error> {
                    if self.needs_separator {
                        self.lines.push("--".to_string());
                        self.needs_separator = false;
                    }
                    Ok(true)
                }
            }

            let mut sink = ContentSink {
                relative: relative.to_string(),
                show_line_numbers,
                lines: Vec::new(),
                needs_separator: false,
            };
            let _ = run_search(searcher, matcher, source, &mut sink);
            if sink.lines.last().is_some_and(|l| l == "--") {
                sink.lines.pop();
            }
            output_lines.extend(sink.lines);
        }
        _ => unreachable!(),
    }
}

/// Apply the post-search finalization shared by the directory grep and the
/// single-file bytes renderer: the empty-result message, then the
/// `offset`/`head_limit` match-window slice.
pub(crate) fn finalize_grep_output(
    output: String,
    pattern: &str,
    offset: usize,
    head_limit: Option<u32>,
) -> String {
    if output.is_empty() {
        return format!("No matches found for pattern '{}'", pattern);
    }

    let lines: Vec<&str> = output.lines().collect();
    let sliced = if offset >= lines.len() {
        Vec::new()
    } else {
        match head_limit {
            Some(limit) => lines[offset..]
                .iter()
                .take(limit as usize)
                .copied()
                .collect(),
            None => lines[offset..].to_vec(),
        }
    };

    sliced.join("\n")
}

/// Render a grep over a single file's bytes, reproducing the live single-file
/// read-grep output without touching the filesystem. The live read feeds this
/// from disk; archival reconstruction will feed it from a git blob. A single
/// file always defaults to `content` mode (you already named the file), and
/// directory/glob/file-type walks stay in `grep_search`. Both paths share this
/// module's matcher, searcher, per-file collection, and finalization helpers, so
/// there is no frozen second copy of the grep-rendering logic.
pub(crate) fn render_single_file_grep(bytes: &[u8], label: &str, payload: &GrepPayload) -> String {
    let output_mode = payload.output_mode.as_deref().unwrap_or("content");
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        );
    }

    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let offset = payload.offset.unwrap_or(0) as usize;
    let head_limit = payload.head_limit;

    let matcher = match build_grep_matcher(payload) {
        Ok(matcher) => matcher,
        Err(error) => return error,
    };
    let mut searcher = build_grep_searcher(payload);

    let mut output_lines: Vec<String> = Vec::new();
    grep_collect_into(
        &mut searcher,
        &matcher,
        label,
        GrepSource::Bytes(bytes),
        output_mode,
        show_line_numbers,
        &mut output_lines,
    );

    finalize_grep_output(
        output_lines.join("\n"),
        &payload.pattern,
        offset,
        head_limit,
    )
}

// ---------------------------------------------------------------------------
// Shared grep param/payload helpers used by both the file-tree pushdown path
// and the universal post-render grep over already-materialized bodies.
// ---------------------------------------------------------------------------

fn grep_query_value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .rev()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
}

fn grep_opt_u32(value: Option<&str>, key: &str) -> Result<Option<u32>, String> {
    value
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
        })
        .transpose()
}

fn grep_opt_bool(value: Option<&str>, key: &str) -> Result<Option<bool>, String> {
    value
        .map(|value| match value {
            "" | "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(format!(
                "Invalid boolean for query parameter '{key}': {value}"
            )),
        })
        .transpose()
}

/// Build a [`GrepPayload`] from the shared grep modifier params (`context`,
/// `-A`/`-B`/`-C`, `-i`, `-n`, `head_limit`, `multiline`). The caller supplies
/// the target-specific fields (`pattern`, `glob`, `file_type`, `output_mode`,
/// `offset`) and an optional `head_limit_fallback` (the `limit` alias). This is
/// the single field-mapping shared by the filesystem projection parser and the
/// universal body-grep parser so neither carries a frozen second copy.
pub(crate) fn build_grep_payload(
    params: &[QueryParam],
    pattern: String,
    glob: Option<String>,
    file_type: Option<String>,
    output_mode: Option<String>,
    offset: Option<u32>,
    head_limit_fallback: Option<u32>,
) -> Result<GrepPayload, String> {
    Ok(GrepPayload {
        pattern,
        path: None,
        glob,
        file_type,
        output_mode,
        context: grep_opt_u32(grep_query_value(params, "context"), "context")?,
        after_context: grep_opt_u32(grep_query_value(params, "-A"), "-A")?,
        before_context: grep_opt_u32(grep_query_value(params, "-B"), "-B")?,
        context_alias: grep_opt_u32(grep_query_value(params, "-C"), "-C")?,
        case_insensitive: grep_opt_bool(grep_query_value(params, "-i"), "-i")?,
        line_numbers: grep_opt_bool(grep_query_value(params, "-n"), "-n")?,
        head_limit: grep_opt_u32(grep_query_value(params, "head_limit"), "head_limit")?
            .or(head_limit_fallback),
        offset,
        multiline: grep_opt_bool(grep_query_value(params, "multiline"), "multiline")?,
    })
}

/// Parse the universal body-grep payload from a target's query params. Returns
/// `None` when no `grep` is present (the caller renders the body normally).
///
/// A materialized body has no file dimension, so this rejects the tree-only
/// modes and selectors: `output_mode=files_with_matches|count` (need a
/// directory/multi-file target), `type` (file-type walk), and `glob` unless the
/// caller `allow_glob` (only `/changed`, which keeps `glob` as its own pushdown
/// applied before grep). `offset` is rejected (paginate matches via
/// `head_limit`); `limit` aliases `head_limit`. The resolved `output_mode` is
/// always `content`.
pub(crate) fn body_grep_payload(
    params: &[QueryParam],
    allow_glob: bool,
) -> Result<Option<GrepPayload>, String> {
    let Some(pattern) = grep_query_value(params, "grep") else {
        return Ok(None);
    };
    if pattern.is_empty() {
        return Err(
            "Empty 'grep' pattern; omit 'grep' for a plain read or provide a search pattern"
                .to_string(),
        );
    }
    if grep_query_value(params, "offset").is_some() {
        return Err("'offset' is a line-window and does not combine with 'grep'; use 'head_limit' or 'limit' to cap the number of matches".to_string());
    }
    if !allow_glob && grep_query_value(params, "glob").is_some() {
        return Err("'glob' selects files within a tree and does not apply to a single rendered body; grep filters the produced text directly".to_string());
    }
    if grep_query_value(params, "type").is_some() {
        return Err(
            "'type' selects files within a tree and does not apply to a single rendered body"
                .to_string(),
        );
    }
    if let Some(mode) = grep_query_value(params, "output_mode") {
        match mode {
            "content" => {}
            "files_with_matches" | "count" => {
                return Err(format!(
                    "output_mode '{}' needs a directory or multi-file target; grep over a single body returns line-numbered content",
                    mode
                ))
            }
            other => {
                return Err(format!(
                    "Invalid output_mode '{}'. A single-body grep only supports 'content'.",
                    other
                ))
            }
        }
    }
    let head_limit_fallback = grep_opt_u32(grep_query_value(params, "limit"), "limit")?;
    let payload = build_grep_payload(
        params,
        pattern.to_string(),
        None,
        None,
        Some("content".to_string()),
        None,
        head_limit_fallback,
    )?;
    Ok(Some(payload))
}

/// Run the universal post-render grep over an already-materialized body: grep
/// the produced text with the same matcher/searcher/finalizer the single-file
/// path uses (path-less output), then count real matches. Returns the rendered
/// grep body and its match count.
pub(crate) fn grep_materialized_body(body: &str, payload: &GrepPayload) -> (String, usize) {
    let rendered = render_single_file_grep(body.as_bytes(), "", payload);
    let (matches, _files) = crate::mcp::handlers::read::grep_counts(&rendered);
    (rendered, matches)
}

/// Perform the actual in-process grep search over a filesystem tree.
pub fn grep_search(
    payload: GrepPayload,
    search_path: &Path,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
) -> Result<String, String> {
    use ignore::overrides::OverrideBuilder;
    use ignore::types::TypesBuilder;
    use ignore::WalkBuilder;

    let matcher = build_grep_matcher(&payload)?;

    let is_file = search_path.is_file();
    let walk_root = if is_file {
        search_path.parent().unwrap_or(search_path)
    } else {
        search_path
    };

    let mut walker_builder = WalkBuilder::new(if is_file { search_path } else { walk_root });
    let filter_root = search_path.to_path_buf();
    walker_builder
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .filter_entry(move |entry| should_walk_entry(entry.path(), &filter_root, &deny_read));

    if let Some(ref ft) = payload.file_type {
        let mut types = TypesBuilder::new();
        types.add_defaults();
        types
            .select(ft)
            .build()
            .map_err(|e| format!("Invalid file type '{}': {}", ft, e))
            .map(|t| {
                walker_builder.types(t);
            })?;
    }

    if let Some(ref g) = payload.glob {
        let mut overrides = OverrideBuilder::new(walk_root);
        overrides
            .add(g)
            .map_err(|e| format!("Invalid glob '{}': {}", g, e))?;
        walker_builder.overrides(
            overrides
                .build()
                .map_err(|e| format!("Failed to build glob override: {}", e))?,
        );
    }

    let mut searcher = build_grep_searcher(&payload);

    let base_path = if is_file {
        search_path.parent().unwrap_or(search_path)
    } else {
        search_path
    };

    let mut output_lines: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + GREP_TIMEOUT;

    for entry in walker_builder.build() {
        if std::time::Instant::now() > deadline {
            log::warn!("grep walk timed out");
            break;
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }

        let path = entry.path().to_path_buf();
        let relative = path
            .strip_prefix(base_path)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();

        grep_collect_into(
            &mut searcher,
            &matcher,
            &relative,
            GrepSource::Path(&path),
            output_mode,
            show_line_numbers,
            &mut output_lines,
        );
    }

    Ok(output_lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worktree_index_search_subdir_accepts_root_and_subdir_but_not_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let src = root.join("src");
        let file = src.join("main.rs");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(&file, "fn main() {}\n").unwrap();

        let worktree_canon = std::fs::canonicalize(root).unwrap();
        let src_canon = std::fs::canonicalize(&src).unwrap();
        let file_canon = std::fs::canonicalize(&file).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_canon = std::fs::canonicalize(outside.path()).unwrap();

        assert_eq!(
            worktree_index_search_subdir(&worktree_canon, &worktree_canon),
            Some(None)
        );
        assert_eq!(
            worktree_index_search_subdir(&src_canon, &worktree_canon),
            Some(Some("src".to_string()))
        );
        assert_eq!(
            worktree_index_search_subdir(&file_canon, &worktree_canon),
            None
        );
        assert_eq!(
            worktree_index_search_subdir(&outside_canon, &worktree_canon),
            None
        );
    }

    #[test]
    fn grep_default_mode_is_content_for_a_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("needle.txt");
        std::fs::write(&file, "hay\nneedle\nhay\n").unwrap();

        assert_eq!(default_grep_output_mode(&file), "content");
    }

    #[test]
    fn grep_default_mode_is_files_with_matches_for_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(default_grep_output_mode(dir.path()), "files_with_matches");
    }

    #[test]
    fn grep_default_mode_treats_missing_path_as_a_directory() {
        // A nonexistent path isn't a file, so it falls back to the
        // directory-style default rather than printing per-line content.
        let missing = Path::new("/this/path/does/not/exist/anywhere");
        assert_eq!(default_grep_output_mode(missing), "files_with_matches");
    }

    #[test]
    fn grep_search_emits_context_lines() {
        // Locks in the context-line feature reachable via `?grep=foo&-C=N`: the
        // matched line uses a `:` separator, surrounding context lines use `-`.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ctx.txt");
        std::fs::write(&file, "alpha\nbeta\nNEEDLE\ngamma\ndelta\n").unwrap();

        let payload = GrepPayload {
            pattern: "NEEDLE".to_string(),
            path: Some(file.display().to_string()),
            glob: None,
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

        let output = grep_search(payload, &file, "content", true, Vec::new()).unwrap();
        // Match line, plus one context line on each side, in the expected format.
        assert!(output.contains("ctx.txt:3:NEEDLE"), "match line: {output}");
        assert!(
            output.contains("ctx.txt:2-beta"),
            "before-context: {output}"
        );
        assert!(
            output.contains("ctx.txt:4-gamma"),
            "after-context: {output}"
        );
        // Out-of-window lines are not surfaced with a single line of context.
        assert!(!output.contains("alpha"), "alpha outside context: {output}");
        assert!(!output.contains("delta"), "delta outside context: {output}");
    }

    fn qp(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn body_grep_payload_none_without_grep() {
        assert!(body_grep_payload(&[qp("limit", "5")], false)
            .unwrap()
            .is_none());
    }

    #[test]
    fn body_grep_payload_rejects_invalid_combinations() {
        // Empty pattern, offset+grep, and the tree-only output modes/selectors
        // are all rejected on a single materialized body.
        assert!(body_grep_payload(&[qp("grep", "")], false).is_err());
        assert!(body_grep_payload(&[qp("grep", "x"), qp("offset", "2")], false).is_err());
        assert!(body_grep_payload(
            &[qp("grep", "x"), qp("output_mode", "files_with_matches")],
            false
        )
        .is_err());
        assert!(body_grep_payload(&[qp("grep", "x"), qp("output_mode", "count")], false).is_err());
        assert!(body_grep_payload(&[qp("grep", "x"), qp("type", "rust")], false).is_err());
        // glob is rejected unless the caller (only `/changed`) allows it.
        assert!(body_grep_payload(&[qp("grep", "x"), qp("glob", "*.rs")], false).is_err());
        assert!(
            body_grep_payload(&[qp("grep", "x"), qp("glob", "*.rs")], true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn body_grep_payload_aliases_limit_and_defaults_to_content() {
        let payload = body_grep_payload(&[qp("grep", "x"), qp("limit", "7")], false)
            .unwrap()
            .unwrap();
        assert_eq!(payload.head_limit, Some(7));
        assert_eq!(payload.output_mode.as_deref(), Some("content"));
        assert_eq!(payload.offset, None);
        // An explicit head_limit wins over the limit alias.
        let payload = body_grep_payload(
            &[qp("grep", "x"), qp("limit", "7"), qp("head_limit", "3")],
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(payload.head_limit, Some(3));
    }

    #[test]
    fn grep_materialized_body_drops_path_and_counts_matches() {
        let body = "alpha\nneedle one\ngamma\nneedle two\n";
        let payload = body_grep_payload(&[qp("grep", "needle")], false)
            .unwrap()
            .unwrap();
        let (rendered, matches) = grep_materialized_body(body, &payload);
        assert_eq!(matches, 2);
        // Path-less, line-number-prefixed match lines.
        assert!(rendered.contains("2:needle one"), "{rendered}");
        assert!(rendered.contains("4:needle two"), "{rendered}");
        assert!(!rendered.contains(":2:"), "no path prefix: {rendered}");

        // An empty result yields the shared finalizer's message.
        let payload = body_grep_payload(&[qp("grep", "zzz")], false)
            .unwrap()
            .unwrap();
        let (rendered, matches) = grep_materialized_body(body, &payload);
        assert_eq!(matches, 0);
        assert_eq!(rendered, "No matches found for pattern 'zzz'");
    }

    #[test]
    fn grep_context_flag_implies_content_for_a_directory() {
        // A directory grep that asks for context but names no output_mode must
        // default to `content`, not `files_with_matches` — otherwise both the
        // requested context and the matched lines are silently dropped.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(resolve_grep_output_mode(None, true, dir.path()), "content");
    }

    #[test]
    fn grep_directory_without_context_stays_files_with_matches() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve_grep_output_mode(None, false, dir.path()),
            "files_with_matches"
        );
    }

    #[test]
    fn grep_explicit_output_mode_wins_over_context_flag() {
        // An explicit output_mode is honored even when context flags are set.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve_grep_output_mode(Some("files_with_matches"), true, dir.path()),
            "files_with_matches"
        );
    }

    #[test]
    fn grep_directory_with_context_returns_content_lines() {
        // End-to-end: resolving the mode for a directory + context flag yields
        // `content`, and grepping the directory in that mode surfaces the match
        // plus its surrounding context lines.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ctx.txt");
        std::fs::write(&file, "alpha\nbeta\nNEEDLE\ngamma\ndelta\n").unwrap();

        let mode = resolve_grep_output_mode(None, true, dir.path());
        assert_eq!(mode, "content");

        let payload = GrepPayload {
            pattern: "NEEDLE".to_string(),
            path: Some(dir.path().display().to_string()),
            glob: None,
            file_type: None,
            output_mode: None,
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

        let output = grep_search(payload, dir.path(), mode, true, Vec::new()).unwrap();
        assert!(output.contains("ctx.txt:3:NEEDLE"), "match line: {output}");
        assert!(
            output.contains("ctx.txt:2-beta"),
            "before-context: {output}"
        );
        assert!(
            output.contains("ctx.txt:4-gamma"),
            "after-context: {output}"
        );
    }

    fn grep_payload(pattern: &str) -> GrepPayload {
        GrepPayload {
            pattern: pattern.to_string(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: None,
            context: None,
            after_context: None,
            before_context: None,
            context_alias: None,
            case_insensitive: None,
            line_numbers: None,
            head_limit: None,
            offset: None,
            multiline: None,
        }
    }

    /// Reference single-file grep through the filesystem walker — the behavior
    /// `render_single_file_grep` must reproduce byte-for-byte from in-memory
    /// bytes. Both paths share this module's matcher/searcher/collect/finalize
    /// helpers, so this asserts the two entry points stay in lockstep.
    fn fs_single_file_grep(content: &str, label: &str, payload: &GrepPayload) -> String {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(label);
        std::fs::write(&file, content).unwrap();

        let requested_context = payload.context.is_some()
            || payload.context_alias.is_some()
            || payload.after_context.is_some()
            || payload.before_context.is_some();
        let mode =
            resolve_grep_output_mode(payload.output_mode.as_deref(), requested_context, &file)
                .to_string();
        let show = payload.line_numbers.unwrap_or(true);
        let offset = payload.offset.unwrap_or(0) as usize;
        let raw = grep_search(payload.clone(), &file, &mode, show, Vec::new()).unwrap();
        finalize_grep_output(raw, &payload.pattern, offset, payload.head_limit)
    }

    fn assert_single_file_grep_matches(content: &str, label: &str, payload: &GrepPayload) {
        let expected = fs_single_file_grep(content, label, payload);
        let actual = render_single_file_grep(content.as_bytes(), label, payload);
        assert_eq!(actual, expected, "payload: {payload:?}");
    }

    #[test]
    fn render_single_file_grep_plain_matches_filesystem() {
        assert_single_file_grep_matches(
            "alpha\nNEEDLE\ngamma\nNEEDLE again\n",
            "f.txt",
            &grep_payload("NEEDLE"),
        );
    }

    #[test]
    fn render_single_file_grep_with_context_matches_filesystem() {
        let mut payload = grep_payload("NEEDLE");
        payload.context_alias = Some(1);
        assert_single_file_grep_matches("alpha\nbeta\nNEEDLE\ngamma\ndelta\n", "ctx.txt", &payload);
    }

    #[test]
    fn render_single_file_grep_with_head_limit_matches_filesystem() {
        let mut payload = grep_payload("NEEDLE");
        payload.head_limit = Some(1);
        assert_single_file_grep_matches(
            "NEEDLE one\nNEEDLE two\nNEEDLE three\n",
            "f.txt",
            &payload,
        );
    }

    #[test]
    fn render_single_file_grep_case_insensitive_matches_filesystem() {
        let mut payload = grep_payload("needle");
        payload.case_insensitive = Some(true);
        assert_single_file_grep_matches("NEEDLE\nhay\nNeEdLe\n", "f.txt", &payload);
    }

    #[test]
    fn render_single_file_grep_count_mode_matches_filesystem() {
        let mut payload = grep_payload("NEEDLE");
        payload.output_mode = Some("count".to_string());
        assert_single_file_grep_matches("NEEDLE\nNEEDLE\nhay\n", "f.txt", &payload);
    }

    #[test]
    fn render_single_file_grep_files_with_matches_mode_matches_filesystem() {
        let mut payload = grep_payload("NEEDLE");
        payload.output_mode = Some("files_with_matches".to_string());
        assert_single_file_grep_matches("NEEDLE\nhay\n", "f.txt", &payload);
    }

    #[test]
    fn render_single_file_grep_no_matches_reports_no_matches() {
        let payload = grep_payload("absent");
        let rendered = render_single_file_grep(b"alpha\nbeta\n", "f.txt", &payload);
        assert_eq!(rendered, "No matches found for pattern 'absent'");
        assert_eq!(
            rendered,
            fs_single_file_grep("alpha\nbeta\n", "f.txt", &payload)
        );
    }
}
