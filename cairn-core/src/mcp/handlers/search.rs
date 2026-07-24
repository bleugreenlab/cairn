//! Search MCP handlers.
//!
//! Handles: search, glob, grep

use crate::mcp::types::McpCallbackRequest;
use crate::models::SearchContentType;
use crate::orchestrator::Orchestrator;
use cairn_common::query::QueryParam;
use cairn_symbols::search_util::{build_glob_matcher, render_grep_lines, GrepLine};
use grep_searcher::SinkError;
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Payload for search tool
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPayload {
    query: String,
    content_types: Option<Vec<String>>,
    /// Author-role facet: `assistant`/`user`/`tool` for events, `user`/`agent`
    /// for comments.
    role: Option<String>,
    /// Match the query against the title field only (the `in=title` axis).
    #[serde(default)]
    title_only: bool,
    project_id: Option<String>,
    issue_id: Option<String>,
    since: Option<i64>,
    limit: Option<usize>,
}

pub(crate) fn render_tree_grep(files: &[(String, Vec<u8>)], payload: &GrepPayload) -> String {
    let output_mode = payload
        .output_mode
        .as_deref()
        .unwrap_or("files_with_matches");
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        );
    }
    let matcher = match build_grep_matcher(payload) {
        Ok(matcher) => matcher,
        Err(error) => return error,
    };
    let glob = match payload.glob.as_deref() {
        Some(pattern) => match cairn_symbols::search_util::build_glob_matcher(pattern) {
            Ok(matcher) => Some(matcher),
            Err(error) => return error,
        },
        None => None,
    };
    let file_types = match payload.file_type.as_deref() {
        Some(file_type) => {
            let mut builder = ignore::types::TypesBuilder::new();
            builder.add_defaults();
            builder.select(file_type);
            match builder.build() {
                Ok(types) => Some(types),
                Err(error) => return format!("Invalid file type '{}': {}", file_type, error),
            }
        }
        None => None,
    };
    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let mut collected: Vec<GrepLine> = Vec::new();
    for (path, bytes) in files {
        let path_ref = Path::new(path);
        if glob.as_ref().is_some_and(|matcher| {
            !matcher.is_match(path_ref)
                && !path_ref
                    .file_name()
                    .is_some_and(|name| matcher.is_match(name))
        }) {
            continue;
        }
        if file_types
            .as_ref()
            .is_some_and(|types| !types.matched(path_ref, false).is_whitelist())
        {
            continue;
        }
        let mut searcher = build_grep_searcher(payload);
        if let Err(error) = grep_collect_into(
            &mut searcher,
            &matcher,
            path,
            GrepSource::Bytes(bytes),
            output_mode,
            None,
            &mut collected,
        ) {
            return error;
        }
    }
    let (before, after) = grep_context_window(payload);

    finalize_grep_output(
        render_grep_lines(
            collected,
            output_mode,
            show_line_numbers,
            false,
            before > 0 || after > 0,
        ),
        &payload.pattern,
        payload.offset.unwrap_or(0) as usize,
        payload.head_limit,
    )
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
    pattern: String,
    path: Option<String>,
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
pub(crate) fn resolve_grep_output_mode<'a>(
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
pub(crate) async fn handle_glob(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
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
pub(crate) async fn handle_grep(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
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
    try_worktree_index_grep_for_worktree(
        orch,
        IndexGrepRequest {
            worktree_raw: &worktree_raw,
            search_path,
            payload,
            output_mode,
            show_line_numbers,
            deny_read,
            native_output: false,
            globs: None,
            max_per_file: None,
            uncovered_timeout: GREP_TIMEOUT,
        },
    )
    .await
}

/// One warm-index grep attempt, as issued by `read ?grep=` or a translated
/// `run` search.
pub(crate) struct IndexGrepRequest<'a> {
    /// Raw DB `worktree_path`, which is also the index pool's key.
    pub worktree_raw: &'a Path,
    pub search_path: &'a Path,
    pub payload: &'a GrepPayload,
    pub output_mode: &'a str,
    pub show_line_numbers: bool,
    pub deny_read: &'a [PathBuf],
    /// Render context lines in ripgrep's `path-N-text` form rather than
    /// Cairn's `path:N-text`.
    pub native_output: bool,
    /// A translated run search's full glob list; `None` uses `payload.glob`.
    pub globs: Option<&'a [String]>,
    pub max_per_file: Option<usize>,
    /// Budget for re-grepping whatever the index could not cover. Overrunning
    /// it declines the query so the caller's own walk answers it in one pass.
    pub uncovered_timeout: Duration,
}

/// Shared fff-first grep seam for `read ?grep=` and translated `run` searches.
///
/// The index answers what it can prove and names the files it could not cover;
/// this greps those few files with the ripgrep walk, merges their lines into
/// the index's, and renders once. `None` tells the caller the index could not
/// contribute at all, so its own cancellation-aware walk should answer.
pub(crate) async fn try_worktree_index_grep_for_worktree(
    orch: &Orchestrator,
    request: IndexGrepRequest<'_>,
) -> Option<String> {
    let IndexGrepRequest {
        worktree_raw,
        search_path,
        payload,
        output_mode,
        show_line_numbers,
        deny_read,
        native_output,
        globs,
        max_per_file,
        uncovered_timeout,
    } = request;
    if payload.multiline == Some(true) || payload.file_type.is_some() {
        return None;
    }
    let scope = resolve_worktree_scope(orch, search_path, worktree_raw)?;
    let worktree_canon = scope.worktree_canon.clone();
    let subdir_label = scope
        .subdir
        .as_deref()
        .map(|rel| format!(" subdir={rel}"))
        .unwrap_or_default();

    let (before_context, after_context) = grep_context_window(payload);
    let params = crate::worktree_search::WorktreeGrepParams {
        pattern: payload.pattern.clone(),
        subdir: scope.subdir.clone(),
        globs: globs
            .map(<[String]>::to_vec)
            .unwrap_or_else(|| payload.glob.clone().into_iter().collect()),
        max_per_file,
        output_mode: output_mode.to_string(),
        case_insensitive: payload.case_insensitive,
        before_context,
        after_context,
        show_line_numbers,
    };
    let deny = deny_read.to_vec();
    let search = scope.search.clone();

    // fff grep is CPU-bound (up to its time budget), so run it off the async
    // runtime exactly like the ripgrep walk it replaces.
    let outcome = tokio::task::spawn_blocking(move || search.try_grep(&params, &deny))
        .await
        .ok()??;

    let mut lines = outcome.lines;
    if !outcome.uncovered.is_empty() {
        let uncovered = outcome.uncovered.clone();
        let walk_payload = payload.clone();
        let root = search_path.to_path_buf();
        let mode = output_mode.to_string();
        let covered = tokio::task::spawn_blocking(move || {
            grep_uncovered_files(
                &walk_payload,
                &root,
                &uncovered,
                &mode,
                max_per_file,
                uncovered_timeout,
            )
        })
        .await
        .ok()?;
        match covered {
            Ok(extra) => lines.extend(extra),
            Err(error) => {
                log::debug!(
                    "worktree_search: could not cover {} residual file(s) for {} ({error}); \
                     falling through to the full walk",
                    outcome.uncovered.len(),
                    worktree_canon.display()
                );
                return None;
            }
        }
    }

    log::debug!(
        "worktree_search: served grep for {}{subdir_label} from warm index ({} file(s) re-grepped)",
        worktree_canon.display(),
        outcome.uncovered.len()
    );
    Some(render_grep_lines(
        lines,
        output_mode,
        show_line_numbers,
        native_output,
        before_context > 0 || after_context > 0,
    ))
}

/// Grep the files the warm index could not cover, each as an explicit
/// single-file target so no directory is traversed twice. An error means the
/// budget ran out, and the caller answers the whole query with its own walk
/// rather than returning a knowingly incomplete result.
pub(crate) fn grep_uncovered_files(
    payload: &GrepPayload,
    search_root: &Path,
    uncovered: &[String],
    output_mode: &str,
    max_per_file: Option<usize>,
    timeout: Duration,
) -> Result<Vec<GrepLine>, String> {
    let matcher = build_grep_matcher(payload)?;
    let mut searcher = build_grep_searcher(payload);
    let (_, after) = grep_context_window(payload);
    let cap = GrepCap::new(max_per_file, after);
    let deadline = Instant::now() + timeout;
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut lines = Vec::new();
    for relative in uncovered {
        if Instant::now() >= deadline {
            return Err(format!("covering the residual files exceeded {timeout:?}"));
        }
        let path = search_root.join(relative);
        // The watcher may have removed the file since the index listed it.
        if !path.is_file() {
            continue;
        }
        let control = SearchControl {
            deadline,
            cancelled: cancelled.clone(),
        };
        grep_collect_into(
            &mut searcher,
            &matcher,
            relative,
            GrepSource::Path(&path, control),
            output_mode,
            cap,
            &mut lines,
        )?;
    }
    Ok(lines)
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

struct SearchControl {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}

impl SearchControl {
    fn timed_out(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline
    }
}

struct ControlledReader {
    file: std::fs::File,
    control: SearchControl,
}

impl Read for ControlledReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.control.timed_out() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "grep deadline elapsed",
            ));
        }
        self.file.read(buffer)
    }
}

/// A content source for a single-file grep: a filesystem path (live directory
/// walk) or an in-memory byte slice (single-file read / archival reconstruction).
enum GrepSource<'a> {
    Path(&'a Path, SearchControl),
    Bytes(&'a [u8]),
}

/// Run a configured searcher over one source. Files use a deadline-aware reader
/// rather than `search_path`, so an outer timeout can cooperatively stop a scan
/// that is already inside one large file instead of detaching hidden work.
fn run_search<S: grep_searcher::Sink>(
    searcher: &mut grep_searcher::Searcher,
    matcher: &grep_regex::RegexMatcher,
    source: GrepSource<'_>,
    sink: S,
) -> Result<(), S::Error> {
    match source {
        GrepSource::Path(path, control) => {
            let file = std::fs::File::open(path).map_err(S::Error::error_io)?;
            searcher.search_reader(matcher, ControlledReader { file, control }, sink)
        }
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

/// ripgrep's `-m N` for one file. The cap is not "stop at the Nth match":
/// ripgrep keeps printing that match's after-context window, and a line inside
/// the window that itself matches renders as a *match* (`:` separator) while
/// neither counting toward the cap nor extending the window. Carrying the
/// after-context alongside the count is what lets the sink reproduce that.
#[derive(Clone, Copy)]
struct GrepCap {
    max_per_file: usize,
    after_context: u64,
}

impl GrepCap {
    fn new(max_per_file: Option<usize>, after_context: usize) -> Option<Self> {
        max_per_file.map(|max_per_file| Self {
            max_per_file,
            after_context: after_context as u64,
        })
    }
}

/// The context window a payload asks for: the `-C`/`context` alias supplies
/// both sides, and `-A`/`-B` override their own side. The single resolution
/// used by the searcher, the warm index params, and the renderer's group
/// separators.
pub(crate) fn grep_context_window(payload: &GrepPayload) -> (usize, usize) {
    let combined = payload.context_alias.or(payload.context);
    (
        payload.before_context.or(combined).unwrap_or(0) as usize,
        payload.after_context.or(combined).unwrap_or(0) as usize,
    )
}

/// Build the searcher (binary detection, line numbers, context window) for a
/// grep. Shared by the directory walker and the single-file bytes renderer.
fn build_grep_searcher(payload: &GrepPayload) -> grep_searcher::Searcher {
    let mut builder = grep_searcher::SearcherBuilder::new();
    let (before, after) = grep_context_window(payload);
    builder
        .binary_detection(grep_searcher::BinaryDetection::quit(b'\x00'))
        .line_number(true)
        .before_context(before)
        .after_context(after);

    if payload.multiline.unwrap_or(false) {
        builder.multi_line(true);
    }

    builder.build()
}

/// Collect one source's grep hits as [`GrepLine`]s, appending to `lines`. This
/// is the single per-file collector shared by the directory walker
/// (`grep_search`), the tree renderer, and the single-file bytes renderer, and
/// it produces the same model the warm index produces — so
/// [`render_grep_lines`] is the only place output bytes are decided.
///
/// `files_with_matches` and `count` need only match coordinates, so their lines
/// carry no text; the renderer projects them to paths and per-path counts.
fn grep_collect_into(
    searcher: &mut grep_searcher::Searcher,
    matcher: &grep_regex::RegexMatcher,
    relative: &str,
    source: GrepSource<'_>,
    output_mode: &str,
    cap: Option<GrepCap>,
    lines: &mut Vec<GrepLine>,
) -> Result<(), String> {
    use grep_searcher::Searcher;

    let max_per_file = cap.map(|cap| cap.max_per_file);
    // `-m 0` asks for no matches at all — ripgrep prints nothing and exits 1.
    // Short-circuit before searching so no output mode can record a first hit
    // and only then discover the cap.
    if max_per_file == Some(0) {
        return Ok(());
    }

    fn coordinate(relative: &str, line_number: u64) -> GrepLine {
        GrepLine {
            path: relative.to_string(),
            line_number,
            is_match: true,
            text: String::new(),
        }
    }

    match output_mode {
        "files_with_matches" => {
            let mut first: Option<u64> = None;
            let sink = grep_searcher::sinks::UTF8(|line_number, _| {
                first = Some(line_number);
                Ok(false)
            });
            run_search(searcher, matcher, source, sink).map_err(|error| error.to_string())?;
            if let Some(line_number) = first {
                lines.push(coordinate(relative, line_number));
            }
        }
        "count" => {
            let mut matched: Vec<u64> = Vec::new();
            let sink = grep_searcher::sinks::UTF8(|line_number, _| {
                matched.push(line_number);
                Ok(max_per_file.is_none_or(|max| matched.len() < max))
            });
            run_search(searcher, matcher, source, sink).map_err(|error| error.to_string())?;
            lines.extend(
                matched
                    .into_iter()
                    .map(|line_number| coordinate(relative, line_number)),
            );
        }
        "content" => {
            struct ContentSink<'a> {
                relative: &'a str,
                cap: Option<GrepCap>,
                matched: usize,
                /// Last line the capped match's after-context window reaches,
                /// set once the cap is hit. See [`GrepCap`].
                window_end: Option<u64>,
                lines: Vec<GrepLine>,
            }

            impl ContentSink<'_> {
                fn push(&mut self, line_number: u64, is_match: bool, bytes: &[u8]) {
                    self.lines.push(GrepLine {
                        path: self.relative.to_string(),
                        line_number,
                        is_match,
                        text: std::str::from_utf8(bytes)
                            .unwrap_or("")
                            .trim_end_matches('\n')
                            .trim_end_matches('\r')
                            .to_string(),
                    });
                }
            }

            impl grep_searcher::Sink for ContentSink<'_> {
                type Error = std::io::Error;

                fn matched(
                    &mut self,
                    _searcher: &Searcher,
                    mat: &grep_searcher::SinkMatch<'_>,
                ) -> Result<bool, Self::Error> {
                    let line_number = mat.line_number().unwrap_or(0);
                    // Inside the capped match's window this line still renders
                    // as a match, but it neither counts toward the cap nor
                    // extends the window.
                    if let Some(end) = self.window_end {
                        if line_number > end {
                            return Ok(false);
                        }
                        self.push(line_number, true, mat.bytes());
                        return Ok(true);
                    }
                    self.push(line_number, true, mat.bytes());
                    self.matched += 1;
                    let Some(cap) = self.cap.filter(|cap| self.matched >= cap.max_per_file) else {
                        return Ok(true);
                    };
                    self.window_end = Some(line_number + cap.after_context);
                    // With no after-context there is nothing left to drain.
                    Ok(cap.after_context > 0)
                }

                fn context(
                    &mut self,
                    _searcher: &Searcher,
                    ctx: &grep_searcher::SinkContext<'_>,
                ) -> Result<bool, Self::Error> {
                    let line_number = ctx.line_number().unwrap_or(0);
                    if self.window_end.is_some_and(|end| line_number > end) {
                        return Ok(false);
                    }
                    self.push(line_number, false, ctx.bytes());
                    Ok(true)
                }
            }

            let mut sink = ContentSink {
                relative,
                cap,
                matched: 0,
                window_end: None,
                lines: Vec::new(),
            };
            run_search(searcher, matcher, source, &mut sink).map_err(|error| error.to_string())?;
            lines.append(&mut sink.lines);
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Apply the post-search finalization shared by the directory grep and the
/// single-file bytes renderer: the empty-result message, then the
/// `offset`/`head_limit` match-window slice.
fn finalize_grep_output(
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

    let mut collected: Vec<GrepLine> = Vec::new();
    if let Err(error) = grep_collect_into(
        &mut searcher,
        &matcher,
        label,
        GrepSource::Bytes(bytes),
        output_mode,
        None,
        &mut collected,
    ) {
        return error;
    }

    let (before, after) = grep_context_window(payload);
    finalize_grep_output(
        render_grep_lines(
            collected,
            output_mode,
            show_line_numbers,
            false,
            before > 0 || after > 0,
        ),
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

/// Engine-level bounds for one filesystem grep walk that the agent-facing
/// [`GrepPayload`] cannot express: the ripgrep-style glob list (repeated
/// `--glob`, `!` negations, the synthetic filename glob an explicit file target
/// produces), rg's `-m N` per-file match cap, and the deadline plus shared
/// cancel flag an outer timeout uses to stop a walk that is already inside one
/// large file.
pub(crate) struct GrepWalkLimits {
    pub globs: Vec<String>,
    pub max_per_file: Option<usize>,
    pub timeout: Duration,
    pub cancelled: Arc<AtomicBool>,
}

impl GrepWalkLimits {
    /// The `read ?grep=` walk: the payload's single glob, no per-file cap, the
    /// module timeout, and no external cancellation.
    fn for_payload(payload: &GrepPayload) -> Self {
        Self {
            globs: payload.glob.clone().into_iter().collect(),
            max_per_file: None,
            timeout: GREP_TIMEOUT,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Perform the actual in-process grep search over a filesystem tree.
pub fn grep_search(
    payload: GrepPayload,
    search_path: &Path,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
) -> Result<String, String> {
    let limits = GrepWalkLimits::for_payload(&payload);
    grep_search_with_format(
        payload,
        search_path,
        output_mode,
        show_line_numbers,
        deny_read,
        false,
        limits,
    )
}

/// The same walk rendered the way ripgrep renders it (`path-N-text` context
/// separators). This is the fallback engine behind a translated `run` search:
/// whenever the warm index declines, the walk answers instead, so an index
/// decline is a routing choice rather than a user-visible failure.
pub(crate) fn grep_search_native(
    payload: GrepPayload,
    search_path: &Path,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
    limits: GrepWalkLimits,
) -> Result<String, String> {
    grep_search_with_format(
        payload,
        search_path,
        output_mode,
        show_line_numbers,
        deny_read,
        true,
        limits,
    )
}

fn grep_search_with_format(
    payload: GrepPayload,
    search_path: &Path,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
    native_output: bool,
    limits: GrepWalkLimits,
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

    if !limits.globs.is_empty() {
        let mut overrides = OverrideBuilder::new(walk_root);
        for glob in &limits.globs {
            overrides
                .add(glob)
                .map_err(|e| format!("Invalid glob '{}': {}", glob, e))?;
        }
        walker_builder.overrides(
            overrides
                .build()
                .map_err(|e| format!("Failed to build glob override: {}", e))?,
        );
    }

    let mut searcher = build_grep_searcher(&payload);
    let (before, after) = grep_context_window(&payload);
    let cap = GrepCap::new(limits.max_per_file, after);

    let base_path = if is_file {
        search_path.parent().unwrap_or(search_path)
    } else {
        search_path
    };

    let mut collected: Vec<GrepLine> = Vec::new();
    let timeout = limits.timeout;
    let deadline = Instant::now() + timeout;

    for entry in walker_builder.build() {
        if limits.cancelled.load(Ordering::Relaxed) || Instant::now() >= deadline {
            log::warn!("grep walk timed out after {timeout:?}");
            return Err(format!("grep timed out after {timeout:?}"));
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        // Virtual search reads regular files only. Special entries such as
        // FIFOs can block in `File::open` before cooperative cancellation is
        // observable, while symlink/device semantics belong to native CLI
        // execution rather than the node-tree read contract.
        if entry.file_type().is_none_or(|ft| !ft.is_file()) {
            continue;
        }

        let path = entry.path().to_path_buf();
        let relative = path
            .strip_prefix(base_path)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();

        let control = SearchControl {
            deadline,
            cancelled: limits.cancelled.clone(),
        };
        if let Err(error) = grep_collect_into(
            &mut searcher,
            &matcher,
            &relative,
            GrepSource::Path(&path, control),
            output_mode,
            cap,
            &mut collected,
        ) {
            if limits.cancelled.load(Ordering::Relaxed)
                || Instant::now() >= deadline
                || error.contains("deadline elapsed")
            {
                return Err(format!("grep timed out after {timeout:?}"));
            }
            return Err(error);
        }
    }

    Ok(render_grep_lines(
        collected,
        output_mode,
        show_line_numbers,
        native_output,
        before > 0 || after > 0,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_grep_deadline_is_failure_not_partial_success() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("match.txt"), "needle\n").unwrap();
        let payload = GrepPayload {
            pattern: "needle".to_string(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some("content".to_string()),
            context: None,
            after_context: None,
            before_context: None,
            context_alias: None,
            case_insensitive: Some(false),
            line_numbers: Some(true),
            head_limit: None,
            offset: None,
            multiline: Some(false),
        };

        let result = grep_search_native(
            payload,
            dir.path(),
            "content",
            true,
            Vec::new(),
            GrepWalkLimits {
                globs: Vec::new(),
                max_per_file: None,
                timeout: Duration::ZERO,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        );
        assert!(matches!(result, Err(error) if error.starts_with("grep timed out after")));
    }

    #[test]
    fn controlled_reader_stops_after_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.txt");
        std::fs::write(&path, vec![b'x'; 128 * 1024]).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut reader = ControlledReader {
            file: std::fs::File::open(path).unwrap(),
            control: SearchControl {
                deadline: Instant::now() + Duration::from_secs(60),
                cancelled: cancelled.clone(),
            },
        };
        let mut buffer = [0_u8; 4096];
        assert!(reader.read(&mut buffer).unwrap() > 0);
        cancelled.store(true, Ordering::Relaxed);
        let error = reader.read(&mut buffer).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[cfg(unix)]
    #[test]
    fn native_grep_skips_fifo_and_worker_terminates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("match.txt"), "needle\n").unwrap();
        let fifo = dir.path().join("blocked.fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let root = dir.path().to_path_buf();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let payload = GrepPayload {
                pattern: "needle".to_string(),
                path: None,
                glob: None,
                file_type: None,
                output_mode: Some("content".to_string()),
                context: None,
                after_context: None,
                before_context: None,
                context_alias: None,
                case_insensitive: Some(false),
                line_numbers: Some(true),
                head_limit: None,
                offset: None,
                multiline: Some(false),
            };
            let result = grep_search_native(
                payload,
                &root,
                "content",
                true,
                Vec::new(),
                GrepWalkLimits {
                    globs: Vec::new(),
                    max_per_file: None,
                    timeout: Duration::from_secs(1),
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
            );
            let _ = send.send(result);
        });

        let result = receive
            .recv_timeout(Duration::from_secs(2))
            .expect("virtual search worker must not block opening a FIFO")
            .unwrap();
        assert!(result.contains("match.txt:1:needle"), "{result}");
    }

    #[test]
    fn native_grep_context_uses_ripgrep_separators() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "before\nmatch\nafter\n").unwrap();
        let mut payload = grep_payload("match");
        payload.context_alias = Some(1);
        payload.output_mode = Some("content".to_string());

        let native = grep_search_native(
            payload.clone(),
            dir.path(),
            "content",
            true,
            Vec::new(),
            GrepWalkLimits {
                globs: Vec::new(),
                max_per_file: None,
                timeout: GREP_TIMEOUT,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        )
        .unwrap();
        assert_eq!(native, "a.txt-1-before\na.txt:2:match\na.txt-3-after");

        let cairn = grep_search(payload, dir.path(), "content", true, Vec::new()).unwrap();
        assert_eq!(cairn, "a.txt:1-before\na.txt:2:match\na.txt:3-after");
    }

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
    fn render_tree_grep_preserves_context_glob_binary_and_slicing() {
        let files = vec![
            (
                "src/a.rs".to_string(),
                b"alpha\ncontext\nNEEDLE\nafter\nNEEDLE again\n".to_vec(),
            ),
            ("src/skip.txt".to_string(), b"NEEDLE\n".to_vec()),
            ("src/binary.rs".to_string(), b"NEEDLE\0ignored\n".to_vec()),
        ];
        let mut payload = grep_payload("NEEDLE");
        payload.output_mode = Some("content".to_string());
        payload.context_alias = Some(1);
        payload.glob = Some("*.rs".to_string());
        payload.head_limit = Some(3);
        let output = render_tree_grep(&files, &payload);
        assert!(output.contains("src/a.rs:3:NEEDLE"), "{output}");
        assert!(output.contains("src/a.rs:2-context"), "{output}");
        assert!(output.contains("src/a.rs:4-after"), "{output}");
        assert!(!output.contains("skip.txt"), "{output}");
        assert!(!output.contains("binary.rs"), "{output}");
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
