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

/// Grep an in-memory file set (a branch's object tree or an overlay), rendering
/// the same output the filesystem walker renders.
///
/// `Err` is a failure of the search itself — an unusable pattern, mode, glob, or
/// file type. It is never a body: a caller that folded it into one would count
/// the failure text as grep output and head it with a count, presenting an error
/// as a result.
pub fn render_tree_grep(
    files: &[(String, Vec<u8>)],
    payload: &GrepPayload,
) -> Result<String, String> {
    let output_mode = payload
        .output_mode
        .as_deref()
        .unwrap_or("files_with_matches");
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return Err(format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        ));
    }
    let matcher = build_grep_matcher(payload)?;
    let glob = match payload.glob.as_deref() {
        Some(pattern) => Some(cairn_symbols::search_util::build_glob_matcher(pattern)?),
        None => None,
    };
    let file_types = match payload.file_type.as_deref() {
        Some(file_type) => {
            let mut builder = ignore::types::TypesBuilder::new();
            builder.add_defaults();
            builder.select(file_type);
            match builder.build() {
                Ok(types) => Some(types),
                Err(error) => return Err(format!("Invalid file type '{}': {}", file_type, error)),
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
        grep_collect_into(
            &mut searcher,
            &matcher,
            path,
            GrepSource::Bytes(bytes),
            output_mode,
            None,
            &mut collected,
        )?;
    }
    let (before, after) = grep_context_window(payload);

    Ok(finalize_grep_output(
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
    ))
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

/// Default grep output mode for a target that has already been classified as
/// one file or a tree. Grepping a single file should print matching lines (you
/// already named the file — echoing it back is useless and dead-ends agents
/// into shelling out to grep), while grepping a tree lists which files matched.
///
/// The classification is the caller's: a filesystem walk stats the path, and a
/// logical project read classifies against the branch's object tree. Taking the
/// decision rather than a path keeps a target that has no host path — every
/// logical `file:` read — from being misclassified as a tree by a `stat` that
/// was never going to find it.
fn default_grep_output_mode(single_file: bool) -> &'static str {
    if single_file {
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
pub(crate) fn resolve_grep_output_mode(
    explicit: Option<&str>,
    requested_context: bool,
    single_file: bool,
) -> &str {
    explicit.unwrap_or_else(|| {
        if requested_context {
            "content"
        } else {
            default_grep_output_mode(single_file)
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

    build_glob_matcher(&payload.pattern)?;

    let deny_read = orch.sandbox_deny_read();

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

    matches.sort_by_key(|entry| std::cmp::Reverse(entry.1));
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

/// The body every glob entry point renders when a pattern matched nothing.
///
/// Shared so the live filesystem walk and the branch/overlay projection spell an
/// absence identically — one message with one meaning, rather than two spellings
/// neither of which a reader downstream can recognize.
pub(crate) fn glob_no_matches_body(pattern: &str, location: impl std::fmt::Display) -> String {
    format!("No files matched pattern '{pattern}' in {location}")
}

/// Handle grep tool call — in-process ripgrep search over the host filesystem.
///
/// `Err` is a failed search (unusable payload/pattern/mode, a timeout, a worker
/// failure), never a body — see [`render_tree_grep`].
pub(crate) async fn handle_grep(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<String, String> {
    let payload: GrepPayload = super::parse_payload(request)?;

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
        search_path.is_file(),
    );
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return Err(format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        ));
    }

    let output_mode = output_mode.to_string();
    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let offset = payload.offset.unwrap_or(0) as usize;
    let head_limit = payload.head_limit;
    let pattern = payload.pattern.clone();
    let deny_read = orch.sandbox_deny_read();

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
        Ok(Ok(Ok(output))) => Ok(finalize_grep_output(output, &pattern, offset, head_limit)),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(e)) => Err(format!("grep task failed: {}", e)),
        Err(_) => Err(format!("grep timed out after {:?}", GREP_TIMEOUT)),
    }
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

/// The body every grep entry point renders when a search produced nothing.
///
/// Exported so the header-count projection can recognize an empty result in any
/// output mode: a `files_with_matches` body is otherwise one line per matched
/// file, and this message would count as a file.
pub(crate) fn grep_no_matches_body(pattern: &str) -> String {
    format!("No matches found for pattern '{}'", pattern)
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
        return grep_no_matches_body(pattern);
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
///
/// `Err` is a failed search, never a body — see [`render_tree_grep`].
pub(crate) fn render_single_file_grep(
    bytes: &[u8],
    label: &str,
    payload: &GrepPayload,
) -> Result<String, String> {
    let output_mode = payload.output_mode.as_deref().unwrap_or("content");
    if !matches!(output_mode, "files_with_matches" | "count" | "content") {
        return Err(format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        ));
    }

    let show_line_numbers = payload.line_numbers.unwrap_or(true);
    let offset = payload.offset.unwrap_or(0) as usize;
    let head_limit = payload.head_limit;

    let matcher = build_grep_matcher(payload)?;
    let mut searcher = build_grep_searcher(payload);

    let mut collected: Vec<GrepLine> = Vec::new();
    grep_collect_into(
        &mut searcher,
        &matcher,
        label,
        GrepSource::Bytes(bytes),
        output_mode,
        None,
        &mut collected,
    )?;

    let (before, after) = grep_context_window(payload);
    Ok(finalize_grep_output(
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
    ))
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
pub(crate) fn grep_materialized_body(
    body: &str,
    payload: &GrepPayload,
) -> Result<(String, usize), String> {
    let rendered = render_single_file_grep(body.as_bytes(), "", payload)?;
    let (matches, _files) = crate::mcp::handlers::read::grep_counts(&rendered);
    Ok((rendered, matches))
}

/// Engine-level bounds for one filesystem grep walk that the agent-facing
/// [`GrepPayload`] cannot express: the ripgrep-style glob list (repeated
/// `--glob`, `!` negations, the synthetic filename glob an explicit file target
/// produces), rg's `-m N` per-file match cap, and the deadline plus shared
/// cancel flag an outer timeout uses to stop a walk that is already inside one
/// large file.
#[derive(Clone)]
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

/// The glob override set a search applies to candidate paths. Both content
/// sources build it here so `--glob` means exactly the same thing whether the
/// bytes came off the filesystem or out of the object store. `root` is what
/// candidate paths are matched relative to: the walk root for a filesystem
/// walk, the empty path for store entries, which are already repo-relative.
fn build_walk_overrides(
    root: &Path,
    globs: &[String],
) -> Result<Option<ignore::overrides::Override>, String> {
    if globs.is_empty() {
        return Ok(None);
    }
    let mut overrides = ignore::overrides::OverrideBuilder::new(root);
    for glob in globs {
        overrides
            .add(glob)
            .map_err(|error| format!("Invalid glob '{}': {}", glob, error))?;
    }
    overrides
        .build()
        .map(Some)
        .map_err(|error| format!("Failed to build glob override: {}", error))
}

/// The `-t`/`--type` filter, shared by both content sources for the same reason
/// [`build_walk_overrides`] is.
fn build_walk_types(file_type: Option<&str>) -> Result<Option<ignore::types::Types>, String> {
    let Some(file_type) = file_type else {
        return Ok(None);
    };
    let mut types = ignore::types::TypesBuilder::new();
    types.add_defaults();
    types
        .select(file_type)
        .build()
        .map(Some)
        .map_err(|error| format!("Invalid file type '{}': {}", file_type, error))
}

/// Whether one candidate path survives the glob and file-type filters. The
/// filesystem walk gets this from `ignore`'s walker; entries handed over as
/// bytes never touch a walker, so they are filtered against the same matchers
/// here rather than against a second, drifting interpretation of the flags.
fn entry_admitted(
    path: &Path,
    overrides: Option<&ignore::overrides::Override>,
    types: Option<&ignore::types::Types>,
) -> bool {
    if overrides.is_some_and(|overrides| overrides.matched(path, false).is_ignore()) {
        return false;
    }
    !types.is_some_and(|types| !types.matched(path, false).is_whitelist())
}

/// The native (ripgrep-shaped) render of a grep over content supplied as bytes
/// instead of read from a filesystem walk.
///
/// This is the store-native content source behind an intercepted `run` search:
/// the project overlay hands over `(repo-relative path, bytes)` at the job's
/// head coordinate, and from there the matcher, the per-file collector, and the
/// renderer are the ones [`grep_search_with_format`] uses. Only where the files
/// come from differs, which is what lets the two agree byte for byte.
///
/// [`render_grep_lines`] sorts by path, so the order entries arrive in cannot
/// affect the output.
pub(crate) fn grep_search_native_entries(
    payload: &GrepPayload,
    files: &[(String, Vec<u8>)],
    output_mode: &str,
    show_line_numbers: bool,
    limits: &GrepWalkLimits,
) -> Result<String, String> {
    let matcher = build_grep_matcher(payload)?;
    let overrides = build_walk_overrides(Path::new(""), &limits.globs)?;
    let types = build_walk_types(payload.file_type.as_deref())?;
    let (before, after) = grep_context_window(payload);
    let cap = GrepCap::new(limits.max_per_file, after);
    let deadline = Instant::now() + limits.timeout;

    let mut collected: Vec<GrepLine> = Vec::new();
    for (relative, bytes) in files {
        if limits.cancelled.load(Ordering::Relaxed) || Instant::now() >= deadline {
            return Err(format!("grep timed out after {:?}", limits.timeout));
        }
        if !entry_admitted(Path::new(relative), overrides.as_ref(), types.as_ref()) {
            continue;
        }
        let mut searcher = build_grep_searcher(payload);
        grep_collect_into(
            &mut searcher,
            &matcher,
            relative,
            GrepSource::Bytes(bytes),
            output_mode,
            cap,
            &mut collected,
        )?;
    }

    Ok(render_grep_lines(
        collected,
        output_mode,
        show_line_numbers,
        true,
        before > 0 || after > 0,
    ))
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

    if let Some(types) = build_walk_types(payload.file_type.as_deref())? {
        walker_builder.types(types);
    }

    if let Some(overrides) = build_walk_overrides(walk_root, &limits.globs)? {
        walker_builder.overrides(overrides);
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
    fn grep_default_mode_is_content_for_a_single_file() {
        assert_eq!(default_grep_output_mode(true), "content");
    }

    #[test]
    fn grep_default_mode_is_files_with_matches_for_a_tree() {
        assert_eq!(default_grep_output_mode(false), "files_with_matches");
    }

    #[test]
    fn filesystem_grep_classifies_by_stat_at_the_call_site() {
        // `handle_grep` walks the host filesystem, so it is the caller that
        // stats — this is the composition it performs. A real file prints its
        // matching lines; a path that is not a file there (missing, or a
        // directory) falls back to the tree default. A logical project read
        // never reaches this stat: it classifies against the branch's object
        // tree, where its bare repository-relative path would never be found.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("needle.txt");
        std::fs::write(&file, "hay\nneedle\nhay\n").unwrap();
        let missing = Path::new("/this/path/does/not/exist/anywhere");

        assert_eq!(
            resolve_grep_output_mode(None, false, file.is_file()),
            "content"
        );
        assert_eq!(
            resolve_grep_output_mode(None, false, missing.is_file()),
            "files_with_matches"
        );
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
        let (rendered, matches) = grep_materialized_body(body, &payload).unwrap();
        assert_eq!(matches, 2);
        // Path-less, line-number-prefixed match lines.
        assert!(rendered.contains("2:needle one"), "{rendered}");
        assert!(rendered.contains("4:needle two"), "{rendered}");
        assert!(!rendered.contains(":2:"), "no path prefix: {rendered}");

        // An empty result yields the shared finalizer's message.
        let payload = body_grep_payload(&[qp("grep", "zzz")], false)
            .unwrap()
            .unwrap();
        let (rendered, matches) = grep_materialized_body(body, &payload).unwrap();
        assert_eq!(matches, 0);
        assert_eq!(rendered, "No matches found for pattern 'zzz'");

        // An unusable pattern is a failed search, not a body: it must never
        // reach a caller as text that a header would then count.
        let mut invalid = payload.clone();
        invalid.pattern = "(unclosed".to_string();
        let error = grep_materialized_body(body, &invalid).unwrap_err();
        assert!(error.contains("Invalid regex pattern"), "{error}");
    }

    #[test]
    fn grep_context_flag_implies_content_for_a_directory() {
        // A directory grep that asks for context but names no output_mode must
        // default to `content`, not `files_with_matches` — otherwise both the
        // requested context and the matched lines are silently dropped.
        assert_eq!(resolve_grep_output_mode(None, true, false), "content");
    }

    #[test]
    fn grep_directory_without_context_stays_files_with_matches() {
        assert_eq!(
            resolve_grep_output_mode(None, false, false),
            "files_with_matches"
        );
    }

    #[test]
    fn grep_explicit_output_mode_wins_over_context_flag() {
        // An explicit output_mode is honored even when context flags are set.
        assert_eq!(
            resolve_grep_output_mode(Some("files_with_matches"), true, false),
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

        let mode = resolve_grep_output_mode(None, true, false);
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
    fn fs_single_file_grep(
        content: &str,
        label: &str,
        payload: &GrepPayload,
    ) -> Result<String, String> {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(label);
        std::fs::write(&file, content).unwrap();

        let requested_context = payload.context.is_some()
            || payload.context_alias.is_some()
            || payload.after_context.is_some()
            || payload.before_context.is_some();
        let mode = resolve_grep_output_mode(
            payload.output_mode.as_deref(),
            requested_context,
            file.is_file(),
        )
        .to_string();
        let show = payload.line_numbers.unwrap_or(true);
        let offset = payload.offset.unwrap_or(0) as usize;
        let raw = grep_search(payload.clone(), &file, &mode, show, Vec::new())?;
        Ok(finalize_grep_output(
            raw,
            &payload.pattern,
            offset,
            payload.head_limit,
        ))
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
        let output = render_tree_grep(&files, &payload).unwrap();
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

    /// The fixture the two content sources and real ripgrep all search.
    fn parity_fixture() -> Vec<(&'static str, &'static str)> {
        vec![
            ("alpha.rs", "fn one() {}\nlet needle = 1;\nfn two() {}\n"),
            (
                "src/beta.rs",
                "use std;\n// NEEDLE here\nlet x = 2;\nneedle again\n",
            ),
            ("src/deep/gamma.txt", "nothing\nneedle in text\ntail\n"),
            ("delta.md", "no hits at all\n"),
        ]
    }

    fn write_fixture(dir: &std::path::Path, files: &[(&str, &str)]) {
        for (path, content) in files {
            let full = dir.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(&full, content).unwrap();
        }
    }

    fn parity_limits(globs: &[&str], max_per_file: Option<usize>) -> GrepWalkLimits {
        GrepWalkLimits {
            globs: globs.iter().map(|glob| (*glob).to_string()).collect(),
            max_per_file,
            timeout: Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The store-native content source must agree with the filesystem walk on
    /// every shape. An agent receives whichever one the router picked and must
    /// not be able to tell which answered.
    fn assert_entries_match_walk(
        payload: &GrepPayload,
        output_mode: &str,
        globs: &[&str],
        max_per_file: Option<usize>,
    ) {
        let files = parity_fixture();
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path(), &files);
        let show_line_numbers = payload.line_numbers.unwrap_or(true);

        let walked = grep_search_native(
            payload.clone(),
            dir.path(),
            output_mode,
            show_line_numbers,
            Vec::new(),
            parity_limits(globs, max_per_file),
        )
        .expect("filesystem walk");

        let entries: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(path, content)| ((*path).to_string(), content.as_bytes().to_vec()))
            .collect();
        let served = grep_search_native_entries(
            payload,
            &entries,
            output_mode,
            show_line_numbers,
            &parity_limits(globs, max_per_file),
        )
        .expect("store-native grep");

        assert_eq!(
            served, walked,
            "store-native grep diverged from the filesystem walk \
             (mode={output_mode}, globs={globs:?}, max_per_file={max_per_file:?})"
        );
    }

    #[test]
    fn store_native_grep_matches_the_walk_across_flag_shapes() {
        let base = grep_payload("needle");

        // Plain content with line numbers: `rg -n needle`.
        assert_entries_match_walk(&base, "content", &[], None);
        // `-l` and `-c` projections.
        assert_entries_match_walk(&base, "files_with_matches", &[], None);
        assert_entries_match_walk(&base, "count", &[], None);
        // `rg needle` without line numbers.
        let mut no_numbers = base.clone();
        no_numbers.line_numbers = Some(false);
        assert_entries_match_walk(&no_numbers, "content", &[], None);
        // `-i`, which must pick up the uppercase NEEDLE.
        let mut insensitive = base.clone();
        insensitive.case_insensitive = Some(true);
        assert_entries_match_walk(&insensitive, "content", &[], None);
        assert_entries_match_walk(&insensitive, "files_with_matches", &[], None);
        // `-A`, `-B`, and `-C`, including the `--` group separators.
        let mut after = base.clone();
        after.after_context = Some(1);
        assert_entries_match_walk(&after, "content", &[], None);
        let mut before = base.clone();
        before.before_context = Some(1);
        assert_entries_match_walk(&before, "content", &[], None);
        let mut around = base.clone();
        around.context = Some(1);
        assert_entries_match_walk(&around, "content", &[], None);
        // `--glob`, positive and negated.
        assert_entries_match_walk(&base, "content", &["*.rs"], None);
        assert_entries_match_walk(&base, "content", &["!*.rs"], None);
        assert_entries_match_walk(&base, "content", &["**/*.txt"], None);
        assert_entries_match_walk(&base, "files_with_matches", &["*.rs", "*.txt"], None);
        // Slash-bearing globs are anchored at the search root, and store entries
        // are matched against an empty root rather than an absolute walk root.
        assert_entries_match_walk(&base, "files_with_matches", &["src/*.rs"], None);
        assert_entries_match_walk(&base, "files_with_matches", &["src/**"], None);
        assert_entries_match_walk(&base, "files_with_matches", &["!src/**"], None);
        assert_entries_match_walk(&base, "content", &["*.rs", "!src/**"], None);
        // `-m`, whose after-context window has its own rendering rule.
        assert_entries_match_walk(&base, "content", &[], Some(1));
        assert_entries_match_walk(&after, "content", &[], Some(1));
        assert_entries_match_walk(&base, "count", &[], Some(1));
        // A pattern nothing matches: both sources must render empty, which is
        // what the caller turns into ripgrep's exit 1.
        assert_entries_match_walk(&grep_payload("absent_pattern"), "content", &[], None);
    }

    /// The anchor for the whole contract: what the walk renders is what real
    /// ripgrep renders. The test above chains the store-native source to this
    /// one, so together they pin served output to the genuine article.
    #[test]
    fn native_grep_output_matches_real_ripgrep() {
        let files = parity_fixture();
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path(), &files);

        let Ok(real) = std::process::Command::new("rg")
            .args(["-n", "--no-heading", "--color", "never", "needle", "."])
            .current_dir(dir.path())
            .output()
        else {
            eprintln!("skipping ripgrep parity: rg is not installed");
            return;
        };

        // ripgrep prints `./`-prefixed paths for an explicit `.` root and does
        // not order its output; the renderer sorts by path. Normalize both.
        let mut expected: Vec<String> = String::from_utf8_lossy(&real.stdout)
            .lines()
            .map(|line| line.trim_start_matches("./").to_string())
            .collect();
        expected.sort();

        let payload = grep_payload("needle");
        let served = grep_search_native_entries(
            &payload,
            &files
                .iter()
                .map(|(path, content)| ((*path).to_string(), content.as_bytes().to_vec()))
                .collect::<Vec<_>>(),
            "content",
            true,
            &parity_limits(&[], None),
        )
        .expect("store-native grep");
        let mut actual: Vec<String> = served.lines().map(str::to_string).collect();
        actual.sort();

        assert!(
            !expected.is_empty(),
            "ripgrep found nothing; the fixture no longer exercises the comparison"
        );
        assert_eq!(
            actual, expected,
            "store-native grep is not byte-faithful to real ripgrep"
        );
    }

    #[test]
    fn render_single_file_grep_no_matches_reports_no_matches() {
        let payload = grep_payload("absent");
        let rendered = render_single_file_grep(b"alpha\nbeta\n", "f.txt", &payload).unwrap();
        assert_eq!(rendered, "No matches found for pattern 'absent'");
        assert_eq!(
            rendered,
            fs_single_file_grep("alpha\nbeta\n", "f.txt", &payload).unwrap()
        );
    }
}
