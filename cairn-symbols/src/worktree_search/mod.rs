//! Per-worktree warm search index (CAIRN-2303).
//!
//! Cairn's `?grep=` reads and the `grep` tool re-walk the worktree from scratch
//! on every call using ripgrep's libraries. Agents issue dozens of greps per
//! session against the same tree, so this module keeps a resident
//! [`fff_search::FilePicker`] per worktree — a background-indexed, filesystem-
//! watched file list that repeated searches hit warm instead of re-walking.
//!
//! ## What routes here, and what does not
//!
//! This is a *deterministic routing split by target shape*, not two parallel
//! answers to one question. The ripgrep libraries remain the engine for:
//!   - single-file / in-memory greps (the archival byte-parity contract in
//!     `mcp::handlers::search::render_single_file_grep`), which never touch fff;
//!   - the universal fallback whenever the index is cold, the root is not a
//!     known worktree, or the query shape is unsupported (multiline, file-type
//!     walks, sub-directory roots).
//!
//! A cold index therefore costs nothing: the first eligible grep in a worktree
//! falls through to ripgrep while the background scan warms, and the second and
//! later greps hit the resident index.
//!
//! ## Not a ripgrep impersonator
//!
//! The fff path is *not* held to byte-identical parity with the ripgrep walk
//! (that contract is load-bearing only for the single-file/archival path, which
//! stays on `grep-searcher`). What it preserves is the **output format** —
//! `path:N:text` for matches, `path:N-text` for context, `--` group separators,
//! and the `files_with_matches` / `count` shapes — so it slots in behind the
//! existing surface as a drop-in. What it deliberately keeps from fff are its
//! niceties: relevance/definition-aware ordering, smart-case (case-insensitive
//! until the pattern contains an uppercase char) as the default, and the SIMD
//! literal (`PlainText`) path for non-regex patterns. The one thing it will not
//! silently shrink is completeness: fff's per-call `page_limit` / per-file cap
//! are lifted so the index returns every match, exactly as the ripgrep walk
//! does, and `head_limit` stays the sole place results get capped — on the
//! agent's terms.
//!
//! ## Runtime shape
//!
//! Frecency and query-history LMDB stores are disabled (default, uninitialized
//! `SharedFrecency` — no on-disk state). fff never memory-maps a file on
//! cairn's behalf, and that is load-bearing for crash-safety. Two distinct
//! mmap paths exist in fff and both are closed here: the *persistent* mmap
//! cache is off (`enable_mmap_cache: false`), and the *fresh* per-grep mmap —
//! which fff otherwise takes for any file at or above `FRESH_MMAP_THRESHOLD`
//! (256 KiB on Unix, 1 MiB on macOS), independent of that cache flag — is made
//! unreachable by capping `max_file_size` below the smallest threshold (see
//! [`MMAP_SAFE_MAX_FILE_SIZE`]). This matters because a file truncated by
//! another process while it is mapped (jj snapshots, agent writes, worktree
//! teardown) raises SIGBUS on the next page-in — an uncatchable signal that
//! kills the whole runner process (CAIRN-2574). `fff`'s process-wide SIGSEGV
//! handler installs only through `fff::log::init_tracing`, which cairn-core
//! never calls, so embedding the library installs no signal handler. Each
//! instance runs ~2 background threads (scan + watcher) plus a content index
//! (~360 B/file); the pool caps how many live at once and teardown drops them
//! per worktree.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fff_search::{
    has_regex_metacharacters, Constraint, FFFMode, FFFQuery, FilePicker, FilePickerOptions,
    FuzzyQuery, FuzzySearchOptions, GrepMode, GrepSearchOptions, PaginationArgs, SharedFilePicker,
    SharedFrecency,
};

use crate::search_util::{build_glob_matcher, format_grep_line, path_within_any};

/// Owned grep parameters handed to [`WorktreeSearch::try_grep`]. Mirrors the
/// fields of `mcp::handlers::search::GrepPayload` that the index path honors,
/// already resolved (output mode picked, context flags collapsed) by the caller.
pub struct WorktreeGrepParams {
    /// The raw search pattern (a regex, per Cairn's `grep-regex` semantics).
    pub pattern: String,
    /// Optional subdirectory root, relative to the worktree with `/` separators.
    pub subdir: Option<String>,
    /// Optional glob constraints, matched with the same override semantics as the
    /// cold walk after stripping `subdir` from the indexed worktree-relative path.
    pub globs: Vec<String>,
    /// Optional per-file match cap passed through to fff.
    pub max_per_file: Option<usize>,
    /// Resolved output mode: `content`, `files_with_matches`, or `count`.
    pub output_mode: String,
    /// `-i`: `Some(true)` forces case-insensitive, `Some(false)` forces
    /// case-sensitive, `None` uses smart-case (the fff default nicety).
    pub case_insensitive: Option<bool>,
    /// Context lines before each match (`-B` / `-C` / `context`).
    pub before_context: usize,
    /// Context lines after each match (`-A` / `-C` / `context`).
    pub after_context: usize,
    /// Whether to prefix output lines with line numbers (`-n`, default true).
    pub show_line_numbers: bool,
}

/// Upper bound on matches drained from the index in one grep, guarding against a
/// pathological pattern flooding memory. Far above any result set an agent
/// consumes; `head_limit` does the real windowing downstream.
const DRAIN_SAFETY_CAP: usize = 200_000;
/// Per-call match budget handed to fff. Large enough that a normal grep drains
/// in a single page; the drain loop resumes past it only for huge result sets.
const PAGE_LIMIT: usize = 100_000;
/// Per-file match cap. fff defaults to 200; the ripgrep walk has no cap, so we
/// lift it well past any single file's realistic match count to preserve
/// completeness.
const MAX_MATCHES_PER_FILE: usize = 100_000;
/// Wall-clock budget for one fff grep, kept under the caller's 30 s ripgrep
/// timeout so the index path never out-waits the fallback it replaces.
const GREP_TIME_BUDGET_MS: u64 = 25_000;
/// fff truncates `GrepMatch.line_content` (and context lines) to a private
/// `MAX_LINE_DISPLAY_LEN` of 512 bytes; the ripgrep walk emits the full line.
/// After backing up to a UTF-8 char boundary a truncated line is always at
/// least `512 - 3` bytes, so any line at or above this threshold is treated as
/// possibly truncated and the whole grep bails to the ripgrep fallback — which
/// emits full content — so a match's bytes are identical whether the index is
/// warm or cold. Rare false positives (a genuinely 509–512-byte line) just take
/// the correct fallback.
const LINE_TRUNCATION_THRESHOLD: usize = 509;
/// fff `=0.9.6` memory-maps any grepped file at or above `FRESH_MMAP_THRESHOLD`
/// (256 KiB on Unix, 1 MiB on macOS — `fff-core/src/constants.rs`). A file
/// truncated by another process while it is mapped raises SIGBUS on the next
/// page-in, which no in-process handler can survive and which takes down the
/// whole runner (CAIRN-2574). Capping `max_file_size` below the smallest
/// threshold makes the mmap branch unreachable: fff's size prefilter
/// (`file.size <= max_file_size`) rejects oversized files before
/// `get_content_for_search` runs, and every file that does reach it is read
/// into a buffer via `read_exact`, which fails cleanly (→ skip) if the file
/// shrinks mid-read rather than faulting. Pinned to fff `=0.9.6`'s thresholds
/// via the exact-version dependency; see the `Cargo.toml` note.
const MMAP_SAFE_MAX_FILE_SIZE: u64 = 256 * 1024 - 1;

/// One match, copied out of the borrowed [`fff_search::GrepResult`] so the
/// picker read-guard can be released before formatting.
struct OwnedMatch {
    rel: String,
    line_number: u64,
    line_content: String,
    context_before: Vec<String>,
    context_after: Vec<String>,
}

/// How to interpret the pattern for fff, derived from the requested case mode.
/// Extracted as a pure function so the mapping is unit-testable without a live
/// index.
fn plan_query(pattern: &str, case_insensitive: Option<bool>) -> (String, GrepMode, bool) {
    let is_regex = has_regex_metacharacters(pattern);
    match case_insensitive {
        // `-i` on: guarantee case-insensitivity regardless of pattern case by
        // compiling as a regex with the inline `(?i)` flag. Smart-case cannot
        // express "insensitive even though the pattern has uppercase", so we do
        // not rely on it here. Cairn treats grep patterns as regex already, so a
        // no-metacharacter pattern is simply a literal regex — no escaping
        // needed.
        Some(true) => (format!("(?i){pattern}"), GrepMode::Regex, false),
        // `-i` explicitly off: force case-sensitive (smart-case disabled),
        // literal patterns take the fast SIMD `PlainText` path.
        Some(false) => {
            let mode = if is_regex {
                GrepMode::Regex
            } else {
                GrepMode::PlainText
            };
            (pattern.to_string(), mode, false)
        }
        // No `-i`: smart-case (fff's default nicety). Literal patterns still
        // take `PlainText`; case handling is fff's smart-case.
        None => {
            let mode = if is_regex {
                GrepMode::Regex
            } else {
                GrepMode::PlainText
            };
            (pattern.to_string(), mode, true)
        }
    }
}

/// A resident, background-indexed search index for one worktree.
pub struct WorktreeSearch {
    worktree: PathBuf,
    shared: SharedFilePicker,
    // Held only to keep the (uninitialized) frecency handle alive alongside the
    // picker; never queried. Dropping it with the instance releases nothing on
    // disk because it was never `init`-ed.
    _frecency: SharedFrecency,
}

impl WorktreeSearch {
    /// Create an index over `worktree` and spawn its background scan + watcher.
    /// Returns immediately; the scan runs on a background thread and readiness
    /// is checked non-blocking in [`try_grep`](Self::try_grep).
    pub fn new(worktree: &Path) -> Result<Self, String> {
        let shared = SharedFilePicker::default();
        let frecency = SharedFrecency::default();
        FilePicker::new_with_shared_state(
            shared.clone(),
            frecency.clone(),
            FilePickerOptions {
                base_path: worktree.to_string_lossy().into_owned(),
                enable_mmap_cache: false,
                enable_content_indexing: true,
                mode: FFFMode::Ai,
                cache_budget: None,
                watch: true,
                follow_symlinks: false,
                enable_fs_root_scanning: false,
                enable_home_dir_scanning: false,
            },
        )
        .map_err(|e| format!("fff picker init failed for {}: {e:?}", worktree.display()))?;
        Ok(Self {
            worktree: worktree.to_path_buf(),
            shared,
            _frecency: frecency,
        })
    }

    /// Block up to `timeout` for the background scan to finish, returning whether
    /// it completed. Exposed so cairn-core's cross-engine parity tests can wait
    /// for a warm index before comparing its output against the ripgrep walk.
    pub fn wait_for_scan(&self, timeout: Duration) -> bool {
        self.shared.wait_for_scan(timeout)
    }

    /// Run a grep against the warm index.
    ///
    /// Returns `None` when the background scan has not finished yet — the caller
    /// then falls through to the ripgrep walk while the index keeps warming.
    /// `Some(body)` means the index served the query; the body is the joined,
    /// unwindowed output lines (empty when there were no matches). The caller
    /// applies the empty-result message and `offset`/`head_limit` windowing via
    /// the shared `finalize_grep_output`.
    pub fn try_grep(&self, params: &WorktreeGrepParams, deny_read: &[PathBuf]) -> Option<String> {
        // Non-blocking readiness probe: `wait_for_scan(ZERO)` returns true only
        // when scanning has already finished. A cold index → None → fallback.
        if !self.shared.wait_for_scan(Duration::ZERO) {
            return None;
        }

        let guard = self.shared.read().ok()?;
        let picker = guard.as_ref()?;

        // Preserve Cairn's invalid-regex error contract. The ripgrep fallback
        // validates every pattern via `grep_regex` and returns `Invalid regex
        // pattern ...` on a compile failure; fff instead silently falls back to
        // literal matching (surfacing the error only in
        // `GrepResult.regex_fallback_error`), which would make a warm `?grep=[`
        // return literal `[` matches while a cold one errors. Reject an invalid
        // pattern here — with the same engine the fallback uses — so the caller
        // falls through to ripgrep and produces the canonical error.
        if grep_regex::RegexMatcherBuilder::new()
            .build(&params.pattern)
            .is_err()
        {
            return None;
        }

        let (grep_text, mode, smart_case) = plan_query(&params.pattern, params.case_insensitive);

        let subdir = params.subdir.as_deref();
        let search_root = subdir
            .map(|rel| self.worktree.join(rel))
            .unwrap_or_else(|| self.worktree.clone());
        let overrides = build_override_filter(&search_root, &params.globs)?;

        // Crash-proofing (CAIRN-2574): `base_options` caps `max_file_size` below
        // fff's mmap threshold so no grepped file is ever memory-mapped (a file
        // truncated while mapped raises SIGBUS and kills the runner). The flip
        // side is that fff then silently skips any in-scope, non-binary file
        // above that cap — a completeness hole. If such a file exists, bail to
        // the ripgrep fallback (buffered, uncapped, crash-safe) exactly like the
        // `LINE_TRUNCATION_THRESHOLD` bail below, so warm and cold routing return
        // the same matches. The picker read guard is held across the whole query,
        // so the watcher cannot change indexed sizes between this check and the
        // search.
        if has_oversized_in_scope_file(
            picker,
            &self.worktree,
            subdir,
            overrides.as_ref(),
            deny_read,
        ) {
            return None;
        }

        // Constraints reference `grep_text` / `subdir_prefilter`, which outlive
        // the query built below. User glob filters deliberately stay out of fff:
        // the exact cold-walk override matcher is applied after draining results.
        let mut constraints: Vec<Constraint<'_>> = Vec::new();
        let subdir_prefilter = subdir
            .filter(|rel| !contains_glob_metachar(rel))
            .map(|rel| format!("{rel}/**"));
        if let Some(prefilter) = subdir_prefilter.as_deref() {
            constraints.push(Constraint::Glob(prefilter));
        }

        let query = FFFQuery {
            raw_query: &grep_text,
            constraints,
            fuzzy_query: FuzzyQuery::Text(&grep_text),
            location: None,
        };

        let base_options = GrepSearchOptions {
            // Kept below fff's mmap threshold so no grepped file is memory-mapped
            // (see MMAP_SAFE_MAX_FILE_SIZE / the has_oversized_in_scope_file bail).
            max_file_size: MMAP_SAFE_MAX_FILE_SIZE,
            max_matches_per_file: params.max_per_file.unwrap_or(MAX_MATCHES_PER_FILE),
            smart_case,
            page_limit: PAGE_LIMIT,
            mode,
            time_budget_ms: GREP_TIME_BUDGET_MS,
            before_context: params.before_context,
            after_context: params.after_context,
            ..GrepSearchOptions::default()
        };

        // Drain every page so the index returns the complete match set (the
        // ripgrep walk it replaces has no per-call cap). Bounded by the safety
        // cap against a pathological flood.
        let mut collected: Vec<OwnedMatch> = Vec::new();
        let mut file_offset = 0usize;
        loop {
            let options = GrepSearchOptions {
                file_offset,
                ..base_options.clone()
            };
            let result = picker.grep(&query, &options);
            for m in &result.matches {
                let Some(file) = result.files.get(m.file_index) else {
                    continue;
                };
                let rel = file.relative_path(picker);
                let rel_path = PathBuf::from(&rel);
                if !deny_read.is_empty() {
                    let abs = self.worktree.join(&rel_path);
                    if path_within_any(&abs, deny_read) {
                        continue;
                    }
                }
                let Some(stripped_rel) = strip_subdir_prefix(&rel, subdir) else {
                    continue;
                };
                if overrides.as_ref().is_some_and(|filter| {
                    filter.matched(Path::new(stripped_rel), false).is_ignore()
                }) {
                    continue;
                }
                // fff truncates long display lines to 512 bytes; bail the whole
                // grep to the ripgrep fallback so match (and context) bytes stay
                // identical between warm and cold routing rather than silently
                // returning a shortened line.
                if m.line_content.len() >= LINE_TRUNCATION_THRESHOLD
                    || m.context_before
                        .iter()
                        .any(|l| l.len() >= LINE_TRUNCATION_THRESHOLD)
                    || m.context_after
                        .iter()
                        .any(|l| l.len() >= LINE_TRUNCATION_THRESHOLD)
                {
                    return None;
                }
                collected.push(OwnedMatch {
                    rel: stripped_rel.to_string(),
                    line_number: m.line_number,
                    line_content: m.line_content.clone(),
                    context_before: m.context_before.clone(),
                    context_after: m.context_after.clone(),
                });
            }
            if result.next_file_offset == 0 || collected.len() >= DRAIN_SAFETY_CAP {
                break;
            }
            file_offset = result.next_file_offset;
        }

        Some(format_matches(&collected, params))
    }

    /// Run a glob against the warm index.
    ///
    /// Returns worktree-relative paths sorted most-recently-modified first, or
    /// `None` when the index cannot prove completeness (cold scan, pagination
    /// truncation, or lock failure) so the caller can fall back to the canonical
    /// filesystem walk.
    pub fn try_glob(
        &self,
        pattern: &str,
        subdir: Option<&str>,
        deny_read: &[PathBuf],
    ) -> Option<Vec<PathBuf>> {
        // `FilePicker::glob` answers from a partial index while the first scan is
        // still running. That would silently return incomplete results, so use
        // the same non-blocking readiness gate as `try_grep`.
        if !self.shared.wait_for_scan(Duration::ZERO) {
            return None;
        }

        let guard = self.shared.read().ok()?;
        let picker = guard.as_ref()?;
        let matcher = build_glob_matcher(pattern).ok()?;

        // fff evaluates the glob against worktree-relative paths, but the user
        // pattern is relative to `subdir` and the exact `matcher` above is applied
        // to the subdir-stripped path below. Handing the user pattern to fff for a
        // subdirectory target would evaluate e.g. `nested/*.rs` at the worktree
        // root and never surface `src/nested/deep.rs` as a candidate. Instead
        // select a superset of everything under the subdir (`{subdir}/**`, the
        // same prefilter the grep path uses) and let the exact stripped-path
        // matcher govern. A subdir whose own name contains glob metacharacters
        // can't be expressed as a safe prefilter, so fall back to the walk.
        let fff_pattern: String = match subdir {
            None => pattern.to_string(),
            Some(rel) if !contains_glob_metachar(rel) => format!("{rel}/**"),
            Some(_) => return None,
        };

        let first_page = picker.glob(&fff_pattern, FuzzySearchOptions::default());
        let result = if glob_result_is_truncated(first_page.total_matched, first_page.items.len()) {
            let limit = first_page.total_files.saturating_add(1);
            picker.glob(
                &fff_pattern,
                FuzzySearchOptions {
                    pagination: PaginationArgs { offset: 0, limit },
                    ..FuzzySearchOptions::default()
                },
            )
        } else {
            first_page
        };

        if glob_result_is_truncated(result.total_matched, result.items.len()) {
            return None;
        }

        let mut matches: Vec<(PathBuf, u64)> = result
            .items
            .iter()
            .filter_map(|file| {
                let rel = file.relative_path(picker);
                let rel_path = PathBuf::from(&rel);
                if !deny_read.is_empty()
                    && path_within_any(&self.worktree.join(&rel_path), deny_read)
                {
                    return None;
                }
                let stripped_rel = strip_subdir_prefix(&rel, subdir)?;
                let stripped_path = PathBuf::from(stripped_rel);
                if !matcher.is_match(&stripped_path) {
                    return None;
                }
                Some((stripped_path, file.modified))
            })
            .collect();

        matches.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        Some(matches.into_iter().map(|(path, _)| path).collect())
    }
}

fn glob_result_is_truncated(total_matched: usize, items_len: usize) -> bool {
    total_matched > items_len
}

fn build_override_filter(
    root: &Path,
    globs: &[String],
) -> Option<Option<ignore::overrides::Override>> {
    if globs.is_empty() {
        return Some(None);
    }
    let mut builder = ignore::overrides::OverrideBuilder::new(root);
    for glob in globs {
        builder.add(glob).ok()?;
    }
    Some(Some(builder.build().ok()?))
}

fn strip_subdir_prefix<'a>(rel: &'a str, subdir: Option<&str>) -> Option<&'a str> {
    let Some(prefix) = subdir else {
        return Some(rel);
    };
    rel.strip_prefix(prefix)?.strip_prefix('/')
}

fn contains_glob_metachar(path: &str) -> bool {
    path.bytes()
        .any(|b| matches!(b, b'*' | b'?' | b'[' | b']' | b'{' | b'}'))
}

/// Does the warm index hold a non-binary file that is in scope for this grep
/// but larger than [`MMAP_SAFE_MAX_FILE_SIZE`]? The `max_file_size` cap keeps
/// such a file from being memory-mapped (and so from raising SIGBUS on a
/// concurrent truncation), but that same cap makes fff silently skip it — so
/// its presence forces [`WorktreeSearch::try_grep`] to bail to the ripgrep
/// fallback, preserving warm/cold completeness parity.
///
/// Scans both the primary file list and the watcher-discovered overflow list (a
/// file added/removed mid-session lands in overflow). "In scope" mirrors the
/// per-match filtering exactly: the worktree-relative path must survive `subdir`
/// stripping, pass the user glob override filter, and not sit under a
/// `deny_read` fence root. Binaries are excluded because neither engine
/// content-searches them and fff never mmaps them, so a large committed binary
/// (a PNG, say) must not poison every grep in the tree.
fn has_oversized_in_scope_file(
    picker: &FilePicker,
    worktree: &Path,
    subdir: Option<&str>,
    overrides: Option<&ignore::overrides::Override>,
    deny_read: &[PathBuf],
) -> bool {
    picker
        .get_files()
        .iter()
        .chain(picker.get_overflow_files())
        .any(|file| {
            if file.is_binary() || file.size <= MMAP_SAFE_MAX_FILE_SIZE {
                return false;
            }
            let rel = file.relative_path(picker);
            let Some(stripped_rel) = strip_subdir_prefix(&rel, subdir) else {
                return false;
            };
            if overrides
                .is_some_and(|filter| filter.matched(Path::new(stripped_rel), false).is_ignore())
            {
                return false;
            }
            if !deny_read.is_empty() && path_within_any(&worktree.join(&rel), deny_read) {
                return false;
            }
            true
        })
}

/// Format drained matches into the `path:N:text` output contract for the
/// requested output mode. fff returns matches file-grouped and line-ordered
/// (files ranked by relevance), so iterating them preserves coherent per-file
/// blocks.
fn format_matches(matches: &[OwnedMatch], params: &WorktreeGrepParams) -> String {
    match params.output_mode.as_str() {
        "files_with_matches" => {
            let mut seen: Vec<&str> = Vec::new();
            for m in matches {
                if !seen.iter().any(|s| *s == m.rel) {
                    seen.push(&m.rel);
                }
            }
            seen.join("\n")
        }
        "count" => {
            // Per-file counts in first-seen (relevance) order, emitted as
            // `path:count` — the shape the ripgrep walk produces.
            let mut order: Vec<&str> = Vec::new();
            let mut counts: HashMap<&str, usize> = HashMap::new();
            for m in matches {
                let entry = counts.entry(m.rel.as_str()).or_insert(0);
                if *entry == 0 {
                    order.push(&m.rel);
                }
                *entry += 1;
            }
            order
                .into_iter()
                .map(|rel| format!("{}:{}", rel, counts[rel]))
                .collect::<Vec<_>>()
                .join("\n")
        }
        // "content" and any already-validated mode fall here.
        _ => {
            let ctx_requested = params.before_context > 0 || params.after_context > 0;
            let mut lines: Vec<String> = Vec::new();
            let mut previous_context_end: Option<(&str, u64)> = None;
            for m in matches {
                let before_start = m.line_number.saturating_sub(m.context_before.len() as u64);
                // `grep_searcher` emits context breaks inside a file when two
                // match groups are separated. It does not insert a break between
                // different files, so mirror that instead of separating every
                // drained fff match whenever context is requested.
                if ctx_requested
                    && previous_context_end
                        .is_some_and(|(rel, end)| rel == m.rel && before_start > end + 1)
                {
                    lines.push("--".to_string());
                }
                for (j, text) in m.context_before.iter().enumerate() {
                    let ln = params.show_line_numbers.then(|| before_start + j as u64);
                    lines.push(format_grep_line(&m.rel, ln, '-', text));
                }
                let ln = params.show_line_numbers.then_some(m.line_number);
                lines.push(format_grep_line(&m.rel, ln, ':', &m.line_content));
                for (j, text) in m.context_after.iter().enumerate() {
                    let ln = params
                        .show_line_numbers
                        .then(|| m.line_number + 1 + j as u64);
                    lines.push(format_grep_line(&m.rel, ln, '-', text));
                }
                previous_context_end = Some((&m.rel, m.line_number + m.context_after.len() as u64));
            }
            lines.join("\n")
        }
    }
}

/// Bounded, LRU-evicting pool of per-worktree indexes, keyed by worktree path.
///
/// Keyed on `worktree_path` (the same unit as `jobs.worktree_path`), so
/// inherited/shared worktrees naturally share one instance. Get-or-create is
/// lazy; creation spawns the background scan and returns immediately. At
/// capacity the least-recently-used instance is dropped, which flips its
/// picker's cancellation flag and lets its background threads exit.
pub struct WorktreeSearchPool {
    capacity: usize,
    inner: Mutex<PoolInner>,
}

struct PoolInner {
    map: HashMap<PathBuf, Arc<WorktreeSearch>>,
    /// Recency order: front = least-recently-used, back = most-recent.
    recency: Vec<PathBuf>,
}

impl PoolInner {
    fn touch(&mut self, key: &Path) {
        self.recency.retain(|p| p != key);
        self.recency.push(key.to_path_buf());
    }
}

impl WorktreeSearchPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(PoolInner {
                map: HashMap::new(),
                recency: Vec::new(),
            }),
        }
    }

    /// Return the worktree's index, creating and warming it on first use.
    /// Returns `None` if the index could not be created (e.g. the path is gone);
    /// the caller then uses the ripgrep fallback.
    pub fn get_or_create(&self, worktree: &Path) -> Option<Arc<WorktreeSearch>> {
        let key = worktree.to_path_buf();
        let mut inner = self.inner.lock().ok()?;
        if let Some(existing) = inner.map.get(&key).cloned() {
            inner.touch(&key);
            return Some(existing);
        }
        let ws = match WorktreeSearch::new(&key) {
            Ok(ws) => Arc::new(ws),
            Err(e) => {
                log::warn!("worktree_search: {e}");
                return None;
            }
        };
        inner.map.insert(key.clone(), ws.clone());
        inner.touch(&key);
        // Evict the least-recently-used entries down to capacity. The entry just
        // inserted is most-recent (back of `recency`), so it is never evicted.
        while inner.map.len() > self.capacity && !inner.recency.is_empty() {
            let evicted = inner.recency.remove(0);
            inner.map.remove(&evicted);
            log::debug!(
                "worktree_search: evicted index for {} (pool cap {})",
                evicted.display(),
                self.capacity
            );
        }
        Some(ws)
    }

    /// Drop the index for a worktree, if present. Idempotent — safe to call at
    /// teardown for every torn-down worktree whether or not it was ever
    /// indexed. Dropping the last `Arc` lets the picker's background threads
    /// exit.
    pub fn drop_worktree(&self, worktree: &Path) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if inner.map.remove(worktree).is_some() {
            inner.recency.retain(|p| p != worktree);
            log::debug!("worktree_search: dropped index for {}", worktree.display());
        }
    }
}

impl Default for WorktreeSearchPool {
    fn default() -> Self {
        // Small cap mirroring the warm-process GC shape; a handful of concurrent
        // worktrees is the realistic ceiling for one host.
        Self::new(8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_query_literal_default_is_smart_case_plaintext() {
        let (text, mode, smart) = plan_query("needle", None);
        assert_eq!(text, "needle");
        assert_eq!(mode, GrepMode::PlainText);
        assert!(smart, "bare literal grep uses smart-case");
    }

    #[test]
    fn plan_query_regex_default_is_smart_case_regex() {
        let (text, mode, smart) = plan_query("foo.*bar", None);
        assert_eq!(text, "foo.*bar");
        assert_eq!(mode, GrepMode::Regex);
        assert!(smart);
    }

    #[test]
    fn plan_query_case_insensitive_literal_takes_regex_path() {
        // A no-metacharacter literal, forced case-insensitive, takes the regex
        // path with an inline (?i) flag; the literal is a valid literal regex.
        let (text, mode, smart) = plan_query("needle", Some(true));
        assert_eq!(text, "(?i)needle");
        assert_eq!(mode, GrepMode::Regex);
        assert!(!smart, "explicit -i disables smart-case");
    }

    #[test]
    fn plan_query_case_insensitive_regex_keeps_pattern() {
        let (text, mode, _smart) = plan_query("foo.*bar", Some(true));
        assert_eq!(text, "(?i)foo.*bar");
        assert_eq!(mode, GrepMode::Regex);
    }

    #[test]
    fn plan_query_case_sensitive_disables_smart_case() {
        let (text, mode, smart) = plan_query("Needle", Some(false));
        assert_eq!(text, "Needle");
        assert_eq!(mode, GrepMode::PlainText);
        assert!(!smart);
    }

    fn content_params() -> WorktreeGrepParams {
        WorktreeGrepParams {
            pattern: "x".to_string(),
            subdir: None,
            globs: Vec::new(),
            max_per_file: None,
            output_mode: "content".to_string(),
            case_insensitive: None,
            before_context: 0,
            after_context: 0,
            show_line_numbers: true,
        }
    }

    fn m(rel: &str, line: u64, text: &str) -> OwnedMatch {
        OwnedMatch {
            rel: rel.to_string(),
            line_number: line,
            line_content: text.to_string(),
            context_before: Vec::new(),
            context_after: Vec::new(),
        }
    }

    #[test]
    fn format_content_emits_path_line_text() {
        let matches = vec![m("src/a.rs", 10, "let x = 1;"), m("src/b.rs", 3, "x()")];
        let out = format_matches(&matches, &content_params());
        assert_eq!(out, "src/a.rs:10:let x = 1;\nsrc/b.rs:3:x()");
    }

    #[test]
    fn format_content_without_line_numbers() {
        let mut params = content_params();
        params.show_line_numbers = false;
        let out = format_matches(&[m("a.rs", 1, "hit")], &params);
        assert_eq!(out, "a.rs:hit");
    }

    #[test]
    fn format_content_with_context_uses_dash_and_separator() {
        let mut params = content_params();
        params.before_context = 1;
        params.after_context = 1;
        let mut first = m("a.rs", 5, "MATCH1");
        first.context_before = vec!["before4".to_string()];
        first.context_after = vec!["after6".to_string()];
        let mut second = m("a.rs", 20, "MATCH2");
        second.context_before = vec!["before19".to_string()];
        let out = format_matches(&[first, second], &params);
        assert_eq!(
            out,
            "a.rs:4-before4\na.rs:5:MATCH1\na.rs:6-after6\n--\na.rs:19-before19\na.rs:20:MATCH2"
        );
    }

    #[test]
    fn format_files_with_matches_dedups_in_order() {
        let matches = vec![
            m("src/b.rs", 1, "x"),
            m("src/a.rs", 2, "x"),
            m("src/b.rs", 9, "x"),
        ];
        let mut params = content_params();
        params.output_mode = "files_with_matches".to_string();
        let out = format_matches(&matches, &params);
        assert_eq!(out, "src/b.rs\nsrc/a.rs");
    }

    #[test]
    fn format_count_reports_per_file_counts() {
        let matches = vec![
            m("src/b.rs", 1, "x"),
            m("src/b.rs", 9, "x"),
            m("src/a.rs", 2, "x"),
        ];
        let mut params = content_params();
        params.output_mode = "count".to_string();
        let out = format_matches(&matches, &params);
        assert_eq!(out, "src/b.rs:2\nsrc/a.rs:1");
    }

    fn write_file(root: &Path, rel: &str, contents: &str) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    fn git_init(root: &Path) {
        // Worktrees are real git checkouts, so index a git repo to exercise the
        // same gitignore semantics fff sees in production.
        let ok = std::process::Command::new("git")
            .arg("init")
            .current_dir(root)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        assert!(ok, "git init failed in test worktree");
    }

    /// End-to-end: build a real index over a temp git repo and grep it. Asserts
    /// the fff path finds matches across the tree, honors `.gitignore`, emits
    /// the `path:N:text` format, and applies the fence `deny_read` filter.
    #[test]
    fn index_greps_tree_honors_gitignore_and_fence() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "alpha.rs", "fn needle() {}\n");
        write_file(root, "sub/beta.rs", "let needle = 1;\n");
        write_file(root, "ignored.log", "needle here\n");
        write_file(root, ".gitignore", "*.log\n");

        let search = WorktreeSearch::new(root).unwrap();
        assert!(
            search.shared.wait_for_scan(Duration::from_secs(15)),
            "background scan did not complete"
        );

        let params = WorktreeGrepParams {
            pattern: "needle".to_string(),
            subdir: None,
            globs: Vec::new(),
            max_per_file: None,
            output_mode: "files_with_matches".to_string(),
            case_insensitive: None,
            before_context: 0,
            after_context: 0,
            show_line_numbers: true,
        };

        let out = search.try_grep(&params, &[]).expect("index ready → Some");
        let files: std::collections::HashSet<&str> = out.lines().collect();
        assert!(files.contains("alpha.rs"), "root match found: {out:?}");
        assert!(files.contains("sub/beta.rs"), "subdir match found: {out:?}");
        assert!(
            !out.contains("ignored.log"),
            "gitignored file excluded: {out:?}"
        );

        // content mode carries `path:N:text`.
        let content = search
            .try_grep(
                &WorktreeGrepParams {
                    output_mode: "content".to_string(),
                    ..params_clone(&params)
                },
                &[],
            )
            .unwrap();
        assert!(
            content.contains("alpha.rs:1:fn needle() {}"),
            "content format: {content:?}"
        );

        // Fence: denying `sub/` drops sub/beta.rs from results.
        let fenced = search.try_grep(&params, &[root.join("sub")]).unwrap();
        let fenced_files: std::collections::HashSet<&str> = fenced.lines().collect();
        assert!(fenced_files.contains("alpha.rs"));
        assert!(
            !fenced_files.contains("sub/beta.rs"),
            "deny_read path excluded from fff output: {fenced:?}"
        );
    }

    /// The two review fixes for cold-vs-warm divergence: an invalid regex bails
    /// to the ripgrep fallback (`None`) instead of silently literal-matching,
    /// and a match on a line past fff's 512-byte display cap bails too so match
    /// content is never silently truncated.
    #[test]
    fn index_bails_on_invalid_regex_and_long_lines() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "short.rs", "let needle = 1;\n");
        // A single line well past fff's 512-byte display cap containing the
        // needle: the warm index would truncate its content, so the grep must
        // bail to the ripgrep fallback rather than return a shortened line.
        let long_line = format!("let needle = \"{}\";\n", "a".repeat(700));
        write_file(root, "long.rs", &long_line);

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        // Invalid regex → None so the caller falls through to ripgrep for the
        // canonical "Invalid regex pattern" error.
        let bad = WorktreeGrepParams {
            pattern: "[".to_string(),
            ..content_params()
        };
        assert!(
            search.try_grep(&bad, &[]).is_none(),
            "invalid regex bails to fallback"
        );

        // A grep whose match set includes an over-cap line bails entirely.
        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        assert!(
            search.try_grep(&needle, &[]).is_none(),
            "a match on an over-cap line bails to the fallback"
        );
    }

    /// A non-binary file larger than the mmap-safe cap forces the whole grep to
    /// bail to the ripgrep fallback (`None`) so fff never memory-maps it — the
    /// SIGBUS crash-proofing (CAIRN-2574). Narrowing scope past the big file
    /// with a glob restores warm service.
    #[test]
    fn index_bails_when_oversized_in_scope_file_present() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "small.rs", "let needle = 1;\n");
        // A ~400 KiB ASCII text file (non-binary), well above the cap.
        let big = format!("// needle\n{}\n", "abcd ".repeat(80_000));
        assert!(big.len() as u64 > MMAP_SAFE_MAX_FILE_SIZE);
        write_file(root, "big.rs", &big);

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        // Scope includes big.rs → bail to the fallback.
        assert!(
            search.try_grep(&needle, &[]).is_none(),
            "a large in-scope non-binary file bails to the ripgrep fallback"
        );

        // A glob that excludes big.rs takes it out of scope → warm service.
        let narrowed = WorktreeGrepParams {
            pattern: "needle".to_string(),
            globs: vec!["small.rs".to_string()],
            ..content_params()
        };
        let out = search
            .try_grep(&narrowed, &[])
            .expect("grep scoped past the big file is served warm");
        assert!(out.contains("small.rs:1:let needle = 1;"), "{out:?}");
    }

    /// A large *binary* file must not trigger the oversized bail: neither engine
    /// content-searches binaries and fff never mmaps them, so an ordinary grep
    /// alongside a big binary blob is still served warm.
    #[test]
    fn index_oversized_binary_file_does_not_bail() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "small.rs", "let needle = 1;\n");
        // ~300 KiB of NUL bytes → classified binary by fff during the scan.
        std::fs::write(root.join("blob.bin"), vec![0u8; 300 * 1024]).unwrap();

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        let out = search
            .try_grep(&needle, &[])
            .expect("a big binary blob does not poison the warm grep");
        assert!(out.contains("small.rs:1:let needle = 1;"), "{out:?}");
    }

    /// The cap must stay below fff `=0.9.6`'s smallest `FRESH_MMAP_THRESHOLD`
    /// (256 KiB on Unix; macOS is 1 MiB). If a future fff bump lowers that
    /// threshold, this assert and the exact-version pin in `Cargo.toml` must
    /// move together — the crash-safety guarantee rests on the version pin.
    #[test]
    fn mmap_safe_cap_is_below_fff_threshold() {
        // Compile-time guard: the cap must stay below fff's smallest threshold.
        const { assert!(MMAP_SAFE_MAX_FILE_SIZE < 256 * 1024) };
    }

    fn params_clone(p: &WorktreeGrepParams) -> WorktreeGrepParams {
        WorktreeGrepParams {
            pattern: p.pattern.clone(),
            subdir: p.subdir.clone(),
            globs: p.globs.clone(),
            max_per_file: p.max_per_file,
            output_mode: p.output_mode.clone(),
            case_insensitive: p.case_insensitive,
            before_context: p.before_context,
            after_context: p.after_context,
            show_line_numbers: p.show_line_numbers,
        }
    }

    fn set_mtime(root: &Path, rel: &str, seconds: i64) {
        let time = filetime::FileTime::from_unix_time(seconds, 0);
        filetime::set_file_mtime(root.join(rel), time).unwrap();
    }

    #[test]
    fn index_glob_orders_by_mtime_descending_with_path_tiebreak() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "newest.rs", "\n");
        write_file(root, "same-a.rs", "\n");
        write_file(root, "same-b.rs", "\n");
        write_file(root, "oldest.rs", "\n");
        set_mtime(root, "oldest.rs", 1_700_000_000);
        set_mtime(root, "same-b.rs", 1_700_000_010);
        set_mtime(root, "same-a.rs", 1_700_000_010);
        set_mtime(root, "newest.rs", 1_700_000_020);

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        assert_eq!(
            search.try_glob("*.rs", None, &[]).unwrap(),
            vec![
                PathBuf::from("newest.rs"),
                PathBuf::from("same-a.rs"),
                PathBuf::from("same-b.rs"),
                PathBuf::from("oldest.rs"),
            ]
        );
    }

    #[test]
    fn index_glob_honors_fence_and_cold_index_bails() {
        let cold = WorktreeSearch {
            worktree: PathBuf::from("/tmp/cold-worktree"),
            shared: SharedFilePicker::default(),
            _frecency: SharedFrecency::default(),
        };
        assert!(cold.try_glob("*.rs", None, &[]).is_none());

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "visible.rs", "\n");
        write_file(root, "denied/hidden.rs", "\n");

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        assert_eq!(
            search
                .try_glob("*.rs", None, &[root.join("denied")])
                .unwrap(),
            vec![PathBuf::from("visible.rs")]
        );
    }

    #[test]
    fn glob_truncation_detector_bails_when_page_is_short() {
        assert!(!glob_result_is_truncated(2, 2));
        assert!(glob_result_is_truncated(3, 2));
    }

    #[test]
    fn pool_evicts_least_recently_used() {
        // Non-existent paths make WorktreeSearch::new fail, so exercise the LRU
        // bookkeeping directly against PoolInner instead.
        let mut inner = PoolInner {
            map: HashMap::new(),
            recency: Vec::new(),
        };
        inner.touch(Path::new("/a"));
        inner.touch(Path::new("/b"));
        inner.touch(Path::new("/a")); // /a now most-recent
        assert_eq!(
            inner.recency,
            vec![PathBuf::from("/b"), PathBuf::from("/a")],
            "touch moves the key to most-recent"
        );
    }
}
