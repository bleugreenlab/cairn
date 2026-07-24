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
//!     known worktree, or the query shape is unsupported (including multiline
//!     and file-type walks);
//!   - the *residue* of a partially covered query: individual files this index
//!     cannot search are named in [`WorktreeGrepOutcome::uncovered`] and the
//!     caller greps just those, merging their lines before rendering.
//!
//! A cold index therefore costs nothing: the first eligible grep in a worktree
//! falls through to ripgrep while the background scan warms, and the second and
//! later greps hit the resident index.
//!
//! ## Output parity
//!
//! This module does not render output. It drains matches out of fff, expands
//! each one's context window into the shared
//! [`crate::search_util::GrepLine`] model, and hands the result to
//! [`crate::search_util::render_grep_lines`] — the same renderer cairn-core's
//! ripgrep walk uses. Match/context separators, `--` group breaks, path-then-
//! line ordering, and the `files_with_matches` / `count` projections are
//! therefore defined in exactly one place, so warm and cold routing cannot
//! drift into disagreeing about them.
//!
//! What this module keeps from fff are its engine niceties: smart-case
//! (case-insensitive until the pattern contains an uppercase char) as the
//! default, and the SIMD literal (`PlainText`) path for non-regex patterns.
//! What it will not silently shrink is completeness: fff's per-call
//! `page_limit` / per-file cap are lifted so the index returns every match,
//! exactly as the ripgrep walk does, and `head_limit` stays the sole place
//! results get capped — on the agent's terms.
//!
//! ## Runtime shape
//!
//! Frecency and query-history LMDB stores are disabled (default, uninitialized
//! `SharedFrecency` — no on-disk state). fff never memory-maps a file on
//! Cairn's behalf, and that is load-bearing for crash-safety. Three distinct
//! mmap paths exist in fff and all are closed here: persistent-cache *warmup*
//! is disabled with `enable_mmap_cache: false`; lazy persistent-cache creation
//! from `get_cached_content` is denied by [`content_cache_budget`]'s zero entry
//! budget; and the *fresh* per-grep mmap — which fff otherwise takes for any
//! file at or above `FRESH_MMAP_THRESHOLD` (256 KiB on Unix, 1 MiB on macOS),
//! independent of the cache flag — is made unreachable by capping
//! `max_file_size` below the smallest threshold (see
//! [`MMAP_SAFE_MAX_FILE_SIZE`]). This matters because a file truncated by
//! another process while it is mapped (jj snapshots, agent writes, worktree
//! teardown) raises SIGBUS on the next page-in — an uncatchable signal that
//! kills the whole runner process (CAIRN-2574, CAIRN-3074). `fff`'s process-wide
//! SIGSEGV handler installs only through `fff::log::init_tracing`, which cairn-core
//! never calls, so embedding the library installs no signal handler. Each
//! instance runs ~2 background threads (scan + watcher) plus a content index
//! (~360 B/file); the pool caps how many live at once and teardown drops them
//! per worktree.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fff_search::{
    has_regex_metacharacters, Constraint, ContentCacheBudget, FFFMode, FFFQuery, FilePicker,
    FilePickerOptions, FuzzyQuery, FuzzySearchOptions, GrepMode, GrepSearchOptions, PaginationArgs,
    SharedFilePicker, SharedFrecency,
};

use crate::search_util::{build_glob_matcher, path_within_any, GrepLine};

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
/// fff `=0.9.6` has two grep-time mmap paths in addition to cache warmup. Its
/// `get_cached_content` can lazily create a persistent mmap at `MMAP_THRESHOLD`
/// (16 KiB on aarch64) before considering grep options; [`content_cache_budget`]
/// closes that path. It can also create a fresh mmap at or above
/// `FRESH_MMAP_THRESHOLD` (256 KiB on Unix, 1 MiB on macOS —
/// `fff-core/src/constants.rs`). Capping `max_file_size` below the smallest
/// fresh threshold makes that branch unreachable: fff's size prefilter rejects
/// oversized files, and every admitted uncached file is read into a buffer via
/// `read_exact`, which fails cleanly if the file shrinks mid-read rather than
/// faulting. A mapped file that is concurrently truncated can instead raise an
/// uncatchable SIGBUS and kill the runner (CAIRN-2574, CAIRN-3074). These
/// assumptions are pinned to fff `=0.9.6`; see the `Cargo.toml` note.
const MMAP_SAFE_MAX_FILE_SIZE: u64 = 256 * 1024 - 1;

const CONTENT_CACHE_MAX_FILES: usize = 0;
const CONTENT_CACHE_MAX_BYTES: u64 = 0;

/// Deny lazy persistent-cache entries while preserving the buffered-read size
/// ceiling. This must be a literal budget rather than `from_overrides`: zeros
/// passed to that constructor inherit fff's generous repository defaults.
fn content_cache_budget() -> ContentCacheBudget {
    ContentCacheBudget {
        // fff checks `cached_count >= max_files` before creating an entry, so
        // 0/0 makes persistent mmap creation structurally impossible.
        max_files: CONTENT_CACHE_MAX_FILES,
        max_bytes: CONTENT_CACHE_MAX_BYTES,
        // The buffered path also consults this field. Keep it aligned with the
        // grep cap so a future path that skips the options prefilter still
        // cannot reach the fresh-mmap threshold.
        max_file_size: MMAP_SAFE_MAX_FILE_SIZE,
        cached_count: AtomicUsize::new(0),
        cached_bytes: AtomicU64::new(0),
    }
}

/// What one warm-index grep proved.
///
/// Coverage is partial by design. A single file the index cannot search — one
/// above the mmap-safe size cap, or one whose display lines fff would truncate
/// — used to discard the entire query; in a repository holding even one such
/// file that made the index dead weight for every root-scoped search. Naming
/// the file instead lets the caller re-grep just that file with its own engine
/// and merge, which is why this carries lines rather than rendered bytes:
/// merged lines render once, so ordering and group separators come out right
/// by construction.
pub struct WorktreeGrepOutcome {
    /// Every line the index proved, in the shared model.
    pub lines: Vec<GrepLine>,
    /// Search-root-relative paths the index could not cover.
    pub uncovered: Vec<String>,
}

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
    served_grep_queries: AtomicU64,
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
                // An explicit budget prevents fff's post-scan auto-budget from
                // replacing it and closes lazy `get_cached_content` mmap creation.
                cache_budget: Some(content_cache_budget()),
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
            served_grep_queries: AtomicU64::new(0),
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
    /// `None` means the index cannot answer this query at all and the caller
    /// must use its own engine for the whole thing: the background scan has not
    /// finished (the file set is not yet known), the picker lock failed, the
    /// pattern or a glob does not compile (the caller emits the canonical
    /// error), or the result set ran past the drain safety cap.
    ///
    /// `Some(outcome)` carries the lines the index proved plus the paths it
    /// could not cover; see [`WorktreeGrepOutcome`]. Rendering, windowing, and
    /// the empty-result message all belong to the caller.
    pub fn try_grep(
        &self,
        params: &WorktreeGrepParams,
        deny_read: &[PathBuf],
    ) -> Option<WorktreeGrepOutcome> {
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

        // ripgrep's `-m N` does not simply stop at the Nth match: it keeps
        // printing that match's after-context window, and a line inside the
        // window that itself matches renders as a match rather than context.
        // fff's per-file cap has no way to express either, so decline the
        // combination whole and let the caller's engine answer it exactly.
        //
        // A zero cap means "no matches" by construction. It is declined rather
        // than answered empty so the PATH shim passes it through to the real
        // binary, whose behavior for `-m 0` differs between ripgrep (nothing,
        // exit 1) and BSD grep (every line, as context).
        if params.max_per_file.is_some_and(|max| max == 0)
            || (params.max_per_file.is_some() && params.after_context > 0)
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
        // above that cap — a completeness hole. Name those files as uncovered so
        // the caller re-greps them with its buffered, uncapped, crash-safe
        // engine. The picker read guard is held across the whole query, so the
        // watcher cannot change indexed sizes between this check and the search.
        let mut uncovered = oversized_in_scope_files(
            picker,
            &self.worktree,
            subdir,
            overrides.as_ref(),
            deny_read,
        );

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
        // Display truncation changes a line's *bytes*, not whether the file
        // matched or how many matches it holds, so it only makes a file
        // uncovered for `content`.
        let checks_truncation = params.output_mode == "content";
        let mut truncated: HashSet<String> = HashSet::new();
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
                // fff truncates long display lines to 512 bytes. Hand that file
                // to the caller's engine, which emits full content, so a match's
                // bytes are identical whether the index is warm or cold.
                if checks_truncation
                    && (m.line_content.len() >= LINE_TRUNCATION_THRESHOLD
                        || m.context_before
                            .iter()
                            .any(|l| l.len() >= LINE_TRUNCATION_THRESHOLD)
                        || m.context_after
                            .iter()
                            .any(|l| l.len() >= LINE_TRUNCATION_THRESHOLD))
                {
                    truncated.insert(stripped_rel.to_string());
                    continue;
                }
                collected.push(OwnedMatch {
                    rel: stripped_rel.to_string(),
                    line_number: m.line_number,
                    line_content: m.line_content.clone(),
                    context_before: m.context_before.clone(),
                    context_after: m.context_after.clone(),
                });
            }
            if result.next_file_offset == 0 {
                break;
            }
            // Past the safety cap the drain would be truncated, and a truncated
            // result set is indistinguishable from a complete one downstream.
            // Decline the whole query rather than pass off a partial answer as
            // a complete one.
            if collected.len() >= DRAIN_SAFETY_CAP {
                log::warn!(
                    "worktree_search: grep for {} passed the {DRAIN_SAFETY_CAP}-match drain cap; \
                     declining so the caller's engine answers completely",
                    self.worktree.display()
                );
                return None;
            }
            file_offset = result.next_file_offset;
        }

        // Matches already drained from a truncating file would duplicate what
        // the caller's re-grep produces, so drop them.
        if !truncated.is_empty() {
            collected.retain(|m| !truncated.contains(&m.rel));
            uncovered.extend(truncated);
            uncovered.sort();
            uncovered.dedup();
        }

        self.served_grep_queries.fetch_add(1, Ordering::Relaxed);
        Some(WorktreeGrepOutcome {
            lines: grep_lines(&collected),
            uncovered,
        })
    }

    /// Number of grep queries completed by this resident index.
    ///
    /// This monotonic diagnostic makes routing observable without coupling callers
    /// to fff internals and lets integration tests prove a virtual search used the
    /// warm engine rather than its filesystem fallback.
    pub fn served_grep_query_count(&self) -> u64 {
        self.served_grep_queries.load(Ordering::Relaxed)
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

/// Search-root-relative paths of the non-binary, in-scope files larger than
/// [`MMAP_SAFE_MAX_FILE_SIZE`]. The `max_file_size` cap keeps such a file from
/// being memory-mapped (and so from raising SIGBUS on a concurrent truncation),
/// but that same cap makes fff silently skip it — so it is reported as
/// uncovered and the caller greps it with its own engine.
///
/// Scans both the primary file list and the watcher-discovered overflow list (a
/// file added/removed mid-session lands in overflow). "In scope" mirrors the
/// per-match filtering exactly: the worktree-relative path must survive `subdir`
/// stripping, pass the user glob override filter, and not sit under a
/// `deny_read` fence root. Binaries are excluded because neither engine
/// content-searches them and fff never mmaps them, so a large committed binary
/// (a PNG, say) costs nothing.
fn oversized_in_scope_files(
    picker: &FilePicker,
    worktree: &Path,
    subdir: Option<&str>,
    overrides: Option<&ignore::overrides::Override>,
    deny_read: &[PathBuf],
) -> Vec<String> {
    let mut paths: Vec<String> = picker
        .get_files()
        .iter()
        .chain(picker.get_overflow_files())
        .filter_map(|file| {
            if file.is_binary() || file.size <= MMAP_SAFE_MAX_FILE_SIZE {
                return None;
            }
            let rel = file.relative_path(picker);
            let stripped_rel = strip_subdir_prefix(&rel, subdir)?;
            if overrides
                .is_some_and(|filter| filter.matched(Path::new(stripped_rel), false).is_ignore())
            {
                return None;
            }
            if !deny_read.is_empty() && path_within_any(&worktree.join(&rel), deny_read) {
                return None;
            }
            Some(stripped_rel.to_string())
        })
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

/// Expand drained matches into the shared [`GrepLine`] model: each match
/// contributes its before-context window, the matched line, and its
/// after-context window at their absolute line numbers. Windows of nearby
/// matches overlap; the renderer's coordinate dedupe is what collapses them,
/// so this deliberately does no grouping of its own.
fn grep_lines(matches: &[OwnedMatch]) -> Vec<GrepLine> {
    let mut lines = Vec::with_capacity(matches.len());
    for m in matches {
        let before_start = m.line_number.saturating_sub(m.context_before.len() as u64);
        for (offset, text) in m.context_before.iter().enumerate() {
            lines.push(GrepLine {
                path: m.rel.clone(),
                line_number: before_start + offset as u64,
                is_match: false,
                text: text.clone(),
            });
        }
        lines.push(GrepLine {
            path: m.rel.clone(),
            line_number: m.line_number,
            is_match: true,
            text: m.line_content.clone(),
        });
        for (offset, text) in m.context_after.iter().enumerate() {
            lines.push(GrepLine {
                path: m.rel.clone(),
                line_number: m.line_number + 1 + offset as u64,
                is_match: false,
                text: text.clone(),
            });
        }
    }
    lines
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
    fn new(capacity: usize) -> Self {
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
    use crate::search_util::render_grep_lines;

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

    /// Drive the same expansion + rendering the live path uses.
    fn render(matches: &[OwnedMatch], params: &WorktreeGrepParams, native_output: bool) -> String {
        render_grep_lines(
            grep_lines(matches),
            &params.output_mode,
            params.show_line_numbers,
            native_output,
            params.before_context > 0 || params.after_context > 0,
        )
    }

    #[test]
    fn format_content_emits_path_line_text() {
        let matches = vec![m("src/a.rs", 10, "let x = 1;"), m("src/b.rs", 3, "x()")];
        let out = render(&matches, &content_params(), false);
        assert_eq!(out, "src/a.rs:10:let x = 1;\nsrc/b.rs:3:x()");
    }

    #[test]
    fn format_content_without_line_numbers() {
        let mut params = content_params();
        params.show_line_numbers = false;
        let out = render(&[m("a.rs", 1, "hit")], &params, false);
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
        let out = render(&[first, second], &params, false);
        assert_eq!(
            out,
            "a.rs:4-before4\na.rs:5:MATCH1\na.rs:6-after6\n--\na.rs:19-before19\na.rs:20:MATCH2"
        );
    }

    /// Two matches close enough that their context windows overlap share the
    /// lines between them. fff reports each window in full, so only the
    /// renderer's coordinate dedupe keeps them from printing twice.
    #[test]
    fn format_content_collapses_overlapping_context_windows() {
        let mut params = content_params();
        params.before_context = 2;
        params.after_context = 2;
        let mut first = m("a.rs", 3, "MATCH1");
        first.context_before = vec!["one".to_string(), "two".to_string()];
        first.context_after = vec!["four".to_string(), "five".to_string()];
        let mut second = m("a.rs", 6, "MATCH2");
        second.context_before = vec!["four".to_string(), "five".to_string()];
        second.context_after = vec!["seven".to_string(), "eight".to_string()];
        assert_eq!(
            render(&[first, second], &params, false),
            "a.rs:1-one\na.rs:2-two\na.rs:3:MATCH1\na.rs:4-four\na.rs:5-five\n\
             a.rs:6:MATCH2\na.rs:7-seven\na.rs:8-eight"
        );
    }

    #[test]
    fn format_content_with_context_separates_files() {
        let mut params = content_params();
        params.before_context = 1;
        let out = render(
            &[m("a.rs", 5, "MATCH1"), m("b.rs", 1, "MATCH2")],
            &params,
            true,
        );
        assert_eq!(out, "a.rs:5:MATCH1\n--\nb.rs:1:MATCH2");
    }

    #[test]
    fn format_files_with_matches_dedups_by_path() {
        let matches = vec![
            m("src/b.rs", 1, "x"),
            m("src/a.rs", 2, "x"),
            m("src/b.rs", 9, "x"),
        ];
        let mut params = content_params();
        params.output_mode = "files_with_matches".to_string();
        let out = render(&matches, &params, false);
        assert_eq!(out, "src/a.rs\nsrc/b.rs");
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
        let out = render(&matches, &params, false);
        assert_eq!(out, "src/a.rs:1\nsrc/b.rs:2");
    }

    /// Render a grep the index covered completely, as the live seam does.
    fn served(
        search: &WorktreeSearch,
        params: &WorktreeGrepParams,
        deny_read: &[PathBuf],
    ) -> String {
        let outcome = search.try_grep(params, deny_read).expect("index served");
        assert!(
            outcome.uncovered.is_empty(),
            "expected full coverage, got uncovered {:?}",
            outcome.uncovered
        );
        render_grep_lines(
            outcome.lines,
            &params.output_mode,
            params.show_line_numbers,
            false,
            params.before_context > 0 || params.after_context > 0,
        )
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

        let out = served(&search, &params, &[]);
        let files: HashSet<&str> = out.lines().collect();
        assert!(files.contains("alpha.rs"), "root match found: {out:?}");
        assert!(files.contains("sub/beta.rs"), "subdir match found: {out:?}");
        assert!(
            !out.contains("ignored.log"),
            "gitignored file excluded: {out:?}"
        );

        // content mode carries `path:N:text`.
        let content = served(
            &search,
            &WorktreeGrepParams {
                output_mode: "content".to_string(),
                ..params_clone(&params)
            },
            &[],
        );
        assert!(
            content.contains("alpha.rs:1:fn needle() {}"),
            "content format: {content:?}"
        );

        // Fence: denying `sub/` drops sub/beta.rs from results.
        let fenced = served(&search, &params, &[root.join("sub")]);
        let fenced_files: HashSet<&str> = fenced.lines().collect();
        assert!(fenced_files.contains("alpha.rs"));
        assert!(
            !fenced_files.contains("sub/beta.rs"),
            "deny_read path excluded from fff output: {fenced:?}"
        );
    }

    /// An invalid regex is the one pattern-level whole-query decline: fff would
    /// silently fall back to literal matching, so the index steps aside and the
    /// caller produces the canonical "Invalid regex pattern" error instead.
    #[test]
    fn index_declines_the_whole_query_for_an_invalid_regex() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "short.rs", "let needle = 1;\n");

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        let bad = WorktreeGrepParams {
            pattern: "[".to_string(),
            ..content_params()
        };
        assert!(search.try_grep(&bad, &[]).is_none());
    }

    /// A line past fff's 512-byte display cap would come back shortened, so the
    /// file naming itself uncovered is how match bytes stay identical between
    /// warm and cold routing. Only that file is affected, and only in `content`
    /// mode — truncated display changes neither whether a file matched nor how
    /// many matches it holds.
    #[test]
    fn a_truncating_line_makes_only_its_file_uncovered_and_only_for_content() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        write_file(root, "short.rs", "let needle = 1;\n");
        write_file(
            root,
            "long.rs",
            &format!("let needle = \"{}\";\n", "a".repeat(700)),
        );

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));

        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        let outcome = search.try_grep(&needle, &[]).expect("partially served");
        assert_eq!(outcome.uncovered, vec!["long.rs".to_string()]);
        assert_eq!(
            render_grep_lines(outcome.lines, "content", true, false, false),
            "short.rs:1:let needle = 1;",
            "the truncating file's drained matches are dropped so the re-grep is not doubled"
        );

        for output_mode in ["files_with_matches", "count"] {
            let params = WorktreeGrepParams {
                pattern: "needle".to_string(),
                output_mode: output_mode.to_string(),
                ..content_params()
            };
            let outcome = search.try_grep(&params, &[]).expect("served");
            assert!(
                outcome.uncovered.is_empty(),
                "{output_mode} must not be poisoned by a long line: {:?}",
                outcome.uncovered
            );
            assert!(
                render_grep_lines(outcome.lines, output_mode, true, false, false)
                    .contains("long.rs")
            );
        }
    }

    /// fff never searches a non-binary file above the mmap-safe cap (the SIGBUS
    /// crash-proofing, CAIRN-2574), so it is named uncovered and the rest of
    /// the tree is still served. Narrowing scope past it with a glob restores
    /// full coverage.
    #[test]
    fn an_oversized_in_scope_file_is_uncovered_not_fatal() {
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
        let outcome = search.try_grep(&needle, &[]).expect("partially served");
        assert_eq!(outcome.uncovered, vec!["big.rs".to_string()]);
        assert_eq!(
            render_grep_lines(outcome.lines, "content", true, false, false),
            "small.rs:1:let needle = 1;"
        );

        // A glob that excludes big.rs takes it out of scope entirely.
        let narrowed = WorktreeGrepParams {
            pattern: "needle".to_string(),
            globs: vec!["small.rs".to_string()],
            ..content_params()
        };
        let out = served(&search, &narrowed, &[]);
        assert!(out.contains("small.rs:1:let needle = 1;"), "{out:?}");
    }

    /// A file in fff's lazy-cache window must still use the buffered grep path.
    /// Observing the counters avoids a truncate-mid-grep test that could SIGBUS
    /// and terminate the test process instead of producing an assertion failure.
    #[test]
    fn grep_does_not_create_a_lazy_persistent_mmap() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        let body = format!("needle\n{}", "a".repeat(64 * 1024));
        assert!(body.len() as u64 > 16 * 1024);
        assert!((body.len() as u64) < MMAP_SAFE_MAX_FILE_SIZE);
        write_file(root, "cache-window.txt", &body);

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(15)));
        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        let outcome = search.try_grep(&needle, &[]).expect("index served");
        assert!(outcome.uncovered.is_empty());

        let guard = search.shared.read().unwrap();
        let budget = guard.as_ref().unwrap().cache_budget();
        assert_eq!(budget.max_files, 0);
        assert_eq!(budget.max_bytes, 0);
        assert_eq!(budget.max_file_size, MMAP_SAFE_MAX_FILE_SIZE);
        assert_eq!(budget.cached_count.load(Ordering::Relaxed), 0);
        assert_eq!(budget.cached_bytes.load(Ordering::Relaxed), 0);
    }

    /// A large *binary* file must not count as uncovered: neither engine
    /// content-searches binaries and fff never mmaps them, so re-grepping it
    /// would be pure waste.
    #[test]
    fn index_oversized_binary_file_stays_fully_covered() {
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
        let out = served(&search, &needle, &[]);
        assert!(out.contains("small.rs:1:let needle = 1;"), "{out:?}");
    }

    /// Past the drain safety cap the result set would be silently cut short,
    /// and a cut-short set is indistinguishable downstream from a complete one.
    /// Decline the whole query so the caller's uncapped engine answers it.
    #[test]
    fn a_result_set_past_the_drain_cap_declines_rather_than_truncating() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_init(root);
        // Each file stays under the mmap-safe cap so none is "uncovered".
        // fff fills a page at PAGE_LIMIT matches, so a page covers about four
        // of these files, and the drain cap is only consulted when a page ends
        // with files still unsearched — hence enough files for a third page to
        // begin after the cap has been passed.
        let per_file = 30_000;
        let body = "needle\n".repeat(per_file);
        assert!((body.len() as u64) < MMAP_SAFE_MAX_FILE_SIZE);
        let files = 12;
        for index in 0..files {
            write_file(root, &format!("flood-{index}.txt"), &body);
        }

        let search = WorktreeSearch::new(root).unwrap();
        assert!(search.shared.wait_for_scan(Duration::from_secs(30)));

        let needle = WorktreeGrepParams {
            pattern: "needle".to_string(),
            ..content_params()
        };
        assert!(
            search.try_grep(&needle, &[]).is_none(),
            "a drain past the safety cap must decline, not return a partial body \
             (if fff's paging changed so this fixture arrives in fewer pages, \
             the cap is never consulted — grow `files`)"
        );
    }

    /// The cap must stay below fff `=0.9.6`'s smallest `FRESH_MMAP_THRESHOLD`
    /// (256 KiB on Unix; macOS is 1 MiB). If a future fff bump lowers that
    /// threshold, this assert and the exact-version pin in `Cargo.toml` must
    /// move together — the crash-safety guarantee rests on the version pin.
    #[test]
    fn mmap_safe_cap_is_below_fff_threshold() {
        // Compile-time guard: the cap must stay below fff's smallest threshold.
        const { assert!(MMAP_SAFE_MAX_FILE_SIZE < 256 * 1024) };
        // Lazy persistent mmap creation stays impossible only while both entry
        // budgets are zero; keep this invariant visible beside the size guard.
        const { assert!(CONTENT_CACHE_MAX_FILES == 0 && CONTENT_CACHE_MAX_BYTES == 0) };
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
            served_grep_queries: AtomicU64::new(0),
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
