//! Store-served source control: `cairn://p/{project}/vcs/{diff,ancestry,log,blame}`.
//!
//! Every answer comes from the project's jj store (the commit graph, through
//! `cairn_vcs::history` and `cairn_vcs::merge_base`) and its Git object database
//! (trees, blobs and diffs, through `cairn_codec`'s gix `ObjectStore`), so a
//! builder can ask what its branch changed, where it forked, or who wrote a
//! line without a checkout. No operation spawns a subprocess or takes the store
//! lock; refs resolve through the same resolver `?branch=` uses.
//!
//! Answers are cached by resolved commit id, never by ref name: the inputs are
//! immutable commits, so nothing is ever invalidated, and a ref that moves
//! resolves to a new id and misses. Revision labels (age, STALE) are not part of
//! the cached answer; they are rendered fresh on every read.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use futures_util::future::{BoxFuture, FutureExt, Shared};

use cairn_codec::diff::render_commit_diff;
use cairn_codec::objects::ObjectStore;
use cairn_common::query::QueryParam;
use cairn_vcs::history::{self, HistoryError};
use globset::GlobBuilder;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use super::diff::{diff_data_between, escape_table, filter_patch, short, RangeDiff};
use super::files::{dedupe_file_changes_by_path, push_file_change_table, FileChangeRow};
use crate::mcp::handlers::branch::{
    resolve_current_for_read, resolve_for_project_read, BranchResolution,
};
use crate::mcp::handlers::read::overlay::ByteLru;
use crate::mcp::handlers::run_context::project_id_by_key;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// Rendered answers held across reads, by bytes.
const ANSWER_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// The longest any one operation may run; the same bound object reads use.
const QUERY_LIMIT: Duration = Duration::from_secs(30);
/// Ahead/behind walks stop here and render as `≥N`.
const COUNT_CAP: usize = 10_000;
const BLAME_MAX_BYTES: usize = 1024 * 1024;
const LOG_DEFAULT_LIMIT: usize = 30;
const LOG_MAX_LIMIT: usize = 500;
/// How many commits a diff's `view=commits` lists.
const DIFF_COMMITS_LIMIT: usize = 500;

fn answer_cache() -> &'static ByteLru {
    static CACHE: OnceLock<ByteLru> = OnceLock::new();
    CACHE.get_or_init(|| ByteLru::new(ANSWER_CACHE_BYTES))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VcsOp {
    Diff,
    Ancestry,
    Log,
    Blame,
}

impl VcsOp {
    fn name(self) -> &'static str {
        match self {
            Self::Diff => "diff",
            Self::Ancestry => "ancestry",
            Self::Log => "log",
            Self::Blame => "blame",
        }
    }

    fn allowed_keys(self) -> &'static [&'static str] {
        match self {
            Self::Diff => &[
                "from", "to", "commit", "base", "view", "file", "glob", "format",
            ],
            Self::Ancestry => &["from", "to", "format"],
            Self::Log => &["from", "to", "path", "limit", "offset", "format"],
            Self::Blame => &["rev", "path", "lines", "format"],
        }
    }
}

/// One query's parameters, validated before anything is resolved.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct VcsQuery {
    from: Option<String>,
    to: Option<String>,
    commit: Option<String>,
    base: DiffBase,
    view: DiffView,
    file: Option<String>,
    glob: Option<String>,
    path: Option<String>,
    rev: Option<String>,
    lines: Option<(usize, usize)>,
    limit: usize,
    offset: usize,
    json: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum DiffBase {
    /// From the merge-base of `from` and `to` (git's `from...to`).
    #[default]
    Fork,
    /// Tree to tree (git's `from to`).
    From,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum DiffView {
    #[default]
    Summary,
    Patch,
    Commits,
}

fn parse_query(op: VcsOp, params: &[QueryParam]) -> Result<VcsQuery, String> {
    let mut query = VcsQuery {
        limit: LOG_DEFAULT_LIMIT,
        ..VcsQuery::default()
    };
    let mut view = None;
    for param in params {
        if !op.allowed_keys().contains(&param.key.as_str()) {
            return Err(format!(
                "Unsupported query parameter '{}' for vcs/{}; accepted: {}",
                param.key,
                op.name(),
                op.allowed_keys().join(", ")
            ));
        }
        let value = param.value.trim();
        if value.is_empty() {
            return Err(format!("Query parameter '{}' needs a value", param.key));
        }
        let value = value.to_string();
        match param.key.as_str() {
            "from" => query.from = Some(value),
            "to" => query.to = Some(value),
            "commit" => query.commit = Some(value),
            "base" => {
                query.base = match value.as_str() {
                    "fork" => DiffBase::Fork,
                    "from" => DiffBase::From,
                    other => return Err(format!("Invalid base '{other}'. Expected fork or from.")),
                }
            }
            "view" => {
                view = Some(match value.as_str() {
                    "summary" => DiffView::Summary,
                    "patch" => DiffView::Patch,
                    "commits" => DiffView::Commits,
                    other => {
                        return Err(format!(
                            "Invalid view '{other}'. Expected summary, patch, or commits."
                        ))
                    }
                })
            }
            "file" => query.file = Some(value),
            "glob" => query.glob = Some(value),
            "path" => query.path = Some(value),
            "rev" => query.rev = Some(value),
            "lines" => query.lines = Some(parse_lines(&value)?),
            "limit" => {
                let limit: usize = value
                    .parse()
                    .map_err(|_| format!("Invalid limit '{value}'"))?;
                if limit == 0 || limit > LOG_MAX_LIMIT {
                    return Err(format!("limit must be between 1 and {LOG_MAX_LIMIT}"));
                }
                query.limit = limit;
            }
            "offset" => {
                query.offset = value
                    .parse()
                    .map_err(|_| format!("Invalid offset '{value}'"))?
            }
            "format" => match value.as_str() {
                "json" => query.json = true,
                other => return Err(format!("Invalid format '{other}'. Expected json.")),
            },
            _ => unreachable!("allowed_keys covers every arm"),
        }
    }
    if op == VcsOp::Diff {
        if query.commit.is_some() && (query.from.is_some() || query.to.is_some()) {
            return Err("vcs/diff takes commit=REF or from/to, not both".to_string());
        }
        if query.commit.is_some() && params.iter().any(|param| param.key == "base") {
            return Err(
                "base= applies to from/to; commit=REF always diffs against its first parent"
                    .to_string(),
            );
        }
        if query.file.is_some() && query.glob.is_some() {
            return Err("vcs/diff accepts either file=PATH or glob=PATTERN, not both".to_string());
        }
        query.view = match view {
            Some(DiffView::Patch) | None if query.file.is_some() => DiffView::Patch,
            Some(_) if query.file.is_some() => {
                return Err("file=PATH is only valid with view=patch".to_string())
            }
            Some(DiffView::Commits) if query.glob.is_some() => {
                return Err("glob=PATTERN is only valid with the summary or patch view".to_string())
            }
            Some(view) => view,
            None => DiffView::Summary,
        };
    }
    if op == VcsOp::Blame && query.path.is_none() {
        return Err("vcs/blame needs path=FILE".to_string());
    }
    Ok(query)
}

fn parse_lines(value: &str) -> Result<(usize, usize), String> {
    let invalid = || format!("Invalid lines '{value}'. Expected A-B with 1 <= A <= B.");
    let (start, end) = value.split_once('-').ok_or_else(invalid)?;
    let start: usize = start.trim().parse().map_err(|_| invalid())?;
    let end: usize = end.trim().parse().map_err(|_| invalid())?;
    if start == 0 || end < start {
        return Err(invalid());
    }
    Ok((start, end))
}

/// A requested revision, resolved and labelled.
struct Endpoint {
    requested: String,
    resolution: BranchResolution,
    label: String,
}

impl Endpoint {
    fn commit(&self) -> &str {
        &self.resolution.commit_id
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "requested": self.requested,
            "rev": self.resolution.rev,
            "commit": self.resolution.commit_id,
            "label": self.label,
        })
    }
}

async fn label(orch: &Orchestrator, resolution: &BranchResolution) -> String {
    crate::orchestrator::base_branch_sync::revision_label(
        orch,
        &resolution.project_id,
        &resolution.rev,
        &resolution.default_branch,
        &resolution.repository_path,
        &resolution.commit_id,
    )
    .await
}

async fn resolve_named(
    orch: &Orchestrator,
    project_id: &str,
    key: &str,
    requested: &str,
) -> Result<Endpoint, String> {
    let resolution = resolve_for_project_read(orch, project_id, requested)
        .await
        .map_err(|error| format!("Unable to resolve {key}='{requested}': {error}"))?;
    let label = label(orch, &resolution).await;
    Ok(Endpoint {
        requested: requested.to_string(),
        resolution,
        label,
    })
}

/// The calling node's own branch, the default for `to` and `rev`.
async fn resolve_caller(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_id: &str,
    key: &str,
) -> Result<Endpoint, String> {
    if request.run_id.as_deref().is_none_or(str::is_empty) {
        return Err(format!(
            "{key}= is required: this read has no calling node whose branch could default it"
        ));
    }
    let resolution = resolve_current_for_read(orch, request)
        .await
        .map_err(|error| format!("Unable to resolve the calling node's branch: {error}"))?;
    if resolution.project_id != project_id {
        return Err(format!(
            "The calling node belongs to a different project; pass {key}= explicitly"
        ));
    }
    let label = label(orch, &resolution).await;
    Ok(Endpoint {
        requested: format!("(this node's branch {})", resolution.rev),
        resolution,
        label,
    })
}

/// `from` as asked, or by default the base the calling node's work merges into
/// (a stacked child's parent branch, else the project default), so a bare
/// `vcs/diff` and `vcs/ancestry` measure a branch against the same base node
/// `/diff` does. A read with no calling node in this project falls back to the
/// project's default branch.
async fn resolve_from(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_id: &str,
    requested: Option<&str>,
) -> Result<Endpoint, String> {
    if let Some(requested) = requested {
        return resolve_named(orch, project_id, "from", requested).await;
    }
    let caller_base = if request.run_id.as_deref().is_some_and(|id| !id.is_empty()) {
        crate::mcp::handlers::branch::current_base_branch(orch, request)
            .await
            .ok()
            .filter(|(caller_project, _)| caller_project == project_id)
            .and_then(|(_, base)| base)
    } else {
        None
    };
    match caller_base {
        Some(base) => {
            let mut endpoint = resolve_named(orch, project_id, "from", &base).await?;
            endpoint.requested = format!("(this node's base {base})");
            Ok(endpoint)
        }
        None => resolve_named(orch, project_id, "from", "main").await,
    }
}

async fn resolve_or_default(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_id: &str,
    key: &str,
    requested: Option<&str>,
) -> Result<Endpoint, String> {
    match requested {
        Some(requested) => resolve_named(orch, project_id, key, requested).await,
        None => resolve_caller(orch, request, project_id, key).await,
    }
}

pub(super) async fn read_vcs(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    op: VcsOp,
    params: &[QueryParam],
) -> String {
    let query = match parse_query(op, params) {
        Ok(query) => query,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let project_id = match project_id_by_key(&db, project).await {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
    let result = match op {
        VcsOp::Diff => read_diff(orch, request, project, &project_id, &query).await,
        VcsOp::Ancestry => read_ancestry(orch, request, project, &project_id, &query).await,
        VcsOp::Log => read_log(orch, request, project, &project_id, &query).await,
        VcsOp::Blame => read_blame(orch, request, project, &project_id, &query).await,
    };
    result.unwrap_or_else(|error| error)
}

/// A computation in progress for one answer key, shared by every read that
/// asks the same question while it runs.
type Flight = Shared<BoxFuture<'static, Result<Arc<[u8]>, String>>>;

fn flights() -> &'static Mutex<HashMap<String, Flight>> {
    static FLIGHTS: OnceLock<Mutex<HashMap<String, Flight>>> = OnceLock::new();
    FLIGHTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_flights() -> MutexGuard<'static, HashMap<String, Flight>> {
    flights().lock().unwrap_or_else(PoisonError::into_inner)
}

/// Serve `key` from the answer cache, or from the one computation answering it.
///
/// At most one computation per key is ever live. A read that arrives while one
/// runs joins it rather than starting another, and the query limit bounds only
/// how long a READER waits: a computation that outlives it keeps running,
/// caches its answer when it completes, and so serves the retry that always
/// follows a timeout. Cancelling it instead would make every retry pay the
/// full cost again and never converge on a large input, while starting a fresh
/// one per read stacked identical multi-gigabyte computations. What bounds an
/// orphaned computation is its own work budget, not the reader's patience.
///
/// An answer that fails is not cached, so a transient store error is retried
/// by the next read.
async fn cached<T, Fut>(what: &'static str, key: &str, compute: Fut) -> Result<T, String>
where
    T: StoreAnswer,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    cached_within(QUERY_LIMIT, what, key, compute).await
}

async fn cached_within<T, Fut>(
    limit: Duration,
    what: &'static str,
    key: &str,
    compute: Fut,
) -> Result<T, String>
where
    T: StoreAnswer,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    // One record per read, saying how it was served: from the cache, by
    // joining a computation already in flight, or by launching one. Read with
    // `vcs_compute`, a retry after a timeout shows as a `launched` read that
    // timed out, a `joined` or `cache` read, and a single computation.
    let span = tracing::info_span!(
        target: "profiler",
        "vcs_answer",
        op = what,
        key = key,
        served = tracing::field::Empty,
        timed_out = tracing::field::Empty,
    );
    if let Some(answer) = cached_answer(key) {
        span.record("served", "cache");
        return Ok(answer);
    }
    let (flight, served) = {
        let mut flights = lock_flights();
        // A flight caches its answer before it leaves the table, so checking
        // again under the lock closes the window in which one finished between
        // the miss above and taking the lock.
        if let Some(answer) = cached_answer(key) {
            span.record("served", "cache");
            return Ok(answer);
        }
        match flights.entry(key.to_string()) {
            Entry::Occupied(entry) => (entry.get().clone(), "joined"),
            Entry::Vacant(entry) => (
                entry.insert(launch(what, key.to_string(), compute)).clone(),
                "launched",
            ),
        }
    };
    span.record("served", served);
    let outcome = tokio::time::timeout(limit, flight)
        .instrument(span.clone())
        .await;
    span.record("timed_out", outcome.is_err());
    match outcome {
        Err(_) => Err(timed_out(what, limit)),
        Ok(Err(error)) => Err(error),
        Ok(Ok(bytes)) => serde_json::from_slice(&bytes)
            .map_err(|error| format!("The {what} answer could not be decoded: {error}")),
    }
}

/// What [`cached`] stores and serves: a serializable answer that can say how
/// many files it covers, so the computation's profiler record carries its size.
trait StoreAnswer: Serialize + DeserializeOwned + Send + 'static {
    fn files(&self) -> Option<usize> {
        None
    }
}

fn cached_answer<T: DeserializeOwned>(key: &str) -> Option<T> {
    let bytes = answer_cache().get(key)?;
    serde_json::from_slice(&bytes).ok()
}

/// Start the one computation for `key` as its own task, so it runs to
/// completion whether or not any reader is still waiting.
fn launch<T, Fut>(what: &'static str, key: String, compute: Fut) -> Flight
where
    T: StoreAnswer,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    // One record per computation, spanning it whole even after every reader
    // has stopped waiting.
    let span = tracing::info_span!(
        target: "profiler",
        "vcs_compute",
        op = what,
        key = key.as_str(),
        files = tracing::field::Empty,
    );
    let recorded = span.clone();
    let task = tokio::spawn(
        async move {
            // Leaves the table however the task ends, a panic included, so a
            // key can never be stranded behind a flight that will not land.
            let _landing = Landing(key.clone());
            let answer = compute.await?;
            if let Some(files) = answer.files() {
                recorded.record("files", files);
            }
            let bytes = serde_json::to_vec(&answer)
                .map_err(|error| format!("The answer could not be encoded: {error}"))?;
            answer_cache().insert(&key, bytes.clone().into());
            Ok(Arc::<[u8]>::from(bytes))
        }
        .instrument(span),
    );
    async move {
        task.await
            .map_err(|error| format!("The store task failed: {error}"))?
    }
    .boxed()
    .shared()
}

struct Landing(String);

impl Drop for Landing {
    fn drop(&mut self) {
        lock_flights().remove(&self.0);
    }
}

fn timed_out(what: &str, limit: Duration) -> String {
    format!(
        "The {what} did not finish within {}s, the store read limit, so no partial answer is \
         served. Narrow the range or the path and read again.",
        limit.as_secs()
    )
}

fn history_error(what: &str, error: HistoryError) -> String {
    match error {
        HistoryError::TimedOut { .. } => timed_out(what, QUERY_LIMIT),
        other => format!("Unable to compute the {what}: {other}"),
    }
}

/// Run a synchronous object-store computation on the blocking pool. It is not
/// timed here: [`cached`] applies the query limit to the reader's wait, so the
/// work finishes, is cached, and answers the retry.
async fn blocking<T, F>(what: &'static str, compute: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    match tokio::task::spawn_blocking(compute).await {
        Err(error) => Err(format!("The {what} task failed: {error}")),
        Ok(result) => result,
    }
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DiffAnswer {
    /// The commit the patch is rendered against.
    base: String,
    tip: String,
    rows: Vec<FileChangeRow>,
    patch: String,
}

impl StoreAnswer for DiffAnswer {
    fn files(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}

async fn read_diff(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    project_id: &str,
    query: &VcsQuery,
) -> Result<String, String> {
    let glob = query
        .glob
        .as_deref()
        .map(|pattern| {
            GlobBuilder::new(pattern)
                .literal_separator(false)
                .build()
                .map(|glob| glob.compile_matcher())
                .map_err(|error| format!("Invalid glob '{pattern}': {error}"))
        })
        .transpose()?;

    if let Some(commit) = query.commit.as_deref() {
        let endpoint = resolve_named(orch, project_id, "commit", commit).await?;
        let objects = endpoint.resolution.object_repository_path.clone();
        let tip = endpoint.commit().to_string();
        let answer = if query.view == DiffView::Commits {
            None
        } else {
            let key = format!("diff-commit|{project_id}|{tip}");
            Some(
                cached(
                    "diff",
                    &key,
                    blocking("diff", move || {
                        let store = ObjectStore::new(&objects, None)?;
                        let patch = render_commit_diff(&store, &tip)?;
                        let rows = cairn_vcs::jj::parse_git_patch(&patch)
                            .into_iter()
                            .map(|change| {
                                (
                                    change.path,
                                    change.status,
                                    Some(change.additions),
                                    Some(change.deletions),
                                    change.previous_path,
                                )
                            })
                            .collect();
                        Ok(DiffAnswer {
                            base: String::new(),
                            tip,
                            rows,
                            patch,
                        })
                    }),
                )
                .await?,
            )
        };
        let commits = if query.view == DiffView::Commits {
            Some(
                log_page(
                    project_id,
                    &endpoint.resolution,
                    None,
                    endpoint.commit(),
                    None,
                    1,
                    0,
                )
                .await?,
            )
        } else {
            None
        };
        let header = DiffHeader {
            project,
            from: None,
            to: &endpoint,
            base: Some("first parent"),
            commit_form: true,
        };
        return Ok(render_diff(&header, answer, commits, query, glob.as_ref()));
    }

    let from = resolve_from(orch, request, project_id, query.from.as_deref()).await?;
    let to = resolve_or_default(orch, request, project_id, "to", query.to.as_deref()).await?;
    let store = to.resolution.repository_path.clone();
    let objects = to.resolution.object_repository_path.clone();
    let from_id = from.commit().to_string();
    let to_id = to.commit().to_string();

    if query.view == DiffView::Commits {
        let page = log_page(
            project_id,
            &to.resolution,
            Some(&from_id),
            &to_id,
            None,
            DIFF_COMMITS_LIMIT,
            0,
        )
        .await?;
        let header = DiffHeader {
            project,
            from: Some(&from),
            to: &to,
            base: None,
            commit_form: false,
        };
        return Ok(render_diff(&header, None, Some(page), query, glob.as_ref()));
    }

    let mode = query.base;
    let key = format!(
        "diff|{project_id}|{}|{from_id}|{to_id}|file={}|glob={}",
        match mode {
            DiffBase::Fork => "fork",
            DiffBase::From => "from",
        },
        query.file.as_deref().unwrap_or(""),
        query.glob.as_deref().unwrap_or(""),
    );
    // Narrowing happens before any file is aligned: a range can touch thousands
    // of files, and diffing them all to keep one is what made a one-file read of
    // a wide range outlive the query limit.
    let include = {
        let file = query.file.clone();
        let glob = glob.clone();
        move |path: &str| {
            file.as_deref().is_none_or(|file| file == path)
                && glob.as_ref().is_none_or(|glob| glob.is_match(path))
        }
    };
    let answer = cached("diff", &key, async move {
        let base = match mode {
            DiffBase::From => from_id.clone(),
            DiffBase::Fork => {
                cairn_vcs::merge_base(&store, &to_id, &from_id)
                    .await
                    .map_err(|error| {
                        format!(
                            "No merge-base between from and to ({error:?}); read with base=from \
                             for a tree-to-tree diff"
                        )
                    })?
                    .base
            }
        };
        blocking("diff", move || {
            let store = ObjectStore::new(&objects, None)?;
            let RangeDiff { patch, rows } = diff_data_between(&store, &base, &to_id, &include)?;
            Ok(DiffAnswer {
                base,
                tip: to_id,
                rows,
                patch,
            })
        })
        .await
    })
    .await?;
    let header = DiffHeader {
        project,
        from: Some(&from),
        to: &to,
        base: Some(match mode {
            DiffBase::Fork => "merge-base",
            DiffBase::From => "from",
        }),
        commit_form: false,
    };
    Ok(render_diff(
        &header,
        Some(answer),
        None,
        query,
        glob.as_ref(),
    ))
}

struct DiffHeader<'a> {
    project: &'a str,
    from: Option<&'a Endpoint>,
    to: &'a Endpoint,
    /// What the patch is rendered against, in words.
    base: Option<&'a str>,
    commit_form: bool,
}

fn render_diff(
    header: &DiffHeader<'_>,
    answer: Option<DiffAnswer>,
    commits: Option<LogAnswer>,
    query: &VcsQuery,
    glob: Option<&globset::GlobMatcher>,
) -> String {
    let mut rows = answer
        .as_ref()
        .map(|answer| dedupe_file_changes_by_path(&answer.rows))
        .unwrap_or_default();
    if let Some(file) = query.file.as_deref() {
        rows.retain(|row| row.0 == file || row.4.as_deref() == Some(file));
    }
    if let Some(glob) = glob {
        rows.retain(|row| {
            glob.is_match(&row.0) || row.4.as_deref().is_some_and(|path| glob.is_match(path))
        });
    }
    let patch = answer.as_ref().map(|answer| {
        if query.file.is_some() || glob.is_some() {
            filter_patch(&answer.patch, query.file.as_deref(), glob)
        } else {
            answer.patch.clone()
        }
    });

    if query.json {
        let mut json = serde_json::json!({
            "to": header.to.json(),
            "from": header.from.map(Endpoint::json),
            "view": match query.view {
                DiffView::Summary => "summary",
                DiffView::Patch => "patch",
                DiffView::Commits => "commits",
            },
        });
        if let Some(answer) = &answer {
            json["base"] = serde_json::Value::from(if header.commit_form {
                None
            } else {
                Some(answer.base.clone())
            });
            json["files"] = rows
                .iter()
                .map(|(path, status, additions, deletions, previous)| {
                    serde_json::json!({
                        "path": path,
                        "status": status,
                        "additions": additions,
                        "deletions": deletions,
                        "previousPath": previous,
                    })
                })
                .collect();
            if query.view == DiffView::Patch {
                json["patch"] = serde_json::Value::from(patch.clone().unwrap_or_default());
            }
        }
        if let Some(commits) = &commits {
            json["commits"] = commits_json(commits);
        }
        return serde_json::to_string_pretty(&json).unwrap_or_default();
    }

    let mut out = format!("# Store diff - {}\n\n", header.project);
    if let Some(from) = header.from {
        out.push_str(&format!("- From: `{}`\n", from.label));
    }
    let to_key = if header.commit_form { "Commit" } else { "To" };
    out.push_str(&format!("- {to_key}: `{}`\n", header.to.label));
    if let (Some(base), Some(answer)) = (header.base, &answer) {
        if header.commit_form {
            out.push_str("- Against: its first parent\n");
        } else {
            out.push_str(&format!(
                "- Against: {base} `{}`\n",
                short(Some(&answer.base))
            ));
        }
    }
    out.push('\n');
    match query.view {
        DiffView::Summary => {
            let additions: i32 = rows.iter().filter_map(|row| row.2).sum();
            let deletions: i32 = rows.iter().filter_map(|row| row.3).sum();
            out.push_str(&format!(
                "**{} files, +{additions} -{deletions}**\n\n",
                rows.len()
            ));
            if rows.is_empty() {
                out.push_str("No changes in this range.\n");
            } else {
                push_file_change_table(&mut out, &rows);
                out.push_str("\nAdd `view=patch&file=PATH` for one file's patch, or `view=patch` for all of it.\n");
            }
        }
        DiffView::Patch => match patch.as_deref() {
            Some("") | None => out.push_str("(no patch sections matched)\n"),
            Some(patch) => out.push_str(patch),
        },
        DiffView::Commits => {
            if let Some(commits) = &commits {
                push_commit_table(&mut out, commits);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LogEntry {
    commit: String,
    parents: Vec<String>,
    author: String,
    author_email: String,
    author_time: i64,
    committer_time: i64,
    description: String,
}

impl LogEntry {
    fn subject(&self) -> &str {
        self.description.lines().next().unwrap_or("").trim_end()
    }
}

impl From<history::CommitInfo> for LogEntry {
    fn from(info: history::CommitInfo) -> Self {
        Self {
            commit: info.commit_id,
            parents: info.parents,
            author: info.author_name,
            author_email: info.author_email,
            author_time: info.author_time,
            committer_time: info.committer_time,
            description: info.description,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LogAnswer {
    entries: Vec<LogEntry>,
    more: bool,
    offset: usize,
}

impl StoreAnswer for LogAnswer {}

async fn log_page(
    project_id: &str,
    resolution: &BranchResolution,
    from: Option<&str>,
    to: &str,
    path: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<LogAnswer, String> {
    let key = format!(
        "log|{project_id}|{}|{to}|{}|{limit}|{offset}",
        from.unwrap_or(""),
        path.unwrap_or("")
    );
    let store = resolution.repository_path.clone();
    let query = history::LogQuery {
        to: to.to_string(),
        from: from.map(str::to_string),
        path: path.map(str::to_string),
        limit,
        offset,
    };
    cached("log", &key, async move {
        let page = history::log(&store, query, QUERY_LIMIT)
            .await
            .map_err(|error| history_error("log", error))?;
        Ok(LogAnswer {
            entries: page.entries.into_iter().map(LogEntry::from).collect(),
            more: page.more,
            offset,
        })
    })
    .await
}

async fn read_log(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    project_id: &str,
    query: &VcsQuery,
) -> Result<String, String> {
    let to = resolve_or_default(orch, request, project_id, "to", query.to.as_deref()).await?;
    let from = match query.from.as_deref() {
        Some(from) => Some(resolve_named(orch, project_id, "from", from).await?),
        None => None,
    };
    let page = log_page(
        project_id,
        &to.resolution,
        from.as_ref().map(Endpoint::commit),
        to.commit(),
        query.path.as_deref(),
        query.limit,
        query.offset,
    )
    .await?;
    if query.json {
        return Ok(serde_json::to_string_pretty(&serde_json::json!({
            "to": to.json(),
            "from": from.as_ref().map(Endpoint::json),
            "path": query.path,
            "offset": page.offset,
            "more": page.more,
            "commits": commits_json(&page),
        }))
        .unwrap_or_default());
    }
    let mut out = format!("# Store log - {project}\n\n- To: `{}`\n", to.label);
    if let Some(from) = &from {
        out.push_str(&format!(
            "- Excluding: `{}` and its ancestors\n",
            from.label
        ));
    }
    if let Some(path) = &query.path {
        out.push_str(&format!("- Path: `{path}`\n"));
    }
    out.push('\n');
    push_commit_table(&mut out, &page);
    if page.more {
        let mut next = format!("cairn://p/{project}/vcs/log?");
        let mut keys = Vec::new();
        if let Some(to) = &query.to {
            keys.push(format!("to={to}"));
        }
        if let Some(from) = &query.from {
            keys.push(format!("from={from}"));
        }
        if let Some(path) = &query.path {
            keys.push(format!("path={path}"));
        }
        keys.push(format!("limit={}", query.limit));
        keys.push(format!("offset={}", query.offset + query.limit));
        next.push_str(&keys.join("&"));
        out.push_str(&format!("\nMore commits match. Next page: `{next}`\n"));
    }
    Ok(out)
}

fn commits_json(page: &LogAnswer) -> serde_json::Value {
    page.entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "commit": entry.commit,
                "parents": entry.parents,
                "author": entry.author,
                "authorEmail": entry.author_email,
                "authorTime": entry.author_time,
                "committerTime": entry.committer_time,
                "subject": entry.subject(),
                "description": entry.description,
            })
        })
        .collect()
}

fn date(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| seconds.to_string())
}

fn push_commit_table(out: &mut String, page: &LogAnswer) {
    if page.entries.is_empty() {
        out.push_str("No commits in this range.\n");
        return;
    }
    out.push_str("| Commit | Date (UTC) | Author | Subject |\n|---|---|---|---|\n");
    for entry in &page.entries {
        let merge = if entry.parents.len() > 1 {
            " (merge)"
        } else {
            ""
        };
        out.push_str(&format!(
            "| `{}`{merge} | {} | {} | {} |\n",
            short(Some(&entry.commit)),
            date(entry.author_time),
            escape_table(&entry.author),
            escape_table(if entry.subject().is_empty() {
                "(no description)"
            } else {
                entry.subject()
            }),
        ));
    }
}

// ---------------------------------------------------------------------------
// ancestry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AncestryAnswer {
    merge_base: Option<String>,
    from_in_to: bool,
    to_in_from: bool,
    ahead: usize,
    ahead_capped: bool,
    behind: usize,
    behind_capped: bool,
}

impl StoreAnswer for AncestryAnswer {}

fn capped(count: usize, capped: bool) -> String {
    if capped {
        format!("≥{count}")
    } else {
        count.to_string()
    }
}

async fn read_ancestry(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    project_id: &str,
    query: &VcsQuery,
) -> Result<String, String> {
    let from = resolve_from(orch, request, project_id, query.from.as_deref()).await?;
    let to = resolve_or_default(orch, request, project_id, "to", query.to.as_deref()).await?;
    let store = to.resolution.repository_path.clone();
    let (from_id, to_id) = (from.commit().to_string(), to.commit().to_string());
    let key = format!("ancestry|{project_id}|{from_id}|{to_id}");
    let answer = cached("ancestry", &key, async move {
        let ancestry = history::ancestry(&store, &from_id, &to_id, COUNT_CAP, QUERY_LIMIT)
            .await
            .map_err(|error| history_error("ancestry", error))?;
        Ok(AncestryAnswer {
            merge_base: ancestry.merge_base,
            from_in_to: ancestry.from_in_to,
            to_in_from: ancestry.to_in_from,
            ahead: ancestry.ahead.count,
            ahead_capped: ancestry.ahead.capped,
            behind: ancestry.behind.count,
            behind_capped: ancestry.behind.capped,
        })
    })
    .await?;
    if query.json {
        return Ok(serde_json::to_string_pretty(&serde_json::json!({
            "from": from.json(),
            "to": to.json(),
            "mergeBase": answer.merge_base,
            "fromInTo": answer.from_in_to,
            "toInFrom": answer.to_in_from,
            "ahead": answer.ahead,
            "aheadCapped": answer.ahead_capped,
            "behind": answer.behind,
            "behindCapped": answer.behind_capped,
        }))
        .unwrap_or_default());
    }
    let yes_no = |value: bool| if value { "yes" } else { "no" };
    Ok(format!(
        "# Store ancestry - {project}\n\n\
         - From: `{}`\n\
         - To: `{}`\n\
         - Merge-base: {}\n\
         - `to` contains `from`: {}\n\
         - `from` contains `to`: {}\n\
         - Ahead (in `to`, not in `from`): {}\n\
         - Behind (in `from`, not in `to`): {}\n",
        from.label,
        to.label,
        answer
            .merge_base
            .as_deref()
            .map(|base| format!("`{base}`"))
            .unwrap_or_else(|| "none (the histories share no commit)".to_string()),
        yes_no(answer.from_in_to),
        yes_no(answer.to_in_from),
        capped(answer.ahead, answer.ahead_capped),
        capped(answer.behind, answer.behind_capped),
    ))
}

// ---------------------------------------------------------------------------
// blame
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BlameLine {
    commit: String,
    original_line: usize,
    text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BlameAnswer {
    lines: Vec<BlameLine>,
    commits: std::collections::BTreeMap<String, LogEntry>,
}

impl StoreAnswer for BlameAnswer {}

async fn read_blame(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    project_id: &str,
    query: &VcsQuery,
) -> Result<String, String> {
    let path = query.path.clone().unwrap_or_default();
    let rev = resolve_or_default(orch, request, project_id, "rev", query.rev.as_deref()).await?;
    let store = rev.resolution.repository_path.clone();
    let commit = rev.commit().to_string();
    let key = format!("blame|{project_id}|{commit}|{path}");
    let blame_path = path.clone();
    let answer = cached("blame", &key, async move {
        let annotation =
            history::annotate(&store, &commit, &blame_path, BLAME_MAX_BYTES, QUERY_LIMIT)
                .await
                .map_err(|error| history_error("blame", error))?;
        Ok(BlameAnswer {
            lines: annotation
                .lines
                .into_iter()
                .map(|line| BlameLine {
                    commit: line.commit_id,
                    original_line: line.original_line,
                    text: line.text,
                })
                .collect(),
            commits: annotation
                .commits
                .into_iter()
                .map(|(id, info)| (id, LogEntry::from(info)))
                .collect(),
        })
    })
    .await?;
    let (start, end) = query.lines.unwrap_or((1, answer.lines.len().max(1)));
    let window = answer
        .lines
        .iter()
        .enumerate()
        .skip(start - 1)
        .take(end + 1 - start);
    if query.json {
        let lines: Vec<_> = window
            .map(|(index, line)| {
                serde_json::json!({
                    "line": index + 1,
                    "commit": line.commit,
                    "originalLine": line.original_line,
                    "text": line.text,
                })
            })
            .collect();
        let commits: serde_json::Map<_, _> = answer
            .commits
            .iter()
            .map(|(id, entry)| {
                (
                    id.clone(),
                    serde_json::json!({
                        "author": entry.author,
                        "authorEmail": entry.author_email,
                        "authorTime": entry.author_time,
                        "subject": entry.subject(),
                    }),
                )
            })
            .collect();
        return Ok(serde_json::to_string_pretty(&serde_json::json!({
            "rev": rev.json(),
            "path": path,
            "lines": lines,
            "commits": commits,
        }))
        .unwrap_or_default());
    }
    let mut out = format!(
        "# Store blame - {project} / {path}\n\n- At: `{}`\n\n",
        rev.label
    );
    if answer.lines.is_empty() {
        out.push_str("(empty file)\n");
        return Ok(out);
    }
    let width = answer.lines.len().to_string().len();
    out.push_str("```\n");
    for (index, line) in window {
        let entry = answer.commits.get(&line.commit);
        let author = entry.map(|entry| entry.author.as_str()).unwrap_or("?");
        let author: String = author.chars().take(16).collect();
        out.push_str(&format!(
            "{} {} {author:<16} {:>width$}| {}\n",
            line.commit.get(..8).unwrap_or(&line.commit),
            entry
                .map(|entry| date(entry.author_time))
                .unwrap_or_default()
                .get(..10)
                .unwrap_or_default(),
            index + 1,
            line.text,
        ));
    }
    out.push_str("```\n");
    Ok(out)
}

#[cfg(test)]
mod tests;
