//! Commit-graph questions answered in-process from the jj index: ancestry
//! between two commits, history over a range, and line attribution.
//!
//! Every function here takes already-resolved hexadecimal commit ids (callers
//! resolve branch names, node URIs and aliases through their own resolver
//! first) and loads the store through the crate's one read loader. Nothing here
//! spawns `git` or `jj`, and nothing takes Cairn's store lock: the questions are
//! about immutable commits, so the answer at one operation head is the answer
//! at every later one.
//!
//! jj-lib futures are not `Send`, so each query runs to completion on a
//! dedicated thread, the same pattern [`crate::merge_base`] uses.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use jj_lib::annotate::FileAnnotator;
use jj_lib::backend::{CommitId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::fileset::FilesetExpression;
use jj_lib::object_id::ObjectId as _;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::ResolvedRevsetExpression;

use crate::{best_common_ancestor, load_repo_for_history};

/// Why a history query produced no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryError {
    /// The id is not a commit id, or the store's index does not hold it.
    UnknownCommit(String),
    /// The path is not a valid repository-relative path.
    InvalidPath(String),
    /// The path names nothing at the commit.
    PathAbsent {
        commit: String,
        path: String,
    },
    /// The question has an answer this surface declines to compute (a binary
    /// or oversized file, a directory, a conflicted path).
    Refused(String),
    /// The query outran its deadline. Callers report the limit rather than a
    /// partial answer.
    TimedOut {
        limit: Duration,
    },
    Repository(String),
}

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommit(id) => write!(f, "commit `{id}` is not in the store"),
            Self::InvalidPath(path) => write!(f, "`{path}` is not a repository-relative path"),
            Self::PathAbsent { commit, path } => {
                write!(f, "`{path}` does not exist at commit {commit}")
            }
            Self::Refused(reason) => f.write_str(reason),
            Self::TimedOut { limit } => {
                write!(
                    f,
                    "the query did not finish within {}s",
                    limit.as_secs_f32()
                )
            }
            Self::Repository(diagnostic) => f.write_str(diagnostic),
        }
    }
}

impl std::error::Error for HistoryError {}

fn repository_error(error: impl fmt::Display) -> HistoryError {
    HistoryError::Repository(error.to_string())
}

/// A count that stops at a cap instead of walking an arbitrarily long range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CappedCount {
    pub count: usize,
    /// The walk stopped at `count`; the true value is at least that.
    pub capped: bool,
}

impl fmt::Display for CappedCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.capped {
            write!(f, "≥{}", self.count)
        } else {
            write!(f, "{}", self.count)
        }
    }
}

/// How two commits relate in the commit graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ancestry {
    pub from: String,
    pub to: String,
    /// The fork point, by the same rule [`crate::merge_base`] uses. `None` when
    /// the histories share no real commit.
    pub merge_base: Option<String>,
    /// `from` is `to` or one of its ancestors: `to` already contains `from`.
    pub from_in_to: bool,
    /// `to` is `from` or one of its ancestors.
    pub to_in_from: bool,
    /// Commits reachable from `to` and not from `from` (`from..to`).
    pub ahead: CappedCount,
    /// Commits reachable from `from` and not from `to` (`to..from`).
    pub behind: CappedCount,
}

/// One commit's metadata, as history surfaces render it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitInfo {
    pub commit_id: String,
    pub parents: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    /// Author timestamp, in seconds since the Unix epoch.
    pub author_time: i64,
    /// Committer timestamp, in seconds since the Unix epoch.
    pub committer_time: i64,
    pub description: String,
}

impl CommitInfo {
    fn of(commit: &Commit) -> Self {
        Self {
            commit_id: commit.id().hex(),
            parents: commit.parent_ids().iter().map(|id| id.hex()).collect(),
            author_name: commit.author().name.clone(),
            author_email: commit.author().email.clone(),
            author_time: commit.author().timestamp.timestamp.0.div_euclid(1000),
            committer_time: commit.committer().timestamp.timestamp.0.div_euclid(1000),
            description: commit.description().to_string(),
        }
    }

    /// The first line of the description.
    pub fn subject(&self) -> &str {
        self.description.lines().next().unwrap_or("").trim_end()
    }
}

/// What a history walk should cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogQuery {
    /// Newest commit of the range; it and its ancestors are candidates.
    pub to: String,
    /// When set, this commit and its ancestors are excluded (`from..to`).
    pub from: Option<String>,
    /// Only commits that change a path at or under this repository-relative
    /// path. Empty or `.` means no filter.
    pub path: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

/// One page of a history walk, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub entries: Vec<CommitInfo>,
    /// More commits match beyond this page.
    pub more: bool,
}

/// One line of an attributed file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotatedLine {
    /// The commit that introduced the line.
    pub commit_id: String,
    /// 1-based line number of the line in that commit's version of the file.
    pub original_line: usize,
    /// The line, without its trailing newline.
    pub text: String,
}

/// A file's line-by-line attribution at one commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotation {
    pub lines: Vec<AnnotatedLine>,
    /// Metadata for every commit some line is attributed to.
    pub commits: HashMap<String, CommitInfo>,
}

/// Run one query against the store at its operation head on a dedicated
/// thread, so the returned future stays `Send` while jj-lib's do not.
async fn with_repo<T, F, Fut>(repository_path: &Path, query: F) -> Result<T, HistoryError>
where
    T: Send + 'static,
    F: FnOnce(Box<dyn Repo>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, HistoryError>>,
{
    let repository_path: PathBuf = repository_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(repository_error)?
            .block_on(async move {
                let repo = load_repo_for_history(&repository_path)
                    .await
                    .map_err(|error| HistoryError::Repository(format!("{error:?}")))?;
                query(repo).await
            })
    })
    .await
    .map_err(repository_error)?
}

/// Parse `hex` and confirm the index holds it. `DefaultIndex::is_ancestor`
/// unwraps its position lookup, so an id that skipped this guard would panic
/// rather than fail.
fn indexed_commit(repo: &dyn Repo, hex: &str) -> Result<CommitId, HistoryError> {
    let hex = hex.trim();
    let id =
        CommitId::try_from_hex(hex).ok_or_else(|| HistoryError::UnknownCommit(hex.to_string()))?;
    if !repo.index().has_id(&id).map_err(repository_error)? {
        return Err(HistoryError::UnknownCommit(hex.to_string()));
    }
    Ok(id)
}

fn repo_path(path: &str) -> Result<Option<RepoPathBuf>, HistoryError> {
    let trimmed = path.trim().trim_matches('/');
    if trimmed.is_empty() || trimmed == "." {
        return Ok(None);
    }
    RepoPathBuf::from_internal_string(trimmed)
        .map(Some)
        .map_err(|_| HistoryError::InvalidPath(path.to_string()))
}

fn check_deadline(started: Instant, limit: Duration) -> Result<(), HistoryError> {
    if started.elapsed() >= limit {
        Err(HistoryError::TimedOut { limit })
    } else {
        Ok(())
    }
}

/// How `from` and `to` relate: their fork point, whether either contains the
/// other, and how many commits each has that the other lacks, counted up to
/// `count_cap`.
pub async fn ancestry(
    repository_path: &Path,
    from: &str,
    to: &str,
    count_cap: usize,
    limit: Duration,
) -> Result<Ancestry, HistoryError> {
    let from = from.to_string();
    let to = to.to_string();
    with_repo(repository_path, move |repo| async move {
        let started = Instant::now();
        let from_id = indexed_commit(repo.as_ref(), &from)?;
        let to_id = indexed_commit(repo.as_ref(), &to)?;
        let index = repo.index();
        let from_in_to = index
            .is_ancestor(&from_id, &to_id)
            .map_err(repository_error)?;
        let to_in_from = index
            .is_ancestor(&to_id, &from_id)
            .map_err(repository_error)?;
        let merge_base = best_common_ancestor(repo.as_ref(), &from_id, &to_id)
            .map_err(HistoryError::Repository)?;
        let ahead =
            count_exclusive(repo.as_ref(), &to_id, &from_id, count_cap, started, limit).await?;
        let behind =
            count_exclusive(repo.as_ref(), &from_id, &to_id, count_cap, started, limit).await?;
        Ok(Ancestry {
            from: from_id.hex(),
            to: to_id.hex(),
            merge_base: merge_base.map(|id| id.hex()),
            from_in_to,
            to_in_from,
            ahead,
            behind,
        })
    })
    .await
}

/// Whether `descendant` contains `ancestor`: it is that commit or one of its
/// descendants. The same answer [`ancestry`] reports as `from_in_to`, without
/// the fork-point and ahead/behind walks a yes/no question has no use for.
/// Unknown coordinates are errors, never "false", and the repository view is
/// not merged or published.
///
/// This is the one indexed ancestry query (it absorbed `is_ancestor`). It
/// passes `repo.as_ref()` because [`with_repo`] hands the closure a
/// `Box<dyn Repo>`, and `&Box<dyn Repo>` does not coerce to `&dyn Repo`.
pub async fn contains(
    repository_path: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, HistoryError> {
    let ancestor = ancestor.to_string();
    let descendant = descendant.to_string();
    with_repo(repository_path, move |repo| async move {
        let ancestor = indexed_commit(repo.as_ref(), &ancestor)?;
        let descendant = indexed_commit(repo.as_ref(), &descendant)?;
        repo.index()
            .is_ancestor(&ancestor, &descendant)
            .map_err(repository_error)
    })
    .await
}

/// Count `::include ~ ::exclude`, stopping at `cap`.
async fn count_exclusive(
    repo: &dyn Repo,
    include: &CommitId,
    exclude: &CommitId,
    cap: usize,
    started: Instant,
    limit: Duration,
) -> Result<CappedCount, HistoryError> {
    let expression = ResolvedRevsetExpression::commit(include.clone())
        .ancestors()
        .minus(&ResolvedRevsetExpression::commit(exclude.clone()).ancestors());
    let revset = expression.evaluate(repo).map_err(repository_error)?;
    let mut stream = revset.stream();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.map_err(repository_error)?;
        if count == cap {
            return Ok(CappedCount {
                count,
                capped: true,
            });
        }
        count += 1;
        if count % 1024 == 0 {
            check_deadline(started, limit)?;
        }
    }
    Ok(CappedCount {
        count,
        capped: false,
    })
}

/// The commits in `query`'s range, newest first, following every parent (not
/// first-parent only), optionally restricted to commits that change a path.
pub async fn log(
    repository_path: &Path,
    query: LogQuery,
    limit: Duration,
) -> Result<LogPage, HistoryError> {
    with_repo(repository_path, move |repo| async move {
        log_in_repo(repo.as_ref(), query, Instant::now(), limit, false)
            .await
            .map(|(page, _)| page)
    })
    .await
}

async fn log_in_repo(
    repo: &dyn Repo,
    query: LogQuery,
    started: Instant,
    limit: Duration,
    count_all: bool,
) -> Result<(LogPage, usize), HistoryError> {
    let to = indexed_commit(repo, &query.to)?;
    let mut expression = ResolvedRevsetExpression::commit(to).ancestors();
    if let Some(from) = query.from.as_deref() {
        let from = indexed_commit(repo, from)?;
        expression = expression.minus(&ResolvedRevsetExpression::commit(from).ancestors());
    }
    // jj's virtual root is an ancestor of everything and is not a commit
    // anyone made.
    expression = expression.minus(&ResolvedRevsetExpression::root());
    // The path filter is tested one commit at a time against the walk,
    // not folded into the revset. A filtered revset (and its
    // `containing_fn`) evaluates lazily inside one step, so a path that
    // rarely or never matches would diff trees across the whole history
    // with no point at which the deadline could be consulted. The test is
    // the one jj's `files()` applies: the commit's tree against its
    // parents' merged tree, restricted to the path.
    let matcher = query
        .path
        .as_deref()
        .map(repo_path)
        .transpose()?
        .flatten()
        .map(|path| FilesetExpression::prefix_path(path).to_matcher());
    let revset = expression.evaluate(repo).map_err(repository_error)?;
    let mut stream = revset.stream();
    let mut skipped = 0;
    let mut entries = Vec::new();
    let mut more = false;
    let mut total = 0;
    while let Some(item) = stream.next().await {
        check_deadline(started, limit)?;
        let id = item.map_err(repository_error)?;
        let commit = repo.store().get_commit(&id).map_err(repository_error)?;
        if let Some(matcher) = &matcher {
            let parent_tree = commit.parent_tree(repo).await.map_err(repository_error)?;
            let tree = commit.tree();
            let mut changes = parent_tree.diff_stream(&tree, matcher.as_ref());
            if changes.next().await.is_none() {
                continue;
            }
        }
        total += 1;
        if skipped < query.offset {
            skipped += 1;
            continue;
        }
        if entries.len() == query.limit {
            more = true;
            if count_all {
                continue;
            }
            break;
        }
        entries.push(CommitInfo::of(&commit));
    }
    Ok((LogPage { entries, more }, total))
}

/// Immutable evidence about the files named by a plan, not a prose notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPathChanges {
    pub written_commit: CommitInfo,
    pub prepared_start_commit: String,
    /// Commits in written..prepared, including when no path resolves.
    pub range_commit_count: usize,
    /// Deduplicated files from paths resolved at the written commit, including
    /// changed files added beneath those resolved directories.
    pub named_files: Vec<String>,
    pub changed_files: Vec<PlannedFileChanges>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFileChanges {
    pub path: String,
    /// Newest first, at most ten per file and forty across the report.
    pub commits: Vec<CommitInfo>,
    pub omitted_commits: usize,
}

fn planned_tokens(markdown: &str) -> BTreeSet<String> {
    let mut candidates = Vec::new();
    for (index, part) in markdown.split('`').enumerate() {
        if index % 2 == 1 {
            candidates.push(part);
        }
    }
    for (index, _) in markdown.match_indices("file:") {
        candidates.push(
            markdown[index..]
                .split(|c: char| {
                    c.is_whitespace() || matches!(c, '`' | ')' | ']' | '>' | '"' | '\'')
                })
                .next()
                .unwrap_or(""),
        );
    }
    candidates
        .into_iter()
        .filter_map(normalize_planned_path)
        .collect()
}

fn normalize_planned_path(token: &str) -> Option<String> {
    let mut path = token
        .trim()
        .strip_prefix("file:")
        .unwrap_or(token.trim())
        .trim_end_matches([',', ';', '.'])
        .trim_end_matches('/');
    // Coordinates are suffixes, not part of the immutable object-tree path.
    for _ in 0..2 {
        if let Some((base, number)) = path.rsplit_once(':') {
            if !number.is_empty() && number.bytes().all(|c| c.is_ascii_digit()) {
                path = base;
            }
        }
    }
    let path = path.strip_prefix("./").unwrap_or(path);
    if path.is_empty()
        || path.contains(char::is_whitespace)
        || path.contains(':')
        || path.starts_with('/')
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
        || !(token.trim().starts_with("file:")
            || token.trim().ends_with('/')
            || path.contains('/')
            || path.contains('.'))
    {
        return None;
    }
    Some(path.to_string())
}

/// Resolve exact paths before suffixes; ambiguous suffixes name nothing.
fn resolve_planned_paths(tokens: &BTreeSet<String>, files: &BTreeSet<String>) -> BTreeSet<String> {
    let mut paths = files.clone();
    for file in files {
        let mut parent = file.as_str();
        while let Some((prefix, _)) = parent.rsplit_once('/') {
            paths.insert(prefix.to_string());
            parent = prefix;
        }
    }
    let mut named = BTreeSet::new();
    for token in tokens {
        let resolved = if paths.contains(token) {
            Some(token.as_str())
        } else {
            let suffix = format!("/{token}");
            let mut matches = paths.iter().filter(|path| path.ends_with(&suffix));
            let first = matches.next();
            if matches.next().is_some() {
                None
            } else {
                first.map(String::as_str)
            }
        };
        if let Some(path) = resolved {
            // Preserve directory identity for additions after the written commit.
            named.insert(if files.contains(path) {
                path.to_string()
            } else {
                format!("{path}/")
            });
        }
    }
    named
}

fn expand_planned_paths(paths: &BTreeSet<String>, files: &BTreeSet<String>) -> Vec<String> {
    files
        .iter()
        .filter(|file| {
            paths
                .iter()
                .any(|path| *file == path || (path.ends_with('/') && file.starts_with(path)))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
fn resolve_planned_files(tokens: &BTreeSet<String>, files: &BTreeSet<String>) -> Vec<String> {
    expand_planned_paths(&resolve_planned_paths(tokens, files), files)
}

/// Compare the plan's immutable tree with the prepared start tree. No checkout,
/// branch lookup, working-copy read, or subprocess participates in the answer.
/// The deadline covers tree enumeration, differences and all history walks.
pub async fn planned_path_changes(
    repository_path: &Path,
    written_at_commit: &str,
    prepared_start_commit: &str,
    markdown: &str,
    limit: Duration,
) -> Result<PlannedPathChanges, HistoryError> {
    let written = written_at_commit.to_string();
    let prepared = prepared_start_commit.to_string();
    let tokens = planned_tokens(markdown);
    with_repo(repository_path, move |repo| async move {
        let started = Instant::now();
        check_deadline(started, limit)?;
        let from_id = indexed_commit(repo.as_ref(), &written)?;
        let to_id = indexed_commit(repo.as_ref(), &prepared)?;
        let from = repo
            .store()
            .get_commit(&from_id)
            .map_err(repository_error)?;
        let to = repo.store().get_commit(&to_id).map_err(repository_error)?;
        let root = repo
            .store()
            .get_commit(repo.store().root_commit_id())
            .map_err(repository_error)?;
        let from_tree = from.tree();
        let root_tree = root.tree();
        let mut files = BTreeSet::new();
        let mut entries = root_tree.diff_stream(&from_tree, &jj_lib::matchers::EverythingMatcher);
        while let Some(entry) = entries.next().await {
            check_deadline(started, limit)?;
            entry.values.map_err(repository_error)?;
            files.insert(entry.path.as_internal_file_string().to_string());
        }
        let named_paths = resolve_planned_paths(&tokens, &files);
        let to_tree = to.tree();
        let mut changes = from_tree.diff_stream(&to_tree, &jj_lib::matchers::EverythingMatcher);
        let mut changed = BTreeSet::new();
        while let Some(entry) = changes.next().await {
            check_deadline(started, limit)?;
            entry.values.map_err(repository_error)?;
            changed.insert(entry.path.as_internal_file_string().to_string());
        }
        // Resolve at the written tree, then include additions under named directories.
        files.extend(changed.iter().cloned());
        let named_files = expand_planned_paths(&named_paths, &files);
        let range_commit_count =
            count_exclusive(repo.as_ref(), &to_id, &from_id, usize::MAX, started, limit)
                .await?
                .count;
        let mut changed_files = Vec::new();
        let mut remaining = 40;
        for path in named_files.iter().filter(|path| changed.contains(*path)) {
            let (page, count) = log_in_repo(
                repo.as_ref(),
                LogQuery {
                    to: to_id.hex(),
                    from: Some(from_id.hex()),
                    path: Some(path.clone()),
                    limit: remaining.min(10),
                    offset: 0,
                },
                started,
                limit,
                true,
            )
            .await?;
            remaining -= page.entries.len();
            changed_files.push(PlannedFileChanges {
                path: path.clone(),
                omitted_commits: count - page.entries.len(),
                commits: page.entries,
            });
        }
        check_deadline(started, limit)?;
        Ok(PlannedPathChanges {
            written_commit: CommitInfo::of(&from),
            prepared_start_commit: to_id.hex(),
            range_commit_count,
            named_files,
            changed_files,
        })
    })
    .await
}

/// How many generations one annotation pass walks before the deadline is
/// consulted again. jj's annotator resumes from its pending commits, so the
/// walk is cut into bounded passes rather than run as one unbounded one.
const ANNOTATE_PASS_GENERATIONS: u64 = 512;

/// Attribute each line of `path` at `commit` to the commit that introduced it.
///
/// Refuses directories, conflicted paths, files over `max_bytes`, and files
/// that look binary. A walk that outruns `limit` fails with
/// [`HistoryError::TimedOut`] rather than returning a partial attribution.
pub async fn annotate(
    repository_path: &Path,
    commit: &str,
    path: &str,
    max_bytes: usize,
    limit: Duration,
) -> Result<Annotation, HistoryError> {
    let commit = commit.to_string();
    let path = path.to_string();
    with_repo(repository_path, move |repo| async move {
        let started = Instant::now();
        let id = indexed_commit(repo.as_ref(), &commit)?;
        let file_path = repo_path(&path)?.ok_or_else(|| HistoryError::InvalidPath(path.clone()))?;
        let starting = repo.store().get_commit(&id).map_err(repository_error)?;
        let value = starting
            .tree()
            .path_value(&file_path)
            .await
            .map_err(repository_error)?;
        match value.as_resolved() {
            Some(None) => {
                return Err(HistoryError::PathAbsent {
                    commit: id.hex(),
                    path,
                })
            }
            Some(Some(TreeValue::File { .. })) => {}
            Some(Some(TreeValue::Tree(_))) => {
                return Err(HistoryError::Refused(format!(
                    "`{path}` is a directory; blame takes a file"
                )))
            }
            Some(Some(_)) => {
                return Err(HistoryError::Refused(format!(
                    "`{path}` is not a regular file"
                )))
            }
            None => {
                return Err(HistoryError::Refused(format!(
                    "`{path}` is conflicted at this commit"
                )))
            }
        }
        let mut annotator = FileAnnotator::from_commit(&starting, &file_path)
            .await
            .map_err(repository_error)?;
        {
            let starting_annotation = annotator.to_annotation();
            let text: &[u8] = starting_annotation.text().as_ref();
            if text.len() > max_bytes {
                return Err(HistoryError::Refused(format!(
                    "`{path}` is {} bytes; blame serves files up to {max_bytes} bytes",
                    text.len()
                )));
            }
            if text.iter().take(8000).any(|byte| *byte == 0) {
                return Err(HistoryError::Refused(format!(
                    "`{path}` looks binary; blame serves text files"
                )));
            }
        }
        loop {
            let pending: Vec<CommitId> = annotator.pending_commits().cloned().collect();
            if pending.is_empty() {
                break;
            }
            check_deadline(started, limit)?;
            let domain = ResolvedRevsetExpression::commits(pending)
                .ancestors_range(0..ANNOTATE_PASS_GENERATIONS);
            annotator
                .compute(repo.as_ref(), &domain)
                .await
                .map_err(repository_error)?;
        }
        let annotation = annotator.to_annotation();
        let mut lines = Vec::new();
        let mut commits = HashMap::new();
        for (origin, line) in annotation.line_origins() {
            // A fully walked history attributes every line; an `Err` origin
            // would name where an incomplete walk stopped.
            let origin = origin.unwrap_or_else(|stopped| stopped);
            let commit_id = origin.commit_id.hex();
            if !commits.contains_key(&commit_id) {
                let commit = repo
                    .store()
                    .get_commit(&origin.commit_id)
                    .map_err(repository_error)?;
                commits.insert(commit_id.clone(), CommitInfo::of(&commit));
            }
            let bytes: &[u8] = line.as_ref();
            let text = String::from_utf8_lossy(bytes);
            lines.push(AnnotatedLine {
                commit_id,
                original_line: origin.line_number + 1,
                text: text
                    .strip_suffix('\n')
                    .map(|line| line.strip_suffix('\r').unwrap_or(line))
                    .unwrap_or(&text)
                    .to_string(),
            });
        }
        Ok(Annotation { lines, commits })
    })
    .await
}

#[cfg(test)]
mod tests;
