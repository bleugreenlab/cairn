//! Pre-placement interception for `rg` and recursive `grep` runs.
//!
//! A search-shaped run item is a read wearing run's clothes: it inspects
//! content and changes nothing. Serving it here keeps it out of the scheduler
//! entirely — no lease, no slot admission, no cell — which is both the canon
//! invariant and, measurably, nearly all of what such a batch would otherwise
//! cost. A run batch carries roughly a second of fixed overhead (checkout
//! preparation, fingerprinting, publication) that a grep over project content
//! dwarfs by an order of magnitude.
//!
//! Three content classes, each with its own authority, none of which admits a
//! cell:
//!
//! * project content tracked at the job's head coordinate, served from the
//!   store overlay — the same truth `read ?grep=` serves. Serving from the
//!   coordinate rather than from a checkout is what retires the staleness
//!   defect of the interceptor this replaces: there is no worktree to be
//!   stale against;
//! * project paths the repository ignores, whose bytes only the live
//!   materialization has (the CAIRN-3048 contract);
//! * paths outside the project altogether (`~/.cairn/logs`, `/tmp`), where the
//!   local filesystem is the only authority and an in-process walk reads it.
//!
//! Honesty governs all three. A batch is served only when *every* item in it
//! can be reproduced exactly — flags, output format, and exit status — so
//! `sequential` and `stop_on_error` never straddle two substrates. Anything
//! else falls through to real execution silently: the agent is never told a
//! command is unsupported and never handed an approximation, which makes a
//! coverage gap cost latency and nothing else.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::hygiene::{expand_tilde, normalize_path};
use super::redact::redact_command;
use super::types::{ItemOutcome, RunSpec};
use crate::mcp::handlers::branch::BranchResolution;
use crate::mcp::handlers::search_translate::{
    translate_search_command_detailed, PostFilter, TranslatedSearch, TranslatedSearchPipeline,
};
use crate::mcp::handlers::{read, search};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// One item's translated search, with the identity needed to report on it.
struct SearchPlan {
    header: String,
    command: String,
    timeout: Option<u32>,
    search: TranslatedSearch,
    post: Vec<PostFilter>,
}

/// What one search target produced.
#[derive(Debug)]
enum Served {
    /// This target's contribution, and whether the target named a single file
    /// (which decides how paths are rendered).
    Body { body: String, was_file: bool },
    /// The item's own time budget ran out. Real execution reports the same
    /// thing, so this is a served outcome rather than a fall-through.
    TimedOut,
    /// This target cannot be reproduced exactly. The batch executes for real.
    FallThrough,
}

/// Which authority owns one search target's content. Classification happens
/// before any lease exists, because none of these classes admits a cell.
#[derive(Debug)]
enum Target {
    /// Project content addressed at the job's head coordinate.
    Project { repo_path: String },
    /// A path outside the project: the local filesystem is its only authority.
    Host { root: PathBuf },
}

/// Serve a search-shaped run batch before it reaches lease acquisition or
/// build-slot placement. `logical` is the job's resolved head coordinate, and
/// its absence marks an ambient caller, who has no project content to address.
///
/// Returning `None` means "this batch executes for real", and is the answer to
/// every shape that cannot be served faithfully.
pub(super) async fn try_run_search_batch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    logical: Option<&BranchResolution>,
    cwd: &str,
    resolved: &[(String, Result<RunSpec, String>)],
    sequential: bool,
    stop_on_error: bool,
) -> Option<Vec<ItemOutcome>> {
    let plans = plan_search_batch(resolved)?;
    let roots = ProjectRoots::new(&orch.config_dir, logical);
    // Built in a loop rather than through a closure: a closure returning a
    // borrowing future here is inferred with a single anonymous lifetime, which
    // costs every downstream caller of `handle_run` its `Send` bound.
    let mut futures = Vec::with_capacity(plans.len());
    for plan in &plans {
        futures.push(serve_item(orch, request, logical, &roots, cwd, plan));
    }
    collect_search_outcomes(futures, sequential, stop_on_error).await
}

/// Decide whether a batch can be served at all, and translate it if so.
///
/// This is where the honesty contract lives, and it is deliberately free of any
/// substrate: a batch qualifies on the shape of its items alone. Faithfulness is
/// a property of the whole batch, not of each item, because serving some items
/// here and executing the rest would break the ordering `sequential` and
/// `stop_on_error` promise. `None` means "execute this batch for real", and no
/// path out of here reports a coverage gap to the agent.
fn plan_search_batch(resolved: &[(String, Result<RunSpec, String>)]) -> Option<Vec<SearchPlan>> {
    let has_search = resolved.iter().any(|(_, spec)| {
        matches!(spec, Ok(RunSpec::Shell { command, .. }) if search_command_identity(command).is_some())
    });
    if !has_search {
        return None;
    }

    let mut plans = Vec::with_capacity(resolved.len());
    for (header, spec) in resolved {
        // A batch that mixes search with anything else executes whole.
        let Ok(RunSpec::Shell { command, timeout }) = spec.as_ref() else {
            return None;
        };
        search_command_identity(command)?;
        let TranslatedSearchPipeline { search, post } =
            match translate_search_command_detailed(command) {
                Ok(translated) => translated,
                Err(reason) => {
                    log::debug!(
                    "run search executes for real (translation gap): command={} reason={reason:?}",
                    redact_command(command)
                );
                    return None;
                }
            };
        // Only a grep projection has an equivalent to serve; `rg --files` and
        // anything else run for real.
        if !matches!(search, TranslatedSearch::Grep { .. }) {
            return None;
        }
        plans.push(SearchPlan {
            header: header.clone(),
            command: command.clone(),
            timeout: *timeout,
            search,
            post,
        });
    }
    Some(plans)
}

fn format_explicit_file_body(
    body: &str,
    indexed_name: &str,
    original_path: Option<&str>,
    include_path: bool,
) -> String {
    body.lines()
        .map(|line| {
            if line == "--" {
                return line.to_string();
            }
            let (separator, suffix) = line
                .strip_prefix(indexed_name)
                .and_then(|line| {
                    line.chars()
                        .next()
                        .filter(|separator| matches!(separator, ':' | '-'))
                        .map(|separator| (separator, &line[separator.len_utf8()..]))
                })
                .unwrap_or((':', line));
            if include_path {
                original_path
                    .map(|path| format!("{path}{separator}{suffix}"))
                    .unwrap_or_else(|| suffix.to_string())
            } else {
                suffix.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn globs_for_path(globs: &[String], path_arg: Option<&str>) -> Vec<String> {
    let Some(root) = path_arg
        .map(|path| path.trim_start_matches("./").trim_end_matches('/'))
        .filter(|path| !path.is_empty() && *path != ".")
    else {
        return globs.to_vec();
    };
    let prefix = format!("{root}/");
    globs
        .iter()
        .filter_map(|glob| {
            let (negated, pattern) = glob
                .strip_prefix('!')
                .map_or((false, glob.as_str()), |pattern| (true, pattern));
            if !pattern.contains('/') || pattern.starts_with("**/") {
                return Some(glob.clone());
            }
            if let Some(rebased) = pattern.strip_prefix(&prefix) {
                return Some(if negated {
                    format!("!{rebased}")
                } else {
                    rebased.to_string()
                });
            }
            // A slash-containing glob outside this explicit root cannot match
            // native rg's displayed `root/relative` path. An unmatched negative
            // has no effect; an unmatched positive makes this root empty.
            (!negated).then(|| "__cairn_no_matching_path__".to_string())
        })
        .collect()
}

fn apply_post_filters(mut body: String, filters: &[PostFilter]) -> String {
    for filter in filters {
        let mut lines: Vec<String> = body.lines().map(str::to_string).collect();
        match filter {
            PostFilter::Head(count) => lines.truncate(*count),
            PostFilter::Tail(count) => {
                let keep_from = lines.len().saturating_sub(*count);
                lines.drain(..keep_from);
            }
            PostFilter::Lines { start, end } => {
                lines = lines
                    .into_iter()
                    .skip(start.saturating_sub(1))
                    .take(end - start + 1)
                    .collect();
            }
            PostFilter::CountLines => {
                body = if cfg!(target_os = "macos") {
                    format!("{:>8}", lines.len())
                } else {
                    lines.len().to_string()
                };
                continue;
            }
            PostFilter::Sort => lines.sort(),
            PostFilter::Uniq => lines.dedup(),
            PostFilter::Grep {
                pattern,
                invert,
                case_insensitive,
            } => {
                let mut builder = regex::RegexBuilder::new(pattern);
                builder.case_insensitive(*case_insensitive);
                let Ok(regex) = builder.build() else {
                    body.clear();
                    continue;
                };
                lines.retain(|line| regex.is_match(line) != *invert);
            }
        }
        body = lines.join("\n");
    }
    body
}

fn search_command_identity(command: &str) -> Option<&str> {
    let stage = command.split('|').next()?.trim();
    let executable = stage.split_whitespace().next()?;
    let executable = Path::new(executable).file_name()?.to_str()?;
    matches!(executable, "rg" | "grep").then_some(executable)
}

/// Gather the batch's item outcomes, preserving input order. A single item that
/// could not be served faithfully abandons the whole batch to real execution;
/// serving is a pure read, so abandoning costs only the work already done.
async fn collect_search_outcomes<I, F>(
    futures: I,
    sequential: bool,
    stop_on_error: bool,
) -> Option<Vec<ItemOutcome>>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = Option<ItemOutcome>>,
{
    if !sequential {
        return futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect();
    }

    let mut outcomes = Vec::new();
    for future in futures {
        let outcome = future.await?;
        let stop = stop_on_error && !outcome.succeeded;
        outcomes.push(outcome);
        if stop {
            break;
        }
    }
    Some(outcomes)
}

/// Budget for a search whose item carried no explicit timeout. Run items are
/// normally given the configured default before this point; this is the floor
/// if one ever is not.
const DEFAULT_SEARCH_TIMEOUT_MS: u32 = 120_000;

/// The absolute locations that hold this project's content rather than the
/// host's. Project content is served from the coordinate and never from a
/// filesystem copy of it, so an absolute path landing in one of these is not
/// host content however it was spelled.
struct ProjectRoots {
    /// The project's own repository. An absolute path under it names project
    /// content directly and maps cleanly onto a repo-relative overlay target.
    repository: Vec<PathBuf>,
    /// Places that hold a *copy* of project content: the fleet's materialized
    /// checkouts, and the runner-owned operation store. Neither can be proven
    /// to match the job's coordinate — a materialization trails the head
    /// between refreshes — so searches there execute for real rather than risk
    /// answering from stale bytes.
    copies: Vec<PathBuf>,
}

impl ProjectRoots {
    fn new(config_dir: &Path, logical: Option<&BranchResolution>) -> Self {
        let mut repository = Vec::new();
        let mut copies = Vec::new();
        push_root(&mut copies, &config_dir.join("build-slots"));
        if let Some(resolution) = logical {
            push_root(&mut repository, &resolution.object_repository_path);
            if resolution.repository_path != resolution.object_repository_path {
                push_root(&mut copies, &resolution.repository_path);
            }
        }
        Self { repository, copies }
    }
}

/// Record a root under both its literal and its symlink-resolved form. On macOS
/// `/var` and `/private/var` name the same directory, and a prefix test that
/// knows only one of them silently misclassifies paths spelled the other way.
fn push_root(roots: &mut Vec<PathBuf>, path: &Path) {
    roots.push(path.to_path_buf());
    if let Ok(canonical) = std::fs::canonicalize(path) {
        if canonical != path {
            roots.push(canonical);
        }
    }
}

/// Resolve `path` through symlinks as far as the filesystem allows, keeping any
/// trailing components that do not exist yet.
///
/// Plain `canonicalize` gives up entirely on a missing leaf, which would leave
/// `/alias/src/absent.rs` looking like host content even when `/alias` points
/// straight into the project.
fn resolve_through_symlinks(path: &Path) -> Option<PathBuf> {
    let mut trailing = Vec::new();
    let mut current = path;
    loop {
        if let Ok(resolved) = std::fs::canonicalize(current) {
            let mut full = resolved;
            for component in trailing.iter().rev() {
                full.push(component);
            }
            return Some(full);
        }
        trailing.push(current.file_name()?);
        current = current.parent()?;
    }
}

/// Decide which authority owns one search target.
///
/// A managed job's process residence is scratch, so a relative path always names
/// project content. An absolute path is host content only when it lands outside
/// every copy of the project: inside the repository it is project content and is
/// served from the coordinate, and inside a materialization or the operation
/// store it executes for real. Spelling a project path absolutely must not be a
/// way to read bytes the coordinate does not vouch for.
///
/// Ambient callers have no logical coordinate, so everything they search is host
/// content rooted at their cwd.
fn classify_target(
    logical: Option<&BranchResolution>,
    roots: &ProjectRoots,
    cwd: &str,
    path_arg: Option<&str>,
) -> Option<Target> {
    if logical.is_none() {
        return Some(Target::Host {
            root: resolve_search_root(cwd, path_arg),
        });
    }
    let Some(path) = path_arg else {
        return Some(Target::Project {
            repo_path: String::new(),
        });
    };

    let expanded = expand_tilde(path);
    if !Path::new(&expanded).is_absolute() {
        // A path that climbs out of the project has no coordinate to be
        // addressed at, and guessing where it lands would not be faithful.
        let repo_path = path.trim_start_matches("./").trim_matches('/');
        if repo_path
            .split('/')
            .any(|component| component == ".." || component == "~")
        {
            return None;
        }
        return Some(Target::Project {
            repo_path: if repo_path == "." {
                String::new()
            } else {
                repo_path.to_string()
            },
        });
    }

    let absolute = PathBuf::from(normalize_path(&expanded, cwd));
    // A symlink can alias project content from anywhere on the filesystem, so
    // the target is tested under both the spelling it was given and the path it
    // actually resolves to. Only classification consults the resolved form; the
    // search itself keeps the original spelling, so a host result's paths read
    // the way the command wrote them.
    let resolved = resolve_through_symlinks(&absolute).filter(|path| *path != absolute);
    let spellings: Vec<&Path> = std::iter::once(absolute.as_path())
        .chain(resolved.as_deref())
        .collect();

    for spelling in &spellings {
        for root in &roots.repository {
            if let Ok(relative) = spelling.strip_prefix(root) {
                return Some(Target::Project {
                    repo_path: relative.to_str()?.trim_matches('/').to_string(),
                });
            }
        }
    }
    if spellings.iter().any(|spelling| {
        roots
            .copies
            .iter()
            .any(|root| spelling.starts_with(root) || root.starts_with(spelling))
    }) {
        return None;
    }
    Some(Target::Host { root: absolute })
}

async fn serve_item(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    logical: Option<&BranchResolution>,
    roots: &ProjectRoots,
    cwd: &str,
    plan: &SearchPlan,
) -> Option<ItemOutcome> {
    let TranslatedSearch::Grep {
        pattern,
        globs,
        output_mode,
        case_insensitive,
        before_context,
        after_context,
        show_line_numbers,
        max_per_file,
        paths,
    } = &plan.search
    else {
        return None;
    };

    let timeout_ms = plan.timeout.unwrap_or(DEFAULT_SEARCH_TIMEOUT_MS);
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(u64::from(timeout_ms));
    let query_paths: Vec<Option<&str>> = if paths.is_empty() {
        vec![None]
    } else {
        paths.iter().map(|path| Some(path.as_str())).collect()
    };
    let multiple_paths = query_paths.len() > 1;
    let deny_read = orch.sandbox_deny_read();
    let mut combined = String::new();

    for path_arg in query_paths {
        let payload = search::GrepPayload {
            pattern: pattern.clone(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some(output_mode.clone()),
            context: None,
            after_context: Some(*after_context as u32),
            before_context: Some(*before_context as u32),
            context_alias: None,
            case_insensitive: *case_insensitive,
            line_numbers: Some(*show_line_numbers),
            head_limit: None,
            offset: None,
            multiline: Some(false),
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Some(search_timeout_outcome(plan.header.clone(), timeout_ms));
        }
        let limits = search::GrepWalkLimits {
            globs: globs_for_path(globs, path_arg),
            max_per_file: *max_per_file,
            timeout: remaining,
            cancelled: Arc::new(AtomicBool::new(false)),
        };

        let served = match classify_target(logical, roots, cwd, path_arg)? {
            Target::Project { repo_path } => {
                serve_project(
                    orch,
                    request,
                    logical?,
                    &repo_path,
                    &payload,
                    output_mode,
                    *show_line_numbers,
                    limits,
                )
                .await
            }
            Target::Host { root } => {
                serve_host(
                    root,
                    &payload,
                    output_mode,
                    *show_line_numbers,
                    deny_read.clone(),
                    limits,
                )
                .await
            }
        };
        let (body, was_file) = match served {
            Served::Body { body, was_file } => (body, was_file),
            Served::TimedOut => {
                return Some(search_timeout_outcome(plan.header.clone(), timeout_ms))
            }
            Served::FallThrough => return None,
        };

        let body = if was_file {
            format_explicit_file_body(
                &body,
                path_arg
                    .and_then(|path| Path::new(path).file_name())
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                path_arg,
                multiple_paths,
            )
        } else {
            reprefix_search_body(&body, path_arg)
        };
        if !body.is_empty() {
            if !combined.is_empty() {
                if *before_context > 0 || *after_context > 0 {
                    combined.push_str("\n--\n");
                } else {
                    combined.push('\n');
                }
            }
            combined.push_str(&body);
        }
    }

    combined = apply_post_filters(combined, &plan.post);
    log::debug!(
        "run search served without placement: {}",
        redact_command(&plan.command)
    );
    Some(native_search_outcome(plan.header.clone(), combined))
}

/// Match a set of `(path, bytes)` entries, mapping engine outcomes onto the
/// three served results. A refusal — an invalid regex or glob — executes for
/// real, where ripgrep reports it in its own words.
fn grep_entries(
    files: &[(String, Vec<u8>)],
    was_file: bool,
    payload: &search::GrepPayload,
    output_mode: &str,
    show_line_numbers: bool,
    limits: &search::GrepWalkLimits,
) -> Served {
    match search::grep_search_native_entries(payload, files, output_mode, show_line_numbers, limits)
    {
        Ok(body) => Served::Body { body, was_file },
        Err(error) if error.contains("timed out") => Served::TimedOut,
        Err(error) => {
            log::debug!("run search executes for real (store grep declined): {error}");
            Served::FallThrough
        }
    }
}

/// Serve a target from content tracked at the job's head coordinate — the same
/// truth `read ?grep=` serves. Because the overlay is keyed by `(base, head)`
/// rather than by a checkout, this cannot serve stale content: there is no
/// worktree for it to be stale against, which is what retires the defect that
/// justified removing the interceptor this replaces.
///
/// `None` means nothing is tracked under this path, sending the caller on to
/// the ignored-path fallback.
fn serve_tracked(
    overlays: &read::overlay::ProjectOverlayRegistry,
    resolution: &BranchResolution,
    repo_path: &str,
    payload: &search::GrepPayload,
    output_mode: &str,
    show_line_numbers: bool,
    mut limits: search::GrepWalkLimits,
) -> Option<Served> {
    let mut files = match overlays.files(
        &resolution.project_id,
        &resolution.object_repository_path,
        &resolution.default_commit_id,
        &resolution.commit_id,
        repo_path,
        &read::object_read::serve_limits(),
    ) {
        Ok(files) => files,
        Err(error) => {
            log::debug!("run search executes for real (overlay declined): {error}");
            return Some(Served::FallThrough);
        }
    };

    let trimmed = repo_path.trim_matches('/');
    if files.is_empty() && !trimmed.is_empty() {
        return None;
    }

    // The overlay strips the prefix from every path it returns except when the
    // prefix *is* the path — exactly the single-file case. Relabelling that
    // entry to its file name reproduces what a walk rooted at the file's parent
    // produces, so the output formatting applies to both sources unchanged.
    let was_file = !trimmed.is_empty() && files.len() == 1 && files[0].0 == trimmed;
    if was_file {
        let Some(name) = Path::new(trimmed)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            return Some(Served::FallThrough);
        };
        files[0].0 = name.to_string();
        limits.globs.push(name.to_string());
    }

    Some(grep_entries(
        &files,
        was_file,
        payload,
        output_mode,
        show_line_numbers,
        &limits,
    ))
}

/// Serve a project target, preferring tracked content at the head coordinate
/// and falling back to the live materialization for paths the repository
/// ignores.
#[allow(clippy::too_many_arguments)]
async fn serve_project(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    resolution: &BranchResolution,
    repo_path: &str,
    payload: &search::GrepPayload,
    output_mode: &str,
    show_line_numbers: bool,
    limits: search::GrepWalkLimits,
) -> Served {
    let overlays = orch.project_overlays.clone();
    let owned_resolution = resolution.clone();
    let owned_repo_path = repo_path.to_string();
    let owned_payload = payload.clone();
    let owned_output_mode = output_mode.to_string();
    let owned_limits = limits.clone();
    let tracked = tokio::task::spawn_blocking(move || {
        serve_tracked(
            &overlays,
            &owned_resolution,
            &owned_repo_path,
            &owned_payload,
            &owned_output_mode,
            show_line_numbers,
            owned_limits,
        )
    })
    .await;

    let tracked = match tracked {
        Ok(tracked) => tracked,
        Err(error) => {
            log::debug!("run search executes for real (overlay read failed): {error}");
            return Served::FallThrough;
        }
    };
    if let Some(served) = tracked {
        return served;
    }

    // Nothing tracked here: either the path is ignored, and only the live
    // materialization holds its bytes, or it is absent at this coordinate,
    // where ripgrep's "no such file" belongs to real execution.
    //
    // The materialization read contract addresses one path and returns its
    // bytes, so only an ignored *file* can be served this way. A recursive
    // search over an ignored directory has nothing to enumerate it with — its
    // contents are by definition absent from the object store — so it executes
    // for real. Closing that would take a new executor operation that walks a
    // materialization, which is a new capability rather than a repair here.
    let trimmed = repo_path.trim_matches('/');
    let Some(bytes) = ignored_target_bytes(orch, request, resolution, trimmed).await else {
        return Served::FallThrough;
    };
    let Some(name) = Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return Served::FallThrough;
    };
    let mut limits = limits;
    limits.globs.push(name.to_string());
    grep_entries(
        &[(name.to_string(), bytes)],
        true,
        payload,
        output_mode,
        show_line_numbers,
        &limits,
    )
}

/// The bytes of an ignored project path, read from the live materialization per
/// the CAIRN-3048 contract. `None` means this path is not ignored, or its
/// materialization could not answer — either way the batch executes for real.
async fn ignored_target_bytes(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    resolution: &BranchResolution,
    repo_path: &str,
) -> Option<Vec<u8>> {
    let service = read::object_read::ObjectReadService::new(
        resolution.object_repository_path.clone(),
        resolution.commit_id.clone(),
        repo_path.to_string(),
    )
    .ok()?;
    if !service.is_ignored_path(repo_path).ok()? {
        return None;
    }
    read::file::read_ignored_path(
        orch,
        request,
        &resolution.project_id,
        repo_path,
        &resolution.commit_id,
    )
    .await
    .ok()
}

/// Serve a target the project does not own — a host log directory, a temp file —
/// by walking the local filesystem in process. This is the class that most
/// obviously must not schedule anything: searching host files has nothing to do
/// with the project tree, yet placement would materialize a whole checkout
/// first. The sandbox's deny list still governs what the walk may open.
async fn serve_host(
    root: PathBuf,
    payload: &search::GrepPayload,
    output_mode: &str,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
    mut limits: search::GrepWalkLimits,
) -> Served {
    let mut search_root = root;
    let was_file = search_root.is_file();
    if was_file {
        let Some(file_name) = search_root
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
        else {
            return Served::FallThrough;
        };
        limits.globs.push(file_name);
        let Some(parent) = search_root.parent() else {
            return Served::FallThrough;
        };
        search_root = parent.to_path_buf();
    }

    let remaining = limits.timeout;
    let cancelled = limits.cancelled.clone();
    let payload = payload.clone();
    let output_mode = output_mode.to_string();
    let walk = tokio::time::timeout(
        remaining,
        tokio::task::spawn_blocking(move || {
            search::grep_search_native(
                payload,
                &search_root,
                &output_mode,
                show_line_numbers,
                deny_read,
                limits,
            )
        }),
    )
    .await;
    match walk {
        Ok(Ok(Ok(body))) => Served::Body { body, was_file },
        Ok(Ok(Err(error))) if error.contains("timed out") => Served::TimedOut,
        Ok(Ok(Err(error))) => {
            log::debug!("run search executes for real (walk declined): {error}");
            Served::FallThrough
        }
        Ok(Err(error)) => {
            log::debug!("run search executes for real (walk failed): {error}");
            Served::FallThrough
        }
        Err(_) => {
            // Stop the scan rather than detaching a thread that keeps reading.
            cancelled.store(true, Ordering::Relaxed);
            Served::TimedOut
        }
    }
}

fn search_timeout_outcome(header: String, timeout_ms: u32) -> ItemOutcome {
    ItemOutcome::failed(header, format!("Command timed out after {timeout_ms}ms"))
}

fn resolve_search_root(cwd: &str, path: Option<&str>) -> PathBuf {
    match path {
        Some(path) => {
            let expanded = expand_tilde(path);
            let path = Path::new(&expanded);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                Path::new(cwd).join(path)
            }
        }
        None => PathBuf::from(cwd),
    }
}

fn reprefix_search_body(body: &str, path_arg: Option<&str>) -> String {
    let Some(arg) = path_arg else {
        return body.to_string();
    };
    let trimmed = arg.trim_end_matches('/');
    let prefix = if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("{trimmed}/")
    };
    body.lines()
        .map(|line| {
            if line == "--" {
                line.to_string()
            } else {
                format!("{prefix}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn native_search_outcome(header: String, stdout: String) -> ItemOutcome {
    if stdout.is_empty() {
        ItemOutcome::failed(header, "Exit code: 1")
    } else {
        ItemOutcome {
            header,
            body: format!("{stdout}\n"),
            succeeded: true,
            suspended: false,
            images: Vec::new(),
            tracked_modifications: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell(command: &str) -> (String, Result<RunSpec, String>) {
        (
            command.to_string(),
            Ok(RunSpec::Shell {
                command: command.to_string(),
                timeout: Some(30_000),
            }),
        )
    }

    fn resolution() -> BranchResolution {
        BranchResolution {
            project_id: "project".to_string(),
            repository_path: PathBuf::from("/repo"),
            object_repository_path: PathBuf::from("/repo"),
            rev: "branch".to_string(),
            commit_id: "head".to_string(),
            default_commit_id: "base".to_string(),
        }
    }

    fn grep_payload(pattern: &str) -> search::GrepPayload {
        search::GrepPayload {
            pattern: pattern.to_string(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some("content".to_string()),
            context: None,
            after_context: Some(0),
            before_context: Some(0),
            context_alias: None,
            case_insensitive: None,
            line_numbers: Some(true),
            head_limit: None,
            offset: None,
            multiline: Some(false),
        }
    }

    fn walk_limits() -> search::GrepWalkLimits {
        search::GrepWalkLimits {
            globs: Vec::new(),
            max_per_file: None,
            timeout: std::time::Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A repository whose head carries an edit its base does not.
    fn advanced_repository() -> (tempfile::TempDir, BranchResolution) {
        use cairn_codec::testutil::{commit_all, init_repo, write_file};

        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "src/lib.rs", b"fn stale() { let marker = 1; }\n");
        write_file(repo, ".gitignore", b"generated/\n");
        let base = commit_all(repo, "base");
        write_file(repo, "src/lib.rs", b"fn fresh() { let marker = 2; }\n");
        let head = commit_all(repo, "head advances");

        let resolution = BranchResolution {
            project_id: "project".to_string(),
            repository_path: repo.to_path_buf(),
            object_repository_path: repo.to_path_buf(),
            rev: "branch".to_string(),
            commit_id: head,
            default_commit_id: base,
        };
        (dir, resolution)
    }

    fn tracked_body(resolution: &BranchResolution, repo_path: &str) -> Option<String> {
        let overlays = read::overlay::ProjectOverlayRegistry::default();
        match serve_tracked(
            &overlays,
            resolution,
            repo_path,
            &grep_payload("marker"),
            "content",
            true,
            walk_limits(),
        )? {
            Served::Body { body, .. } => Some(body),
            other => panic!("expected a served body, got {other:?}"),
        }
    }

    /// The trap the previous interceptor fell into: it could answer from a
    /// checkout that no longer matched the coordinate. Serving from the overlay
    /// must see an edit the instant it lands at head, because the overlay is
    /// keyed by the coordinate rather than by any materialized tree.
    #[test]
    fn a_search_serves_head_content_not_the_base() {
        let (_dir, resolution) = advanced_repository();
        let body = tracked_body(&resolution, "src").expect("src is tracked at head");
        assert!(
            body.contains("marker = 2"),
            "the search must see the edit at head, got: {body}"
        );
        assert!(
            !body.contains("marker = 1"),
            "the search must not serve the base content, got: {body}"
        );
    }

    /// The same trap reached through an absolute path, which is the spelling
    /// that would otherwise route around the coordinate entirely. The working
    /// tree on disk is a copy the coordinate does not vouch for; classification
    /// must refuse to treat it as host content in the first place.
    #[test]
    fn an_absolute_project_path_also_serves_head_content() {
        let (dir, resolution) = advanced_repository();
        let roots = ProjectRoots::new(Path::new("/cairn-home"), Some(&resolution));
        let absolute = dir.path().join("src").display().to_string();

        let repo_path =
            match classify_target(Some(&resolution), &roots, "/scratch", Some(&absolute)) {
                Some(Target::Project { repo_path }) => repo_path,
                other => {
                    panic!("an absolute project path must address the coordinate, got {other:?}")
                }
            };
        assert_eq!(repo_path, "src");

        let body = tracked_body(&resolution, &repo_path).expect("src is tracked at head");
        assert!(
            body.contains("marker = 2") && !body.contains("marker = 1"),
            "an absolute project path must serve head content: {body}"
        );
    }

    /// A symlink can alias project content from anywhere on the filesystem, so
    /// resolving only the known roots is not enough: with `/tmp/link -> /repo`,
    /// `/tmp/link/src` prefix-matches no root and would be read straight off
    /// the working tree. Classification has to resolve the target too.
    #[cfg(unix)]
    #[test]
    fn an_aliased_absolute_path_still_addresses_the_coordinate() {
        let (dir, resolution) = advanced_repository();
        let elsewhere = tempfile::tempdir().unwrap();
        let alias = elsewhere.path().join("project-link");
        std::os::unix::fs::symlink(dir.path(), &alias).unwrap();

        let roots = ProjectRoots::new(Path::new("/cairn-home"), Some(&resolution));
        let classify = |path: PathBuf| {
            classify_target(
                Some(&resolution),
                &roots,
                "/scratch",
                Some(&path.display().to_string()),
            )
        };

        let repo_path = match classify(alias.join("src/lib.rs")) {
            Some(Target::Project { repo_path }) => repo_path,
            other => {
                panic!("a symlink into the project must address the coordinate, got {other:?}")
            }
        };
        assert_eq!(repo_path, "src/lib.rs");

        let body = tracked_body(&resolution, &repo_path).expect("lib.rs is tracked at head");
        assert!(
            body.contains("marker = 2") && !body.contains("marker = 1"),
            "an aliased path must serve head content, not working-tree bytes: {body}"
        );

        // A leaf that does not exist yet resolves through its parent, so naming
        // a missing file cannot turn an alias back into host content.
        match classify(alias.join("src/absent.rs")) {
            Some(Target::Project { repo_path }) => assert_eq!(repo_path, "src/absent.rs"),
            other => {
                panic!("a missing leaf under an alias must stay project content, got {other:?}")
            }
        }

        // An alias that genuinely points outside the project stays host content.
        let outside = elsewhere.path().join("outside-link");
        std::os::unix::fs::symlink(elsewhere.path(), &outside).unwrap();
        match classify(outside.join("notes.txt")) {
            Some(Target::Host { root }) => assert!(root.ends_with("outside-link/notes.txt")),
            other => panic!("an alias outside the project must reach the walk, got {other:?}"),
        }
    }

    /// A directory target renders prefix-relative paths, exactly as a walk
    /// rooted there would; a file target renders as its own name, which is what
    /// lets the shared output formatting treat both content sources alike.
    #[test]
    fn tracked_paths_render_as_the_walk_would() {
        let (_dir, resolution) = advanced_repository();
        assert!(tracked_body(&resolution, "src")
            .expect("tracked directory")
            .starts_with("lib.rs:1:"));
        assert!(tracked_body(&resolution, "src/lib.rs")
            .expect("tracked file")
            .starts_with("lib.rs:1:"));
        assert!(tracked_body(&resolution, "")
            .expect("whole tree")
            .starts_with("src/lib.rs:1:"));
    }

    /// A path with nothing tracked under it is not an empty result: it is a
    /// question the coordinate cannot answer, so it hands off to the ignored
    /// path fallback rather than reporting "no matches".
    #[test]
    fn untracked_paths_leave_the_tracked_substrate() {
        let (_dir, resolution) = advanced_repository();
        assert!(tracked_body(&resolution, "generated").is_none());
        assert!(tracked_body(&resolution, "no/such/path").is_none());
    }

    #[test]
    fn pure_search_batches_are_admitted() {
        for command in [
            "rg needle",
            "rg -n needle src",
            "grep -r needle src include",
            "rg needle | head -5",
        ] {
            assert!(
                plan_search_batch(&[shell(command)]).is_some(),
                "{command} is a plain search and should be served"
            );
        }
        assert_eq!(
            plan_search_batch(&[shell("rg one"), shell("rg two")])
                .expect("both items are searches")
                .len(),
            2
        );
    }

    /// Every shape that cannot be reproduced exactly executes for real, and
    /// none of them reports a coverage gap to the agent. Falling through costs
    /// latency; answering with an approximation would cost correctness.
    #[test]
    fn unservable_shapes_fall_through_without_comment() {
        let unservable: Vec<Vec<(String, Result<RunSpec, String>)>> = vec![
            // Nothing search-shaped at all.
            vec![shell("cargo build")],
            // A search mixed with a build: serving half the batch would break
            // the ordering `sequential`/`stop_on_error` promise.
            vec![shell("rg needle"), shell("cargo build")],
            vec![shell("cargo build"), shell("rg needle")],
            // A search mixed with a non-shell item.
            vec![
                shell("rg needle"),
                (
                    "script".to_string(),
                    Ok(RunSpec::Script {
                        program: "bun".to_string(),
                        args: vec!["x.ts".to_string()],
                        timeout: None,
                        stdin: None,
                    }),
                ),
            ],
            // A pipeline stage no post-filter can represent.
            vec![shell("rg needle | awk '{print $1}'")],
            // `rg --files` has no grep projection to serve.
            vec![shell("rg --files")],
            // A write disguised as a search stage.
            vec![shell("rg needle > out.txt")],
        ];
        for batch in unservable {
            let headers: Vec<&str> = batch.iter().map(|(header, _)| header.as_str()).collect();
            assert!(
                plan_search_batch(&batch).is_none(),
                "{headers:?} cannot be served faithfully and must execute for real"
            );
        }
    }

    fn project_roots(resolution: Option<&BranchResolution>) -> ProjectRoots {
        ProjectRoots::new(Path::new("/cairn-home"), resolution)
    }

    /// A managed job's process residence is scratch, so a relative path names
    /// project content at the head coordinate. This is the routing decision the
    /// whole design rests on.
    #[test]
    fn managed_relative_targets_address_the_coordinate() {
        let resolution = resolution();
        let logical = Some(&resolution);
        let roots = project_roots(logical);
        let project = |path: Option<&str>| match classify_target(logical, &roots, "/scratch", path)
        {
            Some(Target::Project { repo_path }) => repo_path,
            other => panic!("{path:?} should address project content, got {other:?}"),
        };
        assert_eq!(project(None), "");
        assert_eq!(project(Some(".")), "");
        assert_eq!(project(Some("src")), "src");
        assert_eq!(project(Some("./src/")), "src");
        assert_eq!(project(Some("src/lib.rs")), "src/lib.rs");

        // A path that climbs out of the project has no coordinate to be served
        // at, so the batch executes for real rather than guessing.
        assert!(classify_target(logical, &roots, "/scratch", Some("../elsewhere")).is_none());
        assert!(classify_target(logical, &roots, "/scratch", Some("src/../../etc")).is_none());
    }

    /// Spelling a project path absolutely must not become a way to read bytes
    /// the coordinate does not vouch for. Inside the repository an absolute
    /// path is still project content; inside a materialization or the operation
    /// store it is a checkout whose generation nothing can be proven to match,
    /// so it executes for real.
    #[test]
    fn absolute_targets_are_classified_by_where_they_land() {
        let resolution = resolution();
        let logical = Some(&resolution);
        let roots = project_roots(logical);
        let classify = |path: &str| classify_target(logical, &roots, "/scratch", Some(path));

        // Absolute into the project repository: project content at the head.
        match classify("/repo/src") {
            Some(Target::Project { repo_path }) => assert_eq!(repo_path, "src"),
            other => panic!("an absolute project path must address the coordinate, got {other:?}"),
        }
        match classify("/repo/src/../src/lib.rs") {
            Some(Target::Project { repo_path }) => assert_eq!(repo_path, "src/lib.rs"),
            other => panic!("a normalized project path must address the coordinate, got {other:?}"),
        }
        match classify("/repo") {
            Some(Target::Project { repo_path }) => assert_eq!(repo_path, ""),
            other => panic!("the repository root must address the coordinate, got {other:?}"),
        }

        // Absolute into a materialized checkout: no coordinate vouches for it.
        assert!(
            classify("/cairn-home/build-slots/cairn/slot-3/src").is_none(),
            "a materialized checkout must execute for real, never be served as host bytes"
        );
        // The operation store is a copy too when it is distinct from the repo.
        let jj = BranchResolution {
            repository_path: PathBuf::from("/cairn-home/stores/project"),
            ..resolution.clone()
        };
        let jj_roots = project_roots(Some(&jj));
        assert!(
            classify_target(
                Some(&jj),
                &jj_roots,
                "/scratch",
                Some("/cairn-home/stores/project/x")
            )
            .is_none(),
            "the operation store is not addressable as project content"
        );

        // The specimen shape: host logs are nobody's project content, and must
        // reach the filesystem walk rather than a coordinate that lacks them.
        // They live under the Cairn home but outside any checkout.
        match classify("/cairn-home/logs") {
            Some(Target::Host { root }) => assert_eq!(root, PathBuf::from("/cairn-home/logs")),
            other => panic!("host logs must reach the filesystem walk, got {other:?}"),
        }
        match classify("/tmp/logs") {
            Some(Target::Host { root }) => assert_eq!(root, PathBuf::from("/tmp/logs")),
            other => panic!("a temp path must reach the filesystem walk, got {other:?}"),
        }
        match classify("~/.cairn/logs") {
            Some(Target::Host { root }) => assert!(root.ends_with(".cairn/logs")),
            other => panic!("a tilde host path must reach the filesystem walk, got {other:?}"),
        }
        // A sibling whose name merely starts with a root's name is not inside it.
        match classify("/repository-notes") {
            Some(Target::Host { root }) => assert_eq!(root, PathBuf::from("/repository-notes")),
            other => panic!("prefix matching must respect path components, got {other:?}"),
        }
    }

    /// An ambient caller has no logical coordinate, so every target it names is
    /// host content rooted at its cwd — unchanged from before the router.
    #[test]
    fn ambient_targets_stay_on_the_filesystem() {
        let roots = project_roots(None);
        match classify_target(None, &roots, "/work", Some("src")) {
            Some(Target::Host { root }) => assert_eq!(root, PathBuf::from("/work/src")),
            other => panic!("ambient search should walk the filesystem, got {other:?}"),
        }
        match classify_target(None, &roots, "/work", None) {
            Some(Target::Host { root }) => assert_eq!(root, PathBuf::from("/work")),
            other => panic!("ambient search should walk the filesystem, got {other:?}"),
        }
    }

    #[test]
    fn reprefix_preserves_context_separator() {
        assert_eq!(
            reprefix_search_body("a.txt:1:hit\n--\nb.txt:2:hit", Some("src")),
            "src/a.txt:1:hit\n--\nsrc/b.txt:2:hit"
        );
    }

    #[test]
    fn no_match_uses_native_exit_one_shape() {
        let outcome = native_search_outcome("rg absent".into(), String::new());
        assert!(!outcome.succeeded);
        assert_eq!(outcome.body, "Exit code: 1");
    }

    #[test]
    fn rebases_slash_globs_against_explicit_roots() {
        assert_eq!(
            globs_for_path(
                &["*.rs".into(), "!target/**".into(), "!src/target/**".into()],
                Some("src")
            ),
            vec!["*.rs", "!target/**"]
        );
    }

    #[test]
    fn explicit_file_format_omits_single_path_and_restores_multiple_path() {
        assert_eq!(
            format_explicit_file_body("README.md:2:needle", "README.md", Some("README.md"), false),
            "2:needle"
        );
        assert_eq!(
            format_explicit_file_body("a.txt:needle", "a.txt", Some("dir/a.txt"), true),
            "dir/a.txt:needle"
        );
        assert_eq!(
            format_explicit_file_body("a.txt-1-before", "a.txt", Some("dir/a.txt"), true),
            "dir/a.txt-1-before"
        );
    }

    #[tokio::test]
    async fn parallel_search_outcomes_overlap_and_preserve_input_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Barrier;

        let barrier = Arc::new(Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let futures = (0..2).map(|index| {
            let barrier = barrier.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(now, Ordering::SeqCst);
                barrier.wait().await;
                active.fetch_sub(1, Ordering::SeqCst);
                Some(ItemOutcome {
                    header: format!("search-{index}"),
                    body: format!("result-{index}"),
                    succeeded: true,
                    suspended: false,
                    images: Vec::new(),
                    tracked_modifications: None,
                })
            }
        });

        let outcomes = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            collect_search_outcomes(futures, false, true),
        )
        .await
        .expect("parallel virtual searches must overlap rather than deadlock serially")
        .expect("every item served");

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.header.as_str())
                .collect::<Vec<_>>(),
            vec!["search-0", "search-1"]
        );
    }
}
