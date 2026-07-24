//! Pre-placement interception for `rg` and recursive `grep` runs.
//!
//! Command identity is the routing contract: search-shaped run items are reads
//! and therefore never enter build-slot placement. Two engines answer behind
//! that contract — the resident warm index when it can prove completeness, and
//! the in-process ripgrep-equivalent walk whenever it cannot — so an index
//! decline is a routing choice, never a user-visible failure.
//!
//! The one thing that still fails hard is a *translation* gap: an invocation
//! whose flags neither engine can represent (`Indexed search does not yet
//! support flag or stage '…'`). Answering those with a silently different
//! search would be worse than saying so.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::hygiene::expand_tilde;
use super::redact::redact_command;
use super::types::{ItemOutcome, RunSpec};
use crate::mcp::handlers::search_translate::{
    translate_search_command_detailed, PostFilter, TranslatedSearch, TranslatedSearchPipeline,
};
use crate::mcp::handlers::{search, RunContext};
use crate::orchestrator::Orchestrator;

/// Serve a search process batch before it reaches build-slot placement.
/// Mixed search/build batches fail explicitly here because admitting the batch
/// would incorrectly turn its search items into build work.
pub(super) async fn try_run_search_batch(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    resolved: &[(String, Result<RunSpec, String>)],
    branch_scoped: bool,
    sequential: bool,
    stop_on_error: bool,
) -> Option<Vec<ItemOutcome>> {
    // Managed job batches must execute search inside the executor cell
    // materialized at the logical head. The host warm index is keyed to the
    // legacy worktree and is no longer a project-content authority.
    if run_context
        .and_then(|context| context.worktree_path.as_ref())
        .is_some()
    {
        return None;
    }
    let worktree = run_context
        .and_then(|context| context.worktree_path.as_ref())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(cwd));
    let has_search = resolved.iter().any(|(_, spec)| {
        matches!(spec, Ok(RunSpec::Shell { command, .. }) if search_command_identity(command).is_some())
    });
    if !has_search {
        return None;
    }

    if branch_scoped {
        return Some(vec![ItemOutcome::failed(
            "indexed search".to_string(),
            "Branch-scoped indexed searches are not yet supported; no build slot was requested",
        )]);
    }

    let mut searches = Vec::with_capacity(resolved.len());
    for (header, spec) in resolved {
        let Ok(RunSpec::Shell { command, timeout }) = spec.as_ref() else {
            return Some(vec![ItemOutcome::failed(
                header.clone(),
                "Search reads cannot be mixed with non-search run items",
            )]);
        };
        if search_command_identity(command).is_none() {
            return Some(vec![ItemOutcome::failed(
                header.clone(),
                "Search reads cannot be mixed with build commands in one run batch",
            )]);
        }
        let TranslatedSearchPipeline { search, post } =
            match translate_search_command_detailed(command) {
                Ok(translated) => translated,
                Err(reason) => {
                    log::warn!(
                        "run indexed-search coverage gap: command={} reason={reason:?}",
                        redact_command(command)
                    );
                    let detail = match reason {
                        cairn_common::protocol::WarmSearchDeclineReason::UnsupportedFlag(flag) => {
                            format!("Indexed search does not yet support flag or stage '{flag}'")
                        }
                        other => {
                            format!("Indexed search cannot represent this invocation: {other:?}")
                        }
                    };
                    searches.push((header.clone(), command.clone(), *timeout, Err(detail)));
                    continue;
                }
            };
        searches.push((
            header.clone(),
            command.clone(),
            *timeout,
            Ok((search, post)),
        ));
    }

    let futures = searches
        .into_iter()
        .map(|(header, command, timeout, translated)| {
            let worktree = worktree.clone();
            async move {
                match translated {
                    Ok((search, post)) => {
                        serve_search(
                            orch, cwd, &worktree, header, &command, timeout, search, &post,
                        )
                        .await
                    }
                    Err(error) => ItemOutcome::failed(header, error),
                }
            }
        });
    Some(collect_search_outcomes(futures, sequential, stop_on_error).await)
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

async fn collect_search_outcomes<I, F>(
    futures: I,
    sequential: bool,
    stop_on_error: bool,
) -> Vec<ItemOutcome>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = ItemOutcome>,
{
    if !sequential {
        return futures_util::future::join_all(futures).await;
    }

    let mut outcomes = Vec::new();
    for future in futures {
        let outcome = future.await;
        let stop = stop_on_error && !outcome.succeeded;
        outcomes.push(outcome);
        if stop {
            break;
        }
    }
    outcomes
}

#[allow(clippy::too_many_arguments)]
async fn serve_search(
    orch: &Orchestrator,
    cwd: &str,
    worktree: &Path,
    header: String,
    command: &str,
    timeout: Option<u32>,
    translated: TranslatedSearch,
    post: &[PostFilter],
) -> ItemOutcome {
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
    } = translated
    else {
        return ItemOutcome::failed(header, "Unsupported search projection");
    };

    let timeout_ms = timeout.unwrap_or(30_000).min(30_000);
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64);
    let query_paths: Vec<Option<String>> = if paths.is_empty() {
        vec![None]
    } else {
        paths.into_iter().map(Some).collect()
    };
    let deny_read = orch.sandbox_deny_read();
    let mut combined = String::new();
    let multiple_paths = query_paths.len() > 1;

    for path_arg in &query_paths {
        let mut search_root = resolve_search_root(cwd, path_arg.as_deref());
        if !target_is_within_worktree(&search_root, worktree) {
            return ItemOutcome::failed(
                header,
                "Search targets must exist within the node worktree",
            );
        }
        let target_was_file = search_root.is_file();
        let mut query_globs = globs_for_path(&globs, path_arg.as_deref());
        if target_was_file {
            let Some(file_name) = search_root.file_name().and_then(|name| name.to_str()) else {
                return ItemOutcome::failed(header, "Search target has no UTF-8 file name");
            };
            query_globs.push(file_name.to_string());
            let Some(parent) = search_root.parent() else {
                return ItemOutcome::failed(header, "Search target has no parent directory");
            };
            search_root = parent.to_path_buf();
        }
        let payload = search::GrepPayload {
            pattern: pattern.clone(),
            path: None,
            glob: None,
            file_type: None,
            output_mode: Some(output_mode.clone()),
            context: None,
            after_context: Some(after_context as u32),
            before_context: Some(before_context as u32),
            context_alias: None,
            case_insensitive,
            line_numbers: Some(show_line_numbers),
            head_limit: None,
            offset: None,
            multiline: Some(false),
        };
        let mode = output_mode.clone();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return search_timeout_outcome(header, timeout_ms);
        }

        // Ask the warm index first; it creates and warms the resident index for
        // this worktree as a side effect, so a cold tree pays nothing here and
        // the next query hits warm.
        let indexed = tokio::time::timeout(
            remaining,
            search::try_worktree_index_grep_for_worktree(
                orch,
                search::IndexGrepRequest {
                    worktree_raw: worktree,
                    search_path: &search_root,
                    payload: &payload,
                    output_mode: &mode,
                    show_line_numbers,
                    deny_read: &deny_read,
                    native_output: true,
                    globs: Some(&query_globs),
                    max_per_file,
                    uncovered_timeout: remaining,
                },
            ),
        )
        .await;
        let body = match indexed {
            Ok(Some(body)) => body,
            Err(_) => return search_timeout_outcome(header, timeout_ms),
            // Every index decline routes to the walk, which is complete by
            // construction and needs no warm-up.
            Ok(None) => {
                let request = WalkRequest {
                    payload: payload.clone(),
                    search_root: search_root.clone(),
                    output_mode: mode.clone(),
                    show_line_numbers,
                    deny_read: deny_read.clone(),
                    globs: query_globs.clone(),
                    max_per_file,
                };
                match walk_search(request, deadline).await {
                    Ok(body) => body,
                    Err(WalkFailure::TimedOut) => {
                        return search_timeout_outcome(header, timeout_ms)
                    }
                    Err(WalkFailure::Failed(error)) => return ItemOutcome::failed(header, error),
                }
            }
        };
        let body = if target_was_file {
            format_explicit_file_body(
                &body,
                search_root
                    .join(
                        path_arg
                            .as_deref()
                            .and_then(|path| Path::new(path).file_name())
                            .unwrap_or_default(),
                    )
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                path_arg.as_deref(),
                multiple_paths,
            )
        } else {
            let prefix_path = if !target_was_file && search_root.is_dir() {
                path_arg.as_deref()
            } else {
                None
            };
            reprefix_search_body(&body, prefix_path)
        };
        if !body.is_empty() {
            if !combined.is_empty() {
                if before_context > 0 || after_context > 0 {
                    combined.push_str("\n--\n");
                } else {
                    combined.push('\n');
                }
            }
            combined.push_str(&body);
        }
    }

    combined = apply_post_filters(combined, post);
    log::debug!(
        "run virtual search served before placement: {}",
        redact_command(command)
    );
    native_search_outcome(header, combined)
}

/// One translated search, resolved against the filesystem walk rather than the
/// index. Bundled so the fallback reads as a single hand-off.
struct WalkRequest {
    payload: search::GrepPayload,
    search_root: PathBuf,
    output_mode: String,
    show_line_numbers: bool,
    deny_read: Vec<PathBuf>,
    globs: Vec<String>,
    max_per_file: Option<usize>,
}

enum WalkFailure {
    /// The caller's deadline elapsed; reported as rg's timeout, not a defect.
    TimedOut,
    /// The walk itself refused the query — an invalid regex or glob, carrying
    /// its canonical message (rg exits 2 on the same input).
    Failed(String),
}

/// Serve one translated search from the in-process ripgrep-equivalent walk.
/// The walk owns the same deadline the caller does and shares a cancel flag
/// with it, so an outer timeout actually stops the scan instead of detaching a
/// blocking thread that keeps reading.
async fn walk_search(
    request: WalkRequest,
    deadline: tokio::time::Instant,
) -> Result<String, WalkFailure> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(WalkFailure::TimedOut);
    }
    let WalkRequest {
        payload,
        search_root,
        output_mode,
        show_line_numbers,
        deny_read,
        globs,
        max_per_file,
    } = request;
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let walk = tokio::time::timeout(
        remaining,
        tokio::task::spawn_blocking(move || {
            search::grep_search_native(
                payload,
                &search_root,
                &output_mode,
                show_line_numbers,
                deny_read,
                search::GrepWalkLimits {
                    globs,
                    max_per_file,
                    timeout: remaining,
                    cancelled: flag,
                },
            )
        }),
    )
    .await;
    match walk {
        Ok(Ok(Ok(body))) => Ok(body),
        Ok(Ok(Err(error))) if error.contains("timed out") => Err(WalkFailure::TimedOut),
        Ok(Ok(Err(error))) => Err(WalkFailure::Failed(error)),
        Ok(Err(error)) => Err(WalkFailure::Failed(format!("search walk failed: {error}"))),
        Err(_) => {
            cancelled.store(true, Ordering::Relaxed);
            Err(WalkFailure::TimedOut)
        }
    }
}

fn search_timeout_outcome(header: String, timeout_ms: u32) -> ItemOutcome {
    ItemOutcome::failed(header, format!("Command timed out after {timeout_ms}ms"))
}

fn target_is_within_worktree(search_root: &Path, worktree: &Path) -> bool {
    let Ok(search_root) = std::fs::canonicalize(search_root) else {
        return false;
    };
    let Ok(worktree) = std::fs::canonicalize(worktree) else {
        return false;
    };
    search_root.starts_with(worktree)
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
            promoted_terminal: None,
            tracked_modifications: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                ItemOutcome {
                    header: format!("search-{index}"),
                    body: format!("result-{index}"),
                    succeeded: true,
                    suspended: false,
                    images: Vec::new(),
                    promoted_terminal: None,
                    tracked_modifications: None,
                }
            }
        });

        let outcomes = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            collect_search_outcomes(futures, false, true),
        )
        .await
        .expect("parallel virtual searches must overlap rather than deadlock serially");

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
