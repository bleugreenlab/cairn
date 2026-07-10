//! Runner-owned raw warm-search service used by the `rg`/`grep` PATH shim.
//!
//! This boundary receives one Bash-expanded invocation and either returns exact
//! native-style bytes/status or declines so the helper can exec the real binary.

use std::path::{Path, PathBuf};

use cairn_common::protocol::{
    CallbackRequest, WarmSearchDeclineReason, WarmSearchRequest, WarmSearchStdin,
};

use super::search_translate::{translate_search_invocation, TranslatedSearch};
use crate::mcp::handlers::{run_context, search};
use crate::orchestrator::Orchestrator;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarmSearchDecision {
    Serve {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit_code: i32,
    },
    PassThrough {
        reason: WarmSearchDeclineReason,
    },
}

impl WarmSearchDecision {
    fn pass(reason: WarmSearchDeclineReason) -> Self {
        Self::PassThrough { reason }
    }
}

pub async fn execute_warm_search(
    orch: &Orchestrator,
    request: &WarmSearchRequest,
) -> WarmSearchDecision {
    if matches!(
        request.stdin,
        WarmSearchStdin::Pipe | WarmSearchStdin::File | WarmSearchStdin::Other
    ) {
        return WarmSearchDecision::pass(WarmSearchDeclineReason::StdinInput);
    }

    let callback = CallbackRequest {
        cwd: request.cwd.clone(),
        run_id: Some(request.run_id.clone()),
        ..Default::default()
    };
    let run = match run_context::lookup_run_routed(&orch.db, &callback).await {
        Ok((run, _)) => run,
        Err(_) => return WarmSearchDecision::pass(WarmSearchDeclineReason::RunNotFound),
    };
    let Some(worktree_path) = run.worktree_path else {
        return WarmSearchDecision::pass(WarmSearchDeclineReason::MissingWorktree);
    };
    let translated = match translate_search_invocation(&request.program, &request.argv) {
        Ok(translated) => translated,
        Err(reason) => return WarmSearchDecision::pass(reason),
    };

    // The first serving rollout is intentionally limited to content searches.
    // `rg --files` and count/file-list modes need independent native parity
    // matrices before they can safely return bytes from this endpoint.
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
        return WarmSearchDecision::pass(WarmSearchDeclineReason::ParityUncertain);
    };
    if output_mode != "content" {
        return WarmSearchDecision::pass(WarmSearchDeclineReason::ParityUncertain);
    }

    let worktree = PathBuf::from(worktree_path);
    let deny_read = orch.sandbox_deny_read();
    let query_paths: Vec<Option<String>> = if paths.is_empty() {
        vec![None]
    } else {
        paths.into_iter().map(Some).collect()
    };
    let mut combined = String::new();

    for path_arg in &query_paths {
        let search_root = resolve_search_root(&request.cwd, path_arg.as_deref());
        let Some(scope) = search::resolve_worktree_scope(orch, &search_root, &worktree) else {
            return WarmSearchDecision::pass(WarmSearchDeclineReason::ScopeDecline);
        };
        let params = crate::worktree_search::WorktreeGrepParams {
            pattern: pattern.clone(),
            subdir: scope.subdir,
            globs: globs.clone(),
            max_per_file,
            output_mode: output_mode.clone(),
            case_insensitive,
            before_context,
            after_context,
            show_line_numbers,
        };
        let deny = deny_read.clone();
        let body = match tokio::task::spawn_blocking(move || scope.search.try_grep(&params, &deny))
            .await
        {
            Ok(Some(body)) => body,
            _ => return WarmSearchDecision::pass(WarmSearchDeclineReason::ColdOrIncompleteIndex),
        };
        let body = reprefix_search_body(&body, path_arg.as_deref());
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

    let exit_code = if combined.is_empty() { 1 } else { 0 };
    if !combined.is_empty() {
        combined.push('\n');
    }
    WarmSearchDecision::Serve {
        stdout: combined.into_bytes(),
        stderr: Vec::new(),
        exit_code,
    }
}

fn resolve_search_root(cwd: &str, path: Option<&str>) -> PathBuf {
    match path {
        Some(path) if Path::new(path).is_absolute() => PathBuf::from(path),
        Some(path) => Path::new(cwd).join(path),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_expanded_runtime_path_without_touching_context_separator() {
        assert_eq!(
            reprefix_search_body("a.rs:1:hit\n--\nb.rs:2:hit", Some("nested")),
            "nested/a.rs:1:hit\n--\nnested/b.rs:2:hit"
        );
    }

    #[test]
    fn multiple_context_roots_are_separated_like_native_rg() {
        let mut combined = String::from("a/x-before\na/x:needle\na/x-after");
        let body = "b/y-before\nb/y:needle\nb/y-after";
        if !combined.is_empty() {
            combined.push_str("\n--\n");
        }
        combined.push_str(body);
        assert_eq!(
            combined,
            "a/x-before\na/x:needle\na/x-after\n--\nb/y-before\nb/y:needle\nb/y-after"
        );
    }
}
