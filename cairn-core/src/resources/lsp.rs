//! LSP resource reads: node-scoped and project-scoped semantic code navigation.
//!
//! The node-scoped resource (`cairn://p/PROJ/N/EXEC/NODE/lsp[/<symbol>]`) drives
//! language servers over the node's worktree; the project-scoped fallback
//! (`cairn://p/PROJ/lsp[/<symbol>]`) roots at the project's main checkout. Both
//! return the engine's rendered body; the read view layer adds the canonical
//! `=== uri ===` header.

use std::path::{Path, PathBuf};

use cairn_common::query::QueryParam;

use crate::lsp::render::Rendered;
use crate::lsp::LspOp;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

use super::common::{connect_and_find_node_job, connect_for_read, find_query_value};

/// Query parameters the lsp resources accept (the path segment carries the symbol).
const LSP_KEYS: &[&str] = &["op", "search", "in", "at"];

enum OpSelection {
    Overview,
    Op(LspOp),
    Diagnostics,
}

pub(crate) async fn read_node_lsp(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    symbol: Option<&str>,
    params: &[QueryParam],
) -> String {
    let (_, job) =
        match connect_and_find_node_job(&orch.db.local, project, number, exec_seq, node_id).await {
            Ok(pair) => pair,
            Err(error) => return error,
        };
    let job = match crate::jobs::queries::get_job(&orch.db.local, &job.id).await {
        Ok(job) => job,
        Err(error) => return format!("Error loading node job: {error}"),
    };
    let worktree = match job.worktree_path.as_deref() {
        Some(path) if Path::new(path).exists() => PathBuf::from(path),
        _ => {
            return "instance unavailable — worktree for this node is gone (it may have been cleaned up)"
                .to_string()
        }
    };
    dispatch_lsp(orch, &worktree, symbol, params)
}

pub(crate) async fn read_project_lsp(
    orch: &Orchestrator,
    project: &str,
    symbol: Option<&str>,
    params: &[QueryParam],
) -> String {
    let repo_path = {
        let conn = match connect_for_read(&orch.db.local).await {
            Ok(conn) => conn,
            Err(error) => return error,
        };
        match project_repo_path(&conn, project).await {
            Ok(path) => path,
            Err(error) => return error,
        }
    };
    let worktree = match repo_path {
        Some(path) if Path::new(&path).exists() => PathBuf::from(path),
        _ => {
            return "instance unavailable — the project's main checkout is unavailable".to_string()
        }
    };
    dispatch_lsp(orch, &worktree, symbol, params)
}

async fn project_repo_path(
    conn: &turso::Connection,
    project_key: &str,
) -> Result<Option<String>, String> {
    let key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "SELECT repo_path FROM projects WHERE key = ?1 LIMIT 1",
            (key.as_str(),),
        )
        .await
        .map_err(|error| format!("Failed to load project: {error}"))?;
    match rows
        .next()
        .await
        .map_err(|error| format!("Failed to load project: {error}"))?
    {
        Some(row) => row
            .opt_text(0)
            .map_err(|error| format!("Failed to decode project: {error}")),
        None => Err(format!("No project found with key '{key}'")),
    }
}

fn dispatch_lsp(
    orch: &Orchestrator,
    worktree: &Path,
    symbol: Option<&str>,
    params: &[QueryParam],
) -> String {
    if let Some(unsupported) = params
        .iter()
        .find(|param| !LSP_KEYS.contains(&param.key.as_str()))
    {
        return format!(
            "Unsupported query parameter '{}' for lsp resources (supported: {})",
            unsupported.key,
            LSP_KEYS.join(", ")
        );
    }

    let search = find_query_value(params, "search");
    let in_hint = find_query_value(params, "in").map(PathBuf::from);
    let at = match find_query_value(params, "at") {
        Some(raw) => match parse_at(raw, worktree) {
            Ok(parsed) => Some(parsed),
            Err(error) => return error,
        },
        None => None,
    };
    let op = match find_query_value(params, "op") {
        None | Some("") => OpSelection::Overview,
        Some("diagnostics") => OpSelection::Diagnostics,
        Some(name) => match LspOp::from_name(name) {
            Some(op) => OpSelection::Op(op),
            None => {
                return format!(
                    "Unknown lsp op '{name}' (definition|references|hover|implementations|callers|subtypes|diagnostics)"
                )
            }
        },
    };

    let rendered: Rendered = if let Some(query) = search {
        orch.lsp_search(worktree, query, in_hint.as_deref())
    } else if let Some(symbol) = symbol {
        let op = match op {
            OpSelection::Overview => None,
            OpSelection::Op(op) => Some(op),
            OpSelection::Diagnostics => {
                return "diagnostics is not a per-symbol op; drop the trailing symbol".to_string()
            }
        };
        orch.lsp_named(worktree, op, symbol, in_hint.as_deref(), at)
    } else if matches!(op, OpSelection::Diagnostics) {
        orch.lsp_diagnostics(worktree, in_hint.as_deref())
    } else if let Some((file, position)) = at {
        let op = match op {
            OpSelection::Op(op) => Some(op),
            _ => None,
        };
        orch.lsp_named(worktree, op, "", in_hint.as_deref(), Some((file, position)))
    } else {
        return lsp_usage();
    };

    rendered.body
}

/// Parse a `?at=file:PATH:LINE[:COL]` target into an absolute path and a 0-based
/// LSP position. 1-based line/column input (grep-style) maps to 0-based. Shared
/// with the `rename` change mode's `symbol_at` payload so position parsing has
/// one implementation.
pub(crate) fn parse_at(raw: &str, worktree: &Path) -> Result<(PathBuf, (u32, u32)), String> {
    let body = raw.strip_prefix("file:").unwrap_or(raw);
    let parts: Vec<&str> = body.rsplitn(3, ':').collect();
    let (path_str, line_str, col_str) = match parts.as_slice() {
        [col, line, path] if is_num(col) && is_num(line) => (*path, *line, Some(*col)),
        [line, path] if is_num(line) => (*path, *line, None),
        _ => {
            return Err(format!(
                "invalid 'at' target '{raw}'; expected file:PATH:LINE[:COL]"
            ))
        }
    };
    let line: u32 = line_str
        .parse()
        .map_err(|_| format!("invalid line in 'at' target '{raw}'"))?;
    let col: u32 = match col_str {
        Some(col) => col
            .parse()
            .map_err(|_| format!("invalid column in 'at' target '{raw}'"))?,
        None => 1,
    };
    let position = (line.saturating_sub(1), col.saturating_sub(1));
    let path = if Path::new(path_str).is_absolute() {
        PathBuf::from(path_str)
    } else {
        worktree.join(path_str)
    };
    Ok((path, position))
}

fn is_num(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn lsp_usage() -> String {
    "LSP semantic navigation. Append a symbol (`/build_widget`) with an op \
     (`?op=references`), discover with `?search=NAME`, resolve by position with \
     `?at=file:PATH:LINE[:COL]`, or read `?op=diagnostics`. Ops: \
     definition|references|hover|implementations|callers|subtypes|diagnostics \
     (absent op = overview)."
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_at_maps_to_zero_based_position() {
        let worktree = Path::new("/wt");
        assert_eq!(
            parse_at("file:src/lib.rs:15:7", worktree).unwrap(),
            (PathBuf::from("/wt/src/lib.rs"), (14, 6))
        );
        // Column defaults to 1 -> 0 when omitted.
        assert_eq!(
            parse_at("file:src/lib.rs:15", worktree).unwrap(),
            (PathBuf::from("/wt/src/lib.rs"), (14, 0))
        );
        // Absolute paths pass through unchanged.
        assert_eq!(
            parse_at("file:/abs/x.rs:2:3", worktree).unwrap(),
            (PathBuf::from("/abs/x.rs"), (1, 2))
        );
    }

    #[test]
    fn parse_at_rejects_malformed_targets() {
        assert!(parse_at("file:src/lib.rs", Path::new("/wt")).is_err());
        assert!(parse_at("garbage", Path::new("/wt")).is_err());
    }
}
