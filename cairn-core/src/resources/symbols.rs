//! Symbol-navigation resource reads: node-scoped and project-scoped structural
//! code navigation, backed by the in-process ast-grep engine (`crate::symbols`).
//!
//! The node-scoped resource (`cairn://p/PROJ/N/EXEC/NODE/symbols[/<symbol>]`)
//! queries the node's worktree; the project-scoped fallback
//! (`cairn://p/PROJ/symbols[/<symbol>]`) roots at the project's main checkout.
//! Both parse files on demand — no language server, no index, no per-worktree
//! warmup. The read view layer adds the canonical `=== uri ===` header.

use std::path::{Path, PathBuf};

use cairn_common::query::QueryParam;

use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use crate::symbols::nav::{query as symbol_query, SymbolOp};

use super::common::{connect_and_find_node_job, connect_for_read, find_query_value};

/// Query parameters the symbol resources accept (the path segment carries the
/// symbol name). `op` selects the navigation op; `in` scopes to a glob subtree.
const SYMBOL_KEYS: &[&str] = &["op", "in"];

pub(crate) async fn read_node_symbols(
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
    dispatch(&worktree, symbol, params)
}

pub(crate) async fn read_project_symbols(
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
    dispatch(&worktree, symbol, params)
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

fn dispatch(worktree: &Path, symbol: Option<&str>, params: &[QueryParam]) -> String {
    if let Some(unsupported) = params
        .iter()
        .find(|param| !SYMBOL_KEYS.contains(&param.key.as_str()))
    {
        return format!(
            "Unsupported query parameter '{}' for symbol resources (supported: {})",
            unsupported.key,
            SYMBOL_KEYS.join(", ")
        );
    }
    let glob = find_query_value(params, "in");
    let op = match find_query_value(params, "op") {
        None | Some("") => None,
        Some(name) => match SymbolOp::from_name(name) {
            Some(op) => Some(op),
            None => {
                return format!(
                    "Unknown symbol op '{name}' (definition|references|callers|implementations; absent op = overview)"
                )
            }
        },
    };
    let Some(symbol) = symbol else {
        return usage();
    };
    symbol_query(worktree, worktree, op, symbol, glob).body
}

fn usage() -> String {
    "Structural symbol navigation. Append a symbol (`/IssueStatus`) with an op \
     (`?op=references`); ops: definition|references|callers|implementations \
     (absent op = overview: definition site + signature + reference count). \
     Scope with `?in=<glob>`. This resource navigates a name you already have \
     — to discover one, read a file or directory with `?ast=<pattern>` or \
     `?grep=<regex>` first."
        .to_string()
}
