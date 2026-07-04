//! `cairn-cmd` binary entry point: argument dispatch and module wiring.
//!
//! The heavy lifting lives in sibling modules: `schemas` (tool input types),
//! `timeouts` (callback-timeout derivation), `output` (result capping and
//! change-report rendering), `resolve` (Cairn URI resolution), `server` (the
//! stdio MCP server), `cli` (the read/write/watch subcommands), and
//! `test_support` (shared `#[cfg(test)]` helpers).
use anyhow::Result;
use clap::Parser;
use std::env;

use rmcp::ServiceExt;

use cairn_common::uri::parse_uri as parse_cairn_uri;

mod cli;
mod output;
mod resolve;
mod schemas;
mod server;
#[cfg(test)]
mod test_support;
mod timeouts;

use cli::{default_callback_url, run_cli_change, run_cli_read, run_cli_watch};
use schemas::AgentInfo;
use server::CairnCmd;

/// Cairn MCP Server - tools for Claude to interact with Cairn
#[derive(Parser)]
#[command(name = "cairn-cmd", version)]
struct Args {
    /// Subcommand. When omitted (or `mcp`), runs the stdio MCP server.
    #[command(subcommand)]
    command: Option<Command>,

    /// JSON-encoded list of available agents [{name, description}, ...]
    #[arg(long)]
    agents: Option<String>,
}

/// Top-level CLI subcommands.
///
/// The `cairn` binary is a thin client: `read`/`write` build a callback
/// request and forward it to the running Cairn app over the same HTTP callback
/// the MCP server uses, then print the result to stdout. The binary never opens
/// the database itself (Turso holds a process-exclusive lock; the app is the
/// sole owner). If the app is unreachable, we autostart it and retry once.
#[derive(clap::Subcommand)]
enum Command {
    /// Run the stdio MCP server (default when no subcommand is given).
    Mcp,
    /// Read one or more files or Cairn resources and print them to stdout (pipeable).
    Read {
        /// One or more targets: `file:path` (worktree-relative), `file:/abs/path`, `cairn://p/PROJECT/...`, or `cairn:~/...`.
        /// Append `?key=value` to a target for per-target scoping.
        #[arg(required = true, num_args = 1..)]
        targets: Vec<String>,
        /// Start reading from this line number. Single-target convenience: folded
        /// into that target's query string. Rejected with multiple targets
        /// (scope each target via its own query string instead).
        #[arg(long)]
        offset: Option<usize>,
        /// Read at most this many lines. Single-target convenience (see --offset).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Apply ordered file/resource mutations (ChangeInput JSON via --json or stdin).
    #[command(alias = "change")]
    Write {
        /// ChangeInput JSON (an object with `changes`, or a bare `changes` array).
        /// If omitted, JSON is read from stdin.
        #[arg(long)]
        json: Option<String>,
        /// Commit message for file-target changes (`^` to amend).
        #[arg(long = "commit-msg")]
        commit_msg: Option<String>,
    },
    /// Block until an issue needs attention (no polling), then print it and exit.
    ///
    /// A single `cairn watch` is one continuous blocking call to the caller: it
    /// long-polls the running app and transparently re-issues across the server
    /// budget, carrying the last-seen cursor as `--since` so nothing is missed.
    Watch {
        /// Issue URI, e.g. `cairn://p/PROJECT/NUMBER`.
        issue_uri: String,
        /// Only return attention newer than this issue `updated_at` (unix seconds).
        #[arg(long)]
        since: Option<i64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // CLI subcommands (read/write) keep stderr clean for piping — logs go to
    // the file only. The MCP server path also logs to stderr.
    let is_cli = matches!(
        args.command,
        Some(Command::Read { .. }) | Some(Command::Write { .. }) | Some(Command::Watch { .. })
    );
    // `None` level: the spawning app injects `CAIRN_LOG_LEVEL`, which the filter
    // resolution picks up; a directly-launched cairn-cmd falls back to Standard.
    let _log_guard = cairn_common::logging::init(cairn_common::logging::LogConfig {
        process: cairn_common::logging::ProcessTag::Cmd,
        log_dir: None,
        stderr: !is_cli,
        level: None,
    })
    .expect("Failed to initialize logging");

    // CLI subcommands: thin forward to the running app, print to stdout, exit
    // with a code reflecting success. The MCP server path falls through below.
    match &args.command {
        Some(Command::Read {
            targets,
            offset,
            limit,
        }) => {
            let ok = run_cli_read(targets, *offset, *limit).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Write { json, commit_msg }) => {
            let ok = run_cli_change(json.clone(), commit_msg.clone()).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Watch { issue_uri, since }) => {
            let ok = run_cli_watch(issue_uri.clone(), *since).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Mcp) | None => {}
    }

    // Callback URL - passed from main app via MCP config env var.
    // If omitted, fall back to the build-profile default app port.
    let callback_url = env::var("CAIRN_CALLBACK_URL").unwrap_or_else(|_| default_callback_url());
    // Get current working directory - fallback for run identification
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    // Run ID - preferred method for accurate run identification (avoids cwd ambiguity)
    let run_id = env::var("CAIRN_RUN_ID").ok();
    // Shared secret for bearer token authentication (base64-encoded)
    let mcp_secret = env::var("CAIRN_MCP_SECRET")
        .ok()
        .or_else(cairn_common::auth::load_local_mcp_token);
    // Stable home URI for client-side Cairn shorthand resolution.
    let home_uri = env::var("CAIRN_HOME_URI").ok().and_then(|uri| {
        if parse_cairn_uri(&uri).is_some() {
            Some(uri)
        } else {
            tracing::warn!("Ignoring invalid CAIRN_HOME_URI: {}", uri);
            None
        }
    });

    tracing::info!("Starting cairn-cmd server");
    tracing::info!("Callback URL: {}", callback_url);
    tracing::info!("Working directory: {}", cwd);
    if let Some(ref id) = run_id {
        tracing::info!("Run ID: {}", id);
    }
    if let Some(ref uri) = home_uri {
        tracing::info!("Home URI: {}", uri);
    }

    // Parse available agents from JSON argument
    let available_agents: Vec<AgentInfo> = if let Some(ref agents_json) = args.agents {
        match serde_json::from_str::<Vec<AgentInfo>>(agents_json) {
            Ok(agents) => {
                tracing::info!("Loaded {} available agents", agents.len());
                agents
            }
            Err(e) => {
                tracing::warn!("Failed to parse agents JSON: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let service = CairnCmd::new_with_home_uri(
        callback_url,
        cwd,
        run_id,
        mcp_secret,
        available_agents,
        home_uri,
    );

    // Create stdio transport and run the server
    let transport = rmcp::transport::stdio();
    let server = service.serve(transport).await?;

    // Wait for the server to complete
    server.waiting().await?;

    Ok(())
}
