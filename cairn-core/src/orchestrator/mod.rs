//! Orchestrator — bundles all runtime state needed for agent execution.
//!
//! Both the Tauri desktop app and cairn-server create their own `Orchestrator`.
//! All orchestration functions take `&Orchestrator` instead of framework-specific
//! handles (e.g. `&AppHandle`).

pub mod agents;
pub mod conflict_resolution;
pub mod docs;
pub mod lifecycle;
pub mod recipes;
pub mod session;
pub mod settings;
pub mod skills;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

use crate::claude::process::ClaudeProcessState;
use crate::db::DbState;
use crate::mcp::McpAuthState;
use crate::services::{PtyState, Services};

/// Central runtime state for agent orchestration.
///
/// Created once at startup by each host (Tauri app, cairn-server).
/// Passed to all orchestration functions as `&Orchestrator`.
#[derive(Clone)]
pub struct Orchestrator {
    /// Database connection state (SQLite + Diesel)
    pub db: Arc<DbState>,
    /// Service abstractions (event emitter, process spawner, clock, filesystem)
    pub services: Arc<Services>,
    /// Active Claude CLI process tracking
    pub process_state: Arc<ClaudeProcessState>,
    /// MCP authentication (shared secret for TOTP passcodes)
    pub mcp_auth: Arc<McpAuthState>,
    /// Warm process GC (optional — hosts may not enable warm processes)
    pub warm_gc: Option<Arc<crate::claude::gc::WarmProcessGC>>,
    /// Active PTY sessions (terminals)
    pub pty_state: Arc<PtyState>,

    // === Broadcast channels for cross-component communication ===
    /// Permission response broadcast: (request_id, response_json)
    /// Hosts send on this channel when a user responds to a permission prompt.
    pub permission_responses: broadcast::Sender<(String, String)>,
    /// Run completion broadcast: run_id
    /// Emitted when a run finishes (used by sub-agent handlers to unblock).
    pub run_completions: broadcast::Sender<String>,

    /// Tools auto-allowed via "Allow for Session" permission responses.
    /// Checked in handle_permission_prompt before showing UI.
    pub session_allowed_tools: Arc<Mutex<HashSet<String>>>,

    // === Host-specific paths (set by Tauri or cairn-server) ===
    /// Path to the cairn-mcp binary
    pub mcp_binary_path: String,
    /// Directory for writing MCP config files
    pub config_dir: PathBuf,
    /// Directory containing bundled preset schemas (None if not available)
    pub schema_dir: Option<PathBuf>,
    /// Port for the MCP callback server
    pub mcp_callback_port: u16,
}

impl Orchestrator {
    /// Evict a warm process if needed to make room for a new one.
    /// Returns the run_id of the evicted process, if any.
    pub fn collect_warm_if_needed(&self) -> Option<String> {
        let gc = self.warm_gc.as_ref()?;
        let mut conn = self.db.conn.lock().ok()?;

        let eviction_candidate = gc.find_eviction_candidate(&self.process_state, &mut conn);

        if let Some(ref run_id) = eviction_candidate {
            log::info!(
                "GC: evicting warm process {}",
                &run_id[..run_id.len().min(8)]
            );
            if let Err(e) = lifecycle::kill_session(self, run_id) {
                log::error!("GC: failed to kill evicted process {}: {}", run_id, e);
            }
        }

        eviction_candidate
    }
}
