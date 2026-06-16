//! Database types for Cairn Core.
//!
//! Contains the runtime database state wrapper and migration status types.
//! Database initialization and path resolution remain in host crates since
//! they depend on platform-specific app data directories.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::storage::{LocalDb, SearchIndex};

/// Runtime database state.
pub struct DbState {
    pub local: Arc<LocalDb>,
    pub search_index: Arc<SearchIndex>,
}

impl DbState {
    pub fn new(local: Arc<LocalDb>, search_index: Arc<SearchIndex>) -> Self {
        Self {
            local,
            search_index,
        }
    }
}

// ============================================================================
// Migration Status Types (for frontend communication)
// ============================================================================

/// Status check result for migration UI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationStatus {
    pub needed: bool,
    pub pending_migrations: Vec<String>,
    pub current_db_path: String,
    pub error_message: Option<String>,
}

/// Schema change detected during migration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChange {
    pub table: String,
    pub change_type: String,
    pub old_name: Option<String>,
    pub new_name: Option<String>,
    pub auto_mapped: bool,
}

/// Per-table result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMigrationResult {
    pub name: String,
    pub old_count: usize,
    pub new_count: usize,
    pub status: String,
    pub error: Option<String>,
}

/// Final migration result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationResult {
    pub success: bool,
    pub tables: Vec<TableMigrationResult>,
    pub schema_changes: Vec<SchemaChange>,
    pub total_rows_restored: usize,
    pub total_rows_attempted: usize,
    pub warnings: Vec<String>,
}

/// Results from startup recovery.
///
/// Contains outbox entries to replay.
pub struct StartupRecovery {
    /// Pending outbox entries that need to be replayed after Orchestrator is built.
    pub outbox_entries: Vec<crate::effects::outbox::OutboxEntry>,
}
