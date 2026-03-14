//! Database types for Cairn Core.
//!
//! Contains the connection state wrapper and migration status types.
//! Database initialization and path resolution remain in the Tauri crate
//! since they depend on platform-specific app data directories.

use diesel::sqlite::SqliteConnection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

// Embed Diesel migrations
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../../diesel_migrations");

/// Database state wrapping a Diesel SQLite connection.
///
/// Thread-safe via Mutex. Used by both Tauri state management
/// and potential server deployments.
pub struct DbState {
    pub conn: Mutex<SqliteConnection>,
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
