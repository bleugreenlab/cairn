//! App-owned schema installed in every thread database.

use crate::storage::Migration;

/// The migration that drops `_cairn_rulings` once its rows are tenets. It is
/// held back on a store whose rulings have not been imported; see
/// `db_store::open_path`.
pub(super) const RULINGS_TO_TENETS: &str = "0002";

/// The migration that installs the starter record, `intent` and `journal`.
pub(super) const THREAD_RECORD: &str = "0005";

/// The migration that installs `headlines`, the thread's capped, expiring
/// lines for an operator who has been away.
pub(super) const HEADLINES: &str = "0006";

/// A table Cairn owns in every thread store under a friendly name, and the
/// migration that creates it. Before that migration runs,
/// `db_store::open_path` moves a thread's own table of the name aside.
pub(crate) struct AppTable {
    pub name: &'static str,
    pub since: &'static str,
}

/// Tables Cairn owns in every thread store under friendly names, without the
/// `_cairn_` prefix (tenet 182). Their rows are the thread's to write; their
/// shape is Cairn's, so DDL against them is refused.
pub(crate) const APP_TABLES: &[AppTable] = &[
    AppTable {
        name: "intent",
        since: THREAD_RECORD,
    },
    AppTable {
        name: "journal",
        since: THREAD_RECORD,
    },
    AppTable {
        name: "headlines",
        since: HEADLINES,
    },
];

/// Whether `name` is one of the [`APP_TABLES`], compared case-insensitively as
/// SQLite compares identifiers.
pub(crate) fn is_app_table(name: &str) -> bool {
    APP_TABLES
        .iter()
        .any(|table| table.name.eq_ignore_ascii_case(name))
}

pub(super) const THREAD_MIGRATIONS: &[Migration] = &[
    Migration::new(
        "0001",
        "arc_collections",
        include_str!("../../../../thread_migrations/0001_arc_collections.sql"),
    ),
    Migration::new(
        RULINGS_TO_TENETS,
        "rulings_to_tenets",
        include_str!("../../../../thread_migrations/0002_rulings_to_tenets.sql"),
    ),
    // Applies even while the guard holds 0002, so a questions-only marker is
    // never mistaken for one that copied rulings.
    Migration::new(
        "0003",
        "arc_import_scope",
        include_str!("../../../../thread_migrations/0003_arc_import_scope.sql"),
    ),
    Migration::new(
        "0004",
        "retire_procedures",
        include_str!("../../../../thread_migrations/0004_retire_procedures.sql"),
    ),
    Migration::new(
        THREAD_RECORD,
        "thread_record",
        include_str!("../../../../thread_migrations/0005_thread_record.sql"),
    ),
    Migration::new(
        HEADLINES,
        "headlines",
        include_str!("../../../../thread_migrations/0006_headlines.sql"),
    ),
];
