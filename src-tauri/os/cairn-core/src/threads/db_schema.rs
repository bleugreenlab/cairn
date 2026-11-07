//! App-owned schema installed in every thread database.

use crate::storage::Migration;

/// The migration that drops `_cairn_rulings` once its rows are tenets. It is
/// held back on a store whose rulings have not been imported; see
/// `db_store::open_path`.
pub(super) const RULINGS_TO_TENETS: &str = "0002";

/// The migration that installs the starter record, `intent` and `journal`.
/// Before it runs, `db_store::open_path` moves a thread's own table of either
/// name aside.
pub(super) const THREAD_RECORD: &str = "0005";

/// Tables Cairn owns in every thread store under friendly names, without the
/// `_cairn_` prefix (tenet 182). Their rows are the thread's to write; their
/// shape is Cairn's, so DDL against them is refused.
pub(crate) const APP_TABLES: &[&str] = &["intent", "journal"];

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
];
