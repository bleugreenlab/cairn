//! App-owned schema installed in every thread database.

use crate::storage::Migration;

pub(super) const THREAD_MIGRATIONS: &[Migration] = &[Migration::new(
    "0001",
    "arc_collections",
    include_str!("../../../../thread_migrations/0001_arc_collections.sql"),
)];
