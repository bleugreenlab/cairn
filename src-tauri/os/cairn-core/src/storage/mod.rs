//! Facade over the descended `cairn-db` storage layer, plus the core-side team
//! sync loop.
//!
//! The storage engine, domain models, and table records descend into `cairn-db`
//! (its Turso + tantivy quarantine). This module globs cairn-db's `storage` so
//! every `crate::storage::…` path — including the `events`, `content_store`, and
//! `render` child modules and the `TeamId` alias — keeps resolving unchanged.
//!
//! `team_sync` is the one storage symbol that CANNOT descend: it needs
//! `crate::services::EventEmitter`, which stays in cairn-core, so it lives here
//! beside the facade and is re-exported into the same namespace.
pub use cairn_db::storage::*;

mod team_sync;
pub use team_sync::{
    run_pull_task, run_push_task, subscribe_team_pull_applied, RouteReconcile, SyncCadence,
    TeamSyncScope,
};
