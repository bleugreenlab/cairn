//! Unstable host-facing runtime surface for Cairn's first-party apps.
//!
//! This module is gated behind the non-default `internal-api` feature and is not
//! part of the intended semver contract for third-party consumers. The host
//! crates (the Tauri app in `src-tauri/src` and `cairn-server`) reach the
//! crate's implementation modules through `cairn_core::internal::<module>::...`.
//!
//! ## Maintenance: this list must stay complete
//!
//! Each entry re-exports an entire top-level crate module wholesale via
//! `pub mod <name> { pub use crate::<name>::*; }`. The glob re-export carries
//! every public item and every public submodule along with it, so the host
//! crates can reach the full public depth of a module
//! (`internal::workspace::bundle::...`, `internal::mcp::handlers::...`) without
//! any per-item edit here. Adding a function to `jobs/queries.rs`, or an
//! entirely new submodule under one of these modules, needs no change to this
//! file.
//!
//! The wrapper module is required, not stylistic: the implementation modules are
//! declared `mod <name>;` (private) in `lib.rs`, and Rust forbids re-exporting a
//! private module by name (`pub use crate::<name>;` fails with E0365). Wrapping
//! the glob in a fresh `pub mod` is the legal way to surface a private module's
//! public contents to another crate.
//!
//! The one change that DOES require an edit here is introducing a brand-new
//! top-level module in `lib.rs` that a host crate needs to call. A top-level
//! module absent from this list is invisible to the host crates, and the symptom
//! is a confusing unresolved-path error at the call site rather than anything
//! pointing back here. When a host crate cannot resolve
//! `cairn_core::internal::<module>`, add a matching wrapper entry below.
//! Over-exposing is harmless: this whole surface is unstable and feature-gated,
//! so the safe default is to add the module.
//!
//! Only modules the host crates actually consume belong here. Modules that are
//! purely internal to `cairn-core` (their submodules are private, with no host
//! caller) are intentionally omitted.

pub mod account {
    pub use crate::account::*;
}

pub mod agent_process {
    pub use crate::agent_process::*;
}

pub mod api {
    pub use crate::api::*;
}

pub mod backends {
    pub use crate::backends::*;
}

pub mod browser_network {
    pub use crate::browser_network::*;
}

pub mod fleet {
    pub use crate::fleet::*;
}

pub mod browsers {
    pub use crate::browsers::*;
}

pub mod db {
    pub use crate::db::*;
}

pub mod db_records {
    pub use crate::db_records::*;
}

pub mod dispatch {
    pub use crate::dispatch::*;
}

pub mod effects {
    pub use crate::effects::*;
}

pub mod embeddings {
    pub use crate::embeddings::*;
}

pub mod env {
    pub use crate::env::*;
}

pub mod execution {
    pub use crate::execution::*;
}

pub mod git {
    pub use crate::git::*;
}

pub mod jj {
    pub use crate::jj::*;
}

pub mod identity {
    pub use crate::identity::*;
}

pub mod jobs {
    pub use crate::jobs::*;
}

pub mod mcp {
    pub use crate::mcp::*;
}

pub mod node_segments {
    pub use crate::node_segments::*;
}

pub mod notify {
    pub use crate::notify::*;
}

pub mod orchestrator {
    pub use crate::orchestrator::*;
}

pub mod resources {
    pub use crate::resources::*;
}

pub mod runs {
    pub use crate::runs::*;
}

pub mod services {
    pub use crate::services::*;
}

pub mod storage {
    pub use crate::storage::*;
}

pub mod repl_host {
    pub use crate::repl_host::*;
}

pub mod terminal_host {
    pub use crate::terminal_host::*;
}

pub mod team_remote_intents {
    pub use crate::team_remote_intents::*;
}

pub mod workspace {
    pub use crate::workspace::*;
}
