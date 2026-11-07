//! The cairn:// URI grammar: one parser, one builder, and one reader of thread
//! sub-paths, shared by every Rust caller and, compiled to WebAssembly by
//! `cairn-uri-wasm`, by the frontend (tenet 1435).
//!
//! The crate stays free of tokio, sockets, the filesystem, and libc so it builds
//! for `wasm32-unknown-unknown`. The resource contract table stays in
//! `cairn-common`, keyed by [`ResourceKind`].
//!
//! - `types`: the `CairnResource` enum, the scheme constants, and `kind()`.
//! - `kind`: [`ResourceKind`], the data-free discriminant, and its slug.
//! - `accessors`: the field projections (`project`, `issue_number`, `node_id`,
//!   `project_key`, `to_route`).
//! - `build`: the `build_*` constructors and `to_uri()`.
//! - `parse`: [`parse_uri`].
//! - `thread`: [`delegate_thread_descendant`], the one reader of thread
//!   sub-paths.
//! - `scan`: finding the cairn:// URIs written into free text, by handing
//!   candidates to `parse` rather than restating the grammar.
//! - `relative`: joining a typed fragment onto a current location for
//!   cursor-style clients, deferring every validity question to `parse`.
//! - `samples`: one instance of every `CairnResource` variant, exhaustive by
//!   construction, so `build`, `parse`, and `thread` cannot drift apart in
//!   silence.
//! - `thread_name`, `executor_name`, `percent`: the segment rules the parser
//!   applies.

mod accessors;
mod build;
mod executor_name;
mod kind;
mod parse;
pub mod percent;
mod relative;
pub mod samples;
mod scan;
mod thread;
pub mod thread_name;
mod types;

pub use build::*;
pub use executor_name::normalize_executor_name;
pub use kind::ResourceKind;
pub use parse::*;
pub use relative::*;
pub use scan::*;
pub use thread::*;
pub use types::*;

#[cfg(test)]
mod tests;
