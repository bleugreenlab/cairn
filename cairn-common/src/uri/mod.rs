//! Unified cairn:// URI scheme parser.
//!
//! Canonical project-scoped URIs use an explicit namespace token:
//! `cairn://p/PROJECT/...`
//!
//! The parser is split across cohesive submodules behind this façade, which
//! re-exports the full public surface so every `cairn_common::uri::X` path
//! keeps resolving unchanged:
//! - `types` — the `CairnResource` enum, `CairnResourceUri`, the scheme
//!   constants, and the data-free `kind()` discriminant.
//! - `accessors` — the field-projection query methods (`project`,
//!   `issue_number`, `node_id`, `project_key`, `to_route`).
//! - `build` — the `build_*` URI constructors and `to_uri()`.
//! - `parse` — `parse_uri` and `parse_resource_uri`.

mod accessors;
mod build;
mod parse;
mod types;

pub use build::*;
pub use parse::*;
pub use types::*;

#[cfg(test)]
mod tests;
