//! In-process structural code intelligence built on ast-grep (tree-sitter).
//!
//! This is the replacement for the external Language Server adapter: instead of
//! shelling out to a per-worktree language server that must index the codebase
//! before answering, the engine parses files on demand with the right bundled
//! tree-sitter grammar (microseconds, zero install, zero config). The cost is
//! semantic precision — there is no cross-file name resolution or type-directed
//! method resolution — accepted because agents triangulate with `grep` and the
//! compiler, and the speed/universality/zero-config wins dominate.
//!
//! Layers:
//! - [`engine`] parses a source string under a bundled grammar and runs an
//!   ast-grep `Pattern`, mapping matches to render-ready rows. tree-sitter
//!   yields byte offsets directly, so there is no UTF-16 position mapping.
//! - [`render`] formats results as the canonical grep-style `path:line:snippet`
//!   rows plus the `{n} matches[ in {m} files]` header suffix, so agents see no
//!   new output shape.
//! - [`search`] backs the `?ast=` read modifier: a raw structural pattern search
//!   over a file or a directory tree (walked with optional `?glob=`).

pub mod engine;
pub mod grammar;
pub mod nav;
pub mod outline;
pub mod rename;
pub mod render;
pub mod search;
pub mod walk;

pub use nav::SymbolOp;
pub use render::{LocationHit, Rendered};

/// The bundled tree-sitter language enum, re-exported so cairn-core's YAML
/// comment-preserving edit path can name `SupportLang::Yaml` without a direct
/// ast-grep dependency of its own.
pub use ast_grep_language::SupportLang;
