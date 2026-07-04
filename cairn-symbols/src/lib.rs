//! Structural code intelligence and warm worktree file-search, quarantined.
//!
//! This crate holds cairn-core's two heaviest dependency edges behind one
//! boundary so the rest of cairn-core compiles without them (the ~25 compiled
//! tree-sitter grammars and fff-search):
//! - [`symbols`] — in-process structural code intelligence built on ast-grep
//!   (tree-sitter): parse-on-demand navigation, `?ast=` structural search,
//!   outlines, and identifier rename.
//! - [`worktree_search`] — a resident, filesystem-watched fff-search index per
//!   worktree so repeated `?grep=`/`?glob=` reads hit a warm index instead of
//!   re-walking the tree.
//! - [`search_util`] — the small path/glob/format helpers both engines share
//!   (and that cairn-core's ripgrep grep handler re-imports so warm and cold
//!   output stay byte-identical).
//!
//! The dependency direction is one-way: cairn-symbols never imports cairn-core.

pub mod search_util;
pub mod symbols;
pub mod worktree_search;
