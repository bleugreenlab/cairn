//! Structural code intelligence, quarantined from cairn-core.
//!
//! This crate holds cairn-core's tree-sitter dependency edge behind one boundary:
//! - [`symbols`] — in-process structural code intelligence built on ast-grep
//!   (tree-sitter): parse-on-demand navigation, `?ast=` structural search,
//!   outlines, and identifier rename.
//! - [`search_util`] — path, glob, and formatting helpers shared with
//!   cairn-core's store-native search rendering.
//!
//! The dependency direction is one-way: cairn-symbols never imports cairn-core.

pub mod search_util;
pub mod symbols;
