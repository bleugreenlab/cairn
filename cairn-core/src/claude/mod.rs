//! Claude CLI integration (core logic, no Tauri dependency).
//!
//! This module contains the pure business logic for managing Claude CLI processes:
//! event parsing, argument building, process state management, and turn boundary detection.
//!
//! The Tauri app re-exports these types and builds session management on top.

/// Bundled system prompt content (compiled into binary)
pub const SYSTEM_PROMPT: &str = include_str!("system_prompt.md");

pub mod args;
pub mod checkpoints;
pub mod gc;
pub mod process;
pub mod stdin;
pub mod stream;
pub mod toolkits;
pub mod turn_boundary;
