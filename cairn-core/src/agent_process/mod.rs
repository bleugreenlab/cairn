//! Agent CLI process management (core logic, no Tauri dependency).
//!
//! This module contains the pure business logic for managing agent CLI processes:
//! event parsing, argument building, process state management, and turn boundary detection.
//!
//! The Tauri app re-exports these types and builds session management on top.

pub mod args;
pub mod gc;
pub mod memory;
pub mod process;
pub mod stdin;
pub mod stream;
pub mod toolkits;
pub mod turn_boundary;
