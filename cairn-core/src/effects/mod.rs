//! Typed workflow effects.
//!
//! State transition functions produce `Vec<WorkflowEffect>` instead of
//! executing side effects inline. The effect loop in `run.rs` drives a
//! reduce→execute→reduce cycle until quiescent:
//!
//! - **Core effects** (DAG advancement, lifecycle messages)
//!   are handled by the loop directly.
//! - **Host effects** (process spawn, worktree creation, shell commands)
//!   are dispatched to an `EffectExecutor` trait implemented by each host.
//!
//! See `docs/state-machines.md` for the overall lifecycle design.

pub mod checkpoint;
pub mod dag;
pub mod executor;
pub mod outbox;
pub mod reduce;
pub mod run;
pub mod types;
