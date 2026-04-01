//! Shared Cairn system prompt content.
//!
//! The base system prompt is bundled into the binary and shared across
//! backends (Claude, Codex, future engines). Keep all global guardrails and
//! instructions in `system_prompt.md` so every backend runs with identical
//! containment.

/// Cairn's base system prompt (compiled into the binary).
pub const CAIRN_SYSTEM_PROMPT: &str = include_str!("agent_process/system_prompt.md");
