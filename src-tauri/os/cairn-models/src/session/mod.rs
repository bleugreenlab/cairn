//! Model sessions: the Claude CLI and the Codex app-server, the MCP servers a
//! session with tools is given, the local host sessions run on, and the reader
//! a tool-free one-shot is awaited with.
pub mod claude;
pub mod codex;
pub mod events;
pub mod local;
pub mod mcp;
pub mod one_shot;
