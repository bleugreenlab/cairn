//! File operation MCP handlers.
//!
//! Handles: edit (unified file mutations), read

pub(crate) mod change;
mod read;
mod target;

pub use change::handle_change;
pub use read::handle_read_file;
pub(crate) use read::produce_archived_file_segment;
pub(crate) use read::produce_file_segment;
