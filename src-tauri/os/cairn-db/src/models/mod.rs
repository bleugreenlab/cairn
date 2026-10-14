//! Domain types are shared with storage-free consumers.
pub use cairn_models::*;

// Keep the existing query path for runtime callers.
pub use crate::storage::get_latest_context_token_event;
