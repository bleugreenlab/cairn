//! Thread lifetime on a shared app-server: `thread/unsubscribe` and
//! `thread/loaded/list`.
//!
//! Unsubscribing ends one client's subscription; it does not stop a running
//! turn, and Codex unloads the thread only once it has had no subscriber and
//! no activity for its unload delay (30 minutes upstream).

use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ThreadUnsubscribeStatus {
    /// The thread was not loaded on this server at all.
    NotLoaded,
    /// Loaded, but this client held no subscription to it.
    NotSubscribed,
    Unsubscribed,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ThreadUnsubscribeResponse {
    pub status: ThreadUnsubscribeStatus,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadLoadedListResponse {
    /// Thread ids currently loaded in the server's memory.
    pub data: Vec<String>,
    /// Present while more pages remain.
    #[serde(default)]
    pub next_cursor: Option<String>,
}
