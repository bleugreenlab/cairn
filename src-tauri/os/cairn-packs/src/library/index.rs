//! The library index format, `cairn-library-index/1`.
//!
//! One JSON document listing every entry a source offers, the commit it was
//! derived at, and for each item the hash the install lock records. Item hashes
//! are computed by the lock's own functions ([`crate::hash_item_path`],
//! [`crate::hash_mcp_definition`]), so an installed baseline and an index hash
//! are directly comparable.

use serde::{Deserialize, Serialize};

use crate::manifest::{ItemRuns, PackAuthor, PackFormat, PackItemKind};
use crate::server_config::McpServerConfig;

pub const INDEX_FORMAT: &str = "cairn-library-index/1";
pub const INDEX_FILE: &str = "index.json";
pub const SIGNATURE_FILE: &str = "index.json.minisig";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LibraryIndex {
    pub format: String,
    pub source: IndexSource,
    /// Monotonic per source. A client refuses an index older than the newest
    /// one it has verified.
    pub serial: u64,
    pub generated_at: String,
    pub entries: Vec<IndexEntry>,
    /// Marketplace plugins the derivation skipped, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl LibraryIndex {
    pub fn entry(&self, id: &str) -> Option<&IndexEntry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// The index as canonical JSON (sorted keys, two-space indent, trailing
    /// newline). These are the bytes CI signs and the golden vector pins.
    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|e| format!("Failed to serialize library index: {e}"))?;
        let mut text = serde_json::to_string_pretty(&crate::canonical_json(value))
            .map_err(|e| format!("Failed to serialize library index: {e}"))?;
        text.push('\n');
        Ok(text)
    }
}

/// The repository and commit the index was derived from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSource {
    pub url: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexEntry {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<PackAuthor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
    /// The manifest shape of the staged pack root.
    pub format: PackFormat,
    pub origin: EntryOrigin,
    /// [`crate::content_hash`] of the staged pack root.
    pub content_hash: String,
    pub items: Vec<IndexItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl IndexEntry {
    /// Whether staging selects listed paths into a synthesized pack (a
    /// marketplace entry with explicit `skills`/`agents`) rather than copying
    /// the origin directory as it is.
    pub fn is_selection(&self) -> bool {
        self.items.iter().any(|item| item.from.is_some())
    }

    /// The most consequential thing any item of this entry does here.
    pub fn runs(&self) -> ItemRuns {
        self.items
            .iter()
            .map(|item| item.runs)
            .max()
            .unwrap_or_default()
    }

    pub fn item(&self, kind: PackItemKind, id: &str) -> Option<&IndexItem> {
        self.items
            .iter()
            .find(|item| item.kind == kind && item.id == id)
    }
}

/// Where an entry's bytes come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum EntryOrigin {
    /// A directory of the source repository itself, at the index's commit.
    Source { path: String },
    /// Another repository at a pinned commit, fetched at install time and
    /// hash-checked against this index. Its bytes are never copied into the
    /// source repository.
    Upstream {
        url: String,
        commit: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexItem {
    pub kind: PackItemKind,
    pub id: String,
    /// Destination path in the flat workspace layout (absent for MCP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// For a selection entry: the item's path relative to the origin root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    pub hash: String,
    pub runs: ItemRuns,
    /// An MCP item's definition, so the Library can show what would run from
    /// signed bytes before anything is fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<McpServerConfig>,
}
