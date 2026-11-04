//! The per-source index cache, and refreshing it.
//!
//! `<config>/library/<source-id>/` holds the last verified `index.json`, its
//! `index.json.minisig`, and `state.json`. The directory sits inside the
//! workspace config home, which agents can write, so the cache is verified
//! again on every read: a tampered cache drops the remote entries and names
//! the refusal instead of being believed.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::git::{self, GitPolicy};
use super::index::{INDEX_FILE, SIGNATURE_FILE};
use super::source::LibrarySource;
use super::verify::{verify_index, VerifiedIndex};

pub const LIBRARY_DIR: &str = "library";
const STATE_FILE: &str = "state.json";

pub fn cache_dir(config_dir: &Path, source: &LibrarySource) -> PathBuf {
    config_dir.join(LIBRARY_DIR).join(&source.id)
}

/// What the last refresh of one source learned.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceState {
    /// The newest serial verified for this source. Older indexes are refused.
    #[serde(default)]
    pub highest_serial: u64,
    /// The commit the index ref pointed at when last fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_commit: Option<String>,
    /// When the source was last contacted, successfully or not (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<String>,
    /// Why the last contact failed, if it did. The previous verified cache
    /// stays in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

pub fn read_state(config_dir: &Path, source: &LibrarySource) -> SourceState {
    std::fs::read(cache_dir(config_dir, source).join(STATE_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

pub fn write_state(
    config_dir: &Path,
    source: &LibrarySource,
    state: &SourceState,
) -> Result<(), String> {
    let dir = cache_dir(config_dir, source);
    let text = serde_json::to_string_pretty(state)
        .map_err(|e| format!("Failed to serialize library state: {e}"))?;
    write_atomic(&dir, STATE_FILE, text.as_bytes())
}

/// The cached index as this read sees it.
#[derive(Debug, Clone, PartialEq)]
pub enum CachedIndex {
    /// Never fetched on this workspace.
    Absent,
    Verified(Box<VerifiedIndex>),
    /// Present but not believable; the reason names why.
    Refused(String),
}

impl CachedIndex {
    pub fn verified(&self) -> Option<&VerifiedIndex> {
        match self {
            CachedIndex::Verified(index) => Some(index),
            _ => None,
        }
    }
}

/// Read and fully re-verify the cached index for `source`.
pub fn read_cached_index(config_dir: &Path, source: &LibrarySource) -> CachedIndex {
    let dir = cache_dir(config_dir, source);
    let Ok(bytes) = std::fs::read(dir.join(INDEX_FILE)) else {
        return CachedIndex::Absent;
    };
    let Ok(signature) = std::fs::read_to_string(dir.join(SIGNATURE_FILE)) else {
        return CachedIndex::Refused(format!(
            "the cached {} index has no signature beside it",
            source.label
        ));
    };
    let state = read_state(config_dir, source);
    match verify_index(source, &bytes, &signature, state.highest_serial) {
        Ok(verified) => CachedIndex::Verified(Box::new(verified)),
        Err(reason) => CachedIndex::Refused(reason),
    }
}

/// What one refresh did.
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshOutcome {
    /// The index ref has not moved and the cache still verifies.
    Unchanged(Box<VerifiedIndex>),
    /// A newer index was fetched, verified, and cached.
    Updated(Box<VerifiedIndex>),
}

impl RefreshOutcome {
    pub fn index(&self) -> &VerifiedIndex {
        match self {
            RefreshOutcome::Unchanged(index) | RefreshOutcome::Updated(index) => index,
        }
    }
}

/// Contact `source`, fetch its index if the index ref moved (or `force`), and
/// cache it once it verifies. On any failure the previous verified cache is
/// left exactly as it was, and the failure is recorded in `state.json`.
pub fn refresh_index(
    config_dir: &Path,
    source: &LibrarySource,
    policy: GitPolicy,
    force: bool,
) -> Result<RefreshOutcome, String> {
    let mut state = read_state(config_dir, source);
    state.checked_at = Some(chrono::Utc::now().to_rfc3339());
    let result = fetch_and_verify(config_dir, source, policy, force, &mut state);
    state.last_error = result.as_ref().err().cloned();
    if let Err(error) = write_state(config_dir, source, &state) {
        log::warn!("Failed to record {} refresh state: {error}", source.label);
    }
    result
}

fn fetch_and_verify(
    config_dir: &Path,
    source: &LibrarySource,
    policy: GitPolicy,
    force: bool,
    state: &mut SourceState,
) -> Result<RefreshOutcome, String> {
    let index_ref = source.index_ref();
    let remote = git::ls_remote(&source.url, index_ref, policy)
        .map_err(|e| format!("the {} is unreachable: {e}", source.label))?
        .ok_or_else(|| {
            format!(
                "the {} has no `{index_ref}` branch, so it has published no index yet",
                source.label
            )
        })?;

    if !force && state.index_commit.as_deref() == Some(remote.as_str()) {
        if let CachedIndex::Verified(index) = read_cached_index(config_dir, source) {
            return Ok(RefreshOutcome::Unchanged(index));
        }
    }

    let work = tempfile::tempdir().map_err(|e| e.to_string())?;
    let checkout = work.path().join("index");
    git::fetch_tree(&source.url, &remote, &checkout, policy)?;
    let bytes = std::fs::read(checkout.join(INDEX_FILE)).map_err(|_| {
        format!(
            "the {} `{index_ref}` branch at {remote} has no {INDEX_FILE}",
            source.label
        )
    })?;
    let signature = std::fs::read_to_string(checkout.join(SIGNATURE_FILE)).map_err(|_| {
        format!(
            "the {} `{index_ref}` branch at {remote} has no {SIGNATURE_FILE}",
            source.label
        )
    })?;
    let verified = verify_index(source, &bytes, &signature, state.highest_serial)?;

    let dir = cache_dir(config_dir, source);
    // Signature first: a reader that sees the new index beside the old
    // signature refuses it, and the next refresh repairs the pair.
    write_atomic(&dir, SIGNATURE_FILE, signature.as_bytes())?;
    write_atomic(&dir, INDEX_FILE, &bytes)?;
    state.highest_serial = verified.index.serial;
    state.index_commit = Some(remote);
    Ok(RefreshOutcome::Updated(Box::new(verified)))
}

fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("Failed to create {dir:?}: {e}"))?;
    let temp = dir.join(format!(".{name}.tmp"));
    std::fs::write(&temp, bytes).map_err(|e| format!("Failed to write {temp:?}: {e}"))?;
    std::fs::rename(&temp, dir.join(name))
        .map_err(|e| format!("Failed to replace {name} in {dir:?}: {e}"))
}
