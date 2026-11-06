//! The Library's remote sources, orchestrated.
//!
//! `cairn-packs::library` knows how to fetch, verify, and stage. This module
//! decides when: it refreshes each configured source in the background after
//! startup and whenever the Library opens, applies updates that change only
//! content, flips a bundled lock to library provenance once its pack has moved
//! out of the app bundle, and classifies every pending update by whether it
//! changes what runs on this machine. Catalog reads never touch the network;
//! they read the verified cache.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::pack::library::cache::{self, RefreshOutcome};
use crate::config::pack::library::git::GitPolicy;
use crate::config::pack::library::source::{self as library_source, CAIRN_LIBRARY_ID};
use crate::config::pack::library::{IndexEntry, LibrarySource, VerifiedIndex, MOVED_TO_LIBRARY};
use crate::config::pack::server_config::McpServerConfig;
use crate::config::pack::{self, ItemRuns, PackLock, PackManifest, PackSource, PackSourceKind};
use crate::orchestrator::Orchestrator;
use crate::services::RealFileSystem;

#[cfg(feature = "test-utils")]
pub mod testing;

/// The sources this orchestrator reads and the policy it fetches them under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibraryContext {
    pub sources: Vec<LibrarySource>,
    pub policy: GitPolicy,
}

impl LibraryContext {
    /// What every production build reads: [`library_source::configured_sources`],
    /// over https only.
    pub fn configured() -> Self {
        LibraryContext {
            sources: library_source::configured_sources(),
            policy: GitPolicy::production(),
        }
    }

    /// The configured source a lock's URL came from, if any.
    pub fn source_for_url(&self, url: &str) -> Option<&LibrarySource> {
        library_source::source_for_url(&self.sources, url)
    }
}

/// How long a successful contact with a source is reused before the Library
/// contacts it again.
pub const REFRESH_TTL_MINUTES: i64 = 15;

/// Who is asking for a pack install, restore, update, or item reset.
///
/// An act that changes what runs on this machine is the operator's: a library
/// update that changes a server's command or endpoint or a workflow (tenet
/// 1189's "applied on click" is the operator's click), and any act that puts
/// a native machine service's declaration into the workspace, since a selected
/// machine runs that service as a resident outside any fence. The transport
/// decides which one an invoke carries from its credentials; every
/// agent-facing path is [`PackAuthority::Agent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackAuthority {
    Operator,
    Agent,
}

/// The phrase every operator-only pack refusal carries, so a transport can
/// answer it as Forbidden rather than as a malformed request.
pub const OPERATOR_ONLY_PACK_ACT: &str = "changes what runs on this machine";

/// The refusal for any attempt to add a library source in this slice.
pub const ADD_SOURCE_REFUSAL: &str =
    "Adding library sources is not available. Third-party library \
     sources arrive in a later slice behind the operator-only 'Dangerously enable third-party \
     library sources' setting (tenet 1370); the Library reads only the bundled core and Cairn's \
     hosted library.";

/// The source-shaped key a `cairn://packs` payload carries, if any.
pub fn source_key(payload: &serde_json::Value) -> Option<&'static str> {
    ["source", "url", "git"]
        .into_iter()
        .find(|key| payload.get(key).is_some())
}

/// One item an update would change that is an endpoint or runs here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeChange {
    pub kind: String,
    pub id: String,
    pub runs: ItemRuns,
    /// `added`, `changed`, or `removed`.
    pub change: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<McpServerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<McpServerConfig>,
}

/// What a verified index says about one installed library pack.
#[derive(Debug, Clone, PartialEq)]
pub enum PendingUpdate {
    UpToDate,
    /// Only content changes; applied by the background refresh.
    ContentOnly,
    /// Something that runs changes; waits for the operator.
    NeedsReview(Vec<RuntimeChange>),
}

pub fn pending_update(config_dir: &Path, lock: &PackLock, entry: &IndexEntry) -> PendingUpdate {
    if lock.content_hash == entry.content_hash {
        return PendingUpdate::UpToDate;
    }
    let changes = runtime_changes(config_dir, lock, entry);
    if changes.is_empty() {
        PendingUpdate::ContentOnly
    } else {
        PendingUpdate::NeedsReview(changes)
    }
}

/// Every endpoint or run-here item the update from `lock` to `entry` would add,
/// change, or remove. A user's fork and a removed item are untouched by any
/// update, so they are not changes.
pub fn runtime_changes(
    config_dir: &Path,
    lock: &PackLock,
    entry: &IndexEntry,
) -> Vec<RuntimeChange> {
    let installed = installed_definitions(config_dir, &lock.id);
    let mut changes = Vec::new();
    for item in &entry.items {
        if !item.runs.is_runnable() || lock.is_removed(item.kind, &item.id) {
            continue;
        }
        let change = match lock.item(item.kind, &item.id) {
            Some(old) if old.forked => continue,
            Some(old) if old.content_hash.as_deref() == Some(item.hash.as_str()) => continue,
            Some(_) => "changed",
            None => "added",
        };
        changes.push(RuntimeChange {
            kind: item.kind.as_str().to_string(),
            id: item.id.clone(),
            runs: item.runs,
            change: change.to_string(),
            before: installed
                .get(&item.id)
                .cloned()
                .filter(|_| change == "changed"),
            after: item.definition.clone(),
        });
    }
    for old in &lock.items {
        if old.forked || entry.item(old.kind, &old.id).is_some() {
            continue;
        }
        let before = installed.get(&old.id).cloned();
        let runs = ItemRuns::classify(old.kind, before.as_ref());
        if runs.is_runnable() {
            changes.push(RuntimeChange {
                kind: old.kind.as_str().to_string(),
                id: old.id.clone(),
                runs,
                change: "removed".to_string(),
                before,
                after: None,
            });
        }
    }
    changes
}

fn installed_definitions(config_dir: &Path, pack_id: &str) -> BTreeMap<String, McpServerConfig> {
    pack::mcp::parse_pack_mcp_file(&pack::lock::mcp_path(config_dir, pack_id))
        .map(|file| file.servers.into_iter().collect())
        .unwrap_or_default()
}

/// `<config>/packs/<id>/source/`: the copy of what a non-bundled pack shipped,
/// which reset, diff, and "behind shipped" compare against.
pub fn snapshot_root(config_dir: &Path, pack_id: &str) -> PathBuf {
    pack::lock::pack_dir(config_dir, pack_id).join("source")
}

/// The shipped copy of a local or library pack, refused when it is missing or
/// no longer hashes to what the lock recorded.
pub fn snapshot_manifest(config_dir: &Path, lock: &PackLock) -> Result<PackManifest, String> {
    let root = snapshot_root(config_dir, &lock.id);
    if !root.is_dir() {
        return Err(format!(
            "Pack `{}` has no fetched copy of its source on this machine yet; update it to fetch one",
            lock.id
        ));
    }
    if pack::content_hash(&root)? != lock.content_hash {
        return Err(format!(
            "Pack `{}`'s snapshot was modified; update it to fetch it again",
            lock.id
        ));
    }
    let mut manifest = match lock.source.kind {
        PackSourceKind::Local => pack::agent_plugin::load(&root)?.manifest,
        _ => pack::manifest::load(&root)?,
    };
    // A manifest without an explicit id names itself after its directory,
    // which here is `source`; the lock is the authority on what it is called.
    manifest.id = lock.id.clone();
    manifest.name = lock.name.clone();
    manifest.root = root;
    Ok(manifest)
}

/// Ids of every pack the app bundle ships, or `None` when this workspace has
/// no recorded resource directory yet (so nothing can be known about it).
pub fn shipped_ids(config_dir: &Path) -> Option<Vec<String>> {
    pack::source_dir(config_dir).map(|root| {
        pack::discover_available_packs(&root)
            .into_iter()
            .map(|manifest| manifest.id)
            .collect()
    })
}

/// What one source's refresh did.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRefresh {
    pub id: String,
    pub label: String,
    /// `updated`, `unchanged`, `recent` (contacted within the TTL), or `failed`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Packs whose content-only update was applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied: Vec<String>,
    /// Bundled locks flipped to this source's provenance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub migrated: Vec<String>,
    /// Packs whose update changes what runs, waiting for the operator.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub awaiting_review: Vec<String>,
    /// Content-only updates that failed, with why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_updates: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshReport {
    pub sources: Vec<SourceRefresh>,
}

/// Refresh every configured source. Never fails: a source that cannot be
/// reached keeps its last verified index and says why.
pub fn refresh_library_sources(orch: &Orchestrator, force: bool) -> RefreshReport {
    let config_dir = &orch.config_dir;
    let mut report = RefreshReport::default();
    let mut changed = false;
    for source in &orch.library.sources {
        let mut refresh = SourceRefresh {
            id: source.id.clone(),
            label: source.label.clone(),
            ..Default::default()
        };
        let state = cache::read_state(config_dir, source);
        let recent = !force
            && state.last_error.is_none()
            && state
                .checked_at
                .as_deref()
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .is_some_and(|at| {
                    chrono::Utc::now().signed_duration_since(at)
                        < chrono::Duration::minutes(REFRESH_TTL_MINUTES)
                });
        if recent {
            refresh.outcome = "recent".to_string();
            report.sources.push(refresh);
            continue;
        }
        changed = true;
        let verified = match cache::refresh_index(config_dir, source, orch.library.policy, force) {
            Ok(outcome) => {
                refresh.outcome = match outcome {
                    RefreshOutcome::Updated(_) => "updated",
                    RefreshOutcome::Unchanged(_) => "unchanged",
                }
                .to_string();
                Some(outcome.index().clone())
            }
            Err(error) => {
                log::warn!("Library source {} did not refresh: {error}", source.label);
                refresh.outcome = "failed".to_string();
                refresh.error = Some(error);
                None
            }
        };
        if let Some(verified) = verified {
            refresh.migrated = migrate_moved_packs(config_dir, source, &verified);
            apply_content_updates(orch, source, &verified, &mut refresh);
        }
        report.sources.push(refresh);
    }
    if changed {
        orch.emit_pack_registry_change();
    }
    report
}

fn apply_content_updates(
    orch: &Orchestrator,
    source: &LibrarySource,
    verified: &VerifiedIndex,
    refresh: &mut SourceRefresh,
) {
    for lock in pack::installed_packs(&orch.config_dir) {
        let from_here = lock.source.kind == PackSourceKind::Url
            && lock
                .source
                .url
                .as_deref()
                .is_some_and(|url| source.is_url(url));
        let Some(entry) = verified.entry(&lock.id).filter(|_| from_here) else {
            continue;
        };
        match pending_update(&orch.config_dir, &lock, entry) {
            PendingUpdate::UpToDate => {}
            PendingUpdate::NeedsReview(_) => refresh.awaiting_review.push(lock.id.clone()),
            PendingUpdate::ContentOnly => {
                match crate::resources::mutations::packs::install_library_entry(
                    orch,
                    source,
                    verified,
                    &lock.id,
                    Some(&lock),
                ) {
                    Ok(_) => refresh.applied.push(lock.id.clone()),
                    Err(error) => {
                        log::warn!("Content update of pack `{}` failed: {error}", lock.id);
                        refresh.failed_updates.push(format!("{}: {error}", lock.id));
                    }
                }
            }
        }
    }
}

/// Flip a bundled lock to the Cairn library's provenance once its pack has
/// left the bundle and the verified library index carries it.
///
/// Only `lock.source` changes. Items, baselines, removals, and `mcp.yaml` stay
/// byte-identical, so readiness is unchanged and nothing re-materializes. The
/// connectors moved into the library byte for byte, so their content hash
/// matches and no update appears; if one ever differs, the ordinary update
/// path classifies it.
pub fn migrate_moved_packs(
    config_dir: &Path,
    source: &LibrarySource,
    verified: &VerifiedIndex,
) -> Vec<String> {
    if source.id != CAIRN_LIBRARY_ID {
        return Vec::new();
    }
    // With no recorded bundle there is no way to tell "left the bundle" from
    // "not yet synced", so nothing moves.
    let Some(shipped) = shipped_ids(config_dir) else {
        return Vec::new();
    };
    let mut migrated = Vec::new();
    for mut lock in pack::installed_packs(config_dir) {
        if lock.source.kind != PackSourceKind::Bundled
            || !MOVED_TO_LIBRARY.contains(&lock.id.as_str())
            || shipped.contains(&lock.id)
        {
            continue;
        }
        let Some(entry) = verified.entry(&lock.id) else {
            continue;
        };
        lock.source = PackSource::library(
            source.url.clone(),
            source.git_ref.clone(),
            verified.index.source.commit.clone(),
            verified.signer.clone(),
            None,
            entry.format,
        );
        match pack::lock::rewrite_lock(&RealFileSystem, config_dir, &lock) {
            Ok(()) => migrated.push(lock.id.clone()),
            Err(error) => log::warn!("Failed to move pack `{}` to the library: {error}", lock.id),
        }
    }
    migrated
}
