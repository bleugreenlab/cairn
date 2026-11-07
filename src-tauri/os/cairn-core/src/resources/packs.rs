//! The resource-pack catalog: `cairn://packs` and `cairn://packs/{id}`.
//!
//! One typed projection ([`pack_catalog`]) answers everything a picker needs to
//! offer a pack — identity, state, contents, what the user has modified, and why
//! an installed connector is not doing anything. The markdown an agent reads and
//! the JSON the settings screen renders are two renderings of that one value, so
//! they cannot disagree about what a pack is.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use cairn_common::uri::build_pack_uri;

use crate::config::machine_services::MachineServiceSnapshot;
use crate::config::mcp_servers::{
    workspace_mcp_entries, MachineStateTag, NotReady, WorkspaceMcpEntry,
};
use crate::config::pack::library::cache::{self as library_cache, CachedIndex};
use crate::config::pack::library::{EntryOrigin, EntryRefusal, IndexEntry, LibrarySource};
use crate::config::pack::mcp::McpVarRequirement;
use crate::config::pack::server_config::McpServerConfig;
use crate::config::pack::{
    self, ItemRuns, PackItem, PackItemKind, PackLock, PackManifest, PackSourceKind, UpstreamRef,
};
use crate::library::{LibraryContext, PendingUpdate, RuntimeChange};
use crate::project_services::declaration::{Placement, Transport};
use crate::project_services::pack_source::declared_machine_service;
use crate::project_services::supervisor::machine::HOST_MACHINE;
use cairn_config::fleet::MachineServiceKey;

/// One entry a verified library index offers.
struct LibraryOffer {
    source: LibrarySource,
    signer: String,
    commit: String,
    entry: IndexEntry,
}

/// One pack as the catalog sees it: what the app ships, what a library source
/// offers, what the workspace installed, or some of each.
struct PackView {
    id: String,
    available: Option<PackManifest>,
    offered: Option<LibraryOffer>,
    installed: Option<PackLock>,
}

impl PackView {
    /// What a library source says about this installed library pack.
    fn pending(&self, config_dir: &Path) -> PendingUpdate {
        match (&self.offered, &self.installed) {
            (Some(offer), Some(lock)) if lock.source.kind == PackSourceKind::Url => {
                crate::library::pending_update(config_dir, lock, &offer.entry)
            }
            _ => PendingUpdate::UpToDate,
        }
    }

    /// The root holding what this pack shipped, to compare a user's copy with.
    fn shipped_root(&self, config_dir: &Path) -> Option<std::path::PathBuf> {
        if let Some(manifest) = &self.available {
            return Some(manifest.root.clone());
        }
        let lock = self.installed.as_ref()?;
        let root = crate::library::snapshot_root(config_dir, &lock.id);
        (lock.source.kind != PackSourceKind::Bundled && root.is_dir()).then_some(root)
    }

    fn version(&self) -> Option<String> {
        self.available
            .as_ref()
            .map(|m| m.version.clone())
            .or_else(|| self.offered.as_ref().map(|o| o.entry.version.clone()))
    }
    fn state(&self) -> PackState {
        if self.installed.is_some() {
            PackState::Installed
        } else {
            PackState::Available
        }
    }

    /// True when the shipped pack's contents differ from what the install
    /// recorded — a version bump or any edited file in the source tree.
    fn update_available(&self, resource_dir: Option<&Path>) -> bool {
        if let (Some(offer), Some(lock)) = (&self.offered, &self.installed) {
            return lock.source.kind == PackSourceKind::Url
                && lock.content_hash != offer.entry.content_hash;
        }
        let (Some(manifest), Some(lock)) = (&self.available, &self.installed) else {
            return false;
        };
        if resource_dir.is_none() {
            return false;
        }
        pack::content_hash(&manifest.root)
            .map(|hash| hash != lock.content_hash)
            .unwrap_or(false)
    }

    /// The pack's live item set: what the install recorded, or — for a pack that
    /// is only offered — what its source ships.
    fn items(&self) -> Vec<PackItem> {
        match (&self.installed, &self.available) {
            (Some(lock), _) => lock
                .items
                .iter()
                .map(|item| PackItem {
                    kind: item.kind,
                    id: item.id.clone(),
                    path: item.path.clone(),
                })
                .collect(),
            (None, Some(manifest)) => manifest.items(),
            (None, None) => self
                .offered
                .iter()
                .flat_map(|offer| offer.entry.items.iter())
                .map(|item| PackItem {
                    kind: item.kind,
                    id: item.id.clone(),
                    path: item.path.clone(),
                })
                .collect(),
        }
    }

    fn name(&self) -> &str {
        self.available
            .as_ref()
            .map(|m| m.name.as_str())
            .or_else(|| self.offered.as_ref().map(|o| o.entry.name.as_str()))
            .or_else(|| self.installed.as_ref().map(|l| l.name.as_str()))
            .unwrap_or(&self.id)
    }

    fn description(&self) -> &str {
        self.available
            .as_ref()
            .map(|m| m.description.as_str())
            .or_else(|| self.offered.as_ref().map(|o| o.entry.description.as_str()))
            .or_else(|| self.installed.as_ref().map(|l| l.description.as_str()))
            .unwrap_or("")
    }

    /// What the source says about this pack, then what the install recorded
    /// that the source does not: an index derived by an older build cannot say
    /// what this build's sync left out.
    fn notes(&self) -> Vec<String> {
        let mut notes = self
            .available
            .as_ref()
            .map(|m| m.notes.clone())
            .or_else(|| self.offered.as_ref().map(|o| o.entry.notes.clone()))
            .unwrap_or_default();
        for note in self.installed.iter().flat_map(|lock| &lock.notes) {
            if !notes.contains(note) {
                notes.push(note.clone());
            }
        }
        notes
    }

    /// The definition of one MCP item: the live registry's, else the offered
    /// index's, else the bundled pack's.
    fn mcp_definition(
        &self,
        id: &str,
        mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
    ) -> Option<McpServerConfig> {
        mcp_entries
            .get(id)
            .map(|entry| entry.config.clone())
            .or_else(|| {
                self.offered
                    .as_ref()
                    .and_then(|offer| offer.entry.item(PackItemKind::Mcp, id))
                    .and_then(|item| item.definition.clone())
            })
            .or_else(|| {
                let path = self.available.as_ref()?.mcp_source()?;
                pack::mcp::parse_pack_mcp_file(&path)
                    .ok()?
                    .servers
                    .remove(id)
            })
    }
}

/// Whether the workspace has this pack, or is merely being offered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackState {
    Installed,
    Available,
}

impl PackState {
    pub fn as_str(self) -> &'static str {
        match self {
            PackState::Installed => "installed",
            PackState::Available => "available",
        }
    }
}

/// Whether one item of an installed pack is what its name resolves to.
///
/// A pack's item is read where the pack installed it and never edited, so
/// this is a fact about resolution, not ownership: the pack's item is in use,
/// or a definition of the user's is in front of it, or another pack defines
/// the same name and resolving it is refused, or the user removed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackItemState {
    InUse,
    Shadowed,
    Collides,
    RemovedByUser,
}

impl PackItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            PackItemState::InUse => "in-use",
            PackItemState::Shadowed => "shadowed",
            PackItemState::Collides => "collides",
            PackItemState::RemovedByUser => "removed-by-user",
        }
    }
}

/// One installable unit of a pack, with the state the workspace has put it in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCatalogItem {
    pub kind: String,
    pub id: String,
    /// Workspace-relative destination, absent for an MCP server (which lives in
    /// the pack's own `mcp.yaml` rather than the flat workspace layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub state: PackItemState,
    /// For a shadowed item, what is in front of it: `~` (the workspace) for a
    /// library item, `settings` for an MCP server the user defines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadowed_by: Option<String>,
    /// For a colliding item, the other installed packs defining its name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collides_with: Vec<String>,
    /// Why an MCP item cannot connect yet, straight from the registry that
    /// decides it. The UI renders this verdict; it never re-derives one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_ready: Option<NotReady>,
    /// What to ask the user for, one entry per variable [`Self::not_ready`]
    /// names. This is what the pack detail screen turns into a labelled field
    /// with a file picker instead of a variable name and no way to act on it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<McpVarRequirement>,
    /// Whether this item is content, a remote endpoint, or runs here.
    #[serde(default)]
    pub runs: ItemRuns,
    /// For an endpoint: the host it connects to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connects_to: Option<String>,
    /// For a live service item of an installed pack: which machines it can run
    /// on and how each stands. The Library renders this; it never derives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_service: Option<MachineServiceItem>,
    /// For a web-service provider: the slot it fills, whether it is selected,
    /// and its credential's presence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_provider: Option<WebProviderItem>,
    /// For a credential: where its value may be sent and whether one is stored.
    /// Never the value, in any presentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential: Option<CredentialItem>,
}

/// A pack's web-service provider, as the Library and `cairn://packs` show it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebProviderItem {
    pub slot: String,
    pub label: String,
    /// Whether the slot's scalar selects this pack. Always false for a pack
    /// that is not installed.
    pub active: bool,
    /// The credential an `http` provider sends; absent for an MCP binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential: Option<CredentialItem>,
    /// The MCP server an `mcp` provider calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_server: Option<String>,
}

/// A pack credential's declaration and presence. Never its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialItem {
    pub name: String,
    pub label: String,
    /// `secret` or `oauth`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub var: Option<String>,
    /// A secret is stored, or an OAuth token is authorized. False for a pack
    /// that is not installed.
    pub present: bool,
    pub hosts: Vec<String>,
}

/// A machine service's standing across the fleet, as of this read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineServiceItem {
    /// `<pack>/<service>`: the key a selection is written under.
    pub key: String,
    /// How the service is placed: selected per machine, or delivered to the
    /// whole fleet with only overrides in the row.
    pub placement: MachineServicePlacement,
    /// The version the shipped declaration pins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_version: Option<String>,
    /// `mcp-stdio`, `none`, or `ndjson`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    /// Why the pack's own artifact may not run here, so a new selection would
    /// be refused: its declaration does not parse, or its copy was edited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    pub machines: Vec<MachineServiceMachine>,
}

/// `placement:` as the Library reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MachineServicePlacement {
    /// One resident on each machine the fleet row selects it on.
    PerMachine,
    /// Delivered to every machine it ships for; launched on demand.
    Fleet,
}

/// One machine's row: a machine the service ships an artifact for, or one that
/// selected it whatever its platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineServiceMachine {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub attached: bool,
    /// Whether this is the machine the runner runs on, whose files a native
    /// picker here can browse.
    pub this_machine: bool,
    /// Whether the machine has an instance: its key is in the fleet row, or
    /// the service is delivered to the whole fleet.
    pub selected: bool,
    /// The rest describe the instance, so only a selected machine has them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<MachineStateTag>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub override_command: Option<String>,
    /// What it runs and who chose it, in the instance's own words.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<String>,
}

/// How many live items of one kind a pack carries, for the compact
/// "13 agents · 6 recipes · 11 skills" line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackContentCount {
    pub kind: String,
    pub count: usize,
}

/// Where an entry's pack came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackSourceInfo {
    /// `bundled` (shipped in the app), `url` (a library source), or `local`.
    pub kind: String,
    /// What the Library calls the source: `Bundled`, `Cairn library`,
    /// `Local import`, or `Unconfigured source`.
    #[serde(default)]
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The trusted key that signed the index this came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer: Option<String>,
    /// The source commit installed (or offered).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Another repository the content is fetched from, at a pinned commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<UpstreamRef>,
    /// A local import's original directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// A URL pack whose source this build no longer reads: it keeps working,
    /// and cannot update.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub updates_frozen: bool,
}

/// One configured library source, as its verified cache stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LibrarySourceStatus {
    pub id: String,
    pub label: String,
    pub url: String,
    /// `verified`, `unverified` (a cached index that does not verify),
    /// `unreachable` (never fetched, and the last contact failed), or
    /// `not-fetched`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<String>,
    /// Why the last contact failed. The last verified index stays in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Why a cached index is not believed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// Entries the verified index carries that are not offered here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused_entries: Vec<EntryRefusal>,
}

/// One pack, projected for both renderings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCatalogEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub state: PackState,
    /// Version recorded by the install, absent for a pack that is only offered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_version: Option<String>,
    /// Version the app currently ships, absent for an installed-only pack whose
    /// source is no longer on this machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shipped_version: Option<String>,
    pub update_available: bool,
    pub installs_by_default: bool,
    /// Whether the user uninstalled this pack and has not since reinstalled it.
    /// An uninstall outranks the default set, so this is why a `default: true`
    /// pack can sit in the catalog as available.
    pub uninstalled_by_user: bool,
    pub source: PackSourceInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// The most consequential thing any of its items does here.
    #[serde(default)]
    pub runs: ItemRuns,
    /// The pending update changes an endpoint or something that runs here, so
    /// it waits for the operator instead of applying on its own.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub update_needs_review: bool,
    /// What that update would change, from the signed index and the installed
    /// layer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_changes: Vec<RuntimeChange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// Every item the pack carries, including the ones the user removed, sorted
    /// by kind then id.
    pub items: Vec<PackCatalogItem>,
    /// Live (non-removed) item counts by kind.
    pub counts: Vec<PackContentCount>,
    /// How many items a definition of the user's stands in front of, the line
    /// a pack row states so a shadowed item is visible without opening it.
    pub shadowed_count: usize,
    /// The catalog section this entry belongs in: [`PACK_ENTRY_KIND`] for a
    /// bundle, or the kind of the one item it ships. See [`entry_kind`].
    pub entry_kind: String,
}

/// The section an entry that bundles several items belongs in.
pub const PACK_ENTRY_KIND: &str = "pack";

/// Which section an entry belongs in.
///
/// A manifest that ships exactly one item *is* that item: the Linear connector
/// is an MCP server, not a pack of one, and a pack is a set of related things
/// that travel together. Deriving this from content rather than declaring it in
/// the manifest is what lets an entry move between the two without ceremony —
/// the day the Linear channel item joins that manifest, the same id becomes a
/// pack with its lock, install state, and destination paths untouched.
///
/// Every item counts, tombstoned ones included, because the grouping follows
/// what the manifest ships rather than what this workspace currently holds:
/// removing one skill from a two-item pack must not reclassify it.
///
/// A web-service provider is the one exception: it reads a credential, so a
/// pack holding only providers and the credentials they read is a provider,
/// not a pack of two. The day it also ships a skill it becomes a pack.
fn entry_kind(items: &[PackCatalogItem]) -> String {
    let provider = PackItemKind::Provider.as_str();
    let credential = PackItemKind::Credential.as_str();
    if items.iter().any(|item| item.kind == provider)
        && items
            .iter()
            .all(|item| item.kind == provider || item.kind == credential)
    {
        return provider.to_string();
    }
    match items {
        [only] => only.kind.clone(),
        _ => PACK_ENTRY_KIND.to_string(),
    }
}

impl PackCatalogEntry {
    /// Items a definition of the user's stands in front of.
    fn shadowed_items(&self) -> Vec<&PackCatalogItem> {
        self.items
            .iter()
            .filter(|item| item.state == PackItemState::Shadowed)
            .collect()
    }

    fn removed_items(&self) -> Vec<&PackCatalogItem> {
        self.items
            .iter()
            .filter(|item| item.state == PackItemState::RemovedByUser)
            .collect()
    }
}

/// Every pack this workspace knows about, plus whether it can install from
/// anywhere at all.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCatalog {
    /// False when this workspace has no recorded app resource directory yet, so
    /// only already-installed packs are listed and nothing can be installed. It
    /// is recorded on the next startup sync.
    pub source_recorded: bool,
    pub packs: Vec<PackCatalogEntry>,
    /// Every configured library source and whether its index is believed.
    #[serde(default)]
    pub sources: Vec<LibrarySourceStatus>,
}

impl PackCatalog {
    pub fn find(&self, pack_id: &str) -> Option<&PackCatalogEntry> {
        self.packs.iter().find(|entry| entry.id == pack_id)
    }
}

/// Every pack this workspace knows about: shipped by the app, offered by a
/// library source, installed, or several of these. Sorted by id.
///
/// A library entry whose id the app bundle also ships is not offered: the
/// bundled pack wins, and the duplicate is listed on its source as refused.
fn pack_views(
    resource_dir: Option<&Path>,
    config_dir: &Path,
    library: &LibraryContext,
) -> (Vec<PackView>, Vec<LibrarySourceStatus>) {
    let mut by_id: BTreeMap<String, PackView> = BTreeMap::new();
    let view = |id: &str| PackView {
        id: id.to_string(),
        available: None,
        offered: None,
        installed: None,
    };

    if let Some(resource_dir) = resource_dir {
        for manifest in pack::discover_available_packs(resource_dir) {
            let id = manifest.id.clone();
            by_id
                .entry(id.clone())
                .or_insert_with(|| view(&id))
                .available = Some(manifest);
        }
    }

    let mut statuses = Vec::new();
    for source in &library.sources {
        let state = library_cache::read_state(config_dir, source);
        let mut status = LibrarySourceStatus {
            id: source.id.clone(),
            label: source.label.clone(),
            url: source.url.clone(),
            state: String::new(),
            signer: None,
            commit: None,
            serial: None,
            checked_at: state.checked_at.clone(),
            last_error: state.last_error.clone(),
            refusal: None,
            refused_entries: Vec::new(),
        };
        match library_cache::read_cached_index(config_dir, source) {
            CachedIndex::Absent => {
                status.state = if state.last_error.is_some() {
                    "unreachable"
                } else {
                    "not-fetched"
                }
                .to_string();
            }
            CachedIndex::Refused(reason) => {
                status.state = "unverified".to_string();
                status.refusal = Some(reason);
            }
            CachedIndex::Verified(verified) => {
                status.state = "verified".to_string();
                status.signer = Some(verified.signer.clone());
                status.commit = Some(verified.index.source.commit.clone());
                status.serial = Some(verified.index.serial);
                status.refused_entries = verified.refused.clone();
                for entry in &verified.index.entries {
                    let slot = by_id
                        .entry(entry.id.clone())
                        .or_insert_with(|| view(&entry.id));
                    if slot.available.is_some() {
                        status.refused_entries.push(EntryRefusal {
                            id: entry.id.clone(),
                            reason: format!(
                                "`{}` ships with this app, so the {}'s `{}` is not offered here",
                                entry.id, source.label, entry.id
                            ),
                        });
                        continue;
                    }
                    if slot.offered.is_none() {
                        slot.offered = Some(LibraryOffer {
                            source: source.clone(),
                            signer: verified.signer.clone(),
                            commit: verified.index.source.commit.clone(),
                            entry: entry.clone(),
                        });
                    }
                }
            }
        }
        statuses.push(status);
    }

    for lock in pack::installed_packs(config_dir) {
        let id = lock.id.clone();
        by_id
            .entry(id.clone())
            .or_insert_with(|| view(&id))
            .installed = Some(lock);
    }
    // An offer only matters to the pack it came from: a library entry sharing
    // an id with a local import is not that import's update.
    for view in by_id.values_mut() {
        let foreign = match (&view.installed, &view.offered) {
            (Some(lock), Some(offer)) => !lock
                .source
                .url
                .as_deref()
                .is_some_and(|url| offer.source.is_url(url)),
            _ => false,
        };
        if foreign {
            view.offered = None;
        }
    }

    (
        by_id
            .into_values()
            .filter(|view| {
                view.available.is_some() || view.offered.is_some() || view.installed.is_some()
            })
            .collect(),
        statuses,
    )
}

/// The typed catalog for `config_dir`, reading `library`'s verified caches.
/// Never touches the network.
pub fn pack_catalog(
    config_dir: &Path,
    library: &LibraryContext,
    machines: &MachineServiceSnapshot,
) -> PackCatalog {
    let resource_dir = pack::source_dir(config_dir);
    let mcp_entries = workspace_mcp_entries(config_dir, machines);
    let (views, sources) = pack_views(resource_dir.as_deref(), config_dir, library);
    let resolved =
        crate::config::library_index(&cairn_config::library::LibraryRoots::workspace(config_dir));
    let packs = views
        .iter()
        .map(|view| {
            project(
                config_dir,
                resource_dir.as_deref(),
                view,
                &mcp_entries,
                &resolved,
                library,
                machines,
            )
        })
        .collect();
    PackCatalog {
        source_recorded: resource_dir.is_some(),
        packs,
        sources,
    }
}

fn project(
    config_dir: &Path,
    resource_dir: Option<&Path>,
    view: &PackView,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
    resolved: &ResolvedIndex,
    library: &LibraryContext,
    machines: &MachineServiceSnapshot,
) -> PackCatalogEntry {
    let classify = |entry: &mut PackCatalogItem, kind: PackItemKind| {
        let definition = (kind == PackItemKind::Mcp)
            .then(|| view.mcp_definition(&entry.id, mcp_entries))
            .flatten();
        entry.runs = ItemRuns::classify(kind, definition.as_ref());
        entry.connects_to = (entry.runs == ItemRuns::Endpoint)
            .then(|| {
                definition
                    .as_ref()
                    .and_then(|d| d.url.as_deref())
                    .and_then(host_of)
            })
            .flatten();
    };
    let mut items: Vec<PackCatalogItem> = view
        .items()
        .into_iter()
        .map(|item| {
            let kind = item.kind;
            let resolution = item_resolution(&view.id, &item, mcp_entries, resolved);
            let mut entry = catalog_item(item, resolution.state, mcp_entries);
            entry.shadowed_by = resolution.shadowed_by;
            entry.collides_with = resolution.collides_with;
            classify(&mut entry, kind);
            if kind == PackItemKind::Service && view.installed.is_some() {
                entry.machine_service =
                    machine_service_item(config_dir, &view.id, &entry.id, machines);
            }
            if kind.in_pack_layer() {
                describe_web_service_item(config_dir, view, &mut entry, kind, mcp_entries);
            }
            entry
        })
        .collect();
    if let Some(lock) = &view.installed {
        items.extend(lock.removed.iter().cloned().map(|item| {
            let kind = item.kind;
            let mut entry = catalog_item(item, PackItemState::RemovedByUser, mcp_entries);
            classify(&mut entry, kind);
            entry
        }));
    }
    let pending = view.pending(config_dir);
    items.sort_by(|a, b| (a.kind.as_str(), a.id.as_str()).cmp(&(b.kind.as_str(), b.id.as_str())));
    items.dedup_by(|a, b| a.kind == b.kind && a.id == b.id);

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for item in items
        .iter()
        .filter(|item| item.state != PackItemState::RemovedByUser)
    {
        *counts.entry(item.kind.as_str()).or_default() += 1;
    }

    PackCatalogEntry {
        id: view.id.clone(),
        name: view.name().to_string(),
        description: view.description().trim().to_string(),
        state: view.state(),
        installed_version: view.installed.as_ref().map(|l| l.version.clone()),
        shipped_version: view.version(),
        update_available: view.update_available(resource_dir),
        installs_by_default: view.available.as_ref().is_some_and(|m| m.default),
        uninstalled_by_user: view.installed.is_none()
            && pack::lock::is_uninstalled(config_dir, &view.id),
        source: source_info(view, library),
        license: view
            .available
            .as_ref()
            .and_then(|m| m.license.clone())
            .or_else(|| view.offered.as_ref().and_then(|o| o.entry.license.clone())),
        runs: items
            .iter()
            .filter(|item| item.state != PackItemState::RemovedByUser)
            .map(|item| item.runs)
            .max()
            .unwrap_or_default(),
        update_needs_review: matches!(pending, PendingUpdate::NeedsReview(_)),
        runtime_changes: match pending {
            PendingUpdate::NeedsReview(changes) => changes,
            _ => Vec::new(),
        },
        author: view
            .available
            .as_ref()
            .and_then(|m| m.author.as_ref())
            .or_else(|| view.offered.as_ref().and_then(|o| o.entry.author.as_ref()))
            .or_else(|| view.installed.as_ref().and_then(|l| l.author.as_ref()))
            .and_then(|a| a.name.clone()),
        homepage: view
            .available
            .as_ref()
            .and_then(|m| m.homepage.clone())
            .or_else(|| view.offered.as_ref().and_then(|o| o.entry.homepage.clone()))
            .or_else(|| view.installed.as_ref().and_then(|l| l.homepage.clone())),
        keywords: view
            .available
            .as_ref()
            .map(|m| m.keywords.clone())
            .or_else(|| view.offered.as_ref().map(|o| o.entry.keywords.clone()))
            .or_else(|| view.installed.as_ref().map(|l| l.keywords.clone()))
            .unwrap_or_default(),
        installed_at: view.installed.as_ref().map(|l| l.installed_at.clone()),
        notes: view.notes(),
        counts: counts
            .into_iter()
            .map(|(kind, count)| PackContentCount {
                kind: kind.to_string(),
                count,
            })
            .collect(),
        entry_kind: entry_kind(&items),
        shadowed_count: items
            .iter()
            .filter(|item| item.state == PackItemState::Shadowed)
            .count(),
        items,
    }
}

/// The Library's per-machine view of one installed service: the machines its
/// declaration ships an artifact for, plus any selected one whatever its target.
fn machine_service_item(
    config_dir: &Path,
    pack_id: &str,
    service: &str,
    machines: &MachineServiceSnapshot,
) -> Option<MachineServiceItem> {
    let key = MachineServiceKey::new(pack_id, service).ok()?;
    let declared = declared_machine_service(config_dir, &key);
    let package = declared.as_ref().and_then(|d| d.package.as_ref().ok());
    let native = package.and_then(|package| package.declaration.native());
    let views = machines.views(&key);
    let rows = machines
        .machines
        .iter()
        .filter_map(|candidate| {
            let view = views.iter().find(|view| view.machine == candidate.name);
            let ships_here = candidate
                .target
                .as_deref()
                .zip(native)
                .is_some_and(|(target, native)| native.targets.contains_key(target));
            if view.is_none() && !ships_here {
                return None;
            }
            let verdict = view.map(|view| view.state.verdict(&view.machine));
            Some(MachineServiceMachine {
                name: candidate.name.clone(),
                target: candidate.target.clone(),
                attached: candidate.attached,
                this_machine: candidate.name == HOST_MACHINE,
                selected: view.is_some(),
                state: verdict.as_ref().map(|verdict| verdict.state),
                reason: verdict.and_then(|verdict| verdict.reason),
                installed_version: view.and_then(|view| view.installed_version.clone()),
                override_command: view.and_then(|view| view.override_command.clone()),
                provenance: view.map(|view| view.provenance.clone()),
            })
        })
        .collect();
    Some(MachineServiceItem {
        key: key.to_string(),
        placement: match package.map(|package| package.declaration.placement) {
            Some(Placement::Fleet) => MachineServicePlacement::Fleet,
            _ => MachineServicePlacement::PerMachine,
        },
        pinned_version: native.map(|native| native.version.clone()),
        transport: package.map(|package| {
            match package.declaration.transport {
                Transport::McpStdio => "mcp-stdio",
                Transport::None => "none",
                Transport::Ndjson => "ndjson",
            }
            .to_string()
        }),
        refusal: declared.as_ref().and_then(|declared| {
            declared
                .refusal
                .clone()
                .or_else(|| declared.package.as_ref().err().cloned())
        }),
        machines: rows,
    })
}

/// What each bare name resolves to in the workspace (see
/// [`crate::config::library_index`]).
type ResolvedIndex = BTreeMap<
    (cairn_config::library::LibraryKind, String),
    (
        cairn_config::library::Origin,
        cairn_config::library::ListedStatus,
    ),
>;

/// Fill a provider or credential item from its declaration: the installed
/// layer when the pack is installed (with the credential's presence and the
/// provider's readiness), else the shipped source (presence always false).
fn describe_web_service_item(
    config_dir: &Path,
    view: &PackView,
    entry: &mut PackCatalogItem,
    kind: PackItemKind,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
) {
    let installed = view.installed.is_some();
    let root = if installed {
        Some(pack::lock::pack_dir(config_dir, &view.id))
    } else {
        view.shipped_root(config_dir)
    };
    let Some(root) = root else {
        return;
    };
    let credential_item = |credential: &pack::provider::CredentialDecl| {
        let view = crate::web_providers::CredentialView::of(&view.id, credential, installed);
        CredentialItem {
            name: view.name,
            label: view.label,
            kind: view.kind.to_string(),
            var: view.var,
            present: view.present,
            hosts: view.hosts,
        }
    };
    match kind {
        PackItemKind::Provider => {
            let Some(slot) = pack::provider::Slot::parse(&entry.id) else {
                return;
            };
            let Ok(Some(provider)) = pack::provider::load_pack_provider(&root, &view.id, slot)
            else {
                return;
            };
            // A bound provider runs what its server runs, and reaches the
            // server's host; an HTTP provider reaches the host it names.
            let bound = match &provider.provider.adapter {
                pack::provider::Adapter::Mcp(binding) => {
                    view.mcp_definition(&binding.server, mcp_entries)
                }
                pack::provider::Adapter::Http(_) => None,
            };
            entry.runs = ItemRuns::classify(PackItemKind::Provider, bound.as_ref());
            entry.connects_to = match (&provider.provider.http(), &bound) {
                (Some(adapter), _) => Some(adapter.request.host.clone()),
                (None, Some(server)) if entry.runs == ItemRuns::Endpoint => {
                    server.url.as_deref().and_then(host_of)
                }
                _ => None,
            };
            if installed {
                entry.not_ready = crate::web_providers::readiness(config_dir, &provider);
            }
            entry.web_provider = Some(WebProviderItem {
                slot: slot.as_str().to_string(),
                label: provider.provider.label.clone(),
                active: installed
                    && crate::web_providers::active_name(config_dir, slot).as_deref()
                        == Some(view.id.as_str()),
                credential: provider.credential.as_ref().map(credential_item),
                mcp_server: match &provider.provider.adapter {
                    pack::provider::Adapter::Mcp(binding) => Some(binding.server.clone()),
                    pack::provider::Adapter::Http(_) => None,
                },
            });
        }
        PackItemKind::Credential => {
            let Ok(credentials) = pack::provider::load_pack_credentials(&root, &view.id) else {
                return;
            };
            let Some(credential) = credentials.iter().find(|c| c.name == entry.id) else {
                return;
            };
            entry.connects_to = Some(credential.hosts.join(", "));
            if installed {
                entry.not_ready = crate::web_providers::credential_readiness(&view.id, credential);
            }
            entry.credential = Some(credential_item(credential));
        }
        _ => {}
    }
}

fn source_info(view: &PackView, library: &LibraryContext) -> PackSourceInfo {
    let bundled = || PackSourceInfo {
        kind: "bundled".to_string(),
        label: "Bundled".to_string(),
        ..Default::default()
    };
    match (&view.installed, &view.offered) {
        (Some(lock), _) => match lock.source.kind {
            PackSourceKind::Bundled => bundled(),
            PackSourceKind::Local => PackSourceInfo {
                kind: "local".to_string(),
                label: "Local import".to_string(),
                path: lock.source.path.clone(),
                ..Default::default()
            },
            PackSourceKind::Url => {
                let configured = lock
                    .source
                    .url
                    .as_deref()
                    .and_then(|url| library.source_for_url(url));
                PackSourceInfo {
                    kind: "url".to_string(),
                    label: configured
                        .map(|source| source.label.clone())
                        .unwrap_or_else(|| "Unconfigured source".to_string()),
                    url: lock.source.url.clone(),
                    signer: lock.source.signer.clone(),
                    commit: lock.source.sha.clone(),
                    upstream: lock.source.upstream.clone(),
                    path: None,
                    updates_frozen: configured.is_none(),
                }
            }
        },
        (None, Some(offer)) if view.available.is_none() => PackSourceInfo {
            kind: "url".to_string(),
            label: offer.source.label.clone(),
            url: Some(offer.source.url.clone()),
            signer: Some(offer.signer.clone()),
            commit: Some(offer.commit.clone()),
            upstream: match &offer.entry.origin {
                EntryOrigin::Upstream { url, commit, .. } => Some(UpstreamRef {
                    url: url.clone(),
                    sha: commit.clone(),
                }),
                EntryOrigin::Source { .. } => None,
            },
            ..Default::default()
        },
        _ => bundled(),
    }
}

/// The host part of a URL, for "Connects to <host>".
fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit('@').next()?;
    (!host.is_empty()).then(|| host.to_string())
}

struct ItemResolution {
    state: PackItemState,
    shadowed_by: Option<String>,
    collides_with: Vec<String>,
}

/// Whether one live item of pack `pack_id` is what its name resolves to.
///
/// A library item is shadowed when a definition of the user's resolves in
/// front of it, and collides when another pack defines the same name with
/// nothing local in front. An MCP server is shadowed when the user's own
/// `settings.yaml` defines a server of that name, which the registry then
/// attributes to the workspace instead of this pack.
fn item_resolution(
    pack_id: &str,
    item: &PackItem,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
    resolved: &ResolvedIndex,
) -> ItemResolution {
    use cairn_config::library::{LibraryKind, ListedStatus, Origin};
    let in_use = ItemResolution {
        state: PackItemState::InUse,
        shadowed_by: None,
        collides_with: Vec::new(),
    };
    if item.kind == PackItemKind::Mcp {
        let shadowed = mcp_entries
            .get(&item.id)
            .is_some_and(|entry| entry.origin != format!("pack:{pack_id}"));
        return if shadowed {
            ItemResolution {
                state: PackItemState::Shadowed,
                shadowed_by: Some("settings".to_string()),
                collides_with: Vec::new(),
            }
        } else {
            in_use
        };
    }
    let Some(kind) = LibraryKind::from_pack_kind(item.kind) else {
        return in_use;
    };
    match resolved.get(&(kind, item.id.clone())) {
        Some((Origin::Pack { id }, ListedStatus::Active)) if id == pack_id => in_use,
        Some((origin, ListedStatus::Active)) => ItemResolution {
            state: PackItemState::Shadowed,
            shadowed_by: Some(origin.label()),
            collides_with: Vec::new(),
        },
        Some((_, ListedStatus::Collision { packs })) => ItemResolution {
            state: PackItemState::Collides,
            shadowed_by: None,
            collides_with: packs.iter().filter(|p| *p != pack_id).cloned().collect(),
        },
        _ => in_use,
    }
}

fn catalog_item(
    item: PackItem,
    state: PackItemState,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
) -> PackCatalogItem {
    let entry = (item.kind == PackItemKind::Mcp)
        .then(|| mcp_entries.get(&item.id))
        .flatten();
    PackCatalogItem {
        kind: item.kind.as_str().to_string(),
        id: item.id,
        path: item.path,
        state,
        shadowed_by: None,
        collides_with: Vec::new(),
        not_ready: entry.and_then(|e| e.not_ready.clone()),
        setup: entry.map(|e| e.setup.clone()).unwrap_or_default(),
        runs: ItemRuns::Content,
        connects_to: None,
        machine_service: None,
        web_provider: None,
        credential: None,
    }
}

/// The pack's text of one item beside the user's own definition of the same
/// name, for a side-by-side view of what that definition changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackItemDiff {
    pub pack_id: String,
    pub kind: String,
    pub item_id: String,
    /// One entry per file that differs. A single-file item has at most one; a
    /// skill or workflow lists only the files its two trees disagree on.
    pub files: Vec<PackItemFileDiff>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackItemFileDiff {
    /// The item-relative path, the same in the pack and in the workspace.
    pub path: String,
    /// Absent when the pack's item has no such file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shipped: Option<String>,
    /// Absent when your definition has no such file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yours: Option<String>,
    /// True when either side is not UTF-8; neither text is sent.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub binary: bool,
}

/// Compare one item of an installed pack, as installed under `packs/<id>/`,
/// with the workspace's own definition of the same name, which sits at the
/// same relative path in the workspace.
pub fn pack_item_diff(
    config_dir: &Path,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
) -> Result<PackItemDiff, String> {
    let lock = pack::lock::read_lock(config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    let item = lock
        .items
        .iter()
        .map(pack::PackLockItem::manifest_item)
        .chain(lock.removed.iter().cloned())
        .find(|item| item.kind == kind && item.id == item_id)
        .ok_or_else(|| {
            format!(
                "Pack `{pack_id}` does not carry {} `{item_id}`",
                kind.as_str()
            )
        })?;
    let path = item
        .path
        .ok_or_else(|| format!("{} `{item_id}` has no file to compare", kind.as_str()))?;
    let shipped_root = pack::item_destination(config_dir, pack_id, kind, &path);
    let yours_root = config_dir.join(&path);

    let mut files = Vec::new();
    if matches!(
        kind,
        PackItemKind::Service | PackItemKind::Skill | PackItemKind::Workflow
    ) {
        let shipped = tree_texts(&shipped_root)?;
        let yours = tree_texts(&yours_root)?;
        let names: std::collections::BTreeSet<&String> =
            shipped.keys().chain(yours.keys()).collect();
        for name in names {
            let (a, b) = (shipped.get(name), yours.get(name));
            if a != b {
                files.push(file_diff(format!("{path}/{name}"), a.cloned(), b.cloned()));
            }
        }
    } else {
        let read = |path: &Path| path.is_file().then(|| std::fs::read(path).ok()).flatten();
        let (a, b) = (read(&shipped_root), read(&yours_root));
        if a != b {
            files.push(file_diff(path.clone(), a, b));
        }
    }
    Ok(PackItemDiff {
        pack_id: pack_id.to_string(),
        kind: kind.as_str().to_string(),
        item_id: item_id.to_string(),
        files,
    })
}

fn tree_texts(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut out = BTreeMap::new();
    if !root.is_dir() {
        return Ok(out);
    }
    for file in crate::services::guarded_tree_files(root)? {
        if pack::is_os_metadata_path(&file.relative_path) {
            continue;
        }
        let name = file
            .relative_path
            .components()
            .map(|part| part.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let bytes = std::fs::read(&file.resolved_path)
            .map_err(|e| format!("Failed to read {:?}: {e}", file.resolved_path))?;
        out.insert(name, bytes);
    }
    Ok(out)
}

fn file_diff(path: String, shipped: Option<Vec<u8>>, yours: Option<Vec<u8>>) -> PackItemFileDiff {
    let text = |bytes: Option<Vec<u8>>| bytes.map(String::from_utf8);
    match (text(shipped), text(yours)) {
        (Some(Err(_)), _) | (_, Some(Err(_))) => PackItemFileDiff {
            path,
            shipped: None,
            yours: None,
            binary: true,
        },
        (shipped, yours) => PackItemFileDiff {
            path,
            shipped: shipped.map(Result::unwrap),
            yours: yours.map(Result::unwrap),
            binary: false,
        },
    }
}

// ── Markdown rendering ──────────────────────────────────────────────────────
//
// Both readers below render the projection above and nothing else, which is
// what keeps an agent's view of a pack and the settings screen's view the same
// view.

/// One item's label: its id, plus whatever the workspace has done to it.
fn item_label(item: &PackCatalogItem) -> String {
    let mut label = item.id.clone();
    if let Some(reason) = &item.not_ready {
        label.push_str(&format!(" (not ready: {})", reason.summary()));
    }
    if let Some(service) = &item.machine_service {
        let selected: Vec<String> = service
            .machines
            .iter()
            .filter(|machine| machine.selected)
            .map(|machine| {
                format!(
                    "{}: {}",
                    machine.name,
                    machine_state_words(machine, service.placement)
                )
            })
            .collect();
        label.push_str(&if selected.is_empty() {
            match service.placement {
                MachineServicePlacement::PerMachine => " (not selected on any machine)",
                MachineServicePlacement::Fleet => " (on no machine it ships for)",
            }
            .to_string()
        } else {
            format!(" ({})", selected.join(", "))
        });
    }
    match item.state {
        PackItemState::Shadowed => label.push_str(&format!(
            " (shadowed by {})",
            match item.shadowed_by.as_deref() {
                Some("settings") => "your settings.yaml".to_string(),
                Some(origin) => format!("your {origin} definition"),
                None => "your definition".to_string(),
            }
        )),
        PackItemState::Collides => label.push_str(&format!(
            " (collides with {})",
            item.collides_with
                .iter()
                .map(|pack| format!("pack:{pack}"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        PackItemState::RemovedByUser => label.push_str(" (removed by you)"),
        PackItemState::InUse => {}
    }
    label
}

/// One selected machine's state as a phrase. A fleet service's ready machine
/// has the artifact installed; what it needs to work is the program's to say.
fn machine_state_words(
    machine: &MachineServiceMachine,
    placement: MachineServicePlacement,
) -> String {
    let word = match machine.state {
        Some(MachineStateTag::NotInstalled) | None => "not installed",
        Some(MachineStateTag::Installing) => "installing",
        Some(MachineStateTag::MachineAbsent) => "machine absent",
        Some(MachineStateTag::Starting) => "starting",
        Some(MachineStateTag::WaitingForOperator) => "waiting for the operator",
        Some(MachineStateTag::Ready) if placement == MachineServicePlacement::Fleet => "installed",
        Some(MachineStateTag::Ready) => "ready",
        Some(MachineStateTag::Degraded) => "degraded",
        Some(MachineStateTag::Failed) => "failed",
    };
    match &machine.reason {
        Some(reason) => format!("{word}: {reason}"),
        None => word.to_string(),
    }
}

/// Every machine service item's machines, one line each, and how to change them.
fn render_machine_services(out: &mut String, entry: &PackCatalogEntry) {
    for item in &entry.items {
        let Some(service) = &item.machine_service else {
            continue;
        };
        out.push_str(&format!("## Machine service `{}`\n\n", service.key));
        if let Some(version) = &service.pinned_version {
            out.push_str(&format!("Pinned version: {version}\n"));
        }
        if let Some(refusal) = &service.refusal {
            out.push_str(&format!("Refused: {refusal}\n"));
        }
        out.push('\n');
        for machine in &service.machines {
            let standing = if machine.selected {
                machine_state_words(machine, service.placement)
            } else {
                "not selected".to_string()
            };
            let runs = machine
                .provenance
                .as_deref()
                .map(|provenance| format!(" · {provenance}"))
                .unwrap_or_default();
            out.push_str(&format!("- {}: {standing}{runs}\n", machine.name));
        }
        if service.machines.is_empty() {
            out.push_str("- No machine this service ships an artifact for.\n");
        }
        match service.placement {
            MachineServicePlacement::PerMachine => out.push_str(&format!(
                "\nSelect it on a machine with `write cairn://executors/<name>` patch \
                 `{{machineServices: {{\"{key}\": {{}}}}}}`, which runs the service there; `null` \
                 removes it.\n\n",
                key = service.key
            )),
            MachineServicePlacement::Fleet => out.push_str(&format!(
                "\nDelivered to every machine it ships for and started when a session uses it; \
                 sessions start it on {HOST_MACHINE}. Run a build of your own on one machine with \
                 `write cairn://executors/<name>` patch \
                 `{{machineServices: {{\"{key}\": {{override: {{command: /abs/path}}}}}}}}`; `null` \
                 clears the override.\n\n",
                key = service.key
            )),
        }
    }
}

/// A pack's web-service providers and credentials: which slot each fills,
/// whether it is selected, and whether its key is set. Presence only; the
/// value is never read here.
/// The readiness word a pack's web-service item shows.
fn readiness_word(not_ready: Option<&NotReady>) -> String {
    match not_ready {
        None => "ready".to_string(),
        Some(NotReady::MissingVars { vars }) => format!("needs key ({})", vars.join(", ")),
        Some(NotReady::NeedsAuth) => "needs auth".to_string(),
        Some(NotReady::NeedsSetup { missing }) => format!("needs setup ({})", missing.join(", ")),
        Some(other) => other.summary(),
    }
}

fn render_web_services(out: &mut String, entry: &PackCatalogEntry) {
    let installed = entry.state == PackState::Installed;
    let providers: Vec<(&WebProviderItem, Option<&NotReady>)> = entry
        .items
        .iter()
        .filter_map(|item| {
            item.web_provider
                .as_ref()
                .map(|provider| (provider, item.not_ready.as_ref()))
        })
        .collect();
    if !providers.is_empty() {
        out.push_str("## Web services\n\n");
        for (provider, not_ready) in providers {
            let selected = if provider.active {
                format!(" · active for {}", provider.slot.replace('-', " "))
            } else {
                String::new()
            };
            let via = provider
                .mcp_server
                .as_ref()
                .map(|server| format!(" · through MCP server `{server}`"))
                .unwrap_or_default();
            let state = if installed {
                readiness_word(not_ready)
            } else {
                "not installed".to_string()
            };
            out.push_str(&format!(
                "- `{}` — {}{selected}{via} · {state}\n",
                provider.slot, provider.label
            ));
        }
        out.push_str(&format!(
            "\nSelecting a provider and setting its key or connection are done in {}.\n\n",
            crate::web_providers::LIBRARY_LOCATION
        ));
    }
    let credentials: Vec<&CredentialItem> = entry
        .items
        .iter()
        .filter_map(|item| item.credential.as_ref())
        .collect();
    if !credentials.is_empty() {
        out.push_str("## Credentials\n\n");
        for credential in credentials {
            let oauth = credential.kind == "oauth";
            let state = match (installed, credential.present, oauth) {
                (false, _, _) => "not installed",
                (true, true, false) => "set",
                (true, false, false) => "not set",
                (true, true, true) => "connected",
                (true, false, true) => "needs auth",
            };
            let what = match (&credential.var, oauth) {
                (_, true) => "OAuth".to_string(),
                (Some(var), false) => var.clone(),
                (None, false) => credential.label.clone(),
            };
            out.push_str(&format!(
                "- `{}` — {what} · sent only to {} · {state}\n",
                credential.name,
                credential.hosts.join(", ")
            ));
        }
        out.push('\n');
    }
}

fn render_items(out: &mut String, entry: &PackCatalogEntry) {
    if entry.items.is_empty() {
        out.push_str("No contents.\n\n");
        return;
    }

    let mut by_kind: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for item in &entry.items {
        by_kind
            .entry(item.kind.as_str())
            .or_default()
            .push(item_label(item));
    }

    for (kind, ids) in by_kind {
        out.push_str(&format!("- {kind} ({}): {}\n", ids.len(), ids.join(", ")));
    }
    out.push('\n');
}

pub(crate) fn read_packs(
    config_dir: &Path,
    library: &LibraryContext,
    machines: &MachineServiceSnapshot,
    state_filter: Option<&str>,
) -> String {
    render_packs(&pack_catalog(config_dir, library, machines), state_filter)
}

/// One line per configured source: whether its index is believed, and why not.
fn source_status_line(source: &LibrarySourceStatus) -> String {
    let label = &source.label;
    let mut line = match source.state.as_str() {
        "verified" => format!(
            "> **{label}**: verified index at `{}` (serial {}), signed by `{}`.",
            source.commit.as_deref().map(short_commit).unwrap_or("?"),
            source.serial.unwrap_or_default(),
            source.signer.as_deref().unwrap_or("?")
        ),
        "unverified" => format!(
            "> **{label}**: the cached index is not trusted ({}), so none of its entries are \
             offered.",
            source.refusal.as_deref().unwrap_or("no reason recorded")
        ),
        "unreachable" => format!(
            "> **{label}**: unreachable ({}), so only bundled packs are listed.",
            source.last_error.as_deref().unwrap_or("no reason recorded")
        ),
        _ => format!("> **{label}**: not fetched yet; it refreshes when the Library opens."),
    };
    if source.state == "verified" {
        if let Some(error) = &source.last_error {
            line.push_str(&format!(
                " The last refresh failed ({error}); this is the last verified index."
            ));
        }
    }
    if !source.refused_entries.is_empty() {
        let refused: Vec<String> = source
            .refused_entries
            .iter()
            .map(|refusal| format!("`{}` ({})", refusal.id, refusal.reason))
            .collect();
        line.push_str(&format!(" Not offered: {}.", refused.join("; ")));
    }
    line.push_str("\n\n");
    line
}

fn short_commit(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// "Cairn library · signed", "Bundled", "Local import".
fn source_label(source: &PackSourceInfo) -> String {
    let mut label = source.label.clone();
    if source.signer.is_some() {
        label.push_str(" · signed");
    }
    if let Some(upstream) = &source.upstream {
        label.push_str(&format!(" · from {}", upstream.url));
    }
    if source.updates_frozen {
        label.push_str(" · source not configured, updates frozen");
    }
    label
}

/// What installing this entry lets happen here, or nothing for pure content.
fn runs_label(entry: &PackCatalogEntry) -> Option<String> {
    match entry.runs {
        ItemRuns::Content => None,
        ItemRuns::Endpoint => {
            let hosts: Vec<&str> = entry
                .items
                .iter()
                .filter_map(|item| item.connects_to.as_deref())
                .collect();
            Some(if hosts.is_empty() {
                "Connects to a remote server".to_string()
            } else {
                format!("Connects to {}", hosts.join(", "))
            })
        }
        ItemRuns::Here => Some("Runs on this machine".to_string()),
    }
}

/// A definition in one line: the command a stdio server spawns, or the URL
/// a remote one reaches.
fn describe_definition(definition: &McpServerConfig) -> String {
    match definition.transport.as_str() {
        "http" | "sse" => format!(
            "{} {}",
            definition.transport,
            definition.url.as_deref().unwrap_or("?")
        ),
        crate::config::mcp_servers::MACHINE_SERVICE_TRANSPORT => format!(
            "machine service {}",
            definition.service.as_deref().unwrap_or("?")
        ),
        _ => {
            let mut argv = vec![definition.command.clone().unwrap_or_default()];
            argv.extend(definition.args.iter().cloned());
            let mut text = format!("`{}`", argv.join(" "));
            if !definition.env.is_empty() {
                let mut keys: Vec<&String> = definition.env.keys().collect();
                keys.sort();
                let keys: Vec<&str> = keys.into_iter().map(String::as_str).collect();
                text.push_str(&format!(" with env {}", keys.join(", ")));
            }
            text
        }
    }
}

/// The library's sections, in the order they read, each paired with its heading
/// and the singular noun one of its entries is called.
///
/// A kind missing from this table still gets a section — appended after these,
/// with a heading derived from the kind itself — so shipping a new item kind is
/// a manifest change and not a surfacing change.
const SECTIONS: &[(&str, &str, &str)] = &[
    (PACK_ENTRY_KIND, "Packs", "pack"),
    ("mcp", "MCP servers", "MCP server"),
    ("provider", "Web services", "web-service provider"),
    ("credential", "Credentials", "credential"),
    ("service", "Machine services", "machine service"),
    ("skill", "Skills", "skill"),
    ("agent", "Agents", "agent"),
    ("recipe", "Recipes", "recipe"),
    ("workflow", "Workflows", "workflow"),
    ("response", "Responses", "response"),
    ("route", "Routes", "route"),
];

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// The heading a section of entries sits under.
fn section_title(kind: &str) -> String {
    match SECTIONS.iter().find(|(k, _, _)| *k == kind) {
        Some((_, title, _)) => (*title).to_string(),
        None => format!("{}s", capitalize(kind)),
    }
}

/// What one entry of this kind is called, mid-sentence.
fn entry_noun(kind: &str) -> String {
    match SECTIONS.iter().find(|(k, _, _)| *k == kind) {
        Some((_, _, noun)) => (*noun).to_string(),
        None => kind.to_string(),
    }
}

/// The library's sections for these entries: the known kinds in presentation
/// order, then any unrecognized kind in catalog order. Empty sections are
/// dropped, so the shape of the output is the shape of what is actually here.
fn sections<'a>(entries: &[&'a PackCatalogEntry]) -> Vec<(&'a str, Vec<&'a PackCatalogEntry>)> {
    let mut order: Vec<&str> = SECTIONS.iter().map(|(kind, _, _)| *kind).collect();
    for entry in entries {
        if !order.contains(&entry.entry_kind.as_str()) {
            order.push(entry.entry_kind.as_str());
        }
    }
    order
        .into_iter()
        .filter_map(|kind| {
            let members: Vec<&PackCatalogEntry> = entries
                .iter()
                .copied()
                .filter(|entry| entry.entry_kind == kind)
                .collect();
            (!members.is_empty()).then_some((kind, members))
        })
        .collect()
}

fn render_packs(catalog: &PackCatalog, state_filter: Option<&str>) -> String {
    let mut out = "# Library\n\n".to_string();
    if !catalog.source_recorded {
        out.push_str(
            "> This workspace has no recorded app resource directory yet, so only \
             already-installed entries are listed. It is recorded on the next startup sync.\n\n",
        );
    }
    for source in &catalog.sources {
        out.push_str(&source_status_line(source));
    }

    let filtered: Vec<&PackCatalogEntry> = catalog
        .packs
        .iter()
        .filter(|entry| state_filter.is_none_or(|state| entry.state.as_str() == state))
        .collect();

    if filtered.is_empty() {
        out.push_str("Nothing in the library.\n");
        return out;
    }

    for (kind, members) in sections(&filtered) {
        out.push_str(&format!(
            "## {} ({})\n\n",
            section_title(kind),
            members.len()
        ));
        for entry in members {
            let update = if entry.update_needs_review {
                " — update needs review"
            } else if entry.update_available {
                " — update available"
            } else {
                ""
            };
            let runs = runs_label(entry)
                .map(|label| format!(" · {label}"))
                .unwrap_or_default();
            out.push_str(&format!(
                "### [{}]({}) `{}`\n\n",
                entry.name,
                build_pack_uri(&entry.id),
                entry.id
            ));
            out.push_str(&format!(
                "State: {}{} · Version: {} · Source: {}{runs}\n\n",
                entry.state.as_str(),
                update,
                display_version(entry),
                source_label(&entry.source)
            ));
            if !entry.description.is_empty() {
                out.push_str(&format!("{}\n\n", entry.description));
            }
            if let Some(line) = shadowed_line(entry) {
                out.push_str(&format!("{line}\n\n"));
            }
            render_items(&mut out, entry);
        }
    }

    out
}

/// "Shadowed by your definitions: 2", or nothing when every item is in use.
pub(crate) fn shadowed_line(entry: &PackCatalogEntry) -> Option<String> {
    (entry.shadowed_count > 0)
        .then(|| format!("Shadowed by your definitions: {}", entry.shadowed_count))
}

/// What is actually in this workspace wins over what the app ships, so a pack
/// held at an older version reads as that version.
fn display_version(entry: &PackCatalogEntry) -> &str {
    entry
        .installed_version
        .as_deref()
        .or(entry.shipped_version.as_deref())
        .unwrap_or("0.0.0")
}

pub(crate) fn read_pack(
    config_dir: &Path,
    library: &LibraryContext,
    machines: &MachineServiceSnapshot,
    pack_id: &str,
) -> String {
    let catalog = pack_catalog(config_dir, library, machines);
    match catalog.find(pack_id) {
        Some(entry) => render_pack(entry),
        None => format!("Pack not found: {pack_id}"),
    }
}

fn render_pack(entry: &PackCatalogEntry) -> String {
    let noun = entry_noun(&entry.entry_kind);
    let mut out = format!(
        "# {} `{}` — {}\n\n",
        capitalize(&noun),
        entry.id,
        entry.name
    );
    out.push_str(&format!("State: {}\n", entry.state.as_str()));
    if entry.uninstalled_by_user {
        out.push_str(&format!(
            "You uninstalled this {noun}. Install it again to bring it back.\n"
        ));
    }
    out.push_str(&format!("Version: {}\n", display_version(entry)));
    if entry.update_available {
        let shipped = entry.shipped_version.as_deref().unwrap_or("?");
        let offerer = if entry.source.kind == "bundled" {
            "the app ships".to_string()
        } else {
            format!("the {} offers", entry.source.label)
        };
        out.push_str(&format!(
            "Update available: {offerer} {shipped}, which differs from what is installed\n"
        ));
    }
    out.push_str(&format!("Source: {}\n", source_label(&entry.source)));
    if let Some(url) = &entry.source.url {
        out.push_str(&format!("Source URL: {url}\n"));
    }
    if let Some(commit) = &entry.source.commit {
        out.push_str(&format!("Source commit: {commit}\n"));
    }
    if let Some(signer) = &entry.source.signer {
        out.push_str(&format!("Signed by: {signer}\n"));
    }
    if let Some(upstream) = &entry.source.upstream {
        out.push_str(&format!(
            "Fetched from: {} at {}\n",
            upstream.url, upstream.sha
        ));
    }
    if let Some(path) = &entry.source.path {
        out.push_str(&format!("Imported from: {path}\n"));
    }
    if let Some(license) = &entry.license {
        out.push_str(&format!("License: {license}\n"));
    }
    if let Some(runs) = runs_label(entry) {
        out.push_str(&format!("{runs}\n"));
    }

    if let Some(author) = &entry.author {
        out.push_str(&format!("Author: {author}\n"));
    }
    if let Some(homepage) = &entry.homepage {
        out.push_str(&format!("Homepage: {homepage}\n"));
    }
    if !entry.keywords.is_empty() {
        out.push_str(&format!("Keywords: {}\n", entry.keywords.join(", ")));
    }
    if entry.shipped_version.is_some() {
        out.push_str(&format!(
            "Installs by default: {}\n",
            entry.installs_by_default
        ));
    }

    if let Some(installed_at) = &entry.installed_at {
        out.push_str(&format!("Installed at: {installed_at}\n"));
    }
    out.push('\n');

    if entry.update_needs_review {
        out.push_str("## Update needs review\n\n");
        out.push_str(
            "This update changes what runs on this machine, so it is not applied on its own. The \
             operator applies it from the Library page in Settings; an agent's update of this \
             pack is refused.\n\n",
        );
        for change in &entry.runtime_changes {
            let before = change.before.as_ref().map(describe_definition);
            let after = change.after.as_ref().map(describe_definition);
            let detail = match (before, after) {
                (Some(before), Some(after)) => format!(": {before} → {after}"),
                (None, Some(after)) => format!(": {after}"),
                (Some(before), None) => format!(": was {before}"),
                (None, None) => String::new(),
            };
            out.push_str(&format!(
                "- {} `{}` ({}) {}{detail}\n",
                change.kind,
                change.id,
                change.runs.as_str(),
                change.change
            ));
        }
        out.push('\n');
    }

    if !entry.description.is_empty() {
        out.push_str(&format!("{}\n\n", entry.description));
    }

    out.push_str("## Contents\n\n");
    render_items(&mut out, entry);
    render_machine_services(&mut out, entry);
    render_web_services(&mut out, entry);

    let shadowed = entry.shadowed_items();
    if !shadowed.is_empty() {
        out.push_str("## Shadowed by your definitions\n\n");
        out.push_str(
            "A definition of yours resolves in front of each of these. The pack's own version is \
             still installed, keeps updating, and stays addressable at its pack address. Delete \
             your definition to use the pack's again.\n\n",
        );
        for item in shadowed {
            let address = cairn_config::library::LibraryKind::parse(&item.kind)
                .map(|kind| cairn_config::library::pack_item_uri(&entry.id, kind, &item.id));
            match address {
                Some(address) => out.push_str(&format!(
                    "- {} `{}` — shadowed by {}; the pack's: {address}\n",
                    item.kind,
                    item.id,
                    item.shadowed_by.as_deref().unwrap_or("~")
                )),
                None => out.push_str(&format!(
                    "- {} `{}` — shadowed by {}\n",
                    item.kind,
                    item.id,
                    item.shadowed_by.as_deref().unwrap_or("~")
                )),
            }
        }
        out.push('\n');
    }

    let removed = entry.removed_items();
    if !removed.is_empty() {
        out.push_str("## Removed by you\n\n");
        out.push_str(
            "These items are part of the pack but are not installed here. The pack keeps \
             updating around them, and a sync will not copy them back. Bring them back with \
             `patch {action:\"restore\"}`.\n\n",
        );
        for item in removed {
            out.push_str(&format!("- {} `{}`\n", item.kind, item.id));
        }
        out.push('\n');
    }

    if !entry.notes.is_empty() {
        out.push_str("## Notes\n\n");
        for note in &entry.notes {
            out.push_str(&format!("- {note}\n"));
        }
        out.push('\n');
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(kind: &str, id: &str, state: PackItemState) -> PackCatalogItem {
        PackCatalogItem {
            kind: kind.to_string(),
            id: id.to_string(),
            path: (kind != "mcp").then(|| format!("{kind}s/{id}")),
            state,
            shadowed_by: None,
            collides_with: Vec::new(),
            not_ready: None,
            setup: Vec::new(),
            runs: ItemRuns::Content,
            connects_to: None,
            machine_service: None,
            web_provider: None,
            credential: None,
        }
    }

    fn entry() -> PackCatalogEntry {
        PackCatalogEntry {
            id: "matlab".into(),
            name: "MATLAB".into(),
            description: "MATLAB integration".into(),
            state: PackState::Installed,
            installed_version: Some("1.0.0".into()),
            shipped_version: Some("1.1.0".into()),
            update_available: true,
            installs_by_default: false,
            uninstalled_by_user: false,
            source: PackSourceInfo {
                kind: "bundled".into(),
                label: "Bundled".into(),
                ..Default::default()
            },
            license: None,
            runs: ItemRuns::Content,
            update_needs_review: false,
            runtime_changes: vec![],
            author: Some("Cairn".into()),
            homepage: None,
            keywords: vec![],
            installed_at: Some("2026-01-01T00:00:00Z".into()),
            notes: vec![],
            items: vec![
                PackCatalogItem {
                    shadowed_by: Some("~".into()),
                    ..item("skill", "matlab", PackItemState::Shadowed)
                },
                item("mcp", "matlab", PackItemState::RemovedByUser),
            ],
            counts: vec![PackContentCount {
                kind: "skill".into(),
                count: 1,
            }],
            shadowed_count: 1,
            entry_kind: PACK_ENTRY_KIND.into(),
        }
    }

    /// A manifest carrying exactly one item, classified the way the projection
    /// classifies it.
    fn single(id: &str, kind: &str) -> PackCatalogEntry {
        let items = vec![item(kind, id, PackItemState::InUse)];
        PackCatalogEntry {
            id: id.into(),
            name: capitalize(id),
            description: String::new(),
            state: PackState::Available,
            installed_version: None,
            installed_at: None,
            update_available: false,
            entry_kind: entry_kind(&items),
            items,
            counts: vec![PackContentCount {
                kind: kind.into(),
                count: 1,
            }],
            ..entry()
        }
    }

    /// Every per-item verdict the projection makes has to be visible in the
    /// markdown too — that agreement is the whole reason there is one
    /// projection rather than two renderers reading the filesystem twice.
    #[test]
    fn the_detail_page_reports_every_item_state_the_projection_carries() {
        let page = render_pack(&entry());
        assert!(page.contains("State: installed"), "{page}");
        assert!(
            page.contains("Update available: the app ships 1.1.0"),
            "{page}"
        );
        assert!(
            page.contains("matlab (shadowed by your ~ definition)"),
            "{page}"
        );
        assert!(
            page.contains(
                "- skill `matlab` — shadowed by ~; the pack's: cairn://packs/matlab/skills/matlab"
            ),
            "{page}"
        );
        assert_eq!(
            shadowed_line(&entry()).as_deref(),
            Some("Shadowed by your definitions: 1")
        );
        assert!(page.contains("matlab (removed by you)"), "{page}");
        assert!(page.contains("## Shadowed by your definitions"), "{page}");
        assert!(page.contains("## Removed by you"), "{page}");
        assert!(page.contains("- mcp `matlab`"), "{page}");
        assert!(page.contains("Source: Bundled\n"), "{page}");
    }

    /// Every entry says where it came from and what it runs, and a pending
    /// update that changes what runs says so with the definitions on both
    /// sides.
    #[test]
    fn provenance_runs_and_a_review_are_on_the_page() {
        let before: McpServerConfig = serde_yaml::from_str("command: old-mcp\n").unwrap();
        let after: McpServerConfig =
            serde_yaml::from_str("command: new-mcp\nargs: [--stdio]\n").unwrap();
        let mut library = single("acme", "mcp");
        library.state = PackState::Installed;
        library.source = PackSourceInfo {
            kind: "url".into(),
            label: "Cairn library".into(),
            url: Some("https://github.com/bleugreenlab/cairn-library".into()),
            signer: Some("cairn-library-2026".into()),
            commit: Some("0123456789abcdef0123456789abcdef01234567".into()),
            ..Default::default()
        };
        library.runs = ItemRuns::Here;
        library.update_available = true;
        library.update_needs_review = true;
        library.runtime_changes = vec![RuntimeChange {
            kind: "mcp".into(),
            id: "acme".into(),
            runs: ItemRuns::Here,
            change: "changed".into(),
            before: Some(before),
            after: Some(after),
        }];
        let page = render_pack(&library);
        assert!(page.contains("Source: Cairn library · signed\n"), "{page}");
        assert!(page.contains("Signed by: cairn-library-2026"), "{page}");
        assert!(page.contains("Runs on this machine"), "{page}");
        assert!(
            page.contains("Update available: the Cairn library offers"),
            "{page}"
        );
        assert!(page.contains("## Update needs review"), "{page}");
        assert!(
            page.contains("- mcp `acme` (here) changed: `old-mcp` → `new-mcp --stdio`"),
            "{page}"
        );

        let catalog = PackCatalog {
            source_recorded: true,
            packs: vec![library],
            sources: vec![LibrarySourceStatus {
                id: "cairn-library".into(),
                label: "Cairn library".into(),
                url: "https://github.com/bleugreenlab/cairn-library".into(),
                state: "unreachable".into(),
                signer: None,
                commit: None,
                serial: None,
                checked_at: None,
                last_error: Some("no network".into()),
                refusal: None,
                refused_entries: vec![],
            }],
        };
        let page = render_packs(&catalog, None);
        assert!(
            page.contains("**Cairn library**: unreachable (no network)"),
            "{page}"
        );
        assert!(page.contains("update needs review"), "{page}");
    }

    #[test]
    fn hosts_are_read_from_urls() {
        assert_eq!(
            host_of("https://mcp.linear.app/mcp").as_deref(),
            Some("mcp.linear.app")
        );
        assert_eq!(
            host_of("https://u:p@h.test:8443/x?y").as_deref(),
            Some("h.test:8443")
        );
        assert_eq!(host_of("not a url"), None);
    }

    #[test]
    fn an_uninstalled_pack_says_so_and_a_state_filter_selects_by_state() {
        let mut available = entry();
        available.state = PackState::Available;
        available.installed_version = None;
        available.installed_at = None;
        available.update_available = false;
        available.uninstalled_by_user = true;

        let page = render_pack(&available);
        assert!(page.contains("You uninstalled this pack"), "{page}");
        assert!(page.contains("Version: 1.1.0"), "{page}");

        let catalog = PackCatalog {
            source_recorded: true,
            packs: vec![entry(), available],
            sources: vec![],
        };
        assert_eq!(
            render_packs(&catalog, Some("installed"))
                .matches("State: installed")
                .count(),
            1
        );
        assert!(render_packs(&catalog, Some("available")).contains("State: available"));
    }

    #[test]
    fn a_workspace_with_no_recorded_source_says_why_its_catalog_is_short() {
        let catalog = PackCatalog {
            source_recorded: false,
            packs: vec![],
            sources: vec![],
        };
        let page = render_packs(&catalog, None);
        assert!(
            page.contains("no recorded app resource directory"),
            "{page}"
        );
        assert!(page.contains("Nothing in the library."), "{page}");
    }

    /// The taxonomy the whole library reads by: a manifest that ships one item
    /// IS that item, and only a bundle is a pack.
    #[test]
    fn a_one_item_manifest_is_that_item_and_a_bundle_is_a_pack() {
        let bundle = entry();
        assert_eq!(bundle.entry_kind, PACK_ENTRY_KIND);
        assert_eq!(single("linear", "mcp").entry_kind, "mcp");

        // The detail page calls an entry what it is, not what its URI family is.
        let page = render_pack(&single("linear", "mcp"));
        assert!(page.starts_with("# MCP server `linear`"), "{page}");
        assert!(render_pack(&bundle).starts_with("# Pack `matlab`"));
    }

    /// Sections carry the catalog's shape: known kinds in presentation order,
    /// an unrecognized kind in its own section rather than swept into `Packs`,
    /// and no heading for a kind nothing here has.
    #[test]
    fn the_catalog_reads_as_kind_sections_that_an_unknown_kind_extends() {
        let catalog = PackCatalog {
            source_recorded: true,
            packs: vec![
                entry(),
                single("linear", "mcp"),
                single("deepwiki", "mcp"),
                single("triage", "channel"),
            ],
            sources: vec![],
        };
        let page = render_packs(&catalog, None);

        let packs = page.find("## Packs (1)").expect(&page);
        let mcp = page.find("## MCP servers (2)").expect(&page);
        let channels = page.find("## Channels (1)").expect(&page);
        assert!(packs < mcp && mcp < channels, "{page}");
        assert!(!page.contains("## Skills"), "{page}");
        assert!(page.contains("### [Linear]"), "{page}");
    }
}
