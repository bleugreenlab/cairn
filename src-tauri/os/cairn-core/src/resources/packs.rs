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

use crate::config::mcp_servers::{workspace_mcp_entries, NotReady, WorkspaceMcpEntry};
use crate::config::pack::mcp::McpVarRequirement;
use crate::config::pack::{self, ContentHash, PackItem, PackItemKind, PackLock, PackManifest};

/// One pack as the catalog sees it: what the app ships, what the workspace
/// installed, or both.
struct PackView {
    id: String,
    available: Option<PackManifest>,
    installed: Option<PackLock>,
}

impl PackView {
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
            (None, None) => Vec::new(),
        }
    }

    fn name(&self) -> &str {
        self.available
            .as_ref()
            .map(|m| m.name.as_str())
            .or_else(|| self.installed.as_ref().map(|l| l.name.as_str()))
            .unwrap_or(&self.id)
    }

    fn description(&self) -> &str {
        self.available
            .as_ref()
            .map(|m| m.description.as_str())
            .or_else(|| self.installed.as_ref().map(|l| l.description.as_str()))
            .unwrap_or("")
    }

    fn notes(&self) -> Vec<String> {
        self.available
            .as_ref()
            .map(|m| m.notes.clone())
            .or_else(|| self.installed.as_ref().map(|l| l.notes.clone()))
            .unwrap_or_default()
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

/// What has happened to one item of an installed pack.
///
/// This is the backend's verdict, not a hint: an update preserves an
/// `EditedByUser` local copy, an uninstall keeps it, and
/// a `RemovedByUser` item stays out until the pack is explicitly restored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackItemState {
    PackOwned,
    EditedByUser,
    RemovedByUser,
}

impl PackItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            PackItemState::PackOwned => "pack-owned",
            PackItemState::EditedByUser => "edited-by-user",
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
    /// For the user's copy of a file-backed item: whether the version the app
    /// ships now differs from it. This is the case worth acting on -- a copy
    /// that merely equals the shipped one costs nothing to keep.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub behind_shipped: bool,
    /// Why an MCP item cannot connect yet, straight from the registry that
    /// decides it. The UI renders this verdict; it never re-derives one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_ready: Option<NotReady>,
    /// What to ask the user for, one entry per variable [`Self::not_ready`]
    /// names. This is what the pack detail screen turns into a labelled field
    /// with a file picker instead of a variable name and no way to act on it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<McpVarRequirement>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackSourceInfo {
    /// `bundled` (shipped in the app) or `url`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
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
    /// How many items are the user's copies, and how many of those the app now
    /// ships differently -- the line a pack row states so a frozen copy is
    /// visible without opening the pack.
    pub edited_count: usize,
    pub behind_count: usize,
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
fn entry_kind(items: &[PackCatalogItem]) -> String {
    match items {
        [only] => only.kind.clone(),
        _ => PACK_ENTRY_KIND.to_string(),
    }
}

impl PackCatalogEntry {
    /// Items the user has made theirs.
    fn edited_items(&self) -> Vec<&PackCatalogItem> {
        self.items
            .iter()
            .filter(|item| item.state == PackItemState::EditedByUser)
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
}

impl PackCatalog {
    pub fn find(&self, pack_id: &str) -> Option<&PackCatalogEntry> {
        self.packs.iter().find(|entry| entry.id == pack_id)
    }
}

/// Every pack this workspace knows about: shipped by the app, installed, or
/// both. Sorted by id.
fn pack_views(resource_dir: Option<&Path>, config_dir: &Path) -> Vec<PackView> {
    let mut by_id: BTreeMap<String, PackView> = BTreeMap::new();

    if let Some(resource_dir) = resource_dir {
        for manifest in pack::discover_available_packs(resource_dir) {
            by_id.insert(
                manifest.id.clone(),
                PackView {
                    id: manifest.id.clone(),
                    available: Some(manifest),
                    installed: None,
                },
            );
        }
    }

    for lock in pack::installed_packs(config_dir) {
        match by_id.get_mut(&lock.id) {
            Some(view) => view.installed = Some(lock),
            None => {
                by_id.insert(
                    lock.id.clone(),
                    PackView {
                        id: lock.id.clone(),
                        available: None,
                        installed: Some(lock),
                    },
                );
            }
        }
    }

    by_id.into_values().collect()
}

/// The typed catalog for `config_dir`.
pub fn pack_catalog(config_dir: &Path) -> PackCatalog {
    let resource_dir = pack::source_dir(config_dir);
    let mcp_entries = workspace_mcp_entries(config_dir);
    let packs = pack_views(resource_dir.as_deref(), config_dir)
        .iter()
        .map(|view| project(config_dir, resource_dir.as_deref(), view, &mcp_entries))
        .collect();
    PackCatalog {
        source_recorded: resource_dir.is_some(),
        packs,
    }
}

fn project(
    config_dir: &Path,
    resource_dir: Option<&Path>,
    view: &PackView,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
) -> PackCatalogEntry {
    let mut items: Vec<PackCatalogItem> = view
        .items()
        .into_iter()
        .map(|item| {
            let verdict = file_item_verdict(config_dir, view, &item);
            let state = item_state(&view.id, &item, verdict.edited, mcp_entries);
            let mut entry = catalog_item(item, state, mcp_entries);
            entry.behind_shipped = verdict.behind;
            entry
        })
        .collect();
    if let Some(lock) = &view.installed {
        items.extend(
            lock.removed
                .iter()
                .cloned()
                .map(|item| catalog_item(item, PackItemState::RemovedByUser, mcp_entries)),
        );
    }
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
        shipped_version: view.available.as_ref().map(|m| m.version.clone()),
        update_available: view.update_available(resource_dir),
        installs_by_default: view.available.as_ref().is_some_and(|m| m.default),
        uninstalled_by_user: view.installed.is_none()
            && pack::lock::is_uninstalled(config_dir, &view.id),
        source: source_info(view),
        author: view
            .available
            .as_ref()
            .and_then(|m| m.author.as_ref())
            .or_else(|| view.installed.as_ref().and_then(|l| l.author.as_ref()))
            .and_then(|a| a.name.clone()),
        homepage: view
            .available
            .as_ref()
            .and_then(|m| m.homepage.clone())
            .or_else(|| view.installed.as_ref().and_then(|l| l.homepage.clone())),
        keywords: view
            .available
            .as_ref()
            .map(|m| m.keywords.clone())
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
        edited_count: items
            .iter()
            .filter(|item| item.state == PackItemState::EditedByUser)
            .count(),
        behind_count: items.iter().filter(|item| item.behind_shipped).count(),
        items,
    }
}

#[derive(Default)]
struct ItemVerdict {
    edited: bool,
    behind: bool,
}

/// Whether one file-backed item is the user's copy, and whether the app ships
/// something different from it.
///
/// Read live from content hashes against the lock baseline and the shipped
/// source -- never from Git -- so an edit saved a moment ago reads as the
/// user's copy before the next sync records the fork.
fn file_item_verdict(config_dir: &Path, view: &PackView, item: &PackItem) -> ItemVerdict {
    let Some(locked) = view
        .installed
        .as_ref()
        .and_then(|lock| lock.item(item.kind, &item.id))
    else {
        return ItemVerdict::default();
    };
    let (Some(path), Some(_)) = (&item.path, &locked.content_hash) else {
        return ItemVerdict {
            edited: locked.forked,
            behind: false,
        };
    };
    let current = pack::hash_item_path(item.kind, config_dir, &config_dir.join(path))
        .unwrap_or(ContentHash::Missing);
    if current == ContentHash::Missing {
        return ItemVerdict {
            edited: locked.forked,
            behind: false,
        };
    }
    let edited =
        locked.forked || !pack::matches_baseline(config_dir, locked, &current).unwrap_or(false);
    let behind = edited
        && view.available.as_ref().is_some_and(|manifest| {
            matches!(
                pack::hash_item_path(item.kind, &manifest.root, &manifest.root.join(path)),
                Ok(shipped @ ContentHash::Present(_)) if shipped != current
            )
        });
    ItemVerdict { edited, behind }
}

fn source_info(view: &PackView) -> PackSourceInfo {
    match &view.installed {
        Some(lock) => PackSourceInfo {
            kind: match lock.source.kind {
                pack::PackSourceKind::Bundled => "bundled",
                pack::PackSourceKind::Url => "url",
                pack::PackSourceKind::Local => "local",
            }
            .to_string(),
            url: lock.source.url.clone(),
        },
        // Everything the app itself offers is bundled; a URL pack only exists
        // once it has been installed (CAIRN-3773 adds the other direction).
        None => PackSourceInfo {
            kind: "bundled".to_string(),
            url: None,
        },
    }
}

/// Whether the user has taken ownership of one live item.
///
/// A live item is the user's copy when its lock records a fork or its content
/// no longer matches the baseline (`edited`, see [`file_item_verdict`]). An MCP
/// server additionally becomes the user's copy when the resolved registry no
/// longer attributes it to this pack:
/// a user who saved their own version of a pack server shadows it in
/// `settings.yaml`, and the entry's origin flips to `workspace`.
fn item_state(
    pack_id: &str,
    item: &PackItem,
    edited: bool,
    mcp_entries: &BTreeMap<String, WorkspaceMcpEntry>,
) -> PackItemState {
    let forked = edited;
    if item.kind == PackItemKind::Mcp {
        let shadowed = mcp_entries
            .get(&item.id)
            .is_some_and(|entry| entry.origin != format!("pack:{pack_id}"));
        return if forked || shadowed {
            PackItemState::EditedByUser
        } else {
            PackItemState::PackOwned
        };
    }
    if forked {
        PackItemState::EditedByUser
    } else {
        PackItemState::PackOwned
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
        behind_shipped: false,
        not_ready: entry.and_then(|e| e.not_ready.clone()),
        setup: entry.map(|e| e.setup.clone()).unwrap_or_default(),
    }
}

/// The shipped and installed text of one pack item, for a side-by-side view of
/// what a reset would replace.
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
    /// Workspace-relative path.
    pub path: String,
    /// Absent when the app does not ship this file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shipped: Option<String>,
    /// Absent when the workspace copy has no such file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yours: Option<String>,
    /// True when either side is not UTF-8; neither text is sent.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub binary: bool,
}

/// Compare one file-backed item of a shipped pack with the workspace copy.
pub fn pack_item_diff(
    config_dir: &Path,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
) -> Result<PackItemDiff, String> {
    let resource_dir = pack::source_dir(config_dir)
        .ok_or("This workspace has no recorded app resource directory yet")?;
    let manifest = pack::available_pack(&resource_dir, pack_id)
        .ok_or_else(|| format!("No pack `{pack_id}` is shipped"))?;
    let item = manifest
        .items()
        .into_iter()
        .find(|item| item.kind == kind && item.id == item_id)
        .ok_or_else(|| {
            format!(
                "Pack `{pack_id}` does not ship {} `{item_id}`",
                kind.as_str()
            )
        })?;
    let path = item
        .path
        .ok_or_else(|| format!("{} `{item_id}` has no file to compare", kind.as_str()))?;
    let shipped_root = manifest.root.join(&path);
    let yours_root = config_dir.join(&path);

    let mut files = Vec::new();
    if matches!(kind, PackItemKind::Skill | PackItemKind::Workflow) {
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
    match item.state {
        PackItemState::EditedByUser if item.behind_shipped => {
            label.push_str(" (your copy, behind shipped)")
        }
        PackItemState::EditedByUser => label.push_str(" (your copy)"),
        PackItemState::RemovedByUser => label.push_str(" (removed by you)"),
        PackItemState::PackOwned => {}
    }
    label
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

pub(crate) fn read_packs(config_dir: &Path, state_filter: Option<&str>) -> String {
    render_packs(&pack_catalog(config_dir), state_filter)
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
            let update = if entry.update_available {
                " — update available"
            } else {
                ""
            };
            out.push_str(&format!(
                "### [{}]({}) `{}`\n\n",
                entry.name,
                build_pack_uri(&entry.id),
                entry.id
            ));
            out.push_str(&format!(
                "State: {}{} · Version: {}\n\n",
                entry.state.as_str(),
                update,
                display_version(entry)
            ));
            if !entry.description.is_empty() {
                out.push_str(&format!("{}\n\n", entry.description));
            }
            if let Some(line) = copies_line(entry) {
                out.push_str(&format!("{line}\n\n"));
            }
            render_items(&mut out, entry);
        }
    }

    out
}

/// "2 your copies (1 behind shipped)", or nothing when every item is the pack's.
pub(crate) fn copies_line(entry: &PackCatalogEntry) -> Option<String> {
    if entry.edited_count == 0 {
        return None;
    }
    let noun = if entry.edited_count == 1 {
        "copy"
    } else {
        "copies"
    };
    let mut line = format!("Your {noun}: {}", entry.edited_count);
    if entry.behind_count > 0 {
        line.push_str(&format!(" ({} behind shipped)", entry.behind_count));
    }
    Some(line)
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

pub(crate) fn read_pack(config_dir: &Path, pack_id: &str) -> String {
    let catalog = pack_catalog(config_dir);
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
        out.push_str(&format!(
            "Update available: the app ships {shipped}, which differs from what is installed\n"
        ));
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
        out.push_str(&format!("Source: {}", entry.source.kind));
        if let Some(url) = &entry.source.url {
            out.push_str(&format!(" — {url}"));
        }
        out.push('\n');
    }
    out.push('\n');

    if !entry.description.is_empty() {
        out.push_str(&format!("{}\n\n", entry.description));
    }

    out.push_str("## Contents\n\n");
    render_items(&mut out, entry);

    let edited = entry.edited_items();
    if !edited.is_empty() {
        out.push_str("## Your copies\n\n");
        out.push_str(
            "These local copies are yours. Pack updates leave them untouched, and an uninstall \
             keeps them. One marked behind shipped is frozen at an older version than the app now \
             ships. Reset an item explicitly with `patch {action:\"reset-item\", kind, itemId}` to \
             replace it with the current pack version.\n\n",
        );
        for item in edited {
            let label = item.path.as_deref().unwrap_or(&item.id);
            let behind = if item.behind_shipped {
                " — behind shipped"
            } else {
                ""
            };
            out.push_str(&format!("- {} `{label}`{behind}\n", item.kind));
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
            behind_shipped: false,
            not_ready: None,
            setup: Vec::new(),
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
                url: None,
            },
            author: Some("Cairn".into()),
            homepage: None,
            keywords: vec![],
            installed_at: Some("2026-01-01T00:00:00Z".into()),
            notes: vec![],
            items: vec![
                PackCatalogItem {
                    behind_shipped: true,
                    ..item("skill", "matlab", PackItemState::EditedByUser)
                },
                item("mcp", "matlab", PackItemState::RemovedByUser),
            ],
            counts: vec![PackContentCount {
                kind: "skill".into(),
                count: 1,
            }],
            edited_count: 1,
            behind_count: 1,
            entry_kind: PACK_ENTRY_KIND.into(),
        }
    }

    /// A manifest carrying exactly one item, classified the way the projection
    /// classifies it.
    fn single(id: &str, kind: &str) -> PackCatalogEntry {
        let items = vec![item(kind, id, PackItemState::PackOwned)];
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
            page.contains("matlab (your copy, behind shipped)"),
            "{page}"
        );
        assert!(
            page.contains("skill `skills/matlab` — behind shipped"),
            "{page}"
        );
        assert_eq!(
            copies_line(&entry()).as_deref(),
            Some("Your copy: 1 (1 behind shipped)")
        );
        assert!(page.contains("matlab (removed by you)"), "{page}");
        assert!(page.contains("## Your copies"), "{page}");
        assert!(page.contains("- skill `skills/matlab`"), "{page}");
        assert!(page.contains("## Removed by you"), "{page}");
        assert!(page.contains("- mcp `matlab`"), "{page}");
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
