//! Nearest-first resolution of library items (tenet 1434).
//!
//! Every library kind with a bare address (`cairn://agents/<name>` and its
//! siblings) resolves through the one algorithm here, over three layers:
//!
//! 1. the project's `.cairn/<kind>/`, only once the operator has trusted the
//!    project, and only for definitions whose authority-carrying fields the
//!    operator has confirmed;
//! 2. the workspace's `<config_dir>/<kind>/`;
//! 3. installed packs, read where they are installed
//!    (`<config_dir>/packs/<id>/<kind>/`).
//!
//! Pack files are never copied out of their pack and never edited; a local
//! definition is a different file placed in front. Two packs defining the same
//! name with nothing local in front is refused rather than ordered, and a
//! pack's own version stays addressable as `cairn://packs/<pack>/<kind>/<name>`.
//!
//! Each kind's file layout and parsing stay with its loader, which hands this
//! module two closures: one that loads a name from a layer directory, and one
//! that enumerates a layer directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::pack::{self, PackItemKind};
use crate::ConfigResult;

/// The library kinds that resolve by bare name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LibraryKind {
    Agent,
    Recipe,
    Skill,
    Response,
    Route,
    Workflow,
}

impl LibraryKind {
    pub const ALL: [LibraryKind; 6] = [
        LibraryKind::Agent,
        LibraryKind::Recipe,
        LibraryKind::Skill,
        LibraryKind::Response,
        LibraryKind::Route,
        LibraryKind::Workflow,
    ];

    /// The directory a layer keeps this kind in, which is also the plural
    /// segment of its URIs.
    pub fn dir(self) -> &'static str {
        match self {
            LibraryKind::Agent => "agents",
            LibraryKind::Recipe => "recipes",
            LibraryKind::Skill => "skills",
            LibraryKind::Response => "responses",
            LibraryKind::Route => "routes",
            LibraryKind::Workflow => "workflows",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LibraryKind::Agent => "agent",
            LibraryKind::Recipe => "recipe",
            LibraryKind::Skill => "skill",
            LibraryKind::Response => "response",
            LibraryKind::Route => "route",
            LibraryKind::Workflow => "workflow",
        }
    }

    /// A kind by its singular (`agent`) or plural (`agents`) name.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == value || kind.dir() == value)
    }

    pub fn pack_kind(self) -> PackItemKind {
        match self {
            LibraryKind::Agent => PackItemKind::Agent,
            LibraryKind::Recipe => PackItemKind::Recipe,
            LibraryKind::Skill => PackItemKind::Skill,
            LibraryKind::Response => PackItemKind::Response,
            LibraryKind::Route => PackItemKind::Route,
            LibraryKind::Workflow => PackItemKind::Workflow,
        }
    }

    pub fn from_pack_kind(kind: PackItemKind) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.pack_kind() == kind)
    }

    /// Where a definition named `name` lives inside a layer, for messages that
    /// tell someone which file to write.
    pub fn item_rel_path(self, name: &str) -> String {
        match self {
            LibraryKind::Agent | LibraryKind::Response => format!("{}/{name}.md", self.dir()),
            LibraryKind::Recipe | LibraryKind::Route => format!("{}/{name}.yaml", self.dir()),
            LibraryKind::Skill => format!("skills/{name}/SKILL.md"),
            LibraryKind::Workflow => format!("workflows/{name}/workflow.yaml"),
        }
    }
}

impl fmt::Display for LibraryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which layer a definition was read from.
///
/// Rendered `@` (the project), `~` (the workspace) or `pack:<id>`, the same
/// marks listings show. It serializes as that mark, with the project key
/// appended to `@` so it round-trips.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Origin {
    Project { key: String },
    Workspace,
    Pack { id: String },
}

impl Origin {
    pub fn is_project(&self) -> bool {
        matches!(self, Origin::Project { .. })
    }

    pub fn is_workspace(&self) -> bool {
        matches!(self, Origin::Workspace)
    }

    pub fn pack_id(&self) -> Option<&str> {
        match self {
            Origin::Pack { id } => Some(id),
            _ => None,
        }
    }

    /// The short mark listings show: `@`, `~` or `pack:<id>`.
    pub fn label(&self) -> String {
        match self {
            Origin::Project { .. } => "@".to_string(),
            Origin::Workspace => "~".to_string(),
            Origin::Pack { id } => format!("pack:{id}"),
        }
    }

    fn encode(&self) -> String {
        match self {
            Origin::Project { key } => format!("@{key}"),
            other => other.label(),
        }
    }

    fn decode(value: &str) -> Option<Self> {
        if value == "~" {
            return Some(Origin::Workspace);
        }
        if let Some(key) = value.strip_prefix('@') {
            return Some(Origin::Project {
                key: key.to_string(),
            });
        }
        value
            .strip_prefix("pack:")
            .filter(|id| !id.is_empty())
            .map(|id| Origin::Pack { id: id.to_string() })
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label())
    }
}

impl Serialize for Origin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode())
    }
}

impl<'de> Deserialize<'de> for Origin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Origin::decode(&value)
            .ok_or_else(|| serde::de::Error::custom(format!("not a library origin: `{value}`")))
    }
}

/// A name as a reference names it: bare (resolved nearest-first) or pinned to
/// one pack's own version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LibraryRef {
    Bare(String),
    Pack { pack: String, name: String },
}

const PACK_ADDRESS_PREFIX: &str = "cairn://packs/";

impl LibraryRef {
    /// Read a reference to a `kind` item: a bare name, or
    /// `cairn://packs/<pack>/<kind>/<name>`. A pack address naming another
    /// kind is refused, since it can only be a mistake.
    pub fn parse(kind: LibraryKind, value: &str) -> Result<Self, String> {
        let Some(rest) = value.strip_prefix(PACK_ADDRESS_PREFIX) else {
            return Ok(LibraryRef::Bare(value.to_string()));
        };
        let shape = || {
            format!(
                "`{value}` is not a pack item address; the form is cairn://packs/<pack>/{}/<name>",
                kind.dir()
            )
        };
        match rest.split('/').collect::<Vec<_>>().as_slice() {
            [pack, plural, name] if !pack.is_empty() && !name.is_empty() => {
                match LibraryKind::ALL.into_iter().find(|k| k.dir() == *plural) {
                    Some(named) if named == kind => Ok(LibraryRef::Pack {
                        pack: pack.to_string(),
                        name: name.to_string(),
                    }),
                    Some(named) => Err(format!("`{value}` names a {named}, not a {kind}")),
                    None => Err(shape()),
                }
            }
            _ => Err(shape()),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            LibraryRef::Bare(name) => name,
            LibraryRef::Pack { name, .. } => name,
        }
    }
}

/// `cairn://packs/<pack>/<kind>/<name>`: one pack's own version of an item.
pub fn pack_item_uri(pack: &str, kind: LibraryKind, name: &str) -> String {
    format!("{PACK_ADDRESS_PREFIX}{pack}/{}/{name}", kind.dir())
}

/// What a definition carries that grants authority: tools, permissions,
/// hooks, executable scripts, model and account selection. Prose (prompts,
/// descriptions, templates, references) is deliberately excluded, so a
/// changed prompt in a trusted project applies with no prompt while a changed
/// tool grant waits for the operator.
pub trait LibraryItem {
    /// The item's canonical id (the name a layer stores it under).
    fn library_id(&self) -> &str;
    /// Each authority-carrying field that is set, by its definition-file key.
    /// Empty when the definition carries no authority, which needs no
    /// confirmation.
    fn authority(&self) -> Map<String, Value>;
}

/// The SHA-256 of a projection's canonical JSON.
pub fn authority_digest(projection: &Map<String, Value>) -> String {
    let canonical = pack::canonical_json(Value::Object(projection.clone()));
    pack::sha256_bytes(&serde_json::to_vec(&canonical).expect("JSON values always serialize"))
}

/// The fields whose values differ between what the operator confirmed and
/// what the definition says now (absent counts as a value).
pub fn changed_fields(
    confirmed: Option<&Map<String, Value>>,
    current: &Map<String, Value>,
) -> Vec<String> {
    let empty = Map::new();
    let confirmed = confirmed.unwrap_or(&empty);
    let keys: BTreeSet<&String> = confirmed.keys().chain(current.keys()).collect();
    keys.into_iter()
        .filter(|key| {
            confirmed.get(*key).map(|v| pack::canonical_json(v.clone()))
                != current.get(*key).map(|v| pack::canonical_json(v.clone()))
        })
        .cloned()
        .collect()
}

/// Whether the operator has trusted a project, and which authority they have
/// confirmed for its definitions.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ProjectTrust {
    #[default]
    Untrusted,
    Trusted {
        /// The authority projection the operator confirmed, per definition.
        confirmed: BTreeMap<(LibraryKind, String), Map<String, Value>>,
    },
}

impl ProjectTrust {
    pub fn is_trusted(&self) -> bool {
        matches!(self, ProjectTrust::Trusted { .. })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectLayer {
    pub key: String,
    /// The project's checkout; its definitions live under `.cairn/`.
    pub path: PathBuf,
    pub trust: ProjectTrust,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PackLayer {
    pub id: String,
    /// `<config_dir>/packs/<id>`: the pack's items sit beneath it at their
    /// source-relative paths.
    pub root: PathBuf,
    /// Items the user removed from this pack (tenet 1121). They stay on disk
    /// so restoring one is clearing the record, but never resolve.
    pub removed: BTreeSet<(LibraryKind, String)>,
}

/// The layers one resolution reads, nearest first.
#[derive(Debug, Clone, PartialEq)]
pub struct LibraryRoots {
    pub project: Option<ProjectLayer>,
    pub workspace: PathBuf,
    pub packs: Vec<PackLayer>,
}

/// The definition a name resolved to, where it came from, and what it hides.
#[derive(Debug, Clone)]
pub struct Resolved<T> {
    pub item: T,
    pub origin: Origin,
    pub shadows: Vec<Origin>,
}

/// Why a listed definition is or is not the one its name resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedStatus {
    /// This is what the name resolves to.
    Active,
    /// A project definition skipped because the project is not trusted.
    Untrusted,
    /// A trusted project's definition whose authority the operator has not
    /// confirmed in its current form. The name resolves to nothing.
    Unconfirmed { fields: Vec<String> },
    /// Defined by several installed packs and nowhere locally. The name
    /// resolves to nothing.
    Collision { packs: Vec<String> },
}

impl ListedStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, ListedStatus::Active)
    }

    /// A stable word for the status, for DTOs.
    pub fn as_str(&self) -> &'static str {
        match self {
            ListedStatus::Active => "active",
            ListedStatus::Untrusted => "untrusted",
            ListedStatus::Unconfirmed { .. } => "unconfirmed",
            ListedStatus::Collision { .. } => "collision",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Listed<T> {
    pub name: String,
    pub entry: ConfigResult<T>,
    pub origin: Origin,
    pub shadows: Vec<Origin>,
    pub status: ListedStatus,
}

/// The listing mark for one row: the origin, then what it shadows or why it
/// is not used (`~, shadows pack:core`, `@, untrusted: skipped`).
pub fn origin_label(origin: &Origin, shadows: &[Origin], status: &ListedStatus) -> String {
    match status {
        ListedStatus::Active if shadows.is_empty() => origin.label(),
        ListedStatus::Active => format!(
            "{}, shadows {}",
            origin.label(),
            shadows
                .iter()
                .map(Origin::label)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ListedStatus::Untrusted => format!("{}, untrusted: skipped", origin.label()),
        ListedStatus::Unconfirmed { fields } => format!(
            "{}, authority unconfirmed: {}",
            origin.label(),
            fields.join(", ")
        ),
        ListedStatus::Collision { packs } => format!(
            "refused: defined by {}",
            packs
                .iter()
                .map(|p| format!("pack:{p}"))
                .collect::<Vec<_>>()
                .join(" and ")
        ),
    }
}

impl<T> Listed<T> {
    pub fn origin_label(&self) -> String {
        origin_label(&self.origin, &self.shadows, &self.status)
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Listed<U> {
        Listed {
            name: self.name,
            entry: match self.entry {
                ConfigResult::Ok(item) => ConfigResult::Ok(f(item)),
                ConfigResult::Err { path, error } => ConfigResult::Err { path, error },
            },
            origin: self.origin,
            shadows: self.shadows,
            status: self.status,
        }
    }
}

/// The definitions in force, in the shape loaders returned before resolution
/// had layers: one entry per name.
pub fn active<T>(listed: Vec<Listed<T>>) -> Vec<ConfigResult<T>> {
    listed
        .into_iter()
        .filter(|row| row.status.is_active())
        .map(|row| row.entry)
        .collect()
}

/// Why a name did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    NotFound {
        kind: LibraryKind,
        name: String,
    },
    /// No pack named by a pack address is installed.
    PackNotInstalled {
        kind: LibraryKind,
        pack: String,
        name: String,
    },
    Collision {
        kind: LibraryKind,
        name: String,
        packs: Vec<String>,
    },
    Untrusted {
        kind: LibraryKind,
        name: String,
        project: String,
        path: String,
    },
    Unconfirmed {
        kind: LibraryKind,
        name: String,
        project: String,
        path: String,
        fields: Vec<String>,
        /// Whether the operator ever confirmed this definition.
        previously_confirmed: bool,
    },
    Removed {
        kind: LibraryKind,
        pack: String,
        name: String,
    },
    /// The definition in force does not load. `origin` is the layer it was
    /// read from, when it came from one.
    Invalid {
        path: PathBuf,
        error: String,
        origin: Option<Origin>,
    },
}

impl ResolveError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, ResolveError::NotFound { .. })
    }
}

/// "tools", "tools and models", "tools, models and hooks".
fn and_list(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn count_word(n: usize) -> String {
    match n {
        2 => "two".into(),
        3 => "three".into(),
        4 => "four".into(),
        n => n.to_string(),
    }
}

/// Where the operator grants and confirms project trust.
pub const TRUST_SETTINGS_PLACE: &str = "Settings → Project → Library definitions";

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolveError::NotFound { kind, name } => write!(f, "{kind} `{name}` not found"),
            ResolveError::PackNotInstalled { kind, pack, name } => write!(
                f,
                "{} names pack {pack}, which is not installed",
                pack_item_uri(pack, *kind, name)
            ),
            ResolveError::Collision { kind, name, packs } => {
                let addresses: Vec<String> = packs
                    .iter()
                    .map(|pack| pack_item_uri(pack, *kind, name))
                    .collect();
                write!(
                    f,
                    "{kind} `{name}` is defined by {} installed packs and nowhere locally: {}. Define ~/.cairn/{}, or name one of those addresses.",
                    count_word(packs.len()),
                    and_list(&addresses),
                    kind.item_rel_path(name)
                )
            }
            ResolveError::Untrusted {
                kind,
                name,
                project,
                path,
            } => write!(
                f,
                "{kind} `{name}` is defined only in project {project} ({path}), which has not been trusted on this install. The operator trusts it in {TRUST_SETTINGS_PLACE}."
            ),
            ResolveError::Unconfirmed {
                kind,
                name,
                project,
                path,
                fields,
                previously_confirmed,
            } => {
                let what = if *previously_confirmed {
                    format!(
                        "whose {} changed since the operator confirmed them",
                        and_list(fields)
                    )
                } else {
                    format!("whose {} the operator has not confirmed", and_list(fields))
                };
                write!(
                    f,
                    "{kind} `{name}` resolves to project {project}'s {path}, {what}. It is not used, and nothing is used in its place, until the operator confirms the change in {TRUST_SETTINGS_PLACE}."
                )
            }
            ResolveError::Removed { kind, pack, name } => write!(
                f,
                "{} was removed from pack {pack}; restore it from the Library.",
                pack_item_uri(pack, *kind, name)
            ),
            ResolveError::Invalid { error, .. } => f.write_str(error),
        }
    }
}

impl From<ResolveError> for String {
    fn from(error: ResolveError) -> Self {
        error.to_string()
    }
}

/// The project directory definitions live in.
pub const PROJECT_CONFIG_DIR: &str = ".cairn";

/// The refusal for a change to a pack's own item beyond its pins.
pub fn pack_edit_refusal(kind: LibraryKind, name: &str, pack: &str) -> String {
    format!(
        "{kind} `{name}` resolves to pack {pack} ({}), and pack items are never edited; write cairn://{}/{name} with layer:\"workspace\" (or \"project\") to define one in front of it",
        pack_item_uri(pack, kind, name),
        kind.dir()
    )
}

/// Remove the definition of `name` that resolved from `origin`, whose file or
/// package directory is `path`.
///
/// A workspace or project definition is deleted outright; nothing else is
/// recorded, so whatever it shadowed resolves in its place. A pack's own item
/// is recorded as removed from that pack (tenet 1121) and stays on disk, so
/// restoring it is clearing the record. Returns the path that changed: the
/// deleted file or directory, or the pack's lock.
pub fn remove_definition(
    roots: &LibraryRoots,
    kind: LibraryKind,
    name: &str,
    origin: &Origin,
    path: &Path,
) -> Result<PathBuf, String> {
    match origin {
        Origin::Pack { id } => pack::lock::record_removal_from(
            &cairn_platform::services::RealFileSystem,
            &roots.workspace,
            id,
            kind.pack_kind(),
            name,
        ),
        Origin::Project { .. } | Origin::Workspace => {
            if path.is_dir() {
                std::fs::remove_dir_all(path)
            } else {
                std::fs::remove_file(path)
            }
            .map_err(|e| {
                format!(
                    "Failed to delete {kind} `{name}` at {}: {e}",
                    path.display()
                )
            })?;
            Ok(path.to_path_buf())
        }
    }
}

impl LibraryRoots {
    /// The workspace at `config_dir` and the packs installed in it, with no
    /// project.
    pub fn workspace(config_dir: impl AsRef<Path>) -> Self {
        let config_dir = config_dir.as_ref();
        LibraryRoots {
            project: None,
            workspace: config_dir.to_path_buf(),
            packs: pack_layers(config_dir),
        }
    }

    pub fn with_project(mut self, project: ProjectLayer) -> Self {
        self.project = Some(project);
        self
    }

    /// The roots a call site resolves through: the workspace at `config_dir`,
    /// its packs, and the project checked out at `project_path` with the trust
    /// the operator recorded for it. A project the registry does not know is
    /// untrusted, so a miss fails closed.
    pub fn at(config_dir: impl AsRef<Path>, project_path: Option<impl AsRef<Path>>) -> Self {
        let roots = Self::workspace(config_dir);
        match project_path {
            Some(path) => roots.with_project(trust::project_layer(path.as_ref())),
            None => roots,
        }
    }

    fn project_dir(&self, kind: LibraryKind) -> Option<PathBuf> {
        self.project
            .as_ref()
            .map(|p| p.path.join(PROJECT_CONFIG_DIR).join(kind.dir()))
    }

    /// The directory a writer puts `kind` in for a definite layer. A pack is
    /// never written to, so it has none; nor does a project the roots lack.
    pub fn layer_dir(&self, kind: LibraryKind, origin: &Origin) -> Option<PathBuf> {
        match origin {
            Origin::Project { .. } => self.project_dir(kind),
            Origin::Workspace => Some(self.workspace.join(kind.dir())),
            Origin::Pack { .. } => None,
        }
    }

    pub fn project_origin(&self) -> Option<Origin> {
        self.project
            .as_ref()
            .map(|p| Origin::Project { key: p.key.clone() })
    }

    pub fn pack(&self, id: &str) -> Option<&PackLayer> {
        self.packs.iter().find(|p| p.id == id)
    }

    /// A path inside the project as messages show it (`.cairn/agents/x.md`).
    fn project_relative(&self, path: &Path) -> String {
        self.project
            .as_ref()
            .and_then(|p| path.strip_prefix(&p.path).ok())
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/")
    }

    /// Check a trusted project's definition against what the operator
    /// confirmed: the fields in question, and whether it was ever confirmed.
    fn unconfirmed_fields<T: LibraryItem>(
        &self,
        kind: LibraryKind,
        item: &T,
    ) -> Option<(Vec<String>, bool)> {
        let ProjectTrust::Trusted { confirmed } = &self.project.as_ref()?.trust else {
            return None;
        };
        let current = item.authority();
        if current.is_empty() {
            return None;
        }
        let recorded = confirmed.get(&(kind, item.library_id().to_string()));
        let fields = changed_fields(recorded, &current);
        (!fields.is_empty()).then_some((fields, recorded.is_some()))
    }

    /// Resolve `name` in one layer only: the project (behind its trust gate,
    /// so an untrusted or unconfirmed definition is refused by name), the
    /// workspace, or one pack. For a caller that asserted where the item
    /// lives, such as `cairn://p/<key>/agents/<name>`.
    pub fn resolve_in<T: LibraryItem>(
        &self,
        kind: LibraryKind,
        layer: &Origin,
        name: &str,
        load: impl Fn(&Path, &str, &Origin) -> Option<ConfigResult<T>>,
    ) -> Result<Resolved<T>, ResolveError> {
        let not_found = || ResolveError::NotFound {
            kind,
            name: name.to_string(),
        };
        let into = |origin: Origin, entry: ConfigResult<T>| match entry {
            ConfigResult::Ok(item) => Ok(Resolved {
                item,
                origin,
                shadows: Vec::new(),
            }),
            ConfigResult::Err { path, error } => Err(ResolveError::Invalid {
                path,
                error,
                origin: Some(origin),
            }),
        };
        match layer {
            Origin::Pack { id } => self.resolve(
                kind,
                &LibraryRef::Pack {
                    pack: id.clone(),
                    name: name.to_string(),
                },
                load,
            ),
            Origin::Workspace => {
                let entry =
                    load(&self.workspace.join(kind.dir()), name, layer).ok_or_else(not_found)?;
                into(Origin::Workspace, entry)
            }
            Origin::Project { .. } => {
                let (Some(project), Some(dir), Some(origin)) =
                    (&self.project, self.project_dir(kind), self.project_origin())
                else {
                    return Err(not_found());
                };
                let entry = load(&dir, name, &origin).ok_or_else(not_found)?;
                let path = entry_path(&entry, &dir, kind, name);
                if !project.trust.is_trusted() {
                    return Err(ResolveError::Untrusted {
                        kind,
                        name: name.to_string(),
                        project: project.key.clone(),
                        path: self.project_relative(&path),
                    });
                }
                if let ConfigResult::Ok(item) = &entry {
                    if let Some((fields, previously_confirmed)) =
                        self.unconfirmed_fields(kind, item)
                    {
                        return Err(ResolveError::Unconfirmed {
                            kind,
                            name: item.library_id().to_string(),
                            project: project.key.clone(),
                            path: self.project_relative(&path),
                            fields,
                            previously_confirmed,
                        });
                    }
                }
                into(origin, entry)
            }
        }
    }

    /// Resolve one reference nearest-first. `load(dir, name, origin)` reads
    /// `name` from one layer's kind directory, returning `None` when that
    /// layer does not define it.
    pub fn resolve<T: LibraryItem>(
        &self,
        kind: LibraryKind,
        reference: &LibraryRef,
        load: impl Fn(&Path, &str, &Origin) -> Option<ConfigResult<T>>,
    ) -> Result<Resolved<T>, ResolveError> {
        let into_item = |entry: ConfigResult<T>, origin: &Origin| match entry {
            ConfigResult::Ok(item) => Ok(item),
            ConfigResult::Err { path, error } => Err(ResolveError::Invalid {
                path,
                error,
                origin: Some(origin.clone()),
            }),
        };
        let name = match reference {
            LibraryRef::Pack { pack, name } => {
                let Some(layer) = self.pack(pack) else {
                    return Err(ResolveError::PackNotInstalled {
                        kind,
                        pack: pack.clone(),
                        name: name.clone(),
                    });
                };
                if layer.removed.contains(&(kind, name.clone())) {
                    return Err(ResolveError::Removed {
                        kind,
                        pack: pack.clone(),
                        name: name.clone(),
                    });
                }
                let origin = Origin::Pack { id: pack.clone() };
                return match load(&layer.root.join(kind.dir()), name, &origin) {
                    Some(entry) => Ok(Resolved {
                        item: into_item(entry, &origin)?,
                        origin,
                        shadows: Vec::new(),
                    }),
                    None => Err(ResolveError::NotFound {
                        kind,
                        name: pack_item_uri(pack, kind, name),
                    }),
                };
            }
            LibraryRef::Bare(name) => name,
        };

        let mut found: Option<(Origin, ConfigResult<T>)> = None;
        let mut shadows = Vec::new();
        let mut untrusted: Option<String> = None;

        if let (Some(project), Some(dir)) = (&self.project, self.project_dir(kind)) {
            let origin = Origin::Project {
                key: project.key.clone(),
            };
            if let Some(entry) = load(&dir, name, &origin) {
                let path = entry_path(&entry, &dir, kind, name);
                if !project.trust.is_trusted() {
                    untrusted = Some(self.project_relative(&path));
                } else {
                    if let ConfigResult::Ok(item) = &entry {
                        if let Some((fields, previously_confirmed)) =
                            self.unconfirmed_fields(kind, item)
                        {
                            return Err(ResolveError::Unconfirmed {
                                kind,
                                name: item.library_id().to_string(),
                                project: project.key.clone(),
                                path: self.project_relative(&path),
                                fields,
                                previously_confirmed,
                            });
                        }
                    }
                    found = Some((origin, entry));
                }
            }
        }

        let workspace = Origin::Workspace;
        if let Some(entry) = load(&self.workspace.join(kind.dir()), name, &workspace) {
            if found.is_none() {
                found = Some((workspace, entry));
            } else {
                shadows.push(workspace);
            }
        }

        let mut pack_hits = Vec::new();
        for layer in &self.packs {
            let origin = Origin::Pack {
                id: layer.id.clone(),
            };
            if let Some(entry) = load(&layer.root.join(kind.dir()), name, &origin) {
                if layer.removed.contains(&(kind, entry_name(&entry, name))) {
                    continue;
                }
                pack_hits.push((origin, entry));
            }
        }
        if found.is_some() {
            shadows.extend(pack_hits.into_iter().map(|(origin, _)| origin));
        } else if pack_hits.len() > 1 {
            return Err(ResolveError::Collision {
                kind,
                name: name.clone(),
                packs: pack_hits
                    .into_iter()
                    .filter_map(|(origin, _)| origin.pack_id().map(str::to_string))
                    .collect(),
            });
        } else {
            found = pack_hits.pop();
        }

        match found {
            Some((origin, entry)) => Ok(Resolved {
                item: into_item(entry, &origin)?,
                origin,
                shadows,
            }),
            None => match (untrusted, &self.project) {
                (Some(path), Some(project)) => Err(ResolveError::Untrusted {
                    kind,
                    name: name.clone(),
                    project: project.key.clone(),
                    path,
                }),
                _ => Err(ResolveError::NotFound {
                    kind,
                    name: name.clone(),
                }),
            },
        }
    }

    /// Every definition of `kind` across the layers: one row per name in
    /// force, plus a row for each project definition that is not and each
    /// name refused for a collision. `enumerate` lists one layer's kind
    /// directory as `(name, entry)` pairs.
    pub fn list<T: LibraryItem>(
        &self,
        kind: LibraryKind,
        enumerate: impl Fn(&Path, &Origin) -> Result<Vec<(String, ConfigResult<T>)>, String>,
    ) -> Result<Vec<Listed<T>>, String> {
        let mut project: BTreeMap<String, ConfigResult<T>> = BTreeMap::new();
        if let (Some(origin), Some(dir)) = (self.project_origin(), self.project_dir(kind)) {
            if dir.is_dir() {
                for (name, entry) in enumerate(&dir, &origin)? {
                    project.entry(name).or_insert(entry);
                }
            }
        }
        let mut workspace: BTreeMap<String, ConfigResult<T>> = BTreeMap::new();
        let workspace_dir = self.workspace.join(kind.dir());
        if workspace_dir.is_dir() {
            for (name, entry) in enumerate(&workspace_dir, &Origin::Workspace)? {
                workspace.entry(name).or_insert(entry);
            }
        }
        let mut packs: BTreeMap<String, Vec<(Origin, ConfigResult<T>)>> = BTreeMap::new();
        for layer in &self.packs {
            let dir = layer.root.join(kind.dir());
            if !dir.is_dir() {
                continue;
            }
            let origin = Origin::Pack {
                id: layer.id.clone(),
            };
            for (name, entry) in enumerate(&dir, &origin)? {
                if layer.removed.contains(&(kind, name.clone())) {
                    continue;
                }
                packs.entry(name).or_default().push((origin.clone(), entry));
            }
        }

        let names: BTreeSet<String> = project
            .keys()
            .chain(workspace.keys())
            .chain(packs.keys())
            .cloned()
            .collect();
        let trusted = self.project.as_ref().is_some_and(|p| p.trust.is_trusted());
        let project_origin = self.project_origin();

        let mut rows = Vec::new();
        for name in names {
            let mut in_force: Option<(Origin, ConfigResult<T>)> = None;
            let mut shadows = Vec::new();

            if let (Some(entry), Some(origin)) = (project.remove(&name), &project_origin) {
                if !trusted {
                    rows.push(Listed {
                        name: name.clone(),
                        entry,
                        origin: origin.clone(),
                        shadows: Vec::new(),
                        status: ListedStatus::Untrusted,
                    });
                } else {
                    let unconfirmed = match &entry {
                        ConfigResult::Ok(item) => self.unconfirmed_fields(kind, item),
                        ConfigResult::Err { .. } => None,
                    };
                    if let Some((fields, _)) = unconfirmed {
                        rows.push(Listed {
                            name: name.clone(),
                            entry,
                            origin: origin.clone(),
                            shadows: Vec::new(),
                            status: ListedStatus::Unconfirmed { fields },
                        });
                        continue;
                    }
                    in_force = Some((origin.clone(), entry));
                }
            }

            if let Some(entry) = workspace.remove(&name) {
                if in_force.is_none() {
                    in_force = Some((Origin::Workspace, entry));
                } else {
                    shadows.push(Origin::Workspace);
                }
            }

            let mut hits = packs.remove(&name).unwrap_or_default();
            if in_force.is_some() {
                shadows.extend(hits.into_iter().map(|(origin, _)| origin));
            } else if hits.len() > 1 {
                let ids: Vec<String> = hits
                    .iter()
                    .filter_map(|(origin, _)| origin.pack_id().map(str::to_string))
                    .collect();
                let (origin, entry) = hits.remove(0);
                rows.push(Listed {
                    name,
                    entry,
                    origin,
                    shadows: Vec::new(),
                    status: ListedStatus::Collision { packs: ids },
                });
                continue;
            } else {
                in_force = hits.pop();
            }

            if let Some((origin, entry)) = in_force {
                rows.push(Listed {
                    name,
                    entry,
                    origin,
                    shadows,
                    status: ListedStatus::Active,
                });
            }
        }
        Ok(rows)
    }
}

/// The file an entry was loaded from, for messages.
fn entry_path<T>(entry: &ConfigResult<T>, dir: &Path, kind: LibraryKind, name: &str) -> PathBuf {
    match entry {
        ConfigResult::Err { path, .. } => path.clone(),
        ConfigResult::Ok(_) => dir.parent().unwrap_or(dir).join(kind.item_rel_path(name)),
    }
}

fn entry_name<T: LibraryItem>(entry: &ConfigResult<T>, requested: &str) -> String {
    match entry {
        ConfigResult::Ok(item) => item.library_id().to_string(),
        ConfigResult::Err { .. } => requested.to_string(),
    }
}

/// The pack layers installed in `config_dir`, sorted by id.
pub fn pack_layers(config_dir: &Path) -> Vec<PackLayer> {
    pack::installed_packs(config_dir)
        .into_iter()
        .filter(|lock| !pack::lock::is_uninstalled(config_dir, &lock.id))
        .map(|lock| PackLayer {
            root: pack::lock::pack_dir(config_dir, &lock.id),
            removed: lock
                .removed
                .iter()
                .filter_map(|item| {
                    LibraryKind::from_pack_kind(item.kind).map(|kind| (kind, item.id.clone()))
                })
                .collect(),
            id: lock.id,
        })
        .collect()
}

/// The operator's trust decisions, keyed by project checkout path.
///
/// cairn-core is the only writer: it loads every project's record at startup
/// and rewrites a project's entry in the same call that grants, confirms or
/// revokes trust, or that creates, repoints or deletes the project, so a
/// revocation is in force at the next resolution. A path with no entry
/// resolves as untrusted.
pub mod trust {
    use super::*;
    use std::sync::{OnceLock, RwLock};

    #[derive(Debug, Clone)]
    struct Entry {
        key: String,
        trust: ProjectTrust,
        /// The store a refresh read this entry from; `None` for [`set`].
        source: Option<String>,
    }

    fn registry() -> &'static RwLock<BTreeMap<PathBuf, Entry>> {
        static REGISTRY: OnceLock<RwLock<BTreeMap<PathBuf, Entry>>> = OnceLock::new();
        REGISTRY.get_or_init(|| RwLock::new(BTreeMap::new()))
    }

    /// Record (or replace) the trust for the project checked out at `path`.
    pub fn set(path: &Path, key: &str, trust: ProjectTrust) {
        let mut map = registry().write().unwrap_or_else(|e| e.into_inner());
        map.insert(
            path.to_path_buf(),
            Entry {
                key: key.to_string(),
                trust,
                source: None,
            },
        );
    }

    /// Replace everything a refresh of `source` recorded with `entries`: the
    /// runner's refresh reads every project and every trust record in its store
    /// at once, so a project that was removed or moved leaves nothing behind.
    /// Entries from another source are kept, which lets several stores (tests,
    /// chiefly) share the process.
    pub fn replace_all(
        source: &str,
        entries: impl IntoIterator<Item = (PathBuf, String, ProjectTrust)>,
    ) {
        let mut map = registry().write().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, entry| entry.source.as_deref() != Some(source));
        for (path, key, trust) in entries {
            map.insert(
                path,
                Entry {
                    key,
                    trust,
                    source: Some(source.to_string()),
                },
            );
        }
    }

    /// Forget the project checked out at `path`.
    pub fn remove(path: &Path) {
        registry()
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(path);
    }

    /// The project layer for `path`: its recorded key and trust, or untrusted
    /// under the directory's own name when nothing is recorded.
    pub fn project_layer(path: &Path) -> ProjectLayer {
        let map = registry().read().unwrap_or_else(|e| e.into_inner());
        let entry = map.get(path).cloned().or_else(|| {
            let canonical = path.canonicalize().ok()?;
            map.iter()
                .find(|(known, _)| known.canonicalize().ok().as_deref() == Some(&canonical))
                .map(|(_, entry)| entry.clone())
        });
        match entry {
            Some(entry) => ProjectLayer {
                key: entry.key,
                path: path.to_path_buf(),
                trust: entry.trust,
            },
            None => ProjectLayer {
                key: path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                path: path.to_path_buf(),
                trust: ProjectTrust::Untrusted,
            },
        }
    }
}

/// Fixtures that place definitions in a layer the way the loaders expect, so
/// no test hand-joins a layer path.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support {
    use super::*;

    /// The directory holding a project's kind directories (`<project>/.cairn`).
    pub fn project_root(project: &Path) -> PathBuf {
        project.join(PROJECT_CONFIG_DIR)
    }

    /// The directory holding a pack's kind directories
    /// (`<workspace>/packs/<id>`).
    pub fn pack_root(workspace: &Path, pack_id: &str) -> PathBuf {
        pack::lock::pack_dir(workspace, pack_id)
    }

    /// Write `body` as the definition of `name` in the layer rooted at
    /// `layer_root` (the workspace, [`project_root`] or [`pack_root`]),
    /// returning its path.
    pub fn write_item(layer_root: &Path, kind: LibraryKind, name: &str, body: &str) -> PathBuf {
        let path = layer_root.join(kind.item_rel_path(name));
        std::fs::create_dir_all(path.parent().expect("item paths have a parent")).unwrap();
        std::fs::write(&path, body).unwrap();
        path
    }

    /// Record a minimal install lock for `pack_id`, so its directory is read
    /// as a pack layer, with `removed` recorded as the user's removals.
    pub fn install_pack_lock(workspace: &Path, pack_id: &str, removed: &[(LibraryKind, &str)]) {
        let manifest = pack::PackManifest {
            id: pack_id.to_string(),
            name: pack_id.to_string(),
            version: "1.0.0".into(),
            description: String::new(),
            author: None,
            homepage: None,
            license: None,
            keywords: vec![],
            default: false,
            retired: vec![],
            format: pack::PackFormat::Cairn,
            notes: vec![],
            root: PathBuf::new(),
        };
        let mut lock = pack::PackLock::new(
            &manifest,
            String::new(),
            pack::PackSource::bundled(pack::PackFormat::Cairn),
            vec![],
        );
        lock.removed = removed
            .iter()
            .map(|(kind, id)| pack::PackItem::file(kind.pack_kind(), *id, kind.item_rel_path(id)))
            .collect();
        pack::lock::write_lock(&cairn_platform::services::RealFileSystem, workspace, &lock)
            .unwrap();
    }

    /// Trust with exactly these confirmations.
    pub fn trusted(confirmed: &[(LibraryKind, &str, Map<String, Value>)]) -> ProjectTrust {
        ProjectTrust::Trusted {
            confirmed: confirmed
                .iter()
                .map(|(kind, name, projection)| ((*kind, name.to_string()), projection.clone()))
                .collect(),
        }
    }

    pub fn project(path: &Path, key: &str, trust: ProjectTrust) -> ProjectLayer {
        ProjectLayer {
            key: key.to_string(),
            path: path.to_path_buf(),
            trust,
        }
    }
}

#[cfg(test)]
mod tests;
