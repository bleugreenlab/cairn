//! File-based project settings.
//!
//! Project settings are stored in `[project]/.cairn/config.yaml` and are the source of truth.
//! These files can be version-controlled with the project.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

pub use super::contextual_packages::{
    ContextualPackageKind, ContextualPackageRef, ContextualPackagesConfig,
};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::models::{Preset, TerminalCommand};
use crate::project_reference::ProjectReference;

/// Executor materialization settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MaterializationSettings {
    /// Legacy field — deserialized for migration but not re-serialized.
    #[serde(default, skip_serializing)]
    seed_ignored: Option<bool>,
    /// Rules for populating gitignored paths into executor materializations, grouped by strategy.
    #[serde(default, skip_serializing_if = "PopulateConfig::is_empty")]
    pub populate: PopulateConfig,
}

// The check/materialization-populate config value types now live in `models::project`
// (pure serde data, no upward dependency on config). Re-exported here so every
// `project_settings::CheckCommand`-style path keeps resolving.
use crate::models::{read_declared_check, DeclaredCheck};
pub use crate::models::{
    CheckCommand, CheckPolicy, CheckResourceClass, CheckScopeSelector, CheckTree, CheckVocabulary,
    CheckWhen, MainCadence, PopulateConfig, UnsupportedCheck,
};

/// Project settings as stored in YAML file.
/// All fields are optional - missing fields use defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSettingsFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<HashMap<String, CheckCommand>>,
    /// Declared checks this build cannot run (see [`UnsupportedCheck`]). They
    /// are kept out of `checks`, so nothing plans them and no editor that
    /// replaces `checks` can see them, and [`parse_project_settings`] fills
    /// this rather than serde: a settings save writes them back verbatim.
    #[serde(skip)]
    pub unsupported_checks: BTreeMap<String, UnsupportedCheck>,
    /// Inputs of a dependency-graph NODE that no manifest edge expresses, keyed
    /// by the same `rust:<crate>` / `ts:<package>` token a check's `scope` uses.
    /// Attaching them to the node rather than to a check makes them compose
    /// transitively: the SQL a crate `include_str!`s becomes an input of every
    /// check whose closure reaches that crate. See docs/checks.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_inputs: Option<HashMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<ProjectReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<MaterializationSettings>,
    /// Project-level overrides of which backend serves each tier by default,
    /// merged tier by tier over the workspace bindings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier_defaults: Option<HashMap<String, String>>,
    /// Legacy project-level global default provider. It said "every tier in this
    /// project runs on this backend", so it loads as exactly that before any
    /// `tierDefaults` entry refines it. Read-only: nothing writes this key, but
    /// a project config is version-controlled by its own repository, so a value
    /// already committed there is preserved rather than silently deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backend: Option<String>,
    /// Project-level preset overrides (deep-merged with workspace)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,
    /// Project-level external MCP servers (overlay workspace set; project wins
    /// on key collision).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<HashMap<String, cairn_packs::server_config::McpServerConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contextual_packages: Option<ContextualPackagesConfig>,
    /// Upstream tool pins whose version bumps may land through the `pinBump`
    /// merge precondition, keyed by tool name. Read from the merge TARGET's
    /// revision, never the PR's, so a bump cannot redefine its own gate. See
    /// docs/checks.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_bumps: Option<BTreeMap<String, PinBumpSpec>>,
    /// How the store lands work: which registry files merge by structure
    /// rather than by lines. Read from the commit being landed onto, never from
    /// the branch being landed, so no branch can change the rules it lands by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<LandingSettings>,
}

/// One upstream tool pin a `pinBump` merge may advance.
///
/// The pin file is a JSON object with a string `version` and an optional `skip`
/// array of versions a bump must never select. Unknown keys are refused rather
/// than ignored: a misspelled `frozen` would otherwise silently drop a guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PinBumpSpec {
    /// Repo-relative path of the pin file.
    pub pin: String,
    /// Top-level pin fields a bump must leave unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frozen: Vec<String>,
    /// Configured checks that must have passed, with verified evidence, at the
    /// PR head on every one of `platforms`.
    pub checks: Vec<String>,
    /// The platforms a bump must be proven on, each by its own verdict. Every
    /// one must be in each required check's `verdictPlatforms`, which stays a
    /// trust set: widening a check's trust never adds a platform a bump needs.
    pub platforms: Vec<String>,
}

/// The `landing` section of `.cairn/config.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LandingSettings {
    /// Sequential identifiers a branch authors provisionally and the store
    /// binds when the branch lands on the default branch, by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub identifiers: BTreeMap<String, LandingIdentifier>,
    /// Paths whose conflicts the store settles structurally, first match wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merge: Vec<LandingMergeRule>,
}

/// One declared sequential identifier: a binding rule and where it applies.
/// The rules are code; this instantiates one. Fields are kept as written so a
/// kind this build does not know survives a settings round trip;
/// [`LandingIdentifier::rule`] is the typed reading.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LandingIdentifier {
    pub kind: String,
    /// `numbered-file`: the repo-relative directory of `NNNN_name.ext` files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    /// `numbered-file`: digits in a bound number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<usize>,
    /// `version-heading`: the repo-relative file declaring the constant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// `version-heading`: the constant whose doc block holds the headings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
}

/// The binding rules the landing knows how to apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierRule {
    /// Files named `NNNN_name.ext` (or provisionally `NEXT_name.ext`) in one
    /// directory: every file a branch adds takes the destination's next free
    /// numbers, its stem rewritten wherever the branch names it.
    NumberedFile { directory: String, width: usize },
    /// `/// Version N` headings above a constant: the one heading a branch
    /// authors takes the destination's version plus one.
    VersionHeading { file: String, anchor: String },
}

impl LandingIdentifier {
    /// The rule this declaration instantiates: `Ok(None)` for a kind this
    /// build does not know, an error for a known kind missing what it needs.
    pub fn rule(&self) -> Result<Option<IdentifierRule>, String> {
        match self.kind.as_str() {
            "numbered-file" => match (&self.directory, self.width) {
                (Some(directory), width) => Ok(Some(IdentifierRule::NumberedFile {
                    directory: directory.trim_end_matches('/').to_string(),
                    width: width.unwrap_or(4),
                })),
                (None, _) => Err("a `numbered-file` identifier names its `directory`".into()),
            },
            "version-heading" => match (&self.file, &self.anchor) {
                (Some(file), Some(anchor)) => Ok(Some(IdentifierRule::VersionHeading {
                    file: file.clone(),
                    anchor: anchor.clone(),
                })),
                _ => Err("a `version-heading` identifier names its `file` and `anchor`".into()),
            },
            _ => Ok(None),
        }
    }
}

/// One declared structural merge: a merge kind and the path globs it covers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LandingMergeRule {
    /// Repo-relative globs (`*` stays within a path segment, `**` crosses
    /// them).
    pub paths: Vec<String>,
    /// The merge kind's wire name. Kept as written so a kind this build does
    /// not know survives a settings round trip; [`LandingMergeRule::merge_kind`]
    /// is the typed reading.
    pub kind: String,
    /// `version-paragraphs`: the constant whose doc block is merged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
}

/// The structural merges the store knows how to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandingMergeKind {
    /// The elements of a list merge as a set in base order: both sides'
    /// insertions are kept, an identical insertion once.
    OrderedSet,
    /// The paragraphs of one constant's doc block merge as an ordered set,
    /// the same way: two branches each adding a `/// Version` paragraph keep
    /// both, the landed side's first.
    VersionParagraphs,
}

impl LandingMergeRule {
    /// The kind this build can perform, or `None` for one it does not know,
    /// whose paths keep the line merge.
    pub fn merge_kind(&self) -> Option<LandingMergeKind> {
        match self.kind.as_str() {
            "ordered-set" => Some(LandingMergeKind::OrderedSet),
            "version-paragraphs" => Some(LandingMergeKind::VersionParagraphs),
            _ => None,
        }
    }
}

impl ProjectSettingsFile {
    /// The legacy global default provider this project config still carries, if any.
    pub fn legacy_active_backend(&self) -> Option<&str> {
        self.active_backend.as_deref()
    }

    /// Get the populate config for executor materializations.
    /// Returns the configured PopulateConfig, or empty (skip-all) by default.
    pub fn populate_config(&self) -> PopulateConfig {
        self.materialization
            .as_ref()
            .map(|w| w.populate.clone())
            .unwrap_or_default()
    }
}

/// Intermediate struct for loading legacy config files.
/// Used to detect removed fields (ciCommands, persistent) for migration.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyProjectSettingsFile {
    #[serde(default)]
    ci_commands: Option<Vec<String>>,
    #[serde(default)]
    setup_commands: Option<Vec<String>>,
    #[serde(default)]
    copy_files: Option<Vec<String>>,
    #[serde(default)]
    terminal_commands: Option<Vec<LegacyTerminalCommand>>,
    /// Read entry by entry under the build's vocabulary, so one value this
    /// build does not know costs only the check that carries it.
    #[serde(default)]
    checks: Option<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    extra_inputs: Option<HashMap<String, Vec<String>>>,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    references: Option<Vec<ProjectReference>>,
    #[serde(default)]
    materialization: Option<MaterializationSettings>,
    // Preset fields — must be present so they survive the legacy parse path
    #[serde(default)]
    tier_defaults: Option<HashMap<String, String>>,
    #[serde(default)]
    active_backend: Option<String>,
    #[serde(default)]
    backends: Option<HashMap<String, HashMap<String, Preset>>>,
    #[serde(default)]
    mcp_servers: Option<HashMap<String, cairn_packs::server_config::McpServerConfig>>,
    #[serde(default)]
    contextual_packages: Option<ContextualPackagesConfig>,
    #[serde(default)]
    landing: Option<LandingSettings>,
    #[serde(default)]
    pin_bumps: Option<BTreeMap<String, PinBumpSpec>>,
}

/// Legacy terminal command with persistent field
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyTerminalCommand {
    name: String,
    command: String,
    #[serde(default)]
    persistent: bool,
}

/// Get the path to the project config file (\[project\]/.cairn/config.yaml)
pub fn get_project_config_path(project_path: &Path) -> PathBuf {
    project_path.join(".cairn").join("config.yaml")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectConfigMigration {
    Current,
    Legacy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectConfigError {
    Missing {
        path: PathBuf,
    },
    Read {
        path: PathBuf,
        source: String,
    },
    InvalidUtf8 {
        path: PathBuf,
        source: String,
    },
    Parse {
        path: PathBuf,
        source: String,
    },
    InvalidRoot {
        path: PathBuf,
    },
    Mutation {
        path: PathBuf,
        source: String,
    },
    Render {
        path: PathBuf,
        source: String,
    },
    DestructiveChange {
        path: PathBuf,
        before_lines: usize,
        after_lines: usize,
        before_bytes: usize,
        after_bytes: usize,
    },
    Persist {
        path: PathBuf,
        source: String,
    },
    Commit {
        path: PathBuf,
        source: String,
    },
}

impl fmt::Display for ProjectConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { path } => write!(f, "project config is missing at {}", path.display()),
            Self::Read { path, source } => write!(f, "failed to read project config at {}: {source}", path.display()),
            Self::InvalidUtf8 { path, source } => write!(f, "project config at {} is not valid UTF-8: {source}", path.display()),
            Self::Parse { path, source } => write!(f, "failed to parse project config at {}: {source}", path.display()),
            Self::InvalidRoot { path } => write!(f, "project config root at {} must be a YAML mapping", path.display()),
            Self::Mutation { path, source } => write!(f, "project config mutation failed at {}: {source}", path.display()),
            Self::Render { path, source } => write!(f, "failed to render project config at {}: {source}", path.display()),
            Self::DestructiveChange { path, before_lines, after_lines, before_bytes, after_bytes } => write!(f, "refusing destructive project config rewrite at {} (non-comment lines {before_lines} -> {after_lines}, bytes {before_bytes} -> {after_bytes})", path.display()),
            Self::Persist { path, source } => write!(f, "failed to persist project config at {}: {source}", path.display()),
            Self::Commit { path, source } => write!(f, "failed to commit project config at {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for ProjectConfigError {}

/// Strictly load the project configuration without writing or migrating it.
pub fn load_project_settings_strict(
    project_path: &Path,
) -> Result<(ProjectSettingsFile, ProjectConfigMigration), ProjectConfigError> {
    let path = get_project_config_path(project_path);
    let bytes = std::fs::read(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ProjectConfigError::Missing { path: path.clone() }
        } else {
            ProjectConfigError::Read {
                path: path.clone(),
                source: error.to_string(),
            }
        }
    })?;
    parse_project_settings_bytes(&path, &bytes)
}

/// Tolerant, read-only projection. Missing or invalid configuration becomes the
/// default, but is never migrated or otherwise written as a side effect.
pub fn load_project_settings_read_only(project_path: &Path) -> ProjectSettingsFile {
    match load_project_settings_strict(project_path) {
        Ok((file, _)) => file,
        Err(ProjectConfigError::Missing { .. }) => ProjectSettingsFile::default(),
        Err(e) => {
            log::error!("Using default read-only project settings: {e}");
            ProjectSettingsFile::default()
        }
    }
}

/// Resolve the effective default branch for a project.
///
/// Precedence: an explicit `defaultBranch` in the project's `.cairn/config.yaml`
/// wins, then the value stored on the project row, then the hard fallback
/// `"main"`. Both the UI projection and branch-coordinate creation resolve
/// through this helper so they always agree on the durable base.
pub fn resolve_default_branch(
    config: &ProjectSettingsFile,
    stored_default_branch: Option<&str>,
) -> String {
    config
        .default_branch
        .clone()
        .or_else(|| stored_default_branch.map(str::to_string))
        .unwrap_or_else(|| "main".to_string())
}

/// Load the raw project settings file.
/// Returns (settings, needs_migration) where needs_migration is true if legacy fields were found.
fn load_project_settings_file(project_path: &Path) -> Result<(ProjectSettingsFile, bool), String> {
    match load_project_settings_strict(project_path) {
        Ok((settings, migration)) => Ok((settings, migration == ProjectConfigMigration::Legacy)),
        Err(ProjectConfigError::Missing { .. }) => Ok((ProjectSettingsFile::default(), false)),
        Err(error) => Err(error.to_string()),
    }
}

/// The config document's YAML root, after the checks every reader shares:
/// UTF-8, well-formed YAML, and a mapping (or the comment-only null root).
fn parse_project_config_root<'a>(
    path: &Path,
    bytes: &'a [u8],
) -> Result<(&'a str, serde_yaml::Value), ProjectConfigError> {
    let content = std::str::from_utf8(bytes).map_err(|error| ProjectConfigError::InvalidUtf8 {
        path: path.to_path_buf(),
        source: error.to_string(),
    })?;
    let root = serde_yaml::from_str::<serde_yaml::Value>(content).map_err(|error| {
        ProjectConfigError::Parse {
            path: path.to_path_buf(),
            source: error.to_string(),
        }
    })?;
    // A null root is the file Cairn itself generates: the default template is
    // comment-only, which YAML parses as null. Only a genuinely wrong root
    // (sequence, scalar) is invalid.
    if !matches!(
        root,
        serde_yaml::Value::Mapping(_) | serde_yaml::Value::Null
    ) {
        return Err(ProjectConfigError::InvalidRoot {
            path: path.to_path_buf(),
        });
    }
    Ok((content, root))
}

fn parse_project_settings_bytes(
    path: &Path,
    bytes: &[u8],
) -> Result<(ProjectSettingsFile, ProjectConfigMigration), ProjectConfigError> {
    let (content, _root) = parse_project_config_root(path, bytes)?;
    let (settings, legacy) =
        parse_project_settings(content).map_err(|source| ProjectConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    Ok((
        settings,
        if legacy {
            ProjectConfigMigration::Legacy
        } else {
            ProjectConfigMigration::Current
        },
    ))
}

/// Parse `.cairn/config.yaml` CONTENT into settings, independent of where those
/// bytes came from. The filesystem loader reads a checkout; the check cadences
/// read the same file out of the immutable commit they are evaluating. Both go
/// through this one parser so a commit-sourced contract and a checkout-sourced
/// contract can never diverge in how they interpret the same bytes.
///
/// Returns (settings, needs_migration) where needs_migration is true if legacy
/// fields were found. A commit-sourced caller ignores the migration flag: a
/// sealed commit is not something to rewrite.
pub fn parse_project_settings(content: &str) -> Result<(ProjectSettingsFile, bool), String> {
    let parsed = parse_project_settings_under(content, &CheckVocabulary::this_build())?;
    note_unsupported_checks(&parsed.0.unsupported_checks);
    Ok(parsed)
}

/// [`parse_project_settings`] under an explicit check vocabulary, which is how
/// a test stands in for a build older than the config it reads. Logs nothing:
/// a caller judging a file the project does not run yet, such as a merge's
/// preflight, must not report its checks as skipped.
fn parse_project_settings_under(
    content: &str,
    vocabulary: &CheckVocabulary,
) -> Result<(ProjectSettingsFile, bool), String> {
    // First try to parse as legacy format to detect deprecated fields
    let legacy: LegacyProjectSettingsFile = serde_yaml::from_str(content)
        .map_err(|e| format!("Failed to parse project config file: {}", e))?;

    // Check if migration is needed
    let has_ci_commands = legacy.ci_commands.is_some();
    let has_copy_files = legacy.copy_files.is_some();
    if let Some(ref files) = legacy.copy_files {
        log::warn!(
            "Removing deprecated copyFiles from project config: {:?}. \
             Use materialization.populate.copy patterns instead.",
            files
        );
    }
    let has_persistent = legacy
        .terminal_commands
        .as_ref()
        .map(|cmds| cmds.iter().any(|c| c.persistent))
        .unwrap_or(false);
    // Legacy seedIgnored field triggers migration to clear it from the file
    let has_legacy_seed_ignored = legacy
        .materialization
        .as_ref()
        .and_then(|w| w.seed_ignored)
        .is_some();
    let needs_migration =
        has_ci_commands || has_copy_files || has_persistent || has_legacy_seed_ignored;

    let mut checks = None;
    let mut unsupported_checks = BTreeMap::new();
    if let Some(declared) = legacy.checks {
        let mut supported = HashMap::with_capacity(declared.len());
        for (name, entry) in declared {
            match read_declared_check(entry, vocabulary) {
                Ok(DeclaredCheck::Supported(check)) => {
                    supported.insert(name, check);
                }
                Ok(DeclaredCheck::Unsupported(unsupported)) => {
                    unsupported_checks.insert(name, unsupported);
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to parse project config file: checks.{name}: {error}"
                    ))
                }
            }
        }
        checks = Some(supported);
    }

    // Convert to current format (dropping deprecated fields)
    let mut settings = ProjectSettingsFile {
        setup_commands: legacy.setup_commands,
        terminal_commands: legacy.terminal_commands.map(|cmds| {
            cmds.into_iter()
                .map(|c| TerminalCommand {
                    name: c.name,
                    command: c.command,
                })
                .collect()
        }),
        checks,
        unsupported_checks,
        extra_inputs: legacy.extra_inputs,
        default_branch: legacy.default_branch,
        references: legacy.references,
        materialization: legacy.materialization,
        tier_defaults: legacy.tier_defaults,
        active_backend: legacy.active_backend,
        backends: legacy.backends,
        mcp_servers: legacy.mcp_servers,
        contextual_packages: legacy.contextual_packages,
        landing: legacy.landing,
        pin_bumps: legacy.pin_bumps,
    };

    if let Some(policy) = &mut settings.contextual_packages {
        policy.normalize()?;
    }

    Ok((settings, needs_migration))
}

/// Log each unsupported check once per process. The config is parsed on every
/// projection and cadence, so logging per parse would repeat the same line
/// for as long as the build and the file disagree.
fn note_unsupported_checks(unsupported: &BTreeMap<String, UnsupportedCheck>) {
    static NOTED: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    if unsupported.is_empty() {
        return;
    }
    let mut noted = NOTED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (name, check) in unsupported {
        if noted.insert(format!("{name}\0{}\0{}", check.field, check.value)) {
            log::warn!(
                "project config: check `{name}` is skipped: {}",
                check.notice
            );
        }
    }
}

/// Load the project's terminal commands.
///
/// Reads `[project_path]/.cairn/config.yaml` directly without the migration
/// rewrite that `load_project_settings_read_only` performs, so it is safe on the hot
/// logical-fence policy-build path. Returns an empty list when absent or invalid.
pub fn load_terminal_commands(project_path: &Path) -> Vec<crate::models::TerminalCommand> {
    load_project_settings_file(project_path)
        .map(|(file, _)| file.terminal_commands.unwrap_or_default())
        .unwrap_or_default()
}

/// The project's canonical executor setup: what placing ANY batch in the
/// project reads from the live primary checkout.
///
/// Unlike [`load_project_settings_read_only`], errors remain visible so an
/// unreadable or invalid setup policy fails before any command reaches an
/// executor. Because such a failure refuses every execution in the project,
/// this reads only the keys placement needs: a check, a backend preset, or any
/// other section the running build cannot read must not take placement down
/// with it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutionProjectPolicy {
    pub setup_commands: Vec<String>,
    pub populate: PopulateConfig,
}

/// The keys of `.cairn/config.yaml` that [`ExecutionProjectPolicy`] is read
/// from. Every other key is ignored here.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionPolicySection {
    #[serde(default)]
    setup_commands: Option<Vec<String>>,
    #[serde(default)]
    materialization: Option<MaterializationSettings>,
}

pub fn load_execution_project_policy(
    project_path: &Path,
) -> Result<ExecutionProjectPolicy, String> {
    let path = get_project_config_path(project_path);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExecutionProjectPolicy::default())
        }
        Err(error) => {
            return Err(ProjectConfigError::Read {
                path,
                source: error.to_string(),
            }
            .to_string())
        }
    };
    execution_project_policy_from(&path, &bytes)
}

/// [`ExecutionProjectPolicy`] from the bytes of a config document; `path`
/// only names it in an error.
fn execution_project_policy_from(
    path: &Path,
    bytes: &[u8],
) -> Result<ExecutionProjectPolicy, String> {
    let (_, root) = parse_project_config_root(path, bytes).map_err(|error| error.to_string())?;
    let section = match root {
        serde_yaml::Value::Null => ExecutionPolicySection::default(),
        root => serde_yaml::from_value::<ExecutionPolicySection>(root).map_err(|error| {
            ProjectConfigError::Parse {
                path: path.to_path_buf(),
                source: error.to_string(),
            }
            .to_string()
        })?,
    };
    Ok(ExecutionProjectPolicy {
        setup_commands: section.setup_commands.unwrap_or_default(),
        populate: section.materialization.unwrap_or_default().populate,
    })
}

/// What failing to read a config document would cost the project, which is
/// what a refusal to land it has to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnreadableConfigCost {
    /// Placement cannot load its policy, so every batch, check lane, and
    /// terminal in the project is refused.
    EveryExecution,
    /// Placement still works, but the settings do not parse: no check is
    /// planned for any commit carrying the file, and the project's terminals
    /// and Settings do not load.
    ChecksAndSettings,
}

/// A config document the running build cannot read, and what that costs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableProjectConfig {
    pub cost: UnreadableConfigCost,
    /// The parser's own words.
    pub reason: String,
}

/// What the running build makes of one `.cairn/config.yaml` document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectConfigReading {
    /// It loads. The checks it sets aside as unsupported are listed.
    Readable {
        unsupported: BTreeMap<String, UnsupportedCheck>,
    },
    Unreadable(UnreadableProjectConfig),
}

/// Read a config document's bytes the way the running build would read it from
/// a checkout: the placement policy first, since that is what refuses every
/// execution, then the whole settings file. `None` is an absent file, which
/// reads as the defaults.
pub fn read_project_config(content: Option<&[u8]>) -> ProjectConfigReading {
    read_project_config_under(content, &CheckVocabulary::this_build())
}

fn read_project_config_under(
    content: Option<&[u8]>,
    vocabulary: &CheckVocabulary,
) -> ProjectConfigReading {
    let Some(bytes) = content else {
        return ProjectConfigReading::Readable {
            unsupported: BTreeMap::new(),
        };
    };
    let unreadable =
        |cost, reason| ProjectConfigReading::Unreadable(UnreadableProjectConfig { cost, reason });
    let path = Path::new(".cairn").join("config.yaml");
    if let Err(reason) = execution_project_policy_from(&path, bytes) {
        return unreadable(UnreadableConfigCost::EveryExecution, reason);
    }
    // The policy read above confirmed the bytes are UTF-8.
    let content = String::from_utf8_lossy(bytes);
    match parse_project_settings_under(&content, vocabulary) {
        Ok((settings, _)) => ProjectConfigReading::Readable {
            unsupported: settings.unsupported_checks,
        },
        Err(reason) => unreadable(UnreadableConfigCost::ChecksAndSettings, reason),
    }
}

/// What landing a branch's `.cairn/config.yaml` on the default branch would do
/// to the build that is running now, which reads the default branch's copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandingConfigJudgement {
    /// Nothing changes for this build: the branch left the file as it found
    /// it, the file it lands reads with nothing new set aside, or the target's
    /// copy is already unreadable, which is not this landing's doing.
    Unaffected,
    /// The landed file would not load where the target's copy does.
    Unreadable(UnreadableProjectConfig),
    /// The landed file loads, but sets aside checks the target's copy runs or
    /// does not declare: they will not run until the app is rebuilt.
    NeedsNewerBuild(BTreeMap<String, UnsupportedCheck>),
}

/// Judge the file a branch would land, from its content at the fork point, on
/// the target, and at the branch head (`None` where the file is absent).
///
/// The head is judged, not a merge result: the two agree whenever the branch
/// is current with the target, which a base advance keeps it. Like the parse
/// gate, only what the branch changed is judged, and only positive evidence
/// counts against it: a defect the target already carries is not this
/// landing's to answer for.
pub fn judge_landing_config(
    fork: Option<&[u8]>,
    target: Option<&[u8]>,
    head: Option<&[u8]>,
) -> LandingConfigJudgement {
    judge_landing_config_under(fork, target, head, &CheckVocabulary::this_build())
}

fn judge_landing_config_under(
    fork: Option<&[u8]>,
    target: Option<&[u8]>,
    head: Option<&[u8]>,
    vocabulary: &CheckVocabulary,
) -> LandingConfigJudgement {
    if head == fork {
        return LandingConfigJudgement::Unaffected;
    }
    let on_target = read_project_config_under(target, vocabulary);
    match read_project_config_under(head, vocabulary) {
        ProjectConfigReading::Unreadable(unreadable) => match on_target {
            ProjectConfigReading::Unreadable(_) => LandingConfigJudgement::Unaffected,
            ProjectConfigReading::Readable { .. } => LandingConfigJudgement::Unreadable(unreadable),
        },
        ProjectConfigReading::Readable { mut unsupported } => {
            if let ProjectConfigReading::Readable {
                unsupported: already,
            } = on_target
            {
                unsupported.retain(|name, check| {
                    already.get(name).is_none_or(|before| {
                        (&before.field, &before.value) != (&check.field, &check.value)
                    })
                });
            }
            if unsupported.is_empty() {
                LandingConfigJudgement::Unaffected
            } else {
                LandingConfigJudgement::NeedsNewerBuild(unsupported)
            }
        }
    }
}

pub fn load_setup_commands(project_path: &Path) -> Result<Vec<String>, String> {
    load_execution_project_policy(project_path).map(|policy| policy.setup_commands)
}

/// Load just the `checks` contract from a project's `.cairn/config.yaml`.
///
/// Reads the file directly without the migration rewrite (and its scoped git
/// commit) that `load_project_settings_read_only` performs, mirroring
/// [`load_terminal_commands`]. The synchronous `when:write` check runner calls
/// this against the project's LIVE main checkout on every sealed commit, so it
/// must stay side-effect free — a migration commit fired from inside an agent
/// run would be surprising. Returns `None` when the file is absent, invalid, or
/// declares no checks.
pub fn load_checks(project_path: &Path) -> Option<HashMap<String, CheckCommand>> {
    load_checks_contract(project_path).map(|contract| contract.checks)
}

/// A project's checks contract: the checks themselves plus the node-level extra
/// inputs their `scope` closures compose in. They travel together because the
/// input selector cannot be resolved from either half alone.
#[derive(Debug, Clone, Default)]
pub struct ChecksContract {
    pub checks: HashMap<String, CheckCommand>,
    pub extra_inputs: HashMap<String, Vec<String>>,
    /// Declared checks this build cannot run. Never planned; carried so every
    /// surface that lists the contract can say why they are not running.
    pub unsupported: BTreeMap<String, UnsupportedCheck>,
}

/// Load the checks contract from a CHECKOUT without migrating. See [`load_checks`]
/// for why this path must stay side-effect free. Returns `None` when the file is
/// absent, invalid, or declares no checks.
///
/// This is the PROJECT-LEVEL read: the Settings editor and the project-wide
/// display of "what checks does this project declare". It is deliberately NOT
/// the source for a cadence — a cadence's contract comes from the commit it is
/// evaluating (core’s `execution::checks::checks_contract_at_commit`), because a
/// project-sourced contract is what let one branch's config edit rewrite every
/// sibling job's checks (CAIRN-3333).
pub fn load_checks_contract(project_path: &Path) -> Option<ChecksContract> {
    let (file, _) = load_project_settings_file(project_path).ok()?;
    checks_contract_from(file)
}

/// The checks contract carried by already-parsed settings. `None` when the file
/// declares no `checks` at all, which every caller treats as "this project has no
/// checks" rather than "an empty set of checks".
pub fn checks_contract_from(file: ProjectSettingsFile) -> Option<ChecksContract> {
    let checks = file.checks?;
    Some(ChecksContract {
        checks,
        extra_inputs: file.extra_inputs.unwrap_or_default(),
        unsupported: file.unsupported_checks,
    })
}

/// The landing rules carried by already-parsed settings; none declared is the
/// empty set, under which every path keeps the line merge.
pub fn landing_from(file: &ProjectSettingsFile) -> LandingSettings {
    file.landing.clone().unwrap_or_default()
}

/// Managed top-level config keys: the current schema fields plus the legacy keys
/// the migration removes. A top-level key on disk that is NOT in this set is a
/// user-authored key the comment-preserving merge leaves untouched.
const MANAGED_TOP_LEVEL_KEYS: &[&str] = &[
    "setupCommands",
    "terminalCommands",
    "checks",
    "extraInputs",
    "defaultBranch",
    "references",
    "worktree",
    "tierDefaults",
    "activeBackend",
    "backends",
    "mcpServers",
    "contextualPackages",
    "landing",
    "pinBumps",
    // Legacy keys the load-path migration strips.
    "ciCommands",
    "copyFiles",
];

static PROJECT_SETTINGS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Mutate project settings as one process-wide, fail-closed transaction.
///
/// Existing non-empty documents are always updated through the preserving YAML
/// merge. A missing or empty file may be serialized from scratch. The rendered
/// bytes are canonically reparsed and atomically replaced before a scoped commit
/// is attempted.
pub fn mutate_project_settings<T>(
    project_path: &Path,
    mutate: impl FnOnce(&mut ProjectSettingsFile) -> Result<T, String>,
) -> Result<T, ProjectConfigError> {
    let result = mutate_project_settings_inner(project_path, mutate);
    if let Err(error) = &result {
        log::error!("{error}");
    }
    result
}

fn mutate_project_settings_inner<T>(
    project_path: &Path,
    mutate: impl FnOnce(&mut ProjectSettingsFile) -> Result<T, String>,
) -> Result<T, ProjectConfigError> {
    let lock = PROJECT_SETTINGS_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let path = get_project_config_path(project_path);
    let index_already_tracked = project_config_is_tracked(&path);
    let staged_index_entry = staged_project_config_entry(&path);
    let original = match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(ProjectConfigError::Read {
                path,
                source: error.to_string(),
            })
        }
    };
    let existing_nonempty = original
        .as_deref()
        .is_some_and(|bytes| !bytes.iter().all(u8::is_ascii_whitespace));
    let (mut settings, migration) = if existing_nonempty {
        parse_project_settings_bytes(&path, original.as_deref().unwrap())?
    } else {
        (
            ProjectSettingsFile::default(),
            ProjectConfigMigration::Current,
        )
    };
    let before = serde_yaml::to_value(&settings).map_err(|error| ProjectConfigError::Render {
        path: path.clone(),
        source: error.to_string(),
    })?;
    let value = mutate(&mut settings).map_err(|source| ProjectConfigError::Mutation {
        path: path.clone(),
        source,
    })?;
    let after = serde_yaml::to_value(&settings).map_err(|error| ProjectConfigError::Render {
        path: path.clone(),
        source: error.to_string(),
    })?;

    if before == after && migration == ProjectConfigMigration::Current {
        return Ok(value);
    }

    let rendered = render_settings_yaml(&path, original.as_deref(), existing_nonempty, &settings)?;
    parse_project_settings_bytes(&path, rendered.as_bytes())?;
    if original.as_deref() == Some(rendered.as_bytes()) {
        return Ok(value);
    }
    if let Some(original) = original.as_deref() {
        guard_destructive_change(&path, original, rendered.as_bytes())?;
    }

    let parent = path.parent().expect("project config always has a parent");
    std::fs::create_dir_all(parent).map_err(|error| ProjectConfigError::Persist {
        path: path.clone(),
        source: format!("failed to create config directory: {error}"),
    })?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| ProjectConfigError::Persist {
            path: path.clone(),
            source: format!("failed to create temporary file: {error}"),
        })?;
    temporary
        .write_all(rendered.as_bytes())
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| ProjectConfigError::Persist {
            path: path.clone(),
            source: format!("failed to write and sync temporary file: {error}"),
        })?;
    temporary
        .persist(&path)
        .map_err(|error| ProjectConfigError::Persist {
            path: path.clone(),
            source: format!("failed to atomically replace config: {}", error.error),
        })?;

    if let Err(source) = super::commit_config_path_required(&path, "cairn: update project config") {
        rollback_project_config(&path, original.as_deref(), index_already_tracked).map_err(
            |rollback| ProjectConfigError::Commit {
                path: path.clone(),
                source: format!("{source}; rollback also failed: {rollback}"),
            },
        )?;
        return Err(ProjectConfigError::Commit { path, source });
    }
    if let Some(entry) = staged_index_entry {
        restore_project_config_index(&path, &entry).map_err(|source| {
            ProjectConfigError::Commit {
                path: path.clone(),
                source: format!(
                    "config committed but prior staged state could not be restored: {source}"
                ),
            }
        })?;
    }
    Ok(value)
}

#[derive(Debug)]
struct StagedIndexEntry {
    mode: String,
    object: String,
    root: PathBuf,
    relative: PathBuf,
}

fn staged_project_config_entry(path: &Path) -> Option<StagedIndexEntry> {
    let root = super::git_work_tree_root(path.parent()?)?;
    let relative = super::git_relative_path(path, &root)?;
    let output = crate::env::git()
        .args(["ls-files", "--stage", "--"])
        .arg(&relative)
        .current_dir(&root)
        .output()
        .ok()?;
    let line = std::str::from_utf8(&output.stdout).ok()?.lines().next()?;
    let mut fields = line.split_whitespace();
    let mode = fields.next()?.to_string();
    let object = fields.next()?.to_string();
    let head_object = crate::env::git()
        .arg("rev-parse")
        .arg(format!("HEAD:{}", relative.to_string_lossy()))
        .current_dir(&root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string());
    if head_object.as_deref() == Some(object.as_str()) {
        return None;
    }
    Some(StagedIndexEntry {
        mode,
        object,
        root,
        relative,
    })
}

fn restore_project_config_index(_path: &Path, entry: &StagedIndexEntry) -> Result<(), String> {
    let cache_info = format!(
        "{},{},{}",
        entry.mode,
        entry.object,
        entry.relative.to_string_lossy()
    );
    let output = crate::env::git()
        .args(["update-index", "--cacheinfo", &cache_info])
        .current_dir(&entry.root)
        .output()
        .map_err(|error| format!("failed to restore staged config entry: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn project_config_is_tracked(path: &Path) -> bool {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return false;
    };
    crate::env::git()
        .args(["ls-files", "--error-unmatch", "--"])
        .arg(name)
        .current_dir(parent)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn rollback_project_config(
    path: &Path,
    original: Option<&[u8]>,
    index_already_tracked: bool,
) -> Result<(), String> {
    match original {
        Some(bytes) => std::fs::write(path, bytes)
            .map_err(|error| format!("failed to restore original bytes: {error}"))?,
        None => match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("failed to remove newly created config: {error}")),
        },
    }
    if index_already_tracked {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "config has no parent".to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "config has no file name".to_string())?;
    let output = crate::env::git()
        .args(["reset", "--"])
        .arg(name)
        .current_dir(parent)
        .output()
        .map_err(|error| format!("failed to restore Git index: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "failed to restore Git index: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// Test helper for exercising complete typed replacements through the transaction.
/// It still receives all strict loading, merge, guard, and persistence guarantees.
#[cfg(any(test, feature = "test-utils"))]
pub fn save_project_settings(
    project_path: &Path,
    settings: &ProjectSettingsFile,
) -> Result<(), String> {
    mutate_project_settings(project_path, |current| {
        *current = settings.clone();
        Ok(())
    })
    .map_err(|error| error.to_string())
}

/// The settings as the document they render to: the typed fields, plus every
/// unsupported check written back exactly as it was declared. A check the
/// caller declared under the same name replaces it, since that is an edit of
/// the check rather than a failure to read it.
fn settings_document(
    path: &Path,
    settings: &ProjectSettingsFile,
) -> Result<serde_yaml::Value, ProjectConfigError> {
    let mut document =
        serde_yaml::to_value(settings).map_err(|error| ProjectConfigError::Render {
            path: path.to_path_buf(),
            source: error.to_string(),
        })?;
    if settings.unsupported_checks.is_empty() {
        return Ok(document);
    }
    let serde_yaml::Value::Mapping(root) = &mut document else {
        return Ok(document);
    };
    let checks = root
        .entry(serde_yaml::Value::String("checks".to_string()))
        .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()));
    if let serde_yaml::Value::Mapping(checks) = checks {
        for (name, unsupported) in &settings.unsupported_checks {
            checks
                .entry(serde_yaml::Value::String(name.clone()))
                .or_insert_with(|| unsupported.declared.clone());
        }
    }
    Ok(document)
}

fn render_settings_yaml(
    path: &Path,
    original: Option<&[u8]>,
    existing_nonempty: bool,
    settings: &ProjectSettingsFile,
) -> Result<String, ProjectConfigError> {
    let target_value = settings_document(path, settings)?;
    let target_mapping = match &target_value {
        serde_yaml::Value::Mapping(m) => m,
        _ => {
            return Err(ProjectConfigError::Render {
                path: path.to_path_buf(),
                source: "project settings did not serialize to a mapping".to_string(),
            })
        }
    };

    if existing_nonempty {
        let original = std::str::from_utf8(original.expect("nonempty content must exist"))
            .map_err(|error| ProjectConfigError::InvalidUtf8 {
                path: path.to_path_buf(),
                source: error.to_string(),
            })?;
        return super::yaml_edit::merge_into_yaml(original, target_mapping, MANAGED_TOP_LEVEL_KEYS)
            .map_err(|error| ProjectConfigError::Render {
                path: path.to_path_buf(),
                source: format!("comment-preserving merge refused the existing document: {error}"),
            });
    }

    let yaml =
        serde_yaml::to_string(&target_value).map_err(|error| ProjectConfigError::Render {
            path: path.to_path_buf(),
            source: error.to_string(),
        })?;
    Ok(format!("# Cairn Project Configuration\n{}", yaml))
}

fn guard_destructive_change(
    path: &Path,
    before: &[u8],
    after: &[u8],
) -> Result<(), ProjectConfigError> {
    fn non_comment_lines(bytes: &[u8]) -> usize {
        String::from_utf8_lossy(bytes)
            .lines()
            .filter(|line| {
                let line = line.trim();
                !line.is_empty() && !line.starts_with('#')
            })
            .count()
    }
    let before_lines = non_comment_lines(before);
    let after_lines = non_comment_lines(after);
    let removes_half_lines = before_lines >= 20 && after_lines.saturating_mul(2) <= before_lines;
    let removes_half_bytes = before.len() >= 1024 && after.len().saturating_mul(2) <= before.len();
    if removes_half_lines || removes_half_bytes {
        return Err(ProjectConfigError::DestructiveChange {
            path: path.to_path_buf(),
            before_lines,
            after_lines,
            before_bytes: before.len(),
            after_bytes: after.len(),
        });
    }
    Ok(())
}

/// Create a default project config file with commented template
pub fn create_default_project_config(project_path: &Path) -> Result<(), String> {
    let path = get_project_config_path(project_path);

    // Don't overwrite existing config
    if path.exists() {
        return Ok(());
    }

    // Ensure .cairn directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create .cairn directory: {}", e))?;
    }

    let template = r#"# Cairn Project Configuration
#
# This file configures how Cairn works with this project.
# It can be committed to version control to share settings with your team.
#
# Build-cell population: control which gitignored content is pre-populated.
# Paths matching 'copy' patterns are copied from the main repo into each cell.
# Unmatched paths are skipped — setup commands handle the rest.
#
# materialization:
#   populate:
#     copy:
#       - ".env"
#       - ".env.*"

# Commands to run when setting up a new worktree
# setupCommands:
#   - npm install

# Quick-access terminal commands
# terminalCommands:
#   - name: Dev Server
#     command: npm run dev
#   - name: Tests (watch)
#     command: npm test -- --watch

# Default branch for the project (defaults to 'main')
# defaultBranch: main

# External reference repositories and directories
# references:
#   - name: docs
#     git: https://github.com/org/docs.git
#     description: Project documentation
#   - name: specs
#     path: ~/Documents/specs
#     description: Hardware specifications
"#;

    std::fs::write(&path, template)
        .map_err(|e| format!("Failed to write project config template: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn strict_loader_distinguishes_missing_invalid_utf8_parse_and_root() {
        let temp = TempDir::new().unwrap();
        let path = get_project_config_path(temp.path());
        assert!(matches!(
            load_project_settings_strict(temp.path()),
            Err(ProjectConfigError::Missing { .. })
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, [0xff]).unwrap();
        assert!(matches!(
            load_project_settings_strict(temp.path()),
            Err(ProjectConfigError::InvalidUtf8 { .. })
        ));
        std::fs::write(&path, "checks: [").unwrap();
        assert!(matches!(
            load_project_settings_strict(temp.path()),
            Err(ProjectConfigError::Parse { .. })
        ));
        std::fs::write(&path, "- list\n").unwrap();
        assert!(matches!(
            load_project_settings_strict(temp.path()),
            Err(ProjectConfigError::InvalidRoot { .. })
        ));
        std::fs::write(&path, "just a scalar\n").unwrap();
        assert!(matches!(
            load_project_settings_strict(temp.path()),
            Err(ProjectConfigError::InvalidRoot { .. })
        ));
        // A comment-only document parses as a null root and is valid: it is
        // exactly what create_default_project_config generates.
        std::fs::write(&path, "# only comments\n\n# more comments\n").unwrap();
        let (settings, migration) = load_project_settings_strict(temp.path()).unwrap();
        assert_eq!(
            serde_yaml::to_string(&settings).unwrap(),
            serde_yaml::to_string(&ProjectSettingsFile::default()).unwrap()
        );
        assert_eq!(migration, ProjectConfigMigration::Current);
    }

    #[test]
    fn generated_template_loads_as_defaults_and_yields_execution_policy() {
        let temp = TempDir::new().unwrap();
        create_default_project_config(temp.path()).unwrap();

        let (settings, migration) = load_project_settings_strict(temp.path()).unwrap();
        assert_eq!(
            serde_yaml::to_string(&settings).unwrap(),
            serde_yaml::to_string(&ProjectSettingsFile::default()).unwrap()
        );
        assert_eq!(migration, ProjectConfigMigration::Current);

        let policy = load_execution_project_policy(temp.path()).unwrap();
        assert_eq!(policy, ExecutionProjectPolicy::default());
    }

    #[test]
    fn saving_onto_generated_template_preserves_comments_and_appends_keys() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        create_default_project_config(temp.path()).unwrap();
        let config_path = get_project_config_path(temp.path());
        let template = std::fs::read_to_string(&config_path).unwrap();

        let settings = ProjectSettingsFile {
            default_branch: Some("develop".to_string()),
            setup_commands: Some(vec!["bun install".to_string()]),
            ..Default::default()
        };
        save_project_settings(temp.path(), &settings).unwrap();

        let written = std::fs::read_to_string(&config_path).unwrap();
        assert!(
            written.starts_with(&template),
            "comment template must survive"
        );
        let (reloaded, _) = load_project_settings_strict(temp.path()).unwrap();
        assert_eq!(reloaded.default_branch.as_deref(), Some("develop"));
        assert_eq!(
            reloaded.setup_commands,
            Some(vec!["bun install".to_string()])
        );
    }

    #[test]
    fn transactional_mutation_refuses_malformed_and_preserves_unknown_content() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let path = get_project_config_path(temp.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "checks: [").unwrap();
        let error = mutate_project_settings(temp.path(), |settings| {
            settings.default_branch = Some("next".into());
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(error, ProjectConfigError::Parse { .. }));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "checks: [");

        let original = "# keep me\nfutureKey:\n  nested: true\ndefaultBranch: main\n";
        std::fs::write(&path, original).unwrap();
        mutate_project_settings(temp.path(), |settings| {
            settings.default_branch = Some("next".into());
            Ok(())
        })
        .unwrap();
        let rendered = std::fs::read_to_string(&path).unwrap();
        assert!(rendered.contains("# keep me"));
        assert!(rendered.contains("futureKey:\n  nested: true"));
        assert!(rendered.contains("defaultBranch: next"));
    }

    #[test]
    fn transactional_mutation_creates_noops_and_guards_large_deletions() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let path = get_project_config_path(temp.path());
        mutate_project_settings(temp.path(), |settings| {
            settings.default_branch = Some("main".into());
            Ok(())
        })
        .unwrap();
        let created = std::fs::read(&path).unwrap();
        mutate_project_settings(temp.path(), |_| Ok(())).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), created);

        let before = (0..30)
            .map(|index| format!("unknown{index}: {}\n", "x".repeat(40)))
            .collect::<String>()
            + "defaultBranch: main\n";
        std::fs::write(&path, &before).unwrap();
        let error = guard_destructive_change(&path, before.as_bytes(), b"defaultBranch: main\n")
            .unwrap_err();
        assert!(matches!(
            error,
            ProjectConfigError::DestructiveChange {
                before_lines: 31,
                after_lines: 1,
                ..
            }
        ));
    }

    #[test]
    fn removed_executor_keys_are_accepted_and_dropped_on_rewrite() {
        let yaml = r#"
run:
  executor: build-slot
checks:
  frontend:
    command: vitest run
    when: review
    executor: build-slot
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let frontend = settings.checks.as_ref().unwrap().get("frontend").unwrap();
        assert_eq!(frontend.command, "vitest run");
        assert_eq!(frontend.when, CheckWhen::Review);

        let serialized = serde_yaml::to_string(&settings).unwrap();
        assert!(!serialized.contains("run:"));
        assert!(!serialized.contains("executor:"));
    }

    #[test]
    fn resolve_default_branch_precedence() {
        let mut config = ProjectSettingsFile::default();
        // Nothing configured: hard fallback.
        assert_eq!(resolve_default_branch(&config, None), "main");
        // Stored column used when there is no config override.
        assert_eq!(resolve_default_branch(&config, Some("staging")), "staging");
        // Config override wins over the stored column.
        config.default_branch = Some("develop".to_string());
        assert_eq!(resolve_default_branch(&config, Some("staging")), "develop");
    }

    #[test]
    fn test_project_settings_defaults() {
        let settings = ProjectSettingsFile::default();
        assert!(settings.setup_commands.is_none());
        assert!(settings.terminal_commands.is_none());
        assert!(settings.checks.is_none());
        assert!(settings.default_branch.is_none());
        assert!(settings.populate_config().is_empty());
    }

    #[test]
    fn test_project_settings_roundtrip() {
        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["npm install".to_string()]),
            terminal_commands: Some(vec![TerminalCommand {
                name: "Dev Server".to_string(),
                command: "npm run dev".to_string(),
            }]),
            default_branch: Some("develop".to_string()),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&settings).unwrap();
        let parsed: ProjectSettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.setup_commands, settings.setup_commands);
        assert_eq!(parsed.default_branch, settings.default_branch);
        assert_eq!(parsed.terminal_commands.as_ref().map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_checks_parse_and_roundtrip() {
        let yaml = r#"
checks:
  frontend:
    command: vitest related {changedFiles}
    fullCommand: vitest run
    impact:
      - src/**
      - packages/ui/**
    policy: gate
    when: idle
    verdictEnvironment:
      - FEATURE_FLAG
      - SERVICE_TOKEN
    executor: build-slot
  typecheck:
    command: tsc --noEmit
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let checks = settings.checks.as_ref().unwrap();
        let frontend = checks.get("frontend").unwrap();
        assert_eq!(frontend.command, "vitest related {changedFiles}");
        assert_eq!(frontend.full_command.as_deref(), Some("vitest run"));
        assert_eq!(frontend.policy, CheckPolicy::Gate);
        // `when: idle` is a legacy alias that deserializes to the merged Review
        // cadence (see `CheckWhen`).
        assert_eq!(frontend.when, CheckWhen::Review);
        assert_eq!(
            frontend.verdict_environment,
            vec!["FEATURE_FLAG".to_string(), "SERVICE_TOKEN".to_string()]
        );
        assert_eq!(
            frontend.impact.as_deref(),
            Some(&["src/**".to_string(), "packages/ui/**".to_string()][..])
        );

        let typecheck = checks.get("typecheck").unwrap();
        assert_eq!(typecheck.command, "tsc --noEmit");
        assert!(typecheck.full_command.is_none());
        assert_eq!(typecheck.policy, CheckPolicy::Advisory);
        assert_eq!(typecheck.when, CheckWhen::Write);
        assert!(typecheck.verdict_environment.is_empty());

        let serialized = serde_yaml::to_string(&settings).unwrap();
        let reparsed: ProjectSettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.checks, settings.checks);
    }

    #[test]
    fn pin_bumps_parse_round_trip_and_refuse_unknown_keys() {
        let yaml = r#"
pinBumps:
  codex:
    pin: tools/codex-pin.json
    frozen: [integration]
    checks: [codex-pin, codex-contract]
    platforms: [linux, macos]
"#;
        let (settings, legacy) = parse_project_settings(yaml).unwrap();
        assert!(!legacy);
        let bumps = settings.pin_bumps.as_ref().unwrap();
        assert_eq!(
            bumps.get("codex"),
            Some(&PinBumpSpec {
                pin: "tools/codex-pin.json".to_string(),
                frozen: vec!["integration".to_string()],
                checks: vec!["codex-pin".to_string(), "codex-contract".to_string()],
                platforms: vec!["linux".to_string(), "macos".to_string()],
            })
        );
        let serialized = serde_yaml::to_string(&settings).unwrap();
        assert!(serialized.contains("pinBumps:"), "{serialized}");
        let (reparsed, _) = parse_project_settings(&serialized).unwrap();
        assert_eq!(reparsed.pin_bumps, settings.pin_bumps);

        let misspelled =
            "pinBumps:\n  codex:\n    pin: p.json\n    froze: [integration]\n    checks: [a]\n    platforms: [linux]\n";
        let error = parse_project_settings(misspelled).unwrap_err();
        assert!(error.contains("froze"), "{error}");
    }

    #[test]
    fn test_check_timeout_parses_and_round_trips() {
        // A per-check `timeout` (seconds) parses, defaults to `None` when absent,
        // and survives serialization. `timeout` has no `skip_serializing_if`
        // default guard, so a plain non-zero value round-trips faithfully.
        let yaml = r#"
checks:
  rust-full:
    command: bun run test:rust
    when: review
    timeout: 2400
  frontend:
    command: vitest run
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let checks = settings.checks.as_ref().unwrap();
        assert_eq!(checks.get("rust-full").unwrap().timeout, Some(2400));
        // A check that omits `timeout` deserializes to `None` (cadence default).
        assert_eq!(checks.get("frontend").unwrap().timeout, None);

        // Round-trip: the configured timeout survives a serialize/parse cycle.
        let serialized = serde_yaml::to_string(&settings).unwrap();
        let reparsed: ProjectSettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.checks, settings.checks);
    }

    #[test]
    fn test_checks_reject_unknown_enums() {
        let bad_policy = r#"
checks:
  frontend:
    command: vitest run
    policy: blocking
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(bad_policy)
            .expect_err("unknown policy should fail");
        assert!(err.to_string().contains("blocking"));
        assert!(err.to_string().contains("advisory") || err.to_string().contains("Advisory"));

        let bad_when = r#"
checks:
  frontend:
    command: vitest run
    when: commit
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(bad_when)
            .expect_err("unknown cadence should fail");
        assert!(err.to_string().contains("commit"));
        assert!(err.to_string().contains("write") || err.to_string().contains("Write"));

        // `pr` was the old cadence name; it is now rejected in favor of `review`.
        let legacy_pr = r#"
checks:
  frontend:
    command: vitest run
    when: pr
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(legacy_pr)
            .expect_err("legacy `pr` cadence should fail");
        assert!(err.to_string().contains("pr"));

        // `review` is the current full-suite cadence and parses.
        let review = r#"
checks:
  frontend:
    command: vitest run
    when: review
"#;
        let settings = serde_yaml::from_str::<ProjectSettingsFile>(review)
            .expect("`review` cadence should parse");
        assert_eq!(
            settings.checks.unwrap().get("frontend").unwrap().when,
            CheckWhen::Review
        );
    }

    /// A fixture modelled on this repository's `.cairn/config.yaml`, not the
    /// file itself: the `landing-compile` check exactly as #4311 merged it,
    /// whose `when: landing` cadence this build does not have (the landing
    /// gate was removed), beside siblings of the other shapes (a `write`
    /// check, a `review` check, and one placed by an executor selector with
    /// `verdictPlatforms`). Carried inline because the public mirror
    /// `check:external-host` builds has no `.cairn/`.
    const CONFIG_WITH_LANDING_COMPILE: &str = r#"
setupCommands:
- bun install --frozen-lockfile
defaultBranch: main
checks:
  containment-inventory:
    command: bun run containment:inventory:check
    policy: advisory
    when: write
    impact:
    - src-tauri/os/cairn-common/src/executor_protocol.rs
    - docs/security/executor-containment.md
  licenses:
    command: bun run check:licenses
    when: review
    policy: advisory
    impact:
    - src-tauri/Cargo.lock
    - src-tauri/deny.toml
  invoke-admission:
    command: 'bun run test:rust -p cairn-transport --lib routes::invoke::admission::'
    executor:
      requiredToolchains:
      - bun
      - rust
    verdictPlatforms:
    - linux
    policy: advisory
    when: write
    impact:
    - src-tauri/cairn-transport/src/routes/invoke/**
  landing-compile:
    command: cargo check --manifest-path src-tauri/Cargo.toml --workspace --exclude cairn --tests --locked
    executor:
      requiredToolchains:
      - bun
      - rust
    verdictPlatforms:
    - linux
    when: landing
    budgetSeconds: 600
    timeout: 1800
    impact:
    - src-tauri/**
    - rust-toolchain*
    - .cairn/config.yaml
    - package.json
    - scripts/**
    - fixtures/**
    - test-fixtures/**
"#;

    #[test]
    fn a_check_with_a_cadence_this_build_lacks_is_set_aside_alone() {
        assert_only_landing_compile_set_aside(CONFIG_WITH_LANDING_COMPILE, 3);
    }

    /// The same proof over the live repository config, with `landing-compile`
    /// put back as #4311 merged it. Read at runtime and skipped when absent:
    /// the public mirror carries no `.cairn/`.
    #[test]
    fn the_repository_config_with_landing_compile_put_back_sets_aside_only_that_check() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.cairn/config.yaml");
        let Ok(repository) = std::fs::read_to_string(&path) else {
            return;
        };
        let fixture: serde_yaml::Value = serde_yaml::from_str(CONFIG_WITH_LANDING_COMPILE).unwrap();
        let landing_compile = fixture["checks"]["landing-compile"].clone();

        let mut document: serde_yaml::Value = serde_yaml::from_str(&repository).unwrap();
        let checks = document
            .get_mut("checks")
            .and_then(serde_yaml::Value::as_mapping_mut)
            .expect("the repository config declares checks");
        checks.remove("landing-compile");
        let others = checks.len();
        checks.insert("landing-compile".into(), landing_compile);

        assert_only_landing_compile_set_aside(&serde_yaml::to_string(&document).unwrap(), others);
    }

    /// `content` declares `landing-compile` beside `others` sibling checks.
    fn assert_only_landing_compile_set_aside(content: &str, others: usize) {
        let (settings, _) = parse_project_settings(content)
            .expect("one unknown cadence must not fail the project config");
        let unsupported = settings
            .unsupported_checks
            .get("landing-compile")
            .expect("landing-compile is listed as unsupported");
        assert_eq!(unsupported.field, "when");
        assert_eq!(unsupported.value, "landing");
        assert_eq!(unsupported.known, ["write", "idle", "review"]);
        assert_eq!(settings.unsupported_checks.len(), 1);
        let checks = settings.checks.as_ref().unwrap();
        assert!(!checks.contains_key("landing-compile"));
        assert_eq!(checks.len(), others, "every other check is intact");

        let contract = checks_contract_from(settings).unwrap();
        assert_eq!(contract.checks.len(), others);
        assert!(contract.unsupported.contains_key("landing-compile"));
    }

    #[test]
    fn placement_policy_never_reads_the_checks_section() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        // A check no build could read, beside a section that is not placement's.
        let content =
            "setupCommands:\n  - bun install\nchecks:\n  broken:\n    when: review\nbackends: 7\n";
        std::fs::write(&config_path, content).unwrap();

        assert!(load_project_settings_strict(temp.path()).is_err());
        let policy = load_execution_project_policy(temp.path())
            .expect("placement reads only setupCommands and materialization");
        assert_eq!(policy.setup_commands, vec!["bun install"]);

        // The file must still be YAML: a document nothing can read refuses
        // placement, as before.
        std::fs::write(&config_path, "setupCommands: [unterminated").unwrap();
        let error = load_execution_project_policy(temp.path()).unwrap_err();
        assert!(error.contains("failed to parse project config"), "{error}");
    }

    #[test]
    fn a_malformed_check_still_fails_the_checks_contract_by_name() {
        let error = parse_project_settings("checks:\n  frontend:\n    when: review\n").unwrap_err();
        assert!(error.contains("checks.frontend"), "{error}");
        assert!(error.contains("command"), "{error}");
    }

    #[test]
    fn a_config_reads_as_what_failing_it_would_cost_the_project() {
        let read = |content: &str| read_project_config(Some(content.as_bytes()));
        assert_eq!(
            read_project_config(None),
            ProjectConfigReading::Readable {
                unsupported: BTreeMap::new()
            }
        );
        // Placement's own keys broken: every execution is refused.
        let ProjectConfigReading::Unreadable(placement) = read("setupCommands: 7\n") else {
            panic!("a setupCommands that is not a list cannot load placement");
        };
        assert_eq!(placement.cost, UnreadableConfigCost::EveryExecution);
        let ProjectConfigReading::Unreadable(yaml) = read("checks: [unterminated") else {
            panic!("a document that is not YAML cannot load placement");
        };
        assert_eq!(yaml.cost, UnreadableConfigCost::EveryExecution);
        // Placement loads, the settings do not.
        let ProjectConfigReading::Unreadable(settings) =
            read("checks:\n  frontend:\n    when: review\n")
        else {
            panic!("a check with no command fails the settings");
        };
        assert_eq!(settings.cost, UnreadableConfigCost::ChecksAndSettings);
        assert!(
            settings.reason.contains("checks.frontend"),
            "{}",
            settings.reason
        );
    }

    #[test]
    fn landing_judges_only_what_the_branch_changed_against_the_target() {
        let vocabulary = CheckVocabulary::this_build();
        let with_landing = CONFIG_WITH_LANDING_COMPILE;
        let base = "checks:\n  frontend:\n    command: vitest run\n";
        let broken = "checks:\n  frontend:\n    when: review\n";
        let judge = |fork: &str, target: &str, head: &str| {
            judge_landing_config_under(
                Some(fork.as_bytes()),
                Some(target.as_bytes()),
                Some(head.as_bytes()),
                &vocabulary,
            )
        };

        // The #4311 shape: the branch declares a cadence this build lacks.
        let LandingConfigJudgement::NeedsNewerBuild(unsupported) = judge(base, base, with_landing)
        else {
            panic!("a landed check this build cannot run must be named");
        };
        assert_eq!(unsupported.keys().collect::<Vec<_>>(), ["landing-compile"]);
        assert_eq!(unsupported["landing-compile"].value, "landing");
        // A check the target already sets aside the same way is not news.
        assert_eq!(
            judge(base, with_landing, &format!("{with_landing}\n# edited\n")),
            LandingConfigJudgement::Unaffected
        );

        // A file that stops loading is refused, with what it would cost.
        let LandingConfigJudgement::Unreadable(unreadable) = judge(base, base, broken) else {
            panic!("a landed file that stops loading must be refused");
        };
        assert_eq!(unreadable.cost, UnreadableConfigCost::ChecksAndSettings);
        let LandingConfigJudgement::Unreadable(placement) =
            judge(base, base, "setupCommands: [unterminated")
        else {
            panic!("a landed file placement cannot read must be refused");
        };
        assert_eq!(placement.cost, UnreadableConfigCost::EveryExecution);

        // Positive evidence only: the target is already unreadable, or the
        // branch left the file as it found it.
        assert_eq!(
            judge(base, broken, broken),
            LandingConfigJudgement::Unaffected
        );
        assert_eq!(
            judge(broken, base, broken),
            LandingConfigJudgement::Unaffected
        );
        assert_eq!(
            judge_landing_config_under(None, None, None, &vocabulary),
            LandingConfigJudgement::Unaffected
        );
        // Deleting the file lands the defaults, which every build reads.
        assert_eq!(
            judge_landing_config_under(
                Some(base.as_bytes()),
                Some(base.as_bytes()),
                None,
                &vocabulary
            ),
            LandingConfigJudgement::Unaffected
        );
    }

    #[test]
    fn a_settings_save_keeps_a_check_this_build_cannot_read() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let path = get_project_config_path(temp.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "checks:\n  # from a newer Cairn\n  future:\n    command: cargo check\n    when: nightly\n  frontend:\n    command: vitest run\n",
        )
        .unwrap();

        let (loaded, _) = load_project_settings_strict(temp.path()).unwrap();
        assert!(loaded.unsupported_checks.contains_key("future"));
        // The settings editor saves the whole map of the checks it can show.
        mutate_project_settings(temp.path(), |settings| {
            let mut checks = settings.checks.clone().unwrap();
            checks.get_mut("frontend").unwrap().policy = CheckPolicy::Gate;
            settings.checks = Some(checks);
            Ok(())
        })
        .unwrap();

        let rendered = std::fs::read_to_string(&path).unwrap();
        assert!(rendered.contains("when: nightly"), "{rendered}");
        assert!(rendered.contains("# from a newer Cairn"), "{rendered}");
        let (reloaded, _) = load_project_settings_strict(temp.path()).unwrap();
        assert_eq!(reloaded.unsupported_checks["future"].value, "nightly");
        assert_eq!(
            reloaded.checks.unwrap()["frontend"].policy,
            CheckPolicy::Gate
        );
    }

    #[test]
    fn legacy_idle_cadence_aliases_to_review() {
        // The `idle` cadence was collapsed into `review`; a `#[serde(alias)]`
        // keeps un-migrated project configs parsing (rather than silently
        // disabling every check) by mapping the old name onto the merged cadence.
        let yaml = r#"
checks:
  frontend:
    command: vitest run
    when: idle
"#;
        let settings =
            serde_yaml::from_str::<ProjectSettingsFile>(yaml).expect("`idle` alias should parse");
        assert_eq!(
            settings.checks.unwrap().get("frontend").unwrap().when,
            CheckWhen::Review
        );
    }

    #[test]
    fn test_checks_survive_legacy_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        let legacy_content = r#"ciCommands:
  - npm test
checks:
  frontend:
    command: vitest run
    policy: gate
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        let loaded = load_project_settings_read_only(project_path);
        let frontend = loaded
            .checks
            .as_ref()
            .and_then(|checks| checks.get("frontend"))
            .expect("checks should survive migration-triggering load");
        assert_eq!(frontend.command, "vitest run");
        assert_eq!(frontend.policy, CheckPolicy::Gate);

        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            legacy_content,
            "tolerant reads must not migrate configuration"
        );
    }

    #[test]
    fn load_checks_reads_without_migrating() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        // A legacy config (ciCommands) that `load_project_settings_read_only` would rewrite
        // and commit. `load_checks` must read the checks WITHOUT that side effect.
        let legacy =
            "ciCommands:\n  - npm test\nchecks:\n  frontend:\n    command: vitest run\n    when: write\n";
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy).unwrap();

        let checks = load_checks(project_path).expect("checks present");
        assert!(checks.contains_key("frontend"));
        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            legacy,
            "load_checks must not rewrite the file"
        );
    }

    #[test]
    fn load_checks_none_when_absent_or_no_checks() {
        let temp = TempDir::new().unwrap();
        assert!(load_checks(temp.path()).is_none(), "absent file ⇒ None");

        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "setupCommands:\n  - npm install\n").unwrap();
        assert!(load_checks(temp.path()).is_none(), "no checks key ⇒ None");
    }

    #[test]
    fn test_project_settings_without_checks_is_load_save_stable() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        let content =
            "# Cairn Project Configuration\nsetupCommands:\n- npm install\ndefaultBranch: main\n";
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let (_loaded, needs_migration) = load_project_settings_file(project_path).unwrap();
        assert!(!needs_migration);
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), content);
    }

    #[test]
    fn test_yaml_deserialization_partial() {
        let yaml = r#"
setupCommands:
  - npm install
defaultBranch: main
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            settings.setup_commands,
            Some(vec!["npm install".to_string()])
        );
        assert_eq!(settings.default_branch, Some("main".to_string()));
        assert!(settings.terminal_commands.is_none());
        assert!(settings.populate_config().is_empty());
    }

    #[test]
    fn test_materialization_legacy_seed_ignored_parsed() {
        let yaml = r#"
materialization:
  seedIgnored: false
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        // Legacy field is deserialized as Option<bool>
        assert_eq!(
            settings
                .materialization
                .as_ref()
                .and_then(|w| w.seed_ignored),
            Some(false)
        );
        // But populate_config is still empty (legacy field doesn't populate anything)
        assert!(settings.populate_config().is_empty());
    }

    #[test]
    fn test_materialization_populate_config() {
        let yaml = r#"
materialization:
  populate:
    copy:
      - ".env"
      - ".env.*"
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let config = settings.populate_config();

        assert!(!config.is_empty());
        assert_eq!(config.copy, vec![".env", ".env.*"]);
    }

    #[test]
    fn retired_symlink_populate_strategy_is_rejected_with_its_replacement_named() {
        let yaml = r#"
materialization:
  populate:
    copy:
      - ".env"
    symlink:
      - "target/"
"#;
        let error = parse_project_settings(yaml).unwrap_err();
        assert!(
            error.contains("materialization.populate.symlink is no longer supported"),
            "{error}"
        );
        assert!(error.contains("target/"), "{error}");
        assert!(error.contains("materialization.populate.copy"), "{error}");
        assert!(error.contains("skipped"), "{error}");

        // An empty list names no pattern, so there is nothing to migrate.
        let empty = "materialization:\n  populate:\n    copy: [\".env\"]\n    symlink: []\n";
        let (settings, _) = parse_project_settings(empty).unwrap();
        assert_eq!(settings.populate_config().copy, vec![".env"]);
    }

    #[test]
    fn test_legacy_copy_files_triggers_migration() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let project_path = temp.path();

        // Write a legacy config with copyFiles
        let legacy_content = r#"setupCommands:
  - npm install
copyFiles:
  - .env
  - config/secrets.yaml
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        // Migration runs only through the transaction boundary.
        let loaded =
            mutate_project_settings(project_path, |settings| Ok(settings.clone())).unwrap();
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));

        // Verify file was migrated (no copyFiles)
        let migrated_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(!migrated_content.contains("copyFiles"));
    }

    #[test]
    fn test_file_save_and_load() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let project_path = temp.path();

        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["cargo build".to_string()]),
            default_branch: Some("main".to_string()),
            ..Default::default()
        };

        save_project_settings(project_path, &settings).unwrap();

        let loaded = load_project_settings_read_only(project_path);
        assert_eq!(loaded.setup_commands, settings.setup_commands);
        assert_eq!(loaded.default_branch, settings.default_branch);
    }

    fn git_init(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_bare(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_set_origin(repo: &Path, origin: &Path) {
        assert!(crate::env::git()
            .args(["remote", "add", "origin"])
            .arg(origin)
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
    }

    fn git_commit_count(path: &Path) -> usize {
        let out = crate::env::git()
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    }

    fn git_branch(path: &Path) -> String {
        let out = crate::env::git()
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn origin_has_branch(origin: &Path, branch: &str) -> bool {
        crate::env::git()
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .current_dir(origin)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[test]
    fn save_project_settings_commits_scoped_and_pushes() {
        let temp = TempDir::new().unwrap();
        let origin = temp.path().join("origin.git");
        std::fs::create_dir_all(&origin).unwrap();
        git_bare(&origin);
        let proj = temp.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        git_init(&proj);
        git_set_origin(&proj, &origin);
        std::fs::write(proj.join("unrelated.txt"), "dirty").unwrap();

        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["npm install".to_string()]),
            ..Default::default()
        };
        save_project_settings(&proj, &settings).unwrap();

        assert_eq!(git_head_subject(&proj), "cairn: update project config");
        // Exactly one scoped commit (no migration/double commit).
        assert_eq!(git_commit_count(&proj), 1);
        let status = git_status(&proj);
        assert!(
            status.contains("unrelated.txt"),
            "unrelated stays dirty: {status:?}"
        );
        assert!(
            !status.contains("config.yaml"),
            "config.yaml committed: {status:?}"
        );
        // The commit is pushed to origin best-effort on a detached thread, so a
        // diverged local main can never break issue PR merges. Poll until the
        // background push lands the branch.
        let branch = git_branch(&proj);
        let mut pushed = false;
        for _ in 0..40 {
            if origin_has_branch(&origin, &branch) {
                pushed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(pushed, "settings save should push its commit to origin");
    }

    #[test]
    fn test_populate_config_save_and_load_roundtrip() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let project_path = temp.path();

        let settings = ProjectSettingsFile {
            materialization: Some(MaterializationSettings {
                seed_ignored: None,
                populate: PopulateConfig {
                    copy: vec![".env".to_string(), ".env.*".to_string()],
                },
            }),
            ..Default::default()
        };

        save_project_settings(project_path, &settings).unwrap();

        let loaded = load_project_settings_read_only(project_path);
        let config = loaded.populate_config();
        assert_eq!(config.copy, vec![".env", ".env.*"]);

        // Verify the serialized YAML contains expected structure
        let config_path = get_project_config_path(project_path);
        let raw = std::fs::read_to_string(&config_path).unwrap();
        assert!(raw.contains("populate"));
        assert!(raw.contains(".env"));
        assert!(!raw.contains("symlink"));
        // seedIgnored should never appear in new files
        assert!(!raw.contains("seedIgnored"));
    }

    #[test]
    fn test_legacy_seed_populate_config_is_ignored() {
        // The `seed` materialization-populate mechanism was removed (CAIRN-2622): the
        // clone source was a live dev-instance target dir and could capture a
        // torn snapshot; sccache is the canonical cross-worktree compile cache.
        // A pre-existing config that still carries a `seed:` block must keep
        // deserializing — the key is now an ignored unknown field, and the
        // surviving copy rules are honored.
        let raw = r#"
materialization:
  populate:
    copy:
      - ".env"
    seed:
      - from: "~/.warm/target"
        to: "src-tauri/target"
        exclude: ["*/incremental"]
"#;

        let settings: ProjectSettingsFile = serde_yaml::from_str(raw).unwrap();
        let config = settings.populate_config();
        assert_eq!(config.copy, vec![".env"]);
    }

    #[test]
    fn test_create_default_config() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        create_default_project_config(project_path).unwrap();

        let config_path = get_project_config_path(project_path);
        assert!(config_path.exists());

        let content = std::fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("# Cairn Project Configuration"));
        assert!(content.contains("setupCommands"));
    }

    #[test]
    fn test_create_default_config_no_overwrite() {
        let temp = TempDir::new().unwrap();
        init_git_repo(temp.path());
        let project_path = temp.path();

        // Create a custom config first
        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["custom".to_string()]),
            ..Default::default()
        };
        save_project_settings(project_path, &settings).unwrap();

        // Try to create default - should not overwrite
        create_default_project_config(project_path).unwrap();

        let loaded = load_project_settings_read_only(project_path);
        assert_eq!(loaded.setup_commands, Some(vec!["custom".to_string()]));
    }

    #[test]
    fn test_get_project_config_path() {
        let path = get_project_config_path(Path::new("/home/user/project"));
        assert_eq!(path, PathBuf::from("/home/user/project/.cairn/config.yaml"));
    }

    #[test]
    fn test_reference_serde_git() {
        let yaml = r#"
references:
  - name: openpnp
    git: https://github.com/openpnp/openpnp.git
    description: OpenPnP source code
    branch: develop
  - name: local-specs
    path: ~/Documents/specs
    description: Hardware specifications
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let references = settings.references.unwrap();
        assert_eq!(references.len(), 2);

        assert_eq!(references[0].name, "openpnp");
        assert_eq!(
            references[0].git.as_deref(),
            Some("https://github.com/openpnp/openpnp.git")
        );
        assert!(references[0].path.is_none());
        assert_eq!(references[0].branch.as_deref(), Some("develop"));

        assert_eq!(references[1].name, "local-specs");
        assert!(references[1].git.is_none());
        assert_eq!(references[1].path.as_deref(), Some("~/Documents/specs"));
        assert!(references[1].branch.is_none());
    }

    #[test]
    fn test_reference_roundtrip() {
        let settings = ProjectSettingsFile {
            references: Some(vec![ProjectReference {
                name: "docs".to_string(),
                git: Some("https://github.com/org/docs.git".to_string()),
                path: None,
                description: Some("Project docs".to_string()),
                branch: None,
            }]),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&settings).unwrap();
        let parsed: ProjectSettingsFile = serde_yaml::from_str(&yaml).unwrap();
        let references = parsed.references.unwrap();
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].name, "docs");
        assert_eq!(
            references[0].git.as_deref(),
            Some("https://github.com/org/docs.git")
        );
    }

    #[test]
    fn test_settings_without_references() {
        let yaml = r#"
setupCommands:
  - npm install
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(settings.references.is_none());
    }

    #[test]
    fn test_resources_key_is_not_accepted_as_references() {
        let yaml = r#"
resources:
  - name: docs
    git: https://github.com/org/docs.git
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(settings.references.is_none());
    }

    #[test]
    fn test_legacy_config_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Write a legacy config with ciCommands and persistent field
        let legacy_content = r#"# Legacy config
ciCommands:
  - npm test
  - npm run build
setupCommands:
  - npm install
terminalCommands:
  - name: Dev Server
    command: npm run dev
    persistent: true
defaultBranch: main
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        // Load should trigger migration
        let loaded = load_project_settings_read_only(project_path);

        // Verify deprecated fields are gone
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));
        assert_eq!(loaded.default_branch, Some("main".to_string()));
        assert!(loaded.terminal_commands.is_some());
        let cmds = loaded.terminal_commands.unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "Dev Server");
        assert_eq!(cmds[0].command, "npm run dev");

        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            legacy_content,
            "read-only loading must not migrate legacy configuration"
        );
    }

    #[test]
    fn test_preset_fields_survive_legacy_parse() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Write a config with both legacy fields and preset overrides
        let content = r#"
setupCommands:
  - npm install
activeBackend: codex
backends:
  codex:
    lg:
      model: gpt-5.5
      options:
        reasoningEffort: high
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let loaded = load_project_settings_read_only(project_path);

        // Preset fields must survive the legacy parse path
        assert_eq!(loaded.legacy_active_backend(), Some("codex"));
        assert!(loaded.backends.is_some());
        let backends = loaded.backends.unwrap();
        assert!(backends.contains_key("codex"));
        assert_eq!(backends["codex"]["lg"].model.as_str(), "gpt-5.5");
    }

    #[test]
    fn terminal_commands_do_not_serialize_repository_filesystem_capabilities() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        let content = r#"
ciCommands:
  - npm test
terminalCommands:
  - name: Dev
    command: "bun dev:instance --seed empty"
    write:
      - "~/.aws"
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let loaded = load_terminal_commands(project_path);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Dev");
        assert_eq!(loaded[0].command, "bun dev:instance --seed empty");

        load_project_settings_read_only(project_path);
        assert_eq!(std::fs::read_to_string(config_path).unwrap(), content);
    }

    #[test]
    fn load_terminal_commands_empty_when_absent() {
        let temp = TempDir::new().unwrap();
        assert!(load_terminal_commands(temp.path()).is_empty());
    }

    #[test]
    fn test_preset_fields_survive_migration_rewrite() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Legacy config with ciCommands (triggers migration) AND preset overrides
        let content = r#"
ciCommands:
  - npm test
setupCommands:
  - npm install
materialization:
  seedIgnored: false
activeBackend: codex
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        // Load triggers migration (rewrites file without ciCommands)
        let loaded = load_project_settings_read_only(project_path);

        // Preset fields must survive the migration rewrite
        assert_eq!(loaded.legacy_active_backend(), Some("codex"));
        // Legacy seedIgnored is parsed but results in empty populate config
        assert!(loaded.populate_config().is_empty());

        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            content,
            "legacy configuration remains byte-for-byte unchanged on read"
        );
    }

    fn init_git_repo(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn commit_all(repo: &Path, msg: &str) {
        crate::env::git()
            .args(["add", "-A"])
            .current_dir(repo)
            .status()
            .unwrap();
        crate::env::git()
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@local.invalid",
                "commit",
                "-q",
                "-m",
                msg,
            ])
            .current_dir(repo)
            .status()
            .unwrap();
    }

    fn git_status(path: &Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn execution_policy_loads_setup_and_populate_without_rewriting() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        let content = "ciCommands:\n  - old\nsetupCommands:\n  - bun install\nmaterialization:\n  populate:\n    copy: [.env, cache/]\n";
        std::fs::write(&config_path, content).unwrap();

        let policy = load_execution_project_policy(temp.path()).unwrap();
        assert_eq!(policy.setup_commands, vec!["bun install"]);
        assert_eq!(policy.populate.copy, vec![".env", "cache/"]);
        assert_eq!(std::fs::read_to_string(config_path).unwrap(), content);
    }

    #[test]
    fn execution_policy_surfaces_a_retired_symlink_rule_instead_of_defaulting() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        let content = "materialization:\n  populate:\n    symlink: [cache/]\n";
        std::fs::write(&config_path, content).unwrap();

        let error = load_execution_project_policy(temp.path()).unwrap_err();
        assert!(
            error.contains("materialization.populate.symlink is no longer supported"),
            "{error}"
        );
        assert_eq!(std::fs::read_to_string(config_path).unwrap(), content);
    }

    #[test]
    fn setup_command_hot_path_is_fallible_and_side_effect_free() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "setupCommands: [unterminated").unwrap();
        assert!(load_setup_commands(temp.path()).is_err());

        let legacy = "ciCommands:\n  - npm test\nsetupCommands:\n  - npm install\n";
        std::fs::write(&config_path, legacy).unwrap();
        assert_eq!(
            load_setup_commands(temp.path()).unwrap(),
            vec!["npm install".to_string()]
        );
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), legacy);
    }

    #[test]
    fn legacy_config_migration_commits_in_canonical_repo() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        init_git_repo(project_path);

        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            "ciCommands:\n  - npm test\nsetupCommands:\n  - npm install\n",
        )
        .unwrap();
        commit_all(project_path, "seed legacy config");

        // A transaction migrates the file (drops ciCommands) and commits the
        // rewrite; the tolerant read-only path above remains side-effect free.
        let loaded =
            mutate_project_settings(project_path, |settings| Ok(settings.clone())).unwrap();
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));

        let migrated = std::fs::read_to_string(&config_path).unwrap();
        assert!(!migrated.contains("ciCommands"));
        assert!(
            git_status(project_path).is_empty(),
            "migration left the canonical repo dirty"
        );
        assert_eq!(
            git_head_subject(project_path),
            "cairn: update project config"
        );
    }

    #[test]
    fn transaction_restores_preexisting_staged_config_entry_after_commit() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        #[cfg(unix)]
        let repo = {
            let alias = temp.path().join("alias");
            std::os::unix::fs::symlink(&repo, &alias).unwrap();
            alias
        };
        let project_path = repo.as_path();
        init_git_repo(project_path);
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "defaultBranch: main\n").unwrap();
        commit_all(project_path, "seed config");

        std::fs::write(&config_path, "defaultBranch: staged\n").unwrap();
        assert!(crate::env::git()
            .args(["add", "--", ".cairn/config.yaml"])
            .current_dir(project_path)
            .status()
            .unwrap()
            .success());
        std::fs::write(&config_path, "defaultBranch: worktree\n").unwrap();

        mutate_project_settings(project_path, |settings| {
            settings.default_branch = Some("app".to_string());
            Ok(())
        })
        .unwrap();

        let staged = crate::env::git()
            .args(["show", ":.cairn/config.yaml"])
            .current_dir(project_path)
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&staged.stdout),
            "defaultBranch: staged\n"
        );
        let committed = crate::env::git()
            .args(["show", "HEAD:.cairn/config.yaml"])
            .current_dir(project_path)
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&committed.stdout),
            "defaultBranch: app\n"
        );
        assert_eq!(
            std::fs::read_to_string(config_path).unwrap(),
            "defaultBranch: app\n"
        );
    }
}
